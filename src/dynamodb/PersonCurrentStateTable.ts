import { IContext } from '../../context/IContext';
import { DynamoDBTable } from './DynamoDBTable';

/**
 * PersonCurrentState DynamoDB Table Constants
 * 
 * Table Design:
 * - Purpose: Track current hash state for each person (one record per person)
 * - PK: personId - Unique identifier for each person
 * - No SK: Single record per person (overwrite on change)
 * 
 * Attributes:
 * - personId: string (PK) - Unique identifier (e.g., "U12345678")
 * - hash: string - Current computed hash from person data
 * - syncRunId: string - ISO timestamp of last sync that modified this person
 * 
 * Access Patterns:
 * 1. Batch fetch by personId: Used by processors to get previous hashes for delta computation
 * 2. Query by syncRunId via GSI: Used by merger for deletion detection
 * 
 * Usage:
 * - Processors: BatchGetItem to fetch previous state for chunk
 * - Processors: BatchWriteItem to update/create records (skip UNCHANGED)
 * - Merger: Query GSI to find all persons seen in current sync
 */

/**
 * Generate table name following existing naming convention
 * @param context - IContext with STACK_ID and TAGS.Landscape
 * @returns Table name: ${STACK_ID}-person-current-state-${landscape}
 */
export const DYNAMODB_TABLE_NAME = (context: IContext) => 
  `${context.STACK_ID}-person-current-state-${context.TAGS.Landscape.toLowerCase()}`;

/**
 * Isolated PersonCurrentState table for mocked (source simulator + mock target) runs, so
 * simulated hash state never mixes with production person state.
 */
export const DYNAMODB_MOCK_TABLE_NAME = (context: IContext) => 
  `${context.STACK_ID}-mock-person-current-state-${context.TAGS.Landscape.toLowerCase()}`;

/**
 * Partition key: personId
 * Example: "U12345678"
 */
export const DYNAMODB_PARTITION_KEY = 'personId';

/**
 * GSI for querying by sync run
 * PK: syncRunId (ISO timestamp)
 * SK: personId
 * Use case: "Get all persons seen in sync run 2026-03-03T19:58:41.277Z"
 */
export const DYNAMODB_GSI_INDEX_NAME = 'syncRunId-personId-index';
export const DYNAMODB_GSI_PARTITION_KEY = 'syncRunId';
export const DYNAMODB_GSI_SORT_KEY = 'personId';

/**
 * Type definition for PersonCurrentState records
 */
export interface PersonCurrentStateRecord {
  personId: string;
  hash: string;
  syncRunId: string;
}

/**
 * PersonCurrentStateTable utility class
 * 
 * Wraps DynamoDBTable with domain-specific operations for person state tracking.
 * Provides high-level methods for processor and merger phase operations.
 * 
 * Usage:
 * ```typescript
 * const stateTable = new PersonCurrentStateTable(context);
 * 
 * // Processor: Get previous state for chunk
 * const personIds = ['U11111', 'U22222', 'U33333'];
 * const previousStates = await stateTable.batchGetPersonState(personIds);
 * 
 * // Processor: Write updated state
 * const updatedStates = [
 *   { personId: 'U11111', hash: 'hash1', syncRunId: '2026-03-03T19:58:41.277Z' },
 *   { personId: 'U22222', hash: 'hash2', syncRunId: '2026-03-03T19:58:41.277Z' }
 * ];
 * await stateTable.batchWritePersonState(updatedStates);
 * 
 * // Merger: Find all persons in sync run
 * const personsInRun = await stateTable.getPersonsInSyncRun('2026-03-03T19:58:41.277Z');
 * ```
 */
export class PersonCurrentStateTable {
  private table: DynamoDBTable;
  private tableName: string;

  constructor(private context: IContext) {
    const region = context.REGION;
    const tableName = DYNAMODB_TABLE_NAME(context);
    const partitionKey = DYNAMODB_PARTITION_KEY;
    
    this.tableName = tableName;
    this.table = new DynamoDBTable({ 
      region, 
      tableName, 
      partitionKey 
      // No sortKey - single record per person
    });
  }

  /**
   * Batch fetch person state records by personId.
   * Returns a map of personId -> PersonCurrentStateRecord for efficient lookup.
   * 
   * This method is used by processors to retrieve previous hash values
   * for delta computation. Missing personIds indicate new persons (no previous state).
   * 
   * @param personIds - Array of person identifiers to fetch
   * @returns Map of personId to PersonCurrentStateRecord (only includes found records)
   */
  public async batchGetPersonState(personIds: string[]): Promise<Map<string, PersonCurrentStateRecord>> {
    const { BatchGetCommand } = await import('@aws-sdk/lib-dynamodb');
    const { DynamoDBClient } = await import('@aws-sdk/client-dynamodb');
    const { DynamoDBDocumentClient } = await import('@aws-sdk/lib-dynamodb');
    
    const client = DynamoDBDocumentClient.from(new DynamoDBClient({ region: this.context.REGION }));
    const stateMap = new Map<string, PersonCurrentStateRecord>();
    
    // DynamoDB BatchGetItem limit is 100 keys per request
    const batchSize = 100;
    
    for (let i = 0; i < personIds.length; i += batchSize) {
      const batch = personIds.slice(i, i + batchSize);
      const keys = batch.map(personId => ({ [DYNAMODB_PARTITION_KEY]: personId }));
      
      const command = new BatchGetCommand({
        RequestItems: {
          [this.tableName]: { Keys: keys }
        }
      });
      
      const result = await client.send(command);
      const items = result.Responses?.[this.tableName] || [];
      
      items.forEach((item: any) => {
        stateMap.set(item.personId, item as PersonCurrentStateRecord);
      });
    }
    
    return stateMap;
  }

  /**
   * Batch write person state records.
   * Overwrites existing records with same personId.
   * 
   * This method is used by processors to update/create state records
   * after computing deltas. Only modified/created persons are written
   * (UNCHANGED persons are skipped to reduce write costs).
   * 
   * @param records - Array of PersonCurrentStateRecord to write
   */
  public async batchWritePersonState(records: PersonCurrentStateRecord[]): Promise<void> {
    await this.table.batchWrite({ items: records, operation: 'put' });
  }

  /**
   * Query all persons seen in a specific sync run.
   * Uses GSI to find all person records with matching syncRunId.
   * 
   * This method is used by the merger phase for deletion detection:
   * - Compare persons in current sync run vs persons in data source
   * - Identify persons no longer in source (candidates for deactivation)
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @returns Array of PersonCurrentStateRecord matching the sync run
   */
  public async getPersonsInSyncRun(syncRunId: string): Promise<PersonCurrentStateRecord[]> {
    const items = await this.table.queryGSI({
      indexName: DYNAMODB_GSI_INDEX_NAME,
      gsiPartitionKey: DYNAMODB_GSI_PARTITION_KEY,
      partitionKeyValue: syncRunId
    });
    
    return items as PersonCurrentStateRecord[];
  }

  /**
   * Get a single person's current state by personId.
   * 
   * @param personId - Person identifier
   * @returns PersonCurrentStateRecord if found, undefined otherwise
   */
  public async getPersonState(personId: string): Promise<PersonCurrentStateRecord | undefined> {
    const item = await this.table.getItem({ partitionKeyValue: personId });
    return item as PersonCurrentStateRecord | undefined;
  }

  /**
   * Truncate the table (delete all records).
   * WARNING: This is destructive and irreversible.
   * Only use for testing or cleanup.
   * 
   * @param chunkSize - Batch size for deletion (default: 25, max: 25)
   */
  public async truncate(chunkSize?: number): Promise<void> {
    await this.table.truncateTable(chunkSize);
  }

  /**
   * Delete all person state records for a specific sync run and restore from previous run.
   * This method:
   * 1. Queries GSI to find all persons modified in the target sync run
   * 2. For each person, queries their PersonHistory to find their previous state
   * 3. If previous state exists: UPDATES the person record with previous hash/syncRunId
   * 4. If no previous state exists: DELETES the person record (they were created in target run)
   * 
   * This is useful for:
   * - Rolling back a failed integration run
   * - Cleaning up test data while preserving prior state
   * - Pruning integration runs while maintaining data integrity
   * 
   * @param syncRunId - ISO timestamp identifying the sync run to delete
   * @param personHistoryTable - PersonHistoryTable instance for finding previous state
   * @returns Object with deletedCount and restoredCount (updated)
   */
  public async deleteByPartitionKeyAndRestore(
    syncRunId: string, 
    personHistoryTable: any
  ): Promise<{ deletedCount: number; restoredCount: number }> {
    console.log(`Restoring PersonCurrentState to pre-${syncRunId} state...`);
    
    // Step 1: Find all persons modified in the target sync run
    const personsInRun = await this.getPersonsInSyncRun(syncRunId);
    
    if (personsInRun.length === 0) {
      console.log(`No person state records found for sync run ${syncRunId}`);
      return { deletedCount: 0, restoredCount: 0 };
    } 
    
    console.log(`Found ${personsInRun.length} person(s) to restore/delete`);
    
    let deletedCount = 0;
    let restoredCount = 0;
    const recordsToUpdate: PersonCurrentStateRecord[] = [];
    const keysToDelete: any[] = [];
    
    // Step 2: For each person, find their previous state
    for (const person of personsInRun) {
      const previousState = await this.findPreviousPersonState(
        person.personId,
        syncRunId,
        personHistoryTable
      );
      
      if (previousState) {
        // Previous state exists - restore it
        recordsToUpdate.push({
          personId: person.personId,
          hash: previousState.hash,
          syncRunId: previousState.syncRunId
        });
        restoredCount++;
      } else {
        // No previous state - person was created in target run, delete them
        keysToDelete.push({
          [DYNAMODB_PARTITION_KEY]: person.personId
        });
        deletedCount++;
      }
    }
    
    // Step 3: Execute batch updates and deletes
    if (recordsToUpdate.length > 0) {
      console.log(`Restoring ${recordsToUpdate.length} person(s) to previous state...`);
      await this.batchWritePersonState(recordsToUpdate);
    }
    
    if (keysToDelete.length > 0) {
      console.log(`Deleting ${keysToDelete.length} person(s) with no previous state...`);
      await this.table.batchWrite({ items: keysToDelete, operation: 'delete' });
    }
    
    console.log(`Restoration complete. Restored: ${restoredCount}, Deleted: ${deletedCount}`);
    return { deletedCount, restoredCount };
  }

  /**
   * Find the previous state for a specific person before a given sync run.
   * Queries PersonHistory for the person and returns the state from the sync run
   * immediately before the target.
   * 
   * @param personId - Person identifier
   * @param targetSyncRunId - The sync run to roll back from
   * @param personHistoryTable - PersonHistoryTable instance
   * @returns Previous state (hash and syncRunId), or undefined if no previous state exists
   */
  private async findPreviousPersonState(
    personId: string,
    targetSyncRunId: string,
    personHistoryTable: any
  ): Promise<{ hash: string; syncRunId: string } | undefined> {
    const { PersonHistoryTable } = await import('./PersonHistoryTable.js');
    const historyTable = personHistoryTable as InstanceType<typeof PersonHistoryTable>;
    
    // Get all history for this person (sorted by syncRunId ascending)
    const history = await historyTable.getPersonHistory(personId);
    
    if (history.length === 0) {
      return undefined; // No history at all
    }
    
    // Find the target syncRunId
    const targetIndex = history.findIndex(h => h.syncRunId === targetSyncRunId);
    
    if (targetIndex <= 0) {
      // Target not found, or it's the first entry (no previous state)
      return undefined;
    }
    
    // Get the previous entry (chronologically before target)
    const previousEntry = history[targetIndex - 1];
    
    // If previous entry is a DELETED record, there's no valid previous state
    if (previousEntry.changeType === 'DELETED') {
      return undefined;
    }
    
    // Return the previous state
    return {
      hash: previousEntry.hash,
      syncRunId: previousEntry.syncRunId
    };
  }
}


if(require.main === module) {
  const { TestEnvironment } = require('integration-core');
  const testEnvironment = TestEnvironment('PERSON_CURRENT_STATE_TABLE');
  [
    'PERSON_CURRENT_STATE_TABLE_TASK',
    'PERSON_CURRENT_STATE_TABLE_PERSON_ID',
    'PERSON_CURRENT_STATE_TABLE_SYNC_RUN_ID',
    'TRUNCATE_CHUNK_SIZE'
  ].forEach(testEnvironment.getVar);

  const { 
    PERSON_CURRENT_STATE_TABLE_TASK: task,
    PERSON_CURRENT_STATE_TABLE_PERSON_ID: personId,
    PERSON_CURRENT_STATE_TABLE_SYNC_RUN_ID: syncRunId,
    TRUNCATE_CHUNK_SIZE
  } = process.env;

  (async () => {
    const context = require('../../context/context.json') as IContext;
    const stateTable = new PersonCurrentStateTable(context);
    
    switch(task) {
      case 'truncate':
        const chunkSize = TRUNCATE_CHUNK_SIZE ? parseInt(TRUNCATE_CHUNK_SIZE, 10) : undefined;
        await stateTable.truncate(chunkSize);
        break;
      case 'get':
        if(!personId) {
          console.error('Missing required PERSON_CURRENT_STATE_TABLE_PERSON_ID environment variable for get task!');
          process.exit(1);
        }
        const state = await stateTable.getPersonState(personId);
        if(state) {
          console.log(`Current state for person ${personId}:`);
          console.log(JSON.stringify(state, null, 2));
        } else {
          console.log(`No current state found for person ${personId}`);
        }
        break;
      case 'list':
        if(!syncRunId) {
          console.error('Missing required PERSON_CURRENT_STATE_TABLE_SYNC_RUN_ID environment variable for list task!');
          process.exit(1);
        }
        const persons = await stateTable.getPersonsInSyncRun(syncRunId);
        console.log(`Found ${persons.length} person(s) in sync run ${syncRunId}:`);
        console.log(JSON.stringify(persons, null, 2));
        break;
      case 'delete-restore':
        if(!syncRunId) {
          console.error('Missing required PERSON_CURRENT_STATE_TABLE_SYNC_RUN_ID environment variable for delete-restore task!');
          process.exit(1);
        }
        const { PersonHistoryTable } = require('./PersonHistoryTable');
        const historyTable = new PersonHistoryTable(context);
        const result = await stateTable.deleteByPartitionKeyAndRestore(syncRunId, historyTable);
        console.log(`Delete-restore complete. Deleted: ${result.deletedCount}, Restored: ${result.restoredCount}`);
        break;
      default:
        console.error(`Unknown task: ${task}. Supported tasks: truncate, get, list, delete-restore`);
    }
  })();
}

