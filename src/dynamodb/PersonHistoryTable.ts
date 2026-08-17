import { IContext } from '../../context/IContext';
import { DynamoDBTable } from './DynamoDBTable';

/**
 * PersonHistory DynamoDB Table Constants
 * 
 * Table Design:
 * - Purpose: Append-only audit trail of person state changes
 * - PK: personId - Unique identifier for each person
 * - SK: syncRunId - ISO timestamp of sync run that created this record
 * 
 * Attributes:
 * - personId: string (PK) - Unique identifier (e.g., "U12345678")
 * - syncRunId: string (SK) - ISO timestamp of sync run
 * - hash: string - Hash value at this point in time
 * - changeType: 'NEW' | 'UPDATED' | 'DELETED' - Type of change
 * - previousHash?: string - Previous hash value (for UPDATED only)
 * 
 * Write Policy:
 * - NEW: First time person appears in source
 * - UPDATED: Hash changed from previous sync
 * - DELETED: Person removed from source (detected by merger)
 * - UNCHANGED: DO NOT WRITE (skipped entirely)
 * 
 * Access Patterns:
 * 1. Get person's complete history: Query by personId
 * 2. Get all changes in a sync run: Query GSI1 by syncRunId
 * 3. Get changes of specific type in sync run: Query GSI1 by syncRunId + changeType prefix
 * 4. Get all changes of a specific type across runs: Query GSI2 by changeType
 * 
 * Usage:
 * - Processors: PutItem for NEW and UPDATED persons
 * - Merger: PutItem for DELETED persons
 * - Reporting: Query for audit trails and analytics
 */

/**
 * Generate table name following existing naming convention
 * @param context - IContext with STACK_ID and TAGS.Landscape
 * @returns Table name: ${STACK_ID}-person-history-${landscape}
 */
export const DYNAMODB_TABLE_NAME = (context: IContext) => 
  `${context.STACK_ID}-person-history-${context.TAGS.Landscape.toLowerCase()}`;

/**
 * Partition key: personId
 * Example: "U12345678"
 */
export const DYNAMODB_PARTITION_KEY = 'personId';

/**
 * Sort key: syncRunId (ISO timestamp)
 * Example: "2026-03-03T19:58:41.277Z"
 * Enables chronological ordering of history records
 */
export const DYNAMODB_SORT_KEY = 'syncRunId';

/**
 * GSI1: Query all changes in a specific sync run
 * PK: syncRunId
 * SK: changeType (simplified - no composite field)
 * Use case: "Get all NEW persons in sync 2026-03-03T19:58:41.277Z"
 * Use case: "Get all changes in sync 2026-03-03T19:58:41.277Z"
 */
export const DYNAMODB_GSI1_INDEX_NAME = 'syncRunId-changeType-index';
export const DYNAMODB_GSI1_PARTITION_KEY = 'syncRunId';
export const DYNAMODB_GSI1_SORT_KEY = 'changeType';

/**
 * GSI2: Query all changes of a specific type across runs
 * PK: changeType
 * SK: syncRunId
 * Use case: "Get all DELETED persons in the last 30 days"
 */
export const DYNAMODB_GSI2_INDEX_NAME = 'changeType-syncRunId-index';
export const DYNAMODB_GSI2_PARTITION_KEY = 'changeType';
export const DYNAMODB_GSI2_SORT_KEY = 'syncRunId';

/**
 * Type definition for PersonHistory records
 */
export interface PersonHistoryRecord {
  personId: string;
  syncRunId: string;
  hash: string;
  changeType: 'NEW' | 'UPDATED' | 'DELETED';
  previousHash?: string;
}

/**
 * PersonHistoryTable utility class
 * 
 * Wraps DynamoDBTable with domain-specific operations for person history tracking.
 * Provides high-level methods for processor, merger, and reporting operations.
 * 
 * Usage:
 * ```typescript
 * const historyTable = new PersonHistoryTable(context);
 * 
 * // Processor: Write NEW person
 * await historyTable.writeHistory({
 *   personId: 'U11111',
 *   syncRunId: '2026-03-03T19:58:41.277Z',
 *   hash: 'hash1',
 *   changeType: 'NEW'
 * });
 * 
 * // Merger: Write DELETED person
 * await historyTable.writeHistory({
 *   personId: 'U22222',
 *   syncRunId: '2026-03-03T19:58:41.277Z',
 *   hash: 'oldHash',
 *   changeType: 'DELETED',
 *   previousHash: 'oldHash'
 * });
 * 
 * // Reporting: Get person history
 * const history = await historyTable.getPersonHistory('U11111');
 * 
 * // Reporting: Get all changes in sync run
 * const changes = await historyTable.getChangesInSyncRun('2026-03-03T19:58:41.277Z');
 * 
 * // Reporting: Get only NEW persons in sync run
 * const newPersons = await historyTable.getChangesInSyncRun('2026-03-03T19:58:41.277Z', 'NEW');
 * ```
 */
export class PersonHistoryTable {
  private table: DynamoDBTable;

  constructor(private context: IContext) {
    const region = context.REGION;
    const tableName = DYNAMODB_TABLE_NAME(context);
    const partitionKey = DYNAMODB_PARTITION_KEY;
    const sortKey = DYNAMODB_SORT_KEY;
    
    this.table = new DynamoDBTable({ 
      region, 
      tableName, 
      partitionKey,
      sortKey
    });
  }

  /**
   * Write a history record for a person.
   * Used by processors (NEW/UPDATED) and merger (DELETED).
   * 
   * @param record - PersonHistoryRecord to write
   */
  public async writeHistory(record: PersonHistoryRecord): Promise<void> {
    await this.table.putItem(record);
  }

  /**
   * Batch write history records.
   * More efficient than individual writes when processing many changes.
   * 
   * @param records - Array of PersonHistoryRecord to write
   */
  public async batchWriteHistory(records: PersonHistoryRecord[]): Promise<void> {
    await this.table.batchWrite(records, 'put');
  }

  /**
   * Get complete history for a specific person.
   * Returns all records sorted chronologically (oldest to newest).
   * 
   * @param personId - Person identifier
   * @returns Array of PersonHistoryRecord sorted by syncRunId
   */
  public async getPersonHistory(personId: string): Promise<PersonHistoryRecord[]> {
    const items = await this.table.queryByPartitionKey(personId);
    return items as PersonHistoryRecord[];
  }

  /**
   * Get all changes in a specific sync run, optionally filtered by changeType.
   * Uses GSI1 (syncRunId-changeType-index).
   * 
   * @param syncRunId - ISO timestamp of sync run
   * @param changeType - Optional filter for 'NEW', 'UPDATED', or 'DELETED'
   * @returns Array of PersonHistoryRecord matching criteria
   */
  public async getChangesInSyncRun(
    syncRunId: string, 
    changeType?: 'NEW' | 'UPDATED' | 'DELETED'
  ): Promise<PersonHistoryRecord[]> {
    const items = await this.table.queryGSI(
      DYNAMODB_GSI1_INDEX_NAME,
      DYNAMODB_GSI1_PARTITION_KEY,
      syncRunId,
      changeType ? DYNAMODB_GSI1_SORT_KEY : undefined,
      changeType,
      '=' // Exact match on changeType
    );
    
    return items as PersonHistoryRecord[];
  }

  /**
   * Get all changes of a specific type across all sync runs.
   * Optionally filter by sync run time range using sort key conditions.
   * Uses GSI2 (changeType-syncRunId-index).
   * 
   * @param changeType - 'NEW', 'UPDATED', or 'DELETED'
   * @param syncRunIdStart - Optional: earliest sync run to include (uses >=)
   * @returns Array of PersonHistoryRecord matching criteria
   */
  public async getChangesByType(
    changeType: 'NEW' | 'UPDATED' | 'DELETED',
    syncRunIdStart?: string
  ): Promise<PersonHistoryRecord[]> {
    const items = await this.table.queryGSI(
      DYNAMODB_GSI2_INDEX_NAME,
      DYNAMODB_GSI2_PARTITION_KEY,
      changeType,
      syncRunIdStart ? DYNAMODB_GSI2_SORT_KEY : undefined,
      syncRunIdStart,
      syncRunIdStart ? '>=' : '='
    );
    
    return items as PersonHistoryRecord[];
  }

  /**
   * Get the most recent history record for a person.
   * Useful for determining last known state.
   * 
   * @param personId - Person identifier
   * @returns Most recent PersonHistoryRecord, or undefined if no history exists
   */
  public async getLatestHistory(personId: string): Promise<PersonHistoryRecord | undefined> {
    const history = await this.getPersonHistory(personId);
    return history.length > 0 ? history[history.length - 1] : undefined;
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
   * Delete all history records for a specific sync run.
   * Removes all person history records (NEW, UPDATED, DELETED) associated with
   * the given syncRunId across all persons.
   * 
   * This is useful for:
   * - Cleaning up failed integration runs
   * - Removing test data
   * - Pruning old integration history
   * 
   * Note: This uses GSI1 to efficiently find all records for a sync run.
   * 
   * @param syncRunId - ISO timestamp identifying the sync run to delete
   * @returns Number of records deleted
   */
  public async deleteByPartitionKey(syncRunId: string): Promise<number> {
    // Note: PersonHistory table uses personId as PK, syncRunId as SK
    // We need to query GSI1 to find all records for this syncRunId
    const items = await this.table.queryGSI(
      DYNAMODB_GSI1_INDEX_NAME,
      DYNAMODB_GSI1_PARTITION_KEY,
      syncRunId
    );
    
    if (items.length === 0) {
      console.log(`No history records found for sync run ${syncRunId}`);
      return 0;
    }
    
    console.log(`Found ${items.length} history records for sync run ${syncRunId}, deleting...`);
    
    // Extract keys and batch delete
    const keys = items.map(item => ({
      [DYNAMODB_PARTITION_KEY]: item.personId,
      [DYNAMODB_SORT_KEY]: item.syncRunId
    }));
    
    await this.table.batchWrite(keys, 'delete');
    
    console.log(`Deleted ${keys.length} history records for sync run ${syncRunId}`);
    return keys.length;
  }
}


if(require.main === module) {
  const { TestEnvironment } = require('integration-core');
  const testEnvironment = TestEnvironment('PERSON_HISTORY_TABLE');
  [
    'PERSON_HISTORY_TABLE_TASK',
    'PERSON_HISTORY_TABLE_PERSON_ID',
    'PERSON_HISTORY_TABLE_SYNC_RUN_ID',
    'TRUNCATE_CHUNK_SIZE'
  ].forEach(testEnvironment.getVar);

  const { 
    PERSON_HISTORY_TABLE_TASK: task,
    PERSON_HISTORY_TABLE_PERSON_ID: personId,
    PERSON_HISTORY_TABLE_SYNC_RUN_ID: syncRunId,
    TRUNCATE_CHUNK_SIZE
  } = process.env;

  (async () => {
    const context = require('../../context/context.json') as IContext;
    const historyTable = new PersonHistoryTable(context);
    
    switch(task) {
      case 'truncate':
        const chunkSize = TRUNCATE_CHUNK_SIZE ? parseInt(TRUNCATE_CHUNK_SIZE, 10) : undefined;
        await historyTable.truncate(chunkSize);
        break;
      case 'history':
        if(!personId) {
          console.error('Missing required PERSON_HISTORY_TABLE_PERSON_ID environment variable for history task!');
          process.exit(1);
        }
        const history = await historyTable.getPersonHistory(personId);
        console.log(`Found ${history.length} history record(s) for person ${personId}:`);
        console.log(JSON.stringify(history, null, 2));
        break;
      case 'changes':
        if(!syncRunId) {
          console.error('Missing required PERSON_HISTORY_TABLE_SYNC_RUN_ID environment variable for changes task!');
          process.exit(1);
        }
        const changes = await historyTable.getChangesInSyncRun(syncRunId);
        console.log(`Found ${changes.length} change(s) in sync run ${syncRunId}:`);
        console.log(JSON.stringify(changes, null, 2));
        break;
      case 'delete':
        if(!syncRunId) {
          console.error('Missing required PERSON_HISTORY_TABLE_SYNC_RUN_ID environment variable for delete task!');
          process.exit(1);
        }
        const deletedCount = await historyTable.deleteByPartitionKey(syncRunId);
        console.log(`Deleted ${deletedCount} record(s) for sync run ${syncRunId}`);
        break;
      default:
        console.error(`Unknown task: ${task}. Supported tasks: truncate, history, changes, delete`);
    }
  })();
}

