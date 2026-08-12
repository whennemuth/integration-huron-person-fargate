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
    await this.table.batchWrite(records, 'put');
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
    const items = await this.table.queryGSI(
      DYNAMODB_GSI_INDEX_NAME,
      DYNAMODB_GSI_PARTITION_KEY,
      syncRunId
    );
    
    return items as PersonCurrentStateRecord[];
  }

  /**
   * Get a single person's current state by personId.
   * 
   * @param personId - Person identifier
   * @returns PersonCurrentStateRecord if found, undefined otherwise
   */
  public async getPersonState(personId: string): Promise<PersonCurrentStateRecord | undefined> {
    const item = await this.table.getItem(personId);
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
}
