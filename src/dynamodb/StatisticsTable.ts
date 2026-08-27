import { TestEnvironment } from 'integration-core';
import { IContext } from '../../context/IContext';
import { StatisticsItem } from '../ApiErrorTracking';
import { AbstractDynamoDbTable, DynamoDBTable } from './DynamoDBTable';

export const DYNAMODB_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-statistics-${context.TAGS.Landscape.toLowerCase()}`;
export const DYNAMODB_PARTITION_KEY = 'integrationTimestamp';
export const DYNAMODB_SECONDARY_PARTITION_KEY = 'errorType';
export const DYNAMODB_SORT_KEY = 'eventType';
export const DYNAMODB_GSI_INDEX_NAME = 'errorType-timestamp-index';

export class StatisticsTable {
  private table: AbstractDynamoDbTable;

  constructor(private context: IContext, table?: AbstractDynamoDbTable) {
    if (table) {
      this.table = table;
    } else {
      const region = context.REGION;
      const tableName = DYNAMODB_TABLE_NAME(context);
      const partitionKey = DYNAMODB_PARTITION_KEY;
      const sortKey = DYNAMODB_SORT_KEY;
      this.table = new DynamoDBTable({ region, tableName, partitionKey, sortKey });
    }
  }

  public truncate = async (chunkSize?: number): Promise<void> => {
    await this.table.truncateTable(chunkSize);
  }

  /**
   * Fetch aggregated statistics for a specific integration run.
   * Retrieves the statistics record using the integrationTimestamp as the partition key
   * and "STATISTICS" as the sort key.
   * 
   * Note: This only retrieves the aggregated statistics record (SK = 'STATISTICS').
   * For chunk-specific statistics, use getChunkStatistics() or getAllChunkStatistics().
   * 
   * @param integrationTimestamp - ISO timestamp of the integration run
   * @returns The aggregated statistics item for that run, or undefined if not found
   */
  public getStatistics = async (integrationTimestamp: string): Promise<StatisticsItem | undefined> => {
    const eventType = 'STATISTICS';
    const item = await this.table.getItem({ 
      partitionKeyValue: integrationTimestamp, sortKeyValue: eventType 
    });
    return item as StatisticsItem | undefined;
  }

  /**
   * Fetch statistics for a specific chunk within an integration run.
   * 
   * @param integrationTimestamp - ISO timestamp of the integration run
   * @param chunkId - The chunk identifier (e.g., 'chunk-0009')
   * @returns The statistics item for that chunk, or undefined if not found
   */
  public getChunkStatistics = async (integrationTimestamp: string, chunkId: string): Promise<StatisticsItem | undefined> => {
    const eventType = `STATISTICS-${chunkId}`;
    const item = await this.table.getItem({ 
      partitionKeyValue: integrationTimestamp, sortKeyValue: eventType 
    });
    return item as StatisticsItem | undefined;
  }

  /**
   * Fetch statistics for all chunks within an integration run.
   * Queries all records with sort key beginning with 'STATISTICS-chunk-'.
   * 
   * @param integrationTimestamp - ISO timestamp of the integration run
   * @returns Array of statistics items for all chunks, sorted by sort key
   */
  public getAllChunkStatistics = async (integrationTimestamp: string): Promise<StatisticsItem[]> => {
    const items = await this.table.queryByPartitionKey({ partitionKeyValue: integrationTimestamp, sortKeyPrefix: 'STATISTICS-chunk-' });
    return items as StatisticsItem[];
  }

  /**
   * Get a list of sync operations, uniquely identified by their integration timestamps.
   * This method queries the existing errorType-timestamp-index GSI to efficiently retrieve
   * all statistics records (both aggregated and chunk-specific).
   * 
   * Note: Returns all records with errorType = 'STATISTICS', which includes both
   * aggregated records (SK = 'STATISTICS') and chunk-specific records (SK = 'STATISTICS-chunk-XXX').
   * To get only unique integration runs, use getUniqueIntegrationTimestamps().
   * 
   * @returns An array of integration timestamps (may include duplicates if chunks exist),
   *          sorted chronologically (ascending)
   */
  public getSyncList = async (): Promise<string[]> => {
    // Query the GSI with errorType = "STATISTICS" to get all statistics records
    const items = await this.table.queryGSI({
      indexName: DYNAMODB_GSI_INDEX_NAME,
      gsiPartitionKey: DYNAMODB_SECONDARY_PARTITION_KEY,
      partitionKeyValue: 'STATISTICS'
    });

    // Extract and return the integrationTimestamp from each item
    // Results are already sorted chronologically by the GSI's sort key
    return items.map(item => item.integrationTimestamp as string);
  }

  /**
   * Get a list of unique integration timestamps across all sync operations.
   * This returns deduplicated timestamps, useful for listing distinct integration runs
   * when chunk-specific statistics exist.
   * 
   * @returns An array of unique integration timestamps, sorted chronologically (ascending)
   */
  public getUniqueIntegrationTimestamps = async (): Promise<string[]> => {
    const allTimestamps = await this.getSyncList();
    // Deduplicate using Set, then sort chronologically
    return Array.from(new Set(allTimestamps)).sort();
  }

  /**
   * Write FLAGS record for a sync run.
   * 
   * FLAGS record format:
   * - PK: syncRunId (ISO timestamp)
   * - SK: "FLAGS"
   * - bulkReset: boolean
   * - trustPreviousStorage: boolean
   * - ignoreRemovals: boolean
   * - Other boolean flags as needed
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @param flags - Object containing flag key-value pairs
   */
  public async writeFlags(syncRunId: string, flags: Record<string, any>): Promise<void> {
    const record = {
      [DYNAMODB_PARTITION_KEY]: syncRunId,
      [DYNAMODB_SORT_KEY]: 'FLAGS',
      ...flags
    };
    await this.table.putItem(record);
  }

  /**
   * Read FLAGS record for a sync run.
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @returns FLAGS object if found, undefined otherwise
   */
  public async readFlags(syncRunId: string): Promise<Record<string, any> | undefined> {
    const item = await this.table.getItem({ 
      partitionKeyValue: syncRunId, sortKeyValue: 'FLAGS' 
    });
    if (!item) return undefined;
    
    // Remove PK/SK from result
    const { [DYNAMODB_PARTITION_KEY]: _, [DYNAMODB_SORT_KEY]: __, ...flags } = item;
    return flags;
  }

  /**
   * Write METADATA record for a sync run.
   * 
   * METADATA record format:
   * - PK: syncRunId (ISO timestamp)
   * - SK: "METADATA"
   * - startTime: ISO timestamp
   * - endTime: ISO timestamp
   * - totalChunks: number
   * - clientId: string
   * - Other metadata fields as needed
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @param metadata - Object containing metadata key-value pairs
   */
  public async writeMetadata(syncRunId: string, metadata: Record<string, any>): Promise<void> {
    const record = {
      [DYNAMODB_PARTITION_KEY]: syncRunId,
      [DYNAMODB_SORT_KEY]: 'METADATA',
      ...metadata
    };
    await this.table.putItem(record);
  }

  /**
   * Read METADATA record for a sync run.
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @returns METADATA object if found, undefined otherwise
   */
  public async readMetadata(syncRunId: string): Promise<Record<string, any> | undefined> {
    const item = await this.table.getItem({ 
      partitionKeyValue: syncRunId, sortKeyValue: 'METADATA' 
    });
    if (!item) return undefined;
    
    // Remove PK/SK from result
    const { [DYNAMODB_PARTITION_KEY]: _, [DYNAMODB_SORT_KEY]: __, ...metadata } = item;
    return metadata;
  }

  /**
   * Write CHUNK_STATUS record for a specific chunk in a sync run.
   * 
   * CHUNK_STATUS record format:
   * - PK: syncRunId (ISO timestamp)
   * - SK: "CHUNK_STATUS_nnnn" (zero-padded chunk number)
   * - status: 'PENDING' | 'PROCESSING' | 'COMPLETED' | 'FAILED'
   * - startTime: ISO timestamp
   * - endTime: ISO timestamp (when completed/failed)
   * - error: string (when failed)
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @param chunkId - Chunk identifier (e.g., "0009" or "chunk-0009")
   * @param status - Chunk processing status and metadata
   */
  public async writeChunkStatus(
    syncRunId: string, 
    chunkId: string, 
    status: Record<string, any>
  ): Promise<void> {
    // Normalize chunkId to just the number (e.g., "chunk-0009" -> "0009")
    const chunkNumber = chunkId.replace(/^chunk-/, '');
    
    const record = {
      [DYNAMODB_PARTITION_KEY]: syncRunId,
      [DYNAMODB_SORT_KEY]: `CHUNK_STATUS_${chunkNumber}`,
      chunkId: chunkNumber,
      ...status
    };
    await this.table.putItem(record);
  }

  /**
   * Get all CHUNK_STATUS records for a sync run.
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @returns Array of chunk status records
   */
  public async getAllChunkStatuses(syncRunId: string): Promise<any[]> {
    const items = await this.table.queryByPartitionKey({ 
      partitionKeyValue: syncRunId, sortKeyPrefix: 'CHUNK_STATUS_' 
    });
    return items;
  }

  /**
   * Get count of completed chunks for a sync run.
   * Counts CHUNK_STATUS records with status === 'COMPLETED'.
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @returns Number of completed chunks
   */
  public async getCompletedChunkCount(syncRunId: string): Promise<number> {
    const statuses = await this.getAllChunkStatuses(syncRunId);
    return statuses.filter(item => item.status === 'COMPLETED').length;
  }

  /**
   * Get count of failed chunks for a sync run.
   * Counts CHUNK_STATUS records with status === 'FAILED'.
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @returns Number of failed chunks
   */
  public async getFailedChunkCount(syncRunId: string): Promise<number> {
    const statuses = await this.getAllChunkStatuses(syncRunId);
    return statuses.filter(item => item.status === 'FAILED').length;
  }

  /**
   * Delete all records for a specific integration run.
   * Removes all records (STATISTICS, FLAGS, METADATA, CHUNK_STATUS, ERROR records)
   * associated with the given syncRunId.
   * 
   * This is useful for:
   * - Cleaning up failed integration runs
   * - Removing test data
   * - Pruning old integration runs
   * 
   * @param syncRunId - ISO timestamp identifying the integration run to delete
   * @returns Number of records deleted
   */
  public async deleteByPartitionKey(syncRunId: string): Promise<number> {
    return await this.table.deleteByPartitionKey({ partitionKeyValue: syncRunId });
  }
}


if(require.main === module) {
  const testEnvironment = TestEnvironment('STATISTICS_TABLE');
  [
    'STATISTICS_TABLE_TASK',
    'STATISTICS_TABLE_INTEGRATION_TIMESTAMP',
    'TRUNCATE_CHUNK_SIZE'
  ].forEach(testEnvironment.getVar);

  const { 
    STATISTICS_TABLE_TASK: task, STATISTICS_TABLE_INTEGRATION_TIMESTAMP: timestamp,
    TRUNCATE_CHUNK_SIZE
  } = process.env;

  (async () => {
    const context = require('../../context/context.json') as IContext;
    const statisticsTable = new StatisticsTable(context);
    switch(task) {
      case 'truncate':
        const chunkSize = TRUNCATE_CHUNK_SIZE ? parseInt(TRUNCATE_CHUNK_SIZE, 10) : undefined;
        await statisticsTable.truncate(chunkSize);
        break;
      case 'statistics':
        if(!timestamp) {
          console.error('Missing required STATISTICS_TABLE_INTEGRATION_TIMESTAMP environment variable for statistics task!');
          process.exit(1);
        }
        const stats = await statisticsTable.getStatistics(timestamp);
        if(stats) {
          console.log(`Statistics for integration run at ${timestamp}:`, JSON.stringify(stats, null, 2));
        } else {
          console.log(`No statistics found for integration run at ${timestamp}`);
        }
        break;
      case 'list':
        const syncList = await statisticsTable.getUniqueIntegrationTimestamps();
        console.log(`Found ${syncList.length} unique integration run(s):`);
        console.log(JSON.stringify(syncList, null, 2));
        break;
      case 'chunks':
        if(!timestamp) {
          console.error('Missing required STATISTICS_TABLE_INTEGRATION_TIMESTAMP environment variable for chunks task!');
          process.exit(1);
        }
        const chunks = await statisticsTable.getAllChunkStatistics(timestamp);
        console.log(`Found ${chunks.length} chunk(s) for integration run at ${timestamp}:`);
        console.log(JSON.stringify(chunks, null, 2));
        break;
      case 'delete':
        if(!timestamp) {
          console.error('Missing required STATISTICS_TABLE_INTEGRATION_TIMESTAMP environment variable for delete task!');
          process.exit(1);
        }
        const deletedCount = await statisticsTable.deleteByPartitionKey(timestamp);
        console.log(`Deleted ${deletedCount} record(s) for integration run at ${timestamp}`);
        break;
      default:
        console.error(`Unknown task: ${task}. Supported tasks: truncate, statistics, list, chunks, delete`);
    }
  })();
}

