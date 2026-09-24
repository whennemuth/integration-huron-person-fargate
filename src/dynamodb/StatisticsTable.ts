import { TestEnvironment } from 'integration-core';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand, UpdateCommandInput } from '@aws-sdk/lib-dynamodb';
import { IContext } from '../../context/IContext';
import { StatisticsItem } from '../ApiErrorTracking';
import { AbstractDynamoDbTable, DynamoDBTable } from './DynamoDBTable';
import { OCCFlag } from './OCCFlag';

export const DYNAMODB_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-statistics-${context.TAGS.Landscape.toLowerCase()}`;
// Isolated statistics table for mocked (source simulator + mock target) runs, so bulk STATISTICS/ERROR/CHUNK_STATUS records never mix with production data
export const DYNAMODB_MOCK_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-mock-statistics-${context.TAGS.Landscape.toLowerCase()}`;
export const DYNAMODB_PARTITION_KEY = 'integrationTimestamp';
export const DYNAMODB_SECONDARY_PARTITION_KEY = 'errorType';
export const DYNAMODB_SORT_KEY = 'eventType';
export const DYNAMODB_GSI_INDEX_NAME = 'errorType-timestamp-index';

export class StatisticsTable {
  private table: AbstractDynamoDbTable;
  private tableName: string;
  private region: string;

  // An abandoned claim (winning task killed before its finally-block release could run - e.g.
  // docker/chunker.ts's unconditional process.exit()) would otherwise block boosting for the
  // rest of this sync run; comfortably longer than MetricsCatchupDelay's own ~6 minute max wait.
  private static readonly PROCESSOR_BOOST_CLAIM_STALE_AFTER_MS = 10 * 60 * 1000;

  constructor(private context: IContext, table?: AbstractDynamoDbTable, tableNameOverride?: string, regionOverride?: string) {
    if (table) {
      this.table = table;
      this.tableName = tableNameOverride!;
      this.region = regionOverride || process.env.REGION || 'us-east-1';
    } else {
      const region = context.REGION;
      const tableName = DYNAMODB_TABLE_NAME(context);
      const partitionKey = DYNAMODB_PARTITION_KEY;
      const sortKey = DYNAMODB_SORT_KEY;
      this.table = new DynamoDBTable({ region, tableName, partitionKey, sortKey });
      this.tableName = tableName;
      this.region = region;
    }
  }

  /**
   * Build a StatisticsTable from an explicit table name instead of an IContext, for runtime
   * paths (chunker/processor/merger Docker entry points) where a full IContext isn't available.
   */
  public static fromTableName(tableName: string, region?: string): StatisticsTable {
    const resolvedRegion = region || process.env.REGION || 'us-east-1';
    const table = new DynamoDBTable({
      region: resolvedRegion,
      tableName, partitionKey: DYNAMODB_PARTITION_KEY, sortKey: DYNAMODB_SORT_KEY
    });
    return new StatisticsTable({} as IContext, table, tableName, resolvedRegion);
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
   * Update METADATA record with additional fields.
   * Merges new fields into existing METADATA record without overwriting other fields.
   * 
   * This is useful for marking completion milestones such as:
   * - mergerTriggered: true
   * - mergerTriggeredAt: ISO timestamp
   * - mergerTriggeredBy: chunkId
   * 
   * @param syncRunId - ISO timestamp identifying the sync run
   * @param updates - Object containing fields to add/update in metadata
   */
  public async updateMetadata(syncRunId: string, updates: Record<string, any>): Promise<void> {
    // Read existing metadata
    const existing = await this.readMetadata(syncRunId);
    if (!existing) {
      throw new Error(`Cannot update metadata: METADATA record not found for syncRunId ${syncRunId}`);
    }

    // Merge updates with existing data
    const merged = {
      ...existing,
      ...updates
    };

    // Write back to DynamoDB (putItem with PK/SK will overwrite the record)
    await this.table.putItem({
      [DYNAMODB_PARTITION_KEY]: syncRunId,
      [DYNAMODB_SORT_KEY]: 'METADATA',
      ...merged
    });
  }

  /**
   * Atomically add this task's own chunkCount/totalRecords contribution to the run's METADATA
   * record - every parallel chunker task calls this once (partial or full-iterationLimit),
   * replacing the old single-writer S3 rescan. One-time descriptive fields (source, chunkDirectory,
   * etc.) are set via if_not_exists() so whichever task arrives first populates them, and every
   * later call's SET is a harmless no-op re-assertion of the same value - combined, in the same
   * UpdateItem call, with an ADD that atomically accumulates the numeric totals and returns the
   * new running values (mirroring AbstractAtomicCounter.increment()'s pattern).
   *
   * @returns the new, post-add chunkCount/totalRecords - not necessarily the run's true final
   * total (other tasks may still be contributing), just this call's up-to-date view.
   */
  public async addToMetadataTotals(syncRunId: string, params: {
    source: string;
    target?: string;
    chunkDirectory: string;
    itemsPerChunk: number;
    bulkReset: boolean;
    trustPreviousStorage: boolean;
    syncPopulation: string;
    deltaStoragePath: string;
    chunkCountDelta: number;
    totalRecordsDelta: number;
    /** SET only when true - a full/non-partial task's call omits this, never explicitly clearing it back to false. */
    partialOrEmptyChunkEncountered?: boolean;
  }): Promise<{ chunkCount: number; totalRecords: number }> {
    const {
      source, target, chunkDirectory, itemsPerChunk, bulkReset, trustPreviousStorage,
      syncPopulation, deltaStoragePath, chunkCountDelta, totalRecordsDelta, partialOrEmptyChunkEncountered
    } = params;
    const { tableName, region } = this;

    const setClauses = [
      '#source = if_not_exists(#source, :source)',
      '#chunkDirectory = if_not_exists(#chunkDirectory, :chunkDirectory)',
      '#itemsPerChunk = if_not_exists(#itemsPerChunk, :itemsPerChunk)',
      '#bulkReset = if_not_exists(#bulkReset, :bulkReset)',
      '#trustPreviousStorage = if_not_exists(#trustPreviousStorage, :trustPreviousStorage)',
      '#syncPopulation = if_not_exists(#syncPopulation, :syncPopulation)',
      '#deltaStoragePath = if_not_exists(#deltaStoragePath, :deltaStoragePath)',
      '#createdAt = if_not_exists(#createdAt, :createdAt)'
    ];
    const names: Record<string, string> = {
      '#source': 'source',
      '#chunkDirectory': 'chunkDirectory',
      '#itemsPerChunk': 'itemsPerChunk',
      '#bulkReset': 'bulkReset',
      '#trustPreviousStorage': 'trustPreviousStorage',
      '#syncPopulation': 'syncPopulation',
      '#deltaStoragePath': 'deltaStoragePath',
      '#createdAt': 'createdAt',
      '#chunkCount': 'chunkCount',
      '#totalRecords': 'totalRecords'
    };
    const values: Record<string, any> = {
      ':source': source,
      ':chunkDirectory': chunkDirectory,
      ':itemsPerChunk': itemsPerChunk,
      ':bulkReset': bulkReset,
      ':trustPreviousStorage': trustPreviousStorage,
      ':syncPopulation': syncPopulation,
      ':deltaStoragePath': deltaStoragePath,
      ':createdAt': new Date().toISOString(),
      ':chunkCountDelta': chunkCountDelta,
      ':totalRecordsDelta': totalRecordsDelta
    };

    if (target !== undefined) {
      setClauses.push('#target = if_not_exists(#target, :target)');
      names['#target'] = 'target';
      values[':target'] = target;
    }

    if (partialOrEmptyChunkEncountered) {
      setClauses.push('#partialOrEmptyChunkEncountered = :true');
      names['#partialOrEmptyChunkEncountered'] = 'partialOrEmptyChunkEncountered';
      values[':true'] = true;
    }

    const client = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
    const input = {
      TableName: tableName,
      Key: { [DYNAMODB_PARTITION_KEY]: syncRunId, [DYNAMODB_SORT_KEY]: 'METADATA' },
      UpdateExpression: `SET ${setClauses.join(', ')} ADD #chunkCount :chunkCountDelta, #totalRecords :totalRecordsDelta`,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
      ReturnValues: 'UPDATED_NEW'
    } satisfies UpdateCommandInput;

    const result = await client.send(new UpdateCommand(input));
    return {
      chunkCount: result.Attributes?.chunkCount ?? 0,
      totalRecords: result.Attributes?.totalRecords ?? 0
    };
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
   * Attempt to claim the PROCESSOR_BOOST_CLAIM record for this sync run (ProcessorServiceBooster).
   * Uses OCCFlag (optimistic concurrency control) so exactly one concurrent caller wins - see
   * OCCFlag's doc comment for why this is not the same thing as atomicity. If the existing claim
   * is older than PROCESSOR_BOOST_CLAIM_STALE_AFTER_MS, its winner likely crashed/was killed
   * before releasing it, so it's cleared and retried rather than permanently blocking boosting.
   *
   * @param claimedByChunk Identifies which parallel chunker task/offset won the claim, so its log
   * stream can be found later
   * @returns true if this call won the claim, false if another task already holds it
   */
  public async claimProcessorBoost(syncRunId: string, claimedByChunk?: string): Promise<boolean> {
    const { tableName, region } = this;
    const claimFlag = new OCCFlag({
      tableName, region,
      partitionKeyName: DYNAMODB_PARTITION_KEY, partitionKeyValue: syncRunId,
      sortKeyName: DYNAMODB_SORT_KEY, sortKeyValue: 'PROCESSOR_BOOST_CLAIM',
      attributeName: 'claimedAt'
    });

    const attempt = async (claimedAt: string): Promise<boolean> => {
      let won = false;
      await claimFlag.update(claimedAt,
        async () => { won = true; },
        async () => { won = false; }
      );
      return won;
    };

    const claimedAt = new Date().toISOString();
    let won = await attempt(claimedAt);

    if (!won) {
      const existingClaimedAt = await claimFlag.getValue();
      const ageMs = existingClaimedAt ? Date.now() - new Date(existingClaimedAt).getTime() : 0;
      if (ageMs > StatisticsTable.PROCESSOR_BOOST_CLAIM_STALE_AFTER_MS) {
        console.warn(`⚠️  Existing processor boost claim is stale (${Math.round(ageMs / 1000)}s old) - clearing and retrying.`);
        await claimFlag.unset();
        won = await attempt(claimedAt);
      }
    }

    if (won && claimedByChunk !== undefined) {
      await this.table.putItem({
        [DYNAMODB_PARTITION_KEY]: syncRunId,
        [DYNAMODB_SORT_KEY]: 'PROCESSOR_BOOST_CLAIM',
        claimedAt,
        claimedByChunk
      });
    }

    return won;
  }

  /**
   * Release a previously-won PROCESSOR_BOOST_CLAIM record, allowing another task to win it later.
   */
  public async releaseProcessorBoostClaim(syncRunId: string): Promise<void> {
    await this.table.batchWrite({
      items: [{ [DYNAMODB_PARTITION_KEY]: syncRunId, [DYNAMODB_SORT_KEY]: 'PROCESSOR_BOOST_CLAIM' }],
      operation: 'delete'
    });
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

export async function main() {
  const testEnvironment = TestEnvironment('STATISTICS_TABLE');
  [
    'STATISTICS_TABLE_TASK',
    'STATISTICS_TABLE_INTEGRATION_TIMESTAMP',
    'STATISTICS_TABLE_NAME_OVERRIDE',
    'TRUNCATE_CHUNK_SIZE'
  ].forEach(testEnvironment.getVar);

  const { 
    STATISTICS_TABLE_TASK: task, STATISTICS_TABLE_INTEGRATION_TIMESTAMP: timestamp,
    STATISTICS_TABLE_NAME_OVERRIDE: tableNameOverride,
    TRUNCATE_CHUNK_SIZE
  } = process.env;

  (async () => {
    const context = require('../../context/context.json') as IContext;
    // Target a specific table (e.g. a mock variant) directly when overridden, bypassing IContext-based name resolution
    const statisticsTable = tableNameOverride
      ? StatisticsTable.fromTableName(tableNameOverride, context.REGION)
      : new StatisticsTable(context);
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

if(require.main === module) {
  main();
}
