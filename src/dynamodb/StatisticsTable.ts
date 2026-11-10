import { TestEnvironment } from 'integration-core';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand, UpdateCommandInput } from '@aws-sdk/lib-dynamodb';
import { IContext } from '../../context/IContext';
import { StatisticsItem } from '../ApiErrorTracking';
import { AbstractDynamoDbTable, DynamoDBTable, QueryPage } from './DynamoDBTable';
import { OCCFlag } from './OCCFlag';

export const DYNAMODB_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-statistics-${context.TAGS.Landscape.toLowerCase()}`;
export const DYNAMODB_PARTITION_KEY = 'integrationTimestamp';
export const DYNAMODB_SECONDARY_PARTITION_KEY = 'errorType';
export const DYNAMODB_SORT_KEY = 'eventType';
export const DYNAMODB_GSI_INDEX_NAME = 'errorType-timestamp-index';
// Sparse GSI holding exactly one item per run: the RUN marker item, whose distinctRunId copies the table PK
export const DYNAMODB_RUN_INDEX_NAME = 'distinct-run-index';
export const DYNAMODB_RUN_INDEX_PARTITION_KEY = 'distinctRunId';
// Sparse GSI holding the same RUN marker items in one partition, sorted by syncRunId, so the newest runs can be
// read by key: partition key runList (always RUN_LIST_PARTITION), sort key distinctRunId
export const DYNAMODB_RUN_LIST_INDEX_NAME = 'run-list-index';
export const DYNAMODB_RUN_LIST_PARTITION_KEY = 'runList';
export const RUN_LIST_PARTITION = 'RUNS';
export const RUN_MARKER_SORT_KEY = 'RUN';

/** A page of syncRunIds, newest first, and whether older runs remain */
export type RunIdPage = { syncRunIds: string[], hasOlder: boolean };
export const RUN_SUMMARY_SORT_KEY = 'RUN_SUMMARY';

export type DeletionSkipReason = 'TERMINAL_ERROR' | 'PERSON_DELTA' | 'DELETES_DISABLED' | 'NO_MERGE_OUTPUT' | 'MISSING_TABLES';

/** What the post-processor's deferred-deletes step did for a run. */
export type DeletionOutcome =
  | { status: 'COMPLETED'; deletedCount: number; failedCount: number; totalProcessed: number; message: string }
  | { status: 'SKIPPED'; reason: DeletionSkipReason; message: string }
  | { status: 'FAILED'; message: string };

export type RunSummaryStatus = 'SUCCEEDED' | 'FAILED' | 'POST_PROCESSING_FAILED';

/** A run's record counts, summed over its chunks' IntegrationResult counts */
export type RunResults = {
  processed: number;
  succeeded: number;
  failed: number;
  skipped: number;
  added: number;
  updated: number;
  removed: number;
};

/**
 * The totals of the STATISTICS-chunk items a run's processors have written. The post-processor's RUN_SUMMARY is
 * built from them, and a run without a summary yet (still running, or stopped) reports them as its progress.
 */
export type ChunkTotals = {
  /** Chunks that wrote a STATISTICS-chunk item */
  chunksReported: number;
  results: RunResults;
  totalErrors: number;
  throttleCount: number;
  errorsByStatus: Record<string, number>;
  /** The latest chunk endTimestamp; absent when no chunk has reported */
  lastChunkAt?: string;
};

/**
 * Sum a run's STATISTICS-chunk items. The one place a run's figures are added up, so the figures shown while it
 * runs and the summary the post-processor writes can't disagree.
 */
export const sumChunkStatistics = (chunks: StatisticsItem[]): ChunkTotals => {
  const results: RunResults = { processed: 0, succeeded: 0, failed: 0, skipped: 0, added: 0, updated: 0, removed: 0 };
  const errorsByStatus: Record<string, number> = {};
  let totalErrors = 0;
  let throttleCount = 0;
  let lastChunkAt: string | undefined;
  for (const chunk of chunks) {
    results.processed += chunk.totalRecords ?? 0;
    results.succeeded += chunk.successCount ?? 0;
    results.failed += chunk.failureCount ?? 0;
    results.skipped += chunk.skippedCount ?? 0;
    results.added += chunk.addedCount ?? 0;
    results.updated += chunk.updatedCount ?? 0;
    results.removed += chunk.removedCount ?? 0;
    totalErrors += chunk.totalErrors ?? 0;
    throttleCount += chunk.throttleCount ?? 0;
    for (const [status, count] of Object.entries(chunk.errorsByStatus ?? {})) {
      errorsByStatus[status] = (errorsByStatus[status] ?? 0) + Number(count);
    }
    // ISO timestamps order as strings
    if (chunk.endTimestamp && (!lastChunkAt || chunk.endTimestamp > lastChunkAt)) {
      lastChunkAt = chunk.endTimestamp;
    }
  }
  return {
    chunksReported: chunks.length,
    results,
    totalErrors,
    throttleCount,
    errorsByStatus,
    ...(lastChunkAt ? { lastChunkAt } : {})
  };
};

/** Per-run totals written once by the post-processor (SK = RUN_SUMMARY). */
export type RunSummary = {
  syncRunId: string;
  status: RunSummaryStatus;
  storageMode?: string;
  dryRun: boolean;
  createdAt?: string;
  completedAt: string;
  syncPopulation?: string;
  bulkReset?: boolean;
  /** Chunks written and records fetched by the chunker (from METADATA) */
  chunkCount?: number;
  totalRecords?: number;
  /** Chunks that wrote a STATISTICS-chunk item; fewer than chunkCount if processor tasks died */
  chunksReported: number;
  results: RunResults;
  totalErrors: number;
  throttleCount: number;
  errorsByStatus: Record<string, number>;
  deletion?: DeletionOutcome;
};

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
   * paths (chunker/processor/post-processor Docker entry points) where a full IContext isn't available.
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
   * Assemble a run's summary from its METADATA record and STATISTICS-chunk items. A deletion step
   * skipped because the run was marked failed makes the run FAILED, even if post-processing succeeded.
   */
  public buildRunSummary = async (syncRunId: string, params: {
    postProcessingSucceeded: boolean;
    storageMode?: string;
    dryRun: boolean;
    deletion?: DeletionOutcome;
  }): Promise<RunSummary> => {
    const { postProcessingSucceeded, storageMode, dryRun, deletion } = params;
    const metadata = await this.readMetadata(syncRunId) ?? {};
    // The summary keeps its own completedAt; the latest chunk time is only reported for runs without one
    const { lastChunkAt: _, ...totals } = sumChunkStatistics(await this.getAllChunkStatistics(syncRunId));

    let status: RunSummaryStatus = 'SUCCEEDED';
    if (!postProcessingSucceeded) {
      status = 'POST_PROCESSING_FAILED';
    } else if (deletion?.status === 'SKIPPED' && deletion.reason === 'TERMINAL_ERROR') {
      status = 'FAILED';
    }

    return {
      syncRunId,
      status,
      storageMode,
      dryRun,
      createdAt: metadata.createdAt,
      completedAt: new Date().toISOString(),
      syncPopulation: metadata.syncPopulation,
      bulkReset: metadata.bulkReset,
      chunkCount: metadata.chunkCount,
      totalRecords: metadata.totalRecords,
      ...totals,
      deletion
    };
  }

  public async writeRunSummary(summary: RunSummary): Promise<void> {
    await this.table.putItem({
      ...summary,
      [DYNAMODB_PARTITION_KEY]: summary.syncRunId,
      [DYNAMODB_SORT_KEY]: RUN_SUMMARY_SORT_KEY
    });
  }

  /**
   * @returns the run's summary, or undefined if the post-processor never wrote one (run failed
   * before post-processing, is still in progress, or predates run summaries)
   */
  public async readRunSummary(syncRunId: string): Promise<RunSummary | undefined> {
    const item = await this.table.getItem({ partitionKeyValue: syncRunId, sortKeyValue: RUN_SUMMARY_SORT_KEY });
    if (!item) return undefined;
    const { [DYNAMODB_PARTITION_KEY]: _, [DYNAMODB_SORT_KEY]: __, ...summary } = item;
    return summary as RunSummary;
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
   * One page of a run's ERROR items (SK "ERROR:<statusCode>:<timestamp>"), chronological within a
   * status code, optionally limited to one status code.
   */
  public getRunErrorsPage = async (syncRunId: string, page: {
    statusCode?: number; limit: number; startKey?: Record<string, any>
  }): Promise<QueryPage> => {
    const { statusCode, limit, startKey } = page;
    return this.table.queryPage({
      partitionKeyValue: syncRunId,
      sortKeyPrefix: statusCode === undefined ? 'ERROR:' : `ERROR:${statusCode}:`,
      limit,
      exclusiveStartKey: startKey
    });
  }

  /**
   * Get the distinct integration timestamps (syncRunIds) in the table - the equivalent of
   * "SELECT DISTINCT integrationTimestamp". Scans the sparse, KEYS_ONLY distinct-run-index, which
   * holds exactly one small item per run (see registerRun()), so the cost is proportional to the
   * number of runs rather than the number of chunks/errors they produced. Prefer the keyed reads of the
   * run-list-index (getRunIdsPage(), getAllRunIds()); this remains for tables that don't have it yet.
   *
   * @returns An array of unique integration timestamps, sorted chronologically (ascending)
   */
  public getUniqueIntegrationTimestamps = async (): Promise<string[]> => {
    const items = await this.table.scanIndex({ indexName: DYNAMODB_RUN_INDEX_NAME });
    // ISO timestamps sort chronologically as strings; a scan returns them unordered
    return items.map(item => item[DYNAMODB_PARTITION_KEY] as string).sort();
  }

  /**
   * A page of syncRunIds from the run-list-index, newest first: up to `limit` runs older than `before` (a
   * syncRunId) when given. One Query of `limit + 1` keys-only items; the extra one says whether older runs
   * remain. syncRunIds are ISO timestamps, so the index's sort order is time order. Throws the Query's
   * ValidationException if the table has no run-list-index (see isMissingIndexError()).
   */
  public getRunIdsPage = async (params: { limit: number, before?: string }): Promise<RunIdPage> => {
    const { limit, before } = params;
    const { items } = await this.table.queryGSIPage({
      indexName: DYNAMODB_RUN_LIST_INDEX_NAME,
      gsiPartitionKey: DYNAMODB_RUN_LIST_PARTITION_KEY,
      partitionKeyValue: RUN_LIST_PARTITION,
      ...(before !== undefined && { gsiSortKey: DYNAMODB_RUN_INDEX_PARTITION_KEY, sortKeyValue: before, sortKeyComparison: '<' }),
      scanIndexForward: false,
      limit: limit + 1
    });
    const syncRunIds = items.map(item => item[DYNAMODB_PARTITION_KEY] as string);
    return { syncRunIds: syncRunIds.slice(0, limit), hasOlder: syncRunIds.length > limit };
  }

  /**
   * Every syncRunId in the run-list-index, oldest first, reading all its pages. Throws as getRunIdsPage()
   * does when the table has no run-list-index.
   */
  public getAllRunIds = async (): Promise<string[]> => {
    const syncRunIds: string[] = [];
    let exclusiveStartKey: Record<string, any> | undefined;
    do {
      const page = await this.table.queryGSIPage({
        indexName: DYNAMODB_RUN_LIST_INDEX_NAME,
        gsiPartitionKey: DYNAMODB_RUN_LIST_PARTITION_KEY,
        partitionKeyValue: RUN_LIST_PARTITION,
        limit: 1000,
        exclusiveStartKey
      });
      syncRunIds.push(...page.items.map(item => item[DYNAMODB_PARTITION_KEY] as string));
      exclusiveStartKey = page.lastEvaluatedKey;
    } while (exclusiveStartKey);
    return syncRunIds;
  }

  /** The RUN marker item for a run: in both run indexes (distinct-run-index and run-list-index) */
  private static runMarker(syncRunId: string): Record<string, string> {
    return {
      [DYNAMODB_PARTITION_KEY]: syncRunId,
      [DYNAMODB_SORT_KEY]: RUN_MARKER_SORT_KEY,
      [DYNAMODB_RUN_INDEX_PARTITION_KEY]: syncRunId,
      [DYNAMODB_RUN_LIST_PARTITION_KEY]: RUN_LIST_PARTITION
    };
  }

  /**
   * Write the run's RUN marker item, the only item carrying the run indexes' keys. Idempotent,
   * so every write path that can be a run's first item in this table calls it.
   */
  public async registerRun(syncRunId: string): Promise<void> {
    await this.table.putItem(StatisticsTable.runMarker(syncRunId));
  }

  /**
   * One-time backfill of RUN marker items: for runs written before the run indexes existed, and to add the
   * run-list-index key (runList) to markers written before it. Scans the whole base table once.
   *
   * @returns The number of runs registered
   */
  public async backfillRunMarkers(): Promise<number> {
    const items = await this.table.scanAll();
    const syncRunIds = Array.from(new Set(items.map(item => item[DYNAMODB_PARTITION_KEY] as string)));
    await this.table.batchWrite({
      items: syncRunIds.map(syncRunId => StatisticsTable.runMarker(syncRunId)),
      operation: 'put'
    });
    return syncRunIds.length;
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
    await this.registerRun(syncRunId);
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
   * Write the TERMINAL_ERROR record (PK=syncRunId, SK="TERMINAL_ERROR") marking the run as failed.
   */
  public async writeTerminalError(syncRunId: string, marker: Record<string, any>): Promise<void> {
    await this.table.putItem({
      ...marker,
      [DYNAMODB_PARTITION_KEY]: syncRunId,
      [DYNAMODB_SORT_KEY]: 'TERMINAL_ERROR'
    });
    await this.registerRun(syncRunId);
  }

  /**
   * Read the TERMINAL_ERROR record for a sync run.
   * 
   * @returns the marker fields if the run was marked failed, undefined otherwise
   */
  public async readTerminalError(syncRunId: string): Promise<Record<string, any> | undefined> {
    const item = await this.table.getItem({ 
      partitionKeyValue: syncRunId, sortKeyValue: 'TERMINAL_ERROR' 
    });
    if (!item) return undefined;

    const { [DYNAMODB_PARTITION_KEY]: _, [DYNAMODB_SORT_KEY]: __, ...marker } = item;
    return marker;
  }

  /**
   * Update METADATA record with additional fields.
   * Merges new fields into existing METADATA record without overwriting other fields.
   * 
   * This is useful for marking completion milestones such as:
   * - postProcessorTriggered: true
   * - postProcessorTriggeredAt: ISO timestamp
   * - postProcessorTriggeredBy: chunkId
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
    await this.registerRun(syncRunId);
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
   * Attempt to claim the exclusive right to trigger the post-processor for this sync run. Uses OCCFlag
   * (first-writer-wins) so exactly one processor task ever sends the post-processor-trigger SQS message,
   * regardless of how many processor tasks independently observe the completion condition as
   * true. Unlike claimProcessorBoost, there is no staleness/retry recovery here - once the post-processor
   * has been triggered, it must never be triggered again, so an unclaimed-looking record should
   * never be treated as abandoned and cleared.
   *
   * @param claimedByChunk Identifies which processor task/chunk won the claim, so its log stream
   * can be found later
   * @returns true if this call won the claim, false if another processor already triggered
   */
  public async claimPostProcessorTrigger(syncRunId: string, claimedByChunk?: string): Promise<boolean> {
    const { tableName, region } = this;
    const claimFlag = new OCCFlag({
      tableName, region,
      partitionKeyName: DYNAMODB_PARTITION_KEY, partitionKeyValue: syncRunId,
      sortKeyName: DYNAMODB_SORT_KEY, sortKeyValue: 'POST_PROCESSOR_TRIGGER_CLAIM',
      attributeName: 'claimedAt'
    });

    let won = false;
    const claimedAt = new Date().toISOString();
    await claimFlag.update(claimedAt,
      async () => { won = true; },
      async () => { won = false; }
    );

    if (won && claimedByChunk !== undefined) {
      await this.table.putItem({
        [DYNAMODB_PARTITION_KEY]: syncRunId,
        [DYNAMODB_SORT_KEY]: 'POST_PROCESSOR_TRIGGER_CLAIM',
        claimedAt,
        claimedByChunk
      });
    }

    return won;
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
   * Removes all records (STATISTICS, FLAGS, METADATA, TERMINAL_ERROR, CHUNK_STATUS, ERROR,
   * PROCESSOR_BOOST_CLAIM, POST_PROCESSOR_TRIGGER_CLAIM records)
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
        const summary = await statisticsTable.readRunSummary(timestamp);
        if(summary) {
          console.log(`Run summary for integration run at ${timestamp}:`, JSON.stringify(summary, null, 2));
        } else {
          console.log(`No run summary found for integration run at ${timestamp}`);
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
      case 'backfill-runs':
        const registered = await statisticsTable.backfillRunMarkers();
        console.log(`Registered ${registered} run(s) in ${DYNAMODB_RUN_INDEX_NAME} and ${DYNAMODB_RUN_LIST_INDEX_NAME}`);
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
        console.error(`Unknown task: ${task}. Supported tasks: truncate, statistics, list, chunks, backfill-runs, delete`);
    }
  })();
}

if(require.main === module) {
  main();
}
