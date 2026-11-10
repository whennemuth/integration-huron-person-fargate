import { Config } from 'integration-huron-person';
import { MetadataForS3 } from '../chunking/metadata/MetadataForS3';
import { Flags, TerminalError } from '../chunking/metadata/IMetadataStorage';
import { PersonHistoryRecord, PersonHistoryTable } from '../dynamodb/PersonHistoryTable';
import { ChunkTotals, RunIdPage, RunSummary, RunSummaryStatus, StatisticsTable, sumChunkStatistics } from '../dynamodb/StatisticsTable';
import { StatisticsItem } from '../ApiErrorTracking';

export const DEFAULT_PAGE_LIMIT = 100;
export const MAX_PAGE_LIMIT = 500;

export type ChangeType = 'NEW' | 'UPDATED' | 'DELETED';

export type Page<T> = { items: T[]; cursor?: string };

/** RUN_LIST_UNAVAILABLE is the landscape's state, not the request's fault (a 503); the others are the request's (a 400) */
export type ReportingErrorCode = 'INVALID_CURSOR' | 'INVALID_LIMIT' | 'UNSUPPORTED_IN_STORAGE_MODE' | 'RUN_LIST_UNAVAILABLE';

export class ReportingError extends Error {
  constructor(public readonly code: ReportingErrorCode, message: string) {
    super(message);
    this.name = 'ReportingError';
  }
}

/**
 * What a run without a summary has done so far: its chunks' totals, summed as the post-processor sums them for
 * RUN_SUMMARY (sumChunkStatistics). Not final: chunks still running are missing, and a full sync's deferred
 * deletes are only counted by post-processing.
 */
export type RunProgress = ChunkTotals;

/**
 * A run's report: the post-processor's RUN_SUMMARY when it exists, otherwise what can be read from
 * the run's METADATA, FLAGS and terminal error - FAILED if the run was marked failed, else INCOMPLETE
 * (still running, or stopped without being marked failed) - with its progress so far.
 */
export type RunReport = {
  syncRunId: string;
  status: RunSummaryStatus | 'INCOMPLETE';
  summary?: RunSummary;
  metadata?: Record<string, any>;
  flags?: Partial<Flags>;
  terminalError?: TerminalError | Record<string, any>;
  progress?: RunProgress;
};

export type { RunIdPage };

/**
 * The page of a full run list (syncRunIds, oldest first) that a keyed read of the run-list-index returns: up
 * to `limit` runs older than `before`, newest first. For the mocks, which keep their runs in a list.
 */
export const pageOfRunIds = (syncRunIds: string[], params: { limit: number, before?: string }): RunIdPage => {
  const { limit, before } = params;
  const older = syncRunIds.filter(id => before === undefined || id < before);
  return { syncRunIds: older.slice(Math.max(older.length - limit, 0)).reverse(), hasOlder: older.length > limit };
};

/**
 * Whether DynamoDB refused a Query because the index can't be read yet: the table has no such index, or it is
 * still being built ("backfilling"), which it is for some minutes after the stack update that adds it, even
 * once CloudFormation reports the update complete.
 */
export const isMissingIndexError = (error: unknown): boolean =>
  error instanceof Error && error.name === 'ValidationException'
  && /does not have the specified index|cannot read from backfilling global secondary index/i.test(error.message);

/**
 * The run list's error when the statistics table can't serve its run-list-index (isMissingIndexError()): the
 * index is still being built, so try again shortly; or it doesn't exist yet, so the landscape's pipeline stack
 * needs deploying and its run markers backfilling.
 */
export const runListUnavailable = (tableName: string, cause: Error): ReportingError =>
  /backfilling/i.test(cause.message)
    ? new ReportingError('RUN_LIST_UNAVAILABLE',
      `The run list's index on ${tableName} is still being built, which takes some minutes after the pipeline stack update that adds it. Try again shortly.`)
    : new ReportingError('RUN_LIST_UNAVAILABLE',
      `${tableName} has no run-list-index yet: deploy this landscape's pipeline stack, then run the backfill-runs task of the STATISTICS_TABLE harness.`);

export type Capabilities = {
  landscape: string;
  storageMode: string;
  changesSupported: boolean;
};

export type RunReportingParams = {
  landscape: string;
  storageMode: string;
  statisticsTableName: string;
  personHistoryTableName?: string;
  chunksBucket?: string;
  region?: string;
};

/**
 * Read-only reporting over one landscape's sync runs. Lists runs, then reports on one run as deeply
 * as the storage mode allows: per-person change lists exist only in DynamoDB mode.
 */
export class RunReporting {
  private statisticsTable: StatisticsTable;
  private personHistoryTable?: PersonHistoryTable;

  constructor(private params: RunReportingParams, deps?: {
    statisticsTable?: StatisticsTable;
    personHistoryTable?: PersonHistoryTable;
  }) {
    const { statisticsTableName, personHistoryTableName, region } = params;
    this.statisticsTable = deps?.statisticsTable ?? StatisticsTable.fromTableName(statisticsTableName, region);
    this.personHistoryTable = deps?.personHistoryTable
      ?? (personHistoryTableName ? PersonHistoryTable.fromTableName(personHistoryTableName, region) : undefined);
  }

  private get isDynamoDbMode(): boolean {
    return this.params.storageMode.toLowerCase() === 'dynamodb';
  }

  public getCapabilities = (): Capabilities => {
    const { landscape, storageMode } = this.params;
    return { landscape, storageMode, changesSupported: this.isDynamoDbMode && !!this.personHistoryTable };
  }

  /** @returns syncRunIds, oldest first: every page of the run-list-index */
  public listRuns = async (): Promise<string[]> => {
    return this.readRunList(() => this.statisticsTable.getAllRunIds());
  }

  /**
   * A page of syncRunIds, newest first: up to `limit` runs older than `before` (a syncRunId) when given, and
   * whether older runs remain. One Query of the run-list-index, so Overview's latest run reads one key.
   */
  public listRunPage = async (params: { limit: number, before?: string }): Promise<RunIdPage> => {
    return this.readRunList(() => this.statisticsTable.getRunIdsPage(params));
  }

  /**
   * A read of the run-list-index. When the table can't serve it (the index not added yet, or still being built),
   * it fails with a ReportingError (RUN_LIST_UNAVAILABLE) that says why and what to do, rather than DynamoDB's
   * ValidationException. There is no other index to fall back to.
   */
  private async readRunList<T>(read: () => Promise<T>): Promise<T> {
    try {
      return await read();
    } catch (error) {
      if (isMissingIndexError(error)) {
        throw runListUnavailable(this.params.statisticsTableName, error as Error);
      }
      throw error;
    }
  }

  /**
   * A run without a summary also gets its progress: one Query of its STATISTICS-chunk items (partition key
   * plus sort-key prefix), made alongside the flags and terminal error reads. A summarized run costs no more
   * than its RUN_SUMMARY read.
   * @returns undefined if nothing is recorded for the run
   */
  public getRunReport = async (syncRunId: string): Promise<RunReport | undefined> => {
    const summary = await this.statisticsTable.readRunSummary(syncRunId);
    if (summary) {
      return { syncRunId, status: summary.status, summary };
    }

    const metadata = await this.statisticsTable.readMetadata(syncRunId);
    const [{ flags, terminalError }, chunks] = await Promise.all([
      this.readFlagsAndTerminalError(syncRunId, metadata?.chunkDirectory),
      this.statisticsTable.getAllChunkStatistics(syncRunId)
    ]);
    if (!metadata && !flags && !terminalError && chunks.length === 0) {
      return undefined;
    }
    return {
      syncRunId, status: terminalError ? 'FAILED' : 'INCOMPLETE', metadata, flags, terminalError,
      progress: sumChunkStatistics(chunks)
    };
  }

  public getRunChunkStatistics = async (syncRunId: string): Promise<StatisticsItem[]> => {
    return this.statisticsTable.getAllChunkStatistics(syncRunId);
  }

  public getRunErrors = async (syncRunId: string, page: {
    statusCode?: number; limit?: number; cursor?: string
  } = {}): Promise<Page<Record<string, any>>> => {
    const { statusCode, cursor } = page;
    const startKey = cursor ? decodeCursor(cursor, { integrationTimestamp: syncRunId }) : undefined;
    const { items, lastEvaluatedKey } = await this.statisticsTable.getRunErrorsPage(syncRunId, {
      statusCode, limit: resolveLimit(page.limit), startKey
    });
    return { items, cursor: encodeCursor(lastEvaluatedKey) };
  }

  public getRunChanges = async (syncRunId: string, changeType: ChangeType, page: {
    limit?: number; cursor?: string
  } = {}): Promise<Page<PersonHistoryRecord>> => {
    if (!this.isDynamoDbMode || !this.personHistoryTable) {
      throw new ReportingError('UNSUPPORTED_IN_STORAGE_MODE',
        `Per-person change lists are not recorded in ${this.params.storageMode} storage mode`);
    }
    const startKey = page.cursor ? decodeCursor(page.cursor, { syncRunId, changeType }) : undefined;
    const { items, lastEvaluatedKey } = await this.personHistoryTable.getChangesInSyncRunPage(syncRunId, changeType, {
      limit: resolveLimit(page.limit), startKey
    });
    return { items: items as PersonHistoryRecord[], cursor: encodeCursor(lastEvaluatedKey) };
  }

  /**
   * FLAGS and the terminal error live in the statistics table in DynamoDB mode, but in S3 files
   * (under the run's chunk directory) in S3 mode.
   */
  private readFlagsAndTerminalError = async (syncRunId: string, chunkDirectory?: string): Promise<{
    flags?: Partial<Flags>; terminalError?: TerminalError | Record<string, any>
  }> => {
    if (this.isDynamoDbMode) {
      return {
        flags: await this.statisticsTable.readFlags(syncRunId),
        terminalError: await this.statisticsTable.readTerminalError(syncRunId)
      };
    }
    const { chunksBucket: bucketName, statisticsTableName, region } = this.params;
    if (!chunkDirectory || !bucketName) {
      return {};
    }
    const metadataStorage = new MetadataForS3({ config: {} as Config, statisticsTableName });
    const flags = await metadataStorage.readFlags({ bucketName, chunkDirectory, region });
    return {
      flags: Object.keys(flags).length > 0 ? flags : undefined,
      terminalError: await metadataStorage.readTerminalError({ bucketName, chunkDirectory, region })
    };
  }
}

const resolveLimit = (limit?: number): number => {
  if (limit === undefined) return DEFAULT_PAGE_LIMIT;
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_PAGE_LIMIT) {
    throw new ReportingError('INVALID_LIMIT', `limit must be an integer from 1 to ${MAX_PAGE_LIMIT}`);
  }
  return limit;
}

export const encodeCursor = (key?: Record<string, any>): string | undefined => {
  return key ? Buffer.from(JSON.stringify(key)).toString('base64url') : undefined;
}

/**
 * Decode a cursor, rejecting one whose key attributes don't match the requested query, so a
 * cursor can only continue the listing it came from.
 */
export const decodeCursor = (cursor: string, expected: Record<string, string>): Record<string, any> => {
  let key: any;
  try {
    key = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    throw new ReportingError('INVALID_CURSOR', 'cursor is not valid');
  }
  if (!key || typeof key !== 'object' || Array.isArray(key)) {
    throw new ReportingError('INVALID_CURSOR', 'cursor is not valid');
  }
  for (const [name, value] of Object.entries(expected)) {
    if (key[name] !== value) {
      throw new ReportingError('INVALID_CURSOR', 'cursor does not belong to this query');
    }
  }
  return key;
}
