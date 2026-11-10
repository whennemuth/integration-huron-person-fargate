/**
 * Mock run reporting for local development (LOCALHOST_WITH_MOCKS=true): a deterministic history of about
 * forty sync runs over six weeks, rich enough to show every state the run reporting screens handle.
 *
 * - MockRunCatalog builds the runs and everything recorded for them: reports, chunk statistics, errors and
 *   person changes, kept consistent with one another (chunk statistics sum to the summary, or to an unfinished
 *   run's progress, through sumChunkStatistics() as a real landscape's do; error items to its errorsByStatus,
 *   change items to its added/updated/removed counts).
 * - MockRunReportingService serves the catalog through the same RunReportingService interface as the real
 *   RunReporting, with the same page limits and ReportingError codes, so the screens' paging and error
 *   handling are exercised as they are against a real landscape.
 *
 * MOCK_STORAGE_MODE (dynamodb, the default, or s3) selects the storage mode the mocks report. In S3 mode no
 * per-person change list is kept, so getRunChanges() refuses, as RunReporting does.
 */
import {
  Capabilities, ChangeType, DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, Page, pageOfRunIds, ReportingError, RunIdPage, RunReport
} from '../../../src/reporting/RunReporting';
import { DeletionOutcome, RunSummary, RunSummaryStatus, sumChunkStatistics } from '../../../src/dynamodb/StatisticsTable';
import { StatisticsItem } from '../../../src/ApiErrorTracking';
import { PersonHistoryRecord } from '../../../src/dynamodb/PersonHistoryTable';
import { RunReportingService } from '../services/ServiceTypes';
import { LOCAL_LANDSCAPE } from '../services/LandscapeRegistry';

export type MockStorageMode = 'dynamodb' | 's3';
export const MOCK_STORAGE_MODES: MockStorageMode[] = ['dynamodb', 's3'];

/** The newest mock run: the nightly run of 2026-11-04 */
export const NEWEST_MOCK_RUN = '2026-11-04T02:00:00.000Z';
export const MOCK_RUN_COUNT = 40;

const MINUTE = 60 * 1000;
const DAY = 24 * 60 * MINUTE;
const FULL_POPULATION = 14_500;
const ITEMS_PER_CHUNK = 1000;
const ERROR_STATUS_CODES = [400, 404, 429, 500] as const;

/** How a mock run ended */
type Outcome = 'SUCCEEDED' | 'FAILED' | 'POST_PROCESSING_FAILED' | 'INCOMPLETE' | 'TERMINAL_ERROR';

/** One run, as the catalog defines it: everything else is derived from this */
type MockRunSpec = {
  syncRunId: string;
  outcome: Outcome;
  syncPopulation: 'person-full' | 'person-delta';
  /** The BUID of a single-person run (recorded in its source URL) */
  buid?: string;
  totalRecords: number;
  added: number;
  updated: number;
  /** People deactivated by the post-processor's deferred deletes */
  deactivated: number;
  errorsByStatus: Partial<Record<typeof ERROR_STATUS_CODES[number], number>>;
  dryRun: boolean;
  bulkReset: boolean;
  durationMinutes: number;
  deletion?: DeletionOutcome;
};

/** A small deterministic hash, so generated values look varied but never change between requests */
const hash = (...parts: (string | number)[]): number => {
  let h = 2166136261;
  for (const char of parts.join('|')) {
    h = Math.imul(h ^ char.charCodeAt(0), 16777619) >>> 0;
  }
  return h;
};

const hex = (...parts: (string | number)[]): string =>
  [0, 1, 2, 3].map(n => hash(...parts, n).toString(16).padStart(8, '0')).join('');

const buidFor = (...parts: (string | number)[]): string => `U${String(hash(...parts) % 100_000_000).padStart(8, '0')}`;

/**
 * The mock runs and what is recorded for each. Runs are numbered from the newest (0) back; the special
 * cases sit at fixed positions, and the rest are nightly runs: a full sync every seventh night, and a delta
 * sync of the changed people on the others.
 */
export class MockRunCatalog {
  private readonly specs: MockRunSpec[];
  private readonly byId: Map<string, MockRunSpec>;
  /** Generated lists, kept: a list is paged a slice at a time, and the largest holds ~14,000 changes */
  private readonly cache = new Map<string, any>();
  private static readonly shared = new Map<MockStorageMode, MockRunCatalog>();

  constructor(public readonly storageMode: MockStorageMode = 'dynamodb') {
    this.specs = Array.from({ length: MOCK_RUN_COUNT }, (_, n) => MockRunCatalog.spec(n));
    this.byId = new Map(this.specs.map(spec => [spec.syncRunId, spec]));
  }

  /** One catalog per storage mode, shared by every service instance (the provider makes one per request) */
  static for(storageMode: MockStorageMode): MockRunCatalog {
    let catalog = MockRunCatalog.shared.get(storageMode);
    if (!catalog) {
      catalog = new MockRunCatalog(storageMode);
      MockRunCatalog.shared.set(storageMode, catalog);
    }
    return catalog;
  }

  private cached<T>(key: string, build: () => T): T {
    if (!this.cache.has(key)) {
      this.cache.set(key, build());
    }
    return this.cache.get(key);
  }

  /** @returns the syncRunIds, oldest first (as RunReporting.listRuns() returns them) */
  syncRunIds(): string[] {
    return this.specs.map(spec => spec.syncRunId).reverse();
  }

  has(syncRunId: string): boolean {
    return this.byId.has(syncRunId);
  }

  report(syncRunId: string): RunReport | undefined {
    const spec = this.byId.get(syncRunId);
    if (!spec) {
      return undefined;
    }
    if (spec.outcome === 'INCOMPLETE' || spec.outcome === 'TERMINAL_ERROR') {
      // The post-processor never ran, so there's no summary: only what the chunker recorded
      const failed = spec.outcome === 'TERMINAL_ERROR';
      const chunkDirectory = `chunks/${spec.syncPopulation}/${syncRunId}`;
      return {
        syncRunId,
        status: failed ? 'FAILED' : 'INCOMPLETE',
        metadata: {
          createdAt: syncRunId,
          chunkDirectory,
          syncPopulation: spec.syncPopulation,
          bulkReset: spec.bulkReset,
          trustPreviousStorage: false,
          itemsPerChunk: ITEMS_PER_CHUNK,
          source: MockRunCatalog.source(spec),
          // Its chunker found the end of the source records (every chunk is written); only some are processed
          ...(failed ? {} : { chunkCount: MockRunCatalog.chunkCount(spec), totalRecords: spec.totalRecords, partialOrEmptyChunkEncountered: true })
        },
        flags: {
          bulkReset: spec.bulkReset,
          trustPreviousStorage: false,
          syncPopulation: spec.syncPopulation as any,
          ...(failed ? {
            runFailed: true,
            runFailureMessage: 'Source API returned 503 Service Unavailable after 5 attempts',
            runFailureTimestamp: new Date(Date.parse(syncRunId) + 4 * MINUTE).toISOString()
          } : {})
        },
        ...(failed ? {
          terminalError: {
            stage: 'chunking',
            chunkDirectory,
            errorMessage: 'Source API returned 503 Service Unavailable after 5 attempts',
            errorTimestamp: new Date(Date.parse(syncRunId) + 4 * MINUTE).toISOString()
          }
        } : {}),
        // What its processors reported before it stopped, summed as RunReporting sums it
        progress: sumChunkStatistics(this.chunkStatistics(syncRunId))
      };
    }
    const summary = this.summary(spec);
    return { syncRunId, status: summary.status, summary };
  }

  /** Per-chunk statistics, summing to the run's summary. An incomplete run has only some of its chunks */
  chunkStatistics(syncRunId: string): StatisticsItem[] {
    return this.cached(`chunks|${syncRunId}`, () => this.buildChunkStatistics(syncRunId));
  }

  private buildChunkStatistics(syncRunId: string): StatisticsItem[] {
    const spec = this.byId.get(syncRunId);
    if (!spec || spec.outcome === 'TERMINAL_ERROR') {
      return [];
    }
    const chunkCount = MockRunCatalog.chunkCount(spec);
    const reported = spec.outcome === 'INCOMPLETE' ? Math.ceil(chunkCount * 0.6) : chunkCount;
    const share = (total: number, chunk: number) => Math.floor(total / chunkCount) + (chunk < total % chunkCount ? 1 : 0);
    const start = Date.parse(syncRunId);
    return Array.from({ length: reported }, (_, chunk): StatisticsItem => {
      const chunkId = String(chunk).padStart(4, '0');
      const errorsByStatus = Object.fromEntries(Object.entries(spec.errorsByStatus)
        .map(([status, count]) => [status, share(count ?? 0, chunk)])
        .filter(([, count]) => (count as number) > 0)) as Record<number, number>;
      const totalErrors = Object.values(errorsByStatus).reduce((sum, count) => sum + count, 0);
      const throttleCount = errorsByStatus[429] ?? 0;
      const totalRecords = share(spec.totalRecords, chunk);
      const failureCount = totalErrors - throttleCount;
      return {
        integrationTimestamp: syncRunId,
        eventType: `STATISTICS-chunk-${chunkId}`,
        errorType: 'STATISTICS',
        chunkId,
        startTimestamp: new Date(start + (chunk + 1) * MINUTE).toISOString(),
        endTimestamp: new Date(start + (chunk + 2) * MINUTE + (hash(syncRunId, chunk) % 50) * 1000).toISOString(),
        chunkCount,
        chunkSize: ITEMS_PER_CHUNK,
        totalRecords,
        sourceDescription: MockRunCatalog.source(spec),
        successCount: totalRecords - failureCount,
        failureCount,
        skippedCount: 0,
        addedCount: share(spec.added, chunk),
        updatedCount: share(spec.updated, chunk),
        removedCount: 0,
        totalErrors,
        throttleCount,
        errorsByStatus
      };
    });
  }

  /**
   * The run's error items, in the order RunReporting pages them (sort key ERROR:<statusCode>:<timestamp>),
   * optionally only those of one status code
   */
  errors(syncRunId: string, statusCode?: number): Record<string, any>[] {
    return this.cached(`errors|${syncRunId}|${statusCode ?? ''}`, () => this.buildErrors(syncRunId, statusCode));
  }

  private buildErrors(syncRunId: string, statusCode?: number): Record<string, any>[] {
    const spec = this.byId.get(syncRunId);
    if (!spec || spec.outcome === 'TERMINAL_ERROR') {
      return [];
    }
    const recorded = this.chunkStatistics(syncRunId).reduce((counts, chunk) => {
      for (const [status, count] of Object.entries(chunk.errorsByStatus)) {
        counts[Number(status)] = (counts[Number(status)] ?? 0) + count;
      }
      return counts;
    }, {} as Record<number, number>);
    const start = Date.parse(syncRunId);
    return ERROR_STATUS_CODES
      .filter(code => statusCode === undefined || code === statusCode)
      .flatMap(code => Array.from({ length: recorded[code] ?? 0 }, (_, n) => {
        const timestamp = new Date(start + 90 * 1000 + n * 7 * 1000 + (hash(syncRunId, code, n) % 5000)).toISOString();
        const buid = buidFor(syncRunId, code, n);
        return {
          integrationTimestamp: syncRunId,
          eventType: `ERROR:${code}:${timestamp}`,
          errorType: `ERROR:${code}`,
          timestamp,
          statusCode: code,
          ...MockRunCatalog.errorDetail(code, buid, hash(syncRunId, code, n))
        };
      }));
  }

  /** The person changes of one type recorded for the run (DynamoDB mode keeps them; see getRunChanges) */
  changes(syncRunId: string, changeType: ChangeType): PersonHistoryRecord[] {
    return this.cached(`changes|${syncRunId}|${changeType}`, () => this.buildChanges(syncRunId, changeType));
  }

  private buildChanges(syncRunId: string, changeType: ChangeType): PersonHistoryRecord[] {
    const spec = this.byId.get(syncRunId);
    if (!spec || spec.outcome === 'TERMINAL_ERROR' || spec.dryRun) {
      return [];
    }
    const recordedFraction = spec.outcome === 'INCOMPLETE' ? 0.6 : 1;
    const count = Math.floor({ NEW: spec.added, UPDATED: spec.updated, DELETED: spec.deactivated }[changeType] * recordedFraction);
    return Array.from({ length: count }, (_, n): PersonHistoryRecord => ({
      personId: spec.buid ?? buidFor(syncRunId, changeType, n),
      syncRunId,
      hash: hex(syncRunId, changeType, n),
      changeType,
      ...(changeType === 'NEW' ? {} : { previousHash: hex(syncRunId, changeType, n, 'previous') })
    })).sort((a, b) => a.personId < b.personId ? -1 : a.personId > b.personId ? 1 : 0);
  }

  private summary(spec: MockRunSpec): RunSummary {
    // Summed as the post-processor sums a run's chunks
    const { lastChunkAt: _, ...totals } = sumChunkStatistics(this.chunkStatistics(spec.syncRunId));
    const statusFor: Record<string, RunSummaryStatus> = {
      SUCCEEDED: 'SUCCEEDED', FAILED: 'FAILED', POST_PROCESSING_FAILED: 'POST_PROCESSING_FAILED'
    };
    return {
      syncRunId: spec.syncRunId,
      status: statusFor[spec.outcome],
      storageMode: this.storageMode,
      dryRun: spec.dryRun,
      createdAt: spec.syncRunId,
      completedAt: new Date(Date.parse(spec.syncRunId) + spec.durationMinutes * MINUTE).toISOString(),
      syncPopulation: spec.syncPopulation,
      bulkReset: spec.bulkReset,
      chunkCount: MockRunCatalog.chunkCount(spec),
      totalRecords: spec.totalRecords,
      ...totals,
      deletion: spec.deletion
    };
  }

  private static chunkCount(spec: MockRunSpec): number {
    return Math.max(1, Math.ceil(spec.totalRecords / ITEMS_PER_CHUNK));
  }

  private static source(spec: MockRunSpec): string {
    return `https://cdm.example.bu.edu/api/people${spec.buid ? `/person?buid=${spec.buid}` : ''}`;
  }

  /** An error's fields after its code: like ApiErrorTracking's, the person it concerns is in detailsObj (a JSON string) */
  private static errorDetail(code: number, buid: string, seed: number): Record<string, any> {
    return {
      ...MockRunCatalog.errorText(code, buid, seed),
      detailsObj: JSON.stringify({ hrn: `hrn:hrs:persons:${buid}`, sourceIdentifier: buid })
    };
  }

  private static errorText(code: number, buid: string, seed: number): Record<string, any> {
    switch (code) {
      case 400:
        return { statusText: 'Bad Request', message: `Validation failed for ${buid}: organization hrn:hrs:orgs:${1000 + seed % 900} does not exist`, isThrottled: false };
      case 404:
        return { statusText: 'Not Found', message: `No person hrn:hrs:persons:${buid} to update`, isThrottled: false };
      case 429:
        return { statusText: 'Too Many Requests', message: `Rate limit exceeded while updating ${buid}; retried`, isThrottled: true };
      default:
        return { statusText: 'Internal Server Error', message: `Huron returned an unexpected error for ${buid}`, incidentId: `INC-${String(seed % 1_000_000).padStart(6, '0')}`, isThrottled: false };
    }
  }

  /** Run n, counting back from the newest (0) */
  private static spec(n: number): MockRunSpec {
    const nightly = Date.parse(NEWEST_MOCK_RUN) - n * DAY;
    const seed = hash('run', n);
    const full = n % 7 === 0;
    const base: MockRunSpec = {
      syncRunId: new Date(nightly).toISOString(),
      outcome: 'SUCCEEDED',
      syncPopulation: full ? 'person-full' : 'person-delta',
      totalRecords: full ? FULL_POPULATION : 300 + seed % 600,
      added: 2 + seed % 9,
      updated: 20 + seed % 80,
      deactivated: full ? 1 + seed % 6 : 0,
      errorsByStatus: { 400: seed % 3, 429: (seed >> 3) % 4, 500: (seed >> 5) % 2 },
      dryRun: false,
      bulkReset: false,
      durationMinutes: full ? 38 + seed % 15 : 6 + seed % 6
    };
    const spec: MockRunSpec = { ...base, ...MockRunCatalog.specialCase(n, base, seed) };
    if (spec.deletion === undefined && (spec.outcome === 'SUCCEEDED' || spec.outcome === 'FAILED' || spec.outcome === 'POST_PROCESSING_FAILED')) {
      spec.deletion = MockRunCatalog.deletion(spec);
    }
    return spec;
  }

  /** The runs that differ from a nightly run, at fixed positions from the newest */
  private static specialCase(n: number, base: MockRunSpec, seed: number): Partial<MockRunSpec> {
    const afternoon = (day: number) => new Date(Date.parse(NEWEST_MOCK_RUN) - day * DAY + 13 * 60 * MINUTE + 17 * MINUTE).toISOString();
    switch (n) {
      case 0: // The latest run: a full sync with enough errors and changes to need several pages
        return {
          syncPopulation: 'person-full', totalRecords: FULL_POPULATION, added: 130, updated: 240, deactivated: 12,
          errorsByStatus: { 400: 40, 404: 25, 429: 120, 500: 75 }, durationMinutes: 47
        };
      case 1: // Stopped part way, without being marked failed (or still running)
        return { outcome: 'INCOMPLETE', syncPopulation: 'person-full', totalRecords: FULL_POPULATION, deactivated: 0 };
      case 3: // A single person, synced from the dashboard in the afternoon
      case 17:
        return {
          syncRunId: afternoon(n), syncPopulation: 'person-delta', buid: buidFor('single', n), totalRecords: 1,
          added: 0, updated: 1, deactivated: 0, errorsByStatus: {}, durationMinutes: 2
        };
      case 5: // Chunking failed, so the run was marked failed and never post-processed
        return { outcome: 'TERMINAL_ERROR', syncPopulation: 'person-full', totalRecords: 0 };
      case 8: // A dry run: mapped and compared, nothing sent
        return {
          dryRun: true, errorsByStatus: {},
          deletion: { status: 'SKIPPED', reason: 'DELETES_DISABLED', message: 'Dry run: deletions are not applied' }
        };
      case 11: // The post-processor itself failed
        return {
          outcome: 'POST_PROCESSING_FAILED', syncPopulation: 'person-full', totalRecords: FULL_POPULATION,
          deletion: { status: 'FAILED', message: 'Timed out reading the person current-state table' }
        };
      case 14: // Failed: the run was marked failed, so post-processing skipped the deletes
        return {
          outcome: 'FAILED', syncPopulation: 'person-full', totalRecords: FULL_POPULATION, deactivated: 0,
          errorsByStatus: { 500: 18 + seed % 10, 429: 6 },
          deletion: { status: 'SKIPPED', reason: 'TERMINAL_ERROR', message: 'The run is marked failed: deletions skipped' }
        };
      case 20: // A bulk reset: everyone re-sent
        return {
          bulkReset: true, syncPopulation: 'person-full', totalRecords: FULL_POPULATION,
          added: 0, updated: FULL_POPULATION - 40, deactivated: 0, durationMinutes: 95
        };
      default:
        return {};
    }
  }

  private static deletion(spec: MockRunSpec): DeletionOutcome {
    if (spec.syncPopulation === 'person-delta') {
      return { status: 'SKIPPED', reason: 'PERSON_DELTA', message: 'Deletions are detected only in a full sync' };
    }
    return {
      status: 'COMPLETED', deletedCount: spec.deactivated, failedCount: 0, totalProcessed: spec.deactivated,
      message: `Deactivated ${spec.deactivated} people no longer in the source`
    };
  }
}

/**
 * Pages over an in-memory list with offset cursors that, like RunReporting's, only continue the listing
 * they came from: the cursor carries the query it belongs to, and any other is an INVALID_CURSOR.
 */
export class OffsetPager {
  constructor(private readonly query: Record<string, string | number | undefined>) {}

  page<T>(items: T[], page: { limit?: number; cursor?: string } = {}): Page<T> {
    const limit = OffsetPager.resolveLimit(page.limit);
    const offset = page.cursor ? this.decode(page.cursor) : 0;
    const next = offset + limit;
    return {
      items: items.slice(offset, next),
      ...(next < items.length ? { cursor: this.encode(next) } : {})
    };
  }

  private encode(offset: number): string {
    return Buffer.from(JSON.stringify({ ...this.query, offset })).toString('base64url');
  }

  private decode(cursor: string): number {
    let key: any;
    try {
      key = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    } catch {
      throw new ReportingError('INVALID_CURSOR', 'cursor is not valid');
    }
    if (!key || typeof key !== 'object' || !Number.isInteger(key.offset) || key.offset < 0) {
      throw new ReportingError('INVALID_CURSOR', 'cursor is not valid');
    }
    for (const [name, value] of Object.entries(this.query)) {
      if (key[name] !== value) {
        throw new ReportingError('INVALID_CURSOR', 'cursor does not belong to this query');
      }
    }
    return key.offset;
  }

  private static resolveLimit(limit?: number): number {
    if (limit === undefined) return DEFAULT_PAGE_LIMIT;
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_PAGE_LIMIT) {
      throw new ReportingError('INVALID_LIMIT', `limit must be an integer from 1 to ${MAX_PAGE_LIMIT}`);
    }
    return limit;
  }
}

export class MockRunReportingService implements RunReportingService {
  private readonly catalog: MockRunCatalog;

  /** @param storageMode the storage mode to report (default: MOCK_STORAGE_MODE, else dynamodb) */
  constructor(storageMode: string | undefined = process.env.MOCK_STORAGE_MODE) {
    this.catalog = MockRunCatalog.for(MockRunReportingService.parseStorageMode(storageMode));
  }

  static parseStorageMode(value?: string): MockStorageMode {
    const mode = (value || 'dynamodb').trim().toLowerCase();
    if (!MOCK_STORAGE_MODES.includes(mode as MockStorageMode)) {
      throw new Error(`MOCK_STORAGE_MODE must be one of ${MOCK_STORAGE_MODES.join(', ')} (got '${value}')`);
    }
    return mode as MockStorageMode;
  }

  getCapabilities(): Capabilities {
    const { storageMode } = this.catalog;
    return { landscape: LOCAL_LANDSCAPE, storageMode, changesSupported: storageMode === 'dynamodb' };
  }

  async listRuns(): Promise<string[]> {
    return this.catalog.syncRunIds();
  }

  async listRunPage(params: { limit: number; before?: string }): Promise<RunIdPage> {
    return pageOfRunIds(this.catalog.syncRunIds(), params);
  }

  async getRunReport(syncRunId: string): Promise<RunReport | undefined> {
    return this.catalog.report(syncRunId);
  }

  async getRunChunkStatistics(syncRunId: string): Promise<StatisticsItem[]> {
    return this.catalog.chunkStatistics(syncRunId);
  }

  async getRunErrors(syncRunId: string, page: { statusCode?: number; limit?: number; cursor?: string } = {}): Promise<Page<Record<string, any>>> {
    const { statusCode, ...paging } = page;
    return new OffsetPager({ syncRunId, statusCode }).page(this.catalog.errors(syncRunId, statusCode), paging);
  }

  async getRunChanges(syncRunId: string, changeType: ChangeType, page: { limit?: number; cursor?: string } = {}): Promise<Page<PersonHistoryRecord>> {
    if (this.catalog.storageMode !== 'dynamodb') {
      throw new ReportingError('UNSUPPORTED_IN_STORAGE_MODE',
        `Per-person change lists are not recorded in ${this.catalog.storageMode} storage mode`);
    }
    return new OffsetPager({ syncRunId, changeType }).page(this.catalog.changes(syncRunId, changeType), page);
  }
}
