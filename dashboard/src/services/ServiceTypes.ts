import type { StartedBy } from '../../../src/chunking/StartedBy';
import { CrudOperation } from 'integration-core';
import { PersonPushRequest } from "integration-huron-person";
import { StatisticsItem } from '../../../src/ApiErrorTracking';
import { PersonHistoryRecord } from '../../../src/dynamodb/PersonHistoryTable';
import { Capabilities, ChangeType, Page, RunIdPage, RunReport } from '../../../src/reporting/RunReporting';

// Service Interfaces
export type PersonLookupParams = {
  system: 'source' | 'target' | 'source-only' | 'target-only';
  searchType: 'buid' | 'name' | 'email' | 'hrn' | 'sid' | 'uid';
  personId: string;
  firstName?: string;
  lastName?: string;
};
export interface PersonLookupService {
  lookup(params: PersonLookupParams): Promise<PersonLookupResult>;
}

export interface PersonSyncService {
  previewExperiment(personId: string, rawData: any[], operation?: CrudOperation): Promise<PersonPushRequest>;
  preview(personId: string, operation?: CrudOperation): Promise<PersonPushRequest>;
  sync(personId: string, operation: CrudOperation, hrn?: string): Promise<PersonSyncResult>;
}

/**
 * Starting a sync run in this landscape (dashboard/src/services/BulkSync.ts): describe() says what a start would
 * do and whether one is allowed now; plan() checks a request as a start would and returns what it would send;
 * start() checks the same and starts the run. plan() and start() throw a BulkSyncError when refused.
 */
export interface BulkSyncService {
  describe(): Promise<BulkSyncState>;
  /** @param startedBy - who is starting it (the signed-in user), shown in the event and recorded with the run */
  plan(request: BulkSyncRequestBody, startedBy?: StartedBy): Promise<BulkSyncPlan>;
  start(request: BulkSyncRequestBody, startedBy?: StartedBy): Promise<BulkSyncResult>;
}

export interface SystemStatusService {
  getStatus(): Promise<SystemStatusResult>;
}

/**
 * Reporting on this landscape's sync runs (fargate's src/reporting/RunReporting.ts, which
 * implements it). Page cursors are opaque; limits and cursors are validated by the implementation,
 * which throws a ReportingError with a code the routes map to a 400.
 */
export interface RunReportingService {
  getCapabilities(): Capabilities;
  listRuns(): Promise<string[]>;
  listRunPage(params: { limit: number; before?: string }): Promise<RunIdPage>;
  getRunReport(syncRunId: string): Promise<RunReport | undefined>;
  getRunChunkStatistics(syncRunId: string): Promise<StatisticsItem[]>;
  getRunErrors(syncRunId: string, page?: { statusCode?: number; limit?: number; cursor?: string }): Promise<Page<Record<string, any>>>;
  getRunChanges(syncRunId: string, changeType: ChangeType, page?: { limit?: number; cursor?: string }): Promise<Page<PersonHistoryRecord>>;
}

/**
 * The landscapes whose dashboards are registered in SSM (see LandscapeRegistry.ts), for a
 * landscape picker.
 */
export interface LandscapeListingService {
  getLandscapes(): Promise<LandscapeListing>;
}

export type LandscapeListing = {
  /** This dashboard's landscape ('local' when run locally) */
  current: string;
  landscapes: PickerLandscape[];
};

export type PickerLandscape = {
  landscape: string;
  label: string;
  url?: string;
  isDefault: boolean;
  /** True for this dashboard's own landscape */
  current: boolean;
};

// Service Result Types
export interface PersonLookupResult {
  personId: string;
  sourceData: [{
    buId: string;
    name: string;
    email: string;
    department: string;
    lastModified: string;
  }];
  targetData: [{
    hrn: string;
    status: string;
    lastSync: string;
  }];
}

export interface PersonSyncResult {
  personId: string;
  operation: string;
  status: string;
  timestamp: string;
  recordsProcessed: number;
  details: string;
}

/** What the Bulk sync page sends to review or start a run (POST /api/bulk-sync[/plan]), before validation */
export type BulkSyncRequestBody = {
  runType?: unknown;
  buid?: unknown;
  bulkReset?: unknown;
  iterationLimit?: unknown;
  parallelStart?: unknown;
  overrideStalled?: unknown;
};

/**
 * The kind of run: full (everyone in the source, then people missing from it are deactivated), full-no-deactivations
 * (everyone in the source, no one deactivated), or test (one person, by BUID, through the person endpoint, no one
 * deactivated). A delta sync, of only the people who changed, needs a source endpoint that doesn't exist yet.
 */
export type BulkSyncRunType = 'full' | 'full-no-deactivations' | 'test';

/** A run's population: a full sync deactivates people missing from its source, a delta sync doesn't */
export type BulkSyncPopulation = 'person-full' | 'person-delta';

/** The newest run's state, as far as starting another is concerned */
export type RunActivityState = {
  /** none (no runs yet), finished (summarized), failed, underway, stalled (no chunk finished for too long) */
  kind: 'none' | 'finished' | 'failed' | 'underway' | 'stalled';
  syncRunId?: string;
  /** For underway or stalled: when its last chunk finished, if any has */
  lastChunkAt?: string;
};

/** What starting a run in this landscape would do, and whether it is allowed now (GET /api/bulk-sync) */
export type BulkSyncState = {
  landscape: string;
  /** The form's starting values: the scheduled run's choices (src/chunking/ScheduledRun.ts), 10 chunks per task, and one message */
  defaults: { runType: BulkSyncRunType; bulkReset: boolean; iterationLimit: number; parallelStart: number };
  /** Chunks per task's range (0, a single task, aside) */
  iterationLimit: { max: number };
  /** Parallel start's range, and whether it can be changed yet (not until shared queue seeding exists) */
  parallelStart: { max: number; enabled: boolean };
  latestRun: RunActivityState;
  /** Why a start would be refused now (a BulkSyncError code and message); absent when a start is allowed */
  blocked?: { code: BulkSyncErrorCode; message: string };
  /** Things to know before starting that don't block it (e.g. a mock landscape's real source) */
  warnings: string[];
};

export type BulkSyncErrorCode =
  'INVALID_REQUEST' | 'PRODUCTION_LANDSCAPE' | 'RUN_UNDERWAY' | 'RUN_STALLED' | 'SUBSCRIBER_DRY_RUN' | 'NOT_CONFIGURED';

/** A validated request's choices, as the page summarizes them */
export type BulkSyncChoices = {
  runType: BulkSyncRunType;
  population: BulkSyncPopulation;
  /** Set for a test */
  buid?: string;
  bulkReset: boolean;
  trustPreviousStorage: boolean;
  /** Chunks each chunker task fetches before it hands the rest on to a new task (0: a single task fetches them all) */
  iterationLimit: number;
  /** Chunker messages sent at the start (0: one, and auto-scaling adds tasks) */
  parallelStart: number;
  overrideStalled: boolean;
};

/**
 * One setting of a run, named as the manual Runner names it (RunnerEnv in src/runner/RunnerTypes.ts, which prints
 * them at its start), or as the deployed setting it is. source says where its value comes from: the form, the
 * landscape (its deployment), fixed (the dashboard always uses this value), or unused (not part of a run started here).
 */
export type BulkSyncSetting = {
  name: string;
  value: string | number | boolean | null;
  source: 'form' | 'landscape' | 'fixed' | 'unused';
  note?: string;
};

/** What a start would send, and every setting of the run (POST /api/bulk-sync/plan): nothing is started */
export type BulkSyncPlan = {
  landscape: string;
  choices: BulkSyncChoices;
  /** The ApiChunkerEvent the chunker subscriber would receive; processingMetadata.processedAt is set at the start */
  event: Record<string, unknown>;
  settings: BulkSyncSetting[];
};

/** A run that was started: what was asked of the chunker */
export type BulkSyncResult = {
  status: 'started';
  startedAt: string;
  landscape: string;
  run: BulkSyncChoices;
};

export interface SystemStatusResult {
  timestamp: string;
  overall: string;
  services: {
    sourceApi: ServiceHealthCheck;
    targetApi: ServiceHealthCheck;
    database: ServiceHealthCheck;
    s3Storage: ServiceHealthCheck;
  };
  metrics: {
    dailySyncs: number;
    weeklyErrors: number;
    avgProcessingTime: string;
  };
}

export interface ServiceHealthCheck {
  status: string;
  responseTime: number;
  lastCheck: string;
}