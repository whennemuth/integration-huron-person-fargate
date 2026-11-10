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
 * do and whether one is allowed now; start() checks the same guards and starts the run, or throws a BulkSyncError.
 */
export interface BulkSyncService {
  describe(): Promise<BulkSyncState>;
  start(request: BulkSyncRequestBody): Promise<BulkSyncResult>;
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

/** What the Bulk sync page sends to start a run (POST /api/bulk-sync), before validation */
export type BulkSyncRequestBody = {
  population?: unknown;
  bulkReset?: unknown;
  trustPreviousStorage?: unknown;
  buid?: unknown;
  overrideStalled?: unknown;
};

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
  /** The form's starting values: the landscape's context, as the schedule uses them */
  defaults: { population: BulkSyncPopulation; bulkReset: boolean; trustPreviousStorage: boolean };
  latestRun: RunActivityState;
  /** Why a start would be refused now (a BulkSyncError code and message); absent when a start is allowed */
  blocked?: { code: BulkSyncErrorCode; message: string };
  /** Things to know before starting that don't block it (e.g. a mock landscape's real source) */
  warnings: string[];
};

export type BulkSyncErrorCode =
  'INVALID_REQUEST' | 'PRODUCTION_LANDSCAPE' | 'RUN_UNDERWAY' | 'RUN_STALLED' | 'SUBSCRIBER_DRY_RUN' | 'NOT_CONFIGURED';

/** A run that was started: what was asked of the chunker */
export type BulkSyncResult = {
  status: 'started';
  startedAt: string;
  landscape: string;
  run: {
    population: BulkSyncPopulation;
    bulkReset: boolean;
    trustPreviousStorage: boolean;
    /** Set for a single-person run */
    buid?: string;
    /** Batches each chunker task fetches before chaining the next (0: one task fetches them all) */
    chunksPerTask: number;
  };
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