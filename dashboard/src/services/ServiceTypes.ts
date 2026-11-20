import type { Launch } from './LaunchFlag';
import type { RequestAuthorizer } from '../authorization/Authorizer';
import type { StartedBy } from '../../../src/chunking/StartedBy';
import type { MockLandscapeResetRecord } from '../../../src/chunking/MockLandscapeReset';
import { CrudOperation } from 'integration-core';
import { PersonPushRequest } from "integration-huron-person";
import { StatisticsItem } from '../../../src/ApiErrorTracking';
import { PersonHistoryRecord } from '../../../src/dynamodb/PersonHistoryTable';
import { Capabilities, ChangeType, Page, RunIdPage, RunReport } from '../../../src/reporting/RunReporting';
import type { ScheduleInfo, SchedulePreview, UpcomingStart } from '../../../src/schedule/ScheduledRun';
import type { ScheduleChoices } from '../../../src/schedule/ScheduleChoices';
import type { ScheduledStartRefusal } from '../../../src/schedule/ScheduledStartGuard';

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
  /**
   * @param startedBy - who is starting it (the signed-in user), shown in the event and recorded with the run
   * @param authorizer - the request's: its choices that need actions of their own (Start from scratch: mock:reset;
   * Processor tasks above the landscape's maximum: bulk:processors) are refused with a ForbiddenError without it
   * granting them. The route's own action (bulk:start) is the gate's. Without one, no choice is checked.
   */
  plan(request: BulkSyncRequestBody, startedBy?: StartedBy, authorizer?: RequestAuthorizer): Promise<BulkSyncPlan>;
  start(request: BulkSyncRequestBody, startedBy?: StartedBy, authorizer?: RequestAuthorizer): Promise<BulkSyncResult>;
  /** A mock landscape's reset on its own (Overview): the last one, and whether one can be started now */
  resetState(): Promise<MockLandscapeResetState>;
  /** Start a mock landscape's reset on its own; throws a BulkSyncError when refused */
  requestReset(startedBy?: StartedBy): Promise<MockLandscapeResetRequested>;
}

/**
 * The landscape's bulk sync schedule (the Bulk sync schedule page; BulkSchedule over src/schedule/ScheduledRun.ts).
 * Refusals are ScheduleErrors (invalid choices or cron, too frequent, too close to another landscape's schedule,
 * exists, not found) or BulkSyncErrors (the run's settings, not configured); a choice the authorizer doesn't grant is
 * a ForbiddenError, as Bulk sync's.
 */
export interface BulkScheduleService {
  describe(): Promise<BulkScheduleState>;
  /** When a schedule made from the page's choices would run, and any problem with it, saving nothing */
  preview(body: BulkScheduleRequestBody): Promise<SchedulePreview>;
  /** The review: when, and the run, after the checks a save makes; saves nothing */
  plan(body: BulkScheduleRequestBody, authorizer?: RequestAuthorizer): Promise<BulkSchedulePlan>;
  /** Create the schedule, or replace the one there is, as reviewed */
  save(body: BulkScheduleRequestBody, changedBy?: string, authorizer?: RequestAuthorizer): Promise<BulkScheduleSaved>;
  setEnabled(enabled: boolean, changedBy?: string): Promise<BulkScheduleView>;
  delete(): Promise<{ deleted: boolean }>;
}

/** What the page sends: when (ScheduleChoices), the run (Bulk sync's form), and whether the schedule is on */
export type BulkScheduleRequestBody = {
  when?: unknown;
  run?: BulkSyncRequestBody;
  /** Default true */
  enabled?: unknown;
};

/** The landscape's schedule, as the page shows it */
export type BulkScheduleView = ScheduleInfo & {
  /** The run it starts, as Bulk sync's form values (what Edit schedule fills the form with) */
  run: BulkSyncRequestBody;
  /** Its event's endpoint, when the integration config now names another (the schedule would call the old one) */
  staleEndpoint?: { scheduled: { baseUrl: string; fetchPath: string }; configured: { baseUrl: string; fetchPath: string } };
};

/** Another landscape's schedule, for the page to show beside When */
export type OtherScheduleView = { landscape: string; enabled: boolean; realSource: boolean; description: string };

export type BulkScheduleState = {
  landscape: string;
  /** Absent when the landscape has none */
  schedule?: BulkScheduleView;
  /**
   * The last scheduled start the chunker subscriber skipped, and why (a run underway, or one launching); absent when
   * none was, or it can't be read
   */
  lastRefusal?: ScheduledStartRefusal;
  when: {
    /** The choices a new schedule's form starts from (the context's SCHEDULE cron, as choices) */
    defaults: ScheduleChoices;
    timezone: string;
    /** How far apart schedules of different landscapes that read the real source must start */
    minSpacingMinutes: number;
  };
  /** Bulk sync's form state: defaults, ranges, a mock landscape's options */
  form: BulkSyncState;
  otherSchedules: OtherScheduleView[];
  /** Why a schedule can't be created or changed here (e.g. the dashboard isn't told the schedule's role) */
  blocked?: { code: string; message: string };
};

export type BulkSchedulePlan = {
  landscape: string;
  when: SchedulePreview;
  run: { choices: BulkSyncChoices; event: Record<string, unknown>; settings: BulkSyncSetting[] };
  enabled: boolean;
  /** Whether saving replaces a schedule (else creates one) */
  replaces: boolean;
};

export type BulkScheduleSaved = { status: 'created' | 'replaced'; schedule: BulkScheduleView };

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
  /** A mock landscape only: 'real' (the source API) or 'simulator' (the source simulator) */
  source?: unknown;
  /** A mock landscape only, with the simulator as the source: its settings for this run (SimulatorSettings) */
  simulator?: unknown;
  /** A mock landscape only: start from scratch (empty the mock target, Run history and fingerprints first) */
  resetState?: unknown;
  /** The processor service's maximum tasks for this run (1-20), in place of the landscape's; not for a test */
  processorScalingCapacityOverride?: unknown;
};

/** Where a mock landscape's run reads people from: the real source API, or the source simulator */
export type BulkSyncSource = 'real' | 'simulator';

/**
 * How the source simulator simulates a run (simulationSettings() in src/chunking/fetch/SourceSimulator.ts), sent as
 * query parameters in the run's fetchPath
 */
export type SimulatorSettings = {
  /** Simulated people */
  totalPopulation: number;
  /** Seconds each response takes */
  simulatedDelaySeconds: number;
  /** Share of responses (0 to 1) that fail with a 500 */
  errorRate: number;
  /** Share of students (0 to 1) given a term that isn't current */
  nonCurrentTermRate: number;
};

/** A mock landscape's own options (BulkSyncState.mock) */
export type MockLandscapeOptions = {
  simulator: {
    /** Whether the source simulator can be chosen: deployed, with a Function URL */
    available: boolean;
    /** Why it can't be, when it can't */
    reason?: string;
    /** The form's starting values: the simulator's deployed settings (its /health), else the simulator's defaults */
    defaults: SimulatorSettings;
  };
  /** The highest population and delay a request may ask for */
  limits: { totalPopulation: number; simulatedDelaySeconds: number };
};

/**
 * The kind of run: full (everyone in the source, then people missing from it are deactivated), full-no-deactivations
 * (everyone in the source, no one deactivated), or test (one person, by BUID, through the person endpoint, no one
 * deactivated). A delta sync, of only the people who changed, needs a source endpoint that doesn't exist yet.
 */
export type BulkSyncRunType = 'full' | 'full-no-deactivations' | 'test';

/** A run's population: a full sync deactivates people missing from its source, a delta sync doesn't */
export type BulkSyncPopulation = 'person-full' | 'person-delta';

/** The newest run's state, as far as starting another is concerned (fargate src/reporting/RunActivity.ts) */
export type { RunActivityState } from '../../../src/reporting/RunActivity';
import type { RunActivityState } from '../../../src/reporting/RunActivity';

/** What starting a run in this landscape would do, and whether it is allowed now (GET /api/bulk-sync) */
export type BulkSyncState = {
  landscape: string;
  /** The form's starting values: the scheduled run's choices (src/schedule/ScheduledRun.ts), 10 chunks per task, and one message */
  defaults: {
    runType: BulkSyncRunType; bulkReset: boolean; iterationLimit: number; parallelStart: number;
    /** The landscape's processor maximum (maxScalingCapacity), where Processor tasks starts; unknown to an older deploy */
    processorScalingCapacity?: number;
  };
  /** Chunks per task's range (0, a single task, aside) */
  iterationLimit: { max: number };
  /**
   * Parallel start's range (0 to the chunker service's most tasks, at most 20), and whether the landscape offers it
   * (the page keeps it read-only at 0 when it doesn't; BulkSync always offers it)
   */
  parallelStart: { max: number; enabled: boolean };
  /** Processor tasks' range (1 to 20, whatever the landscape's maximum) */
  processorScalingCapacity: { max: number };
  latestRun: RunActivityState;
  /** Why a start would be refused now (a BulkSyncError code and message); absent when a start is allowed */
  blocked?: { code: BulkSyncErrorCode; message: string };
  /** Things to know before starting that don't block it (e.g. a mock landscape's real source) */
  warnings: string[];
  /** The chunker task's ITEMS_PER_CHUNK, for the page's estimate of a parallel start's reach */
  itemsPerChunk?: number;
  /** A mock landscape only: the source and Start from scratch */
  mock?: MockLandscapeOptions;
  /** A run launched moments ago whose chunker hasn't written its first records yet (LaunchFlag); absent otherwise */
  launch?: Launch;
  /**
   * Bulk sync schedules (this landscape's or another's) that read the real source and start within the hour: a run
   * started now would share the source with them. Absent when there are none.
   */
  upcomingSchedules?: UpcomingStart[];
};

export type BulkSyncErrorCode =
  'INVALID_REQUEST' | 'PRODUCTION_LANDSCAPE' | 'RUN_UNDERWAY' | 'RUN_STALLED' | 'SUBSCRIBER_DRY_RUN' | 'NOT_CONFIGURED' |
  'RESET_UNDERWAY' | 'RUN_LAUNCHING';

/** A mock landscape reset's status as of now: `running` beyond 15 minutes is `stopped` (MockLandscapeReset.statusOf()) */
export type MockLandscapeResetStatus = 'running' | 'succeeded' | 'failed' | 'stopped';

/**
 * Overview's Mock landscape section (GET /api/mock-landscape/reset): not a mock landscape; or the last reset, as
 * recorded by the chunker subscriber, and why a reset can't be started now
 */
export type MockLandscapeResetState =
  | { mockLandscape: false }
  | {
      mockLandscape: true;
      landscape: string;
      last?: Omit<MockLandscapeResetRecord, 'status'> & { status: MockLandscapeResetStatus };
      /** Why the last reset couldn't be read, if it couldn't */
      lastError?: string;
      blocked?: { code: BulkSyncErrorCode; message: string };
    };

/** A reset handed to the chunker subscriber (POST /api/mock-landscape/reset, 202) */
export type MockLandscapeResetRequested = { status: 'requested'; landscape: string; requestedAt: string };

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
  /** A mock landscape only (absent elsewhere): where the run reads people from */
  source?: BulkSyncSource;
  /** With the simulator as the source: the settings sent to it */
  simulator?: Partial<SimulatorSettings>;
  /** A mock landscape only: whether the run starts from scratch */
  resetState?: boolean;
  /** The processor service's maximum tasks for this run, when it isn't the landscape's */
  processorScalingCapacityOverride?: number;
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
  /** As BulkSyncState's: schedules starting within the hour */
  upcomingSchedules?: UpcomingStart[];
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