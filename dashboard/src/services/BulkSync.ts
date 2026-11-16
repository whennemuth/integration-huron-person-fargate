import { GetFunctionConfigurationCommand, GetFunctionUrlConfigCommand, InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { Config } from 'integration-huron-person';
import { SyncPopulation } from '../../../docker/chunkTypes';
import type { ApiChunkerEvent, MockLandscapeResetEvent } from '../../../src/chunking/ChunkerSubscriber';
import { MockLandscapeReset, MockLandscapeResetRecord } from '../../../src/chunking/MockLandscapeReset';
import { ScheduledRun } from '../../../src/chunking/ScheduledRun';
import { RunStarter, StartedBy } from '../../../src/chunking/StartedBy';
import type { RunReport } from '../../../src/reporting/RunReporting';
import type { RunnerEnv } from '../../../src/runner/RunnerTypes';
import { DesiredCount } from '../../../src/DesiredCount';
import { isMockLandscape } from '../../../src/Utils';
import {
  DEFAULT_TOTAL_POPULATION, MAX_SIMULATED_DELAY_SECONDS, PERSON_PATH, SIMULATION_QUERY_PARAMETERS, TERMS_PATH, personIndexFromBuid
} from '../../../src/chunking/fetch/SourceSimulator';
import { CACHE_SECRET } from '../handlers/DashboardCache';
import {
  BulkSyncChoices, BulkSyncErrorCode, BulkSyncPlan, BulkSyncPopulation, BulkSyncRequestBody, BulkSyncResult,
  BulkSyncRunType, BulkSyncService, BulkSyncSetting, BulkSyncSource, BulkSyncState, MockLandscapeOptions,
  MockLandscapeResetRequested, MockLandscapeResetState, RunActivityState, RunReportingService, SimulatorSettings
} from './ServiceTypes';

/** Refused starts, each with the HTTP status the route answers with */
export class BulkSyncError extends Error {
  static readonly STATUS: Record<BulkSyncErrorCode, number> = {
    INVALID_REQUEST: 400,
    PRODUCTION_LANDSCAPE: 403,
    RUN_UNDERWAY: 409,
    RUN_STALLED: 409,
    SUBSCRIBER_DRY_RUN: 409,
    RESET_UNDERWAY: 409,
    NOT_CONFIGURED: 503
  };

  constructor(readonly code: BulkSyncErrorCode, message: string) {
    super(message);
    this.name = 'BulkSyncError';
  }

  get status(): number {
    return BulkSyncError.STATUS[this.code];
  }
}

/**
 * A validated request to review or start a run. Run types: a full sync with deactivations (person-full), a full sync
 * without them (person-delta, the pipeline's name for a run that deactivates no one; a source of only the people
 * who changed doesn't exist yet), or a test (one person, by BUID). A test is always person-delta: a run with
 * deactivations deactivates everyone missing from its source, and a one-person run's source holds only that person.
 * A test is also always a single chunker task (iterationLimit 0, as RunnerForSinglePerson sends): with chunks per task
 * the chunker hands on to a next task at the next offset, which the person endpoint ignores, so the next task fetches
 * the same person again. Without bulk reset the first task's chunk usually marks the end before the next task starts,
 * and it cancels; with bulk reset the first task builds the person cache first, the next task gets ahead, and the
 * person is chunked twice (preview, 2026-11-12). Bulk reset is one choice that sets both previous-state flags
 * (ScheduledRun.previousStateFlags()).
 */
export class BulkSyncRequest {
  private constructor(
    readonly runType: BulkSyncRunType,
    readonly buid: string | undefined,
    readonly bulkReset: boolean,
    readonly iterationLimit: number,
    readonly parallelStart: number,
    readonly overrideStalled: boolean,
    /** A mock landscape's run: the source and Start from scratch are its own options */
    readonly mockLandscape: boolean = false,
    readonly source: BulkSyncSource = 'real',
    readonly simulator: Partial<SimulatorSettings> = {},
    readonly resetState: boolean = false
  ) {}

  static readonly SOURCES: BulkSyncSource[] = ['real', 'simulator'];
  /**
   * The most people the source simulator may be asked for: the chunker's default safety cutoff
   * (DEFAULT_MAX_TOTAL_RECORDS in docker/chunkTypes.ts), past which a run would fail as a glitched source
   */
  static readonly TOTAL_POPULATION_MAX = 500_000;
  /** Each simulator setting a request may give: its range, and whether it must be a whole number */
  static readonly SIMULATOR_LIMITS: Record<keyof SimulatorSettings, { min: number; max: number; whole?: boolean }> = {
    totalPopulation: { min: 1, max: BulkSyncRequest.TOTAL_POPULATION_MAX, whole: true },
    simulatedDelaySeconds: { min: 0, max: MAX_SIMULATED_DELAY_SECONDS },
    errorRate: { min: 0, max: 1 },
    nonCurrentTermRate: { min: 0, max: 1 }
  };

  /** A BUID: U followed by digits */
  static readonly BUID = /^U\d+$/;
  static readonly RUN_TYPES: BulkSyncRunType[] = ['full', 'full-no-deactivations', 'test'];
  /** Chunks each chunker task fetches before it hands the rest on to a new task, unless the request says otherwise */
  static readonly CHUNKS_PER_TASK_DEFAULT = 10;
  /** The most chunks per task a request may ask for (0 asks for a single task that fetches them all) */
  static readonly CHUNKS_PER_TASK_MAX = 50;
  /**
   * The most chunker tasks a parallel start may start with (MESSAGES_TO_PREPOPULATE and DESIRED_COUNT alike), whatever
   * the chunker service allows; the service's own maximum lowers it (BulkSync.parallelStartMax()). The chunker
   * subscriber's limit (ParallelStart.MAX in src/chunking/ParallelStart.ts) is the same.
   */
  static readonly PARALLEL_START_MAX = 20;

  /**
   * @param options.mockLandscape Whether the landscape is a mock landscape, the only kind whose runs may choose their
   *   source and start from scratch
   * @throws BulkSyncError INVALID_REQUEST naming what is wrong
   */
  static parse(body: BulkSyncRequestBody, options: { mockLandscape?: boolean } = {}): BulkSyncRequest {
    const invalid = (message: string) => new BulkSyncError('INVALID_REQUEST', message);
    const mockLandscape = !!options.mockLandscape;
    const {
      runType, buid, bulkReset = ScheduledRun.BULK_RESET, iterationLimit = BulkSyncRequest.CHUNKS_PER_TASK_DEFAULT, parallelStart = 0,
      overrideStalled = false, source = 'real', simulator, resetState = false
    } = body || {};
    if (!BulkSyncRequest.RUN_TYPES.includes(runType as BulkSyncRunType)) {
      throw invalid(`runType must be one of ${BulkSyncRequest.RUN_TYPES.map(type => `'${type}'`).join(', ')}`);
    }
    for (const [name, value] of Object.entries({ bulkReset, overrideStalled })) {
      if (typeof value !== 'boolean') {
        throw invalid(`${name} must be true or false`);
      }
    }
    if (buid !== undefined && buid !== null && typeof buid !== 'string') {
      throw invalid('buid must be a string');
    }
    const person = typeof buid === 'string' && buid.trim() !== '' ? buid.trim().toUpperCase() : undefined;
    if (runType === 'test' && !person) {
      throw invalid('A test needs the BUID of the person to sync');
    }
    if (runType !== 'test' && person) {
      throw invalid('A BUID is only for a test: a full sync takes everyone in the source');
    }
    if (person && !BulkSyncRequest.BUID.test(person)) {
      throw invalid(`buid must be a U followed by digits (got '${person}')`);
    }
    if (!Number.isInteger(iterationLimit) || (iterationLimit as number) < 0 || (iterationLimit as number) > BulkSyncRequest.CHUNKS_PER_TASK_MAX) {
      throw invalid(`iterationLimit must be a whole number from 0 (a single task) to ${BulkSyncRequest.CHUNKS_PER_TASK_MAX}`);
    }
    if (!Number.isInteger(parallelStart) || (parallelStart as number) < 0 || (parallelStart as number) > BulkSyncRequest.PARALLEL_START_MAX) {
      throw invalid(`parallelStart must be a whole number from 0 to ${BulkSyncRequest.PARALLEL_START_MAX}`);
    }
    if (parallelStart !== 0 && runType === 'test') {
      throw invalid('A test starts with one chunker task: parallelStart must be 0');
    }
    if (parallelStart !== 0 && iterationLimit === 0) {
      throw invalid("A single task can't be started in parallel: parallelStart needs chunks per task above 0");
    }
    if (!BulkSyncRequest.SOURCES.includes(source as BulkSyncSource)) {
      throw invalid(`source must be one of ${BulkSyncRequest.SOURCES.map(name => `'${name}'`).join(', ')}`);
    }
    if (typeof resetState !== 'boolean') {
      throw invalid('resetState must be true or false');
    }
    if (!mockLandscape && (source === 'simulator' || resetState || simulator !== undefined)) {
      throw invalid('The source simulator and Start from scratch are only for a mock landscape');
    }
    if (simulator !== undefined && source !== 'simulator') {
      throw invalid('Simulator settings are only for a run whose source is the source simulator');
    }
    const settings = BulkSyncRequest.parseSimulator(simulator, invalid);
    const index = person && source === 'simulator' ? personIndexFromBuid(person) : undefined;
    if (person && source === 'simulator' && index === undefined) {
      throw invalid(`The source simulator's BUIDs are a U followed by digits from 1 (got '${person}')`);
    }
    if (index !== undefined && settings.totalPopulation !== undefined && index >= settings.totalPopulation) {
      throw invalid(`${person} is beyond the source simulator's ${settings.totalPopulation} people`);
    }
    // A test is a single chunker task, whatever chunks per task the form sent (see the class's comment)
    const chunksPerTask = runType === 'test' ? 0 : iterationLimit as number;
    return new BulkSyncRequest(
      runType as BulkSyncRunType, person, bulkReset as boolean, chunksPerTask, parallelStart as number,
      overrideStalled as boolean, mockLandscape, source as BulkSyncSource, settings, resetState
    );
  }

  /** The simulator's settings a request gives, each checked against SIMULATOR_LIMITS */
  private static parseSimulator(simulator: unknown, invalid: (message: string) => BulkSyncError): Partial<SimulatorSettings> {
    if (simulator === undefined || simulator === null) {
      return {};
    }
    if (typeof simulator !== 'object' || Array.isArray(simulator)) {
      throw invalid("simulator must be an object of the source simulator's settings");
    }
    const settings: Partial<SimulatorSettings> = {};
    for (const [name, value] of Object.entries(simulator as Record<string, unknown>)) {
      const limits = BulkSyncRequest.SIMULATOR_LIMITS[name as keyof SimulatorSettings];
      if (!limits) {
        throw invalid(`simulator.${name} isn't a source simulator setting (${Object.keys(BulkSyncRequest.SIMULATOR_LIMITS).join(', ')})`);
      }
      if (typeof value !== 'number' || !Number.isFinite(value) || value < limits.min || value > limits.max || (limits.whole && !Number.isInteger(value))) {
        throw invalid(`simulator.${name} must be ${limits.whole ? 'a whole number' : 'a number'} from ${limits.min} to ${limits.max}`);
      }
      settings[name as keyof SimulatorSettings] = value;
    }
    return settings;
  }

  /** Only a full sync with deactivations deactivates people missing from its source */
  get population(): BulkSyncPopulation {
    return this.runType === 'full' ? SyncPopulation.PersonFull : SyncPopulation.PersonDelta;
  }

  get trustPreviousStorage(): boolean {
    return ScheduledRun.previousStateFlags(this.bulkReset).trustPreviousStorage;
  }

  choices(): BulkSyncChoices {
    const {
      runType, population, buid, bulkReset, trustPreviousStorage, iterationLimit, parallelStart, overrideStalled, mockLandscape,
      source, simulator, resetState
    } = this;
    return {
      runType, population, ...(buid ? { buid } : {}), bulkReset, trustPreviousStorage, iterationLimit, parallelStart, overrideStalled,
      // A mock landscape's own options, only there
      ...(mockLandscape ? { source, ...(source === 'simulator' ? { simulator } : {}), resetState } : {})
    };
  }
}

/**
 * The newest run's state, as far as starting another is concerned: underway, or stalled, or neither. The stall
 * rule is the dashboard's (RunView.estimate() in public/js/run-history.js, phase 5a): no chunk has finished for
 * more than STALL_FACTOR times the run's average gap between chunks, and at least MIN_STALL_MS. A run with no
 * chunk finished yet is underway for FIRST_CHUNK_GRACE_MS after it started (the chunker's first fetches), then
 * stalled. Keep the constants in step with run-history.js.
 */
export class RunActivity {
  static readonly STALL_FACTOR = 10;
  static readonly MIN_STALL_MS = 15 * 60_000;
  static readonly FIRST_CHUNK_GRACE_MS = 60 * 60_000;

  static of(report: RunReport | { syncRunId: string; status: 'UNKNOWN' } | undefined, now: number): RunActivityState {
    if (!report) {
      return { kind: 'none' };
    }
    const { syncRunId } = report;
    const full = report as RunReport;
    if (full.summary) {
      return { kind: 'finished', syncRunId };
    }
    if (report.status === 'FAILED') {
      return { kind: 'failed', syncRunId };
    }
    if (report.status === 'UNKNOWN') {
      // Listed, but nothing recorded to report on: nothing is running
      return { kind: 'finished', syncRunId };
    }
    const started = Date.parse(full.metadata?.createdAt ?? syncRunId);
    const lastChunkAt = full.progress?.lastChunkAt;
    const last = lastChunkAt ? Date.parse(lastChunkAt) : NaN;
    const chunks = full.progress?.chunksReported ?? 0;
    if (!Number.isFinite(last) || chunks === 0) {
      const stalled = Number.isFinite(started) && now - started > RunActivity.FIRST_CHUNK_GRACE_MS;
      return { kind: stalled ? 'stalled' : 'underway', syncRunId };
    }
    const averageGap = Number.isFinite(started) && last > started ? (last - started) / chunks : 0;
    const stalled = now - last > Math.max(RunActivity.MIN_STALL_MS, RunActivity.STALL_FACTOR * averageGap);
    return { kind: stalled ? 'stalled' : 'underway', syncRunId, lastChunkAt };
  }
}

/** The chunker service, as far as a parallel start is concerned: how many tasks it may run */
export interface ChunkerService {
  /** The service's auto-scaling maximum, or undefined if it can't be read */
  maxTasks(): Promise<number | undefined>;
}

/** The chunker service's maximum from its scalable target (DesiredCount); undefined, with a warning, if unreadable */
export class AwsChunkerService implements ChunkerService {
  constructor(private readonly service: { clusterName?: string; serviceName?: string; region?: string }) {}

  async maxTasks(): Promise<number | undefined> {
    const { clusterName, serviceName, region } = this.service;
    if (!clusterName || !serviceName || !region) {
      return undefined;
    }
    try {
      return await new DesiredCount({ clusterName, serviceName, region }).getMax();
    } catch (error: any) {
      console.warn(`Couldn't read the chunker service's maximum tasks (${clusterName}/${serviceName}): ${error?.message ?? error}`);
      return undefined;
    }
  }
}

/** A mock landscape's source simulator, as far as starting a run is concerned */
export interface SourceSimulatorService {
  /** Its Function URL, or why there is none to use */
  lookup(): Promise<{ url?: string; reason?: string }>;
  /** Its deployed settings (its /health), or undefined if they can't be read */
  settings(url: string): Promise<Partial<SimulatorSettings> | undefined>;
}

/**
 * The simulator Lambda by name: its Function URL (lambda:GetFunctionUrlConfig, granted only to a mock landscape's
 * dashboard), and its settings from its own unauthenticated /health. Nothing about it is changed.
 */
export class AwsSourceSimulatorService implements SourceSimulatorService {
  static readonly HEALTH_TIMEOUT_MS = 5000;

  constructor(private readonly functionName: string, private readonly client: LambdaClient) {}

  async lookup(): Promise<{ url?: string; reason?: string }> {
    try {
      const { FunctionUrl } = await this.client.send(new GetFunctionUrlConfigCommand({ FunctionName: this.functionName }));
      return FunctionUrl ? { url: FunctionUrl } : { reason: `The source simulator (${this.functionName}) has no Function URL` };
    } catch (error: any) {
      if (error?.name === 'ResourceNotFoundException') {
        return { reason: `The source simulator (${this.functionName}) isn't deployed in this landscape` };
      }
      console.warn(`Couldn't look up the source simulator (${this.functionName}): ${error?.message ?? error}`);
      return { reason: `The source simulator (${this.functionName}) couldn't be looked up: ${error?.message ?? error}` };
    }
  }

  async settings(url: string): Promise<Partial<SimulatorSettings> | undefined> {
    try {
      const response = await fetch(new URL('health', url), { signal: AbortSignal.timeout(AwsSourceSimulatorService.HEALTH_TIMEOUT_MS) });
      if (!response.ok) {
        return undefined;
      }
      const { totalPopulation, simulatedDelaySeconds, errorRate } = await response.json() as Record<string, unknown>;
      const numbers = Object.entries({ totalPopulation, simulatedDelaySeconds, errorRate })
        .filter(([, value]) => typeof value === 'number' && Number.isFinite(value));
      return Object.fromEntries(numbers) as Partial<SimulatorSettings>;
    } catch (error: any) {
      console.warn(`Couldn't read the source simulator's settings (${url}health): ${error?.message ?? error}`);
      return undefined;
    }
  }
}

/** The pipeline's Lambda functions a start touches: the subscribers' DRY_RUN switch, and the chunker's invocation */
export interface ChunkerFunctions {
  /** Whether the function's DRY_RUN environment variable is 'true' (the manual Runner's decorators set it) */
  isDryRun(functionName: string): Promise<boolean>;
  /** Invoke asynchronously, as the EventBridge schedule does (a run's start, or a mock landscape's reset on its own) */
  invokeAsync(functionName: string, event: ApiChunkerEvent | MockLandscapeResetEvent): Promise<void>;
}

export class LambdaChunkerFunctions implements ChunkerFunctions {
  constructor(private readonly client: LambdaClient) {}

  async isDryRun(functionName: string): Promise<boolean> {
    const { Environment } = await this.client.send(new GetFunctionConfigurationCommand({ FunctionName: functionName }));
    return (Environment?.Variables?.DRY_RUN ?? 'false').toLowerCase() === 'true';
  }

  async invokeAsync(functionName: string, event: ApiChunkerEvent | MockLandscapeResetEvent): Promise<void> {
    const { StatusCode, FunctionError } = await this.client.send(new InvokeCommand({
      FunctionName: functionName,
      InvocationType: 'Event',
      Payload: Buffer.from(JSON.stringify(event))
    }));
    if (StatusCode !== 202 || FunctionError) {
      throw new Error(`The chunker function ${functionName} didn't accept the run (status ${StatusCode}${FunctionError ? `, ${FunctionError}` : ''})`);
    }
  }
}

/** What the landscape's dashboard Lambda knows about starting its runs (from its environment; see lib/FunctionUrl.ts) */
export type BulkSyncSettings = {
  landscape: string;
  /** The LANDSCAPE_TONE; 'production' refuses every start until phase 6's roles decide who may */
  landscapeTone?: string;
  region?: string;
  /** The pipeline stack's STACK_ID */
  stackId?: string;
  /** The landscape's integration config secret's name, which the chunker reads */
  secretName?: string;
  chunkerFunctionName?: string;
  processorSubscriberFunctionName?: string;
  /** The chunker task's ITEMS_PER_CHUNK, set at deploy */
  itemsPerChunk?: number;
  /** The chunker service a parallel start scales (through the chunker subscriber) */
  chunkerClusterName?: string;
  chunkerServiceName?: string;
  /** The environment variables a start needs that are missing: every start is refused as NOT_CONFIGURED */
  missing?: string[];
};

/** One run setting, without its name */
type SettingValue = Omit<BulkSyncSetting, 'name'>;

/**
 * Starts a sync run in this landscape the way its EventBridge schedule does: an asynchronous invocation of the
 * landscape's chunker subscriber Lambda with an ApiChunkerEvent. The Lambda resets the chunker's counters for a
 * run's first event (resetCountersForRunStart() in src/chunking/ChunkerSubscriber.ts), for the schedule's runs and
 * these alike. The source endpoint comes from the landscape's integration config: the people endpoint, or the
 * person endpoint with ?buid= for a test (as RunnerForSinglePerson does). The form starts at the scheduled run's
 * choices (src/chunking/ScheduledRun.ts).
 *
 * Guards, until phase 6's roles decide who may start a run: no start in a production-toned landscape, none when the
 * dashboard's environment lacks what a start needs, none while the newest run is underway (a stalled one needs
 * overrideStalled), and none while the chunker or processor subscriber is left in DRY_RUN (by the manual Runner),
 * which would swallow the run. describe() reports the first that applies; plan() and start() throw it.
 */
export class BulkSync implements BulkSyncService {

  constructor(private readonly deps: {
    config: Config;
    settings: BulkSyncSettings;
    runs: Pick<RunReportingService, 'listRunPage' | 'getRunReport'>;
    functions: ChunkerFunctions;
    /** Absent: Parallel start goes up to PARALLEL_START_MAX, and the chunker subscriber checks the service's maximum */
    chunkerService?: ChunkerService;
    /** A mock landscape's source simulator; absent elsewhere (or where the dashboard isn't told its name) */
    simulator?: SourceSimulatorService;
    /** A mock landscape's reset record (MockLandscapeReset.readRecord()); absent elsewhere */
    resetRecords?: { read(): Promise<MockLandscapeResetRecord | undefined> };
    now?: () => number;
  }) {}

  /** The source simulator's settings when neither the request nor the simulator gives them (src/chunking/fetch/SourceSimulator.ts) */
  static readonly SIMULATOR_DEFAULTS: SimulatorSettings = {
    totalPopulation: DEFAULT_TOTAL_POPULATION, simulatedDelaySeconds: 0, errorRate: 0, nonCurrentTermRate: 0
  };

  private get mockLandscape(): boolean {
    return isMockLandscape(this.deps.settings.landscape);
  }

  /** The real service, from the dashboard Lambda's environment. Missing settings refuse starts; the page still shows */
  static fromEnvironment(config: Config, runs: Pick<RunReportingService, 'listRunPage' | 'getRunReport'>): BulkSync {
    const env = process.env;
    const {
      LANDSCAPE, LANDSCAPE_TONE, REGION, PIPELINE_STACK_ID, CHUNKER_FUNCTION_NAME, PROCESSOR_SUBSCRIBER_FUNCTION_NAME,
      ITEMS_PER_CHUNK, CHUNKER_CLUSTER_NAME, CHUNKER_SERVICE_NAME, SOURCE_SIMULATOR_FUNCTION_NAME
    } = env;
    const missing = Object.entries({ LANDSCAPE, REGION, CHUNKER_FUNCTION_NAME, PROCESSOR_SUBSCRIBER_FUNCTION_NAME })
      .filter(([, value]) => !value).map(([name]) => name);
    const settings: BulkSyncSettings = {
      landscape: LANDSCAPE || 'unknown',
      landscapeTone: LANDSCAPE_TONE,
      region: REGION,
      stackId: PIPELINE_STACK_ID,
      secretName: env[CACHE_SECRET.env.arn],
      chunkerFunctionName: CHUNKER_FUNCTION_NAME,
      processorSubscriberFunctionName: PROCESSOR_SUBSCRIBER_FUNCTION_NAME,
      itemsPerChunk: ITEMS_PER_CHUNK ? Number(ITEMS_PER_CHUNK) : undefined,
      chunkerClusterName: CHUNKER_CLUSTER_NAME,
      chunkerServiceName: CHUNKER_SERVICE_NAME,
      missing
    };
    const lambda = new LambdaClient({ region: REGION });
    return new BulkSync({
      config, settings, runs,
      functions: new LambdaChunkerFunctions(lambda),
      chunkerService: new AwsChunkerService({ clusterName: CHUNKER_CLUSTER_NAME, serviceName: CHUNKER_SERVICE_NAME, region: REGION }),
      // A mock landscape's dashboard only is told the simulator's name (dashboard/lib/LandscapeResources.ts)
      ...(SOURCE_SIMULATOR_FUNCTION_NAME ? { simulator: new AwsSourceSimulatorService(SOURCE_SIMULATOR_FUNCTION_NAME, lambda) } : {}),
      // A mock landscape's reset record, in its atomic counter table (GetItem granted to a mock landscape's dashboard only)
      ...(isMockLandscape(LANDSCAPE) && PIPELINE_STACK_ID && REGION ? {
        resetRecords: { read: () => MockLandscapeReset.readRecord({ stackId: PIPELINE_STACK_ID, region: REGION, landscape: LANDSCAPE! }) }
      } : {})
    });
  }

  /** The most tasks Parallel start offers: PARALLEL_START_MAX, lowered to the chunker service's maximum when known */
  async parallelStartMax(): Promise<number> {
    const serviceMax = await this.deps.chunkerService?.maxTasks();
    return serviceMax === undefined ? BulkSyncRequest.PARALLEL_START_MAX : Math.min(BulkSyncRequest.PARALLEL_START_MAX, serviceMax);
  }

  /** @throws BulkSyncError INVALID_REQUEST for a parallel start above the most tasks the chunker service allows */
  private async checkParallelStart(request: BulkSyncRequest): Promise<void> {
    if (request.parallelStart === 0) {
      return;
    }
    const max = await this.parallelStartMax();
    if (request.parallelStart > max) {
      throw new BulkSyncError('INVALID_REQUEST',
        `Parallel start ${request.parallelStart} is more than the ${max} chunker tasks this landscape's chunker service may run`);
    }
  }

  private get now(): number {
    return (this.deps.now ?? Date.now)();
  }

  async describe(): Promise<BulkSyncState> {
    const { settings } = this.deps;
    const latestRun = await this.latestRun();
    let blocked: BulkSyncState['blocked'];
    try {
      await this.checkGuards(latestRun, { overrideStalled: false });
    } catch (error) {
      if (!(error instanceof BulkSyncError)) {
        throw error;
      }
      blocked = { code: error.code, message: error.message };
    }
    return {
      landscape: settings.landscape,
      defaults: {
        runType: ScheduledRun.POPULATION === SyncPopulation.PersonFull ? 'full' : 'full-no-deactivations',
        bulkReset: ScheduledRun.BULK_RESET,
        iterationLimit: BulkSyncRequest.CHUNKS_PER_TASK_DEFAULT,
        parallelStart: 0
      },
      iterationLimit: { max: BulkSyncRequest.CHUNKS_PER_TASK_MAX },
      parallelStart: { max: await this.parallelStartMax(), enabled: true },
      latestRun,
      ...(blocked ? { blocked } : {}),
      warnings: this.warnings(),
      ...(settings.itemsPerChunk !== undefined ? { itemsPerChunk: settings.itemsPerChunk } : {}),
      ...(this.mockLandscape ? { mock: await this.mockOptions() } : {})
    };
  }

  /**
   * Overview's Mock landscape section: the last reset (recorded by the chunker subscriber, as of now), and why a reset
   * can't be started now. Not a mock landscape: just that.
   */
  async resetState(): Promise<MockLandscapeResetState> {
    if (!this.mockLandscape) {
      return { mockLandscape: false };
    }
    let record: MockLandscapeResetRecord | undefined;
    let lastError: string | undefined;
    try {
      record = await this.deps.resetRecords?.read();
    } catch (error: any) {
      lastError = `${error?.message ?? error}`;
    }
    const last = record ? { ...record, status: MockLandscapeReset.statusOf(record, this.now) } : undefined;
    let blocked: { code: BulkSyncErrorCode; message: string } | undefined;
    try {
      await this.checkResetGuards(last?.status);
    } catch (error) {
      if (!(error instanceof BulkSyncError)) {
        throw error;
      }
      blocked = { code: error.code, message: error.message };
    }
    return {
      mockLandscape: true, landscape: this.deps.settings.landscape,
      ...(last ? { last } : {}), ...(lastError ? { lastError } : {}), ...(blocked ? { blocked } : {})
    };
  }

  /**
   * Start a mock landscape's reset on its own (Overview's Reset landscape): the chunker subscriber, invoked
   * asynchronously with a MockLandscapeResetEvent, empties the target simulator, Run history and the fingerprints, and
   * records it. Nothing is started besides.
   * @throws BulkSyncError when refused: not a mock landscape, not configured, a run underway, a reset still running,
   *   or the chunker subscriber in DRY_RUN
   */
  async requestReset(startedBy?: StartedBy): Promise<MockLandscapeResetRequested> {
    const { settings, functions } = this.deps;
    if (!this.mockLandscape) {
      throw new BulkSyncError('INVALID_REQUEST', `${settings.landscape} is not a mock landscape: only a mock landscape can be reset`);
    }
    let record: MockLandscapeResetRecord | undefined;
    try {
      record = await this.deps.resetRecords?.read();
    } catch (error: any) {
      console.warn(`Couldn't read the last mock landscape reset: ${error?.message ?? error}`);
    }
    await this.checkResetGuards(record ? MockLandscapeReset.statusOf(record, this.now) : undefined);
    const requestedAt = new Date(this.now).toISOString();
    const event: MockLandscapeResetEvent = { mockLandscapeReset: true, requestedAt, ...(startedBy ? { startedBy } : {}) };
    await functions.invokeAsync(settings.chunkerFunctionName!, event);
    console.log(`Mock landscape reset requested in ${settings.landscape}: ${JSON.stringify({ startedBy })}`);
    return { status: 'requested', landscape: settings.landscape, requestedAt };
  }

  /** @throws BulkSyncError when a reset would be refused now */
  private async checkResetGuards(lastStatus?: string): Promise<void> {
    const { settings, functions } = this.deps;
    if (settings.missing && settings.missing.length > 0) {
      throw new BulkSyncError('NOT_CONFIGURED',
        `Resetting isn't configured in this landscape's dashboard: missing ${settings.missing.join(', ')} (redeploy its dashboard stack)`);
    }
    const latestRun = await this.latestRun();
    if (latestRun.kind === 'underway') {
      throw new BulkSyncError('RUN_UNDERWAY',
        `The run of ${latestRun.syncRunId} is still underway: a reset would empty the tables it is writing to. Wait for it to finish`);
    }
    if (lastStatus === 'running') {
      throw new BulkSyncError('RESET_UNDERWAY', 'A reset is still running: wait for it to finish (Refresh to see it)');
    }
    if (await functions.isDryRun(settings.chunkerFunctionName!)) {
      throw new BulkSyncError('SUBSCRIBER_DRY_RUN',
        `The chunker subscriber (${settings.chunkerFunctionName}) is in DRY_RUN, left by the manual Runner, so a reset would do nothing: clear it first`);
    }
  }

  /** A mock landscape's options: whether the simulator can be the source, and its settings to start the form from */
  private async mockOptions(): Promise<MockLandscapeOptions> {
    const { url, reason } = await this.simulatorLookup();
    const deployed = url ? await this.deps.simulator!.settings(url) : undefined;
    return {
      simulator: {
        available: !!url,
        ...(url ? {} : { reason }),
        defaults: { ...BulkSync.SIMULATOR_DEFAULTS, ...(deployed ?? {}) }
      },
      limits: { totalPopulation: BulkSyncRequest.TOTAL_POPULATION_MAX, simulatedDelaySeconds: MAX_SIMULATED_DELAY_SECONDS }
    };
  }

  private async simulatorLookup(): Promise<{ url?: string; reason?: string }> {
    return this.deps.simulator
      ? this.deps.simulator.lookup()
      : { reason: "This landscape's dashboard isn't told the source simulator's name (redeploy its dashboard stack)" };
  }

  /**
   * The simulator's Function URL for a request whose source is the simulator; undefined for any other
   * @throws BulkSyncError INVALID_REQUEST when the simulator can't be used
   */
  private async simulatorUrlFor(request: BulkSyncRequest): Promise<string | undefined> {
    if (request.source !== 'simulator') {
      return undefined;
    }
    const { url, reason } = await this.simulatorLookup();
    if (!url) {
      throw new BulkSyncError('INVALID_REQUEST', `The source simulator can't be the source: ${reason}`);
    }
    return url;
  }

  /**
   * Who is starting a run from the dashboard: the signed-in user's buPrincipal (the JWT's SAML attribute, which the
   * IdP sends array-valued), or no one in a public dashboard. Taken from the request's sign-in token
   * (Authenticator.user), never from what the page sends.
   */
  static startedBy(user: any): StartedBy {
    const principal = user && typeof user === 'object' ? [user.buPrincipal].flat()[0] : undefined;
    return RunStarter.dashboard(principal ? `${principal}` : undefined);
  }

  /** What a start would send, after the same checks; nothing is started */
  async plan(body: BulkSyncRequestBody, startedBy?: StartedBy): Promise<BulkSyncPlan> {
    const { settings } = this.deps;
    const request = BulkSyncRequest.parse(body, { mockLandscape: this.mockLandscape });
    await this.checkParallelStart(request);
    await this.checkGuards(await this.latestRun(), request);
    const event = this.event(request, startedBy, await this.simulatorUrlFor(request));
    return {
      landscape: settings.landscape,
      choices: request.choices(),
      event: { ...event, processingMetadata: { ...event.processingMetadata, processedAt: 'set when the run is started' } },
      settings: this.runSettings(request)
    };
  }

  async start(body: BulkSyncRequestBody, startedBy?: StartedBy): Promise<BulkSyncResult> {
    const { settings, functions } = this.deps;
    const request = BulkSyncRequest.parse(body, { mockLandscape: this.mockLandscape });
    await this.checkParallelStart(request);
    await this.checkGuards(await this.latestRun(), request);
    const event = this.event(request, startedBy, await this.simulatorUrlFor(request));
    await functions.invokeAsync(settings.chunkerFunctionName!, event);
    console.log(`Bulk sync started in ${settings.landscape}: ${JSON.stringify({ ...request.choices(), startedBy })}`);
    return {
      status: 'started',
      startedAt: event.processingMetadata!.processedAt!,
      landscape: settings.landscape,
      run: request.choices()
    };
  }

  /**
   * The chunker event for a request, as the schedule (or, for a test, RunnerForSinglePerson) builds it. With the
   * source simulator as the source (simulatorUrl, its Function URL), the source is the simulator as
   * SourceSimulatorRunnerDecorator makes it: its people endpoint, or /person?buid= for a test, with the request's
   * simulator settings as query parameters (which every page request keeps), and its /terms as the termsEndpoint.
   */
  event(request: BulkSyncRequest, startedBy?: StartedBy, simulatorUrl?: string): ApiChunkerEvent {
    const { baseUrl, fetchPath, termsEndpoint } = simulatorUrl
      ? BulkSync.simulatorSource(request, simulatorUrl)
      : this.configuredSource(request);
    return {
      baseUrl,
      fetchPath,
      ...(termsEndpoint ? { termsEndpoint } : {}),
      populationType: request.population as ApiChunkerEvent['populationType'],
      ...ScheduledRun.previousStateFlags(request.bulkReset),
      iterationLimit: request.iterationLimit,
      offset: 0,
      // The chunker subscriber seeds this many messages, waits for the scale-in alarm, and sets the desired count
      ...(request.parallelStart > 0 ? { parallelStart: request.parallelStart } : {}),
      // A mock landscape's start from scratch: the chunker subscriber empties its run state first
      ...(request.resetState ? { resetState: true } : {}),
      ...(startedBy ? { startedBy } : {}),
      processingMetadata: { processedAt: new Date(this.now).toISOString(), processorVersion: '1.0.0' }
    };
  }

  /** The source endpoint in the landscape's integration config: the people endpoint, or the person endpoint for a test */
  private configuredSource(request: BulkSyncRequest): { baseUrl: string; fetchPath: string; termsEndpoint?: undefined } {
    const { config } = this.deps;
    const source = request.buid ? config.dataSource?.person : config.dataSource?.people;
    const baseUrl = (source as any)?.endpointConfig?.baseUrl as string | undefined;
    const fetchPath = (source as any)?.fetchPath as string | undefined;
    if (!baseUrl || !fetchPath) {
      throw new BulkSyncError('NOT_CONFIGURED',
        `The integration config has no ${request.buid ? 'person' : 'people'} endpoint (dataSource.${request.buid ? 'person' : 'people'}) to start a run from`);
    }
    return { baseUrl, fetchPath: request.buid ? `${fetchPath}?buid=${encodeURIComponent(request.buid)}` : fetchPath };
  }

  /** The source simulator as a run's source, from its Function URL (see event()) */
  static simulatorSource(
    request: BulkSyncRequest, simulatorUrl: string
  ): { baseUrl: string; fetchPath: string; termsEndpoint: { baseUrl: string; fetchPath: string } } {
    const url = new URL(simulatorUrl);
    const baseUrl = `${url.protocol}//${url.host}`;
    const query = new URLSearchParams();
    if (request.buid) {
      query.set('buid', request.buid);
    }
    const parameters: [keyof SimulatorSettings, string][] = [
      ['totalPopulation', SIMULATION_QUERY_PARAMETERS.TOTAL_POPULATION],
      ['simulatedDelaySeconds', SIMULATION_QUERY_PARAMETERS.SIMULATED_DELAY_SECONDS],
      ['errorRate', SIMULATION_QUERY_PARAMETERS.ERROR_RATE],
      ['nonCurrentTermRate', SIMULATION_QUERY_PARAMETERS.NON_CURRENT_TERM_RATE]
    ];
    for (const [name, parameter] of parameters) {
      const value = request.simulator[name];
      if (value !== undefined) {
        query.set(parameter, String(value));
      }
    }
    const path = request.buid ? PERSON_PATH : (url.pathname || '/');
    const queryText = query.toString();
    return {
      baseUrl,
      fetchPath: queryText ? `${path}?${queryText}` : path,
      termsEndpoint: { baseUrl, fetchPath: TERMS_PATH }
    };
  }

  /**
   * Every setting of the run, named as the manual Runner names them (every RunnerEnv key: the type checks that none
   * is left out), then the deployed settings that shape it.
   */
  runSettings(request: BulkSyncRequest): BulkSyncSetting[] {
    const { settings } = this.deps;
    const form = (value: SettingValue['value'], note?: string): SettingValue => ({ value, source: 'form', ...(note ? { note } : {}) });
    const landscape = (value: SettingValue['value'] | undefined, note?: string): SettingValue => ({ value: value ?? null, source: 'landscape', ...(note ? { note } : {}) });
    const fixed = (value: SettingValue['value'], note: string): SettingValue => ({ value, source: 'fixed', note });
    const unused = (note: string): SettingValue => ({ value: null, source: 'unused', note });
    const seeded = request.parallelStart > 0;
    const parallel = seeded
      ? 'Parallel start: the chunker subscriber sends this many messages, waits for the scale-in alarm, then sets the desired count'
      : 'Parallel start 0: one message, and auto-scaling adds tasks';
    const dryRunSwitch = "Not offered here: it switches the subscribers' DRY_RUN, which stays switched";
    const simulated = request.source === 'simulator';
    const simulatorSetting = (name: keyof SimulatorSettings): SettingValue => {
      if (!simulated) {
        return unused('Only with the source simulator');
      }
      const value = request.simulator[name];
      return value === undefined
        ? form(null, "Not sent: the simulator's own setting applies")
        : form(value, "Sent to the simulator as a query parameter of the run's fetchPath, not set in its Lambda");
    };
    const runner: Record<keyof RunnerEnv, SettingValue> = {
      secretArn: landscape(settings.secretName, "The landscape's integration config secret (its name), which the chunker reads"),
      configPath: unused('The chunker reads the config secret, not a file'),
      queueUrl: unused('The chunker subscriber sends the event on to its own chunker queue'),
      populationType: form(request.population, request.runType === 'full'
        ? 'People missing from the source are deactivated afterward'
        : "The pipeline's name for a run that deactivates no one"),
      populationScope: form(request.buid ? 'single' : 'standard'),
      iterationLimit: form(request.iterationLimit, request.buid
        ? 'A test is a single chunker task: a second task would fetch the same person again'
        : request.iterationLimit === 0
          ? 'A single chunker task fetches every chunk'
          : 'Chunks each chunker task fetches before it hands the rest on to a new task'),
      buid: form(request.buid ?? null, request.buid ? 'A test, through the person endpoint' : undefined),
      region: landscape(settings.region),
      stackId: landscape(settings.stackId, 'The pipeline stack'),
      landscape: landscape(settings.landscape),
      bulkReset: form(request.bulkReset),
      trustPreviousStorage: form(request.trustPreviousStorage, 'The opposite of bulk reset: the two are one choice'),
      skipAtomicCounterReset: fixed(false, "The chunker subscriber resets the chunker's counters at every run's start"),
      messagesToPrepopulate: form(String(request.parallelStart), parallel),
      desiredCount: form(request.parallelStart, parallel),
      clusterName: seeded ? landscape(settings.chunkerClusterName, 'The chunker service a parallel start scales') : unused('Only for a parallel start'),
      serviceName: seeded ? landscape(settings.chunkerServiceName, 'The chunker service a parallel start scales') : unused('Only for a parallel start'),
      messagingOnly: fixed(false, dryRunSwitch),
      chunkingOnly: fixed(false, dryRunSwitch),
      sourceSimulator: request.mockLandscape
        ? form(simulated, simulated ? 'The source is the source simulator' : 'The source is the real source API (a hybrid run)')
        : fixed(false, 'Mock landscapes only'),
      sourceSimulatorMockTotalPopulation: simulatorSetting('totalPopulation'),
      sourceSimulatorMockSimulatedDelaySeconds: simulatorSetting('simulatedDelaySeconds'),
      sourceSimulatorMockErrorRate: simulatorSetting('errorRate'),
      mockTargetResetState: request.mockLandscape
        ? form(request.resetState, request.resetState
          ? 'The chunker subscriber empties the mock target, Run history and fingerprints before the run starts'
          : undefined)
        : fixed(false, 'Mock landscapes only'),
      personRecordProcessorAddons: fixed(null, 'None: addons are a command-line (Runner) option')
    };
    return [
      ...Object.entries(runner).map(([name, setting]) => ({ name, ...setting })),
      { name: 'itemsPerChunk', ...landscape(settings.itemsPerChunk, "The chunker task's ITEMS_PER_CHUNK, set at deploy") },
      { name: 'chunkerFunction', ...landscape(settings.chunkerFunctionName, 'Invoked asynchronously, as the schedule does') }
    ];
  }

  private async latestRun(): Promise<RunActivityState> {
    const { runs } = this.deps;
    const { syncRunIds: [newest] } = await runs.listRunPage({ limit: 1 });
    if (!newest) {
      return RunActivity.of(undefined, this.now);
    }
    return RunActivity.of((await runs.getRunReport(newest)) ?? { syncRunId: newest, status: 'UNKNOWN' }, this.now);
  }

  /** @throws BulkSyncError when a start would be refused */
  private async checkGuards(latestRun: RunActivityState, request: { overrideStalled: boolean }): Promise<void> {
    const { settings, functions } = this.deps;
    if ((settings.landscapeTone ?? '').toLowerCase() === 'production') {
      throw new BulkSyncError('PRODUCTION_LANDSCAPE',
        `Runs can't be started from the dashboard in ${settings.landscape}, a production landscape, until roles decide who may start them`);
    }
    if (settings.missing && settings.missing.length > 0) {
      throw new BulkSyncError('NOT_CONFIGURED',
        `Starting runs isn't configured in this landscape's dashboard: missing ${settings.missing.join(', ')} (redeploy its dashboard stack)`);
    }
    if (latestRun.kind === 'underway') {
      throw new BulkSyncError('RUN_UNDERWAY', `The run of ${latestRun.syncRunId} is still underway: wait for it to finish`);
    }
    if (latestRun.kind === 'stalled' && !request.overrideStalled) {
      throw new BulkSyncError('RUN_STALLED',
        `The run of ${latestRun.syncRunId} has stalled without finishing: confirm that it has stopped to start another`);
    }
    for (const [label, functionName] of [['chunker', settings.chunkerFunctionName!], ['processor', settings.processorSubscriberFunctionName!]]) {
      if (await functions.isDryRun(functionName)) {
        throw new BulkSyncError('SUBSCRIBER_DRY_RUN',
          `The ${label} subscriber (${functionName}) is in DRY_RUN, left by the manual Runner, so the run would go nowhere: clear it first`);
      }
    }
  }

  private warnings(): string[] {
    const { landscape } = this.deps.settings;
    return isMockLandscape(landscape)
      ? [`${landscape} is a mock landscape: its target is the target simulator, never the real Huron. Its source is the real source API, which a run reads as any landscape's does, unless the source simulator is chosen below.`]
      : [];
  }
}

