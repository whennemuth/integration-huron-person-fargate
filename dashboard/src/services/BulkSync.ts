import { GetFunctionConfigurationCommand, InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { Config } from 'integration-huron-person';
import { SyncPopulation } from '../../../docker/chunkTypes';
import type { ApiChunkerEvent } from '../../../src/chunking/ChunkerSubscriber';
import { ScheduledRun } from '../../../src/chunking/ScheduledRun';
import { RunStarter, StartedBy } from '../../../src/chunking/StartedBy';
import type { RunReport } from '../../../src/reporting/RunReporting';
import type { RunnerEnv } from '../../../src/runner/RunnerTypes';
import { DesiredCount } from '../../../src/DesiredCount';
import { isMockLandscape } from '../../../src/Utils';
import { CACHE_SECRET } from '../handlers/DashboardCache';
import {
  BulkSyncChoices, BulkSyncErrorCode, BulkSyncPlan, BulkSyncPopulation, BulkSyncRequestBody, BulkSyncResult,
  BulkSyncRunType, BulkSyncService, BulkSyncSetting, BulkSyncState, RunActivityState, RunReportingService
} from './ServiceTypes';

/** Refused starts, each with the HTTP status the route answers with */
export class BulkSyncError extends Error {
  static readonly STATUS: Record<BulkSyncErrorCode, number> = {
    INVALID_REQUEST: 400,
    PRODUCTION_LANDSCAPE: 403,
    RUN_UNDERWAY: 409,
    RUN_STALLED: 409,
    SUBSCRIBER_DRY_RUN: 409,
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
    readonly overrideStalled: boolean
  ) {}

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

  /** @throws BulkSyncError INVALID_REQUEST naming what is wrong */
  static parse(body: BulkSyncRequestBody): BulkSyncRequest {
    const invalid = (message: string) => new BulkSyncError('INVALID_REQUEST', message);
    const {
      runType, buid, bulkReset = ScheduledRun.BULK_RESET, iterationLimit = BulkSyncRequest.CHUNKS_PER_TASK_DEFAULT, parallelStart = 0,
      overrideStalled = false
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
    // A test is a single chunker task, whatever chunks per task the form sent (see the class's comment)
    const chunksPerTask = runType === 'test' ? 0 : iterationLimit as number;
    return new BulkSyncRequest(
      runType as BulkSyncRunType, person, bulkReset as boolean, chunksPerTask, parallelStart as number,
      overrideStalled as boolean
    );
  }

  /** Only a full sync with deactivations deactivates people missing from its source */
  get population(): BulkSyncPopulation {
    return this.runType === 'full' ? SyncPopulation.PersonFull : SyncPopulation.PersonDelta;
  }

  get trustPreviousStorage(): boolean {
    return ScheduledRun.previousStateFlags(this.bulkReset).trustPreviousStorage;
  }

  choices(): BulkSyncChoices {
    const { runType, population, buid, bulkReset, trustPreviousStorage, iterationLimit, parallelStart, overrideStalled } = this;
    return { runType, population, ...(buid ? { buid } : {}), bulkReset, trustPreviousStorage, iterationLimit, parallelStart, overrideStalled };
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

/** The pipeline's Lambda functions a start touches: the subscribers' DRY_RUN switch, and the chunker's invocation */
export interface ChunkerFunctions {
  /** Whether the function's DRY_RUN environment variable is 'true' (the manual Runner's decorators set it) */
  isDryRun(functionName: string): Promise<boolean>;
  /** Invoke asynchronously, as the EventBridge schedule does */
  invokeAsync(functionName: string, event: ApiChunkerEvent): Promise<void>;
}

export class LambdaChunkerFunctions implements ChunkerFunctions {
  constructor(private readonly client: LambdaClient) {}

  async isDryRun(functionName: string): Promise<boolean> {
    const { Environment } = await this.client.send(new GetFunctionConfigurationCommand({ FunctionName: functionName }));
    return (Environment?.Variables?.DRY_RUN ?? 'false').toLowerCase() === 'true';
  }

  async invokeAsync(functionName: string, event: ApiChunkerEvent): Promise<void> {
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
    now?: () => number;
  }) {}

  /** The real service, from the dashboard Lambda's environment. Missing settings refuse starts; the page still shows */
  static fromEnvironment(config: Config, runs: Pick<RunReportingService, 'listRunPage' | 'getRunReport'>): BulkSync {
    const env = process.env;
    const {
      LANDSCAPE, LANDSCAPE_TONE, REGION, PIPELINE_STACK_ID, CHUNKER_FUNCTION_NAME, PROCESSOR_SUBSCRIBER_FUNCTION_NAME,
      ITEMS_PER_CHUNK, CHUNKER_CLUSTER_NAME, CHUNKER_SERVICE_NAME
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
    return new BulkSync({
      config, settings, runs,
      functions: new LambdaChunkerFunctions(new LambdaClient({ region: REGION })),
      chunkerService: new AwsChunkerService({ clusterName: CHUNKER_CLUSTER_NAME, serviceName: CHUNKER_SERVICE_NAME, region: REGION })
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
      warnings: this.warnings()
    };
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
    const request = BulkSyncRequest.parse(body);
    await this.checkParallelStart(request);
    await this.checkGuards(await this.latestRun(), request);
    const event = this.event(request, startedBy);
    return {
      landscape: settings.landscape,
      choices: request.choices(),
      event: { ...event, processingMetadata: { ...event.processingMetadata, processedAt: 'set when the run is started' } },
      settings: this.runSettings(request)
    };
  }

  async start(body: BulkSyncRequestBody, startedBy?: StartedBy): Promise<BulkSyncResult> {
    const { settings, functions } = this.deps;
    const request = BulkSyncRequest.parse(body);
    await this.checkParallelStart(request);
    await this.checkGuards(await this.latestRun(), request);
    const event = this.event(request, startedBy);
    await functions.invokeAsync(settings.chunkerFunctionName!, event);
    console.log(`Bulk sync started in ${settings.landscape}: ${JSON.stringify({ ...request.choices(), startedBy })}`);
    return {
      status: 'started',
      startedAt: event.processingMetadata!.processedAt!,
      landscape: settings.landscape,
      run: request.choices()
    };
  }

  /** The chunker event for a request, as the schedule (or, for a test, RunnerForSinglePerson) builds it */
  event(request: BulkSyncRequest, startedBy?: StartedBy): ApiChunkerEvent {
    const { config } = this.deps;
    const source = request.buid ? config.dataSource?.person : config.dataSource?.people;
    const baseUrl = (source as any)?.endpointConfig?.baseUrl as string | undefined;
    const fetchPath = (source as any)?.fetchPath as string | undefined;
    if (!baseUrl || !fetchPath) {
      throw new BulkSyncError('NOT_CONFIGURED',
        `The integration config has no ${request.buid ? 'person' : 'people'} endpoint (dataSource.${request.buid ? 'person' : 'people'}) to start a run from`);
    }
    return {
      baseUrl,
      fetchPath: request.buid ? `${fetchPath}?buid=${encodeURIComponent(request.buid)}` : fetchPath,
      populationType: request.population as ApiChunkerEvent['populationType'],
      ...ScheduledRun.previousStateFlags(request.bulkReset),
      iterationLimit: request.iterationLimit,
      offset: 0,
      // The chunker subscriber seeds this many messages, waits for the scale-in alarm, and sets the desired count
      ...(request.parallelStart > 0 ? { parallelStart: request.parallelStart } : {}),
      ...(startedBy ? { startedBy } : {}),
      processingMetadata: { processedAt: new Date(this.now).toISOString(), processorVersion: '1.0.0' }
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
      sourceSimulator: fixed(false, 'Mock landscapes only, not offered here yet'),
      sourceSimulatorMockTotalPopulation: unused('Only with the source simulator'),
      sourceSimulatorMockSimulatedDelaySeconds: unused('Only with the source simulator'),
      sourceSimulatorMockErrorRate: unused('Only with the source simulator'),
      mockTargetResetState: fixed(false, 'Mock landscapes only, not offered here yet'),
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
      ? [`${landscape} is a mock landscape: its target is the target simulator, but its source is the real source API, which a run reads as any landscape's does`]
      : [];
  }
}

