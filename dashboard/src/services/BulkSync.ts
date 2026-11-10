import { GetFunctionConfigurationCommand, InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { Config } from 'integration-huron-person';
import type { ApiChunkerEvent } from '../../../src/chunking/ChunkerSubscriber';
import type { RunReport } from '../../../src/reporting/RunReporting';
import { isMockLandscape } from '../../../src/Utils';
import {
  BulkSyncErrorCode, BulkSyncPopulation, BulkSyncRequestBody, BulkSyncResult, BulkSyncService, BulkSyncState,
  RunActivityState, RunReportingService
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
 * A validated request to start a run. A single person (a BUID) is always a delta sync: a full sync deactivates
 * everyone missing from its source, and a one-person run's source holds only that person.
 */
export class BulkSyncRequest {
  private constructor(
    readonly population: BulkSyncPopulation,
    readonly bulkReset: boolean,
    readonly trustPreviousStorage: boolean,
    readonly buid: string | undefined,
    readonly overrideStalled: boolean
  ) {}

  /** A BUID: U followed by digits */
  static readonly BUID = /^U\d+$/;

  /**
   * @param defaults - what an omitted bulkReset or trustPreviousStorage takes (the landscape's context)
   * @throws BulkSyncError INVALID_REQUEST naming what is wrong
   */
  static parse(body: BulkSyncRequestBody, defaults: { bulkReset: boolean; trustPreviousStorage: boolean }): BulkSyncRequest {
    const invalid = (message: string) => new BulkSyncError('INVALID_REQUEST', message);
    const { population, bulkReset = defaults.bulkReset, trustPreviousStorage = defaults.trustPreviousStorage, buid, overrideStalled = false } = body || {};
    if (population !== 'person-full' && population !== 'person-delta') {
      throw invalid("population must be 'person-full' or 'person-delta'");
    }
    for (const [name, value] of Object.entries({ bulkReset, trustPreviousStorage, overrideStalled })) {
      if (typeof value !== 'boolean') {
        throw invalid(`${name} must be true or false`);
      }
    }
    const person = typeof buid === 'string' && buid.trim() !== '' ? buid.trim().toUpperCase() : undefined;
    if (buid !== undefined && buid !== null && buid !== '' && typeof buid !== 'string') {
      throw invalid('buid must be a string');
    }
    if (person && !BulkSyncRequest.BUID.test(person)) {
      throw invalid(`buid must be a U followed by digits (got '${person}')`);
    }
    if (person && population === 'person-full') {
      throw invalid('A single-person run must be a delta sync: a full sync deactivates everyone missing from its source');
    }
    return new BulkSyncRequest(population, bulkReset as boolean, trustPreviousStorage as boolean, person, overrideStalled as boolean);
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
  chunkerFunctionName: string;
  processorSubscriberFunctionName: string;
  /** The context's ECS.chunkerService.chunksPerTask, as the schedule passes it */
  chunksPerTask: number;
  /** The form's defaults: the context's BULK_RESET and TRUST_PREVIOUS_STORAGE */
  bulkReset: boolean;
  trustPreviousStorage: boolean;
};

/**
 * Starts a sync run in this landscape the way its EventBridge schedule does: an asynchronous invocation of the
 * landscape's chunker subscriber Lambda with an ApiChunkerEvent. The Lambda resets the chunker's counters for a
 * run's first event (resetCountersForRunStart() in src/chunking/ChunkerSubscriber.ts), for the schedule's runs and
 * these alike. The source endpoint comes from the landscape's integration config: the people endpoint, or the
 * person endpoint with ?buid= for one person (as RunnerForSinglePerson does).
 *
 * Guards, until phase 6's roles decide who may start a run: no start in a production-toned landscape, none while
 * the newest run is underway (a stalled one needs overrideStalled), and none while the chunker or processor
 * subscriber is left in DRY_RUN (by the manual Runner), which would swallow the run.
 */
export class BulkSync implements BulkSyncService {

  constructor(private readonly deps: {
    config: Config;
    settings: BulkSyncSettings;
    runs: Pick<RunReportingService, 'listRunPage' | 'getRunReport'>;
    functions: ChunkerFunctions;
    now?: () => number;
  }) {}

  /** The real service, from the dashboard Lambda's environment */
  static fromEnvironment(config: Config, runs: Pick<RunReportingService, 'listRunPage' | 'getRunReport'>): BulkSync {
    const {
      LANDSCAPE, LANDSCAPE_TONE, REGION, CHUNKER_FUNCTION_NAME, PROCESSOR_SUBSCRIBER_FUNCTION_NAME,
      CHUNKS_PER_TASK = '0', BULK_RESET_DEFAULT = 'false', TRUST_PREVIOUS_STORAGE_DEFAULT = 'false'
    } = process.env;
    const missing = Object.entries({ LANDSCAPE, REGION, CHUNKER_FUNCTION_NAME, PROCESSOR_SUBSCRIBER_FUNCTION_NAME })
      .filter(([, value]) => !value).map(([name]) => name);
    if (missing.length > 0) {
      throw new BulkSyncError('NOT_CONFIGURED', `Starting runs isn't configured in this landscape's dashboard: missing ${missing.join(', ')} (redeploy its dashboard stack)`);
    }
    const settings: BulkSyncSettings = {
      landscape: LANDSCAPE!,
      landscapeTone: LANDSCAPE_TONE,
      chunkerFunctionName: CHUNKER_FUNCTION_NAME!,
      processorSubscriberFunctionName: PROCESSOR_SUBSCRIBER_FUNCTION_NAME!,
      chunksPerTask: Number(CHUNKS_PER_TASK) || 0,
      bulkReset: BULK_RESET_DEFAULT === 'true',
      trustPreviousStorage: TRUST_PREVIOUS_STORAGE_DEFAULT === 'true'
    };
    return new BulkSync({
      config, settings, runs,
      functions: new LambdaChunkerFunctions(new LambdaClient({ region: REGION })),
    });
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
      defaults: { population: 'person-delta', bulkReset: settings.bulkReset, trustPreviousStorage: settings.trustPreviousStorage },
      latestRun,
      ...(blocked ? { blocked } : {}),
      warnings: this.warnings()
    };
  }

  async start(body: BulkSyncRequestBody): Promise<BulkSyncResult> {
    const { settings, functions } = this.deps;
    const request = BulkSyncRequest.parse(body, settings);
    await this.checkGuards(await this.latestRun(), request);
    const event = this.event(request);
    await functions.invokeAsync(settings.chunkerFunctionName, event);
    console.log(`Bulk sync started in ${settings.landscape}: ${JSON.stringify({ ...request, chunksPerTask: settings.chunksPerTask })}`);
    return {
      status: 'started',
      startedAt: event.processingMetadata!.processedAt!,
      landscape: settings.landscape,
      run: {
        population: request.population,
        bulkReset: request.bulkReset,
        trustPreviousStorage: request.trustPreviousStorage,
        ...(request.buid ? { buid: request.buid } : {}),
        chunksPerTask: settings.chunksPerTask
      }
    };
  }

  /** The chunker event for a request, as the schedule (or, for one person, RunnerForSinglePerson) builds it */
  event(request: BulkSyncRequest): ApiChunkerEvent {
    const { config, settings } = this.deps;
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
      bulkReset: request.bulkReset,
      trustPreviousStorage: request.trustPreviousStorage,
      iterationLimit: settings.chunksPerTask,
      offset: 0,
      processingMetadata: { processedAt: new Date(this.now).toISOString(), processorVersion: '1.0.0' }
    };
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
    if (latestRun.kind === 'underway') {
      throw new BulkSyncError('RUN_UNDERWAY', `The run of ${latestRun.syncRunId} is still underway: wait for it to finish`);
    }
    if (latestRun.kind === 'stalled' && !request.overrideStalled) {
      throw new BulkSyncError('RUN_STALLED',
        `The run of ${latestRun.syncRunId} has stalled without finishing: confirm that it has stopped to start another`);
    }
    for (const [label, functionName] of [['chunker', settings.chunkerFunctionName], ['processor', settings.processorSubscriberFunctionName]]) {
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
