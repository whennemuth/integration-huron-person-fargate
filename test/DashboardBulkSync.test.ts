/**
 * Tests starting a sync run from the dashboard (dashboard/src/services/BulkSync.ts): the request it accepts, how
 * it reads the newest run (underway, stalled, finished), the guards, the chunker event it sends (the chunker
 * subscriber resets the counters for it), the review (plan) with every run setting, the real Lambda port against a
 * mocked client, and the routes (GET and POST /api/bulk-sync, POST /api/bulk-sync/plan) under mocks. The form's
 * defaults are the scheduled run's (src/chunking/ScheduledRun.ts; test/ScheduledRun.test.ts).
 */
import { GetFunctionConfigurationCommand, InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { mockClient } from 'aws-sdk-client-mock';
import { handler } from '../dashboard/src/handlers/DashboardHandler';
import { LambdaFunctionUrlEvent } from '../dashboard/src/handlers/DashboardTypes';
import { MOCK_SOURCE_CONFIG, MockBulkSyncService } from '../dashboard/src/handlers/DashboardMocks';
import { BulkSync, BulkSyncError, BulkSyncRequest, LambdaChunkerFunctions, RunActivity } from '../dashboard/src/services/BulkSync';
import { RunReport } from '../src/reporting/RunReporting';

const MIN = 60_000;
const NOW = Date.parse('2026-11-10T12:00:00.000Z');

/** A run report: summarized, failed, or unfinished with its progress */
const report = (syncRunId: string, params: { summary?: boolean; failed?: boolean; chunks?: number; lastChunkAt?: string } = {}): RunReport => {
  const { summary, failed, chunks = 0, lastChunkAt } = params;
  return {
    syncRunId,
    status: summary ? 'SUCCEEDED' : failed ? 'FAILED' : 'INCOMPLETE',
    ...(summary ? { summary: { syncRunId } as any } : {}),
    metadata: { createdAt: syncRunId },
    ...(!summary ? { progress: { chunksReported: chunks, lastChunkAt, results: {} } as any } : {})
  };
};

/** A run history whose newest run is the given report */
const runsEndingWith = (newest?: RunReport) => ({
  listRunPage: jest.fn(async () => ({ syncRunIds: newest ? [newest.syncRunId] : [], hasOlder: false })),
  getRunReport: jest.fn(async () => newest)
});

const finishedRun = report('2026-11-09T02:00:00.000Z', { summary: true });
const stalledRun = report('2026-11-06T17:12:48.824Z', { chunks: 734, lastChunkAt: '2026-11-07T04:59:44.820Z' });

describe('BulkSyncRequest', () => {
  it('takes omitted options from the scheduled run, 10 chunks per task, and one message', () => {
    const request = BulkSyncRequest.parse({ runType: 'full' });
    expect(request.choices()).toEqual({
      runType: 'full', population: 'person-full', bulkReset: false, trustPreviousStorage: true, iterationLimit: 10,
      parallelStart: 0, overrideStalled: false
    });
  });

  it('makes a full sync without deactivations and a test person-delta, and sets trustPreviousStorage from bulk reset', () => {
    expect(BulkSyncRequest.parse({ runType: 'full-no-deactivations', bulkReset: true, iterationLimit: 5, overrideStalled: true }).choices())
      .toMatchObject({ population: 'person-delta', bulkReset: true, trustPreviousStorage: false, iterationLimit: 5, overrideStalled: true });
    expect(BulkSyncRequest.parse({ runType: 'full', iterationLimit: 0 }).iterationLimit).toBe(0);
    expect(BulkSyncRequest.parse({ runType: 'full', iterationLimit: 50 }).iterationLimit).toBe(50);
    expect(BulkSyncRequest.parse({ runType: 'test', buid: ' u12345678 ' }).choices())
      .toMatchObject({ runType: 'test', population: 'person-delta', buid: 'U12345678' });
  });

  it.each([
    [{}, "runType must be one of 'full', 'full-no-deactivations', 'test'"],
    [{ runType: 'delta' }, "runType must be one of 'full', 'full-no-deactivations', 'test'"],
    [{ runType: 'person-full' }, "runType must be one of 'full', 'full-no-deactivations', 'test'"],
    [{ runType: 'full', bulkReset: 'yes' }, 'bulkReset must be true or false'],
    [{ runType: 'test' }, 'A test needs the BUID of the person to sync'],
    [{ runType: 'test', buid: '  ' }, 'A test needs the BUID of the person to sync'],
    [{ runType: 'full-no-deactivations', buid: 'U12345678' }, 'A BUID is only for a test'],
    [{ runType: 'full', buid: 'U12345678' }, 'A BUID is only for a test'],
    [{ runType: 'test', buid: 'B1234' }, "buid must be a U followed by digits (got 'B1234')"],
    [{ runType: 'test', buid: 42 }, 'buid must be a string'],
    [{ runType: 'full', iterationLimit: -1 }, 'iterationLimit must be a whole number from 0 (a single task) to 50'],
    [{ runType: 'full', iterationLimit: 51 }, 'iterationLimit must be a whole number from 0 (a single task) to 50'],
    [{ runType: 'full', iterationLimit: 1.5 }, 'iterationLimit must be a whole number from 0 (a single task) to 50'],
    [{ runType: 'full', iterationLimit: '2' }, 'iterationLimit must be a whole number from 0 (a single task) to 50'],
    [{ runType: 'full', parallelStart: 21 }, 'parallelStart must be a whole number from 0 to 20'],
    [{ runType: 'full', parallelStart: 4 }, "parallelStart must be 0: starting with several chunker messages isn't available until shared queue seeding is built"]
  ])('refuses %j as INVALID_REQUEST', (body, message) => {
    expect(() => BulkSyncRequest.parse(body as any)).toThrow(message);
    try { BulkSyncRequest.parse(body as any); } catch (error) {
      expect(error).toBeInstanceOf(BulkSyncError);
      expect((error as BulkSyncError).status).toBe(400);
    }
  });
});

describe('RunActivity', () => {
  const started = '2026-11-10T10:00:00.000Z';

  it('is none without runs, finished for a summarized or unrecorded run, failed for a failed one', () => {
    expect(RunActivity.of(undefined, NOW)).toEqual({ kind: 'none' });
    expect(RunActivity.of(finishedRun, NOW).kind).toBe('finished');
    expect(RunActivity.of({ syncRunId: started, status: 'UNKNOWN' }, NOW).kind).toBe('finished');
    expect(RunActivity.of(report(started, { failed: true }), NOW).kind).toBe('failed');
  });

  it('is underway while chunks keep finishing, and stalled by the dashboard rule (10 average gaps, at least 15 minutes)', () => {
    // 120 chunks in under 2 hours: under a minute apart, so 10 gaps is under 15 minutes and the floor applies
    const run = (lastChunkAt: string) => report(started, { chunks: 120, lastChunkAt });
    expect(RunActivity.of(run('2026-11-10T11:50:00.000Z'), NOW)).toEqual({ kind: 'underway', syncRunId: started, lastChunkAt: '2026-11-10T11:50:00.000Z' });
    expect(RunActivity.of(run('2026-11-10T11:44:00.000Z'), NOW).kind).toBe('stalled');
    // 6 chunks in 104 minutes: about 17 minutes apart, so stalled only after about 2 h 53 min without one
    const slow = (lastChunkAt: string) => report(started, { chunks: 6, lastChunkAt });
    expect(RunActivity.of(slow('2026-11-10T11:44:00.000Z'), NOW).kind).toBe('underway');
    expect(RunActivity.of(slow('2026-11-10T11:44:00.000Z'), NOW + 3 * 60 * MIN).kind).toBe('stalled');
    // Preview's Nov 6 run: 734 chunks, the last 3 days before
    expect(RunActivity.of(stalledRun, NOW).kind).toBe('stalled');
  });

  it('gives a run with no chunk finished an hour to produce one', () => {
    expect(RunActivity.of(report('2026-11-10T11:10:00.000Z'), NOW).kind).toBe('underway');
    expect(RunActivity.of(report('2026-11-10T10:50:00.000Z'), NOW).kind).toBe('stalled');
  });
});

describe('BulkSync', () => {
  const settings = {
    landscape: 'preview', region: 'us-east-2', stackId: 'huron-person', secretName: 'huron-person-config-preview',
    chunkerFunctionName: 'chunker-subscriber-preview', processorSubscriberFunctionName: 'processor-subscriber-preview',
    itemsPerChunk: 200
  };
  const service = (params: { newest?: RunReport; dryRun?: string[]; tone?: string; config?: any; missing?: string[] } = {}) => {
    const { newest = finishedRun, dryRun = [], tone, config = MOCK_SOURCE_CONFIG, missing } = params;
    const functions = {
      isDryRun: jest.fn(async (name: string) => dryRun.includes(name)),
      invokeAsync: jest.fn(async () => undefined)
    };
    const bulkSync = new BulkSync({
      config, settings: { ...settings, landscapeTone: tone, missing }, runs: runsEndingWith(newest), functions, now: () => NOW
    });
    return { bulkSync, functions };
  };

  it('starts a full sync as the schedule does: the people endpoint, stored hashes trusted; 10 chunks per task', async () => {
    const { bulkSync, functions } = service();
    const result = await bulkSync.start({ runType: 'full' });

    expect(functions.invokeAsync).toHaveBeenCalledWith('chunker-subscriber-preview', {
      baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: 'person-full',
      bulkReset: false, trustPreviousStorage: true, iterationLimit: 10, offset: 0,
      processingMetadata: { processedAt: '2026-11-10T12:00:00.000Z', processorVersion: '1.0.0' }
    });
    expect(result).toEqual({
      status: 'started', startedAt: '2026-11-10T12:00:00.000Z', landscape: 'preview',
      run: {
        runType: 'full', population: 'person-full', bulkReset: false, trustPreviousStorage: true, iterationLimit: 10,
        parallelStart: 0, overrideStalled: false
      }
    });
  });

  it('starts a test through the person endpoint, with a bulk reset and a custom iteration limit', async () => {
    const { bulkSync, functions } = service();
    const result = await bulkSync.start({ runType: 'test', buid: 'U12345678', bulkReset: true, iterationLimit: 4 });
    expect(functions.invokeAsync).toHaveBeenCalledWith('chunker-subscriber-preview', expect.objectContaining({
      fetchPath: '/api/person?buid=U12345678', populationType: 'person-delta', bulkReset: true, trustPreviousStorage: false,
      iterationLimit: 4
    }));
    expect(result.run.buid).toBe('U12345678');
  });

  it.each([
    ['a production landscape', { tone: 'production' }, 'PRODUCTION_LANDSCAPE', 403],
    ['a dashboard without the settings a start needs', { missing: ['CHUNKER_FUNCTION_NAME'] }, 'NOT_CONFIGURED', 503],
    ['a run underway', { newest: report('2026-11-10T10:00:00.000Z', { chunks: 60, lastChunkAt: '2026-11-10T11:55:00.000Z' }) }, 'RUN_UNDERWAY', 409],
    ['a stalled run, not overridden', { newest: stalledRun }, 'RUN_STALLED', 409],
    ['the chunker subscriber in DRY_RUN', { dryRun: ['chunker-subscriber-preview'] }, 'SUBSCRIBER_DRY_RUN', 409],
    ['the processor subscriber in DRY_RUN', { dryRun: ['processor-subscriber-preview'] }, 'SUBSCRIBER_DRY_RUN', 409],
    ['a config without the people endpoint', { config: { dataSource: {} } }, 'NOT_CONFIGURED', 503]
  ])('refuses to review or start with %s, touching nothing', async (_label, params, code, status) => {
    const { bulkSync, functions } = service(params as any);
    for (const failure of [bulkSync.start({ runType: 'full' }), bulkSync.plan({ runType: 'full' })]) {
      await expect(failure).rejects.toBeInstanceOf(BulkSyncError);
      await expect(failure).rejects.toMatchObject({ code, status });
    }
    expect(functions.invokeAsync).not.toHaveBeenCalled();
  });

  it('starts after a stalled run when told it has stopped, and after a failed one', async () => {
    await expect(service({ newest: stalledRun }).bulkSync.start({ runType: 'full', overrideStalled: true })).resolves.toMatchObject({ status: 'started' });
    await expect(service({ newest: report('2026-11-09T02:00:00.000Z', { failed: true }) }).bulkSync.start({ runType: 'full' }))
      .resolves.toMatchObject({ status: 'started' });
    await expect(service({ newest: undefined as any }).bulkSync.start({ runType: 'full' })).resolves.toMatchObject({ status: 'started' });
  });

  it("describes the defaults, the newest run, and why a start would be refused now", async () => {
    expect(await service().bulkSync.describe()).toEqual({
      landscape: 'preview',
      defaults: { runType: 'full', bulkReset: false, iterationLimit: 10, parallelStart: 0 },
      iterationLimit: { max: 50 },
      parallelStart: { max: 20, enabled: false },
      latestRun: { kind: 'finished', syncRunId: finishedRun.syncRunId },
      warnings: []
    });
    const stalled = await service({ newest: stalledRun }).bulkSync.describe();
    expect(stalled.latestRun.kind).toBe('stalled');
    expect(stalled.blocked).toEqual({ code: 'RUN_STALLED', message: expect.stringContaining('has stalled without finishing') });
  });

  it("still describes the latest run when a start isn't configured, refusing starts", async () => {
    const state = await service({ missing: ['CHUNKER_FUNCTION_NAME', 'PROCESSOR_SUBSCRIBER_FUNCTION_NAME'] }).bulkSync.describe();
    expect(state.latestRun.kind).toBe('finished');
    expect(state.blocked).toEqual({
      code: 'NOT_CONFIGURED',
      message: expect.stringContaining('missing CHUNKER_FUNCTION_NAME, PROCESSOR_SUBSCRIBER_FUNCTION_NAME')
    });
  });

  describe('who starts it', () => {
    it("is the signed-in user's buPrincipal (array-valued, as the IdP sends it), or no one in a public dashboard", () => {
      expect(BulkSync.startedBy({ buPrincipal: ['jdoe'], eduPersonPrincipalName: ['jdoe@bu.edu'] })).toEqual({ by: 'dashboard', principal: 'jdoe' });
      expect(BulkSync.startedBy({ buPrincipal: 'jdoe' })).toEqual({ by: 'dashboard', principal: 'jdoe' });
      expect(BulkSync.startedBy({ eduPersonPrincipalName: ['jdoe@bu.edu'] })).toEqual({ by: 'dashboard' });
      expect(BulkSync.startedBy(undefined)).toEqual({ by: 'dashboard' });
    });

    it('goes on the event a start sends, and the one its review shows', async () => {
      const { bulkSync, functions } = service();
      const startedBy = { by: 'dashboard' as const, principal: 'jdoe' };
      expect((await bulkSync.plan({ runType: 'full' }, startedBy)).event).toMatchObject({ startedBy });
      await bulkSync.start({ runType: 'full' }, startedBy);
      expect(functions.invokeAsync).toHaveBeenCalledWith('chunker-subscriber-preview', expect.objectContaining({ startedBy }));
    });
  });

  describe('plan', () => {
    it('returns the event a start would send, without starting anything', async () => {
      const { bulkSync, functions } = service();
      const plan = await bulkSync.plan({ runType: 'test', buid: 'U0000042', bulkReset: true });
      expect(functions.invokeAsync).not.toHaveBeenCalled();
      expect(plan.landscape).toBe('preview');
      expect(plan.choices).toMatchObject({ runType: 'test', buid: 'U0000042', bulkReset: true, trustPreviousStorage: false });
      expect(plan.event).toEqual({
        baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/person?buid=U0000042', populationType: 'person-delta',
        bulkReset: true, trustPreviousStorage: false, iterationLimit: 10, offset: 0,
        processingMetadata: { processedAt: 'set when the run is started', processorVersion: '1.0.0' }
      });
    });

    it("lists every setting the manual Runner prints, then the deployed ones, each with where it's from", async () => {
      const { settings: rows } = await service().bulkSync.plan({ runType: 'full' });
      const byName = Object.fromEntries(rows.map(row => [row.name, row]));
      expect(rows.map(row => row.name)).toEqual([
        'secretArn', 'configPath', 'queueUrl', 'populationType', 'populationScope', 'iterationLimit', 'buid', 'region',
        'stackId', 'landscape', 'bulkReset', 'trustPreviousStorage', 'skipAtomicCounterReset', 'messagesToPrepopulate',
        'desiredCount', 'clusterName', 'serviceName', 'messagingOnly', 'chunkingOnly', 'sourceSimulator',
        'sourceSimulatorMockTotalPopulation', 'sourceSimulatorMockSimulatedDelaySeconds', 'sourceSimulatorMockErrorRate',
        'mockTargetResetState', 'personRecordProcessorAddons', 'itemsPerChunk', 'chunkerFunction'
      ]);
      expect(byName.populationType).toMatchObject({ value: 'person-full', source: 'form' });
      expect(byName.trustPreviousStorage).toMatchObject({ value: true, source: 'form' });
      expect(byName.secretArn).toMatchObject({ value: 'huron-person-config-preview', source: 'landscape' });
      expect(byName.stackId).toMatchObject({ value: 'huron-person', source: 'landscape' });
      expect(byName.messagesToPrepopulate).toMatchObject({ value: '0', source: 'form' });
      expect(byName.desiredCount).toMatchObject({ value: 0, source: 'form' });
      expect(byName.skipAtomicCounterReset).toMatchObject({ value: false, source: 'fixed' });
      expect(byName.chunkingOnly).toMatchObject({ value: false, source: 'fixed' });
      expect(byName.personRecordProcessorAddons).toMatchObject({ value: null, source: 'fixed' });
      expect(byName.clusterName).toMatchObject({ value: null, source: 'unused' });
      expect(byName.itemsPerChunk).toMatchObject({ value: 200, source: 'landscape' });
      expect(byName.chunkerFunction).toMatchObject({ value: 'chunker-subscriber-preview', source: 'landscape' });
      expect(rows.every(row => ['form', 'landscape', 'fixed', 'unused'].includes(row.source))).toBe(true);
    });
  });

  it("warns that a mock landscape's run reads the real source", async () => {
    const mock = new MockBulkSyncService({ runs: runsEndingWith(finishedRun), settings: { landscape: 'mock' } });
    expect((await mock.describe()).warnings).toEqual([expect.stringContaining('mock is a mock landscape')]);
  });

  describe('from the environment', () => {
    const saved = { ...process.env };
    afterEach(() => { process.env = { ...saved }; });

    it("refuses starts, without failing to describe, until the dashboard stack has been deployed with its settings", async () => {
      for (const name of ['LANDSCAPE', 'REGION', 'CHUNKER_FUNCTION_NAME', 'PROCESSOR_SUBSCRIBER_FUNCTION_NAME']) {
        delete process.env[name];
      }
      process.env.LANDSCAPE = 'preview';
      const bulkSync = BulkSync.fromEnvironment(MOCK_SOURCE_CONFIG, runsEndingWith(finishedRun));
      expect((await bulkSync.describe()).blocked).toEqual({
        code: 'NOT_CONFIGURED', message: expect.stringContaining('missing REGION, CHUNKER_FUNCTION_NAME, PROCESSOR_SUBSCRIBER_FUNCTION_NAME')
      });
      await expect(bulkSync.start({ runType: 'full' })).rejects.toMatchObject({ code: 'NOT_CONFIGURED' });
    });

    it('reads its settings', async () => {
      Object.assign(process.env, {
        LANDSCAPE: 'preview', REGION: 'us-east-2', LANDSCAPE_TONE: 'test', PIPELINE_STACK_ID: 'huron-person',
        INTEGRATION_SECRETS_ARN: 'huron-person-config-preview',
        CHUNKER_FUNCTION_NAME: 'chunker-subscriber-preview', PROCESSOR_SUBSCRIBER_FUNCTION_NAME: 'processor-subscriber-preview',
        ITEMS_PER_CHUNK: '150'
      });
      const bulkSync = BulkSync.fromEnvironment(MOCK_SOURCE_CONFIG, runsEndingWith(finishedRun));
      const request = BulkSyncRequest.parse({ runType: 'full' });
      expect(bulkSync.event(request)).toMatchObject({ iterationLimit: 10, bulkReset: false, trustPreviousStorage: true });
      const byName = Object.fromEntries(bulkSync.runSettings(request).map(row => [row.name, row.value]));
      expect(byName).toMatchObject({
        stackId: 'huron-person', secretArn: 'huron-person-config-preview', region: 'us-east-2', itemsPerChunk: 150
      });
    });
  });
});

describe('LambdaChunkerFunctions', () => {
  const lambda = mockClient(LambdaClient);
  beforeEach(() => lambda.reset());

  it('invokes the chunker asynchronously with the event as its payload', async () => {
    lambda.on(InvokeCommand).resolves({ StatusCode: 202 });
    await new LambdaChunkerFunctions(new LambdaClient({})).invokeAsync('chunker-subscriber-preview', { baseUrl: 'b', fetchPath: 'f' } as any);
    const { input } = lambda.commandCalls(InvokeCommand)[0].args[0];
    expect(input).toMatchObject({ FunctionName: 'chunker-subscriber-preview', InvocationType: 'Event' });
    expect(JSON.parse(Buffer.from(input.Payload as Uint8Array).toString())).toEqual({ baseUrl: 'b', fetchPath: 'f' });
  });

  it("fails when the chunker doesn't accept the invocation", async () => {
    lambda.on(InvokeCommand).resolves({ StatusCode: 500, FunctionError: 'Unhandled' });
    await expect(new LambdaChunkerFunctions(new LambdaClient({})).invokeAsync('chunker-subscriber-preview', {} as any))
      .rejects.toThrow("didn't accept the run (status 500, Unhandled)");
  });

  it("reads a subscriber's DRY_RUN switch", async () => {
    lambda.on(GetFunctionConfigurationCommand, { FunctionName: 'on' }).resolves({ Environment: { Variables: { DRY_RUN: 'TRUE' } } });
    lambda.on(GetFunctionConfigurationCommand, { FunctionName: 'off' }).resolves({ Environment: { Variables: {} } });
    const functions = new LambdaChunkerFunctions(new LambdaClient({}));
    expect(await functions.isDryRun('on')).toBe(true);
    expect(await functions.isDryRun('off')).toBe(false);
  });
});

describe('the bulk sync routes, under mocks', () => {
  const request = async (method: string, body?: unknown, path = '/api/bulk-sync') => {
    const result = await handler({
      rawPath: path, headers: {}, body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
      requestContext: { http: { method, path } }, isBase64Encoded: false
    } as unknown as LambdaFunctionUrlEvent);
    return { status: result.statusCode, body: result.body ? JSON.parse(`${result.body}`) : undefined };
  };

  beforeAll(() => { process.env.LOCALHOST_WITH_MOCKS = 'true'; });
  afterAll(() => { delete process.env.LOCALHOST_WITH_MOCKS; });
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  it('describes a start (GET), reviews one (POST plan, 200), and starts one (POST, 202)', async () => {
    const state = await request('GET');
    expect(state.status).toBe(200);
    expect(state.body).toMatchObject({ landscape: 'local', defaults: { runType: 'full', bulkReset: false }, latestRun: { kind: 'finished' } });

    const plan = await request('POST', { runType: 'test', buid: 'U0000042' }, '/api/bulk-sync/plan');
    expect(plan).toMatchObject({ status: 200, body: { choices: { runType: 'test', buid: 'U0000042' }, event: { fetchPath: '/api/person?buid=U0000042' } } });
    expect(plan.body.settings.length).toBeGreaterThan(20);

    // No one is signed in here: the review's event says it came from the dashboard
    expect(plan.body.event.startedBy).toEqual({ by: 'dashboard' });

    const started = await request('POST', { runType: 'test', buid: 'U0000042' });
    expect(started).toMatchObject({ status: 202, body: { status: 'started', run: { population: 'person-delta', buid: 'U0000042' } } });
  });

  it('answers a refused review or start with its status and code', async () => {
    expect(await request('POST', { runType: 'full', buid: 'U0000042' }))
      .toMatchObject({ status: 400, body: { code: 'INVALID_REQUEST' } });
    expect(await request('POST', { runType: 'full', parallelStart: 3 }, '/api/bulk-sync/plan'))
      .toMatchObject({ status: 400, body: { code: 'INVALID_REQUEST' } });
    expect(await request('POST', 'not json')).toMatchObject({ status: 400, body: { code: 'INVALID_REQUEST' } });
    expect((await request('PUT', {})).status).toBe(405);
    const getPlan = await handler({
      rawPath: '/api/bulk-sync/plan', headers: {}, requestContext: { http: { method: 'GET', path: '/api/bulk-sync/plan' } }, isBase64Encoded: false
    } as unknown as LambdaFunctionUrlEvent);
    expect(getPlan.statusCode).toBe(405);
    expect(getPlan.headers).toMatchObject({ Allow: 'POST' });
  });
});
