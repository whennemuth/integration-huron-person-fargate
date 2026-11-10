/**
 * Tests starting a sync run from the dashboard (dashboard/src/services/BulkSync.ts): the request it accepts, how
 * it reads the newest run (underway, stalled, finished), the guards, the chunker event it sends (the chunker
 * subscriber resets the counters for it), the real Lambda port against a mocked client, and the route (GET and POST
 * /api/bulk-sync) under mocks.
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
const DEFAULTS = { bulkReset: false, trustPreviousStorage: true };

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

describe('BulkSyncRequest', () => {
  it("accepts a full or delta sync, taking omitted options from the landscape's defaults", () => {
    expect(BulkSyncRequest.parse({ population: 'person-full' }, DEFAULTS)).toMatchObject({
      population: 'person-full', bulkReset: false, trustPreviousStorage: true, buid: undefined, overrideStalled: false
    });
    expect(BulkSyncRequest.parse({ population: 'person-delta', bulkReset: true, trustPreviousStorage: false, overrideStalled: true }, DEFAULTS))
      .toMatchObject({ bulkReset: true, trustPreviousStorage: false, overrideStalled: true });
  });

  it('takes one person by BUID, always as a delta sync', () => {
    expect(BulkSyncRequest.parse({ population: 'person-delta', buid: ' u12345678 ' }, DEFAULTS).buid).toBe('U12345678');
    expect(BulkSyncRequest.parse({ population: 'person-delta', buid: '' }, DEFAULTS).buid).toBeUndefined();
    expect(() => BulkSyncRequest.parse({ population: 'person-full', buid: 'U12345678' }, DEFAULTS))
      .toThrow('A single-person run must be a delta sync: a full sync deactivates everyone missing from its source');
  });

  it.each([
    [{}, "population must be 'person-full' or 'person-delta'"],
    [{ population: 'everyone' }, "population must be 'person-full' or 'person-delta'"],
    [{ population: 'person-full', bulkReset: 'yes' }, 'bulkReset must be true or false'],
    [{ population: 'person-delta', buid: 'B1234' }, "buid must be a U followed by digits (got 'B1234')"],
    [{ population: 'person-delta', buid: 42 }, 'buid must be a string']
  ])('refuses %j as INVALID_REQUEST', (body, message) => {
    expect(() => BulkSyncRequest.parse(body as any, DEFAULTS)).toThrow(message);
    try { BulkSyncRequest.parse(body as any, DEFAULTS); } catch (error) {
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
    expect(RunActivity.of(report('2026-11-06T17:12:48.824Z', { chunks: 734, lastChunkAt: '2026-11-07T04:59:44.820Z' }), NOW).kind).toBe('stalled');
  });

  it('gives a run with no chunk finished an hour to produce one', () => {
    expect(RunActivity.of(report('2026-11-10T11:10:00.000Z'), NOW).kind).toBe('underway');
    expect(RunActivity.of(report('2026-11-10T10:50:00.000Z'), NOW).kind).toBe('stalled');
  });
});

describe('BulkSync', () => {
  const settings = {
    landscape: 'preview', chunkerFunctionName: 'chunker-subscriber-preview', processorSubscriberFunctionName: 'processor-subscriber-preview',
    chunksPerTask: 0, bulkReset: true, trustPreviousStorage: false
  };
  const service = (params: { newest?: RunReport; dryRun?: string[]; tone?: string; config?: any } = {}) => {
    const { newest = finishedRun, dryRun = [], tone, config = MOCK_SOURCE_CONFIG } = params;
    const functions = {
      isDryRun: jest.fn(async (name: string) => dryRun.includes(name)),
      invokeAsync: jest.fn(async () => undefined)
    };
    const bulkSync = new BulkSync({
      config, settings: { ...settings, landscapeTone: tone }, runs: runsEndingWith(newest), functions, now: () => NOW
    });
    return { bulkSync, functions };
  };

  it("starts a full sync as the schedule does: the people endpoint and the context's chunksPerTask", async () => {
    const { bulkSync, functions } = service();
    const result = await bulkSync.start({ population: 'person-full' });

    expect(functions.invokeAsync).toHaveBeenCalledWith('chunker-subscriber-preview', {
      baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: 'person-full',
      bulkReset: true, trustPreviousStorage: false, iterationLimit: 0, offset: 0,
      processingMetadata: { processedAt: '2026-11-10T12:00:00.000Z', processorVersion: '1.0.0' }
    });
    expect(result).toEqual({
      status: 'started', startedAt: '2026-11-10T12:00:00.000Z', landscape: 'preview',
      run: { population: 'person-full', bulkReset: true, trustPreviousStorage: false, chunksPerTask: 0 }
    });
  });

  it('starts one person through the person endpoint', async () => {
    const { bulkSync, functions } = service();
    const result = await bulkSync.start({ population: 'person-delta', buid: 'U12345678', bulkReset: true });
    expect(functions.invokeAsync).toHaveBeenCalledWith('chunker-subscriber-preview', expect.objectContaining({
      fetchPath: '/api/person?buid=U12345678', populationType: 'person-delta'
    }));
    expect(result.run.buid).toBe('U12345678');
  });

  it.each([
    ['a production landscape', { tone: 'production' }, 'PRODUCTION_LANDSCAPE', 403],
    ['a run underway', { newest: report('2026-11-10T10:00:00.000Z', { chunks: 60, lastChunkAt: '2026-11-10T11:55:00.000Z' }) }, 'RUN_UNDERWAY', 409],
    ['a stalled run, not overridden', { newest: report('2026-11-06T17:12:48.824Z', { chunks: 734, lastChunkAt: '2026-11-07T04:59:44.820Z' }) }, 'RUN_STALLED', 409],
    ['the chunker subscriber in DRY_RUN', { dryRun: ['chunker-subscriber-preview'] }, 'SUBSCRIBER_DRY_RUN', 409],
    ['the processor subscriber in DRY_RUN', { dryRun: ['processor-subscriber-preview'] }, 'SUBSCRIBER_DRY_RUN', 409],
    ['a config without the people endpoint', { config: { dataSource: {} } }, 'NOT_CONFIGURED', 503]
  ])('refuses to start with %s, touching nothing', async (_label, params, code, status) => {
    const { bulkSync, functions } = service(params as any);
    const failure = bulkSync.start({ population: 'person-full' });
    await expect(failure).rejects.toBeInstanceOf(BulkSyncError);
    await expect(failure).rejects.toMatchObject({ code, status });
    expect(functions.invokeAsync).not.toHaveBeenCalled();
  });

  it('starts after a stalled run when told it has stopped, and after a failed one', async () => {
    const stalled = report('2026-11-06T17:12:48.824Z', { chunks: 734, lastChunkAt: '2026-11-07T04:59:44.820Z' });
    await expect(service({ newest: stalled }).bulkSync.start({ population: 'person-full', overrideStalled: true })).resolves.toMatchObject({ status: 'started' });
    await expect(service({ newest: report('2026-11-09T02:00:00.000Z', { failed: true }) }).bulkSync.start({ population: 'person-full' }))
      .resolves.toMatchObject({ status: 'started' });
    await expect(service({ newest: undefined as any }).bulkSync.start({ population: 'person-full' })).resolves.toMatchObject({ status: 'started' });
  });

  it("describes the defaults, the newest run, and why a start would be refused now", async () => {
    expect(await service().bulkSync.describe()).toEqual({
      landscape: 'preview',
      defaults: { population: 'person-delta', bulkReset: true, trustPreviousStorage: false },
      latestRun: { kind: 'finished', syncRunId: finishedRun.syncRunId },
      warnings: []
    });
    const stalled = await service({ newest: report('2026-11-06T17:12:48.824Z', { chunks: 734, lastChunkAt: '2026-11-07T04:59:44.820Z' }) }).bulkSync.describe();
    expect(stalled.latestRun.kind).toBe('stalled');
    expect(stalled.blocked).toEqual({ code: 'RUN_STALLED', message: expect.stringContaining('has stalled without finishing') });
  });

  it("warns that a mock landscape's run reads the real source", async () => {
    const mock = new MockBulkSyncService({ runs: runsEndingWith(finishedRun), settings: { landscape: 'mock' } });
    expect((await mock.describe()).warnings).toEqual([expect.stringContaining('mock is a mock landscape')]);
  });

  describe('from the environment', () => {
    const saved = { ...process.env };
    afterEach(() => { process.env = { ...saved }; });

    it("isn't configured until the dashboard stack has been deployed with its settings", () => {
      for (const name of ['LANDSCAPE', 'REGION', 'CHUNKER_FUNCTION_NAME', 'PROCESSOR_SUBSCRIBER_FUNCTION_NAME']) {
        delete process.env[name];
      }
      process.env.LANDSCAPE = 'preview';
      expect(() => BulkSync.fromEnvironment(MOCK_SOURCE_CONFIG, runsEndingWith())).toThrow(
        expect.objectContaining({ code: 'NOT_CONFIGURED', message: expect.stringContaining('missing REGION, CHUNKER_FUNCTION_NAME, PROCESSOR_SUBSCRIBER_FUNCTION_NAME') })
      );
    });

    it('reads its settings and defaults', async () => {
      Object.assign(process.env, {
        LANDSCAPE: 'preview', REGION: 'us-east-2', LANDSCAPE_TONE: 'test',
        CHUNKER_FUNCTION_NAME: 'chunker-subscriber-preview', PROCESSOR_SUBSCRIBER_FUNCTION_NAME: 'processor-subscriber-preview',
        CHUNKS_PER_TASK: '2', BULK_RESET_DEFAULT: 'true', TRUST_PREVIOUS_STORAGE_DEFAULT: 'false'
      });
      const bulkSync = BulkSync.fromEnvironment(MOCK_SOURCE_CONFIG, runsEndingWith(finishedRun));
      expect(bulkSync.event(BulkSyncRequest.parse({ population: 'person-full' }, { bulkReset: true, trustPreviousStorage: false })))
        .toMatchObject({ iterationLimit: 2, bulkReset: true });
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

describe('the bulk sync route, under mocks', () => {
  const request = async (method: string, body?: unknown) => {
    const result = await handler({
      rawPath: '/api/bulk-sync', headers: {}, body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
      requestContext: { http: { method, path: '/api/bulk-sync' } }, isBase64Encoded: false
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

  it('describes a start (GET), and starts one (POST, 202)', async () => {
    const state = await request('GET');
    expect(state.status).toBe(200);
    expect(state.body).toMatchObject({ landscape: 'local', defaults: { population: 'person-delta' }, latestRun: { kind: 'finished' } });

    const started = await request('POST', { population: 'person-delta', buid: 'U0000042' });
    expect(started).toMatchObject({ status: 202, body: { status: 'started', run: { population: 'person-delta', buid: 'U0000042' } } });
  });

  it('answers a refused start with its status and code', async () => {
    expect(await request('POST', { population: 'person-full', buid: 'U0000042' }))
      .toMatchObject({ status: 400, body: { code: 'INVALID_REQUEST' } });
    expect(await request('POST', 'not json')).toMatchObject({ status: 400, body: { code: 'INVALID_REQUEST' } });
    expect((await request('PUT', {})).status).toBe(405);
  });
});
