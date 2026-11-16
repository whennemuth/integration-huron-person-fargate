/**
 * Tests starting a sync run from the dashboard (dashboard/src/services/BulkSync.ts): the request it accepts, how
 * it reads the newest run (underway, stalled, finished), the guards, the chunker event it sends (the chunker
 * subscriber resets the counters for it), the review (plan) with every run setting, the real Lambda port against a
 * mocked client, and the routes (GET and POST /api/bulk-sync, POST /api/bulk-sync/plan) offline. The form's
 * defaults are the scheduled run's (src/chunking/ScheduledRun.ts; test/ScheduledRun.test.ts).
 */
import { GetFunctionConfigurationCommand, GetFunctionUrlConfigCommand, InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { mockClient } from 'aws-sdk-client-mock';
import { handler } from '../dashboard/src/handlers/DashboardHandler';
import { LambdaFunctionUrlEvent } from '../dashboard/src/handlers/DashboardTypes';
import { OFFLINE_SOURCE_CONFIG, OfflineBulkSyncService } from '../dashboard/src/handlers/OfflineServices';
import {
  AwsSourceSimulatorService, BulkSync, BulkSyncError, BulkSyncRequest, LambdaChunkerFunctions, RunActivity
} from '../dashboard/src/services/BulkSync';
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

  it('makes a test a single chunker task, whatever chunks per task were sent', () => {
    // A chained task would fetch the same person again; with bulk reset it got ahead of the first (preview, 2026-11-12)
    expect(BulkSyncRequest.parse({ runType: 'test', buid: 'U12345678' }).iterationLimit).toBe(0);
    expect(BulkSyncRequest.parse({ runType: 'test', buid: 'U12345678', iterationLimit: 10, bulkReset: true }).choices())
      .toMatchObject({ iterationLimit: 0, bulkReset: true });
    // An invalid value is still refused, as for any run type
    expect(() => BulkSyncRequest.parse({ runType: 'test', buid: 'U12345678', iterationLimit: 51 })).toThrow('iterationLimit must be');
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
    [{ runType: 'test', buid: 'U12345678', parallelStart: 4 }, 'A test starts with one chunker task: parallelStart must be 0'],
    [{ runType: 'full', iterationLimit: 0, parallelStart: 4 }, "A single task can't be started in parallel: parallelStart needs chunks per task above 0"]
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
  const service = (params: {
    newest?: RunReport; dryRun?: string[]; tone?: string; config?: any; missing?: string[]; serviceMax?: number | 'none'
  } = {}) => {
    const { newest = finishedRun, dryRun = [], tone, config = OFFLINE_SOURCE_CONFIG, missing, serviceMax } = params;
    const functions = {
      isDryRun: jest.fn(async (name: string) => dryRun.includes(name)),
      invokeAsync: jest.fn(async () => undefined)
    };
    const chunkerService = serviceMax === undefined ? undefined
      : { maxTasks: jest.fn(async () => (serviceMax === 'none' ? undefined : serviceMax)) };
    const bulkSync = new BulkSync({
      config, settings: { ...settings, landscapeTone: tone, missing, chunkerClusterName: 'huron-person-cluster-preview', chunkerServiceName: 'Chunker' },
      runs: runsEndingWith(newest), functions, chunkerService, now: () => NOW
    });
    return { bulkSync, functions };
  };

  describe('a parallel start', () => {
    it("offers up to 20 tasks, lowered to the chunker service's maximum when it can be read", async () => {
      expect((await service({ serviceMax: 15 }).bulkSync.describe()).parallelStart).toEqual({ max: 15, enabled: true });
      expect((await service({ serviceMax: 40 }).bulkSync.describe()).parallelStart).toEqual({ max: 20, enabled: true });
      expect((await service({ serviceMax: 'none' }).bulkSync.describe()).parallelStart).toEqual({ max: 20, enabled: true });
      expect((await service().bulkSync.describe()).parallelStart).toEqual({ max: 20, enabled: true });
    });

    it('sends parallelStart in the event, so the chunker subscriber seeds, waits and scales', async () => {
      const { bulkSync, functions } = service({ serviceMax: 15 });
      await bulkSync.start({ runType: 'full-no-deactivations', parallelStart: 3 });
      expect(functions.invokeAsync).toHaveBeenCalledWith('chunker-subscriber-preview', expect.objectContaining({ iterationLimit: 10, parallelStart: 3 }));
    });

    it('sends no parallelStart for a start with one message', async () => {
      const { bulkSync, functions } = service({ serviceMax: 15 });
      await bulkSync.start({ runType: 'full' });
      expect((functions.invokeAsync as jest.Mock).mock.calls[0][1]).not.toHaveProperty('parallelStart');
    });

    it("refuses more tasks than the chunker service may run, before invoking anything", async () => {
      const { bulkSync, functions } = service({ serviceMax: 2 });
      for (const attempt of [bulkSync.start({ runType: 'full', parallelStart: 3 }), bulkSync.plan({ runType: 'full', parallelStart: 3 })]) {
        await expect(attempt).rejects.toMatchObject({ code: 'INVALID_REQUEST', message: expect.stringContaining('more than the 2 chunker tasks') });
      }
      expect(functions.invokeAsync).not.toHaveBeenCalled();
    });

    it('shows the review the messages, the task count and the chunker service it scales', async () => {
      const { settings: rows, event } = await service({ serviceMax: 15 }).bulkSync.plan({ runType: 'full', parallelStart: 3 });
      expect(event).toMatchObject({ parallelStart: 3 });
      const byName = Object.fromEntries(rows.map(row => [row.name, row]));
      expect(byName.messagesToPrepopulate).toMatchObject({ value: '3', source: 'form', note: expect.stringContaining('waits for the scale-in alarm') });
      expect(byName.desiredCount).toMatchObject({ value: 3, source: 'form' });
      expect(byName.clusterName).toMatchObject({ value: 'huron-person-cluster-preview', source: 'landscape' });
      expect(byName.serviceName).toMatchObject({ value: 'Chunker', source: 'landscape' });

      const single = Object.fromEntries((await service().bulkSync.plan({ runType: 'full' })).settings.map(row => [row.name, row]));
      expect(single.clusterName).toMatchObject({ source: 'unused' });
      expect(single.messagesToPrepopulate.note).toContain('one message');
    });
  });

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

  it('starts a test through the person endpoint, with a bulk reset, as a single chunker task whatever the form sent', async () => {
    const { bulkSync, functions } = service();
    const result = await bulkSync.start({ runType: 'test', buid: 'U12345678', bulkReset: true, iterationLimit: 4 });
    // A next chunker task would fetch the same person again (the person endpoint ignores offsets), so none is chained
    expect(functions.invokeAsync).toHaveBeenCalledWith('chunker-subscriber-preview', expect.objectContaining({
      fetchPath: '/api/person?buid=U12345678', populationType: 'person-delta', bulkReset: true, trustPreviousStorage: false,
      iterationLimit: 0
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
      parallelStart: { max: 20, enabled: true },
      latestRun: { kind: 'finished', syncRunId: finishedRun.syncRunId },
      warnings: [],
      itemsPerChunk: 200
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
        bulkReset: true, trustPreviousStorage: false, iterationLimit: 0, offset: 0,
        processingMetadata: { processedAt: 'set when the run is started', processorVersion: '1.0.0' }
      });
      // The review says why a test is one task
      const iterationLimit = plan.settings.find(row => row.name === 'iterationLimit');
      expect(iterationLimit).toMatchObject({ value: 0, source: 'form', note: 'A test is a single chunker task: a second task would fetch the same person again' });
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

  it('plays the landscape the launcher names (LANDSCAPE), else local, in its listing and capabilities too', async () => {
    const { OfflineLandscapeListingService, OfflineRunReportingService } = await import('../dashboard/src/handlers/OfflineServices');
    const saved = process.env.LANDSCAPE;
    try {
      delete process.env.LANDSCAPE;
      expect((await new OfflineBulkSyncService({ runs: runsEndingWith(finishedRun) }).describe()).landscape).toBe('local');
      expect((await new OfflineLandscapeListingService().getLandscapes()).current).toBe('local');
      process.env.LANDSCAPE = 'mock';
      const state = await new OfflineBulkSyncService({ runs: runsEndingWith(finishedRun) }).describe();
      expect(state.landscape).toBe('mock');
      expect(state.mock).toBeDefined();
      const listing = await new OfflineLandscapeListingService().getLandscapes();
      expect(listing.current).toBe('mock');
      expect(listing.landscapes.filter(entry => entry.current).map(entry => entry.landscape)).toEqual(['mock']);
      expect((await new OfflineRunReportingService().getCapabilities()).landscape).toBe('mock');
    } finally {
      if (saved === undefined) delete process.env.LANDSCAPE; else process.env.LANDSCAPE = saved;
    }
  });

  it("warns that a mock landscape's run reads the real source", async () => {
    const mock = new OfflineBulkSyncService({ runs: runsEndingWith(finishedRun), settings: { landscape: 'mock' } });
    expect((await mock.describe()).warnings).toEqual([expect.stringContaining('mock is a mock landscape')]);
  });

  describe("a mock landscape's reset on its own (Overview)", () => {
    const record = (status: string, startedAt = '2026-11-10T11:55:00.000Z') => ({ status, via: 'overview', startedAt } as any);
    const mockSync = (params: { landscape?: string; newest?: RunReport; last?: any; dryRun?: string[]; readFails?: boolean } = {}) => {
      const { landscape = 'mock', newest = finishedRun, last, dryRun = [], readFails } = params;
      const functions = { isDryRun: jest.fn(async (name: string) => dryRun.includes(name)), invokeAsync: jest.fn(async () => undefined) };
      const bulkSync = new BulkSync({
        config: OFFLINE_SOURCE_CONFIG,
        settings: { ...settings, landscape, chunkerFunctionName: `chunker-subscriber-${landscape}`, processorSubscriberFunctionName: `processor-subscriber-${landscape}` },
        runs: runsEndingWith(newest), functions, now: () => NOW,
        resetRecords: { read: async () => { if (readFails) throw new Error('AccessDenied'); return last; } }
      });
      return { bulkSync, functions };
    };
    const startedBy = { by: 'dashboard', principal: 'wrh' } as any;

    it('is nothing outside a mock landscape, and refuses a reset there', async () => {
      const { bulkSync, functions } = mockSync({ landscape: 'staging' });
      expect(await bulkSync.resetState()).toEqual({ mockLandscape: false });
      await expect(bulkSync.requestReset(startedBy)).rejects.toMatchObject({ code: 'INVALID_REQUEST', message: expect.stringContaining('not a mock landscape') });
      expect(functions.invokeAsync).not.toHaveBeenCalled();
    });

    it("shows the last reset as of now: one running beyond 15 minutes has stopped", async () => {
      expect(await mockSync({ last: record('succeeded') }).bulkSync.resetState()).toEqual({
        mockLandscape: true, landscape: 'mock', last: record('succeeded')
      });
      expect((await mockSync({ last: record('running', '2026-11-10T11:30:00.000Z') }).bulkSync.resetState() as any).last.status).toBe('stopped');
      expect(await mockSync({ readFails: true }).bulkSync.resetState()).toMatchObject({ mockLandscape: true, lastError: 'AccessDenied' });
    });

    it('hands a reset to the chunker subscriber, with who asked and when, and starts nothing else', async () => {
      const { bulkSync, functions } = mockSync({ last: record('succeeded') });
      expect(await bulkSync.requestReset(startedBy)).toEqual({ status: 'requested', landscape: 'mock', requestedAt: '2026-11-10T12:00:00.000Z' });
      expect(functions.invokeAsync).toHaveBeenCalledTimes(1);
      expect(functions.invokeAsync).toHaveBeenCalledWith('chunker-subscriber-mock', {
        mockLandscapeReset: true, requestedAt: '2026-11-10T12:00:00.000Z', startedBy
      });
    });

    it.each([
      ['a run underway', { newest: report('2026-11-10T11:30:00.000Z', { chunks: 20, lastChunkAt: '2026-11-10T11:58:00.000Z' }) }, 'RUN_UNDERWAY'],
      ['a reset still running', { last: record('running', '2026-11-10T11:58:00.000Z') }, 'RESET_UNDERWAY'],
      ['the chunker subscriber in DRY_RUN', { dryRun: ['chunker-subscriber-mock'] }, 'SUBSCRIBER_DRY_RUN']
    ])('refuses, and says so beforehand, with %s', async (_label, params, code) => {
      const { bulkSync, functions } = mockSync(params as any);
      expect((await bulkSync.resetState() as any).blocked).toMatchObject({ code });
      await expect(bulkSync.requestReset(startedBy)).rejects.toMatchObject({ code });
      expect(functions.invokeAsync).not.toHaveBeenCalled();
    });

    it('allows a reset after a stalled or a stopped one', async () => {
      const stalled = mockSync({ newest: stalledRun, last: record('running', '2026-11-10T11:30:00.000Z') });
      expect((await stalled.bulkSync.resetState() as any).blocked).toBeUndefined();
      await expect(stalled.bulkSync.requestReset()).resolves.toMatchObject({ status: 'requested' });
    });
  });

  describe("a mock landscape's options: the source and Start from scratch", () => {
    const SIMULATOR_URL = 'https://example-simulator.lambda-url.us-east-2.on.aws/';
    const mockService = (simulator?: any) => {
      const service = new OfflineBulkSyncService({
        runs: runsEndingWith(finishedRun), settings: { landscape: 'mock' }, now: () => NOW,
        ...(simulator === undefined ? {} : { simulator })
      });
      return service;
    };
    const simulatedFull = { runType: 'full', source: 'simulator', simulator: { totalPopulation: 50, simulatedDelaySeconds: 0, errorRate: 0, nonCurrentTermRate: 0.25 } };

    it('offers the simulator with its deployed settings, and nothing of the kind outside a mock landscape', async () => {
      expect((await mockService().describe()).mock).toEqual({
        simulator: { available: true, defaults: { totalPopulation: 1950, simulatedDelaySeconds: 3, errorRate: 0, nonCurrentTermRate: 0 } },
        limits: { totalPopulation: 500_000, simulatedDelaySeconds: 30 }
      });
      expect(await service().bulkSync.describe()).not.toHaveProperty('mock');
    });

    it("says why the simulator can't be the source, and refuses a run that asks for it", async () => {
      const missing = mockService({ lookup: async () => ({ reason: "The source simulator (source-simulator-mock) isn't deployed in this landscape" }), settings: async () => undefined });
      expect((await missing.describe()).mock!.simulator).toEqual({
        available: false, reason: "The source simulator (source-simulator-mock) isn't deployed in this landscape", defaults: BulkSync.SIMULATOR_DEFAULTS
      });
      await expect(missing.plan(simulatedFull)).rejects.toMatchObject({ code: 'INVALID_REQUEST', message: expect.stringContaining("can't be the source: The source simulator") });
      expect((await mockService(null).describe()).mock!.simulator).toMatchObject({ available: false, reason: expect.stringContaining('redeploy its dashboard stack') });
    });

    it('sends the simulator as the source, its settings as query parameters, its terms, and the reset', async () => {
      const service = mockService();
      const plan = await service.plan({ ...simulatedFull, resetState: true });
      expect(plan.event).toMatchObject({
        baseUrl: 'https://example-simulator.lambda-url.us-east-2.on.aws',
        fetchPath: '/?totalPopulation=50&simulatedDelaySeconds=0&errorRate=0&nonCurrentTermRate=0.25',
        termsEndpoint: { baseUrl: 'https://example-simulator.lambda-url.us-east-2.on.aws', fetchPath: '/terms' },
        populationType: 'person-full', resetState: true
      });
      expect(plan.choices).toMatchObject({ source: 'simulator', simulator: simulatedFull.simulator, resetState: true });
      await service.start({ ...simulatedFull, resetState: true });
      expect(service.invoked[0]).toMatchObject({ resetState: true, fetchPath: plan.event.fetchPath });
    });

    it("tests one of the simulator's people through its person endpoint", async () => {
      const { event } = await mockService().plan({ runType: 'test', buid: 'U0000042', source: 'simulator', simulator: { totalPopulation: 100 } });
      expect(event.fetchPath).toBe('/person?buid=U0000042&totalPopulation=100');
      expect(event.iterationLimit).toBe(0);
    });

    it('reads the real source as before when it is chosen, and resets nothing unless asked', async () => {
      const { event, choices } = await mockService().plan({ runType: 'full', source: 'real' });
      expect(event).toMatchObject({ baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people' });
      expect(event).not.toHaveProperty('termsEndpoint');
      expect(event).not.toHaveProperty('resetState');
      expect(choices).toMatchObject({ source: 'real', resetState: false });
      expect(choices).not.toHaveProperty('simulator');
    });

    it('lists the mock settings from the form', async () => {
      const byName = async (body: any, service: any = mockService()) =>
        Object.fromEntries((await service.plan(body)).settings.map((row: any) => [row.name, row]));
      const simulated = await byName({ ...simulatedFull, simulator: { totalPopulation: 50 }, resetState: true });
      expect(simulated.sourceSimulator).toMatchObject({ value: true, source: 'form' });
      expect(simulated.sourceSimulatorMockTotalPopulation).toMatchObject({ value: 50, source: 'form', note: expect.stringContaining('query parameter') });
      expect(simulated.sourceSimulatorMockErrorRate).toMatchObject({ value: null, source: 'form', note: expect.stringContaining('own setting applies') });
      expect(simulated.mockTargetResetState).toMatchObject({ value: true, source: 'form' });
      const hybrid = await byName({ runType: 'full' });
      expect(hybrid.sourceSimulator).toMatchObject({ value: false, source: 'form', note: expect.stringContaining('hybrid') });
      expect(hybrid.sourceSimulatorMockTotalPopulation).toMatchObject({ source: 'unused' });
      const elsewhere = Object.fromEntries((await service().bulkSync.plan({ runType: 'full' })).settings.map(row => [row.name, row]));
      expect(elsewhere.sourceSimulator).toMatchObject({ value: false, source: 'fixed', note: 'Mock landscapes only' });
      expect(elsewhere.mockTargetResetState).toMatchObject({ value: false, source: 'fixed', note: 'Mock landscapes only' });
    });

    it.each([
      [{ runType: 'full', source: 'simulator' }, false, 'only for a mock landscape'],
      [{ runType: 'full', resetState: true }, false, 'only for a mock landscape'],
      [{ runType: 'full', simulator: {} }, false, 'only for a mock landscape'],
      [{ runType: 'full', source: 'elsewhere' }, true, "source must be one of 'real', 'simulator'"],
      [{ runType: 'full', resetState: 'yes' }, true, 'resetState must be true or false'],
      [{ runType: 'full', simulator: { totalPopulation: 5 } }, true, 'Simulator settings are only for a run whose source is the source simulator'],
      [{ runType: 'full', source: 'simulator', simulator: [] }, true, "simulator must be an object of the source simulator's settings"],
      [{ runType: 'full', source: 'simulator', simulator: { people: 5 } }, true, "simulator.people isn't a source simulator setting"],
      [{ runType: 'full', source: 'simulator', simulator: { totalPopulation: 0 } }, true, 'simulator.totalPopulation must be a whole number from 1 to 500000'],
      [{ runType: 'full', source: 'simulator', simulator: { totalPopulation: 2.5 } }, true, 'simulator.totalPopulation must be a whole number'],
      [{ runType: 'full', source: 'simulator', simulator: { simulatedDelaySeconds: 31 } }, true, 'simulator.simulatedDelaySeconds must be a number from 0 to 30'],
      [{ runType: 'full', source: 'simulator', simulator: { errorRate: 1.5 } }, true, 'simulator.errorRate must be a number from 0 to 1'],
      [{ runType: 'full', source: 'simulator', simulator: { nonCurrentTermRate: '0.5' } }, true, 'simulator.nonCurrentTermRate must be a number from 0 to 1'],
      [{ runType: 'test', buid: 'U0000000', source: 'simulator' }, true, "The source simulator's BUIDs are a U followed by digits from 1"],
      [{ runType: 'test', buid: 'U0000101', source: 'simulator', simulator: { totalPopulation: 100 } }, true, "U0000101 is beyond the source simulator's 100 people"]
    ])('refuses %j (mock landscape: %s) as INVALID_REQUEST', (body, mockLandscape, message) => {
      expect(() => BulkSyncRequest.parse(body as any, { mockLandscape })).toThrow(message);
    });

    it("reads the simulator's Function URL and its settings, changing nothing", async () => {
      const lambda = mockClient(LambdaClient);
      const fetched: string[] = [];
      const realFetch = global.fetch;
      global.fetch = jest.fn(async (url: any) => {
        fetched.push(String(url));
        return { ok: true, json: async () => ({ status: 'healthy', totalPopulation: 1950, errorRate: 0, simulatedDelaySeconds: 3 }) } as any;
      }) as any;
      try {
        lambda.on(GetFunctionUrlConfigCommand, { FunctionName: 'source-simulator-mock' }).resolves({ FunctionUrl: SIMULATOR_URL } as any);
        const simulator = new AwsSourceSimulatorService('source-simulator-mock', new LambdaClient({ region: 'us-east-2' }));
        expect(await simulator.lookup()).toEqual({ url: SIMULATOR_URL });
        expect(await simulator.settings(SIMULATOR_URL)).toEqual({ totalPopulation: 1950, errorRate: 0, simulatedDelaySeconds: 3 });
        expect(fetched).toEqual([`${SIMULATOR_URL}health`]);
        lambda.on(GetFunctionUrlConfigCommand).rejects(Object.assign(new Error('not found'), { name: 'ResourceNotFoundException' }));
        expect(await simulator.lookup()).toEqual({ reason: "The source simulator (source-simulator-mock) isn't deployed in this landscape" });
        expect(lambda.calls().every(call => call.args[0] instanceof GetFunctionUrlConfigCommand)).toBe(true);
      } finally {
        global.fetch = realFetch;
        lambda.restore();
      }
    });
  });

  describe('from the environment', () => {
    const saved = { ...process.env };
    afterEach(() => { process.env = { ...saved }; });

    it("refuses starts, without failing to describe, until the dashboard stack has been deployed with its settings", async () => {
      for (const name of ['LANDSCAPE', 'REGION', 'CHUNKER_FUNCTION_NAME', 'PROCESSOR_SUBSCRIBER_FUNCTION_NAME']) {
        delete process.env[name];
      }
      process.env.LANDSCAPE = 'preview';
      const bulkSync = BulkSync.fromEnvironment(OFFLINE_SOURCE_CONFIG, runsEndingWith(finishedRun));
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
      const bulkSync = BulkSync.fromEnvironment(OFFLINE_SOURCE_CONFIG, runsEndingWith(finishedRun));
      // The source simulator and the reset record only in a mock landscape
      expect((bulkSync as any).deps.simulator).toBeUndefined();
      expect((bulkSync as any).deps.resetRecords).toBeUndefined();
      process.env.LANDSCAPE = 'mock';
      expect((BulkSync.fromEnvironment(OFFLINE_SOURCE_CONFIG, runsEndingWith(finishedRun)) as any).deps.resetRecords).toBeDefined();
      process.env.LANDSCAPE = 'preview';
      process.env.SOURCE_SIMULATOR_FUNCTION_NAME = 'source-simulator-mock';
      expect((BulkSync.fromEnvironment(OFFLINE_SOURCE_CONFIG, runsEndingWith(finishedRun)) as any).deps.simulator).toBeInstanceOf(AwsSourceSimulatorService);
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

describe('the bulk sync routes, offline', () => {
  const request = async (method: string, body?: unknown, path = '/api/bulk-sync') => {
    const result = await handler({
      rawPath: path, headers: {}, body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
      requestContext: { http: { method, path } }, isBase64Encoded: false
    } as unknown as LambdaFunctionUrlEvent);
    return { status: result.statusCode, body: result.body ? JSON.parse(`${result.body}`) : undefined };
  };

  beforeAll(() => { process.env.LOCAL_OFFLINE = 'true'; });
  afterAll(() => { delete process.env.LOCAL_OFFLINE; });
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
    // The mock chunker service runs at most 15 tasks
    expect(await request('POST', { runType: 'full', parallelStart: 16 }, '/api/bulk-sync/plan'))
      .toMatchObject({ status: 400, body: { code: 'INVALID_REQUEST' } });
    expect(await request('POST', { runType: 'full', parallelStart: 3 }, '/api/bulk-sync/plan'))
      .toMatchObject({ status: 200, body: { event: { parallelStart: 3 } } });
    expect(await request('POST', 'not json')).toMatchObject({ status: 400, body: { code: 'INVALID_REQUEST' } });
    expect((await request('PUT', {})).status).toBe(405);
    const getPlan = await handler({
      rawPath: '/api/bulk-sync/plan', headers: {}, requestContext: { http: { method: 'GET', path: '/api/bulk-sync/plan' } }, isBase64Encoded: false
    } as unknown as LambdaFunctionUrlEvent);
    expect(getPlan.statusCode).toBe(405);
    expect(getPlan.headers).toMatchObject({ Allow: 'POST' });
  });
});
