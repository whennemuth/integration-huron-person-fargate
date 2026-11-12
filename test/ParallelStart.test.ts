/**
 * Tests a run's start with several chunker tasks (src/chunking/ParallelStart.ts, and startInParallel() in
 * src/chunking/ChunkerSubscriber.ts): what is refused before anything is reset or sent, the order of seed, wait and
 * scale, the wait bounded by the Lambda's remaining time, and that the seeded messages say who started the run.
 * AWS is replaced by injected functions throughout.
 */
import type { ApiChunkerEvent } from '../src/chunking/ChunkerSubscriber';
import { startInParallel } from '../src/chunking/ChunkerSubscriber';
import { ParallelStart, ParallelStartDeps } from '../src/chunking/ParallelStart';
import { RunStarter } from '../src/chunking/StartedBy';
import { SyncPopulation } from '../docker/chunkTypes';

import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { QueueSeeder } from '../src/chunking/fetch/QueueSeeder';

// The seeder sends each message through handleApiEvent: record them instead (jest hoists this above the imports)
jest.mock('../src/chunking/fetch/ChunkerApiSubscriber', () => ({ handleApiEvent: jest.fn().mockResolvedValue({ statusCode: 200 }) }));

const ENV = {
  CHUNKER_QUEUE_URL: 'https://sqs.us-east-2.amazonaws.com/123/preview-huron-person-chunker-queue', REGION: 'us-east-2',
  STACK_ID: 'huron-person-fargate-processor', LANDSCAPE: 'preview', CHUNKER_CLUSTER_NAME: 'huron-person-cluster-preview',
  CHUNKER_SERVICE_NAME: 'Chunker'
};

const event = (overrides: Partial<ApiChunkerEvent> = {}): ApiChunkerEvent => ({
  baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: SyncPopulation.PersonDelta,
  bulkReset: false, trustPreviousStorage: true, iterationLimit: 10, offset: 0, parallelStart: 3,
  startedBy: RunStarter.dashboard('wrh'), ...overrides
});

/** Injected AWS, recording the order of calls */
const fakeAws = (params: { max?: number; sent?: number; settled?: boolean } = {}) => {
  const calls: string[] = [];
  const deps: ParallelStartDeps = {
    maxTasks: jest.fn(async () => { calls.push('max'); return params.max ?? 15; }),
    seed: jest.fn(async (n: number) => { calls.push(`seed ${n}`); return params.sent ?? n; }),
    waitForAlarm: jest.fn(async (maxWait?: number) => { calls.push(`wait ${maxWait}`); return params.settled ?? true; }),
    setDesiredCount: jest.fn(async (count: number) => { calls.push(`scale ${count}`); })
  };
  return { deps, calls };
};

beforeEach(() => {
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('ParallelStart.requested', () => {
  it('is the number of messages asked for, and 0 (one message, as today) when absent or 0', () => {
    expect(ParallelStart.requested(event({ parallelStart: 3 }))).toBe(3);
    expect(ParallelStart.requested(event({ parallelStart: 0 }))).toBe(0);
    expect(ParallelStart.requested(event({ parallelStart: undefined }))).toBe(0);
  });
});

describe('ParallelStart.refusal', () => {
  it('accepts a run start with chunks per task, from a people source, in a complete environment', () => {
    expect(ParallelStart.refusal(event(), ENV)).toBeUndefined();
  });

  it.each([
    ['more than the hard maximum', event({ parallelStart: 21 }), ENV, 'whole number from 1 to 20'],
    ['a fraction', event({ parallelStart: 2.5 }), ENV, 'whole number from 1 to 20'],
    ['a message that continues a run (a later offset)', event({ offset: 10 }), ENV, "only for a run's first event"],
    ['a seeded message (it carries the chunk directory)', event({ chunkDirectory: 'chunks/person-delta/2026-11-12' }), ENV, "only for a run's first event"],
    ['a single task (iterationLimit 0)', event({ iterationLimit: 0 }), ENV, 'needs chunks per task'],
    ['a one-person source', event({ fetchPath: '/api/person?buid=U15364542' }), ENV, 'not for a one-person source'],
    ['an environment without the chunker service', event(), { ...ENV, CHUNKER_SERVICE_NAME: '' }, 'CHUNKER_SERVICE_NAME'],
  ])('refuses %s', (_label, ev, env, message) => {
    expect(ParallelStart.refusal(ev, env)).toContain(message);
  });
});

describe('ParallelStart.run', () => {
  it('checks the maximum, seeds, waits for the alarm within the remaining time, then sets the desired count', async () => {
    const { deps, calls } = fakeAws();
    const result = await new ParallelStart(deps).run(event(), 600_000);
    // 600 s left, less the 60 s margin for scaling and returning
    expect(calls).toEqual(['max', 'seed 3', 'wait 540', 'scale 3']);
    expect(result).toMatchObject({ statusCode: 200, requested: 3, seeded: 3, desiredCount: 3, alarmSettled: true });
  });

  it("refuses more tasks than the chunker service allows, before anything is sent", async () => {
    const { deps, calls } = fakeAws({ max: 2 });
    const result = await new ParallelStart(deps).run(event({ parallelStart: 3 }), 600_000);
    expect(calls).toEqual(['max']);
    expect(result).toMatchObject({ statusCode: 400, requested: 3 });
    expect(result.body).toContain("3 chunker tasks is more than the chunker service's maximum of 2; nothing was sent");
  });

  it("still sets the desired count when the alarm hasn't settled in time, and says so", async () => {
    const { deps, calls } = fakeAws({ settled: false });
    const result = await new ParallelStart(deps).run(event(), 120_000);
    expect(calls).toEqual(['max', 'seed 3', 'wait 60', 'scale 3']);
    expect(result).toMatchObject({ statusCode: 200, alarmSettled: false });
    expect(result.body).toContain("the scale-in alarm hadn't settled");
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('within 60s'));
  });

  it('scales to the messages actually sent, and stops when none were', async () => {
    const some = fakeAws({ sent: 2 });
    expect(await new ParallelStart(some.deps).run(event(), 600_000)).toMatchObject({ seeded: 2, desiredCount: 2 });
    expect(some.calls).toContain('scale 2');

    const none = fakeAws({ sent: 0 });
    expect(await new ParallelStart(none.deps).run(event(), 600_000)).toMatchObject({ statusCode: 500, seeded: 0 });
    expect(none.calls).toEqual(['max', 'seed 3']);
  });

  it('waits without a limit when the remaining time is unknown (outside Lambda)', async () => {
    const { deps, calls } = fakeAws();
    await new ParallelStart(deps).run(event());
    expect(calls).toContain('wait undefined');
  });

  it('does nothing in DRY_RUN but say what it would do', async () => {
    const { deps, calls } = fakeAws();
    const result = await new ParallelStart(deps, true).run(event(), 600_000);
    expect(calls).toEqual([]);
    expect(result).toMatchObject({ statusCode: 200, dryRun: true });
  });
});

describe('ParallelStart.start (messages and tasks apart, as the manual Runner uses it)', () => {
  it('seeds only, without waiting or scaling, for 0 tasks, and reads no maximum', async () => {
    const { deps, calls } = fakeAws({ max: 1 });
    const result = await new ParallelStart(deps).start({ messages: 5, tasks: 0 });
    expect(calls).toEqual(['seed 5']);
    expect(result).toMatchObject({ statusCode: 200, seeded: 5, desiredCount: 0 });
  });

  it('starts fewer tasks than messages, checking the maximum against the tasks', async () => {
    const { deps, calls } = fakeAws({ max: 3 });
    const result = await new ParallelStart(deps).start({ messages: 5, tasks: 3 });
    expect(calls).toEqual(['max', 'seed 5', 'wait undefined', 'scale 3']);
    expect(result).toMatchObject({ statusCode: 200, seeded: 5, desiredCount: 3 });
  });

  it('never scales above the messages actually sent', async () => {
    const { deps, calls } = fakeAws({ sent: 2 });
    expect(await new ParallelStart(deps).start({ messages: 5, tasks: 4 })).toMatchObject({ seeded: 2, desiredCount: 2 });
    expect(calls).toContain('scale 2');
  });

  it.each([
    ['more tasks than messages', { messages: 2, tasks: 3 }],
    ['no messages', { messages: 0, tasks: 0 }],
    ['a negative task count', { messages: 2, tasks: -1 }],
  ])('refuses %s, sending nothing', async (_label, params) => {
    const { deps, calls } = fakeAws();
    const result = await new ParallelStart(deps).start(params);
    expect(calls).toEqual([]);
    expect(result).toMatchObject({ statusCode: 400 });
  });
});

describe('startInParallel (the chunker subscriber)', () => {
  it('resets the counters, then starts in parallel with the Lambda\'s remaining time', async () => {
    const { deps, calls } = fakeAws();
    const reset = jest.fn(async () => { calls.push('reset'); });
    const result = await startInParallel(event(), { getRemainingTimeInMillis: () => 600_000 }, ENV, () => new ParallelStart(deps), reset);
    expect(calls).toEqual(['reset', 'max', 'seed 3', 'wait 540', 'scale 3']);
    expect(reset).toHaveBeenCalledWith({ stackId: ENV.STACK_ID, region: ENV.REGION, landscape: ENV.LANDSCAPE });
    expect(result).toMatchObject({ statusCode: 200 });
  });

  it('refuses before resetting the counters or sending anything, returning (not throwing) so it isn\'t retried', async () => {
    const { deps, calls } = fakeAws();
    const reset = jest.fn(async () => { calls.push('reset'); });
    const result = await startInParallel(event({ iterationLimit: 0 }), undefined, ENV, () => new ParallelStart(deps), reset);
    expect(calls).toEqual([]);
    expect(result).toMatchObject({ statusCode: 400 });
    expect(result.body).toContain('needs chunks per task');
  });
});

describe('QueueSeeder', () => {
  const seeder = (startedBy?: any) => new QueueSeeder({
    stackId: ENV.STACK_ID, region: ENV.REGION, landscape: ENV.LANDSCAPE, queueUrl: ENV.CHUNKER_QUEUE_URL,
    baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: SyncPopulation.PersonDelta,
    iterationLimit: 10, messagesToSeed: 1, ...(startedBy ? { startedBy } : {})
  });

  beforeEach(() => (handleApiEvent as jest.Mock).mockClear());

  it("marks the seeded messages with who started the run: the dashboard's user for a parallel start, else the Runner", async () => {
    await seeder(RunStarter.dashboard('wrh')).seedQueue();
    expect((handleApiEvent as jest.Mock).mock.calls[0][0]).toMatchObject({ offset: 0, startedBy: { by: 'dashboard', principal: 'wrh' } });
    expect((handleApiEvent as jest.Mock).mock.calls[0][0]).not.toHaveProperty('parallelStart');

    await seeder().seedQueue();
    expect((handleApiEvent as jest.Mock).mock.calls[1][0]).toMatchObject({ startedBy: { by: 'runner' } });
  });
});
