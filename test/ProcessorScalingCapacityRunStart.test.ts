/**
 * Tests the chunker subscriber setting the processor's live maximum at a run start (setProcessorCapacityForRunStart(),
 * runStartRefusal() and handler() in src/chunking/ChunkerSubscriber.ts, and handleS3Event()'s beforeStart in
 * src/chunking/filedrop/ChunkerS3Subscriber.ts): a run's override, or else the landscape's maxScalingCapacity, set
 * before anything is reset or queued; a refused override changing nothing; an override that can't be set failing the
 * start; continuing events left alone; and a dropped file setting the landscape's maximum back before it is queued.
 * AWS is replaced by aws-sdk-client-mock; queuing is recorded.
 */
import { ApplicationAutoScalingClient, RegisterScalableTargetCommand } from '@aws-sdk/client-application-auto-scaling';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { handler, runStartRefusal, setProcessorCapacityForRunStart } from '../src/chunking/ChunkerSubscriber';
import { ProcessorScalingCapacity } from '../src/chunking/ProcessorScalingCapacity';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { handleS3Event } from '../src/chunking/filedrop/ChunkerS3Subscriber';

jest.mock('../src/chunking/fetch/ChunkerApiSubscriber', () => ({ handleApiEvent: jest.fn() }));
// The S3 branch's own module reads its environment when loaded; its hook placement is tested below in isolation
jest.mock('../src/chunking/filedrop/ChunkerS3Subscriber', () => ({ handleS3Event: jest.fn() }));

const ENV = {
  REGION: 'us-east-2', STACK_ID: 'huron-person-fargate-processor', LANDSCAPE: 'preview', DRY_RUN: 'false',
  CHUNKER_QUEUE_URL: 'https://sqs.us-east-2.amazonaws.com/111111111111/chunker', CHUNKER_CLUSTER_NAME: 'huron-person-cluster-preview',
  CHUNKER_SERVICE_NAME: 'Chunker', PROCESSOR_SERVICE_NAME: 'Processor', PROCESSOR_MAX_SCALING_CAPACITY: '3'
};
const runStart = { baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: 'person-full', previousStateSource: 'storage', iterationLimit: 10 } as any;

beforeEach(() => {
  jest.spyOn(console, 'log').mockImplementation(() => undefined);
  jest.spyOn(console, 'warn').mockImplementation(() => undefined);
  jest.spyOn(console, 'error').mockImplementation(() => undefined);
});
afterEach(() => jest.restoreAllMocks());

describe("the chunker subscriber's run start", () => {
  const aas = mockClient(ApplicationAutoScalingClient);
  const dynamo = mockClient(DynamoDBDocumentClient);
  let saved: NodeJS.ProcessEnv;
  let calls: string[];

  beforeEach(() => {
    saved = { ...process.env };
    Object.assign(process.env, ENV);
    calls = [];
    aas.reset();
    aas.on(RegisterScalableTargetCommand).callsFake(input => { calls.push(`set max ${input.MaxCapacity}`); return {}; });
    dynamo.reset();
    dynamo.on(UpdateCommand).callsFake(input => { calls.push(`reset counter ${input.Key.counter_name}`); return {}; });
    (handleApiEvent as jest.Mock).mockReset().mockImplementation(async () => {
      calls.push('queue');
      return { statusCode: 200, body: 'Message sent to chunker queue' };
    });
    (handleS3Event as jest.Mock).mockReset().mockImplementation(async (_event: any, beforeStart?: () => Promise<unknown>) => {
      await beforeStart?.();
      calls.push('queue file');
      return { statusCode: 200, body: 'Message sent to chunker queue' };
    });
  });
  afterEach(() => { process.env = saved; });
  afterAll(() => { aas.restore(); dynamo.restore(); });

  it("sets a run's override as the processor's maximum before the counters are reset and the run is queued", async () => {
    expect(await handler({ ...runStart, processorScalingCapacityOverride: 8 })).toEqual({ statusCode: 200, body: 'Message sent to chunker queue' });
    expect(calls).toEqual(['set max 8', 'reset counter chunker-offset-counter', 'reset counter chunker-chunk-ordinal-counter', 'queue']);
    expect(aas.commandCalls(RegisterScalableTargetCommand)[0].args[0].input.ResourceId).toBe('service/huron-person-cluster-preview/Processor');
  });

  it("sets the landscape's maxScalingCapacity back on a run without an override (the schedule's, a default Bulk sync)", async () => {
    await handler(runStart);
    expect(calls[0]).toBe('set max 3');
    expect(calls[calls.length - 1]).toBe('queue');
  });

  it('queues the run with the maximum its start set, for its Flags', async () => {
    await handler({ ...runStart, processorScalingCapacityOverride: 8 });
    expect((handleApiEvent as jest.Mock).mock.calls[0][0].processorScalingCapacity).toEqual({ maxTasks: 8, overridden: true });
    await handler(runStart);
    expect((handleApiEvent as jest.Mock).mock.calls[1][0].processorScalingCapacity).toEqual({ maxTasks: 3, overridden: false });
  });

  it("records nothing when the maximum wasn't set (DRY_RUN, or a failed restore)", async () => {
    aas.on(RegisterScalableTargetCommand).rejects(new Error('Throttling'));
    await handler(runStart);
    expect((handleApiEvent as jest.Mock).mock.calls[0][0]).not.toHaveProperty('processorScalingCapacity');
  });

  it('refuses an override out of range with a 400, setting, resetting and queuing nothing', async () => {
    const response = await handler({ ...runStart, processorScalingCapacityOverride: 21 });
    expect(response).toEqual({ statusCode: 400, body: 'processorScalingCapacityOverride must be a whole number from 1 to 20 (got 21)' });
    expect(calls).toEqual([]);
  });

  it("fails the start, having reset and queued nothing, when the run's override can't be set", async () => {
    aas.on(RegisterScalableTargetCommand).rejects(new Error('AccessDeniedException'));
    await expect(handler({ ...runStart, processorScalingCapacityOverride: 8 })).rejects.toThrow(/AccessDeniedException/);
    expect(calls).toEqual([]);
  });

  it("still starts the run when the landscape's maximum can't be set back", async () => {
    aas.on(RegisterScalableTargetCommand).rejects(new Error('Throttling'));
    expect((await handler(runStart)).statusCode).toBe(200);
    expect(calls[calls.length - 1]).toBe('queue');
  });

  it("leaves the maximum alone for an event that doesn't start a run", async () => {
    await handler({ ...runStart, offset: 10, chunkDirectory: 'chunks/person-full/2026-11-17T00:00:00.000Z' });
    expect(aas.calls()).toHaveLength(0);
    expect(calls).toEqual(['queue']);
  });

  it('only logs what it would set in DRY_RUN', async () => {
    process.env.DRY_RUN = 'true';
    await handler({ ...runStart, processorScalingCapacityOverride: 8 });
    expect(aas.calls()).toHaveLength(0);
  });

  it("sets the landscape's maximum back for a dropped file, before its run is queued", async () => {
    await handler({ bucket: 'input', key: 'people.json' } as any);
    expect(calls).toEqual(['set max 3', 'queue file']);
  });

  it('runStartRefusal() names an override it refuses, after a reset and a parallel start', () => {
    expect(runStartRefusal({ ...runStart, processorScalingCapacityOverride: 0 }, ENV)).toMatch(/whole number from 1 to 20/);
    expect(runStartRefusal({ ...runStart, processorScalingCapacityOverride: 5, parallelStart: 99 }, ENV)).toMatch(/^parallelStart must be/);
    expect(runStartRefusal({ ...runStart, processorScalingCapacityOverride: 5 }, ENV)).toBeUndefined();
  });

  it('setProcessorCapacityForRunStart() applies the event\'s override through the given capacity', async () => {
    const apply = jest.fn(async () => ({ applied: true, capacity: 6, override: 6 }));
    const capacity = jest.fn(() => ({ apply }) as unknown as ProcessorScalingCapacity);
    await expect(setProcessorCapacityForRunStart({ processorScalingCapacityOverride: 6 }, ENV, capacity))
      .resolves.toEqual({ applied: true, capacity: 6, override: 6 });
    expect(capacity).toHaveBeenCalledWith(ENV);
    expect(apply).toHaveBeenCalledWith(6);
  });
});

describe("a dropped file's hook (handleS3Event()'s beforeStart)", () => {
  /** handleS3Event() from a fresh module, which reads CHUNKER_QUEUE_URL and DRY_RUN when it loads, with SQS recorded */
  const load = (env: Record<string, string>) => {
    const saved = { ...process.env };
    Object.assign(process.env, env);
    let loaded: any;
    jest.isolateModules(() => {
      const sqsModule = require('@aws-sdk/client-sqs');
      const sqs = mockClient(sqsModule.SQSClient);
      sqs.on(sqsModule.SendMessageCommand).resolves({});
      const { handleS3Event: real } = jest.requireActual('../src/chunking/filedrop/ChunkerS3Subscriber');
      loaded = { handleS3Event: real, sqs };
    });
    process.env = saved;
    return loaded as { handleS3Event: typeof handleS3Event; sqs: ReturnType<typeof mockClient> };
  };

  it('is called once the file starts a run, before its message is sent', async () => {
    const { handleS3Event: real, sqs } = load({ CHUNKER_QUEUE_URL: ENV.CHUNKER_QUEUE_URL, REGION: 'us-east-2', DRY_RUN: 'false' });
    const order: string[] = [];
    const beforeStart = jest.fn(async () => { order.push('hook'); });
    sqs.callsFake(() => { order.push('send'); return {}; });
    expect((await real({ bucket: 'input', key: 'people.json' }, beforeStart)).statusCode).toBe(200);
    expect(order).toEqual(['hook', 'send']);
  });

  it('is not called for a chunk file, or in DRY_RUN', async () => {
    const live = load({ CHUNKER_QUEUE_URL: ENV.CHUNKER_QUEUE_URL, REGION: 'us-east-2', DRY_RUN: 'false' });
    const beforeStart = jest.fn(async () => undefined);
    await live.handleS3Event({ bucket: 'input', key: 'x/chunks/chunk-0000.ndjson' }, beforeStart);
    const dry = load({ CHUNKER_QUEUE_URL: ENV.CHUNKER_QUEUE_URL, REGION: 'us-east-2', DRY_RUN: 'true' });
    await dry.handleS3Event({ bucket: 'input', key: 'people.json' }, beforeStart);
    expect(beforeStart).not.toHaveBeenCalled();
  });
});
