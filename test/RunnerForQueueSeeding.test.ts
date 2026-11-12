/**
 * Tests that the manual Runner's queue seeding (src/runner/RunnerForQueueSeeding.ts) starts its run through the same
 * procedure as the chunker subscriber's parallel start (src/chunking/ParallelStart.ts): MESSAGES_TO_PREPOPULATE
 * messages and DESIRED_COUNT tasks, 0 tasks seeding only, and nothing started without the atomic counter table.
 */
import { ParallelStart } from '../src/chunking/ParallelStart';
import { QueueSeedingRunner } from '../src/runner/RunnerForQueueSeeding';
import { SyncPopulation } from '../docker/chunkTypes';

const endpoint = { baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people' };

/** A Runner whose parallel start and counter-table check are stand-ins */
const runner = (env: Record<string, any>, counterTable = true) => {
  const start = jest.fn().mockResolvedValue({ statusCode: 200, body: 'ok', requested: 0 });
  const instance = new QueueSeedingRunner({
    queueUrl: 'https://sqs.us-east-2.amazonaws.com/123/chunker', region: 'us-east-2', stackId: 'huron-person-fargate-processor',
    landscape: 'preview', iterationLimit: 10, messagesToPrepopulate: '0', desiredCount: 0, ...env
  } as any);
  (instance as any).atomicCounterExists = async () => counterTable;
  const parallelStart = jest.spyOn(instance as any, 'parallelStart').mockReturnValue({ start } as unknown as ParallelStart);
  return { instance, start, parallelStart };
};

beforeEach(() => jest.spyOn(console, 'error').mockImplementation(() => {}));
afterEach(() => jest.restoreAllMocks());

describe('QueueSeedingRunner.execute', () => {
  it('seeds MESSAGES_TO_PREPOPULATE messages and starts DESIRED_COUNT tasks through ParallelStart', async () => {
    const { instance, start, parallelStart } = runner({ messagesToPrepopulate: '5', desiredCount: 3 });
    await instance.execute(endpoint, { endpoint }, {} as any, SyncPopulation.PersonFull);
    expect(parallelStart).toHaveBeenCalledWith(endpoint, SyncPopulation.PersonFull);
    expect(start).toHaveBeenCalledWith({ messages: 5, tasks: 3 });
  });

  it('seeds only when DESIRED_COUNT is 0', async () => {
    const { instance, start } = runner({ messagesToPrepopulate: '4', desiredCount: 0 });
    await instance.execute(endpoint, { endpoint }, {} as any, SyncPopulation.PersonFull);
    expect(start).toHaveBeenCalledWith({ messages: 4, tasks: 0 });
  });

  it("starts nothing without the atomic counter table", async () => {
    const { instance, start } = runner({ messagesToPrepopulate: '4', desiredCount: 2 }, false);
    await instance.execute(endpoint, { endpoint }, {} as any, SyncPopulation.PersonFull);
    expect(start).not.toHaveBeenCalled();
  });

  it("logs a start ParallelStart refused", async () => {
    const { instance, start } = runner({ messagesToPrepopulate: '4', desiredCount: 2 });
    start.mockResolvedValue({ statusCode: 400, body: "2 chunker tasks is more than the chunker service's maximum of 1; nothing was sent", requested: 4 });
    await instance.execute(endpoint, { endpoint }, {} as any, SyncPopulation.PersonFull);
    expect(console.error).toHaveBeenCalledWith(expect.stringContaining("Queue seeding didn't complete: 2 chunker tasks is more than"));
  });
});
