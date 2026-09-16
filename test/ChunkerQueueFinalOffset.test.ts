import { ChunkerQueue } from '../src/chunking/ChunkerQueue';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { TaskParameters } from '../src/chunking/fetch/ChunkFromAPI';
import { SyncPopulation } from '../docker/chunkTypes';

jest.mock('../src/chunking/fetch/ChunkerApiSubscriber', () => ({
  handleApiEvent: jest.fn()
}));

describe('ChunkerQueue.sendNextChunkingMessage finalOffsetProcessed short-circuit', () => {
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    originalEnv = { ...process.env };
    delete process.env.DYNAMODB_ATOMIC_COUNTER_TABLE_NAME;
    delete process.env.STACK_ID;
    jest.clearAllMocks();
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  const taskParameters: TaskParameters = {
    baseUrl: 'https://example.com',
    fetchPath: '/people',
    populationType: SyncPopulation.PersonFull,
    bulkReset: false,
    trustPreviousStorage: true
  };

  it('sends the next message when finalOffsetProcessed is not yet known', async () => {
    const chunkerQueue = new ChunkerQueue({ isEcsTask: false, QueueUrl: 'https://sqs.example.com/queue', region: 'us-east-2', landscape: 'dev' });

    const sent = await chunkerQueue.sendNextChunkingMessage({
      iterationLimit: 10, offset: 540, chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', taskParameters
    });

    expect(sent).toBe(true);
    expect(handleApiEvent).toHaveBeenCalledTimes(1);
  });

  it('sends the next message when the computed next offset is still within finalOffsetProcessed', async () => {
    const chunkerQueue = new ChunkerQueue({ isEcsTask: false, QueueUrl: 'https://sqs.example.com/queue', region: 'us-east-2', landscape: 'dev' });

    const sent = await chunkerQueue.sendNextChunkingMessage({
      iterationLimit: 10, offset: 520, chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', taskParameters,
      finalOffsetProcessed: 543 // next offset will be 530, still within bounds
    });

    expect(sent).toBe(true);
    expect(handleApiEvent).toHaveBeenCalledTimes(1);
  });

  it('skips sending when the computed next offset exceeds finalOffsetProcessed', async () => {
    const chunkerQueue = new ChunkerQueue({ isEcsTask: false, QueueUrl: 'https://sqs.example.com/queue', region: 'us-east-2', landscape: 'dev' });

    const sent = await chunkerQueue.sendNextChunkingMessage({
      iterationLimit: 10, offset: 540, chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', taskParameters,
      finalOffsetProcessed: 543 // next offset will be 550, beyond the known end
    });

    expect(sent).toBe(false);
    expect(handleApiEvent).not.toHaveBeenCalled();
  });
});
