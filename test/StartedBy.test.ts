/**
 * Who or what started a run (src/chunking/StartedBy.ts) travels from the event that starts it to the run's Flags:
 * the event -> the chunker subscriber's queue message -> the chunker's task parameters -> its Flags. (Chained
 * messages: test/ChunkerQueueChainStop.test.ts; DynamoDB flags: test/MetadataForDynamoDb.test.ts; the summary:
 * test/StatisticsTable.test.ts; the schedule's: test/ScheduledRun.test.ts; the dashboard's:
 * test/DashboardBulkSync.test.ts.)
 */
import { Message } from '@aws-sdk/client-sqs';
import { Config } from 'integration-huron-person';
import { SyncPopulation } from '../docker/chunkTypes';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { ChunkFromAPI } from '../src/chunking/fetch/ChunkFromAPI';
import { MetadataForS3 } from '../src/chunking/metadata/MetadataForS3';
import { RunStarter } from '../src/chunking/StartedBy';

const mockSend = jest.fn().mockResolvedValue({});
jest.mock('@aws-sdk/client-sqs', () => ({
  ...jest.requireActual('@aws-sdk/client-sqs'),
  SQSClient: jest.fn().mockImplementation(() => ({ send: (...args: any[]) => mockSend(...args) })),
}));

const config = {
  dataSource: { people: { endpointConfig: { baseUrl: 'https://cdm.example.edu', apiKey: 'people-key' }, fetchPath: '/people' } }
} as unknown as Config;

describe('RunStarter', () => {
  it('makes a dashboard start with or without a principal', () => {
    expect(RunStarter.dashboard('jdoe')).toEqual({ by: 'dashboard', principal: 'jdoe' });
    expect(RunStarter.dashboard(' jdoe ')).toEqual({ by: 'dashboard', principal: 'jdoe' });
    expect(RunStarter.dashboard()).toEqual({ by: 'dashboard' });
    expect(RunStarter.dashboard('  ')).toEqual({ by: 'dashboard' });
  });

  it('reads one back, keeping only the known fields, and nothing from anything else', () => {
    expect(RunStarter.from({ by: 'schedule', extra: 1 })).toEqual({ by: 'schedule' });
    expect(RunStarter.from({ by: 'runner' })).toEqual({ by: 'runner' });
    expect(RunStarter.from({ by: 'dashboard', principal: 'jdoe', extra: 1 })).toEqual({ by: 'dashboard', principal: 'jdoe' });
    expect(RunStarter.from({ by: 'dashboard', principal: 42 })).toEqual({ by: 'dashboard' });
    for (const other of [undefined, null, 'schedule', { by: 'someone' }, {}]) {
      expect(RunStarter.from(other)).toBeUndefined();
    }
  });
});

describe('startedBy propagation', () => {
  beforeEach(() => {
    mockSend.mockClear();
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  const event = { baseUrl: 'https://cdm.example.edu', fetchPath: '/people', populationType: SyncPopulation.PersonFull };

  it('handleApiEvent puts it on the chunker queue message, and leaves it off when absent', async () => {
    await handleApiEvent({ ...event, startedBy: RunStarter.dashboard('jdoe') }, 'https://sqs.example.com/queue');
    await handleApiEvent(event, 'https://sqs.example.com/queue');

    expect(JSON.parse(mockSend.mock.calls[0][0].input.MessageBody).startedBy).toEqual({ by: 'dashboard', principal: 'jdoe' });
    expect(JSON.parse(mockSend.mock.calls[1][0].input.MessageBody)).not.toHaveProperty('startedBy');
  });

  it('the chunker reads it from the queue message, ignoring a value that is not one', () => {
    const read = (startedBy: unknown) => {
      const chunker = new ChunkFromAPI(JSON.parse(JSON.stringify(config)));
      chunker.setTaskParametersFromQueueMessage({ Body: JSON.stringify({ ...event, startedBy }) } as Message);
      return chunker.getStartedBy();
    };
    expect(read({ by: 'schedule' })).toEqual({ by: 'schedule' });
    expect(read({ by: 'someone' })).toBeUndefined();
    expect(read(undefined)).toBeUndefined();
  });

  it('S3-mode flags include it when present', async () => {
    const writeFile = jest.fn().mockResolvedValue(undefined);
    const metadata = new MetadataForS3({ config, storage: { writeFile } as any });

    await metadata.writeFlags({
      bucketName: 'bucket', chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z',
      bulkReset: false, trustPreviousStorage: true, syncPopulation: SyncPopulation.PersonFull, startedBy: RunStarter.RUNNER
    });

    expect(JSON.parse(writeFile.mock.calls[0][1]).startedBy).toEqual({ by: 'runner' });
  });
});
