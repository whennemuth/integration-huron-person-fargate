/**
 * Where a run's previous state comes from (src/chunking/PreviousStateSource.ts): `storage` or `target`, travelling the
 * run's messages as previousStateSource. The pipeline has no default: an event or message that doesn't name it is
 * refused, as is one carrying the bulkReset / trustPreviousStorage pair it replaced. (The chunker's choice, and its
 * person cache: ChunkerEndCheck.test.ts; the queue message: ChunkFromAPI.test.ts and ChunkFromS3.test.ts; the summary:
 * StatisticsTable.test.ts; Run history, which still shows old runs' bulkReset: DashboardRunHistory.test.ts.)
 */
import { SyncPopulation } from '../docker/chunkTypes';
import { ApiChunkerEvent, runStartRefusal, withPreviousStateSource } from '../src/chunking/ChunkerSubscriber';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { previousStateRefusal, previousStateSourceIn, requirePreviousStateSource } from '../src/chunking/PreviousStateSource';
import { resolveCommonFlags } from '../src/processing/ProcessorCommon';

const mockSend = jest.fn().mockResolvedValue({});
jest.mock('@aws-sdk/client-sqs', () => ({
  ...jest.requireActual('@aws-sdk/client-sqs'),
  SQSClient: jest.fn().mockImplementation(() => ({ send: (...args: any[]) => mockSend(...args) })),
}));

const QUEUE_URL = 'https://sqs.example.com/queue';
const event = { baseUrl: 'https://cdm.example.edu', fetchPath: '/people', populationType: SyncPopulation.PersonFull };
/** An event, message or record carrying the pair previousStateSource replaced (untyped, as one from elsewhere would be) */
const withPair = (fields: object, pair: object) => ({ ...fields, ...pair }) as any;

beforeEach(() => {
  mockSend.mockClear();
  jest.spyOn(console, 'log').mockImplementation(() => undefined);
  jest.spyOn(console, 'error').mockImplementation(() => undefined);
});
afterEach(() => jest.restoreAllMocks());

describe('previousStateSourceIn()', () => {
  it.each(['storage', 'target'])('reads %p', value => {
    expect(previousStateSourceIn({ previousStateSource: value })).toBe(value);
  });

  it.each([{}, { previousStateSource: '' }, { previousStateSource: ' ', bulkReset: null, trustPreviousStorage: '' }])(
    'reads nothing from %j', fields => {
      expect(previousStateSourceIn(fields)).toBeUndefined();
    });

  it('throws for a value it does not know', () => {
    expect(() => previousStateSourceIn({ previousStateSource: 'bulk' })).toThrow('Invalid previousStateSource');
  });

  it.each([
    { bulkReset: false, trustPreviousStorage: true },
    { bulkReset: 'TRUE' },
    { trustPreviousStorage: 'false' },
    { previousStateSource: 'target', bulkReset: true }
  ])('throws for %j, which carries the replaced pair, naming it', fields => {
    expect(() => previousStateSourceIn(fields)).toThrow('no longer accepted');
  });
});

describe('previousStateRefusal() and requirePreviousStateSource()', () => {
  it('refuses fields that name nothing, rather than default', () => {
    expect(previousStateRefusal({})).toContain('No previousStateSource');
    expect(() => requirePreviousStateSource({}, 'The message')).toThrow('The message has no previousStateSource');
  });

  it('refuses an invalid value or the replaced pair with its reason, and accepts a good one', () => {
    expect(previousStateRefusal({ previousStateSource: 'bulk' })).toContain('Invalid previousStateSource');
    expect(previousStateRefusal({ bulkReset: true, trustPreviousStorage: false })).toContain('no longer accepted');
    expect(previousStateRefusal({ previousStateSource: 'storage' })).toBeUndefined();
    expect(requirePreviousStateSource({ previousStateSource: 'target' }, 'The message')).toBe('target');
  });
});

describe('the chunker subscriber', () => {
  it('refuses a run start that names nothing, or the replaced pair, before any other check', () => {
    expect(runStartRefusal({ ...event } as ApiChunkerEvent, {})).toContain('No previousStateSource');
    expect(runStartRefusal(withPair(event, { bulkReset: true, trustPreviousStorage: false }), {})).toContain('no longer accepted');
    expect(runStartRefusal({ ...event, previousStateSource: 'storage' }, {})).toBeUndefined();
  });

  it('settles the event on its previousStateSource for the seeded and chained messages', () => {
    expect(withPreviousStateSource({ ...event, previousStateSource: 'storage' }).previousStateSource).toBe('storage');
  });
});

describe('handleApiEvent()', () => {
  const sent = () => JSON.parse(mockSend.mock.calls[0][0].input.MessageBody);

  it.each(['storage', 'target'] as const)('queues previousStateSource %p', async previousStateSource => {
    expect((await handleApiEvent({ ...event, previousStateSource }, QUEUE_URL)).statusCode).toBe(200);
    expect(sent().previousStateSource).toBe(previousStateSource);
  });

  it('refuses with a 400, queuing nothing, an event that names nothing (a seeded or chained message included)', async () => {
    const result = await handleApiEvent({ ...event }, QUEUE_URL);
    expect(result.statusCode).toBe(400);
    expect(result.body).toContain('No previousStateSource');
    expect(mockSend).not.toHaveBeenCalled();
  });

  it.each([{ bulkReset: false, trustPreviousStorage: true }, { previousStateSource: 'storage', bulkReset: true }])(
    'refuses with a 400, queuing nothing, an event carrying the replaced pair: %j', async fields => {
      const result = await handleApiEvent(withPair(event, fields), QUEUE_URL);
      expect(result.statusCode).toBe(400);
      expect(result.body).toContain('no longer accepted');
      expect(mockSend).not.toHaveBeenCalled();
    });
});

describe("the processors' resolveCommonFlags()", () => {
  it('reads the Flags previousStateSource', () => {
    expect(resolveCommonFlags({ previousStateSource: 'target' }, 'storage').previousStateSource).toBe('target');
    expect(resolveCommonFlags({ previousStateSource: 'storage' }, 'target').previousStateSource).toBe('storage');
  });

  it('refuses Flags carrying the replaced pair', () => {
    expect(() => resolveCommonFlags({ bulkReset: false, trustPreviousStorage: true } as any)).toThrow('no longer accepted');
  });

  it('falls back to PREVIOUS_STATE_SOURCE for Flags without it (a local run), and refuses rather than guess', () => {
    expect(resolveCommonFlags({}, 'target').previousStateSource).toBe('target');
    expect(resolveCommonFlags({}, 'storage').previousStateSource).toBe('storage');
    expect(() => resolveCommonFlags({})).toThrow('no previousStateSource');
    expect(() => resolveCommonFlags({}, 'true')).toThrow('Invalid previousStateSource');
  });
});
