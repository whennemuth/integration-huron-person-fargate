/**
 * Where a run's previous state comes from (src/chunking/PreviousStateSource.ts): `storage` or `target`, travelling the
 * run's messages as previousStateSource in place of the bulkReset / trustPreviousStorage pair. The pipeline has no
 * default: an event or message that names neither is refused. The pair is still translated, from senders not yet moved
 * over and from what was queued or recorded before. (The chunker's choice, and its person cache: ChunkerEndCheck.test.ts;
 * the queue message: ChunkFromAPI.test.ts; the summary: StatisticsTable.test.ts; Run history: DashboardRunHistory.test.ts.)
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

beforeEach(() => {
  mockSend.mockClear();
  jest.spyOn(console, 'log').mockImplementation(() => undefined);
  jest.spyOn(console, 'error').mockImplementation(() => undefined);
});
afterEach(() => jest.restoreAllMocks());

describe('previousStateSourceIn()', () => {
  it.each([
    [{ previousStateSource: 'storage' }, 'storage'],
    [{ previousStateSource: 'target' }, 'target'],
    // The pair, as a message or record from before previousStateSource carried it
    [{ bulkReset: false, trustPreviousStorage: true }, 'storage'],
    [{ bulkReset: true, trustPreviousStorage: true }, 'target'],
    [{ bulkReset: false, trustPreviousStorage: false }, 'target'],
    [{ bulkReset: true, trustPreviousStorage: false }, 'target'],
    // Booleans that travelled as strings
    [{ bulkReset: 'false', trustPreviousStorage: 'true' }, 'storage'],
    [{ bulkReset: 'TRUE', trustPreviousStorage: 'true' }, 'target'],
    // The pipeline's meaning of a half-given pair: an unset trustPreviousStorage was false (a reset)
    [{ bulkReset: false }, 'target'],
    [{ trustPreviousStorage: true }, 'storage'],
    // Agreeing with the pair is fine
    [{ previousStateSource: 'target', bulkReset: true, trustPreviousStorage: false }, 'target']
  ])('reads %j as %p', (fields, expected) => {
    expect(previousStateSourceIn(fields)).toBe(expected);
  });

  it.each([{}, { previousStateSource: '' }, { previousStateSource: ' ', bulkReset: null, trustPreviousStorage: '' }])(
    'reads nothing from %j', fields => {
      expect(previousStateSourceIn(fields)).toBeUndefined();
    });

  it('throws for a value it does not know, or a contradiction', () => {
    expect(() => previousStateSourceIn({ previousStateSource: 'bulk' })).toThrow('Invalid previousStateSource');
    expect(() => previousStateSourceIn({ previousStateSource: 'storage', bulkReset: true, trustPreviousStorage: true }))
      .toThrow('contradicts');
  });
});

describe('previousStateRefusal() and requirePreviousStateSource()', () => {
  it('refuses fields that name neither, rather than default', () => {
    expect(previousStateRefusal({})).toContain('No previousStateSource');
    expect(() => requirePreviousStateSource({}, 'The message')).toThrow('The message has no previousStateSource');
  });

  it('refuses an invalid value with its reason, and accepts a good one', () => {
    expect(previousStateRefusal({ previousStateSource: 'bulk' })).toContain('Invalid previousStateSource');
    expect(previousStateRefusal({ previousStateSource: 'storage' })).toBeUndefined();
    expect(requirePreviousStateSource({ bulkReset: true, trustPreviousStorage: true }, 'The message')).toBe('target');
  });
});

describe('the chunker subscriber', () => {
  it('refuses a run start that names neither, before any other check', () => {
    expect(runStartRefusal({ ...event } as ApiChunkerEvent, {})).toContain('No previousStateSource');
    expect(runStartRefusal({ ...event, previousStateSource: 'storage' }, {})).toBeUndefined();
    expect(runStartRefusal({ ...event, bulkReset: true, trustPreviousStorage: false }, {})).toBeUndefined();
  });

  it('settles the event on previousStateSource alone, so the seeded and chained messages carry only it', () => {
    const settled = withPreviousStateSource({ ...event, bulkReset: false, trustPreviousStorage: true });
    expect(settled.previousStateSource).toBe('storage');
    expect(settled).not.toHaveProperty('bulkReset');
    expect(settled).not.toHaveProperty('trustPreviousStorage');
  });
});

describe('handleApiEvent()', () => {
  const sent = () => JSON.parse(mockSend.mock.calls[0][0].input.MessageBody);

  it.each([
    [{ previousStateSource: 'target' as const }, 'target'],
    [{ bulkReset: false, trustPreviousStorage: true }, 'storage'],
    [{ bulkReset: true, trustPreviousStorage: true }, 'target']
  ])('queues %j as previousStateSource %p, never the pair', async (fields, expected) => {
    expect((await handleApiEvent({ ...event, ...fields }, QUEUE_URL)).statusCode).toBe(200);
    expect(sent().previousStateSource).toBe(expected);
    expect(sent()).not.toHaveProperty('bulkReset');
    expect(sent()).not.toHaveProperty('trustPreviousStorage');
  });

  it('refuses with a 400, queuing nothing, an event that names neither (a seeded or chained message included)', async () => {
    const result = await handleApiEvent({ ...event }, QUEUE_URL);
    expect(result.statusCode).toBe(400);
    expect(result.body).toContain('No previousStateSource');
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('refuses a contradiction with a 400', async () => {
    const result = await handleApiEvent({ ...event, previousStateSource: 'storage', bulkReset: true }, QUEUE_URL);
    expect(result.statusCode).toBe(400);
    expect(mockSend).not.toHaveBeenCalled();
  });
});

describe("the processors' resolveCommonFlags()", () => {
  it('reads the Flags previousStateSource', () => {
    expect(resolveCommonFlags({ previousStateSource: 'target' }, 'false').previousStateSource).toBe('target');
    expect(resolveCommonFlags({ previousStateSource: 'storage' }, 'true').previousStateSource).toBe('storage');
  });

  it("translates the Flags of a run underway when previousStateSource was deployed", () => {
    expect(resolveCommonFlags({ bulkReset: false, trustPreviousStorage: true } as any).previousStateSource).toBe('storage');
    expect(resolveCommonFlags({ bulkReset: true, trustPreviousStorage: true } as any).previousStateSource).toBe('target');
  });

  it("falls back to the task definition's BULK_RESET for Flags without either, as before", () => {
    expect(resolveCommonFlags({}, 'true').previousStateSource).toBe('target');
    expect(resolveCommonFlags({}, 'false').previousStateSource).toBe('storage');
    expect(resolveCommonFlags({}).previousStateSource).toBe('storage');
  });
});
