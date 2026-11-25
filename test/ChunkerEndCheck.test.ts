/**
 * docker/chunker.ts main(): the end-of-run check runs at start-up and again just before fetching, so
 * a task whose start-up (above all the person cache) outlasts another task reaching the end of the
 * records cancels instead of fetching and writing a duplicate chunk (preview, 2026-11-12).
 */

// Everything main() does, in order, for the assertions
const mockCalls: string[] = [];
// isAlreadyFinished() answers, in order (start-up, then before fetching)
let mockFinishedAnswers: boolean[] = [];
// The message's previous state source, whether a baseline exists, and the Flags main() wrote
let mockPreviousStateSource: 'storage' | 'target' | 'storage-then-target' | undefined = 'target';
let mockBaselineExists = true;
let mockWrittenFlags: any;

jest.mock('../src/TaskProtection', () => ({
  TaskProtection: class {
    enable = jest.fn().mockResolvedValue(undefined);
    disable = jest.fn().mockResolvedValue(undefined);
  }
}));

jest.mock('../src/Utils', () => ({
  getConfig: jest.fn().mockResolvedValue({}),
  getLocalConfig: jest.fn(),
  objectExistsInS3: jest.fn().mockImplementation(async () => mockBaselineExists)
}));

jest.mock('../src/chunking/ChunkerQueue', () => ({
  ChunkerQueue: class {
    hasAtomicCounterSupport = () => false;
    popMessageFromQueue = jest.fn().mockResolvedValue(undefined);
    deleteMessageFromQueue = jest.fn().mockResolvedValue(undefined);
  }
}));

jest.mock('../src/chunking/filedrop/ChunkFromS3', () => ({
  ChunkFromS3: class {
    hasSufficientTaskInfo = () => false;
    setTaskParametersFromQueueMessage = jest.fn();
  }
}));

jest.mock('../src/chunking/fetch/ChunkFromAPI', () => ({
  ChunkFromAPI: class {
    hasSufficientConfig = () => true;
    hasSufficientTaskInfo = () => true;
    setTaskParametersFromQueueMessage = jest.fn();
    setChunkOrdinalAllocator = jest.fn();
    getChunkDirectory = () => 'chunks/person-full/2026-11-12T17:04:00.376Z';
    getIterationLimitAndOffset = () => ({ offset: 0, iterationLimit: 10 });
    getMessage = () => ({ ReceiptHandle: 'receipt' });
    getPreviousStateSource = () => mockPreviousStateSource;
    getSyncPopulation = () => 'person-full';
    getPersonRecordProcessorAddons = () => undefined;
    getTermsEndpoint = () => undefined;
    getStartedBy = () => undefined;
    getProcessorScalingCapacity = () => undefined;
    sendNextChunkingMessage = jest.fn().mockImplementation(async () => { mockCalls.push('sendNextChunkingMessage'); return false; });
    runChunking = jest.fn().mockImplementation(async () => { mockCalls.push('runChunking'); });
  }
}));

jest.mock('../src/chunking/metadata', () => ({
  MetadataBroker: class {
    isTerminalErrorEncountered = jest.fn().mockResolvedValue(false);
    isAlreadyFinished = jest.fn().mockImplementation(async () => {
      mockCalls.push('isAlreadyFinished');
      return mockFinishedAnswers.shift() ?? false;
    });
    hasAnyTaskEncounteredPartial = jest.fn().mockResolvedValue(false);
    writeFlags = jest.fn().mockImplementation(async (params: any) => { mockCalls.push('writeFlags'); mockWrittenFlags = params; });
    markRunFailed = jest.fn().mockResolvedValue(undefined);
  }
}));

jest.mock('../src/person-cache/PersonCacheFactory', () => ({
  PersonCacheFactory: {
    create: () => ({
      ensureCache: jest.fn().mockImplementation(async () => {
        mockCalls.push('ensureCache');
        return { existed: false, created: true, skipped: false };
      })
    })
  }
}));

import { cancelIfAlreadyFinished, main } from '../docker/chunker';

describe('chunker main(): the end-of-run check before fetching', () => {
  let originalEnv: NodeJS.ProcessEnv;
  let exitSpy: jest.SpyInstance;
  let logSpy: jest.SpyInstance;

  beforeEach(() => {
    originalEnv = { ...process.env };
    process.env.CHUNKS_BUCKET = 'chunks-bucket';
    process.env.REGION = 'us-east-2';
    process.env.PREVIOUS_STORAGE_TYPE = 's3';
    process.env.SQS_QUEUE_URL = 'https://sqs.example.com/queue';
    delete process.env.IS_ECS_TASK;
    delete process.env.ITEMS_PER_CHUNK;
    mockCalls.length = 0;
    mockPreviousStateSource = 'target';
    mockBaselineExists = true;
    mockWrittenFlags = undefined;
    exitSpy = jest.spyOn(process, 'exit').mockImplementation((() => undefined) as any);
    logSpy = jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'warn').mockImplementation();
  });

  afterEach(() => {
    process.env = originalEnv;
    jest.restoreAllMocks();
  });

  const logged = (text: string) => logSpy.mock.calls.some(([msg]) => String(msg).includes(text));

  it('cancels without fetching when another task reached the end while the cache was being built', async () => {
    mockFinishedAnswers = [false, true];

    await main();

    expect(mockCalls).toEqual([
      'isAlreadyFinished', 'sendNextChunkingMessage', 'writeFlags', 'ensureCache', 'isAlreadyFinished'
    ]);
    expect(mockCalls).not.toContain('runChunking');
    expect(logged('Cancelling before fetching')).toBe(true);
    expect(exitSpy).toHaveBeenCalledWith(0);
  });

  it('fetches after the start-up steps when the run has not reached its end', async () => {
    mockFinishedAnswers = [false, false];

    await main();

    expect(mockCalls).toEqual([
      'isAlreadyFinished', 'sendNextChunkingMessage', 'writeFlags', 'ensureCache', 'isAlreadyFinished', 'runChunking'
    ]);
    expect(exitSpy).toHaveBeenCalledWith(0);
  });

  it('still cancels at start-up, before chaining or building the cache', async () => {
    mockFinishedAnswers = [true];

    await main();

    expect(mockCalls).toEqual(['isAlreadyFinished']);
    expect(logged('created before the service "realized" it had reached the end')).toBe(true);
    expect(exitSpy).toHaveBeenCalledWith(0);
  });
});

describe('chunker main(): where the run\'s previous state comes from', () => {
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    originalEnv = { ...process.env };
    process.env.CHUNKS_BUCKET = 'chunks-bucket';
    process.env.REGION = 'us-east-2';
    process.env.PREVIOUS_STORAGE_TYPE = 's3';
    process.env.SQS_QUEUE_URL = 'https://sqs.example.com/queue';
    delete process.env.IS_ECS_TASK;
    mockCalls.length = 0;
    mockFinishedAnswers = [false, false];
    mockWrittenFlags = undefined;
    jest.spyOn(process, 'exit').mockImplementation((() => undefined) as any);
    jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'warn').mockImplementation();
    jest.spyOn(console, 'error').mockImplementation();
  });

  afterEach(() => {
    process.env = originalEnv;
    jest.restoreAllMocks();
  });

  it.each([
    ['storage', true, 'storage', false],
    ['storage', false, 'target', true],
    ['target', true, 'target', true],
    ['target', false, 'target', true],
    // Only the people with no stored hash are looked up, directly: no person cache
    ['storage-then-target', true, 'storage-then-target', false],
    ['storage-then-target', false, 'target', true]
  ] as const)('a message asking for %p with a baseline %p records %p, building the person cache: %p',
    async (fromMessage, baselineExists, recorded, buildsCache) => {
      mockPreviousStateSource = fromMessage;
      mockBaselineExists = baselineExists;

      await main();

      expect(mockWrittenFlags.previousStateSource).toBe(recorded);
      expect(mockWrittenFlags).not.toHaveProperty('bulkReset');
      expect(mockWrittenFlags).not.toHaveProperty('trustPreviousStorage');
      expect(mockCalls.includes('ensureCache')).toBe(buildsCache);
      expect(mockCalls).toContain('runChunking');
    });

  it('writes no Flags and fetches nothing for a message without one (when a baseline exists)', async () => {
    mockPreviousStateSource = undefined;
    mockBaselineExists = true;

    await main();

    expect(mockCalls).not.toContain('writeFlags');
    expect(mockCalls).not.toContain('runChunking');
  });
});

describe('cancelIfAlreadyFinished', () => {
  it('answers false, logging nothing, while the run has not reached its end', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation();
    const broker = { isAlreadyFinished: jest.fn().mockResolvedValue(false) };

    expect(await cancelIfAlreadyFinished(broker as any, {} as any, 'before-fetch')).toBe(false);
    expect(logSpy).not.toHaveBeenCalled();
    logSpy.mockRestore();
  });
});
