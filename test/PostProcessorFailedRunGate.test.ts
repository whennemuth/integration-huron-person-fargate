import { MetadataFactoryForBootstrap } from '../src/chunking/metadata';
import { PostProcessorForDynamoDB } from '../src/post-processing/PostProcessorForDynamoDB';
import { PostProcessorForS3 } from '../src/post-processing/PostProcessorForS3';
import { DeferredDeleteHandlerForDynamoDB } from '../src/post-processing/DeferredDeleteHandlerForDynamoDB';
import { DeferredDeleteHandlerForS3 } from '../src/post-processing/DeferredDeleteHandlerForS3';
import { SyncPopulation } from '../docker/chunkTypes';
import { StatisticsTable } from '../src/dynamodb/StatisticsTable';

jest.mock('../src/post-processing/DeferredDeleteHandlerForDynamoDB');
jest.mock('../src/post-processing/DeferredDeleteHandlerForS3');
jest.mock('../src/chunking/metadata', () => ({
  ...jest.requireActual('../src/chunking/metadata'),
  MetadataFactoryForBootstrap: jest.fn()
}));

describe('Post-processor deferred deletes are never attempted for a failed run', () => {
  const chunkDir = 'chunks/person-full/2026-10-01T00:00:00.000Z';
  const postProcessorContext = {
    bucketName: 'bucket', chunkDir, region: 'us-east-2', dryRun: false,
    syncRunId: '2026-10-01T00:00:00.000Z', sharedDeltaStorageDir: 'delta-storage',
    primaryKeyFieldNames: ['personid'], primaryKeyFieldSet: new Set(['personid'])
  };

  const mockResolve = (terminalErrorExists: boolean) => {
    const terminalErrorExistsFn = jest.fn().mockResolvedValue(terminalErrorExists);
    (MetadataFactoryForBootstrap as unknown as jest.Mock).mockImplementation(() => ({
      readFlagsForBootstrap: jest.fn().mockResolvedValue({
        metadata: { terminalErrorExists: terminalErrorExistsFn },
        flags: { syncPopulation: SyncPopulation.PersonFull }
      })
    }));
    return terminalErrorExistsFn;
  };

  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.clearAllMocks();
  });

  it('PostProcessorForDynamoDB skips deletion handling when a terminal error marker exists', async () => {
    const terminalErrorExists = mockResolve(true);
    const postProcessor = new PostProcessorForDynamoDB();
    jest.spyOn(postProcessor, 'getPostProcessorContext').mockResolvedValue(postProcessorContext as any);

    await (postProcessor as any).runDeferredDeletes({ chunkDir, syncRunId: postProcessorContext.syncRunId });

    expect(terminalErrorExists).toHaveBeenCalledWith({ bucketName: 'bucket', chunkDirectory: chunkDir, region: 'us-east-2' });
    expect(DeferredDeleteHandlerForDynamoDB).not.toHaveBeenCalled();
  });

  it('PostProcessorForS3 skips deletion handling when a terminal error marker exists', async () => {
    const terminalErrorExists = mockResolve(true);
    const postProcessor = new PostProcessorForS3();
    jest.spyOn(postProcessor, 'getPostProcessorContext').mockResolvedValue(postProcessorContext as any);

    const outcome = await (postProcessor as any).runDeferredDeletes({ outputKey: `${chunkDir}/merged.ndjson` });

    expect(terminalErrorExists).toHaveBeenCalled();
    expect(DeferredDeleteHandlerForS3).not.toHaveBeenCalled();
    expect(outcome).toMatchObject({ status: 'SKIPPED', reason: 'TERMINAL_ERROR' });
  });

  it('PostProcessorForDynamoDB reports a terminal-error skip as its deletion outcome', async () => {
    mockResolve(true);
    const postProcessor = new PostProcessorForDynamoDB();
    jest.spyOn(postProcessor, 'getPostProcessorContext').mockResolvedValue(postProcessorContext as any);

    const outcome = await (postProcessor as any).runDeferredDeletes({ chunkDir, syncRunId: postProcessorContext.syncRunId });

    expect(outcome).toMatchObject({ status: 'SKIPPED', reason: 'TERMINAL_ERROR' });
  });

  it('PostProcessorForS3 reports completed deletions as its deletion outcome', async () => {
    mockResolve(false);
    const result = { deletedCount: 2, failedCount: 1, totalProcessed: 3, message: 'done' };
    (DeferredDeleteHandlerForS3 as unknown as jest.Mock).mockImplementation(() => ({
      processDeletes: jest.fn().mockResolvedValue(result)
    }));
    const postProcessor = new PostProcessorForS3();
    jest.spyOn(postProcessor, 'getPostProcessorContext').mockResolvedValue(postProcessorContext as any);

    const outcome = await (postProcessor as any).runDeferredDeletes({ outputKey: `${chunkDir}/merged.ndjson` });

    expect(outcome).toEqual({ status: 'COMPLETED', ...result });
  });

  it('PostProcessorForS3 reports a deletion error as a FAILED outcome', async () => {
    mockResolve(false);
    (DeferredDeleteHandlerForS3 as unknown as jest.Mock).mockImplementation(() => ({
      processDeletes: jest.fn().mockRejectedValue(new Error('target api down'))
    }));
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    const postProcessor = new PostProcessorForS3();
    jest.spyOn(postProcessor, 'getPostProcessorContext').mockResolvedValue(postProcessorContext as any);

    const outcome = await (postProcessor as any).runDeferredDeletes({ outputKey: `${chunkDir}/merged.ndjson` });

    expect(outcome).toEqual({ status: 'FAILED', message: 'target api down' });
  });

  it('PostProcessorForS3 proceeds to deletion handling when the run is not marked failed', async () => {
    mockResolve(false);
    (DeferredDeleteHandlerForS3 as unknown as jest.Mock).mockImplementation(() => ({
      processDeletes: jest.fn().mockResolvedValue({})
    }));
    const postProcessor = new PostProcessorForS3();
    jest.spyOn(postProcessor, 'getPostProcessorContext').mockResolvedValue(postProcessorContext as any);

    await (postProcessor as any).runDeferredDeletes({ outputKey: `${chunkDir}/merged.ndjson` }).catch(() => undefined);

    expect(DeferredDeleteHandlerForS3).toHaveBeenCalled();
  });
});

describe('Post-processor run summary', () => {
  const chunkDir = 'chunks/person-full/2026-10-01T00:00:00.000Z';
  const env = process.env;

  beforeEach(() => {
    process.env = { ...env, DYNAMODB_STATISTICS_TABLE_NAME: 'stats', REGION: 'us-east-2', DRY_RUN: 'true', PREVIOUS_STORAGE_TYPE: 'DynamoDB' };
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    process.env = env;
    jest.restoreAllMocks();
  });

  it('builds the summary for the run in the chunk directory and writes it', async () => {
    const summary = { syncRunId: '2026-10-01T00:00:00.000Z', status: 'SUCCEEDED' };
    const buildRunSummary = jest.fn().mockResolvedValue(summary);
    const writeRunSummary = jest.fn().mockResolvedValue(undefined);
    jest.spyOn(StatisticsTable, 'fromTableName').mockReturnValue({ buildRunSummary, writeRunSummary } as any);
    const deletion = { status: 'SKIPPED', reason: 'DELETES_DISABLED', message: 'off' };

    await (new PostProcessorForS3() as any).writeRunSummary(chunkDir, true, deletion);

    expect(StatisticsTable.fromTableName).toHaveBeenCalledWith('stats', 'us-east-2');
    expect(buildRunSummary).toHaveBeenCalledWith('2026-10-01T00:00:00.000Z', {
      postProcessingSucceeded: true, storageMode: 'dynamodb', dryRun: true, deletion
    });
    expect(writeRunSummary).toHaveBeenCalledWith(summary);
  });

  it('never throws when the summary cannot be written', async () => {
    jest.spyOn(StatisticsTable, 'fromTableName').mockReturnValue({
      buildRunSummary: jest.fn().mockRejectedValue(new Error('throttled'))
    } as any);

    await expect((new PostProcessorForS3() as any).writeRunSummary(chunkDir, false)).resolves.toBeUndefined();
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('throttled'));
  });

  it('skips the summary when the chunk directory is unknown', async () => {
    const fromTableName = jest.spyOn(StatisticsTable, 'fromTableName');

    await (new PostProcessorForS3() as any).writeRunSummary(undefined, false);

    expect(fromTableName).not.toHaveBeenCalled();
  });
});
