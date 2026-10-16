import { MetadataFactoryForBootstrap } from '../src/chunking/metadata';
import { PostProcessorForDynamoDB } from '../src/post-processing/PostProcessorForDynamoDB';
import { PostProcessorForS3 } from '../src/post-processing/PostProcessorForS3';
import { DeferredDeleteHandlerForDynamoDB } from '../src/post-processing/DeferredDeleteHandlerForDynamoDB';
import { DeferredDeleteHandlerForS3 } from '../src/post-processing/DeferredDeleteHandlerForS3';
import { SyncPopulation } from '../docker/chunkTypes';

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

    await (postProcessor as any).runDeferredDeletes({ outputKey: `${chunkDir}/merged.ndjson` });

    expect(terminalErrorExists).toHaveBeenCalled();
    expect(DeferredDeleteHandlerForS3).not.toHaveBeenCalled();
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
