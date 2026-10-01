import { Config } from 'integration-huron-person';
import { MetadataFactory } from '../src/chunking/metadata/MetadataFactory';
import { MetadataBroker } from '../src/chunking/metadata';

jest.mock('../src/chunking/metadata/MetadataFactory', () => ({
  MetadataFactory: { create: jest.fn() }
}));

describe('MetadataBroker.isAlreadyFinished', () => {
  const config = {} as Config;
  const params = { config, bucketName: 'bucket', chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', region: 'us-east-2' };

  const mockRead = (result: any) => {
    (MetadataFactory.create as jest.Mock).mockReturnValue({ read: jest.fn().mockResolvedValue(result) });
  };

  afterEach(() => jest.clearAllMocks());

  it('returns false when no metadata exists yet', async () => {
    mockRead({});
    expect(await new MetadataBroker(params).isAlreadyFinished()).toBe(false);
  });

  it('returns false when metadata exists but partialOrEmptyChunkEncountered is not set', async () => {
    mockRead({ chunkCount: 5 });
    expect(await new MetadataBroker(params).isAlreadyFinished()).toBe(false);
  });

  it('returns true once partialOrEmptyChunkEncountered is true, regardless of any other fields', async () => {
    mockRead({ chunkCount: 5, partialOrEmptyChunkEncountered: true });
    expect(await new MetadataBroker(params).isAlreadyFinished()).toBe(true);
  });
});

describe('MetadataBroker.getRunningTotalRecords', () => {
  const config = {} as Config;
  const params = { config, bucketName: 'bucket', chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', region: 'us-east-2' };

  const mockRead = (result: any) => {
    (MetadataFactory.create as jest.Mock).mockReturnValue({ read: jest.fn().mockResolvedValue(result) });
  };

  afterEach(() => jest.clearAllMocks());

  it('returns 0 when no metadata exists yet', async () => {
    mockRead(undefined);
    expect(await new MetadataBroker(params).getRunningTotalRecords()).toBe(0);
  });

  it('returns the accumulated totalRecords', async () => {
    mockRead({ totalRecords: 1234 });
    expect(await new MetadataBroker(params).getRunningTotalRecords()).toBe(1234);
  });
});
