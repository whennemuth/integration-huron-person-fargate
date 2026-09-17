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

  it('returns true (legacy blanket abort) when metadata exists without finalOffsetProcessed', async () => {
    mockRead({ chunkCount: 5 });
    expect(await new MetadataBroker(params).isAlreadyFinished(100)).toBe(true);
  });

  it('returns true (legacy blanket abort) when metadata exists but currentOffset is not supplied', async () => {
    mockRead({ chunkCount: 5, finalOffsetProcessed: 543 });
    expect(await new MetadataBroker(params).isAlreadyFinished()).toBe(true);
  });

  it('returns false (proceed) when currentOffset is at or below finalOffsetProcessed', async () => {
    mockRead({ finalOffsetProcessed: 543 });
    expect(await new MetadataBroker(params).isAlreadyFinished(430)).toBe(false);
    expect(await new MetadataBroker(params).isAlreadyFinished(543)).toBe(false);
  });

  it('returns true (abort) when currentOffset is strictly beyond finalOffsetProcessed', async () => {
    mockRead({ finalOffsetProcessed: 543 });
    expect(await new MetadataBroker(params).isAlreadyFinished(544)).toBe(true);
  });
});

describe('MetadataBroker.getFinalOffsetProcessed', () => {
  const config = {} as Config;
  const params = { config, bucketName: 'bucket', chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', region: 'us-east-2' };

  afterEach(() => jest.clearAllMocks());

  it('returns undefined when no metadata exists', async () => {
    (MetadataFactory.create as jest.Mock).mockReturnValue({ read: jest.fn().mockResolvedValue({}) });
    expect(await new MetadataBroker(params).getFinalOffsetProcessed()).toBeUndefined();
  });

  it('returns the recorded finalOffsetProcessed when present', async () => {
    (MetadataFactory.create as jest.Mock).mockReturnValue({ read: jest.fn().mockResolvedValue({ finalOffsetProcessed: 543 }) });
    expect(await new MetadataBroker(params).getFinalOffsetProcessed()).toBe(543);
  });
});

describe('MetadataBroker.isOffsetPastKnownEnd', () => {
  const config = {} as Config;
  const params = { config, bucketName: 'bucket', chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', region: 'us-east-2' };

  const mockRead = (result: any) => {
    (MetadataFactory.create as jest.Mock).mockReturnValue({ read: jest.fn().mockResolvedValue(result) });
  };

  afterEach(() => jest.clearAllMocks());

  it('returns false when no finalOffsetProcessed is recorded yet', async () => {
    mockRead({});
    expect(await new MetadataBroker(params).isOffsetPastKnownEnd(848)).toBe(false);
  });

  it('returns false when offset is at or below the recorded finalOffsetProcessed', async () => {
    mockRead({ finalOffsetProcessed: 808 });
    expect(await new MetadataBroker(params).isOffsetPastKnownEnd(808)).toBe(false);
    expect(await new MetadataBroker(params).isOffsetPastKnownEnd(700)).toBe(false);
  });

  it('returns true when offset is strictly beyond the recorded finalOffsetProcessed', async () => {
    mockRead({ finalOffsetProcessed: 808 });
    expect(await new MetadataBroker(params).isOffsetPastKnownEnd(809)).toBe(true);
  });
});
