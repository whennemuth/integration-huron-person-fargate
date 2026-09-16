import { Config } from 'integration-huron-person';
import { MetadataFactory } from '../src/chunking/metadata';

jest.mock('../src/chunking/metadata', () => ({
  MetadataFactory: { create: jest.fn() }
}));

import { chunkingAlreadyFinished, getFinalOffsetProcessed } from '../docker/chunker';

describe('chunkingAlreadyFinished', () => {
  const config = {} as Config;
  const params = { bucketName: 'bucket', chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', region: 'us-east-2' };

  const mockRead = (result: any) => {
    (MetadataFactory.create as jest.Mock).mockReturnValue({ read: jest.fn().mockResolvedValue(result) });
  };

  afterEach(() => jest.clearAllMocks());

  it('returns false when no metadata exists yet', async () => {
    mockRead({});
    expect(await chunkingAlreadyFinished(config, params)).toBe(false);
  });

  it('returns true (legacy blanket abort) when metadata exists without finalOffsetProcessed', async () => {
    mockRead({ chunkCount: 5 });
    expect(await chunkingAlreadyFinished(config, params, 100)).toBe(true);
  });

  it('returns true (legacy blanket abort) when metadata exists but currentOffset is not supplied', async () => {
    mockRead({ chunkCount: 5, finalOffsetProcessed: 543 });
    expect(await chunkingAlreadyFinished(config, params)).toBe(true);
  });

  it('returns false (proceed) when currentOffset is at or below finalOffsetProcessed', async () => {
    mockRead({ finalOffsetProcessed: 543 });
    expect(await chunkingAlreadyFinished(config, params, 430)).toBe(false);
    expect(await chunkingAlreadyFinished(config, params, 543)).toBe(false);
  });

  it('returns true (abort) when currentOffset is strictly beyond finalOffsetProcessed', async () => {
    mockRead({ finalOffsetProcessed: 543 });
    expect(await chunkingAlreadyFinished(config, params, 544)).toBe(true);
  });
});

describe('getFinalOffsetProcessed', () => {
  const config = {} as Config;
  const params = { bucketName: 'bucket', chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z', region: 'us-east-2' };

  afterEach(() => jest.clearAllMocks());

  it('returns undefined when no metadata exists', async () => {
    (MetadataFactory.create as jest.Mock).mockReturnValue({ read: jest.fn().mockResolvedValue({}) });
    expect(await getFinalOffsetProcessed(config, params)).toBeUndefined();
  });

  it('returns the recorded finalOffsetProcessed when present', async () => {
    (MetadataFactory.create as jest.Mock).mockReturnValue({ read: jest.fn().mockResolvedValue({ finalOffsetProcessed: 543 }) });
    expect(await getFinalOffsetProcessed(config, params)).toBe(543);
  });
});
