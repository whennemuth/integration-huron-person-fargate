import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { MetadataManager } from '../src/chunking/Metadata';
import { mockClient } from 'aws-sdk-client-mock';

const s3Mock = mockClient(S3Client);

describe('Metadata Aggregation Helpers', () => {
  const bucketName = 'test-bucket';
  const chunkDirectory = 'chunks/person-full/2026-05-26T15:00:00.000Z';
  const region = 'us-east-2';

  beforeEach(() => {
    s3Mock.reset();
  });

  describe('listChunkFiles', () => {
    it('should list all chunk files in directory', async () => {
      const chunkFiles = [
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0000.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0001.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0002.ndjson',
      ];

      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: chunkFiles.map(key => ({ Key: key })),
        IsTruncated: false,
      });

      const result = await MetadataManager.listChunkFiles(bucketName, chunkDirectory, region);

      expect(result).toEqual(chunkFiles);
      expect(result.length).toBe(3);
    });

    it('should handle pagination with continuation tokens', async () => {
      let callCount = 0;
      s3Mock.on(ListObjectsV2Command).callsFake(async () => {
        callCount++;
        if (callCount === 1) {
          // First page
          return {
            Contents: [
              { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0000.ndjson' },
              { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0001.ndjson' },
            ],
            IsTruncated: true,
            NextContinuationToken: 'token-1',
          };
        } else {
          // Second page
          return {
            Contents: [
              { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0002.ndjson' },
              { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0003.ndjson' },
            ],
            IsTruncated: false,
          };
        }
      });

      const result = await MetadataManager.listChunkFiles(bucketName, chunkDirectory, region);

      expect(result.length).toBe(4);
      expect(result).toEqual([
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0000.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0001.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0002.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0003.ndjson',
      ]);
    });

    it('should filter for chunk-*.ndjson files only', async () => {
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [
          { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0000.ndjson' },
          { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/_metadata.json' },
          { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0001.ndjson' },
          { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/_flags.json' },
        ],
        IsTruncated: false,
      });

      const result = await MetadataManager.listChunkFiles(bucketName, chunkDirectory, region);

      expect(result).toEqual([
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0000.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0001.ndjson',
      ]);
    });

    it('should sort chunks by number', async () => {
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [
          { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0010.ndjson' },
          { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0002.ndjson' },
          { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0001.ndjson' },
          { Key: 'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0100.ndjson' },
        ],
        IsTruncated: false,
      });

      const result = await MetadataManager.listChunkFiles(bucketName, chunkDirectory, region);

      expect(result).toEqual([
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0001.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0002.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0010.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0100.ndjson',
      ]);
    });

    it('should throw if no chunk files found', async () => {
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [],
        IsTruncated: false,
      });

      await expect(
        MetadataManager.listChunkFiles(bucketName, chunkDirectory, region)
      ).resolves.toEqual([]);
    });
  });

  describe('computeTotalRecords', () => {
    it('should sum NDJSON line counts across chunks', async () => {
      const chunkKeys = [
        'chunk-0000.ndjson',
        'chunk-0001.ndjson',
        'chunk-0002.ndjson',
      ];

      let callCount = 0;
      s3Mock.on(GetObjectCommand).callsFake(async () => {
        const responses = [
          { Body: { transformToString: async () => '{"id":1}\n{"id":2}\n{"id":3}' } },
          { Body: { transformToString: async () => '{"id":4}\n{"id":5}' } },
          { Body: { transformToString: async () => '{"id":6}' } },
        ];
        return responses[callCount++];
      });

      const result = await MetadataManager.computeTotalRecords(bucketName, chunkKeys, region);

      expect(result).toBe(6);
    });

    it('should handle empty chunks', async () => {
      const chunkKeys = [
        'chunk-0000.ndjson',
        'chunk-0001.ndjson',
      ];

      let callCount = 0;
      s3Mock.on(GetObjectCommand).callsFake(async () => {
        const responses = [
          { Body: { transformToString: async () => '{"id":1}\n{"id":2}' } },
          { Body: { transformToString: async () => '' } },
        ];
        return responses[callCount++];
      });

      const result = await MetadataManager.computeTotalRecords(bucketName, chunkKeys, region);

      expect(result).toBe(2);
    });

    it('should skip chunks that fail to read', async () => {
      const chunkKeys = [
        'chunk-0000.ndjson',
        'chunk-0001.ndjson',
        'chunk-0002.ndjson',
      ];

      let callCount = 0;
      s3Mock.on(GetObjectCommand).callsFake(async () => {
        if (callCount === 1) {
          callCount++;
          throw new Error('Read error');
        }
        const responses = [
          { Body: { transformToString: async () => '{"id":1}\n{"id":2}' } },
          null, // This is for callCount===1, will throw instead
          { Body: { transformToString: async () => '{"id":3}' } },
        ];
        return responses[callCount++];
      });

      const result = await MetadataManager.computeTotalRecords(bucketName, chunkKeys, region);

      expect(result).toBe(3);
    });

    it('should handle chunks with only whitespace', async () => {
      const chunkKeys = ['chunk-0000.ndjson'];

      s3Mock.on(GetObjectCommand).resolves({
        Body: { transformToString: async () => '\n\n   \n' } as any,
      });

      const result = await MetadataManager.computeTotalRecords(bucketName, chunkKeys, region);

      expect(result).toBe(0);
    });
  });

  describe('buildAggregatedMetadata', () => {
    it('should build complete aggregated metadata', async () => {
      const chunkFiles = [
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0000.ndjson',
        'chunks/person-full/2026-05-26T15:00:00.000Z/chunk-0001.ndjson',
      ];

      // Mock ListObjectsV2Command
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: chunkFiles.map(key => ({ Key: key })),
        IsTruncated: false,
      });

      // Mock GetObjectCommand for line counting
      let callCount = 0;
      s3Mock.on(GetObjectCommand).callsFake(async () => {
        const responses = [
          { Body: { transformToString: async () => '{"id":1}\n{"id":2}\n{"id":3}' } },
          { Body: { transformToString: async () => '{"id":4}\n{"id":5}\n{"id":6}\n{"id":7}' } },
        ];
        return responses[callCount++];
      });

      const result = await MetadataManager.buildAggregatedMetadata(bucketName, chunkDirectory, region);

      expect(result).toEqual({
        chunkKeys: chunkFiles,
        chunkCount: 2,
        totalRecords: 7,
      });
    });

    it('should throw error if no chunks found', async () => {
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [],
        IsTruncated: false,
      });

      await expect(
        MetadataManager.buildAggregatedMetadata(bucketName, chunkDirectory, region)
      ).rejects.toThrow('No chunk files found');
    });

    it('should verify chunkCount equals chunkKeys.length', async () => {
      const chunkFiles = Array.from({ length: 10 }, (_, i) =>
        `chunks/person-full/2026-05-26T15:00:00.000Z/chunk-${String(i).padStart(4, '0')}.ndjson`
      );

      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: chunkFiles.map(key => ({ Key: key })),
        IsTruncated: false,
      });

      // Mock all GetObjectCommands - each chunk has 2 records
      s3Mock.on(GetObjectCommand).callsFake(async () => ({
        Body: { transformToString: async () => '{"id":1}\n{"id":2}' },
      }));

      const result = await MetadataManager.buildAggregatedMetadata(bucketName, chunkDirectory, region);

      expect(result.chunkCount).toBe(result.chunkKeys.length);
      expect(result.chunkCount).toBe(10);
      expect(result.totalRecords).toBe(20);
    });

    it('should handle parallel scenario with many chunks (480+)', async () => {
      // Simulate 480 chunks written by parallel tasks
      const chunkFiles = Array.from({ length: 480 }, (_, i) =>
        `chunks/person-full/2026-05-26T15:00:00.000Z/chunk-${String(i).padStart(4, '0')}.ndjson`
      );

      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: chunkFiles.map(key => ({ Key: key })),
        IsTruncated: false,
      });

      // Mock all GetObjectCommands - each chunk has 200 records
      s3Mock.on(GetObjectCommand).callsFake(async () => ({
        Body: { 
          transformToString: async () => 
            Array.from({ length: 200 }, (_, j) => `{"id":${j}}`).join('\n')
        },
      }));

      const result = await MetadataManager.buildAggregatedMetadata(bucketName, chunkDirectory, region);

      expect(result.chunkCount).toBe(480);
      expect(result.chunkKeys.length).toBe(480);
      expect(result.totalRecords).toBe(480 * 200); // 96,000 records
    });
  });
});
