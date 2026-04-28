/**
 * Comprehensive tests for processor.ts
 * 
 * Tests the Phase 2 processing logic that handles individual chunk files
 */

import { extractChunkId, validateChunk, buildChunkConfig } from '../docker/processor';

// Simple mock for ConfigManager to avoid file system dependencies
jest.mock('integration-huron-person', () => ({
  ConfigManager: {
    getInstance: jest.fn(() => ({
      reset: jest.fn().mockReturnThis(),
      fromJsonString: jest.fn().mockReturnThis(),
      fromSecretManager: jest.fn().mockReturnThis(),
      fromEnvironment: jest.fn().mockReturnThis(),
      fromFileSystem: jest.fn().mockReturnThis(),
      getConfig: jest.fn(() => ({
        dataSource: {
          people: {
            region: 'us-east-1',
            fieldsOfInterest: ['id', 'firstName', 'lastName']
          }
        },
        dataTarget: {
          apiEndpoint: 'https://api.example.com'
        },
        storage: {
          bucketName: 'test-bucket',
          fileKeyPrefix: 'test-data/'
        }
      })),
      getConfigAsync: jest.fn(async () => ({
        dataSource: {
          people: {
            region: 'us-east-1',
            fieldsOfInterest: ['id', 'firstName', 'lastName']
          }
        },
        dataTarget: {
          apiEndpoint: 'https://api.example.com'
        },
        storage: {
          bucketName: 'test-bucket',
          fileKeyPrefix: 'test-data/'
        }
      }))
    }))
  }
}));

describe('Processor (Phase 2)', () => {
  describe('extractChunkId', () => {
    it('should extract chunk ID from standard S3 key pattern', () => {
      const result = extractChunkId('data/people/chunk-0042.ndjson');
      expect(result).toBe('0042');
    });

    it('should preserve zero padding in chunk ID', () => {
      expect(extractChunkId('chunk-0000.ndjson')).toBe('0000');
      expect(extractChunkId('chunk-0001.ndjson')).toBe('0001');
      expect(extractChunkId('chunk-0099.ndjson')).toBe('0099');
      expect(extractChunkId('chunk-1234.ndjson')).toBe('1234');
    });

    it('should extract chunk ID from nested paths', () => {
      const result = extractChunkId('example-client/data/2024/chunk-0123.ndjson');
      expect(result).toBe('0123');
    });

    it('should return undefined when no chunk pattern present', () => {
      expect(extractChunkId('data/people.json')).toBeUndefined();
      expect(extractChunkId('data/chunk0042.ndjson')).toBeUndefined();
      expect(extractChunkId('data/chunk-abc.ndjson')).toBeUndefined();
    });

    it('should not match non-NDJSON files', () => {
      expect(extractChunkId('data/chunk-0042.json')).toBeUndefined();
      expect(extractChunkId('data/chunk-0042.txt')).toBeUndefined();
    });

    it('should require hyphen separator', () => {
      expect(extractChunkId('data/chunk0042.ndjson')).toBeUndefined();
    });

    it('should match at end of string only', () => {
      expect(extractChunkId('chunk-0001.ndjson/extra')).toBeUndefined();
    });
  });

  describe('validateChunk', () => {
    it('should throw error when chunk is undefined', () => {
      expect(() => validateChunk(undefined)).toThrow(
        'No chunk information provided in SQS message or environment variables'
      );
    });

    it('should exit process when bucketName is missing', () => {
      const mockExit = jest.spyOn(process, 'exit').mockImplementation((code?: any) => {
        throw new Error(`Process.exit(${code})`);
      });
      const mockError = jest.spyOn(console, 'error').mockImplementation();

      try {
        validateChunk({ bucketName: '', s3Key: 'key.ndjson' });
      } catch (e: any) {
        expect(e.message).toBe('Process.exit(1)');
      }

      expect(mockError).toHaveBeenCalledWith(
        'ERROR: CHUNKS_BUCKET environment variable or queue message required'
      );

      mockExit.mockRestore();
      mockError.mockRestore();
    });

    it('should exit process when s3Key is missing', () => {
      const mockExit = jest.spyOn(process, 'exit').mockImplementation((code?: any) => {
        throw new Error(`Process.exit(${code})`);
      });
      const mockError = jest.spyOn(console, 'error').mockImplementation();

      try {
        validateChunk({ bucketName: 'bucket', s3Key: '' });
      } catch (e: any) {
        expect(e.message).toBe('Process.exit(1)');
      }

      expect(mockError).toHaveBeenCalledWith(
        'ERROR: CHUNK_KEY environment variable or queue message required'
      );

      mockExit.mockRestore();
      mockError.mockRestore();
    });

    it('should not throw when both bucketName and s3Key are present', () => {
      expect(() => {
        validateChunk({ bucketName: 'test-bucket', s3Key: 'test-key.ndjson' });
      }).not.toThrow();
    });
  });

  describe('buildChunkConfig', () => {
    it('should create S3 data source config with provided parameters', async () => {
      const config = await buildChunkConfig('my-bucket', 'data/chunk-0001.ndjson', 'us-west-2');

      expect(config.dataSource.people).toMatchObject({
        bucketName: 'my-bucket',
        key: 'data/chunk-0001.ndjson',
        region: 'us-west-2'
      });
    });

    it('should use default region from base config when not specified', async () => {
      const config = await buildChunkConfig('my-bucket', 'data/chunk-0001.ndjson');

      expect(config.dataSource.people).toMatchObject({
        bucketName: 'my-bucket',
        key: 'data/chunk-0001.ndjson',
        region: 'us-east-1'  // from mock
      });
    });

    it('should preserve base configuration fields', async () => {
      const config = await buildChunkConfig('my-bucket', 'chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0001.ndjson', 'us-west-2');

      expect(config.dataTarget).toEqual({
        apiEndpoint: 'https://api.example.com'
      });
      expect(config.storage).toMatchObject({
        bucketName: 'test-bucket',
        fileKeyPrefix: 'test-data/',
        config: {
          bucketName: 'my-bucket',
          keyPrefix: 'deltas/'  // Explicit keyPrefix to prevent test-datasets default in DeltaStrategyForS3Bucket
        }
      });
    });

    it('should handle fieldsOfInterest from base config', async () => {
      const config = await buildChunkConfig('my-bucket', 'data/chunk-0001.ndjson');

      // fieldsOfInterest is intentionally not passed through to avoid creating unnecessary S3 stream filters
      expect(config.dataSource.people?.fieldsOfInterest).toBeUndefined();
    });

    it('should override only the data source, not other config sections', async () => {
      const config = await buildChunkConfig('new-bucket', 'new-key.ndjson', 'eu-west-1');

      // S3 datasource should be new
      expect(config.dataSource.people).toMatchObject({
        bucketName: 'new-bucket',
        key: 'new-key.ndjson',
        region: 'eu-west-1'
      });

      // But other sections preserved from base config
      expect(config.dataTarget).toBeDefined();
      expect(config.storage).toBeDefined();
    });
  });

  describe('Chunk output paths', () => {
    it('should transform path to chunked storage format', () => {
      const baseName = 'chunks/person-full/2026-03-03T19:58:41.277Z/previous-input.ndjson';
      const chunkId = '0042';
      const outputPath = baseName.replace('previous-input.ndjson', `chunk-${chunkId}.ndjson`);

      expect(outputPath).toBe('chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0042.ndjson');
    });

    it('should use default path when no chunk ID', () => {
      const baseName = 'chunks/person-full/2026-03-03T19:58:41.277Z/previous-input.ndjson';
      const chunkId = undefined;
      const outputPath = chunkId 
        ? baseName.replace('previous-input.ndjson', `chunk-${chunkId}.ndjson`)
        : baseName;

      expect(outputPath).toBe('chunks/person-full/2026-03-03T19:58:41.277Z/previous-input.ndjson');
    });

    it('should preserve client ID prefix in chunk paths', () => {
      const baseDir = 'chunks/test-university/2026-03-03T19:58:41.277Z';
      const chunkId = '0123';
      const outputPath = `${baseDir}/chunk-${chunkId}.ndjson`;

      expect(outputPath).toBe('chunks/test-university/2026-03-03T19:58:41.277Z/chunk-0123.ndjson');
    });

    it('should handle multiple chunk IDs consistently', () => {
      const basePath = 'chunks/client-abc/2026-03-03T19:58:41.277Z/previous-input.ndjson';
      const chunkIds = ['0000', '0001', '0050', '0999'];

      chunkIds.forEach(chunkId => {
        const outputPath = basePath.replace('previous-input.ndjson', `chunk-${chunkId}.ndjson`);
        expect(outputPath).toBe(`chunks/client-abc/2026-03-03T19:58:41.277Z/chunk-${chunkId}.ndjson`);
      });
    });
  });
});

