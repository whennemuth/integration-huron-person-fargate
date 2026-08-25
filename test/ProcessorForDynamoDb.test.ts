/**
 * Comprehensive tests for ProcessorForDynamoDb (src/processing/ProcessorForDynamoDb.ts)
 * 
 * Tests the Phase 2 processing logic for DynamoDB storage mode
 */

/**
 * Comprehensive tests for ProcessorForDynamoDb (src/processing/ProcessorForDynamoDb.ts)
 * 
 * Tests the Phase 2 processing logic for DynamoDB storage mode
 */

// Set PREVIOUS_STORAGE_TYPE and table names before importing ProcessorForDynamoDb
// (Module has top-level call to MetadataFactoryForBootstrap which requires this)
process.env.PREVIOUS_STORAGE_TYPE = 'dynamodb';
process.env.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME = 'test-person-current-state-table';
process.env.DYNAMODB_PERSON_HISTORY_TABLE_NAME = 'test-person-history-table';
process.env.DYNAMODB_STATISTICS_TABLE_NAME = 'test-statistics-table';

// Mock StatisticsTable to avoid context requirements during module load
jest.mock('../src/dynamodb/StatisticsTable', () => ({
  StatisticsTable: jest.fn().mockImplementation(() => ({
    writeFlags: jest.fn(),
    readFlags: jest.fn(),
    writeMetadata: jest.fn(),
    readMetadata: jest.fn(),
  })),
  DYNAMODB_TABLE_NAME: jest.fn((context: any) => 'test-statistics-table'),
  DYNAMODB_PARTITION_KEY: 'integrationTimestamp',
  DYNAMODB_SORT_KEY: 'eventType',
}));

import { buildChunkConfig } from '../src/processing/ProcessorForDynamoDb';
import { ChunkFileManager } from '../src/chunking/metadata';

const validateChunk = ChunkFileManager.validateChunk;

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
          type: 's3',
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
          type: 's3',
          bucketName: 'test-bucket',
          fileKeyPrefix: 'test-data/'
        },
        integration: {
          clientId: 'test-client'
        }
      }))
    }))
  }
}));

describe('ProcessorForDynamoDb (Phase 2 - DynamoDB Mode)', () => {
  describe('validateChunk (shared utility)', () => {
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
    it('should create DynamoDB storage config with provided table names', async () => {
      const result = await buildChunkConfig({
        bucketName: 'test-bucket',
        s3Key: 'chunks/chunk-0001.ndjson',
        personCurrentStateTableName: 'PersonCurrentStateTable',
        personHistoryTableName: 'PersonHistoryTable',
        region: 'us-east-2'
      });

      expect(result.storage.type).toBe('dynamodb');
      const storageConfig = result.storage.config as any;
      expect(storageConfig.region).toBe('us-east-2');
      expect(storageConfig.personCurrentStateTableName).toBe('PersonCurrentStateTable');
      expect(storageConfig.personHistoryTableName).toBe('PersonHistoryTable');
      expect(storageConfig.currentStateGSIName).toBe('syncRunId-personId-index');
    });

    it('should use default region from base config when not specified', async () => {
      const result = await buildChunkConfig({
        bucketName: 'test-bucket',
        s3Key: 'chunks/chunk-0001.ndjson',
        personCurrentStateTableName: 'PersonCurrentStateTable',
        personHistoryTableName: 'PersonHistoryTable'
      });

      expect(result.storage.type).toBe('dynamodb');
      const storageConfig = result.storage.config as any;
      expect(storageConfig.region).toBe('us-east-1');
    });

    it('should preserve base configuration fields', async () => {
      const result = await buildChunkConfig({
        bucketName: 'test-bucket',
        s3Key: 'chunks/chunk-0001.ndjson',
        personCurrentStateTableName: 'PersonCurrentStateTable',
        personHistoryTableName: 'PersonHistoryTable',
        region: 'us-west-2'
      });

      expect(result.dataTarget).toEqual({
        apiEndpoint: 'https://api.example.com'
      });
    });

    it('should create S3 data source config for chunk', async () => {
      const result = await buildChunkConfig({
        bucketName: 'my-chunks-bucket',
        s3Key: 'chunks/person-full/2026-01-01T00:00:00.000Z/chunk-0042.ndjson',
        personCurrentStateTableName: 'PersonCurrentStateTable',
        personHistoryTableName: 'PersonHistoryTable',
        region: 'us-east-2'
      });

      expect(result.dataSource.people).toEqual({
        bucketName: 'my-chunks-bucket',
        key: 'chunks/person-full/2026-01-01T00:00:00.000Z/chunk-0042.ndjson',
        region: 'us-east-2'
      });
    });

    it('should set clientId to dynamodb-processor', async () => {
      const result = await buildChunkConfig({
        bucketName: 'test-bucket',
        s3Key: 'chunks/chunk-0001.ndjson',
        personCurrentStateTableName: 'PersonCurrentStateTable',
        personHistoryTableName: 'PersonHistoryTable',
        region: 'us-east-2'
      });

      expect(result.integration?.clientId).toBe('dynamodb-processor');
    });

    it('should include currentStateGSIName in storage config', async () => {
      const result = await buildChunkConfig({
        bucketName: 'test-bucket',
        s3Key: 'chunks/chunk-0001.ndjson',
        personCurrentStateTableName: 'PersonCurrentStateTable',
        personHistoryTableName: 'PersonHistoryTable',
        region: 'us-east-2'
      });

      expect(result.storage.type).toBe('dynamodb');
      const storageConfig = result.storage.config as any;
      expect(storageConfig.currentStateGSIName).toBe('syncRunId-personId-index');
    });

    it('should handle chunk files from different directories', async () => {
      const result = await buildChunkConfig({
        bucketName: 'test-bucket',
        s3Key: 'example-client/data/2024/chunk-0123.ndjson',
        personCurrentStateTableName: 'PersonCurrentStateTable',
        personHistoryTableName: 'PersonHistoryTable',
        region: 'us-east-2'
      });

      expect(result.dataSource.people).toEqual({
        bucketName: 'test-bucket',
        key: 'example-client/data/2024/chunk-0123.ndjson',
        region: 'us-east-2'
      });
    });

    it('should override base storage config with DynamoDB-specific settings', async () => {
      const result = await buildChunkConfig({
        bucketName: 'test-bucket',
        s3Key: 'chunks/chunk-0001.ndjson',
        personCurrentStateTableName: 'MyCurrentStateTable',
        personHistoryTableName: 'MyHistoryTable',
        region: 'us-east-2'
      });

      // Verify base config had S3 storage, but result has DynamoDB
      expect(result.storage.type).toBe('dynamodb');
      const storageConfig = result.storage.config as any;
      expect(storageConfig.personCurrentStateTableName).toBe('MyCurrentStateTable');
      expect(storageConfig.personHistoryTableName).toBe('MyHistoryTable');
    });
  });
});
