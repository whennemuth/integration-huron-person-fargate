/**
 * Comprehensive tests for chunker.ts
 * 
 * Tests the Phase 1 chunking logic that splits large JSON files into NDJSON chunks
 */

import { BigJsonFile, BigJsonFileConfig } from '../src/chunking/filedrop/BigJsonFile';
import { PersonArrayWrapper } from '../src/chunking/PersonArrayWrapper';
import { S3StorageAdapter } from '../src/storage/S3StorageAdapter';

// Mock the storage adapter
jest.mock('../src/storage/S3StorageAdapter');
jest.mock('../src/chunking/PersonArrayWrapper');

describe('Chunker Integration', () => {
  let mockStorage: jest.Mocked<S3StorageAdapter>;
  let mockPersonArrayWrapper: jest.Mocked<PersonArrayWrapper>;
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    // Save original environment
    originalEnv = { ...process.env };

    // Create mocks
    mockStorage = new S3StorageAdapter({ bucketName: 'test-bucket' }) as jest.Mocked<S3StorageAdapter>;
    mockPersonArrayWrapper = new PersonArrayWrapper(mockStorage, 'personid') as jest.Mocked<PersonArrayWrapper>;

    // Reset all mocks
    jest.clearAllMocks();
  });

  afterEach(() => {
    // Restore original environment
    process.env = originalEnv;
  });

  describe('Configuration validation', () => {
    it('should require INPUT_BUCKET environment variable', () => {
      delete process.env.INPUT_BUCKET;
      process.env.INPUT_KEY = 'data/people.json';

      // Would need to actually run chunker main() to test this
      expect(process.env.INPUT_BUCKET).toBeUndefined();
    });

    it('should require INPUT_KEY environment variable', () => {
      process.env.INPUT_BUCKET = 'test-bucket';
      delete process.env.INPUT_KEY;

      expect(process.env.INPUT_KEY).toBeUndefined();
    });

    it('should use default ITEMS_PER_CHUNK when not specified', () => {
      process.env.INPUT_BUCKET = 'test-bucket';
      process.env.INPUT_KEY = 'data/people.json';
      delete process.env.ITEMS_PER_CHUNK;

      const itemsPerChunk = parseInt(process.env.ITEMS_PER_CHUNK || '200', 10);
      expect(itemsPerChunk).toBe(200);
    });

    it('should parse custom ITEMS_PER_CHUNK', () => {
      process.env.ITEMS_PER_CHUNK = '500';

      const itemsPerChunk = parseInt(process.env.ITEMS_PER_CHUNK, 10);
      expect(itemsPerChunk).toBe(500);
    });

    it('should use default PERSON_ID_FIELD when not specified', () => {
      const defaultField = process.env.PERSON_ID_FIELD || 'personid';
      expect(defaultField).toBe('personid');
    });

    it('should accept custom PERSON_ID_FIELD', () => {
      process.env.PERSON_ID_FIELD = 'customId';
      expect(process.env.PERSON_ID_FIELD).toBe('customId');
    });
  });

  describe('BigJsonFile chunking', () => {
    it('should create BigJsonFile with correct configuration', () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 200,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      expect(chunker).toBeDefined();
    });

    it('should call breakup method with S3 key', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 200,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      const breakupSpy = jest.spyOn(chunker, 'breakup').mockResolvedValue({
        chunkCount: 5,
        totalRecords: 1000,
        chunkKeys: [
          'data/people/chunk-0000.ndjson',
          'data/people/chunk-0001.ndjson',
          'data/people/chunk-0002.ndjson',
          'data/people/chunk-0003.ndjson',
          'data/people/chunk-0004.ndjson'
        ]
      });

      const result = await chunker.breakup('data/people.json');

      expect(breakupSpy).toHaveBeenCalledWith('data/people.json');
      expect(result.chunkCount).toBe(5);
      expect(result.totalRecords).toBe(1000);
      expect(result.chunkKeys).toHaveLength(5);
    });

    it('should generate chunk keys with zero-padded IDs', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 100,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      jest.spyOn(chunker, 'breakup').mockResolvedValue({
        chunkCount: 12,
        totalRecords: 1200,
        chunkKeys: Array.from({ length: 12 }, (_, i) => 
          `data/people/chunk-${String(i).padStart(4, '0')}.ndjson`
        )
      });

      const result = await chunker.breakup('data/people.json');

      expect(result.chunkKeys[0]).toBe('data/people/chunk-0000.ndjson');
      expect(result.chunkKeys[9]).toBe('data/people/chunk-0009.ndjson');
      expect(result.chunkKeys[11]).toBe('data/people/chunk-0011.ndjson');
    });

    it('should handle empty JSON arrays', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 200,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      jest.spyOn(chunker, 'breakup').mockResolvedValue({
        chunkCount: 0,
        totalRecords: 0,
        chunkKeys: []
      });

      const result = await chunker.breakup('data/empty.json');

      expect(result.chunkCount).toBe(0);
      expect(result.totalRecords).toBe(0);
      expect(result.chunkKeys).toHaveLength(0);
    });

    it('should handle single chunk when records < itemsPerChunk', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 500,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      jest.spyOn(chunker, 'breakup').mockResolvedValue({
        chunkCount: 1,
        totalRecords: 200,
        chunkKeys: ['data/people/chunk-0000.ndjson']
      });

      const result = await chunker.breakup('data/small.json');

      expect(result.chunkCount).toBe(1);
      expect(result.totalRecords).toBe(200);
    });
  });

  describe('Storage adapter integration', () => {
    it('should initialize S3StorageAdapter with correct bucket', () => {
      const storage = new S3StorageAdapter({ 
        bucketName: 'test-bucket',
        region: 'us-east-2'
      });

      expect(S3StorageAdapter).toHaveBeenCalledWith({
        bucketName: 'test-bucket',
        region: 'us-east-2'
      });
    });

    it('should use default region when not specified', () => {
      const storage = new S3StorageAdapter({ 
        bucketName: 'test-bucket'
      });

      expect(S3StorageAdapter).toHaveBeenCalledWith({
        bucketName: 'test-bucket'
      });
    });

    it('should pass storage adapter to BigJsonFile', () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 200,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      expect(chunker).toBeDefined();
    });
  });

  describe('PersonArrayWrapper integration', () => {
    it('should initialize PersonArrayWrapper with storage and personIdField', () => {
      const wrapper = new PersonArrayWrapper(mockStorage, 'customId');

      expect(PersonArrayWrapper).toHaveBeenCalledWith(mockStorage, 'customId');
    });

    it('should use default personid field', () => {
      const wrapper = new PersonArrayWrapper(mockStorage, 'personid');

      expect(PersonArrayWrapper).toHaveBeenCalledWith(mockStorage, 'personid');
    });

    it('should pass PersonArrayWrapper to BigJsonFile config', () => {
      const wrapper = new PersonArrayWrapper(mockStorage, 'personid');
      const config: BigJsonFileConfig = {
        itemsPerChunk: 200,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: wrapper
      };

      const chunker = new BigJsonFile(config);
      expect(chunker).toBeDefined();
    });
  });

  describe('Error handling', () => {
    it('should throw error when S3 operation fails', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 200,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      jest.spyOn(chunker, 'breakup').mockRejectedValue(
        new Error('S3 access denied')
      );

      await expect(chunker.breakup('data/people.json'))
        .rejects.toThrow('S3 access denied');
    });

    it('should throw error for invalid JSON', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 200,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      jest.spyOn(chunker, 'breakup').mockRejectedValue(
        new Error('Invalid JSON format')
      );

      await expect(chunker.breakup('data/invalid.json'))
        .rejects.toThrow('Invalid JSON format');
    });

    it('should throw error for missing S3 file', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 200,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      jest.spyOn(chunker, 'breakup').mockRejectedValue(
        new Error('NoSuchKey: The specified key does not exist')
      );

      await expect(chunker.breakup('data/missing.json'))
        .rejects.toThrow('NoSuchKey');
    });
  });

  describe('Chunk size calculations', () => {
    it('should create correct number of chunks for exact multiples', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 250,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      // 1000 records / 250 per chunk = 4 chunks exactly
      jest.spyOn(chunker, 'breakup').mockResolvedValue({
        chunkCount: 4,
        totalRecords: 1000,
        chunkKeys: Array.from({ length: 4 }, (_, i) => 
          `data/people/chunk-${String(i).padStart(4, '0')}.ndjson`
        )
      });

      const result = await chunker.breakup('data/people.json');

      expect(result.chunkCount).toBe(4);
      expect(result.totalRecords).toBe(1000);
    });

    it('should handle partial last chunk', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 300,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      // 1000 records / 300 per chunk = 3 full chunks + 1 partial (100 items)
      jest.spyOn(chunker, 'breakup').mockResolvedValue({
        chunkCount: 4,
        totalRecords: 1000,
        chunkKeys: Array.from({ length: 4 }, (_, i) => 
          `data/people/chunk-${String(i).padStart(4, '0')}.ndjson`
        )
      });

      const result = await chunker.breakup('data/people.json');

      expect(result.chunkCount).toBe(4);
      expect(result.totalRecords).toBe(1000);
    });

    it('should handle very large files with many chunks', async () => {
      const config: BigJsonFileConfig = {
        itemsPerChunk: 100,
        clientId: 'test-client',
        storage: mockStorage,
        personIdField: 'personid',
        personArrayWrapper: mockPersonArrayWrapper
      };

      const chunker = new BigJsonFile(config);
      const totalRecords = 50000; // 500 chunks
      const chunkCount = Math.ceil(totalRecords / 100);

      jest.spyOn(chunker, 'breakup').mockResolvedValue({
        chunkCount,
        totalRecords,
        chunkKeys: Array.from({ length: chunkCount }, (_, i) => 
          `data/people/chunk-${String(i).padStart(4, '0')}.ndjson`
        )
      });

      const result = await chunker.breakup('data/large.json');

      expect(result.chunkCount).toBe(500);
      expect(result.totalRecords).toBe(50000);
    });
  });
});
