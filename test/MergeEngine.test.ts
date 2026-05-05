/**
 * Tests for MergeEngine class
 * 
 * Tests the core merge engine that concatenates chunk delta outputs
 */

import { MergeEngine, MergerConfig, MergeResult } from '../src/merging/MergeEngine';
import { S3 } from '@aws-sdk/client-s3';
import { Readable } from 'stream';

// Mock AWS SDK
jest.mock('@aws-sdk/client-s3');

describe('MergeEngine', () => {
  let mockS3: any;
  let mergerConfig: MergerConfig;
  let originalEnv: NodeJS.ProcessEnv;

  const createMockStream = (data: string) => {
    const stream = new Readable();
    stream.push(data);
    stream.push(null);
    return stream;
  };

  const setupBasicMocks = (chunkKeys: string[], chunkData: string = '{"test":"data"}') => {
    mockS3.listObjectsV2.mockResolvedValue({
      Contents: chunkKeys.map(Key => ({ Key }))
    });
    mockS3.getObject.mockResolvedValue({ 
      Body: createMockStream(chunkData)
    });
    mockS3.putObject.mockResolvedValue({});
    mockS3.deleteObjects.mockResolvedValue({});
  };

  beforeEach(() => {
    // Save original environment
    originalEnv = { ...process.env };

    // Create S3 mock
    mockS3 = {
      listObjectsV2: jest.fn(),
      getObject: jest.fn(),
      putObject: jest.fn(),
      deleteObjects: jest.fn()
    };

    (S3 as jest.MockedClass<typeof S3>).mockImplementation(() => mockS3);

    // Default merger config
    mergerConfig = {
      bucketName: 'test-bucket',
      deltaDir: 'test-client',
      sharedDeltaDir: 'delta-storage/test',
      region: 'us-east-2'
    };

    // Reset all mocks
    jest.clearAllMocks();
  });

  afterEach(() => {
    // Restore original environment
    process.env = originalEnv;
  });

  describe('Configuration validation', () => {
    it('should accept valid merger config', () => {
      const config: MergerConfig = {
        bucketName: 'test-bucket',
        deltaDir: 'test-delta',
        sharedDeltaDir: 'test-shared',
        region: 'us-west-2'
      };
      
      const merger = new MergeEngine(config);
      expect(merger).toBeDefined();
    });

    it('should accept merger config without region', () => {
      const config: MergerConfig = {
        bucketName: 'test',
        deltaDir: 'test-delta',
        sharedDeltaDir: 'test-shared'
      };
      
      const merger = new MergeEngine(config);
      expect(merger).toBeDefined();
    });
  });

  describe('Chunk file listing', () => {
    it('should handle empty chunks directory', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: []
      } as any);

      const merger = new MergeEngine(mergerConfig);
      const result = await merger.merge();

      expect(result.chunkCount).toBe(0);
      expect(result.totalLines).toBe(0);
      expect(mockS3.putObject).not.toHaveBeenCalled();
    });
  });

  describe('Chunk file reading', () => {
    it('should read NDJSON content from each chunk', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      const ndjsonData = '{"id":"1","name":"Alice"}\n{"id":"2","name":"Bob"}';
      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream(ndjsonData)
      } as any);

      mockS3.putObject.mockResolvedValue({} as any);
      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      const result = await merger.merge();

      expect(mockS3.getObject).toHaveBeenCalledWith({
        Bucket: 'test-bucket',
        Key: 'test-client/chunks/chunk-0000.ndjson'
      });

      expect(result.totalLines).toBe(2);
    });

    it('should handle multi-line NDJSON correctly', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      const ndjsonData = '{"id":"1"}\n{"id":"2"}\n{"id":"3"}\n{"id":"4"}\n{"id":"5"}';
      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream(ndjsonData)
      } as any);

      mockS3.putObject.mockResolvedValue({} as any);
      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      const result = await merger.merge();

      expect(result.totalLines).toBe(5);
    });

    it('should skip empty lines in NDJSON', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      const ndjsonData = '{"id":"1"}\n\n{"id":"2"}\n  \n{"id":"3"}';
      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream(ndjsonData)
      } as any);

      mockS3.putObject.mockResolvedValue({} as any);
      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      const result = await merger.merge();

      expect(result.totalLines).toBe(3); // Only non-empty lines
    });
  });

  describe('File concatenation', () => {
    it('should concatenate multiple chunks in order', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' },
          { Key: 'test-client/chunks/chunk-0001.ndjson' },
          { Key: 'test-client/chunks/chunk-0002.ndjson' }
        ]
      } as any);

      mockS3.getObject
        .mockResolvedValueOnce({ Body: createMockStream('{"chunk":0,"line":1}\n{"chunk":0,"line":2}') } as any)
        .mockResolvedValueOnce({ Body: createMockStream('{"chunk":1,"line":1}\n{"chunk":1,"line":2}') } as any)
        .mockResolvedValueOnce({ Body: createMockStream('{"chunk":2,"line":1}\n{"chunk":2,"line":2}') } as any);

      mockS3.putObject.mockResolvedValue({} as any);
      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      const result = await merger.merge();

      expect(result.totalLines).toBe(6);  // 2 lines × 3 chunks
      expect(result.chunkCount).toBe(3);
    });

    it('should preserve line order within each chunk', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      const orderedData = '{"seq":1}\n{"seq":2}\n{"seq":3}\n{"seq":4}\n{"seq":5}';
      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream(orderedData)
      } as any);

      let capturedContent = '';
      mockS3.putObject.mockImplementation((params: any) => {
        capturedContent = params.Body;
        return Promise.resolve({} as any);
      });

      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      await merger.merge();

      // Verify order is preserved
      const lines = capturedContent.trim().split('\n');
      expect(lines[0]).toContain('"seq":1');
      expect(lines[1]).toContain('"seq":2');
      expect(lines[2]).toContain('"seq":3');
      expect(lines[3]).toContain('"seq":4');
      expect(lines[4]).toContain('"seq":5');
    });
  });

  describe('Merged file output', () => {
    it('should write to correct output path', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream('{"test":"data"}')
      } as any);

      mockS3.putObject.mockResolvedValue({} as any);
      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      const result = await merger.merge();

      expect(mockS3.putObject).toHaveBeenCalledWith({
        Bucket: 'test-bucket',
        Key: 'test-client/previous-input.ndjson',
        Body: expect.any(String),
        ContentType: 'application/x-ndjson'
      });

      expect(result.outputKey).toBe('test-client/previous-input.ndjson');
    });

    it('should set correct ContentType for NDJSON', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream('{"test":"data"}')
      } as any);

      mockS3.putObject.mockResolvedValue({} as any);
      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      await merger.merge();

      expect(mockS3.putObject).toHaveBeenCalledWith(
        expect.objectContaining({
          ContentType: 'application/x-ndjson'
        })
      );
    });

    it('should end merged file with newline', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream('{"test":"data"}')
      } as any);

      let capturedContent = '';
      mockS3.putObject.mockImplementation((params: any) => {
        capturedContent = params.Body;
        return Promise.resolve({} as any);
      });

      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      await merger.merge();

      expect(capturedContent).toMatch(/\n$/);
    });
  });

  describe('Chunk cleanup', () => {
    it('should not delete chunks if no chunks found', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: []
      } as any);

      const merger = new MergeEngine(mergerConfig);
      await merger.merge();

      expect(mockS3.deleteObjects).not.toHaveBeenCalled();
    });
  });

  describe('Error handling', () => {
    it('should throw error when S3 listObjects fails', async () => {
      mockS3.listObjectsV2.mockRejectedValue(new Error('AccessDenied'));

      const merger = new MergeEngine(mergerConfig);

      await expect(merger.merge()).rejects.toThrow('Failed to list delta chunk files');
    });

    it('should throw error when chunk read fails', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      mockS3.getObject.mockRejectedValue(new Error('NoSuchKey'));

      const merger = new MergeEngine(mergerConfig);

      await expect(merger.merge()).rejects.toThrow('Failed to read chunk');
    });

    it('should throw error when putObject fails', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream('{"test":"data"}')
      } as any);

      mockS3.putObject.mockRejectedValue(new Error('InsufficientStorage'));

      const merger = new MergeEngine(mergerConfig);

      await expect(merger.merge()).rejects.toThrow('Failed to write merged delta file');
    });

    it('should throw error when deleteObjects fails', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' }
        ]
      } as any);

      mockS3.getObject.mockResolvedValue({ 
        Body: createMockStream('{"test":"data"}')
      } as any);

      mockS3.putObject.mockResolvedValue({} as any);
      mockS3.deleteObjects.mockRejectedValue(new Error('DeleteFailed'));

      const merger = new MergeEngine(mergerConfig);

      // merge() now completes successfully; cleanup() is where deletion happens
      await merger.merge();
      await expect(merger.cleanup()).rejects.toThrow('Failed to delete delta chunk files');
    });
  });

  describe('Merge result validation', () => {
    it('should return complete merge result', async () => {
      mockS3.listObjectsV2.mockResolvedValue({
        Contents: [
          { Key: 'test-client/chunks/chunk-0000.ndjson' },
          { Key: 'test-client/chunks/chunk-0001.ndjson' }
        ]
      } as any);

      mockS3.getObject
        .mockResolvedValueOnce({ Body: createMockStream('{"id":1}\n{"id":2}') } as any)
        .mockResolvedValueOnce({ Body: createMockStream('{"id":3}\n{"id":4}\n{"id":5}') } as any);

      mockS3.putObject.mockResolvedValue({} as any);
      mockS3.deleteObjects.mockResolvedValue({} as any);

      const merger = new MergeEngine(mergerConfig);
      const result = await merger.merge();

      expect(result).toMatchObject({
        totalLines: 5,
        chunkCount: 2,
        outputKey: 'test-client/previous-input.ndjson',
        deletedChunks: [
          'test-client/chunks/chunk-0000.ndjson',
          'test-client/chunks/chunk-0001.ndjson'
        ]
      });
    });
  });
});
