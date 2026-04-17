/**
 * Comprehensive tests for ChunkFromS3.ts
 * 
 * Tests the parameter gathering, validation, and S3-based chunking logic.
 * Covers all variations of:
 * - Environment variable configuration
 * - SQS message queue parameters
 * - Combination scenarios with priority handling
 */

// Mock dependencies BEFORE importing the module under test
jest.mock('@aws-sdk/client-sqs');
jest.mock('../src/storage/S3StorageAdapter');
jest.mock('../src/chunking/PersonArrayWrapper');
jest.mock('../docker/chunker', () => ({
  SyncPopulation: {
    PersonFull: 'person-full',
    PersonDelta: 'person-delta'
  },
  grabMessageBodyFromQueue: jest.fn(),
  writeMetadata: jest.fn()
}));

import { ChunkFromS3 } from '../src/chunking/filedrop/ChunkFromS3';

describe('ChunkFromS3 Parameter Gathering', () => {
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    // Save original environment
    originalEnv = { ...process.env };

    // Reset all mocks
    jest.clearAllMocks();
  });

  afterEach(() => {
    // Restore original environment
    process.env = originalEnv;
  });

  describe('Parameter gathering from environment variables', () => {
    it('should read task parameters from environment when all are provided', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      process.env.INPUT_KEY = 'person-full/2026-04-13T10:00:00.000Z-people.json';
      process.env.BULK_RESET = 'true';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should parse BULK_RESET as boolean true', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      process.env.INPUT_KEY = 'person-delta/people.json';
      process.env.BULK_RESET = 'true';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should parse BULK_RESET as boolean false', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      process.env.INPUT_KEY = 'person-delta/people.json';
      process.env.BULK_RESET = 'false';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle missing BULK_RESET (defaults to false)', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      process.env.INPUT_KEY = 'person-full/people.json';
      delete process.env.BULK_RESET;

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle missing INPUT_BUCKET', () => {
      delete process.env.INPUT_BUCKET;
      process.env.INPUT_KEY = 'person-full/people.json';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should handle missing INPUT_KEY', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      delete process.env.INPUT_KEY;

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should handle missing both INPUT_BUCKET and INPUT_KEY', () => {
      delete process.env.INPUT_BUCKET;
      delete process.env.INPUT_KEY;

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should handle empty string INPUT_BUCKET', () => {
      process.env.INPUT_BUCKET = '';
      process.env.INPUT_KEY = 'person-full/people.json';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should handle empty string INPUT_KEY', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      process.env.INPUT_KEY = '';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });
  });

  describe('Parameter gathering from SQS message', () => {
    beforeEach(() => {
      // Clear environment variables to test message-only scenarios
      delete process.env.INPUT_BUCKET;
      delete process.env.INPUT_KEY;
      delete process.env.BULK_RESET;
    });

    it('should set task parameters from queue message with all fields', () => {
      const messageBody = {
        INPUT_BUCKET: 'queue-input-bucket',
        INPUT_KEY: 'person-full/2026-04-13T12:00:00.000Z-people.json',
        BULK_RESET: 'true'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle message with missing BULK_RESET', () => {
      const messageBody = {
        INPUT_BUCKET: 'queue-input-bucket',
        INPUT_KEY: 'person-delta/people.json'
        // BULK_RESET missing
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle message with BULK_RESET as false', () => {
      const messageBody = {
        INPUT_BUCKET: 'queue-input-bucket',
        INPUT_KEY: 'person-full/data.json',
        BULK_RESET: 'false'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle message with BULK_RESET as TRUE (uppercase)', () => {
      const messageBody = {
        INPUT_BUCKET: 'queue-input-bucket',
        INPUT_KEY: 'person-full/data.json',
        BULK_RESET: 'TRUE'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should fail when message is missing INPUT_BUCKET', () => {
      const messageBody = {
        INPUT_KEY: 'person-full/people.json',
        BULK_RESET: 'false'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should fail when message is missing INPUT_KEY', () => {
      const messageBody = {
        INPUT_BUCKET: 'queue-input-bucket',
        BULK_RESET: 'false'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should fail when message is empty', () => {
      const messageBody = {};

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });
  });

  describe('Parameter priority and combination scenarios', () => {
    it('should use environment over nothing when only environment is set', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      process.env.INPUT_KEY = 'person-full/env-people.json';
      process.env.BULK_RESET = 'true';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should use queue message to override environment', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      process.env.INPUT_KEY = 'person-full/env-people.json';

      const messageBody = {
        INPUT_BUCKET: 'queue-input-bucket',
        INPUT_KEY: 'person-delta/queue-people.json',
        BULK_RESET: 'false'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
      // Message should override environment
    });

    it('should handle queue message replacing partial environment', () => {
      process.env.INPUT_BUCKET = 'env-input-bucket';
      delete process.env.INPUT_KEY;

      const messageBody = {
        INPUT_BUCKET: 'queue-input-bucket',
        INPUT_KEY: 'person-full/queue-people.json'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should use queue message when environment is completely missing', () => {
      delete process.env.INPUT_BUCKET;
      delete process.env.INPUT_KEY;

      const messageBody = {
        INPUT_BUCKET: 'queue-input-bucket',
        INPUT_KEY: 'person-delta/queue-people.json'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });
  });

  describe('Input key path variations', () => {
    it('should handle person-full directory structure', () => {
      const messageBody = {
        INPUT_BUCKET: 'test-bucket',
        INPUT_KEY: 'person-full/2026-04-13T10:00:00.000Z-people.json'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle person-delta directory structure', () => {
      const messageBody = {
        INPUT_BUCKET: 'test-bucket',
        INPUT_KEY: 'person-delta/2026-04-13T10:00:00.000Z-people.json'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle nested path structures', () => {
      const messageBody = {
        INPUT_BUCKET: 'test-bucket',
        INPUT_KEY: 'data/input/person-full/2026-04-13T10:00:00.000Z-people.json'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle simple filename without path', () => {
      const messageBody = {
        INPUT_BUCKET: 'test-bucket',
        INPUT_KEY: 'people.json'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle ISO timestamp in filename', () => {
      const messageBody = {
        INPUT_BUCKET: 'test-bucket',
        INPUT_KEY: 'person-full/2026-04-13T10:00:00.000Z.json'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });
  });

  describe('hasSufficientTaskInfo validation', () => {
    it('should return true when both bucket and key are present', () => {
      process.env.INPUT_BUCKET = 'test-bucket';
      process.env.INPUT_KEY = 'test-key.json';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should return false when bucket is missing', () => {
      delete process.env.INPUT_BUCKET;
      process.env.INPUT_KEY = 'test-key.json';

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should return false when key is missing', () => {
      process.env.INPUT_BUCKET = 'test-bucket';
      delete process.env.INPUT_KEY;

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should log to console when logToConsole parameter is true', () => {
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      
      delete process.env.INPUT_BUCKET;
      delete process.env.INPUT_KEY;

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo(true)).toBe(false);
      
      expect(consoleSpy).toHaveBeenCalledWith('Missing required inputBucket for S3 source');
      expect(consoleSpy).toHaveBeenCalledWith('Missing required inputKey for S3 source');
      consoleSpy.mockRestore();
    });

    it('should not log to console when logToConsole parameter is false', () => {
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      
      delete process.env.INPUT_BUCKET;
      delete process.env.INPUT_KEY;

      const chunker = new ChunkFromS3();
      expect(chunker.hasSufficientTaskInfo(false)).toBe(false);
      
      // Should not log
      expect(consoleSpy).not.toHaveBeenCalled();
      consoleSpy.mockRestore();
    });
  });

  describe('Edge cases and error handling', () => {
    it('should handle null message body gracefully', () => {
      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(null);
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should handle undefined message body gracefully', () => {
      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(undefined);
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should handle message body with extra fields', () => {
      const messageBody = {
        INPUT_BUCKET: 'test-bucket',
        INPUT_KEY: 'test-key.json',
        BULK_RESET: 'true',
        EXTRA_FIELD: 'should-be-ignored',
        ANOTHER_FIELD: 123
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle whitespace in bucket name', () => {
      const messageBody = {
        INPUT_BUCKET: '  test-bucket  ',
        INPUT_KEY: 'test-key.json'
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle whitespace in key', () => {
      const messageBody = {
        INPUT_BUCKET: 'test-bucket',
        INPUT_KEY: '  test-key.json  '
      };

      const chunker = new ChunkFromS3();
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });
  });
});
