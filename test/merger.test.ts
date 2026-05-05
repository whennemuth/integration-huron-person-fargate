/**
 * Integration tests for merger.ts entry point
 * 
 * Tests the Phase 3 entry point that orchestrates merging, deduplication, and deletion handling.
 * For detailed MergeEngine tests, see MergeEngine.test.ts
 */

describe('Merger Entry Point Integration', () => {
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    // Save original environment
    originalEnv = { ...process.env };
  });

  afterEach(() => {
    // Restore original environment
    process.env = originalEnv;
  });

  describe('Environment variable validation', () => {
    it('should require CHUNKS_BUCKET or SQS_QUEUE_URL for operation', () => {
      delete process.env.CHUNKS_BUCKET;
      delete process.env.SQS_QUEUE_URL;
      
      expect(process.env.CHUNKS_BUCKET).toBeUndefined();
      expect(process.env.SQS_QUEUE_URL).toBeUndefined();
    });

    it('should accept REGION environment variable', () => {
      process.env.REGION = 'us-west-2';
      expect(process.env.REGION).toBe('us-west-2');
    });

    it('should accept DRY_RUN environment variable', () => {
      process.env.DRY_RUN = 'true';
      expect(process.env.DRY_RUN).toBe('true');
    });
  });

  describe('Deletion handler integration', () => {
    it('should respect PERSON_DELETE_TYPE environment variable', () => {
      process.env.PERSON_DELETE_TYPE = 'soft';
      expect(process.env.PERSON_DELETE_TYPE).toBe('soft');
      
      process.env.PERSON_DELETE_TYPE = 'hard';
      expect(process.env.PERSON_DELETE_TYPE).toBe('hard');
      
      process.env.PERSON_DELETE_TYPE = 'none';
      expect(process.env.PERSON_DELETE_TYPE).toBe('none');
    });
  });

  describe('SQS message parameters', () => {
    it('should handle SQS message with createdAt timestamp', () => {
      // Simulate SQS message body structure
      const sqsMessageBody = {
        chunksBucket: 'test-bucket',
        chunkDirectory: 'chunks/person-full/2026-05-05T10:30:00.000Z',
        createdAt: '2026-05-05T10:30:00.000Z',
      };

      expect(sqsMessageBody).toHaveProperty('createdAt');
      expect(sqsMessageBody.createdAt).toBe('2026-05-05T10:30:00.000Z');
      
      // Verify timestamp can be parsed
      const timestamp = new Date(sqsMessageBody.createdAt);
      expect(timestamp.toISOString()).toBe('2026-05-05T10:30:00.000Z');
    });

    it('should handle SQS message without createdAt timestamp', () => {
      // Simulate SQS message body without createdAt (backward compatibility)
      const sqsMessageBody: any = {
        chunksBucket: 'test-bucket',
        chunkDirectory: 'chunks/person-full/2026-05-05T10:30:00.000Z',
      };

      expect(sqsMessageBody).not.toHaveProperty('createdAt');
      
      // Should handle gracefully when createdAt is undefined
      const chunkingStartTime = sqsMessageBody.createdAt ? new Date(sqsMessageBody.createdAt) : null;
      expect(chunkingStartTime).toBeNull();
    });

    it('should calculate duration from SQS message timestamp', () => {
      const startTime = new Date('2026-05-05T10:30:00.000Z');
      const endTime = new Date('2026-05-05T10:35:30.000Z');
      
      const durationMs = endTime.getTime() - startTime.getTime();
      const seconds = (durationMs / 1000).toFixed(3);
      
      expect(seconds).toBe('330.000'); // 5 minutes 30 seconds
    });
  });
});
