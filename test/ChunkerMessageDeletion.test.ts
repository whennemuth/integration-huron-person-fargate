/**
 * Tests for docker/chunker.ts main() function
 * 
 * Tests that line 392 executes: await chunkerQueue.deleteMessageFromQueue(message!);
 */

import { Message } from '@aws-sdk/client-sqs';
import { main } from '../docker/chunker';
import { ChunkerQueue } from '../src/chunking/ChunkerQueue';
import { ChunkFromAPI } from '../src/chunking/fetch/ChunkFromAPI';
import { ChunkFromS3 } from '../src/chunking/filedrop/ChunkFromS3';
import { TaskProtection } from '../src/TaskProtection';
import { MetadataManager } from '../src/chunking/Metadata';
import { HuronPersonCache } from '../src/PersonCache';
import { ConfigManager } from 'integration-huron-person';
import * as Utils from '../src/Utils';

// Mock all dependencies
jest.mock('../src/chunking/ChunkerQueue');
jest.mock('../src/chunking/fetch/ChunkFromAPI');
jest.mock('../src/chunking/filedrop/ChunkFromS3');
jest.mock('../src/PersonCache');
jest.mock('../src/TaskProtection');
jest.mock('../src/Utils');
jest.mock('../src/chunking/Metadata');
jest.mock('integration-huron-person');

describe('Chunker Main - Message Deletion', () => {
  let mockChunkerQueue: jest.Mocked<ChunkerQueue>;
  let mockChunkFromAPI: jest.Mocked<ChunkFromAPI>;
  let mockChunkFromS3: jest.Mocked<ChunkFromS3>;
  let mockMessage: Message;
  let deleteMessageSpy: jest.SpyInstance;
  let processExitSpy: jest.SpyInstance;
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    // Save original environment
    originalEnv = { ...process.env };

    // Mock process.exit to prevent test termination
    processExitSpy = jest.spyOn(process, 'exit').mockImplementation(() => undefined as never);

    // Set up environment for ECS execution
    process.env.IS_ECS_TASK = 'true';
    process.env.SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue';
    process.env.REGION = 'us-east-1';
    process.env.CHUNKS_BUCKET = 'test-chunks-bucket';
    process.env.ITEMS_PER_CHUNK = '200';
    process.env.PERSON_ID_FIELD = 'personid';
    process.env.DRY_RUN = 'false';

    // Create mock message
    mockMessage = {
      MessageId: 'test-message-id',
      ReceiptHandle: 'test-receipt-handle-12345',
      Body: JSON.stringify({
        baseUrl: 'https://api.test.com',
        fetchPath: '/people',
        populationType: 'person-full',
        bulkReset: false,
        trustPreviousStorage: true,
        limit: 100,
        offset: 0,
        chunkDirectory: 'test-chunk-dir'
      })
    } as Message;

    // Mock ChunkerQueue
    deleteMessageSpy = jest.fn().mockResolvedValue(undefined);
    mockChunkerQueue = {
      receiveMessageFromQueue: jest.fn().mockResolvedValue(mockMessage),
      getMessageBody: jest.fn().mockReturnValue(JSON.parse(mockMessage.Body!)),
      deleteMessageFromQueue: deleteMessageSpy,
      popMessageFromQueue: jest.fn().mockResolvedValue(mockMessage),
      sendNextChunkingMessage: jest.fn().mockResolvedValue(true)
    } as any;

    // Mock ChunkFromAPI
    mockChunkFromAPI = {
      getMessage: jest.fn().mockReturnValue(mockMessage),
      getChunkDirectory: jest.fn().mockReturnValue('test-chunk-dir'),
      setTaskParametersFromQueueMessage: jest.fn(),
      hasSufficientConfig: jest.fn().mockReturnValue(true),
      hasSufficientTaskInfo: jest.fn().mockReturnValue(true),
      sendNextChunkingMessage: jest.fn().mockResolvedValue(true),
      runChunking: jest.fn().mockResolvedValue(undefined),
      getBulkResetFlag: jest.fn().mockReturnValue(false),
      getTrustPreviousStorageFlag: jest.fn().mockReturnValue(true),
      getSyncPopulation: jest.fn().mockReturnValue('person-full'),
      noMessagesFromQueue: undefined
    } as any;

    // Mock ChunkFromS3
    mockChunkFromS3 = {
      getMessage: jest.fn().mockReturnValue(mockMessage),
      getChunkDirectory: jest.fn().mockReturnValue('s3-chunk-dir'),
      setTaskParametersFromQueueMessage: jest.fn(),
      hasSufficientTaskInfo: jest.fn().mockReturnValue(false),
      runChunking: jest.fn().mockResolvedValue(undefined),
      getBulkResetFlag: jest.fn().mockReturnValue(false),
      getTrustPreviousStorageFlag: jest.fn().mockReturnValue(true),
      getSyncPopulation: jest.fn().mockReturnValue('person-full'),
      noMessagesFromQueue: undefined
    } as any;

    // Mock constructors
    (ChunkerQueue as jest.MockedClass<typeof ChunkerQueue>).mockImplementation(() => mockChunkerQueue);
    (ChunkFromAPI as jest.MockedClass<typeof ChunkFromAPI>).mockImplementation(() => mockChunkFromAPI);
    (ChunkFromS3 as jest.MockedClass<typeof ChunkFromS3>).mockImplementation(() => mockChunkFromS3);

    // Mock TaskProtection
    (TaskProtection as jest.MockedClass<typeof TaskProtection>).mockImplementation(() => ({
      enable: jest.fn().mockResolvedValue(undefined),
      disable: jest.fn().mockResolvedValue(undefined)
    } as any));

    // Mock MetadataManager
    (MetadataManager.read as jest.Mock) = jest.fn().mockResolvedValue({});
    (MetadataManager.writeFlags as jest.Mock) = jest.fn().mockResolvedValue(undefined);

    // Mock Utils
    (Utils.objectExistsInS3 as jest.Mock) = jest.fn().mockResolvedValue(true);
    (Utils.getLocalConfig as jest.Mock) = jest.fn().mockReturnValue('./config.json');

    // Mock ConfigManager
    const mockConfigManager = {
      reset: jest.fn().mockReturnThis(),
      fromJsonString: jest.fn().mockReturnThis(),
      fromSecretManager: jest.fn().mockReturnThis(),
      fromEnvironment: jest.fn().mockReturnThis(),
      fromFileSystem: jest.fn().mockReturnThis(),
      getConfigAsync: jest.fn().mockResolvedValue({
        dataSource: {
          endpointConfig: {
            baseUrl: 'https://api.test.com',
            fetchPath: '/people'
          }
        }
      })
    };
    (ConfigManager.getInstance as jest.Mock) = jest.fn().mockReturnValue(mockConfigManager);

    // Mock HuronPersonCache
    (HuronPersonCache as jest.MockedClass<typeof HuronPersonCache>).mockImplementation(() => ({
      setS3PopulationCache: jest.fn().mockResolvedValue(undefined)
    } as any));
    (HuronPersonCache as any).CACHE_FILE_NAME = 'cache.ndjson';

    jest.clearAllMocks();
  });

  afterEach(() => {
    process.env = originalEnv;
    processExitSpy.mockRestore();
  });

  describe('Line 392 Execution', () => {
    it('should call deleteMessageFromQueue in finally block after successful chunking', async () => {
      // Act: Call the actual main() function
      await main();

      // Assert: Verify line 392 executed
      expect(deleteMessageSpy).toHaveBeenCalledWith(mockMessage);
      expect(deleteMessageSpy).toHaveBeenCalledTimes(1);
      expect(processExitSpy).toHaveBeenCalledWith(0);
    });

    it('should call deleteMessageFromQueue even if runChunking throws error', async () => {
      // Arrange: Make runChunking throw
      mockChunkFromAPI.runChunking = jest.fn().mockRejectedValue(new Error('Chunking failed'));

      // Act: Call main()
      await main();

      // Assert: Finally block still executed line 392
      expect(deleteMessageSpy).toHaveBeenCalledWith(mockMessage);
      expect(deleteMessageSpy).toHaveBeenCalledTimes(1);
      expect(processExitSpy).toHaveBeenCalledWith(1);
    });

    it('should NOT call deleteMessageFromQueue if no messages in queue', async () => {
      // Arrange: Empty queue (getMessageBody returns undefined)
      mockChunkerQueue.getMessageBody = jest.fn().mockReturnValue(undefined);
      mockChunkerQueue.receiveMessageFromQueue = jest.fn().mockResolvedValue(undefined);

      // Act: Call main()
      await main();

      // Assert: No deletion attempted (chunker.noMessagesFromQueue = true)
      expect(deleteMessageSpy).not.toHaveBeenCalled();
      expect(processExitSpy).toHaveBeenCalledWith(0);
    });

    it('should NOT call deleteMessageFromQueue if chunker is undefined', async () => {
      // Arrange: Both S3 and API chunkers fail to initialize
      mockChunkFromS3.hasSufficientTaskInfo = jest.fn().mockReturnValue(false);
      mockChunkFromAPI.hasSufficientConfig = jest.fn().mockReturnValue(false);

      // Act: Call main()
      await main();

      // Assert: No deletion (chunker is undefined)
      expect(deleteMessageSpy).not.toHaveBeenCalled();
      expect(processExitSpy).toHaveBeenCalledWith(1);
    });

    it('should call deleteMessageFromQueue with S3 chunker message', async () => {
      // Arrange: S3 chunker has sufficient config
      mockChunkFromS3.hasSufficientTaskInfo = jest.fn().mockReturnValue(true);
      mockChunkFromS3.getMessage = jest.fn().mockReturnValue(mockMessage);

      // Act: Call main()
      await main();

      // Assert: S3 chunker's message deleted
      expect(deleteMessageSpy).toHaveBeenCalledWith(mockMessage);
      expect(deleteMessageSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('Early Exit Scenarios', () => {
    it('should NOT delete message if CHUNKS_BUCKET is missing', async () => {
      // Arrange: Remove required env var
      delete process.env.CHUNKS_BUCKET;

      // Act: Call main()
      await main();

      // Assert: Early return, no deletion
      expect(deleteMessageSpy).not.toHaveBeenCalled();
      expect(processExitSpy).toHaveBeenCalledWith(1);
    });

    it('should STILL delete message even if chunking already finished (early exit)', async () => {
      // Arrange: Metadata exists (chunking already done) - causes early return
      const readSpy = jest.spyOn(MetadataManager, 'read').mockResolvedValue({
        chunkCount: 10,
        totalRecords: 2000
      });

      // Act: Call main()
      await main();

      // Assert: Message still deleted in finally block (prevents it from becoming visible again)
      // This is CORRECT behavior - even though we exit early, the message was already
      // received and needs to be deleted to prevent reprocessing
      expect(deleteMessageSpy).toHaveBeenCalledWith(mockMessage);
      expect(processExitSpy).toHaveBeenCalledWith(0);
      
      readSpy.mockRestore();
    });
  });

  describe('Debug Information', () => {
    it('should execute finally block even with process.exit', async () => {
      const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();

      // Act: Call main()
      await main();

      // Assert: Timer logs show finally block executed
      expect(consoleLogSpy).toHaveBeenCalledWith(
        expect.stringContaining('✓ Chunker process completed successfully')
      );
      expect(deleteMessageSpy).toHaveBeenCalled();

      consoleLogSpy.mockRestore();
    });
  });
});
