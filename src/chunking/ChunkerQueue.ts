import { DeleteMessageCommand, DeleteMessageCommandInput, DeleteMessageCommandOutput, Message, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { TestEnvironment } from "integration-core";
import { AbstractAtomicCounter } from "../AtomicCounter";
import { ApiChunkerEvent } from "./ChunkerSubscriber";
import { handleApiEvent } from "./fetch/ChunkerApiSubscriber";
import { TaskParameters } from "./fetch/ChunkFromAPI";
import { SyncPopulation } from "../../docker/chunkTypes";

export type ChunkerQueueParams = {
  isEcsTask: boolean,
  QueueUrl?: string,
  region?: string
};

export const CHUNKER_COUNTER_NAME = 'chunker-offset-counter';

/**
 * Utility class to handle interactions with the SQS queue for the chunker service, with 
 * respect to getting and removing the single message associated with the current chunking task.
 */
export class ChunkerQueue {
  private readonly isEcsTask: boolean;
  private readonly QueueUrl?: string;
  private readonly region?: string;
  private atomicCounter: AbstractAtomicCounter | undefined;

  private message: Message | undefined;

  constructor(params: ChunkerQueueParams) {
    const { SQS_QUEUE_URL, REGION, STACK_ID, DYNAMODB_ATOMIC_COUNTER_TABLE_NAME } = process.env;
    const { isEcsTask, QueueUrl = SQS_QUEUE_URL, region = REGION } = params;
    this.isEcsTask = isEcsTask;
    this.QueueUrl = QueueUrl;
    this.region = region ?? REGION;
    if (!this.QueueUrl) {
      throw new Error('SQS_QUEUE_URL is required in environment variables');
    }
    if (!this.region) {
      throw new Error('REGION is required in environment variables');
    }
    if (DYNAMODB_ATOMIC_COUNTER_TABLE_NAME && STACK_ID) {
      console.log('Atomic counter detected');
      this.atomicCounter = new class extends AbstractAtomicCounter {
        getCounterName(): string {
          return CHUNKER_COUNTER_NAME;
        }
      }(STACK_ID, region!);
    }
  }

  public get queueUrl(): string | undefined {
    return this.QueueUrl;
  }


  /**
   * Receives a message from the SQS queue, which contains the parameters for the 
   * chunking task to process.
   * @returns 
   */
  public receiveMessageFromQueue = async (): Promise<Message | undefined> => {
    const { QueueUrl, region, isEcsTask } = this;
    console.log(`SQS_QUEUE_URL detected: ${QueueUrl} - reading task parameters from SQS queue`);
    const sqsClient = new SQSClient({ region });
  
    try {
      const command = new ReceiveMessageCommand({
        QueueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,
      });
  
      const response = await sqsClient.send(command);
      const messages = response.Messages || [];
  
      if (messages.length === 0) {
        if (isEcsTask) {
          console.log('No messages in queue - this probably means that the desired count for the ' +
            'service has not scaled down yet to zero after processing the last message and deleting ' +
            'it from  the queue. An empty queue will eventually cause the service to scale down to ' +
            'zero, but in the meantime we should just exit the task.');
          console.log('✗ Task cancelled')
          return undefined;
        }
        console.log('No messages in queue');
        return undefined;
      }
  
      const message = messages[0];
      this.message = message;
  
      return message;
    } catch (error) {
      console.error('Error reading from SQS queue:', error);
      this.message = undefined;
      return undefined;
    }
  }

  public getMessageBody = (): string | undefined => {
    const { message, message: { Body = '{}' } = {} } = this;
    if(!message) {
      return undefined;
    }
    return JSON.parse(Body || '{}');
  }

  /**
   * Deletes the specified message from the SQS queue to prevent it from being processed again.
   * @param message 
   * @returns 
   */
  public deleteMessageFromQueue = async (message?: Message): Promise<void> => {
    const { QueueUrl, region, message: currentMessage } = this;
    if(!message) {
      message = currentMessage;
    }
    if (!message || !message.ReceiptHandle) {
      console.warn('Cannot delete message from queue - missing ReceiptHandle:', JSON.stringify(message));
      return;
    }
    try {
      const input = {
        QueueUrl,
        ReceiptHandle: message.ReceiptHandle,
      } as DeleteMessageCommandInput;
      console.log(`Deleting message from queue: ${JSON.stringify(input)}`);
      const sqsClient = new SQSClient({ region });
      const output = await sqsClient.send(
        new DeleteMessageCommand({
          QueueUrl,
          ReceiptHandle: message.ReceiptHandle,
        })
      ) as DeleteMessageCommandOutput;
      output.$metadata.httpStatusCode === 200
        ? console.log('✓ Message deleted from queue successfully')
        : console.warn('✗ Failed to delete message from queue:', output);
    }
    catch (error) {
      console.error('Error deleting message from SQS queue:', error);
    }
  }

  /**
   * Reads task parameters from the next message from the SQS queue, and then deletes the message.
   * Priority: SQS message > Environment variables
   */
  public popMessageFromQueue = async (): Promise<Message | undefined> => {
    const { QueueUrl, receiveMessageFromQueue, deleteMessageFromQueue } = this;
  
    if (QueueUrl) {
      console.log(`SQS_QUEUE_URL detected: ${QueueUrl} - reading task parameters from SQS queue`);
  
      try {
        // Read (lease) message from SQS queue
        const message: Message | undefined = await receiveMessageFromQueue();
  
        if(message) {
  
          // Delete message from queue (prevents reprocessing)
          await deleteMessageFromQueue(message);
  
          return message;
        }
        return undefined;
      } catch (error) {
        console.error('Error reading from SQS queue:', error);
      }
    }
    return undefined;
  }

  private getNextOffset = async (currentOffset: number, limit: number): Promise<number> => {
    const { atomicCounter } = this;
    if(atomicCounter) {
      return await atomicCounter.increment(limit);
    }
    else {
      return currentOffset + limit;
    }
  }

  /**
   * Create and send the next SQS message for parallel chunking.
   * Uses atomic counter (if available) to calculate next offset, preventing collisions.
   * This is called BEFORE starting the current chunking task to enable true parallelism.
   * 
   * @param params.limit Number of batches to process per task
   * @param params.offset Current offset in population
   * @param params.chunkDirectory Directory to store chunk data for this message's chunking task
   * @param params.taskParameters The parameters for the chunking task to process, which will be passed through to the next message
   * @param params.dryRun If true, will not actually send the message but will log the parameters instead (default: false)
   * @returns true if message sent successfully, false if skipped
   */
  public sendNextChunkingMessage = async ( params: {
    limit: number; offset: number, chunkDirectory: string, taskParameters: TaskParameters, dryRun?: boolean
  }): Promise<boolean> => {

    const { QueueUrl } = this;

    const { limit, offset: currentOffset, chunkDirectory, taskParameters, dryRun } = params; 

    const { 
      baseUrl, 
      fetchPath, 
      populationType, 
      bulkReset, 
      trustPreviousStorage
    } = taskParameters;

    // Don't create next message if limit is 0 (process all)
    if (limit === 0) {
      console.log('ℹ️  limit=0 (process all). Not creating next message.');
      return false;
    }

    try {
      const nextOffset = await this.getNextOffset(currentOffset, limit);
      if (dryRun) {
        console.log(`[DRY RUN] Would send next chunking message to SQS: offset=${nextOffset}, limit=${limit}`);
        return true;
      }

      const apiChunkerEvent = {
        baseUrl, 
        fetchPath, 
        populationType, 
        bulkReset, 
        trustPreviousStorage, 
        limit, 
        offset: nextOffset, 
        chunkDirectory
      } satisfies ApiChunkerEvent;

      // Send the SQS message
      await handleApiEvent(apiChunkerEvent, QueueUrl);

      return true;
    } catch (error) {
      console.error('Error sending next chunking message to SQS queue:', error);
      return false;
    }
  }
}

/**
 * Test harness to verify sendNextChunkingMessage behavior with atomic counter.
 * 
 * Simulates the scenario after queue seeding:
 * 1. Queue has been seeded with 15 messages (offsets 0-140)
 * 2. Atomic counter is at 140
 * 3. A task calls sendNextChunkingMessage with currentOffset=140
 * 4. Expected: Next message should have offset=150 (atomic counter incremented by limit=10)
 * 
 * To test multiple tasks in parallel (simulating real ECS behavior):
 * - Run this harness multiple times simultaneously
 * - Each invocation should get a unique offset (150, 160, 170, etc.)
 * - No collisions should occur
 */
async function main() {
  
  let {
    BASE_URL: baseUrl,
    FETCH_PATH: fetchPath,
    POPULATION_TYPE: populationType,
    DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT: limitStr,
    OFFSET: offsetStr,
    CHUNK_DIRECTORY: chunkDirectory,
    BULK_RESET: bulkReset,
    TRUST_PREVIOUS_STORAGE: trustPreviousStorage,
    DRY_RUN: dryRun
  } = process.env;

  // Validate required parameters
  if (!baseUrl) {
    throw new Error('BASE_URL is required');
  }
  if (!fetchPath) {
    throw new Error('FETCH_PATH is required');
  }
  if (!populationType) {
    throw new Error('POPULATION_TYPE is required');
  }
  if (!limitStr) {
    throw new Error('LIMIT is required');
  }
  if (!offsetStr) {
    throw new Error('OFFSET is required (current offset for this task)');
  }
  if(!chunkDirectory) {
    throw new Error('CHUNK_DIRECTORY is required (directory to store chunk data for this message\'s chunking task)');
  }

  // Parse numeric parameters
  const limit = parseInt(limitStr, 10);
  const offset = parseInt(offsetStr, 10);

  // Validate populationType
  const { PersonDelta, PersonFull } = SyncPopulation;
  const validPopulationTypes = [PersonFull, PersonDelta];
  const normalizedPopulationType = populationType?.toLowerCase() === PersonDelta ? PersonDelta : PersonFull;

  if (!validPopulationTypes.includes(normalizedPopulationType)) {
    throw new Error(`Invalid POPULATION_TYPE: ${populationType}. Must be one of: ${validPopulationTypes.join(', ')}`);
  }

  // Validate numeric parameters
  if (isNaN(limit) || limit <= 0) {
    throw new Error(`Invalid LIMIT: ${limitStr}. Must be a positive integer.`);
  }
  if (isNaN(offset) || offset < 0) {
    throw new Error(`Invalid OFFSET: ${offsetStr}. Must be a non-negative integer.`);
  }

  console.log('🔧 ChunkerQueue Test Harness\n');
  console.log(`📋 Configuration:`);
  console.log(`   - Current Offset: ${offset}`);
  console.log(`   - Limit: ${limit}`);
  console.log(`   - Population Type: ${normalizedPopulationType}`);
  console.log(`   - Base URL: ${baseUrl}`);
  console.log(`   - Fetch Path: ${fetchPath}`);
  console.log(`   - Chunk Directory: ${chunkDirectory}`);
  console.log(`   - Dry Run: ${dryRun === 'true' ? 'YES' : 'NO'}\n`);

  try {
    // Create ChunkerQueue instance
    const chunkerQueue = new ChunkerQueue({
      isEcsTask: false  // Running locally as test harness
    });

    // Prepare task parameters
    const taskParameters: TaskParameters = {
      baseUrl,
      fetchPath,
      populationType: normalizedPopulationType,
      bulkReset: bulkReset?.toLowerCase() === 'true',
      trustPreviousStorage: trustPreviousStorage?.toLowerCase() === 'true'
    };

    console.log('📤 Calling sendNextChunkingMessage...\n');

    // Call sendNextChunkingMessage (simulates what happens after processing a chunk)
    const success = await chunkerQueue.sendNextChunkingMessage({
      limit,
      offset,
      chunkDirectory,
      taskParameters,
      dryRun: dryRun === 'true'
    });

    if (success) {
      console.log('\n✅ sendNextChunkingMessage completed successfully');
      console.log('   Next message created with incremented offset (check logs above for details)\n');
      process.exit(0);
    } else {
      console.error('\n❌ sendNextChunkingMessage failed or was skipped\n');
      process.exit(1);
    }
  } catch (error) {
    console.error('\n❌ Error during test harness execution:', error);
    process.exit(1);
  }
}


if (require.main === module) {
  const testEnvironment = TestEnvironment('CHUNKER_QUEUE');

  // Required environment variables
  [
    'SQS_QUEUE_URL',
    'REGION',
    'STACK_ID',
    'DYNAMODB_ATOMIC_COUNTER_TABLE_NAME',
    'BASE_URL',
    'FETCH_PATH',
    'POPULATION_TYPE',
    'DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT',
    'OFFSET',
    'BULK_RESET',
    'TRUST_PREVIOUS_STORAGE',
    'CHUNK_DIRECTORY'
  ].forEach(testEnvironment.getVar);
  
  main();
}