import { DeleteMessageCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

/**
 * Represents the next chunk to be processed
 */
export type NextChunk = {
  bucketName: string;
  s3Key: string;
}

/**
 * Represents a task message to be sent to a queue
 */
export type TaskMessage = {
  [key: string]: string;
}

/**
 * Abstract base class for sending messages to trigger queue-processing Fargate tasks.
 * 
 * This abstraction allows triggering tasks by sending messages to SQS queues,
 * which the QueueProcessingFargateService monitors and auto-scales based on queue depth.
 * 
 * Instead of directly launching ECS tasks (which doesn't work with QueueProcessingFargateService),
 * we send messages to SQS, and the service auto-scales to process them.
 */
export abstract class QueueMessageSender {
  /**
   * Sends a message to the queue to trigger task processing.
   * The QueueProcessingFargateService will detect the message and auto-scale to handle it.
   * 
   * @param message - Task parameters as key-value pairs (e.g., {INPUT_BUCKET: 'my-bucket', INPUT_KEY: 'data.json'})
   * @returns True if message was successfully sent, false otherwise
   */
  public abstract sendMessage(message: TaskMessage): Promise<boolean>;
}

/**
 * SQS implementation for sending messages to trigger Fargate tasks.
 * 
 * When a message is sent to the queue:
 * 1. Queue depth (ApproximateNumberOfMessagesVisible) increases
 * 2. QueueProcessingFargateService detects the metric change
 * 3. Service auto-scales up (increases desired count)
 * 4. ECS launches Fargate task(s) to process messages
 * 5. Task reads message from queue and performs work
 * 6. Queue depth decreases, service scales back down
 * 
 * This is the correct way to trigger queue-processing services, not direct ECS RunTask calls.
 */
export class SQSMessageSender extends QueueMessageSender {
  private sqsClient: SQSClient;
  private queueUrl: string;

  constructor(queueUrl: string, sqsClient?: SQSClient) {
    super();
    this.queueUrl = queueUrl;
    this.sqsClient = sqsClient || new SQSClient({});
  }

  async sendMessage(message: TaskMessage): Promise<boolean> {
    try {
      const command = new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(message),
      });

      await this.sqsClient.send(command);
      console.log(`✅ Message sent to queue: ${this.queueUrl}`);
      console.log(`   Message: ${JSON.stringify(message)}`);
      return true;
    } catch (error) {
      console.error(`❌ Error sending message to SQS queue ${this.queueUrl}:`, error);
      return false;
    }
  }
}

/**
 * Abstract base class for reading chunk processing messages from different sources.
 * 
 * This abstraction allows the processor to work in two different environments:
 * 1. Local development (docker-compose): Uses environment variables CHUNKS_BUCKET and CHUNK_KEY
 * 2. AWS Fargate: Reads S3 event notifications from SQS queue
 * 
 * The appropriate implementation is automatically selected based on environment variables.
 */
export abstract class QueueReader {
  /**
   * Retrieves the next chunk to process.
   * Returns undefined when no chunks are available.
   */
  public abstract receiveMessage(): Promise<NextChunk | undefined>;

  /**
   * Factory method that returns the appropriate QueueReader implementation
   * based on the current environment.
   * 
   * Decision logic:
   * - If CHUNKS_BUCKET and CHUNK_KEY environment variables exist → EnvironmentBasedQueueSimulator (local dev)
   * - Otherwise → SQSQueueReader (AWS Fargate)
   */
  public static getInstance = (): QueueReader => {
    const { CHUNKS_BUCKET, CHUNK_KEY } = process.env;
    if (CHUNKS_BUCKET && CHUNK_KEY) {
      console.log('Running in non-ECS context - simulating queue with environment variables CHUNKS_BUCKET and CHUNK_KEY');
      return new EnvironmentBasedQueueSimulator();
    } else {
      console.log('Running in ECS context - using SQSQueueReader to read from SQS');
      return new SQSQueueReader(process.env.SQS_QUEUE_URL || '');
    }
  }
}

/**
 * Queue implementation for local development using docker-compose.
 * 
 * Simulates a single-message queue by reading CHUNKS_BUCKET and CHUNK_KEY environment variables.
 * After the first receiveMessage() call, the queue becomes empty.
 * 
 * Usage:
 *   CHUNKS_BUCKET=my-bucket CHUNK_KEY=chunks/chunk-0001.ndjson docker-compose up processor
 */
export class EnvironmentBasedQueueSimulator extends QueueReader {
  private nextChunk: NextChunk | undefined;

  constructor() {
    super();
    const { CHUNKS_BUCKET, CHUNK_KEY } = process.env;
    if (CHUNKS_BUCKET && CHUNK_KEY) {
      this.nextChunk = { bucketName: CHUNKS_BUCKET, s3Key: CHUNK_KEY };
    } else {
      console.warn('Environment variables CHUNKS_BUCKET and/or CHUNK_KEY not set. Queue will be empty.');
      this.nextChunk = undefined;
    }
  }

  async receiveMessage(): Promise<NextChunk | undefined> {
    const chunkToReturn = this.nextChunk;
    this.nextChunk = undefined; // Simulate message being consumed (single-use queue)
    return chunkToReturn;
  }
}

/**
 * Queue implementation for AWS Fargate that reads from SQS.
 * 
 * Processes S3 event notifications sent to SQS when chunk files are created.
 * S3 event notifications have the following JSON structure:
 * 
 * {
 *   "Records": [{
 *     "s3": {
 *       "bucket": {"name": "my-bucket"},
 *       "object": {"key": "chunks/chunk-0001.ndjson"}
 *     }
 *   }]
 * }
 * 
 * This implementation:
 * 1. Polls SQS with long polling (20s wait time)
 * 2. Parses S3 event notification JSON
 * 3. Extracts bucket name and object key
 * 4. Deletes message from queue (prevents reprocessing)
 * 5. Returns NextChunk for processor to handle
 * 
 * See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
 */
export class SQSQueueReader extends QueueReader {
  private sqsClient: SQSClient;
  private queueUrl: string;

  constructor(queueUrl: string, sqsClient?: SQSClient) {
    super();
    this.queueUrl = queueUrl;
    this.sqsClient = sqsClient || new SQSClient({});
  }

  async receiveMessage(): Promise<NextChunk | undefined> {
    try {
      const command = new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20 // Long polling reduces empty responses and costs
      });

      const response = await this.sqsClient.send(command);
      const messages = response.Messages || [];
      
      if (messages.length === 0) {
        return undefined; // No messages available (queue empty or wait expired)
      }

      const message = messages[0];
      
      // Parse S3 event notification JSON structure
      const s3Event = JSON.parse(message.Body || '{}');
      const records = s3Event.Records || [];
      
      if (records.length === 0) {
        console.warn('SQS message contained no S3 event records');
        await this.deleteMessage(message.ReceiptHandle!);
        return undefined;
      }

      // Extract S3 bucket and key from first record
      const record = records[0];
      const bucketNameEncoded = record.s3?.bucket?.name;
      const s3KeyEncoded = record.s3?.object?.key;

      if (!bucketNameEncoded || !s3KeyEncoded) {
        console.error('Invalid S3 event notification structure:', JSON.stringify(record));
        await this.deleteMessage(message.ReceiptHandle!);
        return undefined;
      }

      // S3 event notifications URL-encode both bucket names and object keys
      // Decode to get actual values (e.g., "2026-04-07T16%3A33%3A13.197Z" -> "2026-04-07T16:33:13.197Z")
      const bucketName = decodeURIComponent(bucketNameEncoded);
      const s3Key = decodeURIComponent(s3KeyEncoded);
      
      console.log(`Received S3 event: bucket=${bucketName}, key=${s3Key}`);

      // Delete message after successful parsing (prevents reprocessing)
      await this.deleteMessage(message.ReceiptHandle!);

      return { bucketName, s3Key };
    } catch (error) {
      console.error('Error receiving message from SQS:', error);
      return undefined;
    }
  }

  /**
   * Helper method to delete a message from the queue.
   * Called after successfully processing the message to prevent redelivery.
   */
  private async deleteMessage(receiptHandle: string): Promise<void> {
    try {
      await this.sqsClient.send(new DeleteMessageCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: receiptHandle
      }));
    } catch (error) {
      console.error('Error deleting message from SQS:', error);
      // Don't throw - message will become visible again after visibility timeout
    }
  }
}