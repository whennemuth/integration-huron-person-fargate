import { DeleteMessageCommand, Message, ReceiveMessageCommand, SQSClient, DeleteMessageCommandOutput, DeleteMessageCommandInput } from "@aws-sdk/client-sqs";
import { handleApiEvent } from "./fetch/ChunkerApiSubscriber";
import { ApiChunkerEvent } from "./ChunkerSubscriber";
import { TaskParameters } from "./fetch/ChunkFromAPI";

export type ChunkerQueueParams = {
  isEcsTask: boolean,
  QueueUrl?: string,
  region?: string
};

/**
 * Utility class to handle interactions with the SQS queue for the chunker service, with 
 * respect to getting and removing the single message associated with the current chunking task.
 */
export class ChunkerQueue {
  private readonly isEcsTask: boolean;
  private readonly QueueUrl?: string;
  private readonly region?: string;

  private message: Message | undefined;

  constructor(params: ChunkerQueueParams) {
    const { SQS_QUEUE_URL, REGION } = process.env;
    const { isEcsTask, QueueUrl = SQS_QUEUE_URL, region = REGION } = params;
    this.isEcsTask = isEcsTask;
    this.QueueUrl = QueueUrl;
    this.region = region;
    if (!this.QueueUrl) {
      throw new Error('SQS_QUEUE_URL is required in environment variables');
    }
    if (!this.region) {
      throw new Error('REGION is required in environment variables');
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

  /**
   * Create and send the next SQS message for parallel chunking.
   * Calculates the next offset (currentOffset + limit) and sends a message to the chunker queue.
   * This is called BEFORE starting the current chunking task to enable true parallelism.
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

    // Don't create next message if limit is 0 (process all)
    if (limit === 0) {
      console.log('ℹ️  limit=0 (process all). Not creating next message.');
      return false;
    }

    try {
      const { baseUrl, fetchPath, populationType, bulkReset, trustPreviousStorage } = taskParameters;
      const nextOffset = currentOffset + limit;
      if (dryRun) {
        console.log(`[DRY RUN] Would send next chunking message to SQS: offset=${nextOffset}, limit=${limit}`);
        return true;
      }

      const apiChunkerEvent = {
        baseUrl, fetchPath, populationType, bulkReset, trustPreviousStorage, limit, offset: nextOffset, 
        chunkDirectory
      } as ApiChunkerEvent;

      // Send the SQS message
      await handleApiEvent(apiChunkerEvent, QueueUrl);

      return true;
    } catch (error) {
      return false;
    }
  }
  
}