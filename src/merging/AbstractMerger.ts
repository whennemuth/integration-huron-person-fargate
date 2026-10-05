/**
 * Abstract base class for merger implementations.
 * 
 * Implements the Template Method pattern to share common logic between
 * S3-based and DynamoDB-based mergers while allowing mode-specific implementations.
 * 
 * ## Common Responsibilities (Implemented Here)
 * - Reading task parameters from SQS or environment variables
 * - Processing deferred deletions (DeferredDeleteHandler)
 * - Timer management and task protection
 * - Error handling and exit code management
 * 
 * ## Mode-Specific Responsibilities (Subclass Implementation)
 * - merge(): File consolidation logic (S3) or no-op (DynamoDB)
 * 
 * ## Template Method Flow
 * 1. Get task parameters (SQS or environment)
 * 2. Call subclass merge() implementation
 * 3. Process deferred deletions (if configured)
 * 4. Log summary and cleanup
 */

import { DeleteMessageCommand, DeleteMessageCommandInput, DeleteMessageCommandOutput, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Timer } from 'integration-core';
import { TaskProtection } from '../TaskProtection';
import { AbstractDeferredDeleteHandler } from './AbstractDeferredDeleteHandler';

export type TaskParameters = {
  createdAt?: string; // Timestamp from chunker metadata (when chunking started)
}

export type MergeResult = { }

export type MergeContext = {
  region?: string;
  dryRun: boolean;
}

/**
 * Abstract merger base class implementing Template Method pattern.
 */
export abstract class AbstractMerger {
  protected taskParameters: TaskParameters | null = null;
  protected mergeContext: MergeContext | null = null;

  /**
   * Read a message from the SQS queue and delete it after retrieval.
   * @returns The parsed message body if a message was retrieved, or null if no message was available or an error occurred.
   */
  protected async getMessageFromSQS(): Promise<any | null> {
    const { SQS_QUEUE_URL, REGION } = process.env;

    if (SQS_QUEUE_URL) {
      console.log('Running in ECS context - reading task parameters from SQS queue');
      const sqsClient = new SQSClient({ region: REGION });

      try {
        const command = new ReceiveMessageCommand({
          QueueUrl: SQS_QUEUE_URL,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 20,
        });

        const response = await sqsClient.send(command);
        const messages = response.Messages || [];

        if (messages.length === 0) {
          console.log('No messages in queue (queue empty or wait expired)');
          console.log('Empty queue - this probably means that the desired count for the ' +
            'service has not scaled down yet to zero after processing the last message and deleting ' +
            'it from the queue. An empty queue will eventually cause the service to scale down to ' +
            'zero, but in the meantime we should just exit the task.');
          console.log('✗ Task cancelled.');
          process.exit(0);
        }

        const message = messages[0];
        const body = JSON.parse(message.Body || '{}');

        // Delete message from queue (prevents reprocessing)
        if (message.ReceiptHandle) {
          const input = {
            QueueUrl: SQS_QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
          } as DeleteMessageCommandInput;
          console.log(`Deleting message from queue: ${JSON.stringify(input)}`);
          const output = await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: SQS_QUEUE_URL,
              ReceiptHandle: message.ReceiptHandle,
            })
          ) as DeleteMessageCommandOutput;
          output.$metadata.httpStatusCode === 200
            ? console.log('✓ Message deleted from queue successfully')
            : console.warn('✗ Failed to delete message from queue:', output);
        }
        console.log('Task parameters from SQS:', JSON.stringify(body));
        return body;
      } catch (error) {
        console.error('Error reading from SQS queue:', error);
        return null;
      }  
    }
    return null;   
  }

  /**
   * Reads task parameters from SQS queue or environment variables.
   * Priority: SQS message > Environment variables
   * 
   * COMMON LOGIC: Same for both S3 and DynamoDB modes
   */  
  public abstract getTaskParameters(): Promise<TaskParameters | null>;

  public abstract getMergeContext(): Promise<MergeContext | null>;

  /**
   * Mode-specific merge implementation.
   * 
   * MODE-SPECIFIC: Must be implemented by subclasses
   * 
   * - S3 mode: Consolidates chunk delta files, merges with baseline
   * - DynamoDB mode: No file consolidation needed (atomic writes to shared tables)
   * 
   * @param context Merge context with common parameters
   * @returns Merge result with statistics
   */
  protected abstract merge(taskParams?: TaskParameters): Promise<MergeResult | null>;

  /**
   * Processes deferred deletions using an instance of AbstractDeferredDeleteHandler.
   * Compares current sync state against previous state to identify records
   * removed from source and soft-deletes them from target API.
   * 
   * @param params Context for deletion processing
   */
  protected abstract runDeferredDeletes(mergeResult: MergeResult): Promise<void>; 

  public getChunkingStartTime = async (): Promise<Date | null> => {
    const taskParams = await this.getTaskParameters();
    const { createdAt } = taskParams as TaskParameters;
    return createdAt ? new Date(createdAt) : null;
  }

  /**
   * Template method orchestrating the entire merge process.
   * 
   * TEMPLATE METHOD: Defines the skeleton of the algorithm
   * 
   * Flow:
   * 1. Get task parameters (common)
   * 2. Prepare merge context (common)
   * 3. Call subclass merge() implementation (mode-specific)
   * 4. Process deferred deletions (common)
   * 5. Log summary and cleanup (common)
   */
  public async main(taskParams?: TaskParameters): Promise<void> {
    const timer = new Timer();
    timer.start();
    let exitCode = 0;
    let chunkingStartTime: Date | null = null;

    try {
      // Enable task protection for 1 hour (protects from sigkills by ECS during scale-in)
      await new TaskProtection(60).enable();

      // Step 1: Get task parameters
      if(!taskParams) {
        taskParams = await this.getTaskParameters() || undefined;
      }

      if (!taskParams) {
        exitCode = 1;
        return;
      }

      // Step 2: Call mode-specific merge implementation
      const result = await this.merge(taskParams);
      
      // Step 3: Check if deletion handling is configured
      if (!AbstractDeferredDeleteHandler.isConfiguredForDeletes()) {
        console.log(`\nStep 3.5: Deletion handling disabled (skipping)`);
        return;
      }

      if (result) {
        // Step 4: Process deferred deletions (if merge produced output)
        await this.runDeferredDeletes(result);
      }        

      exitCode = 0;

    } catch (error: any) {
      console.error('\n✗ Merge failed:', error.message);
      console.error(error.stack);
      exitCode = 1;
    } finally {
      timer.stop();

      if (exitCode === 0) {
        timer.logElapsed('\n✓ Merge phase duration');

        // Calculate and log full sync duration from chunking start to merge end
        const chunkingStartTime = await this.getChunkingStartTime();
        if (chunkingStartTime) {
          const fullDurationMs = Date.now() - chunkingStartTime.getTime();
          const fullDuration = timer.getDuration(fullDurationMs);
          console.log(`✓ Full sync duration (chunking → processing → merging): ${fullDuration}`);
        } else {
          console.log('  (Full sync duration unavailable - createdAt timestamp not provided)');
        }
      } else {
        timer.logElapsed('\n✗ Duration until failure');
      }
      await new TaskProtection().disable();
      process.exit(exitCode);
    }
  }
}
