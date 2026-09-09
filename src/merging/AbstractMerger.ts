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
import { FieldDefinitions } from 'integration-huron-person';
import { extractChunkDirectory } from '../chunking/filedrop/ChunkPathUtils';
import { DeferredDeleteHandler } from './DeferredDeleteHandler';
import { MetadataFactory } from '../chunking/metadata';
import { getConfig } from '../../docker/chunker';
import { SyncPopulation } from '../../docker/chunkTypes';
import { TaskProtection } from '../TaskProtection';

export interface TaskParameters {
  chunksBucket: string;
  chunkDirectory: string | null;
  createdAt?: string; // Timestamp from chunker metadata (when chunking started)
}

export interface MergeResult {
  chunkCount: number;
  totalLines: number;
  outputKey: string;
  deletedChunks: string[];
}

export interface MergeContext {
  bucketName: string;
  chunkDir: string;
  region?: string;
  sharedDeltaStorageDir: string;
  dryRun: boolean;
  primaryKeyFieldNames: string[];
  primaryKeyFieldSet: Set<string>;
}

/**
 * Abstract merger base class implementing Template Method pattern.
 */
export abstract class AbstractMerger {
  /**
   * Reads task parameters from SQS queue or environment variables.
   * Priority: SQS message > Environment variables
   * 
   * COMMON LOGIC: Same for both S3 and DynamoDB modes
   */
  protected async getTaskParameters(): Promise<TaskParameters | null> {
    const { SQS_QUEUE_URL, CHUNKS_BUCKET, CHUNK_DIRECTORY, INPUT_KEY, REGION } = process.env;

    // Mode 1: ECS Fargate - read from SQS queue
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
        return {
          chunksBucket: body.chunksBucket,
          chunkDirectory: body.chunkDirectory || null,
          createdAt: body.createdAt, // Timestamp from chunker metadata
        };
      } catch (error) {
        console.error('Error reading from SQS queue:', error);
        return null;
      }
    }

    // Mode 2: Local development - read from environment variables
    if (CHUNKS_BUCKET && (CHUNK_DIRECTORY || INPUT_KEY)) {
      console.log('Running in local context - reading task parameters from environment variables');
      return {
        chunksBucket: CHUNKS_BUCKET,
        chunkDirectory: CHUNK_DIRECTORY || (INPUT_KEY ? extractChunkDirectory(INPUT_KEY) : null),
      };
    }

    return null;
  }

  /**
   * Processes deferred deletions using DeferredDeleteHandler.
   * 
   * COMMON LOGIC: Same for both S3 and DynamoDB modes
   * 
   * Compares current sync state against previous state to identify records
   * removed from source and soft-deletes them from target API.
   * 
   * @param params Context for deletion processing
   */
  protected async processDeferredDeletes(params: {
    bucketName: string;
    chunkDir: string;
    sourceKey: string;
    targetKey: string;
    primaryKeyFieldNames: string[];
    region?: string;
  }): Promise<void> {
    const { bucketName, chunkDir, sourceKey, targetKey, primaryKeyFieldNames, region } = params;

    if (!DeferredDeleteHandler.isConfiguredForDeletes()) {
      console.log(`\nStep 3.5: Deletion handling disabled (skipping)`);
      return;
    }

    // First check if the population type for this sync is compatible with deletion processing
    const config = await getConfig();
    const metadataManager = MetadataFactory.create({
      config,
      previousStorageType: process.env.PREVIOUS_STORAGE_TYPE,
      statisticsTableName: process.env.DYNAMODB_STATISTICS_TABLE_NAME
    });
    const flags = await metadataManager.readFlags({ bucketName, chunkDirectory: chunkDir, region });
    const { syncPopulation, useMockTarget } = flags;

    if (syncPopulation === SyncPopulation.PersonDelta) {
      console.log(`  Sync population type is PersonDelta - Deletion handling does NOT apply.`);
      // NOTE: Even if this check were not being carried out, the ignoreRemovals flag would have
      // already been set to true in the MergerSubscriber when the chunking job was kicked off 
      // for a PersonDelta sync, which would have prevented any deletions from being included 
      // in the merged output via use of the appropriate DeltaStrategy decorator 
      // (ie: src\delta-strategy\IgnoreRemovalsDeltaStrategy.ts). So this is really just an 
      // additional safeguard to avoid accidentally running deletion logic for an incompatible sync type.
      return;
    }

    console.log(`\nStep 3.5: Processing deletions`);
    try {
      const deleteHandler = await DeferredDeleteHandler.getInstance({
        region,
        bucketName,
        sourceKey,
        targetKey,
        primaryKeyFieldNames,
        useMockTarget,
      });

      const deletionResult = await deleteHandler!.processDeletes();
      console.log(`  ${deletionResult.message}`);
      if (deletionResult.totalProcessed > 0) {
        console.log(`  Deleted: ${deletionResult.deletedCount} of ${deletionResult.totalProcessed}`);
        if (deletionResult.failedCount > 0) {
          console.warn(`  Failed: ${deletionResult.failedCount} deletions failed`);
        }
      }
    } catch (deleteError: any) {
      console.error(`  Failed to process deletions: ${deleteError.message}`);
      // Don't fail the entire merge if deletions fail - log and continue
      console.warn(`  Continuing with merge despite deletion failure`);
    }
  }

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
  protected abstract merge(context: MergeContext): Promise<MergeResult | null>;

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
  public async main(): Promise<void> {
    const timer = new Timer();
    timer.start();
    let exitCode = 0;
    let chunkingStartTime: Date | null = null;

    try {
      // Enable task protection for 1 hour (protects from sigkills by ECS during scale-in)
      await new TaskProtection(60).enable();

      // Step 1: Get task parameters
      const taskParams = await this.getTaskParameters();

      if (!taskParams || !taskParams.chunkDirectory) {
        console.error('ERROR: No task parameters available (checked SQS queue and environment variables)');
        exitCode = 1;
        return;
      }

      const { chunksBucket: bucketName, chunkDirectory: chunkDir, createdAt } = taskParams;

      // Read additional configuration from environment
      const {
        REGION: region,
        SHARED_DELTA_STORAGE_DIR = 'delta-storage',
        DRY_RUN = 'false'
      } = process.env;

      // Parse chunking start time from task parameters (provided by MergerSubscriber lambda)
      chunkingStartTime = createdAt ? new Date(createdAt) : null;
      if (chunkingStartTime) {
        console.log(`\nChunking started at: ${createdAt}`);
      }
      const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';

      // Extract primary key field names
      const primaryKeyFieldNames = FieldDefinitions.filter(fd => fd.isPrimaryKey).map(fd => fd.name);
      const primaryKeyFieldSet = new Set(primaryKeyFieldNames);

      // Step 2: Prepare merge context
      const context: MergeContext = {
        bucketName,
        chunkDir,
        region,
        sharedDeltaStorageDir: SHARED_DELTA_STORAGE_DIR,
        dryRun,
        primaryKeyFieldNames,
        primaryKeyFieldSet,
      };

      // Step 3: Call mode-specific merge implementation
      const result = await this.merge(context);

      // Step 4: Process deferred deletions (if merge produced output)
      if (result && result.outputKey) {
        const deltaDir = chunkDir.replace(/^chunks\//, 'deltas/');
        const sourceKey = result.outputKey;
        const targetKey = `${SHARED_DELTA_STORAGE_DIR}/previous-input.ndjson`;

        await this.processDeferredDeletes({
          bucketName,
          chunkDir,
          sourceKey,
          targetKey,
          primaryKeyFieldNames,
          region,
        });
      }

      // Step 5: Log summary
      if (result) {
        console.log(`\nMerge Summary:`);
        console.log(`  Chunks consolidated: ${result.chunkCount}`);
        console.log(`  Records in chunks: ${result.totalLines}`);
        console.log(`  Primary key field(s): ${Array.from(primaryKeyFieldSet).join(', ')}`);
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
