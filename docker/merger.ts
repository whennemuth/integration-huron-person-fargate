/**
 * Merger Entry Point (Phase 3)
 * 
 * This module runs after all processor tasks complete. It concatenates
 * all the individual delta chunk output files into a single previous-input.ndjson 
 * file for the next full sync cycle.
 * 
 * Two modes of operation:
 * 1. ECS Fargate (production): Reads CHUNKS_BUCKET and CHUNK_DIRECTORY from SQS message
 * 2. Local development: Reads from environment variables (fallback)
 * 
 * Architecture:
 * - Lists all files matching pattern: deltas/{key-path}/{timestamp}/chunk-*.ndjson
 * - Streams each delta chunk file and concatenates NDJSON content
 * - Writes concatenated output to: delta-storage/previous-input.ndjson (no timestamp)
 * - Deletes all delta chunk files after successful merge
 * 
 * Environment Variables:
 * - SQS_QUEUE_URL: SQS queue URL to read task parameters from (ECS mode)
 * - CHUNKS_BUCKET: (Local mode) Bucket containing the delta chunk files
 * - INPUT_KEY: (Local mode) Original input file key (used to derive delta directory path)
 * - CHUNK_DIRECTORY: (Local mode) Optional explicit chunk directory (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z")
 * - REGION: AWS region (e.g., 'us-east-2')
 * - DRY_RUN: If "true", runs the merge without writing output or deleting chunks (default: false)
 * 
 * Input:
 * - Multiple NDJSON delta files: deltas/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson, etc.
 * 
 * Output:
 * - Single merged file: delta-storage/previous-input.ndjson
 * - Cleanup of all delta chunk files
 * 
 * Example Local Usage:
 * ```bash
 * CHUNKS_BUCKET=chunks-bucket INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json REGION=us-east-2 node dist/merger.js
 * ```
 */

import { S3 } from '@aws-sdk/client-s3';
import { DeleteMessageCommand, DeleteMessageCommandInput, DeleteMessageCommandOutput, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { FieldSet, Timer } from 'integration-core';
import { FieldDefinitions, HashMapMerger } from 'integration-huron-person';
import { objectExistsInS3 } from '../src/Utils';
import { extractChunkDirectory } from '../src/chunking/filedrop/ChunkPathUtils';
import { DeferredDeleteHandler } from '../src/merging/DeferredDeleteHandler';
import { MergeEngine } from '../src/merging/MergeEngine';
import { MetadataManager } from '../src/chunking/Metadata';
import { SyncPopulation } from './chunkTypes';

interface TaskParameters {
  chunksBucket: string;
  chunkDirectory: string | null;
  createdAt?: string; // Timestamp from chunker metadata (when chunking started)
}

/**
 * Reads task parameters from SQS queue or environment variables.
 * Priority: SQS message > Environment variables
 */
async function getTaskParameters(): Promise<TaskParameters | null> {
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
 * Main entry point
 */
async function main() {

  const timer = new Timer();
  timer.start();
  let exitCode = 0;

  // Get task parameters from SQS or environment
  const taskParams = await getTaskParameters();

  if (!taskParams || !taskParams.chunkDirectory) {
    console.error('ERROR: No task parameters available (checked SQS queue and environment variables)');
    exitCode = 1;
    return;
  }

  const { chunksBucket: bucketName, chunkDirectory: chunkDir, createdAt } = taskParams;

  // Read additional configuration from environment
  const {
    REGION: region,
    SHARED_DELTA_STORAGE_DIR='delta-storage',
    DRY_RUN='false'
  } = process.env;

  // Parse chunking start time from task parameters (provided by MergerSubscriber lambda)
  const chunkingStartTime = createdAt ? new Date(createdAt) : null;
  if (chunkingStartTime) {
    console.log(`\nChunking started at: ${createdAt}`);
  }
  const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';
  
  // Extract primary key field names from the same FieldDefinitions used when writing delta files.
  // This ensures consistency between delta file writing (processor.ts → SyncPeople.ts → EndToEnd.ts → 
  // InputUtils.getKeyAndHashFieldSets) and merging (this file). Both use the isPrimaryKey flag from 
  // DataMapper._fieldDefinitions (exported as FieldDefinitions).
  const primaryKeyFieldNames = FieldDefinitions.filter(fd => fd.isPrimaryKey).map(fd => fd.name);
  const primaryKeyFieldSet = new Set(primaryKeyFieldNames);

  try {
    // Convert chunk directory to delta directory
    // Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "deltas/person-full/2026-03-03T19:58:41.277Z"
    const deltaDir = chunkDir.replace(/^chunks\//, 'deltas/');
    
    // Shared output is always delta-storage (agnostic to person-full/person-delta sync type)
    // Since previous-input.ndjson is cumulative (records never removed, only added/updated),
    // there's no need to separate full vs. delta syncs - both merge into the same state.
    const sharedOutputPath = SHARED_DELTA_STORAGE_DIR;

    console.log(`\nSource delta directory: ${deltaDir}`);
    console.log(`Target output directory: ${sharedOutputPath}`);

    // Create single Merger instance with both paths
    const merger = new MergeEngine({ bucketName, deltaDir, sharedDeltaDir: sharedOutputPath, region });
    
    // Merge delta chunks from timestamped directory
    const result = await merger.merge();

    // Now copy the merged result to shared location (without timestamp)
    const sourceKey = result.outputKey;
    const targetKey = merger.getSharedOutputKey();

    // Check if the merged chunks delta file actually exists before attempting to read and merge with existing baseline.
    const consolidatedMergableExists = await objectExistsInS3(bucketName, sourceKey, region);

    if(consolidatedMergableExists) {
      console.log(`\nMerging output (in 4 steps) to shared location:`);
      console.log(`  From: s3://${bucketName}/${sourceKey}`);
      console.log(`  Into: s3://${bucketName}/${targetKey}`);

      const s3 = new S3({ region });
      
      // Step 1: Read consolidated chunks from delta directory and parse as FieldSets
      console.log(`\nStep 1: Reading consolidated chunks from: s3://${bucketName}/${sourceKey}`);
      const { Body } = await s3.getObject({ Bucket: bucketName, Key: sourceKey });
      const content = await Body?.transformToString();
      const lines = content?.split('\n').filter(l => l.trim()) || [];
      
      const consolidatedChunks: FieldSet[] = [];
      for (const line of lines) {
        try {
          consolidatedChunks.push(JSON.parse(line) as FieldSet);
        } catch (parseError: any) {
          throw new Error(`Failed to parse consolidated chunks: ${parseError.message}`);
        }
      }
      console.log(`  Parsed ${consolidatedChunks.length} records from consolidated chunks`);
      
      // Step 2: Read existing previous-input.ndjson from shared location (if exists)
      console.log(`\nStep 2: Reading existing baseline from shared location`);
      const existingBaseline = await merger.readExistingMergedFile();
      
      // Step 3: Merge consolidated chunks with existing baseline
      console.log(`\nStep 3: Merging data`);
      const hashMapMerger = new HashMapMerger();
      
      if (!existingBaseline || existingBaseline.length === 0) {
        console.log(`  No existing baseline found - writing consolidated chunks as new baseline`);
        
        // First run - just write consolidated chunks
        await merger.writeMergedToSharedLocation(consolidatedChunks);

        // Step 4: Cleanup
        console.log(`\nStep 4: Cleanup 🧹`);
        await merger.cleanup();
      } else {
        console.log(`  Existing baseline: ${existingBaseline.length} records`);
        console.log(`  Consolidated chunks: ${consolidatedChunks.length} records`);
        
        // Convert to KeyHashPairs
        const baselineKeyPairs = HashMapMerger.fieldSetsToKeyHashPairs(existingBaseline, primaryKeyFieldSet);
        const incrementalKeyPairs = HashMapMerger.fieldSetsToKeyHashPairs(consolidatedChunks, primaryKeyFieldSet);
        
        // Merge
        const mergeResult = hashMapMerger.merge(baselineKeyPairs, incrementalKeyPairs);
        
        console.log(`  Merge statistics:`);
        console.log(`    Retained (baseline only): ${mergeResult.stats.retained}`);
        console.log(`    Added (new): ${mergeResult.stats.added}`);
        console.log(`    Updated (hash changed): ${mergeResult.stats.updated}`);
        console.log(`    Unchanged (same hash): ${mergeResult.stats.unchanged}`);
        console.log(`    Total: ${mergeResult.stats.total}`);
        
        // Step 3.5: Process deletions if configured
        // After merge, compare consolidated (current source state) vs baseline (previous state)
        // to identify records truly removed from source and soft-delete them from target API
        if (DeferredDeleteHandler.isConfiguredForDeletes()) {
          // First check if the population type for this sync is compatible with deletion processing (e.g., PersonDelta). We don't want to run deletion logic for sync types that aren't designed for it.
          const flags = await MetadataManager.readFlags({ bucketName, chunkDirectory: chunkDir, region });
          const { syncPopulation } = flags;
          if(syncPopulation === SyncPopulation.PersonDelta) {
            console.log(`  Sync population type is PersonDelta - Deletion handling does NOT apply.`);
            // NOTE: Even if this check were not being carried out, the ignoreRemovals flag would have
            // already been set to true in the MergerSubscriber when the chunking job was kicked off 
            // for a PersonDelta sync, which would have prevented any deletions from being included 
            // in the merged output via use of the appropriate DeltaStrategy decorator 
            // (ie: src\delta-strategy\IgnoreRemovalsDeltaStrategy.ts). So this is really just an 
            // additional safeguard to avoid accidentally running deletion logic for an incompatible sync type.
          }
          else {
            console.log(`\nStep 3.5: Processing deletions`);
            try {
              const deleteHandler = await DeferredDeleteHandler.getInstance({
                region, bucketName, sourceKey, targetKey, primaryKeyFieldNames,
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
        } else {
          console.log(`\nStep 3.5: Deletion handling disabled (skipping)`);
        }
        
        // Convert back to FieldSets
        const mergedFieldSets = HashMapMerger.keyHashPairsToFieldSets(mergeResult.merged);
        
        // Write merged result
        await merger.writeMergedToSharedLocation(mergedFieldSets);

        // Cleanup
        console.log(`\nCleaning up 🧹`);
        await merger.cleanup();
      }
    }
    else {
      console.log(`\n⚠ Merged delta file not found at expected location: s3://${bucketName}/${sourceKey}. ` +
        `This means that there were no deltas detected for this chunk that differ from the existing ` +
        `baseline, so no merge was necessary. Also, no consolidated chunk delta output file was written, and ` +
        `so no cleanup was necessary. This is expected if there were no changes in the data within the scope ` +
        `of the chunk since the last sync.`
      );
    }

    console.log(`\nMerge Summary:`);
    console.log(`  Chunks consolidated: ${result.chunkCount}`);
    console.log(`  Records in chunks: ${result.totalLines}`);
    console.log(`  Timestamped output: s3://${bucketName}/${sourceKey}`);
    console.log(`  Shared output (merged): s3://${bucketName}/${merger.getSharedOutputKey()}`);
    console.log(`  Cleanup: ${result.deletedChunks.length} delta chunk files deleted`);
    console.log(`  Primary key field(s): ${Array.from(primaryKeyFieldSet).join(', ')}`);

    exitCode = 0;

  } catch (error: any) {
    console.error('\n✗ Merge failed:', error.message);
    console.error(error.stack);
    exitCode = 1;
  }
  finally {
    timer.stop();
    
    if(exitCode === 0) {
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
    
    process.exit(exitCode);
  }
}

// Run if executed directly
if (require.main === module) {
  main();
}

