/**
 * S3-based Merger Implementation
 * 
 * Extends AbstractMerger to implement S3-specific merge logic:
 * - Consolidates delta chunk files from deltas/{population}/{timestamp}/
 * - Merges consolidated deltas with existing baseline (previous-input.ndjson)
 * - Writes merged result to shared location (delta-storage/previous-input.ndjson)
 * - Cleans up temporary delta chunk files
 * 
 * Uses HashMapMerger for hash-based deduplication and merge logic.
 */

import { S3 } from '@aws-sdk/client-s3';
import { FieldSet } from 'integration-core';
import { FieldDefinitions, HashMapMerger } from 'integration-huron-person';
import { objectExistsInS3 } from '../Utils';
import { MergeEngine } from './MergeEngine';
import { AbstractMerger, MergeContext, MergeResult, TaskParameters } from './AbstractMerger';
import { extractChunkDirectory } from '../chunking/filedrop/ChunkPathUtils';
import { SyncPopulation } from '../../docker/chunkTypes';
import { MetadataFactoryForBootstrap } from '../chunking/metadata';
import { DeferredDeleteHandlerForS3, DeferredDeleteHandlerForS3Params } from './DeferredDeleteHandlerForS3';

export type TaskParametersForS3 = TaskParameters & {
  chunksBucket: string;
  chunkDirectory: string | null;
}

export type MergeContextForS3 = MergeContext & {
  bucketName: string;
  chunkDir: string;
  sharedDeltaStorageDir: string;
  primaryKeyFieldNames: string[];
  primaryKeyFieldSet: Set<string>;
}

export type MergeResultForS3 = MergeResult & {
  chunkCount: number;
  totalLines: number;
  outputKey: string;
  deletedChunks: string[];
}

export class MergerForS3 extends AbstractMerger {

  /**
   * Reads task parameters from SQS queue or environment variables.
   * Priority: SQS message > Environment variables
   * 
   * COMMON LOGIC: Same for both S3 and DynamoDB modes
   */
  public async getTaskParameters(): Promise<TaskParametersForS3 | null> {
    if (this.taskParameters) {
      return this.taskParameters as TaskParametersForS3;
    }

    const { SQS_QUEUE_URL, CHUNKS_BUCKET, CHUNK_DIRECTORY, INPUT_KEY } = process.env;
    let taskParams: TaskParametersForS3 | null = null;
    
    if(SQS_QUEUE_URL) {
      const msgBody = await this.getMessageFromSQS();
      if (msgBody) {
        taskParams = {
          chunksBucket: msgBody.chunksBucket,
          chunkDirectory: msgBody.chunkDirectory || null,
          createdAt: msgBody.createdAt, // Timestamp from chunker metadata
        };
      }
    }
    else {
      // Mode 2: Local development - read from environment variables
      if (CHUNKS_BUCKET && (CHUNK_DIRECTORY || INPUT_KEY)) {
        console.log('Running in local context - reading task parameters from environment variables');
        taskParams = {
          chunksBucket: CHUNKS_BUCKET,
          chunkDirectory: CHUNK_DIRECTORY || (INPUT_KEY ? extractChunkDirectory(INPUT_KEY) : null),
        };
      }
    }
      
    if (taskParams && taskParams.chunkDirectory) {
      this.taskParameters = taskParams;
      return taskParams;
    }

    console.error('ERROR: No task parameters available (checked SQS queue and environment variables)');
    return null;
  }

  public async getMergeContext(taskParams?: TaskParametersForS3): Promise<MergeContextForS3 | null> {
    if (this.mergeContext) {
      return this.mergeContext as MergeContextForS3;
    }

    if (!taskParams) {
      taskParams = await this.getTaskParameters() as TaskParametersForS3;
    }
    const { chunksBucket: bucketName, chunkDirectory: chunkDir, createdAt } = taskParams;

    // Read additional configuration from environment
    const {
      REGION: region,
      // Shared output is always delta-storage (agnostic to person-full/person-delta sync type)
      SHARED_DELTA_STORAGE_DIR = 'delta-storage',
      DRY_RUN = 'false'
    } = process.env;

    // Parse chunking start time from task parameters (provided by MergerSubscriber lambda)
    const chunkingStartTime = await this.getChunkingStartTime();
    if (chunkingStartTime) {
      console.log(`\nChunking started at: ${createdAt}`);
    }

    // Extract primary key field names
    const primaryKeyFieldNames = FieldDefinitions.filter(fd => fd.isPrimaryKey).map(fd => fd.name);
    const primaryKeyFieldSet = new Set(primaryKeyFieldNames);

    const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';    
    this.mergeContext = {
      bucketName,
      chunkDir,
      region,
      sharedDeltaStorageDir: SHARED_DELTA_STORAGE_DIR,
      dryRun,
      primaryKeyFieldNames,
      primaryKeyFieldSet,
    } as MergeContextForS3;

    return this.mergeContext as MergeContextForS3;
  }

  /**
   * S3-specific merge implementation.
   * 
   * Four-step process:
   * 1. Read consolidated chunks from delta directory
   * 2. Read existing baseline from shared location (if exists)
   * 3. Merge consolidated chunks with baseline using HashMapMerger
   * 4. Write merged result and cleanup
   * 
   * @param context Merge context with bucket, directories, and configuration
   * @returns Merge result with statistics and output location
   */
  protected async merge(taskParams?: TaskParametersForS3): Promise<MergeResultForS3 | null> {

    const mergeContext = await this.getMergeContext(taskParams) as MergeContextForS3;
    const {
      chunkDir,
      bucketName,
      region,
      sharedDeltaStorageDir,
      primaryKeyFieldNames,
      primaryKeyFieldSet,
      dryRun
    } = mergeContext;

    // const { bucketName, chunkDir, region, sharedDeltaStorageDir, primaryKeyFieldSet } = context;

    // Convert chunk directory to delta directory
    // Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "deltas/person-full/2026-03-03T19:58:41.277Z"
    const deltaDir = `${chunkDir}`.replace(/^chunks\//, 'deltas/');

    console.log(`\nSource delta directory: ${deltaDir}`);
    console.log(`Target output directory: ${sharedDeltaStorageDir}`);

    // Create MergeEngine instance with both paths
    const merger = new MergeEngine({ bucketName, deltaDir, sharedDeltaDir: sharedDeltaStorageDir, region });
    
    // Merge delta chunks from timestamped directory
    const result = await merger.merge();

    // Now copy the merged result to shared location (without timestamp)
    const sourceKey = result.outputKey;
    const targetKey = merger.getSharedOutputKey();

    // Check if the merged chunks delta file actually exists before attempting to read and merge with existing baseline.
    const consolidatedMergableExists = await objectExistsInS3(bucketName, sourceKey, region);

    if (!consolidatedMergableExists) {
      console.log(`\n⚠ Merged delta file not found at expected location: s3://${bucketName}/${sourceKey}. ` +
        `This means that there were no deltas detected for this chunk that differ from the existing ` +
        `baseline, so no merge was necessary. Also, no consolidated chunk delta output file was written, and ` +
        `so no cleanup was necessary. This is expected if there were no changes in the data within the scope ` +
        `of the chunk since the last sync.`
      );
      return null;
    }

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
      
      // Convert back to FieldSets
      const mergedFieldSets = HashMapMerger.keyHashPairsToFieldSets(mergeResult.merged);
      
      // Write merged result
      await merger.writeMergedToSharedLocation(mergedFieldSets);

      // Cleanup
      console.log(`\nStep 4: Cleanup 🧹`);
      await merger.cleanup();
    }

    console.log(`\nS3 Merge Complete:`);
    console.log(`  Timestamped output: s3://${bucketName}/${sourceKey}`);
    console.log(`  Shared output (merged): s3://${bucketName}/${merger.getSharedOutputKey()}`);
    console.log(`  Cleanup: ${result.deletedChunks.length} delta chunk files deleted`);

    console.log(`\nMerge Summary:`);
    console.log(`  Chunks consolidated: ${result.chunkCount}`);
    console.log(`  Records in chunks: ${result.totalLines}`);
    console.log(`  Primary key field(s): ${Array.from(primaryKeyFieldSet).join(', ')}`);

    return {
      chunkCount: result.chunkCount,
      totalLines: result.totalLines,
      outputKey: sourceKey,
      deletedChunks: result.deletedChunks,
    };
  }

  protected async runDeferredDeletes(result: MergeResultForS3): Promise<void> {
    if(!result.outputKey) {
      console.warn(`  No output key found in merge result. Skipping deferred deletes.`);
      return;
    }

    const mergeContext = await this.getMergeContext() as MergeContextForS3;
    const { bucketName, chunkDir, region, sharedDeltaStorageDir, primaryKeyFieldNames } = mergeContext;
    const sourceKey = result.outputKey;
    const targetKey = `${sharedDeltaStorageDir}/previous-input.ndjson`;

    // First check if the population type for this sync is compatible with deletion processing.
    // Tries the mock statistics table first (if configured), falling back to the real table, since
    // flags.useMockTarget (what determines which table chunker used) can only be learned from the
    // flags themselves. See MetadataFactoryForBootstrap.resolveMockAwareFlags().
    const { flags } = await new MetadataFactoryForBootstrap().resolveMockAwareFlags({
      bucketName, chunkDirectory: chunkDir, region
    });
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
      const deleteHandler = new DeferredDeleteHandlerForS3({
        bucketName, 
        primaryKeyFieldNames, 
        baselineNdjsonPath: targetKey,
        mergedNdjsonPath: sourceKey,
        region,
        useMockTarget,
      } as DeferredDeleteHandlerForS3Params);

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
}
