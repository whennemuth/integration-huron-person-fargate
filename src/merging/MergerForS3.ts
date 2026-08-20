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
import { HashMapMerger } from 'integration-huron-person';
import { objectExistsInS3 } from '../Utils';
import { MergeEngine } from './MergeEngine';
import { AbstractMerger, MergeContext, MergeResult } from './AbstractMerger';

export class MergerForS3 extends AbstractMerger {
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
  protected async merge(context: MergeContext): Promise<MergeResult | null> {
    const { bucketName, chunkDir, region, sharedDeltaStorageDir, primaryKeyFieldSet } = context;

    // Convert chunk directory to delta directory
    // Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "deltas/person-full/2026-03-03T19:58:41.277Z"
    const deltaDir = chunkDir.replace(/^chunks\//, 'deltas/');
    
    // Shared output is always delta-storage (agnostic to person-full/person-delta sync type)
    const sharedOutputPath = sharedDeltaStorageDir;

    console.log(`\nSource delta directory: ${deltaDir}`);
    console.log(`Target output directory: ${sharedOutputPath}`);

    // Create MergeEngine instance with both paths
    const merger = new MergeEngine({ bucketName, deltaDir, sharedDeltaDir: sharedOutputPath, region });
    
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

    return {
      chunkCount: result.chunkCount,
      totalLines: result.totalLines,
      outputKey: sourceKey,
      deletedChunks: result.deletedChunks,
    };
  }
}
