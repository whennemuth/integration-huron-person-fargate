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
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand, DeleteMessageCommandInput, DeleteMessageCommandOutput } from '@aws-sdk/client-sqs';
import * as readline from 'readline';
import { Readable } from 'stream';
import { extractChunkBasePath } from '../src/chunking/filedrop/ChunkPathUtils';
import { HashMapMerger, FieldDefinitions } from 'integration-huron-person';
import { FieldSet } from 'integration-core';

interface TaskParameters {
  chunksBucket: string;
  chunkDirectory: string | null;
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
      chunkDirectory: CHUNK_DIRECTORY || (INPUT_KEY ? extractChunkBasePath(INPUT_KEY) : null),
    };
  }

  return null;
}

interface MergerConfig {
  bucketName: string;
  deltaDir: string;
  sharedDeltaDir: string;
  region?: string;
}

interface MergeResult {
  totalLines: number;
  chunkCount: number;
  outputKey: string;
  deletedChunks: string[];
}

/**
 * Merger class handles concatenation of chunk outputs into final file
 */
class Merger {
  private s3: S3;
  private config: MergerConfig;
  private chunkKeys: string[] = [];

  constructor(config: MergerConfig) {
    this.config = config;
    this.s3 = new S3({ region: config.region || 'us-east-1' });
  }

  /**
   * List all delta chunk files in S3
   */
  private async listChunkFiles(): Promise<string[]> {
    const prefix = `${this.config.deltaDir}/`;
    console.log(`Listing delta chunk files with prefix: ${prefix}`);

    try {
      const response = await this.s3.listObjectsV2({
        Bucket: this.config.bucketName,
        Prefix: prefix
      });

      const chunkKeys = (response.Contents || [])
        .map(obj => obj.Key!)
        .filter(key => key.match(/chunk-\d+\.ndjson$/))
        .sort(); // Sort to ensure deterministic order

      console.log(`Found ${chunkKeys.length} delta chunk files`);
      return chunkKeys;

    } catch (error: any) {
      throw new Error(`Failed to list delta chunk files: ${error.message}`);
    }
  }

  /**
   * Stream a single delta chunk file and return line count
   */
  private async readChunkLines(chunkKey: string): Promise<string[]> {
    console.log(`  Reading: s3://${this.config.bucketName}/${chunkKey}`);

    try {
      const response = await this.s3.getObject({
        Bucket: this.config.bucketName,
        Key: chunkKey
      });

      const lines: string[] = [];
      const stream = response.Body as Readable;
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (line.trim()) {
          lines.push(line);
        }
      }

      console.log(`    Read ${lines.length} lines`);
      return lines;

    } catch (error: any) {
      throw new Error(`Failed to read chunk ${chunkKey}: ${error.message}`);
    }
  }

  /**
   * Get the full path to the shared previous-input.ndjson file
   */
  public getSharedOutputKey(): string {
    return `${this.config.sharedDeltaDir}/previous-input.ndjson`;
  }

  /**
   * Read existing merged file from shared location if it exists
   */
  public async readExistingMergedFile(): Promise<FieldSet[] | null> {
    const key = this.getSharedOutputKey();
    try {
      console.log(`  Checking for existing file: s3://${this.config.bucketName}/${key}`);
      
      const response = await this.s3.getObject({
        Bucket: this.config.bucketName,
        Key: key
      });

      const fieldSets: FieldSet[] = [];
      const stream = response.Body as Readable;
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (line.trim()) {
          try {
            const fieldSet = JSON.parse(line) as FieldSet;
            fieldSets.push(fieldSet);
          } catch (parseError: any) {
            throw new Error(`Failed to parse NDJSON line in existing file: ${parseError.message}`);
          }
        }
      }

      console.log(`    Found ${fieldSets.length} existing records`);
      return fieldSets;

    } catch (error: any) {
      // NoSuchKey means file doesn't exist yet (first run) - this is okay
      if (error.name === 'NoSuchKey' || error.Code === 'NoSuchKey') {
        console.log(`    No existing file found (first run)`);
        return null;
      }
      
      // Any other error is a real failure - fail fast
      throw new Error(`Failed to read existing merged file ${key}: ${error.message}`);
    }
  }

  /**
   * Write merged content to output file containing
   */
  private async writeMergedDeltaFile(lines: string[], outputKey: string): Promise<void> {
    console.log(`\nWriting merged delta file: s3://${this.config.bucketName}/${outputKey}`);
    console.log(`Total lines: ${lines.length}`);

    const content = lines.join('\n') + '\n';

    try {
      await this.s3.putObject({
        Bucket: this.config.bucketName,
        Key: outputKey,
        Body: content,
        ContentType: 'application/x-ndjson'
      });

      console.log('✓ Merged delta file written successfully');

    } catch (error: any) {
      throw new Error(`Failed to write merged delta file: ${error.message}`);
    }
  }

  /**
   * Write merged content to shared location
   */
  public async writeMergedToSharedLocation(fieldSets: FieldSet[]): Promise<void> {
    const key = this.getSharedOutputKey();
    console.log(`  ✓ Writing ${fieldSets.length} records to shared location: s3://${this.config.bucketName}/${key}`);
    
    const content = fieldSets.map((fs: FieldSet) => JSON.stringify(fs)).join('\n') + '\n';
    
    try {
      await this.s3.putObject({
        Bucket: this.config.bucketName,
        Key: key,
        Body: content,
        ContentType: 'application/x-ndjson'
      });
    } catch (error: any) {
      throw new Error(`Failed to write to shared location: ${error.message}`);
    }
  }

  /**
   * Delete all delta chunk files after successful merge
   */
  private async deleteDeltaChunkFiles(chunkKeys: string[]): Promise<void> {
    const { bucketName: Bucket} = this.config;

    console.log(`\nDeleting ${chunkKeys.length} delta chunk files...`);

    if (chunkKeys.length === 0) {
      console.log('No delta chunk files to delete');
      return;
    }

    try {
      // AWS S3 deleteObjects can handle up to 1000 objects at once
      const chunks = [];
      for (let i = 0; i < chunkKeys.length; i += 1000) {
        chunks.push(chunkKeys.slice(i, i + 1000));
      }

      for (const chunk of chunks) {
        console.log('Deleting delta chunk files:');
        chunk.forEach(key => console.log(`  s3://${Bucket}/${key}`));

        await this.s3.deleteObjects({
          Bucket,
          Delete: {
            Objects: chunk.map(key => ({ Key: key })),
            Quiet: true
          }
        });
      }

      console.log('✓ All delta chunk files deleted');

    } catch (error: any) {
      throw new Error(`Failed to delete delta chunk files: ${error.message}`);
    }
  }

  /**
   * Cleanup delta files after successful merge.
   * 
   * Why cleanup in merger task instead of processor:
   * ================================================
   * Delta files created and deleted within 6-15 seconds can cause S3 event notifications
   * to be cancelled or never sent. S3 needs time to process the ObjectCreated event internally.
   * 
   * If a file is deleted before the event completes:
   * - S3 detects object no longer exists
   * - Event is cancelled or never sent
   * - Lambda never invokes (no log stream)
   * - Delta files never merged = data loss
   * 
   * Solution: Keep delta files until merger completes successfully, then cleanup.
   * This ensures:
   * - Delta files persist long enough for S3 events to process
   * - Cleanup only happens after proven merge success
   * - Delta files available for debugging if merge fails
   * - Marker files preserved for audit trail (never deleted)
   */
  private async deleteDeltaFiles(): Promise<void> {
    console.log(`\n🧹 Cleaning up delta files from: s3://${this.config.bucketName}/${this.config.deltaDir}/`);

    const { bucketName: Bucket} = this.config;

    try {
      const prefix = `${this.config.deltaDir}/`;
      const response = await this.s3.listObjectsV2({
        Bucket,
        Prefix: prefix,
      });

      const deltaFiles = (response.Contents || [])
        .map((obj) => obj.Key!)
        .filter((key) => /\.ndjson$/.test(key)); // All .ndjson files (chunks and merged outputs), not marker files

      if (deltaFiles.length === 0) {
        console.log('No delta files to cleanup');
        return;
      }

      console.log(`Deleting ${deltaFiles.length} delta files...`);
      
      // AWS S3 deleteObjects can handle up to 1000 objects at once
      const chunks = [];
      for (let i = 0; i < deltaFiles.length; i += 1000) {
        chunks.push(deltaFiles.slice(i, i + 1000));
      }

      for (const chunk of chunks) {
        chunk.forEach(key => console.log(`  s3://${Bucket}/${key}`));
        await this.s3.deleteObjects({
          Bucket,
          Delete: {
            Objects: chunk.map(key => ({ Key: key })),
            Quiet: true
          }
        });
      }

      console.log(`✓ Cleanup complete: ${deltaFiles.length} delta files deleted`);
      console.log('Note: Marker files preserved for audit trail');

    } catch (error: any) {
      console.error(`Failed to cleanup delta files: ${error.message}`);
      // Don't throw - cleanup failure shouldn't block merger task completion
    }
  }

  /**
   * Execute the merge operation
   */
  async merge(): Promise<MergeResult> {
    console.log('=== Phase 3: Merger ===\n');
    console.log(`Bucket: ${this.config.bucketName}`);
    console.log(`Delta directory: ${this.config.deltaDir}`);
    console.log(`Shared delta directory: ${this.config.sharedDeltaDir}`);
    console.log(`Region: ${this.config.region || 'default (us-east-1)'}\n`);

    // Step 1: List all delta chunk files
    this.chunkKeys = await this.listChunkFiles();

    if (this.chunkKeys.length === 0) {
      console.warn('⚠ No delta chunk files found. Nothing to merge.');
      return {
        totalLines: 0,
        chunkCount: 0,
        outputKey: `${this.config.deltaDir}/previous-input.ndjson`,
        deletedChunks: []
      };
    }

    // Step 2: Read and concatenate all chunks
    console.log('\nReading delta chunk files:');
    const allLines: string[] = [];

    for (const chunkKey of this.chunkKeys) {
      const lines = await this.readChunkLines(chunkKey);
      allLines.push(...lines);
    }

    // Step 3: Write merged delta file
    const outputKey = `${this.config.deltaDir}/previous-input.ndjson`;
    await this.writeMergedDeltaFile(allLines, outputKey);

    console.log('\n✓ Merge completed successfully');

    return {
      totalLines: allLines.length,
      chunkCount: this.chunkKeys.length,
      outputKey,
      deletedChunks: this.chunkKeys
    };
  }

  async cleanup(): Promise<void> {
    // Step 4: Cleanup delta chunk files and delta files
    await this.deleteDeltaChunkFiles(this.chunkKeys);
    await this.deleteDeltaFiles();
  }
}

/**
 * Main entry point
 */
async function main() {
  // Get task parameters from SQS or environment
  const taskParams = await getTaskParameters();

  if (!taskParams || !taskParams.chunkDirectory) {
    console.error('ERROR: No task parameters available (checked SQS queue and environment variables)');
    process.exit(1);
  }

  const { chunksBucket: bucketName, chunkDirectory: chunkDir } = taskParams;

  // Read additional configuration from environment
  const {
    REGION: region,
    SHARED_DELTA_STORAGE_DIR='delta-storage',
    DRY_RUN='false'
  } = process.env;
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
    const merger = new Merger({ 
      bucketName, 
      deltaDir, 
      sharedDeltaDir: sharedOutputPath, 
      region 
    });
    
    // Merge delta chunks from timestamped directory
    const result = await merger.merge();

    // Now copy the merged result to shared location (without timestamp)
    const sourceKey = result.outputKey;
    const targetKey = merger.getSharedOutputKey();
    
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
      console.log(`\nCleaning up 🧹`);
      await merger.cleanup();
    }

    console.log(`\nMerge Summary:`);
    console.log(`  Chunks consolidated: ${result.chunkCount}`);
    console.log(`  Records in chunks: ${result.totalLines}`);
    console.log(`  Timestamped output: s3://${bucketName}/${sourceKey}`);
    console.log(`  Shared output (merged): s3://${bucketName}/${merger.getSharedOutputKey()}`);
    console.log(`  Cleanup: ${result.deletedChunks.length} delta chunk files deleted`);
    console.log(`  Primary key field(s): ${Array.from(primaryKeyFieldSet).join(', ')}`);

    process.exit(0);

  } catch (error: any) {
    console.error('\n✗ Merge failed:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  main();
}

export { Merger, MergerConfig, MergeResult };
