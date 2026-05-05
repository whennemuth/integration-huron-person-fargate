import { S3 } from '@aws-sdk/client-s3';
import * as readline from 'readline';
import { Readable } from 'stream';
import { FieldSet } from 'integration-core';

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
 * MergeEngine - Phase 3 of the Fargate Processing Pipeline
 * 
 * Consolidates individual chunk delta outputs into a single baseline file for the next sync cycle.
 * This completes the three-phase pipeline:
 * - Phase 1 (Chunker): Splits large input files into processable chunks
 * - Phase 2 (Processor): Processes each chunk independently, producing delta files
 * - Phase 3 (Merger): Concatenates chunk deltas and writes to shared baseline location
 * 
 * Workflow:
 * 1. Lists chunk files: `deltas/{key-path}/{timestamp}/chunk-*.ndjson`
 * 2. Streams and concatenates NDJSON content from each chunk
 * 3. Writes consolidated output to timestamped delta directory
 * 4. Calling code merges with existing baseline using HashMapMerger
 * 5. DeferredDeleteHandler detects removals and soft-deletes them
 * 6. Merged result written to shared baseline: `delta-storage/previous-input.ndjson`
 * 7. Cleanup deletes chunk files after successful merge
 * 
 * Critical: Cleanup happens AFTER merge to prevent S3 event notification cancellation. Files
 * deleted within 6-15 seconds of creation can cause S3 to cancel ObjectCreated events before
 * Lambda invokes, leading to data loss. Delaying cleanup ensures events process successfully.
 */
class MergeEngine {
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

export { MergeEngine, MergerConfig, MergeResult };
