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
 * - Writes concatenated output to: delta-storage/{key-path}/previous-input.ndjson (no timestamp)
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
 * - Single merged file: delta-storage/person-full/previous-input.ndjson
 * - Cleanup of all delta chunk files
 * 
 * Example Local Usage:
 * ```bash
 * CHUNKS_BUCKET=chunks-bucket INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json REGION=us-east-2 node dist/merger.js
 * ```
 */

import { S3 } from '@aws-sdk/client-s3';
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import * as readline from 'readline';
import { Readable } from 'stream';
import { extractChunkBasePath } from '../src/chunking/filedrop/ChunkPathUtils';

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
        console.log('No messages in queue - exiting');
        return null;
      }

      const message = messages[0];
      const body = JSON.parse(message.Body || '{}');

      // Delete message from queue (prevents reprocessing)
      if (message.ReceiptHandle) {
        await sqsClient.send(
          new DeleteMessageCommand({
            QueueUrl: SQS_QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
          })
        );
      }

      console.log('Task parameters from SQS:', JSON.stringify(body));
      return {
        chunksBucket: body.CHUNKS_BUCKET,
        chunkDirectory: body.CHUNK_DIRECTORY || null,
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
  clientId: string;
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

  constructor(config: MergerConfig) {
    this.config = config;
    this.s3 = new S3({ region: config.region || 'us-east-1' });
  }

  /**
   * List all chunk files in S3
   */
  private async listChunkFiles(): Promise<string[]> {
    const prefix = `${this.config.clientId}/chunks/`;
    console.log(`Listing chunk files with prefix: ${prefix}`);

    try {
      const response = await this.s3.listObjectsV2({
        Bucket: this.config.bucketName,
        Prefix: prefix
      });

      const chunkKeys = (response.Contents || [])
        .map(obj => obj.Key!)
        .filter(key => key.match(/chunk-\d+\.ndjson$/))
        .sort(); // Sort to ensure deterministic order

      console.log(`Found ${chunkKeys.length} chunk files`);
      return chunkKeys;

    } catch (error: any) {
      throw new Error(`Failed to list chunk files: ${error.message}`);
    }
  }

  /**
   * Stream a single chunk file and return line count
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
   * Write merged content to output file
   */
  private async writeMergedFile(lines: string[], outputKey: string): Promise<void> {
    console.log(`\nWriting merged file: s3://${this.config.bucketName}/${outputKey}`);
    console.log(`Total lines: ${lines.length}`);

    const content = lines.join('\n') + '\n';

    try {
      await this.s3.putObject({
        Bucket: this.config.bucketName,
        Key: outputKey,
        Body: content,
        ContentType: 'application/x-ndjson'
      });

      console.log('✓ Merged file written successfully');

    } catch (error: any) {
      throw new Error(`Failed to write merged file: ${error.message}`);
    }
  }

  /**
   * Delete all chunk files after successful merge
   */
  private async deleteChunkFiles(chunkKeys: string[]): Promise<void> {
    if (chunkKeys.length === 0) {
      return;
    }

    console.log(`\nDeleting ${chunkKeys.length} chunk files...`);

    try {
      // AWS S3 deleteObjects can handle up to 1000 objects at once
      const chunks = [];
      for (let i = 0; i < chunkKeys.length; i += 1000) {
        chunks.push(chunkKeys.slice(i, i + 1000));
      }

      for (const chunk of chunks) {
        await this.s3.deleteObjects({
          Bucket: this.config.bucketName,
          Delete: {
            Objects: chunk.map(key => ({ Key: key })),
            Quiet: true
          }
        });
      }

      console.log('✓ All chunk files deleted');

    } catch (error: any) {
      throw new Error(`Failed to delete chunk files: ${error.message}`);
    }
  }

  /**
   * Execute the merge operation
   */
  async merge(): Promise<MergeResult> {
    console.log('=== Phase 3: Merger ===\n');
    console.log(`Bucket: ${this.config.bucketName}`);
    console.log(`Client ID: ${this.config.clientId}`);
    console.log(`Region: ${this.config.region || 'default (us-east-1)'}\n`);

    // Step 1: List all chunk files
    const chunkKeys = await this.listChunkFiles();

    if (chunkKeys.length === 0) {
      console.warn('⚠ No chunk files found. Nothing to merge.');
      return {
        totalLines: 0,
        chunkCount: 0,
        outputKey: `${this.config.clientId}/previous-input.ndjson`,
        deletedChunks: []
      };
    }

    // Step 2: Read and concatenate all chunks
    console.log('\nReading chunk files:');
    const allLines: string[] = [];

    for (const chunkKey of chunkKeys) {
      const lines = await this.readChunkLines(chunkKey);
      allLines.push(...lines);
    }

    // Step 3: Write merged file
    const outputKey = `${this.config.clientId}/previous-input.ndjson`;
    await this.writeMergedFile(allLines, outputKey);

    // Step 4: Cleanup chunk files
    await this.deleteChunkFiles(chunkKeys);

    console.log('\n✓ Merge completed successfully');

    return {
      totalLines: allLines.length,
      chunkCount: chunkKeys.length,
      outputKey,
      deletedChunks: chunkKeys
    };
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
    DRY_RUN='false'
  } = process.env;
  const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';

  try {
    // Convert chunk directory to delta directory
    // Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "deltas/person-full/2026-03-03T19:58:41.277Z"
    const deltaDir = chunkDir.replace(/^chunks\//, 'deltas/');
    
    // Extract the key path without timestamp for the shared output location
    // Example: "deltas/person-full/2026-03-03T19:58:41.277Z" -> "person-full"
    const keyPathMatch = deltaDir.match(/^deltas\/(.+?)\/[^\/]+$/);
    const keyPath = keyPathMatch ? keyPathMatch[1] : deltaDir.replace(/^deltas\//, '').replace(/\/[^\/]+$/, '');
    const sharedOutputPath = `delta-storage/${keyPath}`;

    console.log(`\nSource delta directory: ${deltaDir}`);
    console.log(`Target output directory: ${sharedOutputPath}`);

    // Read delta chunks from timestamped directory
    const deltaMerger = new Merger({ bucketName, clientId: deltaDir, region });
    const result = await deltaMerger.merge();

    // Now copy the merged result to shared location (without timestamp)
    const sourceKey = result.outputKey;
    const targetKey = `${sharedOutputPath}/previous-input.ndjson`;
    
    console.log(`\nCopying merged output to shared location:`);
    console.log(`  From: s3://${bucketName}/${sourceKey}`);
    console.log(`  To:   s3://${bucketName}/${targetKey}`);

    const s3 = new S3({ region });
    
    // Read from delta directory
    const { Body } = await s3.getObject({ Bucket: bucketName, Key: sourceKey });
    const content = await Body?.transformToString();
    
    // Write to shared location, overwriting the prior older copy if it exists.
    await s3.putObject({
      Bucket: bucketName,
      Key: targetKey,
      Body: content,
      ContentType: 'application/x-ndjson'
    });

    console.log(`✓ Merged output copied to shared location`);

    console.log(`\nMerge Summary:`);
    console.log(`  Total lines: ${result.totalLines}`);
    console.log(`  Delta chunks merged: ${result.chunkCount}`);
    console.log(`  Timestamped output: s3://${bucketName}/${sourceKey}`);
    console.log(`  Shared output: s3://${bucketName}/${targetKey}`);
    console.log(`  Cleanup: ${result.deletedChunks.length} files deleted`);

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
