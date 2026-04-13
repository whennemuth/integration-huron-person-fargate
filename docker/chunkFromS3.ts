import { ConfigManager } from "integration-huron-person";
import { PersonArrayWrapper } from "../src/PersonArrayWrapper";
import { BigJsonFile, BigJsonFileConfig } from "../src/chunking/filedrop/BigJsonFile";
import { extractChunkBasePath } from '../src/chunking/filedrop/ChunkPathUtils';
import { S3StorageAdapter } from "../src/storage/S3StorageAdapter";
import { ChunkFromParams, getTaskParameters, IChunkFromSource, writeMetadata } from "./chunker";
import { getLocalConfig } from "../src/Utils";

/**
 * Chunker Entry Point (Phase 1)
 * 
 * This module runs in a Fargate task caused by SQS messages created by ChunkerSubscriber Lambda.
 * It streams a large JSON file containing person records *** FROM AN S3 BUCKET *** and breaks 
 * it up into smaller NDJSON chunk files for parallel processing.
 * 
 * Two modes of operation:
 * 1. ECS Fargate (production): Reads INPUT_BUCKET and INPUT_KEY from SQS message
 * 2. Local development: Reads INPUT_BUCKET and INPUT_KEY from environment variables (fallback)
 * 
 * Environment Variables:
 *   - SYNC_TYPE: 'person-full' or 'person-delta'
 *   - INPUT_BUCKET: Source bucket containing the JSON file
 *   - INPUT_KEY: Key of the JSON file to process
 *   - SQS_QUEUE_URL: SQS queue URL to read task parameters from (if running in ECS context)
 *   - CHUNKS_BUCKET: Destination bucket for chunk files
 *   - REGION: AWS region (e.g., 'us-east-2')
 *   - ITEMS_PER_CHUNK: Number of persons per chunk (default: 200)
 *   - PERSON_ID_FIELD: Field name for person IDs (default: 'personid')
 *   - DRY_RUN: If 'true', performs a dry run without writing chunks (default: 'false')
 * 
 * Output:
 * - Creates NDJSON chunk files in S3 at: {inputBucket}/chunks/chunk-0000.ndjson, chunk-0001.ndjson, etc.
 *   (Note: inputBucket name becomes the top-level folder prefix in chunks bucket)
 * - Each chunk triggers an S3 event notification that feeds into the processor SQS queue
 * 
 * Example Local Usage:
 * ```bash
 * INPUT_BUCKET=input-bucket INPUT_KEY=data.json CHUNKS_BUCKET=chunks-bucket node dist/docker/chunker.js
 * ```
 */
export class ChunkFromS3 implements IChunkFromSource {
  constructor() { }
  
  /**
   * Run chunking operation using BigJsonFile (S3 source)
   */
  public runChunking = async (params: ChunkFromParams) => {
    const { chunksBucket, region, itemsPerChunk, personIdField, dryRun } = params;

    // Get task parameters from SQS or environment
    const taskParams = await getTaskParameters();

    if (!taskParams) {
      console.error('ERROR: No task parameters available for S3 mode (checked SQS queue and environment variables)');
      process.exit(1);
    }

    const { inputBucket, inputKey } = taskParams;

    // Extract chunk base path from input key
    // This uses the key path + ISO timestamp (if present) or filename without extension
    const chunkBasePath = extractChunkBasePath(inputKey);
    
    console.log(`Input: s3://${inputBucket}/${inputKey}`);
    console.log(`Chunks: s3://${chunksBucket}/${chunkBasePath}/`);
    console.log(`Region: ${region || 'default'}`);
    console.log(`Items per chunk: ${itemsPerChunk}`);
    console.log(`Person ID field: ${personIdField}\n`);

    try {
      // Create storage adapters for input and output
      const inputStorage = new S3StorageAdapter({ bucketName: inputBucket, region });
      const chunksStorage = new S3StorageAdapter({ bucketName: chunksBucket, region });
      const personArrayWrapper = new PersonArrayWrapper(inputStorage, personIdField);

      // Configure chunker to read from input bucket and write to chunks bucket
      // The clientId is derived from the input key path and timestamp/filename
      // This creates a path structure like: s3://chunks-bucket/chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson
      const config: BigJsonFileConfig = {
        itemsPerChunk,
        storage: inputStorage, // Read from input bucket
        outputStorage: chunksStorage, // Write chunks to chunks bucket
        personIdField,
        clientId: chunkBasePath, // Derived from input key with timestamp or filename
        personArrayWrapper,
        dryRun: dryRun.toLowerCase() === 'true'
      };

      // Run chunking operation
      const chunker = new BigJsonFile(config);
      const result = await chunker.breakup(inputKey);

      // Write metadata and log results
      await writeMetadata(
        chunksStorage,
        chunksBucket,
        chunkBasePath,
        result,
        itemsPerChunk,
        `s3://${inputBucket}/${inputKey}`,
        config.dryRun || false
      );

      // Exit with success
      process.exit(0);

    } catch (error: any) {
      console.error('\n✗ S3 chunking failed:', error.message);
      console.error(error.stack);
      process.exit(1);
    }
  }
}


if (require.main === module) {
  /**
   * Read additional configuration from environment. 
   * We don't need any secrets since all activity is against s3, and security is handled on the 
   * basis of the task role's permissions to the relevant bucket(s).
   */
  const {
    CHUNKS_BUCKET: chunksBucket = '',
    REGION: region,
    ITEMS_PER_CHUNK: itemsPerChunkStr = '200',
    PERSON_ID_FIELD: personIdField = 'personid',
    DRY_RUN: dryRun = 'false'
  } = process.env;

  // Validate bucket name required for output is provided.
  if (!chunksBucket) {
    console.error('ERROR: CHUNKS_BUCKET environment variable is required');
    process.exit(1);
  }

  // Validate items per chunk is a positive integer
  const itemsPerChunk = parseInt(itemsPerChunkStr, 10);
  if (isNaN(itemsPerChunk) || itemsPerChunk <= 0) {
    console.error(`ERROR: Invalid ITEMS_PER_CHUNK: ${itemsPerChunkStr}`);
    process.exit(1);
  }

  // Run API fetch and chunking operation.
  (async () => {
    await new ChunkFromS3().runChunking({
      chunksBucket, region, itemsPerChunk, personIdField, dryRun
    });
  })();
}