/**
 * Chunker Entry Point (Phase 1)
 * 
 * This module runs in a Fargate task caused by SQS messages created by ChunkerSubscriber Lambda.
 * It streams a large JSON file containing person records and breaks it up
 * into smaller NDJSON chunk files for parallel processing.
 * 
 * Two modes of operation:
 * 1. ECS Fargate (production): Reads INPUT_BUCKET and INPUT_KEY from SQS message
 * 2. Local development: Reads from environment variables (fallback)
 * 
 * Environment Variables:
 * - SOURCE_FETCH_TYPE: 's3' or 'api' (determines data source)
 * - SQS_QUEUE_URL: SQS queue URL to read task parameters from (ECS mode)
 * - INPUT_BUCKET: (S3 mode) Source bucket containing the JSON file
 * - INPUT_KEY: (S3 mode) Key of the JSON file to process
 * - HURON_PERSON_CONFIG_JSON: (API mode) Full config as JSON string from TaskDef secrets
 * - SECRET_ARN: (API mode) Secrets Manager ARN containing config (fallback)
 * - HURON_PERSON_CONFIG_PATH: (API mode) Path to config.json (local dev only)
 * - SYNC_TYPE: (API mode) 'person-full' or 'person-delta'
 * - CHUNKS_BUCKET: Destination bucket for chunk files
 * - REGION: AWS region (e.g., 'us-east-2')
 * - ITEMS_PER_CHUNK: Number of persons per chunk (default: 200)
 * - PERSON_ID_FIELD: Field name for person IDs (default: 'personid')
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

import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { ConfigManager, AxiosResponseStreamFilter } from 'integration-huron-person';
import { BigJsonFile, BigJsonFileConfig } from '../src/chunking/filedrop/BigJsonFile';
import { BigJsonFetch, BigJsonFetchConfig } from '../src/chunking/fetch/BigJsonFetch';
import { PersonArrayWrapper } from '../src/PersonArrayWrapper';
import { S3StorageAdapter } from '../src/storage/S3StorageAdapter';
import { extractChunkBasePath } from '../src/chunking/filedrop/ChunkPathUtils';

interface TaskParameters {
  inputBucket: string;
  inputKey: string;
}

/**
 * Reads task parameters from SQS queue or environment variables.
 * Priority: SQS message > Environment variables
 */
async function getTaskParameters(): Promise<TaskParameters | null> {
  const { SQS_QUEUE_URL, INPUT_BUCKET, INPUT_KEY, REGION } = process.env;

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
        inputBucket: body.INPUT_BUCKET,
        inputKey: body.INPUT_KEY,
      };
    } catch (error) {
      console.error('Error reading from SQS queue:', error);
      return null;
    }
  }

  // Mode 2: Local development - read from environment variables
  if (INPUT_BUCKET && INPUT_KEY) {
    console.log('Running in local context - reading task parameters from environment variables');
    return {
      inputBucket: INPUT_BUCKET,
      inputKey: INPUT_KEY,
    };
  }

  return null;
}

async function main() {
  console.log('=== Phase 1: Chunker ===\n');

  // Read additional configuration from environment
  const {
    SOURCE_FETCH_TYPE: sourceFetchType = 's3',
    CHUNKS_BUCKET: chunksBucket,
    REGION: region,
    ITEMS_PER_CHUNK: itemsPerChunkStr = '200',
    PERSON_ID_FIELD: personIdField = 'personid',
    DRY_RUN: dryRun = 'false'
  } = process.env;

  if (!chunksBucket) {
    console.error('ERROR: CHUNKS_BUCKET environment variable is required');
    process.exit(1);
  }

  const itemsPerChunk = parseInt(itemsPerChunkStr, 10);
  if (isNaN(itemsPerChunk) || itemsPerChunk <= 0) {
    console.error(`ERROR: Invalid ITEMS_PER_CHUNK: ${itemsPerChunkStr}`);
    process.exit(1);
  }

  console.log(`Source fetch type: ${sourceFetchType}\n`);

  if (sourceFetchType === 'api') {
    // API mode - fetch from API endpoint
    await runApiFetchChunking(chunksBucket, region, itemsPerChunk, personIdField, dryRun);
  } else if (sourceFetchType === 's3') {
    // S3 mode - read from S3 bucket
    await runStreamFromS3Chunking(chunksBucket, region, itemsPerChunk, personIdField, dryRun);
  } else {
    console.error(`ERROR: Invalid SOURCE_FETCH_TYPE: ${sourceFetchType} (must be 's3' or 'api')`);
    process.exit(1);
  }
}

/**
 * Run chunking operation using BigJsonFile (S3 source)
 */
async function runStreamFromS3Chunking(
  chunksBucket: string,
  region: string | undefined,
  itemsPerChunk: number,
  personIdField: string,
  dryRun: string
) {
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

/**
 * Run fetch and chunk operation using BigJsonFetch (API source)
 * Uses same config loading pattern as processor.ts
 */
async function runApiFetchChunking(
  chunksBucket: string,
  region: string | undefined,
  itemsPerChunk: number,
  personIdField: string,
  dryRun: string
) {
  const {
    HURON_PERSON_CONFIG_PATH,
    SECRET_ARN,
    SYNC_TYPE: syncType = 'person-full'
  } = process.env;

  if (syncType !== 'person-full' && syncType !== 'person-delta') {
    console.error(`ERROR: Invalid SYNC_TYPE: ${syncType} (must be 'person-full' or 'person-delta')`);
    process.exit(1);
  }

  // Create synthetic input key with timestamp (mimics S3 file structure)
  const timestamp = new Date().toISOString();
  const syntheticInputKey = `${syncType}/${timestamp}.json`;
  
  // Extract chunk base path (creates: chunks/person-full/2026-04-09T15:28:18.703Z)
  const chunkBasePath = extractChunkBasePath(syntheticInputKey);
  
  console.log(`Sync type: ${syncType}`);
  console.log(`Chunks: s3://${chunksBucket}/${chunkBasePath}/`);
  console.log(`Region: ${region || 'default'}`);
  console.log(`Items per chunk: ${itemsPerChunk}`);
  console.log(`Person ID field: ${personIdField}\n`);

  try {
    // Load configuration using same pattern as processor.ts
    // Priority: HURON_PERSON_CONFIG_JSON (TaskDef) > SECRET_ARN (Secrets Manager) > Environment > FileSystem (local dev)
    const configManager = ConfigManager.getInstance();
    const cfg = await configManager
      .reset()
      .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
      .fromSecretManager(SECRET_ARN)                // ← Fallback to Secrets Manager
      .fromEnvironment()                            // ← Individual env var overrides
      .fromFileSystem(HURON_PERSON_CONFIG_PATH)     // ← Local dev only
      .getConfigAsync('people');

    // Create storage adapter for output chunks
    const chunksStorage = new S3StorageAdapter({ bucketName: chunksBucket, region });
    const personArrayWrapper = new PersonArrayWrapper(chunksStorage, personIdField);
    const responseFilter = new AxiosResponseStreamFilter({ fieldsOfInterest: [personIdField] });

    // Configure fetcher
    const config: BigJsonFetchConfig = {
      itemsPerChunk,
      config: cfg,
      responseFilter,
      outputStorage: chunksStorage,
      clientId: chunkBasePath, // Derived from synthetic input key with timestamp
      personIdField,
      personArrayWrapper,
      dryRun: dryRun.toLowerCase() === 'true'
    };

    // Run fetch and chunk operation
    const fetcher = new BigJsonFetch(config);
    const result = await fetcher.fetchAndChunk();

    // Write metadata and log results
    await writeMetadata(
      chunksStorage,
      chunksBucket,
      chunkBasePath,
      result,
      itemsPerChunk,
      `api://config`,
      config.dryRun || false
    );

    // Exit with success
    process.exit(0);

  } catch (error: any) {
    console.error('\n✗ API fetch and chunk failed:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

/**
 * Write metadata file for merger trigger detection
 */
async function writeMetadata(
  chunksStorage: S3StorageAdapter,
  chunksBucket: string,
  chunkBasePath: string,
  result: { chunkCount: number; totalRecords: number; chunkKeys: string[] },
  itemsPerChunk: number,
  sourceDescription: string,
  dryRun: boolean
) {
  // Write metadata file for merger trigger detection
  // Path uses the chunk base path: s3://chunks-bucket/chunks/person-full/2026-03-03T19:58:41.277Z/_metadata.json
  const metadataKey = `${chunkBasePath}/_metadata.json`;
  
  // Derive delta storage path from chunk base path
  // Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "deltas/person-full/2026-03-03T19:58:41.277Z"
  const deltaStoragePath = chunkBasePath.replace(/^chunks\//, 'deltas/');
  
  const metadata = {
    chunkCount: result.chunkCount,
    totalRecords: result.totalRecords,
    itemsPerChunk,
    source: sourceDescription,
    chunkDirectory: chunkBasePath,
    deltaStoragePath,
    createdAt: new Date().toISOString(),
    chunkKeys: result.chunkKeys
  };

  if (!dryRun) {
    await chunksStorage.writeFile(metadataKey, JSON.stringify(metadata, null, 2), 'application/json');
    console.log(`\n✓ Metadata written: s3://${chunksBucket}/${metadataKey}`);
  } else {
    console.log(`[DRY RUN] Would write metadata to: s3://${chunksBucket}/${metadataKey}`);
  }

  // Log results
  console.log('\n✓ Chunking completed successfully');
  console.log(`Created ${result.chunkCount} chunks with ${result.totalRecords} person records`);
  console.log(`\nChunk files:`);
  result.chunkKeys.forEach(key => console.log(`  - s3://${chunksBucket}/${key}`));
}

// Run if executed directly
if (require.main === module) {
  main();
}
