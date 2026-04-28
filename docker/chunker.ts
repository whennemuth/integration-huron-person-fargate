/**
 * Chunker Entry Point (Phase 1)
 * 
 * This module runs in a Fargate task caused by SQS messages created by ChunkerSubscriber Lambda.
 * It streams a large JSON file containing person records and breaks it up
 * into smaller NDJSON chunk files for parallel processing.
 * 
 * Two modes of operation:
 * 1. ECS Fargate (production): Reads INPUT_BUCKET and INPUT_KEY from SQS message
 * 2. Local development: Reads INPUT_BUCKET and INPUT_KEY from environment variables (fallback)
 * 
 * Environment Variables:
 * 
 * - (api datasource):
 *   - SECRET_ARN: Secrets Manager ARN containing config (fallback)
 *     and/or...
 *   - HURON_PERSON_CONFIG_PATH: Path to config.json (local dev only)
 *     and/or...
 *   - HURON_PERSON_CONFIG_JSON: Full config as JSON string from TaskDef secrets
 * 
 * - (s3 datasource):
 *   - INPUT_BUCKET: Source bucket containing the JSON file
 *   - INPUT_KEY: Key of the JSON file to process
 * 
 * - (both datasources):
 *   - SYNC_TYPE: 'person-full' or 'person-delta'
 *   - SQS_QUEUE_URL: SQS queue URL to read task parameters from (if running in ECS context)
 *   - CHUNKS_BUCKET: Destination bucket for chunk files
 *   - REGION: AWS region (e.g., 'us-east-2')
 *   - ITEMS_PER_CHUNK: Number of persons per chunk (default: 200)
 *   - PERSON_ID_FIELD: Field name for person IDs (default: 'personid')
 *   - BULK_RESET: Used to tag the chunk files (will be referenced by processor.ts when processing the chunk files)
 *   - DRY_RUN: If 'true', performs a dry run without writing chunks (default: 'false')
 * 
 * Output:
 * - Creates NDJSON chunk files in S3 at: {inputBucket}/chunks/chunk-0000.ndjson, chunk-0001.ndjson, etc.
 *   (Note: inputBucket name becomes the top-level folder prefix in chunks bucket)
 * - Each chunk triggers an S3 event notification that feeds into the processor SQS queue
 * 
 * Example Local Usage:
 * ```bash
 * INPUT_BUCKET=input-bucket INPUT_KEY=data.json CHUNKS_BUCKET=chunks-bucket BULK_RESET=false node dist/docker/chunker.js
 * ```
 */

import { DeleteMessageCommand, DeleteMessageCommandInput, DeleteMessageCommandOutput, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Config, ConfigManager } from 'integration-huron-person';
import { ChunkFromAPI } from '../src/chunking/fetch/ChunkFromAPI';
import { ChunkFromS3 } from '../src/chunking/filedrop/ChunkFromS3';
import { S3StorageAdapter } from '../src/storage/S3StorageAdapter';
import { getLocalConfig } from '../src/Utils';

export type IChunkFromSource = {
  runChunking: (params: ChunkFromParams) => Promise<void>
}

export type ChunkFromParams = {
  chunksBucket: string,
  region: string | undefined,
  itemsPerChunk: number,
  personIdField: string,
  dryRun: string
}

const isEcsTask = () => process.env.IS_ECS_TASK === 'true';

/**
 * Reads task parameters from SQS queue or environment variables.
 * Priority: SQS message > Environment variables
 */
export const grabMessageBodyFromQueue = async (): Promise<any> => {
  const { SQS_QUEUE_URL, REGION } = process.env;

  if (SQS_QUEUE_URL) {
    console.log(`SQS_QUEUE_URL detected: ${SQS_QUEUE_URL} - reading task parameters from SQS queue`);
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
        if (isEcsTask()) {
          console.log('No messages in queue - this probably means that the desired count for the ' +
            'service has not scaled down yet to zero after processing the last message and deleting ' +
            'it from  the queue. An empty queue will eventually cause the service to scale down to ' +
            'zero, but in the meantime we should just exit the task.');
          console.log('✗ Task cancelled')
          return null;
        }
        console.log('No messages in queue');
        return null;
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
      return undefined;
    }
  }
}

/**
 * Write metadata file for merger trigger detection
 */
export async function writeMetadata(
  chunksStorage: S3StorageAdapter,
  chunksBucket: string,
  chunkBasePath: string,
  result: { chunkCount: number; totalRecords: number; chunkKeys: string[] },
  itemsPerChunk: number,
  source: string,
  target: string | undefined,
  dryRun: boolean,
  bulkReset: boolean
) {
  // Write metadata file for merger trigger detection
  // Path uses the chunk base path: s3://chunks-bucket/chunks/person-full/2026-03-03T19:58:41.277Z/_metadata.json
  const metadataKey = `${chunkBasePath}/_metadata.json`;
  
  // Derive delta storage path from chunk base path
  // Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "deltas/person-full/2026-03-03T19:58:41.277Z"
  const deltaStoragePath = chunkBasePath.replace(/^chunks\//, 'deltas/');
  
  const metadata: any = {
    chunkCount: result.chunkCount,
    totalRecords: result.totalRecords,
    itemsPerChunk,
    source,
    chunkDirectory: chunkBasePath,
    deltaStoragePath,
    bulkReset,
    createdAt: new Date().toISOString(),
    chunkKeys: result.chunkKeys
  };

  // Add target if provided
  if (target) {
    metadata.target = target;
  }

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

/**
 * Get configuration from environment variables, Secrets Manager, or local file system 
 * (for local dev)
 * Priority: 
 *   HURON_PERSON_CONFIG_JSON (TaskDef secret injection) > 
 *   SECRET_ARN (Secrets Manager) > 
 *   Environment > 
 *   FileSystem (local dev)
 * @returns 
 */
export const getConfig = async (): Promise<Config> => {
  const { 
    /** SECRET_ARN: Secrets Manager ARN containing config */
    SECRET_ARN,
    /** HURON_PERSON_CONFIG_PATH: Path to config.json (fallback for local dev only) */
    HURON_PERSON_CONFIG_PATH
  } = process.env;

  // Load configuration.
  const configManager = ConfigManager.getInstance();
  const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
  return await configManager
    .reset()
    .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
    .fromSecretManager(SECRET_ARN)                // ← Fallback to Secrets Manager
    .fromEnvironment()                            // ← Fallback to individual env var overrides
    .fromFileSystem(localConfigPath)              // ← Local dev only
    .getConfigAsync('people');
}

const getChunkerInstance = async (config: Config): Promise<IChunkFromSource | undefined> => {
  
  if (isEcsTask()) {
    // Fargate execution: Message takes priority over config/environment
    console.log('Running in ECS task - checking SQS message first...');
    const msgBody = await grabMessageBodyFromQueue();
    
    // If queue is empty, exit immediately (grabMessageBodyFromQueue already logged)
    if (!msgBody) {
      console.log('No messages in queue - exiting');
      process.exit(0);
    }
    
    // Try S3 chunker with message parameters
    const s3Chunker = new ChunkFromS3();
    s3Chunker.setTaskParametersFromQueueMessageBody(msgBody);
    if(s3Chunker.hasSufficientTaskInfo()) {
      console.log('Data source type: S3 (person data will be streamed from S3 bucket)');
      return s3Chunker;
    }
    
    // Try API chunker with message parameters (config provides fallback values)
    const apiChunker = new ChunkFromAPI(config);
    apiChunker.setTaskParametersFromQueueMessageBody(msgBody);
    if(apiChunker.hasSufficientConfig()) {
      console.log('Data source type: API (person data will be fetched from API endpoint)');
      return apiChunker;
    }
  } else {
    // Local execution: Config/environment takes priority
    console.log('Running locally - using config/environment variables...');
    
    // Check S3 from environment first
    const s3Chunker = new ChunkFromS3();
    if(s3Chunker.hasSufficientTaskInfo()) {
      console.log('Data source type: S3 (person data will be streamed from S3 bucket)');
      return s3Chunker;
    }
    
    // Check API from config
    const apiChunker = new ChunkFromAPI(config);
    if(apiChunker.hasSufficientConfig()) {
      console.log('Data source type: API (person data will be fetched from API endpoint)');
      return apiChunker;
    }
    
    // Fallback: try reading message if available
    const msgBody = await grabMessageBodyFromQueue();
    
    s3Chunker.setTaskParametersFromQueueMessageBody(msgBody);
    if(s3Chunker.hasSufficientTaskInfo(true)) {
      console.log('Data source type: S3 (person data will be streamed from S3 bucket)');
      return s3Chunker;
    }
    
    apiChunker.setTaskParametersFromQueueMessageBody(msgBody);
    if(apiChunker.hasSufficientTaskInfo(true)) {
      console.log('Data source type: API (person data will be fetched from API endpoint)');
      return apiChunker;
    }
  }
  
  return undefined;
}


async function main() {
  console.log('=== Phase 1: Chunker ===\n');

  // Read additional configuration from environment
  const {
    CHUNKS_BUCKET: chunksBucket,
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

  // Instantiate general chunker params object to pass to either chunking class.
  const chunkFromParams: ChunkFromParams = { 
    chunksBucket, region, itemsPerChunk, personIdField, dryRun
  };

  // Run chunking operation based on source type (API or S3)
  (async () => {
    const config = await getConfig();
    const chunker = await getChunkerInstance(config);
    if(!chunker) {
      console.error('ERROR: Insufficient task parameters. Must provide either API config or S3 input parameters via SQS message or environment variables.');
      process.exit(1);
    }
    await chunker.runChunking(chunkFromParams);
  })();
}

// Run if executed directly
if (require.main === module) {
  main();
}
