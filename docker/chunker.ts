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

import { Message } from '@aws-sdk/client-sqs';
import { TestEnvironment, Timer } from 'integration-core';
import { Config, ConfigManager } from 'integration-huron-person';
import { ChunkerQueue } from '../src/chunking/ChunkerQueue';
import { ChunkFromAPI } from '../src/chunking/fetch/ChunkFromAPI';
import { ChunkFromS3 } from '../src/chunking/filedrop/ChunkFromS3';
import { MetadataManager, ReadMetadataParams, WriteMetadataParams } from '../src/chunking/Metadata';
import { HuronPersonCache } from '../src/PersonCache';
import { TaskProtection } from '../src/TaskProtection';
import { getLocalConfig, objectExistsInS3 } from '../src/Utils';
import { SyncPopulation } from './chunkTypes';

export type IChunkFromSource = {
  runChunking: (params: ChunkFromParams) => Promise<void>
  noMessagesFromQueue?: boolean
  getMessage: () => Message | undefined
  getChunkDirectory: () => string
  getBulkResetFlag?: () => boolean  // Optional getter for bulkReset flag from task parameters
  getTrustPreviousStorageFlag?: () => boolean  // Optional getter for trustPreviousStorage flag from task parameters
  getSyncPopulation?: () => SyncPopulation  // Optional getter for syncPopulation from task parameters
}

export type ChunkFromParams = {
  chunksBucket: string,
  region: string | undefined,
  itemsPerChunk: number,
  personIdField: string,
  bulkReset?: boolean, // To override the bulkReset flag set in the TaskParameters of the chunker instance.
  trustPreviousStorage?: boolean, // Controls whether previous delta storage is trusted for create-vs-patch decisions.
  dryRun: string
}

const isEcsTask = () => process.env.IS_ECS_TASK === 'true';

/**
 * Write metadata file for merger trigger detection
 */
export async function writeChunkMetadata(params: WriteMetadataParams) {
  await MetadataManager.write(params);
  console.log('\n✓ Metadata file written to S3');
}

/**
 * Bail out if this is an extraneous task where the end of chunking was reached after its SQS message 
 * was created. Presence of the metadata file indicates that the chunking process had already 
 * completed and the service had already "realized" it had reached the end and scaled down, but due 
 * to the asynchronous nature of SQS and scaling, we may have some tasks that were triggered by 
 * messages that were created before the service realized it had reached the end, and these tasks 
 * should just exit immediately without doing any work.
 * @param params 
 * @returns true if chunking has already finished (metadata file exists), false otherwise
 */
export async function chunkingAlreadyFinished(params: { 
  bucketName: string, chunkDirectory: string, region: string | undefined 
}): Promise<boolean> {
  const { bucketName, chunkDirectory, region } = params;
  const metadata = await MetadataManager.read({ 
    bucketName, chunkDirectory, region 
  } satisfies ReadMetadataParams);

  let retval = true; // Assume finished unless we can confirm otherwise by finding metadata
  if(!metadata) {
    retval = false;
  }

  if (Object.keys(metadata).length === 0) {
    retval = false;
  }
  
  if(retval) {
    console.log(`🔍 Existing metadata found for this chunk directory: ${JSON.stringify(metadata)}`);
  }
  return retval;
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

const getChunkerInstance = async (config: Config, chunkerQueue: ChunkerQueue): Promise<IChunkFromSource | undefined> => {
  
  if (isEcsTask()) {
    // Fargate execution: Message takes priority over config/environment
    console.log('Running in ECS task - checking SQS message first...');

    // Read message, but do not delete it - it must remain as "in-flight" so it 
    // (combined with others) can bear upon the desiredCount of the service.
    const message: Message | undefined = await chunkerQueue.receiveMessageFromQueue();
    const msgBody = chunkerQueue.getMessageBody();
    console.log('Task parameters from SQS:', JSON.stringify(msgBody));
    
    // If queue is empty, exit immediately (popMessageBodyFromQueue already logged)
    if (!msgBody) {     
      return new class implements IChunkFromSource {
        runChunking = async (params: ChunkFromParams) => { return; }
        noMessagesFromQueue = true
        getMessage = () => undefined
        getChunkDirectory = () => ''
        getSyncPopulation = () => SyncPopulation.PersonFull
      }();
    }
    
    // Try S3 chunker with message parameters
    const s3Chunker = new ChunkFromS3();
    s3Chunker.setTaskParametersFromQueueMessage(message);
    if(s3Chunker.hasSufficientTaskInfo()) {
      console.log('Data source type: S3 (person data will be streamed from S3 bucket)');
      return s3Chunker;
    }
    
    // Try API chunker with message parameters (config provides fallback values)
    const apiChunker = new ChunkFromAPI(config);
    apiChunker.setTaskParametersFromQueueMessage(message);
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
    
    // Fallback: try reading message if available. It is ok to delete immediately in local 
    // context since we are not relying on the message for scaling decisions like we are in ECS 
    // context, and this allows us to test the message-based parameter passing in local dev as well.
    const message: Message | undefined = await chunkerQueue.popMessageFromQueue();
    
    s3Chunker.setTaskParametersFromQueueMessage(message);
    if(s3Chunker.hasSufficientTaskInfo(true)) {
      console.log('Data source type: S3 (person data will be streamed from S3 bucket)');
      return s3Chunker;
    }
    
    apiChunker.setTaskParametersFromQueueMessage(message);
    if(apiChunker.hasSufficientTaskInfo(true)) {
      console.log('Data source type: API (person data will be fetched from API endpoint)');
      return apiChunker;
    }
  }
  
  return undefined;
}

/**
 * Determine if the shared delta storage file exists in S3, which indicates that a previous sync 
 * operation was run and we have a baseline to compare against for delta processing. Without this
 * baseline, there is no other way to check if any given person exists in the target system, except
 * by looking them up first, which requires the bulkReset flag be set to true (env var: BULK_RESET=true).
 * @param bucket 
 * @param region 
 */
const sharedDeltaStorageFileExists = async (bucket: string, region?: string): Promise<boolean> => {
  const { SHARED_DELTA_STORAGE_DIR='delta-storage' } = process.env;
  const deltaStorageKey = `${SHARED_DELTA_STORAGE_DIR}/previous-input.ndjson`;
  const retval = await objectExistsInS3(bucket, deltaStorageKey, region);
  if(retval) {
    console.log(`✓ Found existing delta storage file at s3://${bucket}/${deltaStorageKey}`);
  } else {
    console.warn(`✗ No existing delta storage file found at s3://${bucket}/${deltaStorageKey} - ` +
      `setting/overriding bulkReset=true to force target system lookups to determine create vs ` +
      `update for each person record (this may cause the sync to run slower than usual, or this ` +
      `may be the first time a sync has been run and you forgot to set the BULK_RESET ` +
      `environment variable to true)`);
  }
  return retval;
}

export async function main() {
  console.log('=== Phase 1: Chunker ===\n');

  let chunker: IChunkFromSource | undefined;
  let chunkerQueue: ChunkerQueue = new ChunkerQueue({
    QueueUrl: process.env.SQS_QUEUE_URL,
    region: process.env.REGION,
    isEcsTask: isEcsTask()
  });
  
  let exitCode = 0;
  const timer = new Timer();
  timer.start();

  try {
    // Enable task protection for 4 hours (protects from sigkills by ECS during scale-in)
    await new TaskProtection(60 * 4).enable();

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
      exitCode = 1;
      return;
    }

    // Validate items per chunk is a positive integer
    const itemsPerChunk = parseInt(itemsPerChunkStr, 10);
    if (isNaN(itemsPerChunk) || itemsPerChunk <= 0) {
      console.error(`ERROR: Invalid ITEMS_PER_CHUNK: ${itemsPerChunkStr}`);
      exitCode = 1;
      return;
    }

    // Instantiate general chunker params object to pass to either chunking class.
    const chunkFromParams: ChunkFromParams = { 
      chunksBucket, region, itemsPerChunk, personIdField, dryRun
    };

    // Get chunking operation instance based on source type (API or S3)
    const config = await getConfig();
    chunker = await getChunkerInstance(config, chunkerQueue);

    // Bail out if there is some kind of unexpected lapse in configuration or message parameters that leaves us without a clear source of person data to chunk from.
    if(!chunker) {
      console.error('ERROR: Insufficient task parameters. Must provide either API config or S3 input parameters via SQS message or environment variables.');
      exitCode = 1;
      return;
    }

    // Bail out if there are no messages in the queue (this likely means the service is still scaling down after processing the last message and deleting it from the queue, and we should just exit the task)
    if(chunker.noMessagesFromQueue) {
      console.log('⊘ No messages in queue - exiting');
      exitCode = 0;
      return;
    }

    // Bail out if this is an extraneous task where the chunking has already ended.
    const alreadyFinished = await chunkingAlreadyFinished({
      bucketName: chunksBucket, chunkDirectory: chunker?.getChunkDirectory(), region
    });
    if (alreadyFinished) {
      console.log(`⊘ Cancelling. This means this task was based on a SQS message that was created before the service "realized" it had reached the end.`);
      exitCode = 0;
      return;
    };

    if(chunker instanceof ChunkFromAPI) {
      // Send next chunking message BEFORE starting this task's processing
      // This enables true parallelism: the next task can start before the current one finishes
      await chunker.sendNextChunkingMessage(chunkerQueue, dryRun.toLowerCase() === 'true');
    }

    // Check if shared delta storage file exists in S3 to determine if we have a baseline for doing 
    // lookups during chunk processing, or if we need to set the bulkReset flag to true to force lookups 
    // for every record.
    // Priority: SQS message bulkReset > No historical data check
    const hasHistoricalData = await sharedDeltaStorageFileExists(chunksBucket, region);
    const bulkResetFromMessage = chunker.getBulkResetFlag?.() || false;
    
    if (bulkResetFromMessage) {
      console.log('✓ bulkReset=true from SQS message - will create person cache for lookups (if not already created).');
      chunkFromParams.bulkReset = true;
    } else if (!hasHistoricalData) {
      console.log('✓ No historical data found - setting bulkReset=true to force target system lookups');
      chunkFromParams.bulkReset = true;
    } else {
      console.log('✓ Historical data exists and bulkReset not requested - using delta comparison');
      chunkFromParams.bulkReset = false;
    }

    chunkFromParams.trustPreviousStorage = chunker.getTrustPreviousStorageFlag?.() || false;
    console.log(`Trust previous storage flag: ${chunkFromParams.trustPreviousStorage}`);

    // Get syncPopulation from chunker
    const syncPopulation = chunker.getSyncPopulation?.() || SyncPopulation.PersonFull;
    console.log(`Sync population type: ${syncPopulation}`);

    // Write flags file BEFORE chunking starts so processor tasks can read it immediately
    await MetadataManager.writeFlags({
      bucketName: chunksBucket,
      chunkDirectory: chunker.getChunkDirectory(),
      bulkReset: chunkFromParams.bulkReset,
      trustPreviousStorage: chunkFromParams.trustPreviousStorage,
      syncPopulation,
      dryRun: dryRun === 'true',
      region
    });

    /**
     * Writes the full population from the target API to an S3 file as a cache for lookup during chunk processing.
     */
    if(chunkFromParams.bulkReset || !chunkFromParams.trustPreviousStorage) {
      const config = await getConfig();
      const { CACHE_FILE_NAME } = HuronPersonCache;
      await new HuronPersonCache({ config }).setS3PopulationCache({ 
        bucketName: chunksBucket, 
        key: chunker.getChunkDirectory() + `/${CACHE_FILE_NAME}`, 
        region: region! 
      });
    }

    await chunker.runChunking(chunkFromParams);
  }
  catch (error) {
    console.error('Error in chunking process:', error);
    exitCode = 1;
  }
  finally {
    if(chunker) {
      if(!chunker.noMessagesFromQueue) {
        const message = chunker.getMessage();
        await chunkerQueue.deleteMessageFromQueue(message!);
      }
    }
    timer.stop();
    if(exitCode === 0) {
      timer.logElapsed('\n✓ Chunker process completed successfully');
    } else {
      timer.logElapsed('\n✗ Chunker process failed');
    }
    await new TaskProtection().disable();
    process.exit(exitCode);
  }
}

// Run if executed directly
if (require.main === module) {
  const testEnvironment = TestEnvironment('DOCKER_CHUNKER');

  [
    'CHUNKS_BUCKET',
    'REGION',
    'STACK_ID',
    'ITEMS_PER_CHUNK',
    'PERSON_ID_FIELD',
    'DRY_RUN',
    'SQS_QUEUE_URL',
    'SHARED_DELTA_STORAGE_DIR',
    'IS_ECS_TASK',
    'ECS_AGENT_URI',
    'HURON_PERSON_CONFIG_PATH',
    'SECRET_ARN',
    'HURON_PERSON_CONFIG_JSON',
    'POPULATION_SCOPE',
    'POPULATION_TYPE',
    'CHUNK_DIRECTORY',
    'SINGLE_PERSON_BUID',
    'INPUT_BUCKET',
    'INPUT_KEY',
    'BULK_RESET',
    'TRUST_PREVIOUS_STORAGE',
    'MAX_SCALING_CAPACITY',
    'ECS_CLUSTER_NAME',
    'CACHE_ENABLED',
    'CACHE_PATH',
  ].forEach(testEnvironment.getVar);

  main();
}
