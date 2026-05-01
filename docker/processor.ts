/**
 * Processor Entry Point (Phase 2)
 * 
 * This module runs in Fargate tasks that are triggered by SQS messages.
 * Each message contains an S3 key pointing to an NDJSON chunk file.
 * The processor uses HuronPersonIntegration from huron-person project,
 * treating each chunk as a "mini full sync" with S3DataSourceConfig.
 * 
 * Architecture:
 * - Reuses HuronPersonIntegration.run() completely
 * - Each chunk is processed as a bulk sync operation (1 API call for all persons)
 * - Aggregate of all chunk syncs = complete full sync
 * - Zero code duplication from SyncPeople.ts
 * 
 * Environment Variables:
 * - REGION: AWS region (e.g., 'us-east-2')
 * - SECRET_ARN: Name of the Secrets Manager secret containing huron-person config. 
 *   Gets the secret as an alternative to the secrets injection of 'HURON_PERSON_CONFIG_JSON' 
 *   environment variable.
 * - CHUNKS_BUCKET: Bucket containing the chunk file (or from SQS message when running in ECS)
 * - CHUNK_KEY: Key of the NDJSON chunk file to process (or from SQS message when running in ECS)
 * - SQS_QUEUE_URL: URL of the SQS queue to read chunk messages from (if not using env vars for CHUNKS_BUCKET and CHUNK_KEY)
 * - STATIC_MAP_USAGE: JSON string specifying which static maps to load (e.g., '{ "orgMap": true, "stateMap": true, "countryMap": true }')
 * - BULK_RESET: If "true", will "upsert" all persons in the chunk, ignoring previous delta state. SEE: src\UpsertDeltaStrategy.ts
 * - DRY_RUN: If "true", runs the sync without making API calls (default: false)
 * - All huron-person config env vars (HURON_API_ENDPOINT, JWT credentials, storage config, etc.)
 * 
 * Input:
 * - NDJSON file with one person record per line
 * 
 * Output:
 * - Logs processing results
 * - Syncs all persons in chunk to Huron API via bulk sync
 * 
 * Example Usage:
 * ```bash
 * CHUNKS_BUCKET=my-bucket CHUNK_KEY=data/people/chunk-0000.ndjson node dist/processor.js
 * ```
 */

import {
  Config,
  ConfigManager,
  HuronPersonIntegration,
  S3DataSourceConfig,
  BasicCache,
  TargetApiErrorEventProcessor
} from 'integration-huron-person';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getChunkMetadata } from '../src/merging/MergerSubscriber';
import { NextChunk, QueueReader } from '../src/Queue';
import type { StaticMapUsage } from 'integration-huron-person/dist/types/src/data-mapper/DataMapper';
import { getLocalConfig } from '../src/Utils';
import { LoggingTargetApiErrorProcessor, TrackingTargetApiErrorProcessor } from '../src/processing/ApiErrorTracking';
import { getRetryStrategy } from '../src/processing/ApiErrorRetryStrategy';

const isEcsTask = () => process.env.IS_ECS_TASK === 'true';

/**
 * Create a config with S3 data source for the chunk
 * Builds base config from environment/filesystem, then injects S3 chunk details
 * Also derives delta storage path from chunk key to organize delta outputs by run
 */
export const buildChunkConfig = async (params: {
  bucketName: string, 
  s3Key: string, 
  integratedDeltaStoragePath: string,
  region?: string
}): Promise<Config> => {
  const { bucketName, s3Key, integratedDeltaStoragePath, region } = params;
  // Load base configuration from environment/filesystem
  const { HURON_PERSON_CONFIG_PATH, SECRET_ARN } = process.env;
  const configManager = ConfigManager.getInstance();
  const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
  const baseConfig = await configManager
    .reset()
    .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← Check JSON first
    .fromSecretManager(SECRET_ARN)                // ← Then check Secrets Manager if SECRET_ARN is provided
    .fromEnvironment()                            // ← Then individual overrides
    .fromFileSystem(localConfigPath)              // ← Then file-based config
    .getConfigAsync('people');

  // Create S3 data source config for this chunk
  const baseRegion = baseConfig.dataSource.people && 'region' in baseConfig.dataSource.people 
    ? baseConfig.dataSource.people.region 
    : 'us-east-1';
  
  const s3DataSource: S3DataSourceConfig = {
    bucketName,
    key: s3Key,
    region: region || baseRegion,
    // fieldsOfInterest: baseConfig.dataSource.people?.fieldsOfInterest
  };

  // Derive delta storage paths from chunk key
  // Chunked path: "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0124.ndjson" 
  //            -> "deltas/person-full/2026-03-03T19:58:41.277Z" (for chunk-specific delta writes)
  // Integrated path: SHARED_DELTA_STORAGE_DIR env var (for reading previous-input.ndjson created by merger)
  //   This matches the sharedDeltaDir used by merger.ts
  const chunkDir = s3Key.substring(0, s3Key.lastIndexOf('/'));
  const chunkedDeltaStoragePath = chunkDir.replace(/^chunks\//, 'deltas/');

  console.log(`Chunked delta storage path: ${chunkedDeltaStoragePath}`);
  console.log(`Integrated delta storage path: ${integratedDeltaStoragePath}`);

  // For DeltaStrategyForS3Bucket:
  // - keyPrefix: '' (empty - paths are already complete)
  // - clientId: Full path including 'deltas/' prefix
  // This ensures:
  //   - Chunked writes: deltas/person-full/{timestamp}/chunk-{id}.ndjson
  //   - Shared reads: delta-storage/previous-input.ndjson (no 'deltas/' prefix)
  const clientIdForDeltaStrategy = chunkedDeltaStoragePath; // Keep full path with 'deltas/' prefix

  // Return config with S3 data source and overridden storage/clientId for delta storage
  // IMPORTANT: 
  // - storage.config.bucketName: Use chunks bucket (not input bucket)
  // - integration.clientId: Full path with 'deltas/' prefix for chunked writes
  // - integratedDeltaClientId: Full path (delta-storage) for shared reads
  // - storage.config.keyPrefix: Empty string (paths are complete, no prefix needed)
  // - storage.config.keyPrefix: Set to 'deltas/' to override the test-datasets/${clientId} default
  //   This prevents DeltaStrategyForS3Bucket from defaulting to test-datasets/person-full/{timestamp}
  return {
    ...baseConfig,
    dataSource: {
      ...baseConfig.dataSource,
      people: s3DataSource
    },
    integration: {
      ...baseConfig.integration,
      clientId: clientIdForDeltaStrategy // Full path for chunk-specific delta writes: deltas/person-full/{timestamp}
    },
    integratedDeltaClientId: integratedDeltaStoragePath, // Path for reading shared previous-input.ndjson: delta-storage
    storage: {
      ...baseConfig.storage,
      config: (() => {
        const baseConfigStorage = (baseConfig.storage.config as any) || {};
        const { keyPrefix: _, ...otherConfig } = baseConfigStorage;
        return {
          ...otherConfig,
          bucketName: bucketName,     // Use chunks bucket, not input bucket from base config
          keyPrefix: ''               // Empty - no prefix needed since clientId values above contain complete paths
        };
      })()
    }
  } as Config;
}

/**
 * Extract chunk ID from S3 key
 * @param s3Key - S3 key like "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0124.ndjson"
 * @returns chunk ID like "0124" or undefined if not present
 */
export const extractChunkId = (s3Key: string): string | undefined => {
  const match = s3Key.match(/chunk-(\d+)\.ndjson$/);
  return match ? match[1] : undefined;
}

/**
 * Extract integration timestamp from S3 key
 * @param s3Key - S3 key like "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0124.ndjson"
 * @returns ISO timestamp like "2026-03-03T19:58:41.277Z" or undefined if not present
 */
export const extractIntegrationTimestamp = (s3Key: string): string | undefined => {
  const match = s3Key.match(/\/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\//);
  return match ? match[1] : undefined;
}

/**
 * Read chunk metadata file from S3
 * @param bucketName - S3 bucket containing the metadata
 * @param s3Key - Chunk S3 key like "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0124.ndjson"
 * @param region - AWS region
 * @returns Metadata object with bulkReset, chunkCount, deltaStoragePath, etc.
 */
export const readChunkMetadata = async (
  bucketName: string,
  s3Key: string,
  region?: string
): Promise<{ bulkReset?: boolean; chunkCount?: number; totalRecords?: number; deltaStoragePath?: string; [key: string]: any }> => {
  try {
    // Derive chunk directory from chunk key
    // "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson" -> "chunks/person-full/2026-03-03T19:58:41.277Z"
    const chunkDirectory = s3Key.substring(0, s3Key.lastIndexOf('/'));
    
    console.log(`Reading metadata from chunk directory: ${chunkDirectory}`);
    
    // Reuse shared getChunkMetadata function from MergerSubscriber
    const metadata = await getChunkMetadata(chunkDirectory, bucketName, region);
    
    if (metadata) {
      console.log(`Metadata loaded: bulkReset=${metadata.bulkReset ?? 'not specified'}, chunkCount=${metadata.chunkCount}, totalRecords=${metadata.totalRecords}`);
      return metadata;
    }
    
    return {};
  } catch (error: any) {
    console.warn(`Warning: Could not read metadata file: ${error.message}`);
    console.warn('Falling back to environment variables for configuration');
    return {};
  }
};

export const validateChunk = (chunk: NextChunk | undefined) => {
  if (!chunk) {
    throw new Error('No chunk information provided in SQS message or environment variables');
  }
  const { bucketName, s3Key } = chunk;
  if (!bucketName) {
    console.error('ERROR: CHUNKS_BUCKET environment variable or queue message required');
    process.exit(1);
  }

  if (!s3Key) {
    console.error('ERROR: CHUNK_KEY environment variable or queue message required');
    process.exit(1);
  }
}

/**
 * Creates a _processing_complete marker file to trigger the merger Lambda.
 * 
 * This solves the S3 event race condition where delta files created and deleted
 * within 6-15 seconds can prevent S3 event notifications from being sent.
 * 
 * Timeline that caused the issue:
 * - 20:44:45.333Z: Delta file created → S3 begins internal event processing
 * - 20:44:51.771Z: Delta file deleted (6.4s later) → S3 event processing aborted
 * - Result: Lambda never invoked (no log stream)
 * 
 * Solution: Create marker file AFTER processing completes. Marker file:
 * - Never deleted (remains for audit trail)
 * - Guarantees S3 event will trigger because file persists
 * - Contains metadata about the completed chunk for debugging
 * 
 * @param bucketName - S3 bucket containing chunks
 * @param chunkKey - S3 key of the processed chunk (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0042.ndjson")
 * @param region - AWS region
 */
async function createProcessingCompleteMarker(bucketName: string, chunkKey: string, region?: string): Promise<void> {
  // Derive delta storage path from chunk key
  // Example: "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0042.ndjson" 
  //       -> "deltas/person-full/2026-03-03T19:58:41.277Z"
  const chunkDirectory = chunkKey.substring(0, chunkKey.lastIndexOf('/'));
  const deltaStoragePath = chunkDirectory.replace(/^chunks\//, 'deltas/');
  
  // Extract chunk ID from key
  const chunkFilename = chunkKey.substring(chunkKey.lastIndexOf('/') + 1);
  const chunkIdMatch = chunkFilename.match(/chunk-(\d+)\.ndjson$/);
  const chunkId = chunkIdMatch ? chunkIdMatch[1] : 'unknown';
  
  // Create marker file path: deltas/person-full/2026-03-03T19:58:41.277Z/chunk-0042_processing_complete.json
  const markerKey = `${deltaStoragePath}/chunk-${chunkId}_processing_complete.json`;
  
  const markerContent = {
    chunkId,
    chunkKey,
    deltaStoragePath,
    processedAt: new Date().toISOString(),
    status: 'complete'
  };
  
  console.log(`Creating marker file: s3://${bucketName}/${markerKey}`);
  
  const s3Client = new S3Client({ region });
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: markerKey,
      Body: JSON.stringify(markerContent, null, 2),
      ContentType: 'application/json'
    }));
    console.log(`✓ Marker file created successfully`);
  } catch (error: any) {
    console.error(`Failed to create marker file: ${error.message}`);
    throw error;
  }
}

export async function main(queueReader: QueueReader) {
  // Check for expected environment variables
  const { 
    REGION:region, 
    CHUNKS_BUCKET: chunksBucket,
    CHUNK_KEY: chunkKey,
    SQS_QUEUE_URL: queueUrl,
    HURON_PERSON_CONFIG_JSON,
    STATIC_MAP_USAGE,
    DRY_RUN,
    BULK_RESET,
    DYNAMODB_TABLE_NAME: dynamoDbTableName,
    RETRY_STRATEGY,
    SHARED_DELTA_STORAGE_DIR='delta-storage'
  } = process.env;  
  const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';
  const staticMapUsage: StaticMapUsage | undefined = STATIC_MAP_USAGE ? JSON.parse(STATIC_MAP_USAGE) : undefined;

  console.log(`=== ${dryRun ? 'DRY RUN: ' : ''}Phase 2: Processor (using HuronPersonIntegration) ===\n`);
  console.log(`Chunks bucket: ${chunksBucket || 'from SQS messages'}`);
  console.log(`Chunk key: ${chunkKey || 'from SQS messages'}`);
  console.log(`SQS queue URL: ${queueUrl || 'not set, using environment variables for bucket/key'}`);
  console.log(`Huron person config json: ${HURON_PERSON_CONFIG_JSON?.substring(0, 10)}...`);
  console.log(`Static map usage: ${JSON.stringify(staticMapUsage ?? {})}`);
  console.log(`DynamoDB table: ${dynamoDbTableName || 'not configured'}`);
  
  // Read chunk information from queue or environment
  let nextChunk: NextChunk | undefined;
  if(chunksBucket && chunkKey) {
    nextChunk = {bucketName: chunksBucket, s3Key: chunkKey};
  } 
  else if(queueUrl) {
    console.log('Reading chunk information from SQS queue...');
    nextChunk = await queueReader.receiveMessage() as NextChunk;
    if(isEcsTask() && !nextChunk) {
      console.log('Empty queue - this probably means that the desired count for the ' +
        'service has not scaled down yet to zero after processing the last message and deleting ' +
        'it from the queue. An empty queue will eventually cause the service to scale down to ' +
        'zero, but in the meantime we should just exit the task.');
      console.log('✗ Task cancelled.');
      process.exit(0);
    }
  } 
  else {
    console.error('ERROR: Either CHUNKS_BUCKET and CHUNK_KEY environment variables or SQS_QUEUE_URL must be provided');
    process.exit(1);
  }
  const { bucketName, s3Key } = nextChunk || {};

  // Validate required information
  validateChunk(nextChunk);

  // Read chunk metadata to get per-sync configuration (bulkReset, etc.)
  const metadata = await readChunkMetadata(bucketName!, s3Key!, region);
  
  // Use bulkReset from metadata if available, otherwise fall back to environment variable
  const bulkReset = metadata.bulkReset ?? (`${BULK_RESET}`.trim().toLowerCase() === 'true');
  console.log(`Bulk Reset: ${bulkReset}${metadata.bulkReset !== undefined ? ' (from metadata)' : ' (from environment)'}`);

  // Extract chunk ID from S3 key (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0029.ndjson" -> "0029")
  const chunkId = extractChunkId(s3Key!);

  // Extract integration timestamp from S3 key (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0029.ndjson" -> "2026-03-03T19:58:41.277Z")
  const integrationTimestamp = extractIntegrationTimestamp(s3Key!) || new Date().toISOString();

  console.log(`Processing chunk: s3://${bucketName}/${s3Key}`);
  if (chunkId) {
    console.log(`Chunk ID: ${chunkId}`);
  }
  console.log(`Integration timestamp: ${integrationTimestamp}`);
  console.log(`Region: ${region || 'default (us-east-1)'}\n`);

  // Initialize a retry strategy based on environment variable configuration
  const retryStrategy = getRetryStrategy(RETRY_STRATEGY);
  if(retryStrategy) {
    console.log(`Retry strategy initialized: ${RETRY_STRATEGY}`);
  }

  // Initialize error tracker for capturing errors and statistics to DynamoDB
  let errorTracker: TargetApiErrorEventProcessor | undefined;
  if (dynamoDbTableName) {
    errorTracker = new TrackingTargetApiErrorProcessor({
      tableName: dynamoDbTableName,
      integrationTimestamp,
      region,
      logToConsole: true
    });
    console.log(`Error tracker initialized with table: ${dynamoDbTableName}`);
  } else {
    console.warn('WARNING: DYNAMODB_TABLE_NAME not configured - error tracking disabled');
    errorTracker = new LoggingTargetApiErrorProcessor();
  }

  const startTimestamp = new Date().toISOString();
  let processedRecordCount = 0;

  try {
    // Build config with S3 data source pointing to this chunk
    const config = await buildChunkConfig({
      bucketName: bucketName!,
      s3Key: s3Key!,
      integratedDeltaStoragePath: SHARED_DELTA_STORAGE_DIR,
      region
    });

    // Create shared cache instance for JWT tokens to avoid repeated authentication
    // This cache will be shared across all API client instances (organizations, person lookups, person updates)
    const cache = BasicCache.getInstance(config);
    if(cache) {
      console.log(`Cache instance created: ${cache.constructor.name}`);
    }

    /**
     * Disable cleanup of delta files to prevent S3 event race condition.
     * 
     * Why? S3 event notifications require internal processing time (typically 1-15 seconds).
     * If delta files are created and deleted within this window, S3 may:
     * 1. Still be processing the "ObjectCreated" event internally
     * 2. Detect the object no longer exists
     * 3. Cancel or never send the event notification to the merger Lambda
     * 
     * Timeline that caused the issue:
     * - 20:44:45.333Z: Delta file created → S3 begins internal event processing
     * - 20:44:51.771Z: Delta file deleted (6.4s later) → S3 event processing aborted
     * - Result: Merger Lambda never invoked (no log stream = no invocation)
     * 
     * Solution: Set cleanupPreviousData=false to prevent delta file deletion.
     * Delta files are cleaned up by the merger task after successful merge.
     * See: integration-huron-person-fargate/docker/merger.ts (cleanupDeltaFiles method)
     */
    const cleanupPreviousData = false;

    // Create and run integration using HuronPersonIntegration
    const integration = new HuronPersonIntegration({ 
      config,  // Pass pre-built config with S3 or API data source
      staticMapUsage, // Pass through static map usage from environment variable
      bulkReset, // Pass through bulk reset flag from environment variable
      cache, // Shared cache for JWT tokens
      errorEventProcessor: errorTracker, // Inject error tracker for tracking errors and throttling
      retryStrategy, // Inject retry strategy for handling transient API failures (429, 5xx, network errors)
      cleanupPreviousData
    });
    
    /**
     * Pass chunkId to enable chunked storage output.
     * If in dry run mode, this won't actually write deltas but allows us to see the intended 
     * storage paths in logs.
     * 
     * NOTE: This function engages the "EndToEnd" flow of the integration, which "believes" it is
     * processing a full sync, but the S3 data source is actually scoped to just the chunk file.
     * When the "EndToEnd" flow executes the DeltaStorage.updatePreviousData method, it won't be
     * be overwriting a global previous-input.ndjson file, but instead writing to a chunk-specific,
     * unique delta storage path derived from the chunk ID (as part of parallel processing). 
     * Thus, no prior state is ever actually being overwritten, though we let the "EndToEnd" flow 
     * maintain the illusion that it is doing so. We will later have the merger read all of these 
     * chunk-specific delta outputs and merge them together to create a new global 
     * previous-input.ndjson for the next run.
     */
    const result = await integration.run(`Processing chunk: s3://${bucketName}/${s3Key}`, chunkId);

    // Extract actual record count from integration result
    processedRecordCount = result.totalProcessed;
    
    console.log(`\n✓ Chunk integration completed with results:`);
    console.log(`  - Total Processed: ${result.totalProcessed}`);
    console.log(`  - ✓ Successful: ${result.successCount}`);
    console.log(`  - ✗ Failed: ${result.failureCount}`);
    console.log(`  - + Added: ${result.addedCount}`);
    console.log(`  - ~ Updated: ${result.updatedCount}`);
    console.log(`  - - Removed: ${result.removedCount}`);
    console.log(`  - ⧗ Duration: ${result.duration}ms`);

    // Create marker file to trigger merger Lambda
    // This prevents S3 event race condition where delta files are deleted before events process
    await createProcessingCompleteMarker(bucketName!, s3Key!, region);

    console.log('\n✓ Chunk processing completed successfully');

  } catch (error: any) {
    console.error(`\n✗ Processing chunk: s3://${bucketName}/${s3Key} failed:`, error.message);
    console.error(error.stack);
  } finally {
    // Write statistics to DynamoDB
    if (errorTracker instanceof TrackingTargetApiErrorProcessor) {
      const endTimestamp = new Date().toISOString();
      try {
        await errorTracker.writeStatistics({
          startTimestamp,
          endTimestamp,
          chunkCount: 1, // This processor handles 1 chunk per run
          chunkSize: processedRecordCount,
          totalRecords: processedRecordCount,
          sourceDescription: `chunk-${chunkId || 'unknown'}`
        });

        // Log statistics summary
        const stats = errorTracker.getStatisticsSummary();
        console.log('\n=== Processing Statistics ===');
        console.log(`Total errors: ${stats.totalErrors}`);
        console.log(`Throttle events: ${stats.throttleCount}`);
        console.log(`Errors by status:`, stats.errorsByStatus);
      } catch (statsError: any) {
        console.error('Failed to write statistics to DynamoDB:', statsError);
        // Don't fail the entire process if statistics write fails
      }
    }
  }
  
  // Exit after finally block completes
  // Exit code 0 for success, 1 if there was an error (errorTracker will have non-zero totalErrors)
  const exitCode = (errorTracker instanceof TrackingTargetApiErrorProcessor && errorTracker.getStatisticsSummary().totalErrors > 0) ? 1 : 0;
  process.exit(exitCode);
}

// Run if executed directly
if (require.main === module) {
  main(QueueReader.getInstance());
}
