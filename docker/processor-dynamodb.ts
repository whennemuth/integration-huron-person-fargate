/**
 * Processor Entry Point - DynamoDB Delta Strategy (Phase 2)
 * 
 * This is a simplified processor that uses DynamoDB tables for delta storage
 * instead of S3 file-based mini-deltas. Eliminates the "maintain the illusion"
 * coordination problem by writing directly to shared DynamoDB tables.
 * 
 * Key Differences from processor.ts:
 * - No mini-delta S3 files (writes directly to DynamoDB)
 * - No marker files (no consolidation needed)
 * - No outputKeyPrefix complexity (DynamoDB handles concurrency)
 * - Simpler configuration (no delta storage path manipulation)
 * 
 * Architecture:
 * - Reuses HuronPersonIntegration.run() completely
 * - Each chunk writes directly to PersonCurrentState + PersonHistory tables
 * - All processors share the same tables (atomic BatchWriteItem operations)
 * - Merger simplified to deletion detection only (no file consolidation)
 * 
 * Environment Variables:
 * - REGION: AWS region (e.g., 'us-east-2')
 * - SECRET_ARN: Name of the Secrets Manager secret containing huron-person config
 * - CHUNKS_BUCKET: Bucket containing the chunk file (or from SQS message)
 * - CHUNK_KEY: Key of the NDJSON chunk file to process (or from SQS message)
 * - SQS_QUEUE_URL: URL of the SQS queue to read chunk messages from
 * - PERSON_CURRENT_STATE_TABLE_NAME: DynamoDB table for current person hash state
 * - PERSON_HISTORY_TABLE_NAME: DynamoDB table for person history audit trail
 * - STATIC_MAP_USAGE: JSON string specifying which static maps to load
 * - BULK_RESET: If "true", will upsert all persons (ignore previous state)
 * - DRY_RUN: If "true", runs without making API calls
 * - DYNAMODB_STATISTICS_TABLE_NAME: Table for error tracking and statistics
 * - All huron-person config env vars (HURON_API_ENDPOINT, JWT credentials, etc.)
 * 
 * Input:
 * - NDJSON file with one person record per line
 * 
 * Output:
 * - Logs processing results
 * - Writes to PersonCurrentState table (hash + syncRunId)
 * - Writes to PersonHistory table (full audit trail with changeType)
 * - Syncs persons to Huron API via bulk sync
 * 
 * Example Usage:
 * ```bash
 * CHUNKS_BUCKET=my-bucket \
 * CHUNK_KEY=chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson \
 * PERSON_CURRENT_STATE_TABLE_NAME=my-stack-person-current-state-preview \
 * PERSON_HISTORY_TABLE_NAME=my-stack-person-history-preview \
 * node dist/processor-dynamodb.js
 * ```
 */

import { FieldSet, humanReadableFromMilliseconds, Timer, TestEnvironment, DeltaStrategyForDynamoDB } from 'integration-core';
import {
  BasicCache,
  Config,
  ConfigManager,
  HuronPersonIntegration,
  S3DataSourceConfig,
  TargetApiErrorEventProcessor
} from 'integration-huron-person';
import type { StaticMapUsage } from 'integration-huron-person/dist/types/src/data-mapper/DataMapper';
import { MetadataManager, Flags } from '../src/chunking/Metadata';
import { getRetryStrategy } from '../src/ApiErrorRetryStrategy';
import { LoggingTargetApiErrorProcessor, TrackingTargetApiErrorProcessor } from '../src/ApiErrorTracking';
import { NextChunk, QueueReader } from '../src/Queue';
import { getLocalConfig } from '../src/Utils';
import { HuronPersonCache } from '../src/PersonCache';
import { SyncPopulation } from './chunkTypes';
import { TaskProtection } from '../src/TaskProtection';

const isEcsTask = () => process.env.IS_ECS_TASK === 'true';

/**
 * Create config with S3 data source and DynamoDB delta storage
 * Much simpler than file-based processor - no complex path manipulation needed
 */
export const buildChunkConfig = async (params: {
  bucketName: string,
  s3Key: string,
  personCurrentStateTableName: string,
  personHistoryTableName: string,
  region?: string
}): Promise<Config> => {
  const { bucketName, s3Key, personCurrentStateTableName, personHistoryTableName, region } = params;
  
  // Load base configuration
  const { HURON_PERSON_CONFIG_PATH, SECRET_ARN } = process.env;
  const configManager = ConfigManager.getInstance();
  const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
  const baseConfig = await configManager
    .reset()
    .fromJsonString('HURON_PERSON_CONFIG_JSON')
    .fromSecretManager(SECRET_ARN)
    .fromEnvironment()
    .fromFileSystem(localConfigPath)
    .getConfigAsync('people');

  // Create S3 data source config for this chunk
  const baseRegion = baseConfig.dataSource.people && 'region' in baseConfig.dataSource.people 
    ? baseConfig.dataSource.people.region 
    : 'us-east-1';
  
  const s3DataSource: S3DataSourceConfig = {
    bucketName,
    key: s3Key,
    region: region || baseRegion
  };

  // Return config with S3 data source and DynamoDB delta storage
  return {
    ...baseConfig,
    dataSource: {
      ...baseConfig.dataSource,
      people: s3DataSource
    },
    storage: {
      type: 'dynamodb',
      config: {
        region: region || baseRegion,
        personCurrentStateTableName,
        personHistoryTableName,
        currentStateGSIName: 'syncRunId-personId-index'
      }
    },
    integration: {
      ...baseConfig.integration,
      clientId: 'dynamodb-processor' // Not used by DynamoDB strategy, but required by Config type
    }
  } as Config;
};

/**
 * Extract chunk ID from S3 key
 */
export const extractChunkId = (s3Key: string): string | undefined => {
  const match = s3Key.match(/chunk-(\d+)\.ndjson$/);
  return match ? match[1] : undefined;
};

/**
 * Extract integration timestamp from S3 key
 */
export const extractIntegrationTimestamp = (s3Key: string): string | undefined => {
  const match = s3Key.match(/\/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\//);
  return match ? match[1] : undefined;
};

/**
 * Read flags file from S3
 */
export const readFlagInfo = async (
  bucketName: string,
  s3Key: string,
  region?: string
): Promise<Partial<Flags>> => {
  return MetadataManager.readFlagsFromChunkKey(bucketName, s3Key, region);
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
};

export async function main(queueReader: QueueReader) {
  const { 
    REGION: region, 
    CHUNKS_BUCKET: chunksBucket,
    CHUNK_KEY: chunkKey,
    SQS_QUEUE_URL: queueUrl,
    HURON_PERSON_CONFIG_JSON,
    STATIC_MAP_USAGE,
    DRY_RUN,
    BULK_RESET,
    DYNAMODB_STATISTICS_TABLE_NAME: dynamoDbStatisticsTableName,
    RETRY_STRATEGY
  } = process.env;
  
  const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';
  const staticMapUsage: StaticMapUsage | undefined = STATIC_MAP_USAGE ? JSON.parse(STATIC_MAP_USAGE) : undefined;

  const timer = new Timer();
  timer.start();

  // Enable task protection for 4 hours
  await new TaskProtection(60 * 4).enable();

  console.log(`=== ${dryRun ? 'DRY RUN: ' : ''}Phase 2: Processor - DynamoDB Strategy ===\n`);
  console.log(`Chunks bucket: ${chunksBucket || 'from SQS messages'}`);
  console.log(`Chunk key: ${chunkKey || 'from SQS messages'}`);
  console.log(`SQS queue URL: ${queueUrl || 'not set'}`);
  console.log(`Static map usage: ${JSON.stringify(staticMapUsage ?? {})}`);
  console.log(`DynamoDB statistics table: ${dynamoDbStatisticsTableName || 'not configured'}`);
  
  // Read chunk information from queue or environment
  let nextChunk: NextChunk | undefined;
  if (chunksBucket && chunkKey) {
    nextChunk = { bucketName: chunksBucket, s3Key: chunkKey };
  } else if (queueUrl) {
    console.log('Reading chunk information from SQS queue...');
    nextChunk = await queueReader.receiveMessage() as NextChunk;
    if (isEcsTask() && !nextChunk) {
      console.log('Empty queue - service will scale down. Exiting task.');
      process.exit(0);
    }
  } else {
    console.error('ERROR: Either CHUNKS_BUCKET and CHUNK_KEY or SQS_QUEUE_URL must be provided');
    process.exit(1);
  }
  
  const { bucketName, s3Key } = nextChunk || {};
  validateChunk(nextChunk);

  // Read flags file
  const flags = await readFlagInfo(bucketName!, s3Key!, region);
  const bulkReset = flags.bulkReset ?? (`${BULK_RESET}`.trim().toLowerCase() === 'true');
  const trustPreviousStorage = flags.trustPreviousStorage ?? true;
  const syncPopulation = flags.syncPopulation ?? SyncPopulation.PersonFull;

  console.log(`Bulk Reset: ${bulkReset}${flags.bulkReset !== undefined ? ' (from flags)' : ' (from environment)'}`);
  console.log(`Trust Previous Storage: ${trustPreviousStorage}${flags.trustPreviousStorage !== undefined ? ' (from flags)' : ' (defaulted)'}`);
  console.log(`Sync Population: ${syncPopulation}${flags.syncPopulation !== undefined ? ' (from flags)' : ' (defaulted)'}`);

  const chunkId = extractChunkId(s3Key!);
  const integrationTimestamp = extractIntegrationTimestamp(s3Key!) || new Date().toISOString();

  console.log(`Processing chunk: s3://${bucketName}/${s3Key}`);
  if (chunkId) {
    console.log(`Chunk ID: ${chunkId}`);
  }
  console.log(`Integration timestamp: ${integrationTimestamp}`);
  console.log(`Region: ${region || 'default (us-east-1)'}\n`);

  // Get DynamoDB table names from environment variables (required)
  const currentStateTableName = process.env.PERSON_CURRENT_STATE_TABLE_NAME;
  const historyTableName = process.env.PERSON_HISTORY_TABLE_NAME;

  if (!currentStateTableName || !historyTableName) {
    console.error('ERROR: PERSON_CURRENT_STATE_TABLE_NAME and PERSON_HISTORY_TABLE_NAME environment variables required');
    process.exit(1);
  }

  console.log(`PersonCurrentState table: ${currentStateTableName}`);
  console.log(`PersonHistory table: ${historyTableName}\n`);

  // Initialize retry strategy
  const retryStrategy = getRetryStrategy(RETRY_STRATEGY);
  if (retryStrategy) {
    console.log(`Retry strategy initialized: ${RETRY_STRATEGY}`);
  }

  // Initialize error tracker
  let errorTracker: TargetApiErrorEventProcessor | undefined;
  if (dynamoDbStatisticsTableName) {
    errorTracker = new TrackingTargetApiErrorProcessor({
      tableName: dynamoDbStatisticsTableName,
      integrationTimestamp,
      region,
      logToConsole: true
    });
    console.log(`Error tracker initialized with table: ${dynamoDbStatisticsTableName}`);
  } else {
    console.warn('WARNING: DYNAMODB_STATISTICS_TABLE_NAME not configured - error tracking disabled');
    errorTracker = new LoggingTargetApiErrorProcessor();
  }

  let processedRecordCount = 0;
  let processingError: Error | null = null;

  try {
    // Build config with DynamoDB delta storage
    const config = await buildChunkConfig({
      bucketName: bucketName!,
      s3Key: s3Key!,
      personCurrentStateTableName: currentStateTableName,
      personHistoryTableName: historyTableName,
      region
    });

    // Create shared cache for JWT tokens
    const cache = BasicCache.getInstance(config);
    if (cache) {
      console.log(`Cache instance created: ${cache.constructor.name}`);
    }

    // Implement cache lookup for source identifiers
    let cachedSourceIdentifiers: Set<string> | undefined;
    const lookupPersonInTargetSystemCache = async (person: FieldSet | string): Promise<any> => {
      if (!cachedSourceIdentifiers) {
        const personCache = new HuronPersonCache({ config });
        const chunkDirectory = s3Key!.substring(0, s3Key!.lastIndexOf('/'));
        const key = chunkDirectory + `/${HuronPersonCache.CACHE_FILE_NAME}`;

        cachedSourceIdentifiers = await personCache.getS3PopulationCache({ 
          bucketName: bucketName!, key, region: region! 
        });

        if (!cachedSourceIdentifiers) {
          cachedSourceIdentifiers = new Set<string>();
        }
        
        console.log(`Loaded ${cachedSourceIdentifiers.size} source identifiers from target system cache`);
      }

      // Extract sourceIdentifier from person
      let sourceIdentifier: string | undefined;
      if (typeof person === 'string') {
        sourceIdentifier = person;
      } else if (typeof person === 'object' && person.fieldValues) {
        const field = person.fieldValues.find((fv: any) => {
          const fieldName = Object.keys(fv)[0];
          return fieldName === 'sourceIdentifier';
        });
        if (field) {
          sourceIdentifier = Object.values(field)[0] as string;
        }
      }

      if (sourceIdentifier && cachedSourceIdentifiers.has(sourceIdentifier)) {
        return sourceIdentifier;
      }
      
      return undefined;
    };

    // Create and run integration
    const integration = new HuronPersonIntegration({ 
      config,
      staticMapUsage,
      bulkReset,
      trustPreviousStorage,
      cache,
      lookupPersonInTargetSystemCache, 
      errorEventProcessor: errorTracker,
      retryStrategy,
      cleanupPreviousData: false, // DynamoDB manages its own data, no cleanup needed
      ignoreRemovals: syncPopulation === SyncPopulation.PersonDelta
    });
    
    const result = await integration.run(`Processing chunk: s3://${bucketName}/${s3Key}`, chunkId);

    processedRecordCount = result.totalProcessed;
    
    console.log(`\n✓ Chunk integration completed with results:`);
    console.log(`  - Total Processed: ${result.totalProcessed}`);
    console.log(`  - ✓ Successful: ${result.successCount}`);
    console.log(`  - ✗ Failed: ${result.failureCount}`);
    console.log(`  - ⊘ Skipped: ${result.skippedCount}`);
    console.log(`  - + Added: ${result.addedCount}`);
    console.log(`  - ~ Updated: ${result.updatedCount}`);
    console.log(`  - - Removed: ${result.removedCount}`);
    console.log(`  - ⧗ Duration: ${humanReadableFromMilliseconds(result.duration ?? 0)}`);
    
    console.log('\n✓ Chunk processing completed successfully');
    console.log('✓ DynamoDB tables updated (no marker files needed)');

  } catch (error: any) {
    console.error(`\n✗ Processing chunk: s3://${bucketName}/${s3Key} failed:`, error.message);
    console.error(error.stack);
    processingError = error;
  } finally {
    // No marker files needed with DynamoDB strategy!
    // DynamoDB handles concurrency atomically
    // Merger queries DynamoDB directly for deletion detection

    timer.stop();
    const durationMs = timer.getElapsedMilliseconds();
    
    console.log(`\n=== Processing Summary ===`);
    console.log(`Chunk: ${s3Key}`);
    console.log(`Status: ${processingError ? '✗ FAILED' : '✓ SUCCESS'}`);
    console.log(`Records processed: ${processedRecordCount}`);
    console.log(`Duration: ${humanReadableFromMilliseconds(durationMs)}`);
    
    if (processingError) {
      console.error(`\nError: ${processingError.message}`);
      
      // Note: SQS message deletion is handled automatically by QueueReader.receiveMessage()
      // when using SQSQueueReader. No explicit deleteMessage() call needed.
      
      process.exit(1);
    }

    console.log('\n✓ Processor task completed successfully');
  }
}

// Entry point
if (require.main === module) {
  const testEnvironment = TestEnvironment('DOCKER_PROCESSOR_DYNAMODB');
  
  // Explicit environment variable declarations for this harness
  [
    'REGION',
    'CHUNKS_BUCKET',
    'CHUNK_KEY',
    'SQS_QUEUE_URL',
    'PERSON_CURRENT_STATE_TABLE_NAME',
    'PERSON_HISTORY_TABLE_NAME',
    'STATIC_MAP_USAGE',
    'DRY_RUN',
    'BULK_RESET',
    'DYNAMODB_STATISTICS_TABLE_NAME',
    'RETRY_STRATEGY',
    'HURON_PERSON_CONFIG_PATH',
    'SECRET_ARN',
    'STACK_ID',
    'LANDSCAPE'
  ].forEach(testEnvironment.getVarOrEmptyString);

  const queueReader = QueueReader.getInstance();
  main(queueReader).catch(error => {
    console.error('Fatal error in processor:', error);
    process.exit(1);
  });
}
