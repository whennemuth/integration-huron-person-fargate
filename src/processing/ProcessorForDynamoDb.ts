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
 * - DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME: DynamoDB table for current person hash state
 * - DYNAMODB_PERSON_HISTORY_TABLE_NAME: DynamoDB table for person history audit trail
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
 * DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME=my-stack-person-current-state-preview \
 * DYNAMODB_PERSON_HISTORY_TABLE_NAME=my-stack-person-history-preview \
 * node dist/processor-dynamodb.js
 * ```
 */

import { FieldSet, humanReadableFromMilliseconds, TestEnvironment, Timer } from 'integration-core';
import {
  BasicCache,
  Config,
  ConfigManager,
  HuronPersonIntegration,
  S3DataSourceConfig,
  TargetApiErrorEventProcessor
} from 'integration-huron-person';
import type { StaticMapUsage } from 'integration-huron-person/dist/types/src/data-mapper/DataMapper';
import { getRetryStrategy } from '../ApiErrorRetryStrategy';
import { LoggingTargetApiErrorProcessor, TrackingTargetApiErrorProcessor } from '../ApiErrorTracking';
import { NextChunk, QueueReader } from '../Queue';
import { TaskProtection } from '../TaskProtection';
import { getLocalConfig } from '../Utils';
import { ChunkFileManager, Flags } from '../chunking/metadata';
import { MetadataFactoryForBootstrap } from '../chunking/metadata/MetadataFactory';
import { StandardMetadataUtils } from '../chunking/metadata/MetadataUtils';
import { PersonCacheLookup } from '../person-cache/PersonCacheLookup';
import { SyncPopulation } from '../../docker/chunkTypes';

const metadataStorage = new MetadataFactoryForBootstrap().createMetadataForBootstrap();
const metadataUtils = new StandardMetadataUtils({});

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
  ChunkFileManager.validateChunk(nextChunk);

  // Read flags file
  const flags = await metadataStorage.readFlagsFromChunkKey(bucketName, s3Key, region);
  const bulkReset = flags.bulkReset ?? (`${BULK_RESET}`.trim().toLowerCase() === 'true');
  const trustPreviousStorage = flags.trustPreviousStorage ?? true;
  const syncPopulation = flags.syncPopulation ?? SyncPopulation.PersonFull;

  console.log(`Bulk Reset: ${bulkReset}${flags.bulkReset !== undefined ? ' (from flags)' : ' (from environment)'}`);
  console.log(`Trust Previous Storage: ${trustPreviousStorage}${flags.trustPreviousStorage !== undefined ? ' (from flags)' : ' (defaulted)'}`);
  console.log(`Sync Population: ${syncPopulation}${flags.syncPopulation !== undefined ? ' (from flags)' : ' (defaulted)'}`);

  const chunkId = metadataUtils.extractChunkId(s3Key!);
  const integrationTimestamp = metadataUtils.extractIntegrationTimestamp(s3Key!) || new Date().toISOString();

  console.log(`Processing chunk: s3://${bucketName}/${s3Key}`);
  if (chunkId) {
    console.log(`Chunk ID: ${chunkId}`);
  }
  console.log(`Integration timestamp: ${integrationTimestamp}`);
  console.log(`Region: ${region || 'default (us-east-1)'}\n`);

  // Get DynamoDB table names from environment variables (required)
  const currentStateTableName = process.env.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME;
  const historyTableName = process.env.DYNAMODB_PERSON_HISTORY_TABLE_NAME;

  if (!currentStateTableName || !historyTableName) {
    console.error('ERROR: DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME and DYNAMODB_PERSON_HISTORY_TABLE_NAME environment variables required');
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

  const startTimestamp = new Date().toISOString();
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

    // Create and run integration
    const integration = new HuronPersonIntegration({ 
      config,
      staticMapUsage,
      bulkReset,
      trustPreviousStorage,
      cache,
      lookupPersonInTargetSystemCache: async (person: FieldSet | string) => {
        // Provide a lookup against s3 for a list of ALL buids, which is retained as a cache.
        return new PersonCacheLookup({ 
          config, region, bucketName 
        }).lookupPersonInTargetSystemCache({ person, s3Key });
      },
      errorEventProcessor: errorTracker,
      retryStrategy,
      cleanupPreviousData: false, // DynamoDB manages its own data, no cleanup needed
      ignoreRemovals: syncPopulation === SyncPopulation.PersonDelta,
      flags, // Pass flags for mock target support
      syncRunId: integrationTimestamp // Pass integration timestamp as sync run ID
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
    
    // Verify the math: successful operations should equal delta operations (failures and skips don't produce deltas)
    const deltaSum = result.addedCount + result.updatedCount + result.removedCount;
    if (result.successCount !== deltaSum) {
      console.warn(`  ⚠️  Math mismatch: Successful(${result.successCount}) should equal Added(${result.addedCount}) + Updated(${result.updatedCount}) + Removed(${result.removedCount}) = ${deltaSum}`);
    }
    
    console.log('\n✓ Chunk processing completed successfully');
    console.log('✓ DynamoDB tables updated (no marker files needed)');

  } catch (error: any) {
    console.error(`\n✗ Processing chunk: s3://${bucketName}/${s3Key} failed:`, error.message);
    console.error(error.stack);
    // Store error for reporting in finally block
    processingError = error;
  } finally {
    // No marker files needed with DynamoDB strategy!
    // DynamoDB handles concurrency atomically via BatchWriteItem
    // Merger queries DynamoDB directly for deletion detection

    // Write statistics to DynamoDB
    try {
      if (errorTracker instanceof TrackingTargetApiErrorProcessor) {
        const endTimestamp = new Date().toISOString();
        // Extract chunk ID from S3 key (format: "chunk-0009")
        const chunkIdFromDesc = chunkId ? `chunk-${chunkId}` : undefined;
        
        await errorTracker.writeStatistics({
          startTimestamp,
          endTimestamp,
          chunkCount: 1, // This processor handles 1 chunk per run
          chunkSize: processedRecordCount,
          totalRecords: processedRecordCount,
          sourceDescription: `chunk-${chunkId || 'unknown'}`,
          chunkId: chunkIdFromDesc // Pass chunk ID to prevent overwrites
        });

        // Log statistics summary
        const stats = errorTracker.getStatisticsSummary();
        console.log('\n=== Processing Statistics ===');
        console.log(`Total errors: ${stats.totalErrors}`);
        console.log(`Throttle events: ${stats.throttleCount}`);
        console.log(`Errors by status:`, stats.errorsByStatus);
      }
      
      timer.stop();
      const durationMs = timer.getElapsedMilliseconds();
      
      console.log(`\n=== Processing Summary ===`);
      console.log(`Chunk: ${s3Key}`);
      console.log(`Status: ${processingError ? '✗ FAILED' : '✓ SUCCESS'}`);
      console.log(`Records processed: ${processedRecordCount}`);
      console.log(`Duration: ${humanReadableFromMilliseconds(durationMs)}`);
      
      if (processingError) {
        console.error(`\nError: ${processingError.message}`);
      } else {
        console.log('\n✓ Processor task completed successfully');
      }
    } 
    catch (statsError: any) {
      console.error('Failed to write statistics to DynamoDB:', statsError);
      // Don't fail the entire process if statistics write fails
    }
    finally {
      // Always disable task protection, even if statistics write fails
      await new TaskProtection().disable();
    }
  }
  
  // Exit after finally block completes
  // Exit code 0 for success, 1 if there was an error
  const exitCode = (errorTracker instanceof TrackingTargetApiErrorProcessor && errorTracker.getStatisticsSummary().totalErrors > 0) ? 1 : 0;
  process.exit(exitCode);
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
    'DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME',
    'DYNAMODB_PERSON_HISTORY_TABLE_NAME',
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
