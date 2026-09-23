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

import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { FieldSet, humanReadableFromMilliseconds, TestEnvironment, Timer } from 'integration-core';
import {
  BasicCache,
  Config,
  ConfigManager,
  HuronPersonIntegration,
  PersonRecordProcessor,
  S3DataSourceConfig,
  TargetApiErrorEventProcessor
} from 'integration-huron-person';
import type { StaticMapUsage } from 'integration-huron-person/dist/types/src/data-mapper/DataMapper';
import { SyncPopulation } from '../../docker/chunkTypes';
import { getRetryStrategy } from '../ApiErrorRetryStrategy';
import { NextChunk, QueueReader } from '../Queue';
import { TaskProtection } from '../TaskProtection';
import { getLocalConfig } from '../Utils';
import { ChunkFileManager } from '../chunking/metadata';
import { MetadataFactoryForBootstrap } from '../chunking/metadata/MetadataFactory';
import { StandardMetadataUtils } from '../chunking/metadata/MetadataUtils';
import { StatisticsTable } from '../dynamodb/StatisticsTable';
import { PersonCacheLookup } from '../person-cache/PersonCacheLookup';
import {
  buildErrorTracker,
  computeExitCode,
  logIntegrationResult,
  resolveCommonFlags,
  resolveCustomPersonProcessor,
  resolveNextChunk,
  resolveStaticMapUsage,
  resolveTableName,
  writeTrackerStatistics
} from './ProcessorCommon';

const metadataUtils = new StandardMetadataUtils({});

export { resolveStaticMapUsage, resolveTableName };

/**
 * Triggers the merger Fargate task by sending a message to SQS.
 * Used in DynamoDB mode when the last processor detects all chunks are complete.
 * 
 * @param chunkDirectory - The chunk directory path to pass to merger
 * @param createdAt - ISO timestamp when chunking started
 * @param mergerQueueUrl - URL of the merger SQS queue
 * @param bucketName - S3 bucket name
 * @param region - AWS region
 * @returns true if message sent successfully, false otherwise
 */
async function triggerMerger(
  chunkDirectory: string, 
  createdAt: string,
  mergerQueueUrl: string,
  bucketName: string,
  region?: string
): Promise<boolean> {
  if (!mergerQueueUrl) {
    console.error('Missing required MERGER_QUEUE_URL environment variable');
    return false;
  }

  console.log('🚀 Last processor detected completion - triggering merger via SQS queue...');

  const message = {
    chunksBucket: bucketName,
    chunkDirectory,
    createdAt
  };

  try {
    const sqsClient = new SQSClient({ region });
    console.log('Sending message to merger queue:', JSON.stringify(message, null, 2));
    await sqsClient.send(new SendMessageCommand({
      QueueUrl: mergerQueueUrl,
      MessageBody: JSON.stringify(message),
    }));
    console.log('✅ Merger trigger message sent successfully');
    return true;
  } catch (error: any) {
    console.error(`❌ Failed to send merger trigger message: ${error.message}`);
    return false;
  }
}

/**
 * Create config with S3 data source and DynamoDB delta storage
 * Much simpler than file-based processor - no complex path manipulation needed
 */
export const buildChunkConfig = async (params: {
  bucketName: string,
  s3Key: string,
  personCurrentStateTableName: string,
  personHistoryTableName: string,
  region?: string,
  integrationTimestamp?: string
}): Promise<Config> => {
  const { bucketName, s3Key, personCurrentStateTableName, personHistoryTableName, region, integrationTimestamp } = params;
  
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
        currentStateGSIName: 'syncRunId-personId-index',
        syncRunId: integrationTimestamp
      }
    },
    integration: {
      ...baseConfig.integration,
      clientId: 'dynamodb-processor' // Not used by DynamoDB strategy, but required by Config type
    }
  } as Config;
};

export async function main(queueReader: QueueReader, personRecordProcessor?: PersonRecordProcessor) {
  const { 
    REGION: region, 
    CHUNKS_BUCKET: chunksBucket,
    CHUNK_KEY: chunkKey,
    SQS_QUEUE_URL: queueUrl,
    STATIC_MAP_USAGE,
    DRY_RUN,
    BULK_RESET,
    DYNAMODB_STATISTICS_TABLE_NAME: dynamoDbStatisticsTableName,
    RETRY_STRATEGY,
    MERGER_QUEUE_URL: mergerQueueUrl,
    HURON_PERSON_CONFIG_JSON,
  } = process.env;
  
  const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';
  let staticMapUsage: StaticMapUsage | undefined = STATIC_MAP_USAGE ? JSON.parse(STATIC_MAP_USAGE) : undefined;

  const timer = new Timer();
  timer.start();

  // Enable task protection for 4 hours
  await new TaskProtection(60 * 4).enable();

  console.log(`=== ${dryRun ? 'DRY RUN: ' : ''}Phase 2: Processor - DynamoDB Strategy ===\n`);
  console.log(`Chunks bucket: ${chunksBucket || 'from SQS messages'}`);
  console.log(`Chunk key: ${chunkKey || 'from SQS messages'}`);
  console.log(`Huron person config json: ${HURON_PERSON_CONFIG_JSON?.substring(0, 10)}...`);
  console.log(`SQS queue URL: ${queueUrl || 'not set'}`);
  console.log(`Static map usage: ${JSON.stringify(staticMapUsage ?? {})}`);
  console.log(`DynamoDB statistics table: ${dynamoDbStatisticsTableName || 'not configured'}`);

  // Hoisted so the catch/finally below can report on however far execution got
  let bucketName: string | undefined;
  let s3Key: string | undefined;
  let chunkId: string | undefined;
  let integrationTimestamp: string | undefined;
  let chunkDirectory: string | undefined;
  let errorTracker: TargetApiErrorEventProcessor | undefined;
  let errorTrackingStatisticsTableName: string | undefined;
  let processedRecordCount = 0;
  let processingError: Error | null = null;
  const startTimestamp = new Date().toISOString();

  try {
    // Read chunk information from queue or environment
    const nextChunk = await resolveNextChunk({ queueReader, chunksBucket, chunkKey, queueUrl });
    ({ bucketName, s3Key } = nextChunk || {});

    // Validate required information
    ChunkFileManager.validateChunk(nextChunk);

    // Read flags - tries the mock statistics table first (if configured), falling back to the
    // real table, since flags.useMockTarget (what determines which table chunker used) can only
    // be learned from the flags themselves. See MetadataFactoryForBootstrap.resolveMockAwareFlags().
    chunkDirectory = s3Key!.substring(0, s3Key!.lastIndexOf('/'));
    const { flags, statisticsTableName: resolvedStatisticsTableName } = await new MetadataFactoryForBootstrap()
      .resolveMockAwareFlags({ bucketName, chunkDirectory, region });

    const { bulkReset, trustPreviousStorage, syncPopulation } = resolveCommonFlags(flags, BULK_RESET);

    staticMapUsage = resolveStaticMapUsage(staticMapUsage, flags.useMockTarget);
    if (flags.useMockTarget) {
      console.log(`Static map usage overridden for mock target mode: ${JSON.stringify(staticMapUsage)}`);
    }


    // Extract chunk ID from S3 key (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0029.ndjson" -> "0029")
    chunkId = metadataUtils.extractChunkId(s3Key!);

    // Extract integration timestamp from S3 key (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0029.ndjson" -> "2026-03-03T19:58:41.277Z")
    integrationTimestamp = metadataUtils.extractIntegrationTimestamp(s3Key!) || new Date().toISOString();

    console.log(`Processing chunk: s3://${bucketName}/${s3Key}`);
    if (chunkId) {
      console.log(`Chunk ID: ${chunkId}`);
    }
    console.log(`Integration timestamp: ${integrationTimestamp}`);
    console.log(`Region: ${region || 'default (us-east-1)'}\n`);

    // Initialize a retry strategy based on environment variable configuration
    const retryStrategy = getRetryStrategy(RETRY_STRATEGY);
    if (retryStrategy) {
      console.log(`Retry strategy initialized: ${RETRY_STRATEGY}`);
    }

    // Get DynamoDB table names from environment variables (required)
    // Redirected to isolated mock tables when flags.useMockTarget is true, so DeltaStrategyForDynamoDB
    // never mixes mocked person hash/history state with production data
    const currentStateTableName = resolveTableName(
      process.env.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME,
      process.env.DYNAMODB_MOCK_PERSON_CURRENT_STATE_TABLE_NAME,
      flags.useMockTarget
    );
    const historyTableName = resolveTableName(
      process.env.DYNAMODB_PERSON_HISTORY_TABLE_NAME,
      process.env.DYNAMODB_MOCK_PERSON_HISTORY_TABLE_NAME,
      flags.useMockTarget
    );

    if (!currentStateTableName || !historyTableName) {
      console.error('ERROR: DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME and DYNAMODB_PERSON_HISTORY_TABLE_NAME environment variables required');
      process.exit(1);
    }

    console.log(`PersonCurrentState table: ${currentStateTableName}`);
    console.log(`PersonHistory table: ${historyTableName}\n`);

    // Initialize error tracker
    // Uses the already-resolved statistics table (mock or real, whichever holds this run's FLAGS -
    // see MetadataFactoryForBootstrap.resolveMockAwareFlags()), so STATISTICS/ERROR/CHUNK_STATUS/
    // METADATA records for a mock run never mix with production data, and never split across tables.
    errorTrackingStatisticsTableName = resolvedStatisticsTableName;
    errorTracker = buildErrorTracker({ tableName: errorTrackingStatisticsTableName, integrationTimestamp, region });

    // Build config with DynamoDB delta storage
    const config = await buildChunkConfig({
      bucketName: bucketName!,
      s3Key: s3Key!,
      personCurrentStateTableName: currentStateTableName,
      personHistoryTableName: historyTableName,
      region,
      integrationTimestamp
    });

    // Create shared cache for JWT tokens
    const cache = BasicCache.getInstance(config);
    if (cache) {
      console.log(`Cache instance created: ${cache.constructor.name}`);
    }

    // Optional custom per-person async hook (e.g. outlier logging). Uses the explicitly injected
    // processor if provided (see docker/processor.ts); otherwise resolves it from this run's
    // Flags (flags.personRecordProcessorCustomizations) - no-op if neither is present.
    const customPersonProcessor = await resolveCustomPersonProcessor(personRecordProcessor, flags.personRecordProcessorCustomizations);

    // Create and run integration
    const integration = new HuronPersonIntegration({ 
      config,
      staticMapUsage,
      bulkReset,
      trustPreviousStorage,
      cache,
      lookupPersonInTargetSystemCache: (() => {
        // Create PersonCacheLookup instance once for entire chunk processing
        const personCacheLookup = new PersonCacheLookup({ config, region, bucketName });
        // Return the lookup function that uses the cached instance
        return (person: FieldSet | string) => 
          personCacheLookup.lookupPersonInTargetSystemCache({ person, s3Key: s3Key! });
      })(),
      errorEventProcessor: errorTracker,
      retryStrategy,
      cleanupPreviousData: false, // DynamoDB manages its own data, no cleanup needed
      ignoreRemovals: syncPopulation === SyncPopulation.PersonDelta,
      flags, // Pass flags for mock target support
      syncRunId: integrationTimestamp, // Pass integration timestamp as sync run ID
      personRecordProcessor: customPersonProcessor
    });
    
    const result = await integration.run(`Processing chunk: s3://${bucketName}/${s3Key}`, chunkId);

    processedRecordCount = result.totalProcessed;

    logIntegrationResult(result);

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
      if (errorTracker) {
        await writeTrackerStatistics({ errorTracker, startTimestamp, chunkId, processedRecordCount });
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

      // DynamoDB Mode: Write CHUNK_STATUS and check for completion to trigger merger
      if (errorTrackingStatisticsTableName && chunkId && integrationTimestamp) {
        try {
          // Uses the same resolved (mock-or-real) statistics table as the error tracker, so a
          // mock run's completion detection never looks at the wrong table's chunkCount.
          const statisticsTable = StatisticsTable.fromTableName(errorTrackingStatisticsTableName, region);

          // Step 1: Write this processor's CHUNK_STATUS
          const chunkStatus = {
            status: processingError ? 'FAILED' : 'COMPLETED',
            endTime: new Date().toISOString(),
            recordCount: processedRecordCount,
            ...(processingError && { error: processingError.message })
          };
          
          await statisticsTable.writeChunkStatus(integrationTimestamp, chunkId, chunkStatus);
          console.log(`✓ CHUNK_STATUS written: ${chunkId} = ${chunkStatus.status}`);

          // Step 2: Only check for completion if this chunk succeeded
          if (!processingError) {
            const completedCount = await statisticsTable.getCompletedChunkCount(integrationTimestamp);
            const metadata = await statisticsTable.readMetadata(integrationTimestamp);
            
            // >= rather than === : metadata isn't written until the ~80s post-chunking aggregation
            // completes, by which point completedCount may have already reached or passed the
            // true total, so an exact match could be missed entirely.
            if (metadata?.chunkCount && completedCount >= metadata.chunkCount) {
              // Step 3: We're the last processor - trigger merger!
              console.log(`✅ Last processor (chunk-${chunkId}): All ${metadata.chunkCount} chunks complete, triggering merger`);
              
              const triggered = await triggerMerger(
                chunkDirectory!,
                metadata.createdAt || integrationTimestamp,
                mergerQueueUrl!,
                bucketName!,
                region
              );

              if (triggered) {
                // Step 4: Mark merger as triggered for audit trail
                await statisticsTable.updateMetadata(integrationTimestamp, {
                  mergerTriggered: true,
                  mergerTriggeredAt: new Date().toISOString(),
                  mergerTriggeredBy: chunkId
                });
              }
            } else {
              console.log(`⏳ Processor (chunk-${chunkId}): ${completedCount} of ${metadata?.chunkCount || '?'} chunks complete, waiting...`);
            }
          }
        } catch (completionError: any) {
          console.error('Failed to check/trigger merger completion:', completionError.message);
          // Don't fail the processor just because merger trigger check failed
        }
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
  process.exit(computeExitCode(errorTracker));
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
    'DYNAMODB_MOCK_PERSON_CURRENT_STATE_TABLE_NAME',
    'DYNAMODB_MOCK_PERSON_HISTORY_TABLE_NAME',
    'DYNAMODB_MOCK_STATISTICS_TABLE_NAME',
    'HURON_PERSON_CONFIG_JSON',
    'STATIC_MAP_USAGE',
    'DRY_RUN',
    'BULK_RESET',
    'DYNAMODB_STATISTICS_TABLE_NAME',
    'RETRY_STRATEGY',
    'MERGER_QUEUE_URL',
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
