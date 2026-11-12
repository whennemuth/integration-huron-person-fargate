/**
 * Shared logic between ProcessorForS3.ts and ProcessorForDynamoDb.ts.
 *
 * Both processors run the same chunk-processing lifecycle around HuronPersonIntegration -
 * only how each stores delta state and signals post-processor completion differs (S3 marker files vs
 * DynamoDB CHUNK_STATUS/SQS). This module holds the parts that are identical between them, so
 * fixes/changes to shared behavior only need to be made once.
 */

import { CrudOperation, humanReadableFromMilliseconds, IntegrationResult } from 'integration-core';
import { Config, PersonRecordProcessor, TargetApiErrorEventProcessor } from 'integration-huron-person';
import { SyncPopulation } from '../../docker/chunkTypes';
import { LoggingTargetApiErrorProcessor, TrackingTargetApiErrorProcessor } from '../ApiErrorTracking';
import { NextChunk, QueueReader } from '../Queue';
import { Flags } from '../chunking/metadata/IMetadataStorage';
import { personRecordProcessorFactory } from './addon/PersonRecordProcessorFactory';

export const isEcsTask = () => process.env.IS_ECS_TASK === 'true';

/**
 * Resolves which chunk to process from either env vars (direct invocation) or the SQS queue
 * (ECS task). Exits the process directly for the "no config" and "empty queue" cases, matching
 * both processors' original behavior.
 */
export async function resolveNextChunk(params: {
  queueReader: QueueReader,
  chunksBucket?: string,
  chunkKey?: string,
  queueUrl?: string
}): Promise<NextChunk | undefined> {
  const { queueReader, chunksBucket, chunkKey, queueUrl } = params;
  if (chunksBucket && chunkKey) {
    return { bucketName: chunksBucket, s3Key: chunkKey };
  }
  if (queueUrl) {
    console.log('Reading chunk information from SQS queue...');
    const nextChunk = await queueReader.receiveMessage() as NextChunk;
    if (isEcsTask() && !nextChunk) {
      console.log('Empty queue - this probably means that the desired count for the ' +
        'service has not scaled down yet to zero after processing the last message and deleting ' +
        'it from the queue. An empty queue will eventually cause the service to scale down to ' +
        'zero, but in the meantime we should just exit the task.');
      console.log('✗ Task cancelled.');
      process.exit(0);
    }
    return nextChunk;
  }
  console.error('ERROR: Either CHUNKS_BUCKET and CHUNK_KEY or SQS_QUEUE_URL must be provided');
  process.exit(1);
}

/**
 * Derives bulkReset/trustPreviousStorage/syncPopulation from this run's Flags, falling back to
 * environment/defaults, with the console logging both processors report identically.
 */
export function resolveCommonFlags(flags: Partial<Flags>, bulkResetEnvVar?: string): {
  bulkReset: boolean,
  trustPreviousStorage: boolean,
  syncPopulation: SyncPopulation
} {
  const bulkReset = flags.bulkReset ?? (`${bulkResetEnvVar}`.trim().toLowerCase() === 'true');
  console.log(`Bulk Reset: ${bulkReset}${flags.bulkReset !== undefined ? ' (from flags)' : ' (from environment)'}`);

  const trustPreviousStorage = flags.trustPreviousStorage ?? true;
  console.log(`Trust Previous Storage: ${trustPreviousStorage}${flags.trustPreviousStorage !== undefined ? ' (from flags)' : ' (defaulted)'}`);

  const syncPopulation = flags.syncPopulation ?? SyncPopulation.PersonFull;
  console.log(`Sync Population: ${syncPopulation}${flags.syncPopulation !== undefined ? ' (from flags)' : ' (defaulted)'}`);

  return { bulkReset, trustPreviousStorage, syncPopulation };
}

/**
 * Applies this run's terms endpoint override (flags.termsEndpoint, set only for simulated-source
 * runs) to the config: only baseUrl and fetchPath change, the apiKey and timeout stay as configured.
 * Returns a copy; the config is returned as-is if there is no override or no terms data source.
 */
export function applyTermsEndpoint(config: Config, termsEndpoint?: Flags['termsEndpoint']): Config {
  const { terms } = config.dataSource ?? {};
  if (!termsEndpoint || !terms) {
    return config;
  }
  const { baseUrl, fetchPath } = termsEndpoint;
  console.log(`Terms endpoint: ${baseUrl}${fetchPath} (from flags)`);
  return {
    ...config,
    dataSource: {
      ...config.dataSource,
      terms: { ...terms, fetchPath, endpointConfig: { ...terms.endpointConfig, baseUrl } }
    }
  };
}

/**
 * Builds the error tracker, falling back to console-only logging when no statistics table is configured.
 */
export function buildErrorTracker(params: {
  tableName?: string,
  integrationTimestamp: string,
  region?: string
}): TargetApiErrorEventProcessor {
  const { tableName, integrationTimestamp, region } = params;
  if (tableName) {
    console.log(`Error tracker initialized with table: ${tableName}`);
    return new TrackingTargetApiErrorProcessor({ tableName, integrationTimestamp, region, logToConsole: true });
  }
  console.warn('WARNING: DYNAMODB_STATISTICS_TABLE_NAME not configured - error tracking disabled');
  return new LoggingTargetApiErrorProcessor();
}

/**
 * Resolves the optional per-person hook: an explicitly injected processor wins, otherwise it's
 * resolved from this run's Flags (flags.personRecordProcessorAddons) - a no-op if neither is present.
 */
export async function resolveAddonPersonProcessor(
  personRecordProcessor: PersonRecordProcessor | undefined,
  addons: string | undefined
): Promise<PersonRecordProcessor | undefined> {
  return personRecordProcessor
    ?? (await personRecordProcessorFactory(addons))?.processRecord;
}

/** How many people a chunk added, updated and removed in the target */
export type ChangeCounts = Pick<IntegrationResult, 'addedCount' | 'updatedCount' | 'removedCount'>;

/**
 * The people a chunk added, updated and removed in the target: its successful pushes, counted by operation.
 *
 * IntegrationResult's addedCount, updatedCount and removedCount are the sizes of the computed delta, taken
 * before the push, so they also count the people whose push failed, who are counted again among the failures:
 * a chunk of 200 with 5 failed creates reported 200 added, 195 succeeded and 5 failed. Counting the successes
 * makes added + updated + removed equal the successes, so with the failures and skips they add up to the
 * people processed. Every push result names its operation (SinglePushResult.crud); if a success doesn't (a
 * target that leaves it out), or the result carries no push results, the delta's counts are kept.
 */
export function pushedChangeCounts(result: IntegrationResult): ChangeCounts {
  const { addedCount, updatedCount, removedCount, successes } = result;
  if (!successes || successes.some(success => !success.crud)) {
    return { addedCount, updatedCount, removedCount };
  }
  const count = (crud: CrudOperation) => successes.filter(success => success.crud === crud).length;
  return {
    addedCount: count(CrudOperation.CREATE),
    updatedCount: count(CrudOperation.UPDATE),
    removedCount: count(CrudOperation.DELETE)
  };
}

/** Logs HuronPersonIntegration.run()'s result and warns if the success/change math doesn't add up. */
export function logIntegrationResult(result: IntegrationResult): void {
  const { addedCount, updatedCount, removedCount } = pushedChangeCounts(result);
  console.log(`\n✓ Chunk integration completed with results:`);
  console.log(`  - Total Processed: ${result.totalProcessed}`);
  console.log(`  - ✓ Successful: ${result.successCount}`);
  console.log(`  - ✗ Failed: ${result.failureCount}`);
  console.log(`  - ⊘ Skipped: ${result.skippedCount}`);
  console.log(`  - + Added: ${addedCount}`);
  console.log(`  - ~ Updated: ${updatedCount}`);
  console.log(`  - - Removed: ${removedCount}`);
  console.log(`  - ⧗ Duration: ${humanReadableFromMilliseconds(result.duration ?? 0)}`);

  const changeSum = addedCount + updatedCount + removedCount;
  if (result.successCount !== changeSum) {
    console.warn(`  ⚠️  Math mismatch: Successful(${result.successCount}) should equal Added(${addedCount}) + Updated(${updatedCount}) + Removed(${removedCount}) = ${changeSum}`);
  }
}

/**
 * Writes this chunk's statistics to the error tracker (no-op for the console-only fallback
 * tracker) and logs the summary. Excludes overall timer reporting, which differs per processor.
 */
export async function writeTrackerStatistics(params: {
  errorTracker: TargetApiErrorEventProcessor,
  startTimestamp: string,
  chunkId?: string,
  processedRecordCount: number,
  integrationResult?: IntegrationResult
}): Promise<void> {
  const { errorTracker, startTimestamp, chunkId, processedRecordCount, integrationResult } = params;
  if (!(errorTracker instanceof TrackingTargetApiErrorProcessor)) {
    return;
  }
  const endTimestamp = new Date().toISOString();
  const chunkIdFromDesc = chunkId ? `chunk-${chunkId}` : undefined;

  await errorTracker.writeStatistics({
    startTimestamp,
    endTimestamp,
    chunkCount: 1, // This processor handles 1 chunk per run
    chunkSize: processedRecordCount,
    totalRecords: processedRecordCount,
    sourceDescription: `chunk-${chunkId || 'unknown'}`,
    chunkId: chunkIdFromDesc, // Pass chunk ID to prevent overwrites
    ...(integrationResult && {
      successCount: integrationResult.successCount,
      failureCount: integrationResult.failureCount,
      skippedCount: integrationResult.skippedCount,
      ...pushedChangeCounts(integrationResult)
    })
  });

  const stats = errorTracker.getStatisticsSummary();
  console.log('\n=== Processing Statistics ===');
  console.log(`Total errors: ${stats.totalErrors}`);
  console.log(`Throttle events: ${stats.throttleCount}`);
  console.log(`Errors by status:`, stats.errorsByStatus);
}

/** Non-zero exit code if the tracker recorded any errors, matching both processors' original behavior. */
export function computeExitCode(errorTracker: TargetApiErrorEventProcessor | undefined): number {
  return (errorTracker instanceof TrackingTargetApiErrorProcessor && errorTracker.getStatisticsSummary().totalErrors > 0) ? 1 : 0;
}
