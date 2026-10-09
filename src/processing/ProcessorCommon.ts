/**
 * Shared logic between ProcessorForS3.ts and ProcessorForDynamoDb.ts.
 *
 * Both processors run the same chunk-processing lifecycle around HuronPersonIntegration -
 * only how each stores delta state and signals merger completion differs (S3 marker files vs
 * DynamoDB CHUNK_STATUS/SQS). This module holds the parts that are identical between them, so
 * fixes/changes to shared behavior only need to be made once.
 */

import { humanReadableFromMilliseconds, IntegrationResult } from 'integration-core';
import { PersonRecordProcessor, TargetApiErrorEventProcessor } from 'integration-huron-person';
import { SyncPopulation } from '../../docker/chunkTypes';
import { LoggingTargetApiErrorProcessor, TrackingTargetApiErrorProcessor } from '../ApiErrorTracking';
import { NextChunk, QueueReader } from '../Queue';
import { Flags } from '../chunking/metadata/IMetadataStorage';
import { personRecordProcessorFactory } from './custom/PersonRecordProcessorFactory';

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
 * resolved from this run's Flags (flags.personRecordProcessorCustomizations) - a no-op if neither is present.
 */
export async function resolveCustomPersonProcessor(
  personRecordProcessor: PersonRecordProcessor | undefined,
  customizations: string | undefined
): Promise<PersonRecordProcessor | undefined> {
  return personRecordProcessor
    ?? (await personRecordProcessorFactory(customizations))?.processRecord;
}

/** Logs HuronPersonIntegration.run()'s result and warns if the success/delta math doesn't add up. */
export function logIntegrationResult(result: IntegrationResult): void {
  console.log(`\n✓ Chunk integration completed with results:`);
  console.log(`  - Total Processed: ${result.totalProcessed}`);
  console.log(`  - ✓ Successful: ${result.successCount}`);
  console.log(`  - ✗ Failed: ${result.failureCount}`);
  console.log(`  - ⊘ Skipped: ${result.skippedCount}`);
  console.log(`  - + Added: ${result.addedCount}`);
  console.log(`  - ~ Updated: ${result.updatedCount}`);
  console.log(`  - - Removed: ${result.removedCount}`);
  console.log(`  - ⧗ Duration: ${humanReadableFromMilliseconds(result.duration ?? 0)}`);

  const deltaSum = result.addedCount + result.updatedCount + result.removedCount;
  if (result.successCount !== deltaSum) {
    console.warn(`  ⚠️  Math mismatch: Successful(${result.successCount}) should equal Added(${result.addedCount}) + Updated(${result.updatedCount}) + Removed(${result.removedCount}) = ${deltaSum}`);
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
  processedRecordCount: number
}): Promise<void> {
  const { errorTracker, startTimestamp, chunkId, processedRecordCount } = params;
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
    chunkId: chunkIdFromDesc // Pass chunk ID to prevent overwrites
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
