/**
 * Chunker Subscriber Entry Point
 * 
 * This Lambda function acts as a dispatcher for chunker Fargate tasks.
 * It receives events from two possible sources:
 * 
 * 1. **S3-based events**: Triggered when a large JSON file is uploaded to the input S3 bucket
 *    Event shape: { s3Path, bucket, key, processingMetadata }
 * 
 * 2. **API-based events**: Triggered by EventBridge schedule for periodic API fetching
 *    Event shape: { baseUrl, fetchPath, populationType, processingMetadata }
 * 
 * Based on the event shape, this function delegates to the appropriate handler:
 * - S3 events → ChunkerS3Subscriber
 * - API events → ChunkerApiSubscriber
 * 
 * Both handlers send messages to the chunker SQS queue, which triggers the
 * QueueProcessingFargateService to auto-scale and process the data.
 * 
 * Architecture:
 * 1. Event (S3 upload or EventBridge schedule) triggers this Lambda
 * 2. Lambda analyzes event structure and delegates to appropriate handler
 * 3. Handler sends message to SQS queue with task parameters
 * 4. QueueProcessingFargateService detects queue depth increase
 * 5. Service auto-scales up (increases desired count)
 * 6. ECS launches Fargate task(s)
 * 7. Task reads message from queue and processes the data
 * 8. Queue depth decreases, service scales back down
 * 
 * An API event that starts a run (the schedule's, the dashboard's Bulk sync) first resets the chunker's counters
 * (resetCountersForRunStart()), so every run that starts through this Lambda starts them from zero.
 *
 * Environment Variables:
 * - CHUNKER_QUEUE_URL: URL of the SQS queue for chunker tasks
 * - REGION: AWS region (e.g., 'us-east-2')
 * - DRY_RUN: If set to 'true', the function will not send messages (default: 'false')
 * - STACK_ID, LANDSCAPE: name the landscape's atomic counter table, for the counter reset
 * - CHUNKS_BUCKET_NAME: a mock landscape's chunks bucket, for its run state reset (resetState); not set elsewhere
 */

import { handleS3Event } from './filedrop/ChunkerS3Subscriber';
import { handleApiEvent } from './fetch/ChunkerApiSubscriber';
import { isRunStartEvent, resetRunCounters, RunCounterLocation } from './RunCounters';
import { ParallelStart } from './ParallelStart';
import { MockLandscapeLocation, MockLandscapeReset } from './MockLandscapeReset';
import { SourceEndpoint, SyncPopulation } from '../../docker/chunkTypes';
import type { StartedBy } from './StartedBy';

/**
 * S3-based event structure
 */
export interface S3ChunkerEvent {
  s3Path?: string;
  bucket?: string;
  key?: string;
  processingMetadata?: {
    processedAt?: string;
    processorVersion?: string;
  };
}

/**
 * API-based event structure
 */
export interface ApiChunkerEvent {
  baseUrl: string;
  fetchPath: string;
  populationType: SyncPopulation;
  bulkReset?: boolean;
  trustPreviousStorage?: boolean;
  iterationLimit?: number;
  offset?: number;
  chunkDirectory?: string;
  /**
   * Comma-delimited list of personRecordProcessor Addon enum keys (see
   * src/processing/addon/AbstractAddonPersonProcessor.ts) to activate for this run.
   */
  personRecordProcessorAddons?: string;
  /**
   * Terms data source endpoint for this run, overriding the config's dataSource.terms baseUrl and
   * fetchPath (not its apiKey) in the processor tasks. Set only when the source is the source
   * simulator, so a simulated run also gets simulated terms. Absent otherwise, including hybrid runs
   * (real source -> mock target), which use the configured terms endpoint.
   */
  termsEndpoint?: SourceEndpoint;
  /** Who or what started the run (src/chunking/StartedBy.ts), recorded in its Flags and summary for Run history */
  startedBy?: StartedBy;
  /**
   * Start the run with this many chunker tasks at once (src/chunking/ParallelStart.ts): seed this many messages, wait
   * for the chunker's scale-in alarm, and set the service's desired count. Only on a run's first event; absent or 0
   * starts with one message. Never forwarded to the seeded or chained messages.
   */
  parallelStart?: number;
  /**
   * Start the run from scratch in a mock landscape (src/chunking/MockLandscapeReset.ts): empty the mock target, the
   * statistics (Run history) and the delta baseline before the run starts. Only on a run's first event, and refused
   * outside a mock landscape. Never forwarded to the seeded or chained messages.
   */
  resetState?: boolean;
  processingMetadata?: {
    processedAt?: string;
    processorVersion?: string;
  };
}

/**
 * Before an API event that starts a run (isRunStartEvent()) is queued, reset the chunker's counters, as the manual
 * Runner does before it sends one. Here rather than in handleApiEvent(), which the Runner's queue seeding calls
 * for every seeded message, taking their offsets from the counter it has just reset. Skipped in DRY_RUN (nothing
 * starts), and, with a warning, when the environment doesn't name the counter table (a Lambda deployed before
 * this). A failed reset fails the invocation, so the run isn't queued against stale counters.
 *
 * @returns Whether the counters were reset
 */
export async function resetCountersForRunStart(
  event: ApiChunkerEvent,
  env: NodeJS.ProcessEnv = process.env,
  reset: (location: RunCounterLocation) => Promise<void> = resetRunCounters
): Promise<boolean> {
  if (!isRunStartEvent(event)) {
    return false;
  }
  const { STACK_ID, LANDSCAPE, REGION, DRY_RUN = 'false' } = env;
  if (DRY_RUN.toLowerCase() === 'true') {
    return false;
  }
  if (!STACK_ID || !LANDSCAPE || !REGION) {
    console.warn("Not resetting the chunker's counters for this run's start: STACK_ID, LANDSCAPE or REGION isn't set");
    return false;
  }
  await reset({ stackId: STACK_ID, region: REGION, landscape: LANDSCAPE });
  return true;
}

/** The environment a mock landscape's reset needs: its tables' names and its chunks bucket */
const RESET_ENVIRONMENT = ['STACK_ID', 'LANDSCAPE', 'REGION', 'CHUNKS_BUCKET_NAME'];

/**
 * Why an event's reset (ApiChunkerEvent.resetState) can't be done, or undefined when it can or none is asked for:
 * only on a run's first event, only in a mock landscape (the Lambda's LANDSCAPE), and only with the environment the
 * reset needs (CDK gives a mock landscape's subscriber CHUNKS_BUCKET_NAME and the grants, and no other's).
 */
export function resetRefusal(event: ApiChunkerEvent, env: NodeJS.ProcessEnv = process.env): string | undefined {
  if (!event.resetState) {
    return undefined;
  }
  if (!isRunStartEvent(event)) {
    return "resetState is only for a run's first event (offset 0, no chunkDirectory)";
  }
  const notMock = MockLandscapeReset.refusal(env.LANDSCAPE);
  if (notMock) {
    return notMock;
  }
  const missing = RESET_ENVIRONMENT.filter(name => !env[name]);
  return missing.length > 0
    ? `Can't reset the mock landscape's run state: ${missing.join(', ')} isn't set in this Lambda's environment`
    : undefined;
}

/**
 * Before a run that asks for it starts (ApiChunkerEvent.resetState, already checked by resetRefusal()), reset the
 * mock landscape's run state: the mock target, the statistics and the delta baseline (MockLandscapeReset). It comes
 * before the counter reset and before anything is queued, so the run starts from scratch. Skipped in DRY_RUN
 * (nothing starts). A failed reset fails the invocation, which isn't retried, so the run doesn't start.
 *
 * @returns Whether the run state was reset
 */
export async function resetMockLandscapeForRunStart(
  event: ApiChunkerEvent,
  env: NodeJS.ProcessEnv = process.env,
  reset: (location: MockLandscapeLocation) => Promise<unknown> = location => new MockLandscapeReset(MockLandscapeReset.awsDeps(location)).run()
): Promise<boolean> {
  if (!event.resetState) {
    return false;
  }
  const { STACK_ID, LANDSCAPE, REGION, CHUNKS_BUCKET_NAME, DRY_RUN = 'false' } = env;
  if (DRY_RUN.toLowerCase() === 'true') {
    console.log(`[DRY RUN] Would reset the run state of mock landscape ${LANDSCAPE}`);
    return false;
  }
  await reset({ stackId: STACK_ID!, landscape: LANDSCAPE!, region: REGION!, chunksBucketName: CHUNKS_BUCKET_NAME! });
  return true;
}

/**
 * Why a run's first event can't be started, checked before anything is reset or sent: its reset (resetRefusal()), then
 * its parallel start (ParallelStart.refusal()). Undefined when it can be.
 */
export function runStartRefusal(event: ApiChunkerEvent, env: NodeJS.ProcessEnv = process.env): string | undefined {
  return resetRefusal(event, env)
    ?? (ParallelStart.requested(event) > 0 ? ParallelStart.refusal(event, env) : undefined);
}

/**
 * The Lambda handler function that dispatches based on event type.
 *
 * An API event is checked first (runStartRefusal(): a reset it asks for, then a parallel start), before anything is
 * reset or sent; a refusal is returned as a 400 rather than thrown. Then a mock landscape's run state is reset if the
 * event asks for it (resetState). An event asking for a parallel start (parallelStart > 0) is started with that many
 * chunker tasks, the wait inside bounded by the Lambda's remaining time (context); any other is queued as one message.
 *
 * @param event - Either an S3ChunkerEvent or ApiChunkerEvent
 * @param context - The Lambda context (its getRemainingTimeInMillis() bounds a parallel start's wait)
 * @returns Response with status code and message
 */
export async function handler(event: S3ChunkerEvent | ApiChunkerEvent, context?: { getRemainingTimeInMillis?: () => number }): Promise<any> {
  console.log('Received event:', JSON.stringify(event, null, 2));

  // Determine event type by checking for API-specific fields
  if (isApiEvent(event)) {
    console.log('Event type: API-based chunking');
    const apiEvent = event as ApiChunkerEvent;
    const refusal = runStartRefusal(apiEvent);
    if (refusal) {
      console.error(`Run start refused: ${refusal}`);
      return { statusCode: 400, body: refusal };
    }
    await resetMockLandscapeForRunStart(apiEvent);
    if (ParallelStart.requested(apiEvent) > 0) {
      return await startInParallel(apiEvent, context);
    }
    await resetCountersForRunStart(apiEvent);
    return await handleApiEvent(apiEvent);
  } else if (isS3Event(event)) {
    console.log('Event type: S3-based chunking');
    return await handleS3Event(event as S3ChunkerEvent);
  } else {
    const errMsg = 'Invalid event structure: Must contain either (s3Path/bucket/key) or (baseUrl/fetchPath/populationType)';
    console.error(errMsg);
    return { statusCode: 400, body: errMsg };
  }
}

/**
 * A run's start with several chunker tasks: refused (nothing reset or sent) if it can't be done, otherwise the counters
 * reset and then the parallel start (ParallelStart.run()). A refusal is returned, not thrown: an asynchronous
 * invocation that throws is retried, and a refused start would only be refused again.
 */
export async function startInParallel(
  event: ApiChunkerEvent,
  context?: { getRemainingTimeInMillis?: () => number },
  env: NodeJS.ProcessEnv = process.env,
  parallelStart: (event: ApiChunkerEvent) => ParallelStart = event => ParallelStart.fromEnvironment(event, env),
  reset: (location: RunCounterLocation) => Promise<void> = resetRunCounters
): Promise<any> {
  const refusal = ParallelStart.refusal(event, env);
  if (refusal) {
    console.error(`Parallel start refused: ${refusal}`);
    return { statusCode: 400, body: refusal };
  }
  await resetCountersForRunStart(event, env, reset);
  return await parallelStart(event).run(event, context?.getRemainingTimeInMillis?.());
}

/**
 * Type guard to determine if event is an API-based event
 */
function isApiEvent(event: any): event is ApiChunkerEvent {
  return !!(event.baseUrl && event.fetchPath && event.populationType);
}

/**
 * Type guard to determine if event is an S3-based event
 */
function isS3Event(event: any): event is S3ChunkerEvent {
  return !!(event.s3Path || (event.bucket && event.key));
}
