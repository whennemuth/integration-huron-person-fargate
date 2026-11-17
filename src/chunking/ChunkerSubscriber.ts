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
 * Every run start, an API event's or a dropped file's, also sets the processor service's live maximum tasks
 * (setProcessorCapacityForRunStart()): to the run's processorScalingCapacityOverride, or else back to the landscape's
 * maxScalingCapacity.
 *
 * Environment Variables:
 * - CHUNKER_QUEUE_URL: URL of the SQS queue for chunker tasks
 * - REGION: AWS region (e.g., 'us-east-2')
 * - DRY_RUN: If set to 'true', the function will not send messages (default: 'false')
 * - STACK_ID, LANDSCAPE: name the landscape's atomic counter table, for the counter reset
 * - CHUNKS_BUCKET_NAME: a mock landscape's chunks bucket, for its run state reset (resetState); not set elsewhere
 * - PROCESSOR_SERVICE_NAME, PROCESSOR_MAX_SCALING_CAPACITY: the processor service (in CHUNKER_CLUSTER_NAME's
 *   cluster) and the landscape's maxScalingCapacity, for setting its live maximum at a run start
 */

import { handleS3Event } from './filedrop/ChunkerS3Subscriber';
import { handleApiEvent } from './fetch/ChunkerApiSubscriber';
import { isRunStartEvent, resetRunCounters, RunCounterLocation } from './RunCounters';
import { ParallelStart } from './ParallelStart';
import { ProcessorScalingCapacity, ProcessorScalingCapacityResult } from './ProcessorScalingCapacity';
import { MockLandscapeLocation, MockLandscapeReset, MockLandscapeResetVia } from './MockLandscapeReset';
import { SourceEndpoint, SyncPopulation } from '../../docker/chunkTypes';
import type { StartedBy } from './StartedBy';
import { ProcessorCapacity, ProcessorCapacityRecord } from './ProcessorCapacityRecord';

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
  /**
   * The processor service's maximum tasks for this run (src/chunking/ProcessorScalingCapacity.ts), in place of the
   * landscape's maxScalingCapacity: the subscriber sets the service's live maximum to it at the run start. Every run
   * start without one sets the maximum back to maxScalingCapacity. Only on a run's first event; never forwarded to the
   * seeded or chained messages.
   */
  processorScalingCapacityOverride?: number;
  /**
   * The processor maximum the run started with (src/chunking/ProcessorCapacityRecord.ts), which the subscriber adds to
   * the run's first event once it has set it, and every seeded and chained message carries on, for the run's Flags.
   */
  processorScalingCapacity?: ProcessorCapacityRecord;
  processingMetadata?: {
    processedAt?: string;
    processorVersion?: string;
  };
}

/**
 * A mock landscape's reset on its own, not a run's start (the dashboard's Overview, Reset landscape): the chunker
 * subscriber empties the landscape's run state (MockLandscapeReset) and records it, within the same timeout as a run's
 * Start from scratch, and starts nothing.
 */
export interface MockLandscapeResetEvent {
  mockLandscapeReset: true;
  /** Who asked for it (the dashboard's signed-in user), recorded with the reset */
  startedBy?: StartedBy;
  /** When the dashboard sent it */
  requestedAt?: string;
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
  reset: (location: MockLandscapeLocation, via: MockLandscapeResetVia, startedBy?: StartedBy) => Promise<unknown> = resetMockLandscape
): Promise<boolean> {
  if (!event.resetState) {
    return false;
  }
  const { STACK_ID, LANDSCAPE, REGION, CHUNKS_BUCKET_NAME, DRY_RUN = 'false' } = env;
  if (DRY_RUN.toLowerCase() === 'true') {
    console.log(`[DRY RUN] Would reset the run state of mock landscape ${LANDSCAPE}`);
    return false;
  }
  await reset({ stackId: STACK_ID!, landscape: LANDSCAPE!, region: REGION!, chunksBucketName: CHUNKS_BUCKET_NAME! }, 'bulk-sync', event.startedBy);
  return true;
}

/** A mock landscape's reset, recorded in its atomic counter table (MockLandscapeReset.recorder()) */
export async function resetMockLandscape(location: MockLandscapeLocation, via: MockLandscapeResetVia, startedBy?: StartedBy): Promise<unknown> {
  return new MockLandscapeReset(MockLandscapeReset.awsDeps(location))
    .run({ via, startedBy, recorder: MockLandscapeReset.recorder(location) });
}

/** Why a reset on its own can't be done here, or undefined when it can: a mock landscape's, with its environment */
export function resetEventRefusal(env: NodeJS.ProcessEnv = process.env): string | undefined {
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
 * A mock landscape's reset on its own (MockLandscapeResetEvent): refused with a 400 outside a mock landscape (nothing
 * touched), skipped in DRY_RUN, otherwise run and recorded. A failed reset fails the invocation (not retried), after
 * recording the failure.
 */
export async function handleResetEvent(
  event: MockLandscapeResetEvent,
  env: NodeJS.ProcessEnv = process.env,
  reset: (location: MockLandscapeLocation, via: MockLandscapeResetVia, startedBy?: StartedBy) => Promise<unknown> = resetMockLandscape
): Promise<{ statusCode: number; body: string }> {
  const refusal = resetEventRefusal(env);
  if (refusal) {
    console.error(`Mock landscape reset refused: ${refusal}`);
    return { statusCode: 400, body: refusal };
  }
  const { STACK_ID, LANDSCAPE, REGION, CHUNKS_BUCKET_NAME, DRY_RUN = 'false' } = env;
  if (DRY_RUN.toLowerCase() === 'true') {
    const body = `[DRY RUN] Would reset the run state of mock landscape ${LANDSCAPE}`;
    console.log(body);
    return { statusCode: 200, body };
  }
  const result = await reset({ stackId: STACK_ID!, landscape: LANDSCAPE!, region: REGION!, chunksBucketName: CHUNKS_BUCKET_NAME! }, 'overview', event.startedBy);
  return { statusCode: 200, body: JSON.stringify(result) };
}

/**
 * Why a run's first event can't be started, checked before anything is reset or sent: its reset (resetRefusal()), its
 * parallel start (ParallelStart.refusal()), then its processor scaling capacity override
 * (ProcessorScalingCapacity.refusal()). Undefined when it can be.
 */
export function runStartRefusal(event: ApiChunkerEvent, env: NodeJS.ProcessEnv = process.env): string | undefined {
  return resetRefusal(event, env)
    ?? (ParallelStart.requested(event) > 0 ? ParallelStart.refusal(event, env) : undefined)
    ?? ProcessorScalingCapacity.refusal(event, env);
}

/**
 * At a run's start, set the processor service's live maximum tasks: to the run's processorScalingCapacityOverride
 * (already checked by runStartRefusal()), or else back to the landscape's maxScalingCapacity
 * (PROCESSOR_MAX_SCALING_CAPACITY). Called before anything is reset or sent, so a run whose override can't be set
 * fails its start (ProcessorScalingCapacity.apply() throws) having changed nothing; restoring the landscape's maximum
 * only warns. Logged, not set, in DRY_RUN.
 *
 * @param event The run's first event: an API event, or {} for a dropped file (which carries no override)
 * @returns What was set (the run records it: see processor-max-tasks-plan.md, step 3)
 */
export async function setProcessorCapacityForRunStart(
  event: Pick<ApiChunkerEvent, 'processorScalingCapacityOverride'> = {},
  env: NodeJS.ProcessEnv = process.env,
  capacity: (env: NodeJS.ProcessEnv) => ProcessorScalingCapacity = env => ProcessorScalingCapacity.fromEnvironment(env)
): Promise<ProcessorScalingCapacityResult> {
  return capacity(env).apply(event.processorScalingCapacityOverride);
}

/**
 * The Lambda handler function that dispatches based on event type.
 *
 * An API event is checked first (runStartRefusal(): a reset it asks for, a parallel start, a processor scaling capacity
 * override), before anything is reset or sent; a refusal is returned as a 400 rather than thrown. A run's first event
 * then sets the processor's live maximum (setProcessorCapacityForRunStart()). Then a mock landscape's run state is
 * reset if the event asks for it (resetState). An event asking for a parallel start (parallelStart > 0) is started with
 * that many chunker tasks, the wait inside bounded by the Lambda's remaining time (context); any other is queued as one
 * message. A dropped file (an S3 event) sets the processor's maximum back to the landscape's before its run is queued.
 *
 * @param event - Either an S3ChunkerEvent or ApiChunkerEvent
 * @param context - The Lambda context (its getRemainingTimeInMillis() bounds a parallel start's wait)
 * @returns Response with status code and message
 */
export async function handler(event: S3ChunkerEvent | ApiChunkerEvent | MockLandscapeResetEvent, context?: { getRemainingTimeInMillis?: () => number }): Promise<any> {
  console.log('Received event:', JSON.stringify(event, null, 2));

  if (isResetEvent(event)) {
    console.log('Event type: mock landscape reset');
    return await handleResetEvent(event);
  }
  // Determine event type by checking for API-specific fields
  if (isApiEvent(event)) {
    console.log('Event type: API-based chunking');
    let apiEvent = event as ApiChunkerEvent;
    const refusal = runStartRefusal(apiEvent);
    if (refusal) {
      console.error(`Run start refused: ${refusal}`);
      return { statusCode: 400, body: refusal };
    }
    if (isRunStartEvent(apiEvent)) {
      apiEvent = withProcessorCapacity(apiEvent, await setProcessorCapacityForRunStart(apiEvent));
    }
    await resetMockLandscapeForRunStart(apiEvent);
    if (ParallelStart.requested(apiEvent) > 0) {
      return await startInParallel(apiEvent, context);
    }
    await resetCountersForRunStart(apiEvent);
    return await handleApiEvent(apiEvent);
  } else if (isS3Event(event)) {
    console.log('Event type: S3-based chunking');
    return await handleS3Event(event as S3ChunkerEvent, () => setProcessorCapacityForRunStart());
  } else {
    const errMsg = 'Invalid event structure: Must contain either (s3Path/bucket/key) or (baseUrl/fetchPath/populationType)';
    console.error(errMsg);
    return { statusCode: 400, body: errMsg };
  }
}

/**
 * The run's first event carrying the processor maximum its start set (ProcessorCapacity.record()), for the run's
 * Flags; the event as it was when nothing was set
 */
export function withProcessorCapacity(event: ApiChunkerEvent, result: ProcessorScalingCapacityResult): ApiChunkerEvent {
  const processorScalingCapacity = ProcessorCapacity.record(result);
  return processorScalingCapacity ? { ...event, processorScalingCapacity } : event;
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

/** Type guard: a mock landscape's reset on its own */
function isResetEvent(event: any): event is MockLandscapeResetEvent {
  return event?.mockLandscapeReset === true;
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
