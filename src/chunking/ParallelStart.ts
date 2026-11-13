import { DesiredCount } from '../DesiredCount';
import { MetricsCatchupDelay } from '../runner/MetricsCatchupDelay';
import { QueueSeeder } from './fetch/QueueSeeder';
import { isRunStartEvent } from './RunCounters';
import type { ApiChunkerEvent } from './ChunkerSubscriber';

/** What a seeded run start does in AWS, injectable so it can be tested without AWS */
export type ParallelStartDeps = {
  /** The chunker service's most tasks (its auto-scaling maximum), undefined if it can't be read */
  maxTasks: () => Promise<number | undefined>;
  /** Send the run's first n chunker messages, at offsets iterationLimit apart; resolves to how many were sent */
  seed: (n: number) => Promise<number>;
  /** Wait, at most maxWaitSeconds, for the chunker's scale-in alarm to leave ALARM; resolves to whether it did */
  waitForAlarm: (maxWaitSeconds?: number) => Promise<boolean>;
  /** Set the chunker service's desired count */
  setDesiredCount: (count: number) => Promise<void>;
};

/** What a seeded run start did */
export type ParallelStartResult = {
  statusCode: number;
  body: string;
  requested: number;
  seeded?: number;
  desiredCount?: number;
  alarmSettled?: boolean;
  dryRun?: boolean;
};

/** How many chunker messages to seed and tasks to start with, and the time left to do it in */
export type ParallelStartParams = {
  /** Chunker messages to seed, at offsets iterationLimit apart (at least 1) */
  messages: number;
  /** The chunker service's desired count afterward, no more than messages; 0 seeds without waiting or scaling */
  tasks: number;
  /** The caller's remaining time (a Lambda's context.getRemainingTimeInMillis()), which bounds the wait */
  remainingMs?: number;
};

/**
 * A run started with several chunker tasks at once: seed chunker messages, wait for the chunker's scale-in alarm to
 * catch up with the queue (MetricsCatchupDelay: without the wait, auto-scaling can take the new desired count straight
 * back to 0, as on 2026-07-02), then set the chunker service's desired count. The one procedure for both ways to do
 * it, with the counters already reset for the run:
 * - the chunker subscriber, for a run's first event with parallelStart (the dashboard's Bulk sync): N messages and N
 *   tasks (run()), its wait bounded by the Lambda's remaining time;
 * - the manual Runner's queue seeding (RunnerForQueueSeeding): MESSAGES_TO_PREPOPULATE messages and DESIRED_COUNT
 *   tasks (start()), where 0 tasks seeds only.
 *
 * The service's maximum is checked before anything is sent. The wait is bounded by the remaining time, less
 * WAIT_MARGIN_SECONDS, so a Lambda ends by itself (logging that the alarm hadn't settled) rather than being stopped by
 * its timeout after seeding but before scaling. If the alarm hasn't settled, the desired count is still set: the seeded
 * messages are on the queue either way, and auto-scaling adds tasks for them as it would for one.
 *
 * Only a run's first event seeds; the seeded messages (each with the run's chunkDirectory) and the chunker's chained
 * messages never carry parallelStart. See queue-seeding-plan.md.
 */
export class ParallelStart {
  /** The most chunker messages a start may seed, whatever the service allows (Bulk sync's PARALLEL_START_MAX) */
  static readonly MAX = 20;
  /** Time left after the wait, for setting the desired count and returning */
  static readonly WAIT_MARGIN_SECONDS = 60;

  constructor(private readonly deps: ParallelStartDeps, private readonly dryRun = false) {}

  /** The number of chunker messages an event asks to start with: 0 for one message, as without parallel start */
  static requested(event: ApiChunkerEvent): number {
    const value = event.parallelStart;
    return typeof value === 'number' && value > 0 ? value : 0;
  }

  /**
   * Why an event's parallel start can't be done, or undefined. Checked before the counters are reset or anything is
   * sent: the subscriber can't trust its caller to have checked (Bulk sync does too).
   */
  static refusal(event: ApiChunkerEvent, env: NodeJS.ProcessEnv): string | undefined {
    const n = ParallelStart.requested(event);
    if (!Number.isInteger(n) || n < 1 || n > ParallelStart.MAX) {
      return `parallelStart must be a whole number from 1 to ${ParallelStart.MAX} (got ${event.parallelStart})`;
    }
    if (!isRunStartEvent(event)) {
      return "parallelStart is only for a run's first event (offset 0, no chunkDirectory)";
    }
    if (!((event.iterationLimit ?? 0) > 0)) {
      return 'parallelStart needs chunks per task (iterationLimit) above 0: seeded offsets are that far apart, so a single task can\'t be seeded';
    }
    if (/[?&]buid=/.test(event.fetchPath)) {
      return 'parallelStart is not for a one-person source: every seeded task would fetch the same person';
    }
    const missing = ['CHUNKER_QUEUE_URL', 'REGION', 'STACK_ID', 'LANDSCAPE', 'CHUNKER_CLUSTER_NAME', 'CHUNKER_SERVICE_NAME']
      .filter(name => !env[name]);
    if (missing.length > 0) {
      return `A parallel start needs ${missing.join(', ')} in the chunker subscriber's environment`;
    }
    return undefined;
  }

  /**
   * The AWS side of a parallel start: the chunker service's maximum and desired count (DesiredCount, made only if a
   * task count needs it), the scale-in alarm (MetricsCatchupDelay), and the caller's own seeding.
   */
  static awsDeps(service: { clusterName?: string; serviceName?: string; region?: string }, seed: (n: number) => Promise<number>): ParallelStartDeps {
    let desiredCount: DesiredCount | undefined;
    const chunkerService = () => (desiredCount = desiredCount ?? new DesiredCount(service));
    return {
      maxTasks: () => chunkerService().getMax(),
      seed,
      waitForAlarm: (maxWaitSeconds?: number) => new MetricsCatchupDelay({ ...service, maxWaitSeconds }).startDelay(),
      setDesiredCount: (count: number) => chunkerService().setTo(count)
    };
  }

  /** The AWS-backed parallel start for an event, from the chunker subscriber's environment */
  static fromEnvironment(event: ApiChunkerEvent, env: NodeJS.ProcessEnv = process.env): ParallelStart {
    const {
      CHUNKER_QUEUE_URL: queueUrl, REGION: region, STACK_ID: stackId, LANDSCAPE: landscape,
      CHUNKER_CLUSTER_NAME: clusterName, CHUNKER_SERVICE_NAME: serviceName, DRY_RUN = 'false'
    } = env;
    const seed = (n: number) => new QueueSeeder({
      stackId, region, landscape, queueUrl: queueUrl!,
      baseUrl: event.baseUrl, fetchPath: event.fetchPath, populationType: event.populationType,
      bulkReset: event.bulkReset, trustPreviousStorage: event.trustPreviousStorage,
      iterationLimit: event.iterationLimit!, messagesToSeed: n,
      personRecordProcessorAddons: event.personRecordProcessorAddons, termsEndpoint: event.termsEndpoint,
      startedBy: event.startedBy
    }).seedQueue();
    return new ParallelStart(ParallelStart.awsDeps({ clusterName, serviceName, region }, seed), DRY_RUN.toLowerCase() === 'true');
  }

  /**
   * The chunker subscriber's parallel start: as many tasks as messages (the event's parallelStart). The counters must
   * already be reset for this run (resetCountersForRunStart()).
   * @param remainingMs The Lambda's remaining time (context.getRemainingTimeInMillis()), which bounds the wait
   */
  run(event: ApiChunkerEvent, remainingMs?: number): Promise<ParallelStartResult> {
    const requested = ParallelStart.requested(event);
    return this.start({ messages: requested, tasks: requested, remainingMs });
  }

  /** Seed, then (for a task count above 0) wait for the scale-in alarm and set the desired count */
  async start({ messages, tasks, remainingMs }: ParallelStartParams): Promise<ParallelStartResult> {
    const requested = messages;
    if (!Number.isInteger(messages) || messages < 1 || !Number.isInteger(tasks) || tasks < 0 || tasks > messages) {
      const body = `A parallel start needs at least 1 message and from 0 to that many tasks (got ${messages} messages, ${tasks} tasks); nothing was sent`;
      console.error(body);
      return { statusCode: 400, body, requested };
    }
    if (this.dryRun) {
      const body = `[DRY RUN] Would seed ${messages} chunker messages` +
        (tasks > 0 ? `, wait for the scale-in alarm, and set the desired count to ${tasks}` : '');
      console.log(body);
      return { statusCode: 200, body, requested, dryRun: true };
    }

    // Before anything is sent: a start the service can't run that many tasks for is refused
    if (tasks > 0) {
      const max = await this.deps.maxTasks();
      if (max !== undefined && tasks > max) {
        const body = `${tasks} chunker tasks is more than the chunker service's maximum of ${max}; nothing was sent`;
        console.error(body);
        return { statusCode: 400, body, requested };
      }
    }

    console.log(`Seeding ${messages} chunker messages${tasks > 0 ? `, to start ${tasks} chunker tasks` : ''}`);
    const seeded = await this.deps.seed(messages);
    if (seeded === 0) {
      const body = `None of the ${messages} chunker messages could be sent; the run didn't start`;
      console.error(body);
      return { statusCode: 500, body, requested, seeded };
    }
    if (tasks === 0) {
      const body = `Seeded ${seeded} of ${messages} chunker messages; auto-scaling starts their tasks`;
      console.log(body);
      return { statusCode: 200, body, requested, seeded, desiredCount: 0 };
    }

    const maxWaitSeconds = remainingMs === undefined ? undefined : Math.floor(remainingMs / 1000) - ParallelStart.WAIT_MARGIN_SECONDS;
    const alarmSettled = await this.deps.waitForAlarm(maxWaitSeconds);
    if (!alarmSettled) {
      console.warn(`The chunker's scale-in alarm hadn't left ALARM${maxWaitSeconds === undefined ? '' : ` within ${Math.max(0, maxWaitSeconds)}s`}; ` +
        `setting the desired count anyway: if auto-scaling takes it back, it adds tasks for the ${seeded} queued messages as it would for one`);
    }

    // No more tasks than messages actually sent
    const desiredCount = Math.min(tasks, seeded);
    await this.deps.setDesiredCount(desiredCount);
    const body = `Seeded ${seeded} of ${messages} chunker messages and set the desired count to ${desiredCount}` +
      (alarmSettled ? '' : " (the scale-in alarm hadn't settled)");
    console.log(body);
    return { statusCode: 200, body, requested, seeded, desiredCount, alarmSettled };
  }
}
