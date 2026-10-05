import { GetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";
import { TestEnvironment } from "integration-core";
import { ConfigManager } from "integration-huron-person";
import { DesiredCount } from "../../DesiredCount";
import { MetadataBroker } from "../metadata";
import { MetricsCatchupDelay } from "../../runner/MetricsCatchupDelay";
import { getLocalConfig } from "../../Utils";

export type ProcessorServiceBoosterParams = {
  clusterName?: string;
  serviceName?: string;
  queueUrl?: string;
  region?: string;
  /** Identifies which parallel chunker task/offset is running this check, so its log stream can be found later if it wins the claim. */
  claimedByChunk?: string;
};

/**
 * "Hit the ground running" for the processor service: while the chunker phase is producing
 * chunks (and therefore processor queue messages) faster than the processor's own alarm-driven
 * autoscaling can react to, jump the processor's desiredCount straight to its auto-scaling max
 * instead of waiting for the usual +1-per-alarm-period ramp.
 *
 * Cheap current/max/backlog checks run on every tick from every concurrent chunker task's own
 * timer (self-guarding: once desiredCount already equals max, every check is a no-op). Only the
 * expensive part - waiting out MetricsCatchupDelay and then calling setTo() - is deduplicated via
 * a metadata-backed claim, so multiple tasks don't all independently poll the scale-in alarm at
 * once. Application Auto Scaling applies step-scaling ChangeInCapacity adjustments relative to
 * its OWN remembered desiredCount, not by re-querying ECS - so boosting while the processor's
 * scale-in alarm is still stuck in a stale ALARM state (e.g. right after a cold start from an
 * empty queue) risks the very next alarm evaluation reverting the boost. MetricsCatchupDelay
 * guards against that. If the claiming task fails to complete (error, the delay gives up while
 * still ALARM, or the delay is aborted because the caller's own work finished first), the claim
 * is released so another task can retry later.
 */
export class ProcessorServiceBooster {
  private readonly abortController = new AbortController();
  /** True between winning a claim and either releasing it or boosting (a successful boost keeps its claim). */
  private claimNeedsRelease = false;
  private readonly desiredCount?: DesiredCount;
  private readonly queueUrl?: string;
  private readonly region?: string;
  private readonly clusterName?: string;
  private readonly serviceName?: string;
  private readonly claimedByChunk?: string;

  constructor(private metadataBroker: MetadataBroker, params?: ProcessorServiceBoosterParams) {
    if (!ProcessorServiceBooster.isEnabled()) {
      console.log('ProcessorServiceBooster disabled via BOOST_PROCESSOR=false - boosting skipped.');
      return;
    }

    const {
      clusterName = process.env.ECS_CLUSTER_NAME,
      serviceName = process.env.PROCESSOR_ECS_SERVICE_NAME,
      queueUrl = process.env.PROCESSOR_QUEUE_URL,
      region = process.env.REGION,
      claimedByChunk,
    } = params ?? {};

    this.queueUrl = queueUrl;
    this.region = region;
    this.clusterName = clusterName;
    this.serviceName = serviceName;
    this.claimedByChunk = claimedByChunk;

    if (!clusterName || !serviceName || !queueUrl || !region) {
      console.warn('⚠️  ProcessorServiceBooster missing configuration (clusterName/serviceName/queueUrl/region) - boosting disabled.');
      return;
    }

    this.desiredCount = new DesiredCount({ clusterName, serviceName, region });
  }

  /** Feature flag, defaults to enabled unless explicitly set to 'false' (e.g. BOOST_PROCESSOR=false). */
  private static isEnabled = (): boolean => process.env.BOOST_PROCESSOR?.toLowerCase() !== 'false';

  /** Cut short any in-progress catch-up delay so its claim is released instead of orphaned. */
  public abort = (): void => this.abortController.abort();

  private releaseClaim = async (): Promise<void> => {
    await this.metadataBroker.releaseProcessorBoostClaim();
    this.claimNeedsRelease = false;
    console.log('  ✓ Processor boost claim released - available for another task to retry.');
  }

  /**
   * Stop-time safety net: release this booster's claim if it still holds one (e.g. the in-flight
   * check's own release failed). Never touches a claim held by another task.
   */
  public releaseClaimIfHeld = async (): Promise<void> => {
    if (!this.claimNeedsRelease) {
      return;
    }
    try {
      await this.releaseClaim();
    } catch (error: any) {
      console.warn(`⚠️  Failed to release processor boost claim at shutdown (it will expire as stale): ${error.message}`);
    }
  }

  private getApproximateMessageCount = async (): Promise<number | undefined> => {
    const { queueUrl, region } = this;
    if (!queueUrl) {
      return undefined;
    }

    const client = new SQSClient({ region });
    const response = await client.send(new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ['ApproximateNumberOfMessages']
    }));

    const count = response.Attributes?.ApproximateNumberOfMessages;
    return count === undefined ? undefined : parseInt(count, 10);
  }

  /**
   * Boost the processor service to its max desired count if warranted: desiredCount is below max,
   * and the processor queue's backlog is more than double the max task count. The expensive
   * delay+setTo work is gated behind a claim so concurrent tasks don't duplicate it.
   */
  public checkAndBoostIfNeeded = async (): Promise<void> => {
    const { desiredCount, getApproximateMessageCount, metadataBroker, clusterName, serviceName, region, claimedByChunk } = this;
    const { signal } = this.abortController;
    if (!desiredCount) {
      return;
    }

    try {
      const [current, max, messageCount] = await Promise.all([
        desiredCount.getCurrent(),
        desiredCount.getMax(),
        getApproximateMessageCount()
      ]);

      if (current === undefined || max === undefined || messageCount === undefined) {
        return;
      }

      if (current >= max) {
        return; // Already boosted (or otherwise at/above max) - nothing to do
      }

      if (messageCount <= max * 2) {
        return; // Backlog not yet large enough to justify jumping straight to max
      }

      if (signal.aborted) {
        return; // Caller is shutting down - don't take a claim we can't see through
      }

      const claimed = await metadataBroker.claimProcessorBoost(claimedByChunk);
      if (!claimed) {
        console.log('  Processor boost already claimed by another task - skipping.');
        return;
      }
      this.claimNeedsRelease = true;

      let boosted = false;
      try {
        console.log(`\n🚀 Processor queue backlog (${messageCount}) exceeds 2x max capacity (${max}) `
          + `while desiredCount (${current}) is below max - claim won, waiting for scale-in alarm to catch up...`);

        const alarmCleared = await new MetricsCatchupDelay({ clusterName, serviceName, region }).startDelay(signal);
        if (!alarmCleared) {
          if (signal.aborted) {
            console.warn('  Chunking finished before scale-in alarm cleared - abandoning boost; releasing claim for another task to retry.');
          } else {
            console.warn('  Scale-in alarm still ALARM after catch-up delay - not safe to boost yet; releasing claim for a later attempt.');
          }
          return;
        }

        await desiredCount.setTo(max);
        boosted = true;
        this.claimNeedsRelease = false;
      } finally {
        if (!boosted) {
          await this.releaseClaim();
        }
      }
    } catch (error: any) {
      console.warn(`⚠️  ProcessorServiceBooster check failed (non-fatal, continuing): ${error.message}`);
    }
  }

  /**
   * Run checkAndBoostIfNeeded() on a fixed wall-clock interval, decoupled from chunk-write
   * cadence, for as long as chunking is actively fetching/writing. Returns a stop function that
   * MUST be awaited (e.g. in a finally block) once the caller's own chunking work concludes: it
   * stops the interval, aborts any in-flight catch-up delay, and resolves only once that check
   * has released its claim - so a subsequent process.exit() can't orphan the claim.
   */
  public static startPeriodicCheck = (metadataBroker: MetadataBroker, intervalSeconds: number = 60, params?: ProcessorServiceBoosterParams): (() => Promise<void>) => {
    if (!ProcessorServiceBooster.isEnabled()) {
      console.log('ProcessorServiceBooster disabled via BOOST_PROCESSOR=false - periodic check not started.');
      return async () => {};
    }

    const booster = new ProcessorServiceBooster(metadataBroker, params);
    const inFlight = new Set<Promise<void>>();
    const intervalId = setInterval(() => {
      const check: Promise<void> = booster.checkAndBoostIfNeeded().finally(() => inFlight.delete(check));
      inFlight.add(check);
    }, intervalSeconds * 1000);

    return async () => {
      console.log(`Stopping processor booster periodic check (${inFlight.size} check(s) in flight) - `
        + 'aborting any catch-up delay and releasing this task\'s boost claim if it holds one...');
      clearInterval(intervalId);
      booster.abort();
      await Promise.all(inFlight);
      await booster.releaseClaimIfHeld();
      console.log('Processor booster periodic check stopped.');
    };
  }

  /** One-shot check - convenience for a single call site (e.g. the race-winner's own final check). */
  public static boostIfNeeded = async (metadataBroker: MetadataBroker, params?: ProcessorServiceBoosterParams): Promise<void> => {
    await new ProcessorServiceBooster(metadataBroker, params).checkAndBoostIfNeeded();
  }
}

if (require.main === module) {
  (async () => {
    const testEnvironment = TestEnvironment('PROCESSOR_SERVICE_BOOSTER');
    [
      'ECS_CLUSTER_NAME',
      'PROCESSOR_ECS_SERVICE_NAME',
      'PROCESSOR_QUEUE_URL',
      'REGION',
      'CHUNKS_BUCKET',
      'CHUNK_DIRECTORY',
      'HURON_PERSON_CONFIG_PATH',
      'SECRET_ARN',
      'DRY_RUN',
      'BOOST_PROCESSOR'
    ].forEach(testEnvironment.getVar);

    const { CHUNKS_BUCKET: bucketName, CHUNK_DIRECTORY: chunkDirectory, REGION: region } = process.env;
    if (!bucketName || !chunkDirectory) {
      console.error('CHUNKS_BUCKET and CHUNK_DIRECTORY environment variables are required for this harness.');
      process.exit(1);
    }

    const configManager = ConfigManager.getInstance();
    const localConfigPath = process.env.HURON_PERSON_CONFIG_PATH || getLocalConfig();
    const config = await configManager
      .reset()
      .fromJsonString('HURON_PERSON_CONFIG_JSON')
      .fromSecretManager(process.env.SECRET_ARN)
      .fromEnvironment()
      .fromFileSystem(localConfigPath)
      .getConfigAsync('people');

    const metadataBroker = new MetadataBroker({ config, bucketName, chunkDirectory, region });

    await ProcessorServiceBooster.boostIfNeeded(metadataBroker);
  })();
}

