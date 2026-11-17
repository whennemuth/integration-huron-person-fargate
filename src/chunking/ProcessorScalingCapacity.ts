import { DesiredCount } from '../DesiredCount';
import { isRunStartEvent } from './RunCounters';
import type { ApiChunkerEvent } from './ChunkerSubscriber';

/** What setting the processor's maximum does in AWS, injectable so it can be tested without AWS */
export type ProcessorScalingCapacityDeps = {
  /** Set the processor service's live maximum tasks (its scalable target's MaxCapacity); rejects on failure */
  setMax: (maxTasks: number) => Promise<void>;
};

/** What a run start did to the processor's maximum */
export type ProcessorScalingCapacityResult = {
  /** The maximum the run gets: its override, else the landscape's maxScalingCapacity; undefined if neither is known */
  capacity?: number;
  /** The run's override, when it carried one */
  override?: number;
  /** Whether the live maximum was set to capacity */
  applied: boolean;
  /** DRY_RUN: logged, not set */
  dryRun?: boolean;
  /** Why it wasn't set, when that didn't stop the run */
  warning?: string;
};

/**
 * The processor service's maximum tasks for a run. The landscape's deployed maximum is the context's
 * ECS.processorService.maxScalingCapacity (CloudFormation's MaxCapacity, which never changes at run time); a run may
 * carry processorScalingCapacityOverride (Bulk sync's Processor tasks, the Runner's
 * PROCESSOR_SCALING_CAPACITY_OVERRIDE) to run with another for that run only. ProcessorServiceBooster raises the
 * processor's desired count to whatever maximum is live, so this decides how many processors the run uses.
 *
 * Every run start sets the live maximum (apply()): to the run's override, or else back to maxScalingCapacity. Nothing
 * sets it back after a run: the next run start does, and the scheduled run never carries an override. Between runs the
 * service keeps the last run's maximum, but runs no tasks with nothing queued.
 *
 * Failures: a run with an override fails its start if the maximum can't be set (it would run with a maximum it didn't
 * ask for). Setting maxScalingCapacity back on a run without one only warns, so a glitch can't stop the schedule.
 * See processor-max-tasks-plan.md.
 */
export class ProcessorScalingCapacity {
  /** The lowest override a run may carry */
  static readonly MIN = 1;
  /** The highest override a run may carry, whatever the context's maxScalingCapacity (Bulk sync's dropdown) */
  static readonly MAX = 20;

  /**
   * @param deps The AWS side (awsDeps())
   * @param deployed The landscape's maxScalingCapacity (PROCESSOR_MAX_SCALING_CAPACITY), if known
   * @param dryRun Log what would be set, and set nothing
   */
  constructor(private readonly deps: ProcessorScalingCapacityDeps, private readonly deployed?: number, private readonly dryRun = false) {}

  /** The override an event carries, as sent (unchecked: see refusal()) */
  static override(event: Pick<ApiChunkerEvent, 'processorScalingCapacityOverride'>): unknown {
    return event.processorScalingCapacityOverride;
  }

  /**
   * Why an event's override can't be used, or undefined (also when it carries none). Checked before the counters are
   * reset or anything is sent: the subscriber can't trust its caller to have checked (Bulk sync does too).
   */
  static refusal(event: ApiChunkerEvent, env: NodeJS.ProcessEnv): string | undefined {
    const value = ProcessorScalingCapacity.override(event);
    if (value === undefined) {
      return undefined;
    }
    const invalid = ProcessorScalingCapacity.valueRefusal(value);
    if (invalid) {
      return invalid;
    }
    if (!isRunStartEvent(event)) {
      return "processorScalingCapacityOverride is only for a run's first event (offset 0, no chunkDirectory)";
    }
    const missing = ['REGION', 'CHUNKER_CLUSTER_NAME', 'PROCESSOR_SERVICE_NAME'].filter(name => !env[name]);
    if (missing.length > 0) {
      return `A processor scaling capacity override needs ${missing.join(', ')} in the chunker subscriber's environment`;
    }
    return undefined;
  }

  /** Why a value can't be an override (a whole number from MIN to MAX), or undefined when it can */
  static valueRefusal(value: unknown, name = 'processorScalingCapacityOverride'): string | undefined {
    return typeof value !== 'number' || !Number.isInteger(value) || value < ProcessorScalingCapacity.MIN || value > ProcessorScalingCapacity.MAX
      ? `${name} must be a whole number from ${ProcessorScalingCapacity.MIN} to ${ProcessorScalingCapacity.MAX} (got ${value})`
      : undefined;
  }

  /** The landscape's maxScalingCapacity, from PROCESSOR_MAX_SCALING_CAPACITY; undefined if unset or not a whole number above 0 */
  static deployedCapacity(env: NodeJS.ProcessEnv): number | undefined {
    const raw = env.PROCESSOR_MAX_SCALING_CAPACITY;
    if (raw === undefined || raw.trim() === '') {
      return undefined;
    }
    const value = Number(raw);
    if (!Number.isInteger(value) || value < 1) {
      console.warn(`PROCESSOR_MAX_SCALING_CAPACITY is not a whole number above 0 (got ${raw}): it is ignored`);
      return undefined;
    }
    return value;
  }

  /** The AWS side: the processor service's scalable target (DesiredCount.setMax()) */
  static awsDeps(service: { clusterName?: string; serviceName?: string; region?: string }): ProcessorScalingCapacityDeps {
    return { setMax: (maxTasks: number) => new DesiredCount(service).setMax(maxTasks) };
  }

  /**
   * The chunker subscriber's, from its environment: the processor service (PROCESSOR_SERVICE_NAME) in the landscape's
   * one cluster (CHUNKER_CLUSTER_NAME, which every service of the landscape runs in), PROCESSOR_MAX_SCALING_CAPACITY
   * and DRY_RUN
   */
  static fromEnvironment(env: NodeJS.ProcessEnv = process.env): ProcessorScalingCapacity {
    const { REGION: region, CHUNKER_CLUSTER_NAME: clusterName, PROCESSOR_SERVICE_NAME: serviceName, DRY_RUN = 'false' } = env;
    return new ProcessorScalingCapacity(
      ProcessorScalingCapacity.awsDeps({ clusterName, serviceName, region }),
      ProcessorScalingCapacity.deployedCapacity(env),
      DRY_RUN.toLowerCase() === 'true'
    );
  }

  /** The maximum a run gets: its override if it has one, else the landscape's */
  resolve(override?: number): { capacity?: number; override?: number } {
    return override === undefined ? { capacity: this.deployed } : { capacity: override, override };
  }

  /**
   * Set the processor's live maximum for a run starting now: to its override, or else back to the landscape's
   * maxScalingCapacity. Call it once a start's refusals are checked (refusal()), before the counters are reset and
   * anything is sent, so the run's first processors find it set.
   * @param override The run's processorScalingCapacityOverride, already checked
   * @throws when a run with an override can't have its maximum set
   */
  async apply(override?: number): Promise<ProcessorScalingCapacityResult> {
    const { capacity } = this.resolve(override);
    const result = { ...(capacity !== undefined ? { capacity } : {}), ...(override !== undefined ? { override } : {}) };
    if (capacity === undefined) {
      const warning = "No processor scaling capacity override, and the landscape's maxScalingCapacity "
        + "(PROCESSOR_MAX_SCALING_CAPACITY) isn't known: the processor's live maximum is left as it is";
      console.warn(warning);
      return { ...result, applied: false, warning };
    }
    const what = override === undefined
      ? `the processor's maximum tasks back to the landscape's ${capacity}`
      : `the processor's maximum tasks to ${capacity} for this run (the landscape's is ${this.deployed ?? 'unknown'})`;
    if (this.dryRun) {
      console.log(`[DRY RUN] Would set ${what}`);
      return { ...result, applied: false, dryRun: true };
    }
    try {
      await this.deps.setMax(capacity);
      console.log(`Set ${what}`);
      return { ...result, applied: true };
    } catch (error: any) {
      const message = `Couldn't set ${what}: ${error?.message ?? error}`;
      if (override !== undefined) {
        throw new Error(message);
      }
      console.warn(`${message}. The run starts with the processor's live maximum as it is.`);
      return { ...result, applied: false, warning: message };
    }
  }
}
