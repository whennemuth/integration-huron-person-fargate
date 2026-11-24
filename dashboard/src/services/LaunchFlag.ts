import { DeleteParameterCommand, GetParameterCommand, PutParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import { TestEnvironment } from 'integration-core';
import type { StartedBy } from '../../../src/chunking/StartedBy';
import { ScheduledStartGuard } from '../../../src/schedule/ScheduledStartGuard';
import type { BulkSyncChoices } from './ServiceTypes';

/**
 * A run launched from Bulk sync, until its chunker writes its first records: when it was launched (moments after
 * Start, not the run's syncRunId, which the chunker task chooses), until when the flag stands, who launched it, and
 * what was asked for.
 */
export type Launch = {
  launchedAt: string;
  expiresAt: string;
  startedBy?: StartedBy;
  choices: BulkSyncChoices;
};

/** Where a flag is kept: one value per name, written only if absent */
export interface LaunchFlagStore {
  /** Write the value if no value is kept under the name; false (nothing written) if one is */
  putIfAbsent(name: string, value: string): Promise<boolean>;
  /** The value kept under the name, or undefined */
  get(name: string): Promise<string | undefined>;
  /** Remove the value kept under the name; nothing kept is fine */
  delete(name: string): Promise<void>;
}

/** A String parameter in SSM Parameter Store (standard tier: no charge) */
export class SsmLaunchFlagStore implements LaunchFlagStore {
  constructor(private readonly client: SSMClient) {}

  async putIfAbsent(name: string, value: string): Promise<boolean> {
    try {
      await this.client.send(new PutParameterCommand({
        Name: name, Value: value, Type: 'String', Overwrite: false,
        Description: 'A run launched from the dashboard\'s Bulk sync, until its chunker writes its first records (LaunchFlag)'
      }));
      return true;
    } catch (error: any) {
      if (error?.name === 'ParameterAlreadyExists') {
        return false;
      }
      throw error;
    }
  }

  async get(name: string): Promise<string | undefined> {
    try {
      const { Parameter } = await this.client.send(new GetParameterCommand({ Name: name }));
      return Parameter?.Value;
    } catch (error: any) {
      if (error?.name === 'ParameterNotFound') {
        return undefined;
      }
      throw error;
    }
  }

  async delete(name: string): Promise<void> {
    try {
      await this.client.send(new DeleteParameterCommand({ Name: name }));
    } catch (error: any) {
      if (error?.name !== 'ParameterNotFound') {
        throw error;
      }
    }
  }
}

/** Kept in memory, for the offline services and tests */
export class MemoryLaunchFlagStore implements LaunchFlagStore {
  private readonly values = new Map<string, string>();

  async putIfAbsent(name: string, value: string): Promise<boolean> {
    if (this.values.has(name)) {
      return false;
    }
    this.values.set(name, value);
    return true;
  }

  async get(name: string): Promise<string | undefined> {
    return this.values.get(name);
  }

  async delete(name: string): Promise<void> {
    this.values.delete(name);
  }
}

/**
 * Says that a run was just launched in a landscape, between Bulk sync's Start and the moment the run's chunker writes
 * its first records, when the run appears in Run history (mock-run-feedback-plan.md, issue 1). Until then nothing
 * lists the run: the chunker subscriber, invoked asynchronously, may first reset a mock landscape (up to 10 minutes)
 * or seed a parallel start (several minutes), and the chunker task chooses the run's syncRunId when it starts.
 *
 * One SSM parameter per landscape, `/hp-dashboard/launch-flags/<landscape>`, holding a Launch as JSON:
 * - raised by BulkSync.start() once the start's checks pass, just before it invokes the chunker subscriber, and only
 *   if none is up (so two starts can't both get through); lowered again if the invoke fails.
 * - assessed by the routes that show runs and by the start's checks: lowered once the newest listed run is no earlier
 *   than the launch (less CLOCK_SLACK_MS), or once it expires; otherwise active, shown as a run launching, and a start
 *   is refused (RUN_LAUNCHING).
 * The pipeline knows nothing of it. A start that dies after the invoke leaves the flag to expire, without saying so.
 */
export class LaunchFlag {
  /** Where every landscape's flag is kept, apart from the landscape registry (/hp-dashboard/landscapes) */
  /** The chunker subscriber reads it too, to skip a scheduled start while a launch is up (ScheduledStartGuard) */
  static readonly PARAMETER_PATH = ScheduledStartGuard.LAUNCH_FLAG_PARAMETER_PATH;
  /** How much earlier than its launch a run's syncRunId may be: the chunker task's clock against the dashboard's */
  static readonly CLOCK_SLACK_MS = 30_000;
  /** How long a launch's flag stands: a plain start's chunker task is running within minutes */
  static readonly BASE_LIFETIME_MS = 5 * 60_000;
  /** Added for a parallel start: the chunker subscriber seeds, then waits up to about 6 minutes for the scale-in alarm */
  static readonly PARALLEL_START_MS = 7 * 60_000;
  /** Added for a start from scratch: the chunker subscriber first resets the mock landscape, up to 10 minutes */
  static readonly RESET_STATE_MS = 10 * 60_000;

  private readonly name: string;

  constructor(private readonly params: { landscape: string; store: LaunchFlagStore; now?: () => number }) {
    this.name = LaunchFlag.parameterName(params.landscape);
  }

  /** The landscape's flag in SSM, in the given region */
  static forLandscape(params: { landscape: string; region: string; now?: () => number }): LaunchFlag {
    const { landscape, region, now } = params;
    return new LaunchFlag({ landscape, store: new SsmLaunchFlagStore(new SSMClient({ region })), now });
  }

  /** A landscape's flag kept in memory (offline, and tests) */
  static inMemory(landscape: string, now?: () => number): LaunchFlag {
    return new LaunchFlag({ landscape, store: new MemoryLaunchFlagStore(), now });
  }

  static parameterName(landscape: string): string {
    return `${LaunchFlag.PARAMETER_PATH}/${landscape}`;
  }

  /** How long a launch with these choices is given to appear: 5 minutes, plus 7 for a parallel start, plus 10 for a start from scratch */
  static lifetimeMs(choices: Pick<BulkSyncChoices, 'parallelStart' | 'resetState'>): number {
    return LaunchFlag.BASE_LIFETIME_MS
      + (choices.parallelStart > 0 ? LaunchFlag.PARALLEL_START_MS : 0)
      + (choices.resetState ? LaunchFlag.RESET_STATE_MS : 0);
  }

  /** A launch at launchedAt with these choices, expiring after its lifetime */
  static launch(launchedAt: string, choices: BulkSyncChoices, startedBy?: StartedBy): Launch {
    const expiresAt = new Date(Date.parse(launchedAt) + LaunchFlag.lifetimeMs(choices)).toISOString();
    return { launchedAt, expiresAt, ...(startedBy ? { startedBy } : {}), choices };
  }

  get parameterName(): string {
    return this.name;
  }

  private get now(): number {
    return (this.params.now ?? Date.now)();
  }

  /** Raise the flag for a launch, unless one is up already. @returns false, with nothing written, if one is */
  async raise(launch: Launch): Promise<boolean> {
    return this.params.store.putIfAbsent(this.name, JSON.stringify(launch));
  }

  /** The flag as kept, whatever its state; undefined when none is up (or what is kept can't be read as a launch) */
  async read(): Promise<Launch | undefined> {
    return LaunchFlag.parse(await this.params.store.get(this.name));
  }

  /** A kept value as a launch; undefined for none, or for one that isn't a launch */
  static parse(value: string | undefined): Launch | undefined {
    if (value === undefined) {
      return undefined;
    }
    try {
      const launch = JSON.parse(value);
      const times = launch && [launch.launchedAt, launch.expiresAt];
      return times && times.every((time: unknown) => typeof time === 'string' && Number.isFinite(Date.parse(time))) ? launch : undefined;
    } catch {
      return undefined;
    }
  }

  /** Whether the launch's time is up (at expiresAt or after) */
  static isStale(launch: Launch, now: number): boolean {
    return !(now < Date.parse(launch.expiresAt));
  }

  /** Whether its run has appeared: the newest listed run is no earlier than the launch, less CLOCK_SLACK_MS */
  static isResolved(launch: Launch, newestSyncRunId: string | undefined): boolean {
    if (!newestSyncRunId) {
      return false;
    }
    const newest = Date.parse(newestSyncRunId);
    return Number.isFinite(newest) && newest >= Date.parse(launch.launchedAt) - LaunchFlag.CLOCK_SLACK_MS;
  }

  /** Lower the flag; one already gone is fine */
  async lower(): Promise<void> {
    await this.params.store.delete(this.name);
  }

  /**
   * The launch still waiting for its run: read the flag, lower it if its run has appeared (newestSyncRunId) or its
   * time is up, and return it only while it is neither.
   */
  async assess(newestSyncRunId: string | undefined): Promise<Launch | undefined> {
    const value = await this.params.store.get(this.name);
    if (value === undefined) {
      return undefined;
    }
    const launch = LaunchFlag.parse(value);
    // Something kept that isn't a launch would block every start until lowered
    if (!launch || LaunchFlag.isResolved(launch, newestSyncRunId) || LaunchFlag.isStale(launch, this.now)) {
      await this.lower();
      return undefined;
    }
    return launch;
  }
}

/**
 * Harness: a landscape's launch flag in SSM.
 *
 * TASK:
 *   read    the flag as kept, whatever its state
 *   assess  read it and lower it if resolved (NEWEST_SYNC_RUN_ID, optional) or expired; prints what remains
 *   raise   raise a flag for a made-up launch (a full sync, now), lasting LIFETIME_MINUTES (default 5); refused if one is up
 *   lower   lower it
 */
if (require.main === module) {
  const testEnvironment = TestEnvironment('DASHBOARD_LAUNCH_FLAG');
  [
    'TASK',
    'LANDSCAPE',
    'REGION',
    'NEWEST_SYNC_RUN_ID',
    'LIFETIME_MINUTES'
  ].forEach(testEnvironment.getVar);

  const task = testEnvironment.getVar('TASK');
  const landscape = testEnvironment.getVar('LANDSCAPE');
  const region = testEnvironment.getVar('REGION');

  (async () => {
    if (!landscape || !region) {
      throw new Error('DASHBOARD_LAUNCH_FLAG_LANDSCAPE and DASHBOARD_LAUNCH_FLAG_REGION are required');
    }
    const flag = LaunchFlag.forLandscape({ landscape, region });
    const show = (label: string, result: any) => console.log(`${label} (${flag.parameterName}):`, JSON.stringify(result, null, 2));
    switch (task) {
      case 'read':
        show('Read', await flag.read() ?? 'none');
        break;
      case 'assess':
        show('Active', await flag.assess(testEnvironment.getVar('NEWEST_SYNC_RUN_ID') || undefined) ?? 'none');
        break;
      case 'raise': {
        const minutes = Number(testEnvironment.getVar('LIFETIME_MINUTES') || 5);
        const launchedAt = new Date().toISOString();
        const launch: Launch = {
          ...LaunchFlag.launch(launchedAt, {
            runType: 'full', population: 'person-full', previousStateSource: 'storage-then-target',
            iterationLimit: 10, parallelStart: 0, overrideStalled: false
          }, { by: 'runner' }),
          expiresAt: new Date(Date.parse(launchedAt) + minutes * 60_000).toISOString()
        };
        show(await flag.raise(launch) ? 'Raised' : 'Refused: a flag is already up', launch);
        break;
      }
      case 'lower':
        await flag.lower();
        show('Lowered', 'done');
        break;
      default:
        throw new Error(`Unknown DASHBOARD_LAUNCH_FLAG_TASK: ${task} (read | assess | raise | lower)`);
    }
  })().catch(error => {
    console.error(error);
    process.exit(1);
  });
}
