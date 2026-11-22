import { GetParameterCommand, PutParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import type { ApiChunkerEvent } from '../chunking/ChunkerSubscriber';
import { isRunStartEvent } from '../chunking/RunCounters';
import { RunActivity, RunActivityState } from '../reporting/RunActivity';
import { RunReporting } from '../reporting/RunReporting';

/** A scheduled start the chunker subscriber refused, as recorded for the dashboard's Bulk sync schedule page */
export type ScheduledStartRefusal = {
  /** When (ISO) */
  refusedAt: string;
  reason: string;
  /** The run in the way, if it was one */
  syncRunId?: string;
};

/** Where the last refusal is kept: one value per landscape, each refusal replacing the one before */
export interface ScheduledStartRefusalRecord {
  record(refusal: ScheduledStartRefusal): Promise<void>;
  read(): Promise<ScheduledStartRefusal | undefined>;
}

/**
 * An SSM parameter, /hp-dashboard/schedule-refusals/<landscape> (standard tier), beside the dashboard's launch flags:
 * the chunker subscriber writes it, the dashboard reads it. EventBridge Scheduler doesn't record what its target
 * answered, so without it a skipped scheduled run would leave no trace but a missing run.
 */
export class SsmScheduledStartRefusalRecord implements ScheduledStartRefusalRecord {
  private readonly client: SSMClient;

  constructor(private readonly landscape: string, region?: string, client?: SSMClient) {
    this.client = client ?? new SSMClient(region ? { region } : {});
  }

  get parameterName(): string {
    return ScheduledStartGuard.refusalParameterName(this.landscape);
  }

  async record(refusal: ScheduledStartRefusal): Promise<void> {
    await this.client.send(new PutParameterCommand({
      Name: this.parameterName, Value: JSON.stringify(refusal), Type: 'String', Overwrite: true,
      Description: "The last scheduled bulk sync start the chunker subscriber skipped, and why (ScheduledStartGuard)",
    }));
  }

  async read(): Promise<ScheduledStartRefusal | undefined> {
    try {
      const { Parameter } = await this.client.send(new GetParameterCommand({ Name: this.parameterName }));
      return ScheduledStartGuard.parseRefusal(Parameter?.Value);
    } catch (error: any) {
      if (error?.name === 'ParameterNotFound') {
        return undefined;
      }
      throw error;
    }
  }
}

/** Kept in memory, for the offline services and tests */
export class MemoryScheduledStartRefusalRecord implements ScheduledStartRefusalRecord {
  private last?: ScheduledStartRefusal;

  async record(refusal: ScheduledStartRefusal): Promise<void> {
    this.last = refusal;
  }

  async read(): Promise<ScheduledStartRefusal | undefined> {
    return this.last;
  }
}

/**
 * Whether the chunker subscriber skips a scheduled start: the landscape's bulk sync schedule starting a run while one
 * is underway there, or while a run started from the dashboard moments ago hasn't appeared yet (its launch flag).
 * Two runs at once in a landscape would share the chunker's counters and the run list.
 *
 * Only a start the schedule makes (startedBy.by 'schedule', a run's first event) is checked: a person starting a run
 * from the dashboard or the Runner has their own guards. A stalled run (RunActivity) doesn't hold the schedule back,
 * or a run that died would stop every scheduled run after it. When the run state can't be read, the start is skipped:
 * a missed day is safer than two runs at once. Each refusal is recorded (ScheduledStartRefusalRecord) for the
 * dashboard, and a recording that fails only warns.
 *
 * In S3 storage mode a failed run's terminal error is in the chunks bucket, which this doesn't read: such a run reads
 * as underway until it stalls.
 */
export class ScheduledStartGuard {
  /** Where the dashboard keeps its launch flags (dashboard/src/services/LaunchFlag.ts takes it from here) */
  static readonly LAUNCH_FLAG_PARAMETER_PATH = '/hp-dashboard/launch-flags';
  static readonly REFUSAL_PARAMETER_PATH = '/hp-dashboard/schedule-refusals';

  static launchFlagParameterName(landscape: string): string {
    return `${ScheduledStartGuard.LAUNCH_FLAG_PARAMETER_PATH}/${landscape}`;
  }

  static refusalParameterName(landscape: string): string {
    return `${ScheduledStartGuard.REFUSAL_PARAMETER_PATH}/${landscape}`;
  }

  constructor(private readonly deps: {
    /** The landscape's newest run, as far as starting another is concerned */
    latestRun: () => Promise<RunActivityState>;
    /** When the landscape's launch flag expires (ISO), or undefined when none is up */
    launchExpiresAt: () => Promise<string | undefined>;
    refusals?: ScheduledStartRefusalRecord;
    now?: () => number;
  }) {}

  /**
   * The guard for the chunker subscriber's landscape, from its environment (lib/services/chunker/
   * ChunkerSubscribingLambda.ts sets it); undefined, with a warning, when the environment lacks what it needs (a
   * subscriber deployed before it), so a scheduled start isn't checked.
   */
  static fromEnvironment(env: NodeJS.ProcessEnv = process.env): ScheduledStartGuard | undefined {
    const { LANDSCAPE, REGION, DYNAMODB_STATISTICS_TABLE_NAME, PREVIOUS_STORAGE_TYPE = 'dynamodb' } = env;
    if (!LANDSCAPE || !REGION || !DYNAMODB_STATISTICS_TABLE_NAME) {
      console.warn('Scheduled starts are not checked for a run underway: LANDSCAPE, REGION or DYNAMODB_STATISTICS_TABLE_NAME is missing');
      return undefined;
    }
    const runs = new RunReporting({
      landscape: LANDSCAPE, storageMode: PREVIOUS_STORAGE_TYPE, statisticsTableName: DYNAMODB_STATISTICS_TABLE_NAME, region: REGION,
    });
    const ssm = new SSMClient({ region: REGION });
    return new ScheduledStartGuard({
      latestRun: async () => {
        const { syncRunIds: [newest] } = await runs.listRunPage({ limit: 1 });
        if (!newest) {
          return RunActivity.of(undefined, Date.now());
        }
        return RunActivity.of((await runs.getRunReport(newest)) ?? { syncRunId: newest, status: 'UNKNOWN' }, Date.now());
      },
      launchExpiresAt: async () => {
        try {
          const { Parameter } = await ssm.send(new GetParameterCommand({ Name: ScheduledStartGuard.launchFlagParameterName(LANDSCAPE) }));
          const expiresAt = Parameter?.Value ? JSON.parse(Parameter.Value)?.expiresAt : undefined;
          return typeof expiresAt === 'string' ? expiresAt : undefined;
        } catch (error: any) {
          if (error?.name === 'ParameterNotFound') {
            return undefined;
          }
          throw error;
        }
      },
      refusals: new SsmScheduledStartRefusalRecord(LANDSCAPE, REGION, ssm),
    });
  }

  /** A start the schedule makes: a run's first event, started by the schedule */
  static isScheduledStart(event: ApiChunkerEvent): boolean {
    return event.startedBy?.by === 'schedule' && isRunStartEvent(event);
  }

  private get now(): number {
    return (this.deps.now ?? Date.now)();
  }

  /** Why the scheduled start is skipped (recorded), or undefined to let it start; any other event is never refused */
  async refusal(event: ApiChunkerEvent): Promise<string | undefined> {
    if (!ScheduledStartGuard.isScheduledStart(event)) {
      return undefined;
    }
    let refusal: ScheduledStartRefusal | undefined;
    const refusedAt = new Date(this.now).toISOString();
    try {
      const latest = await this.deps.latestRun();
      if (latest.kind === 'underway') {
        refusal = {
          refusedAt, syncRunId: latest.syncRunId,
          reason: `The scheduled run was skipped: run ${latest.syncRunId} is underway in this landscape`,
        };
      } else {
        const expiresAt = await this.deps.launchExpiresAt();
        if (expiresAt && this.now < Date.parse(expiresAt)) {
          refusal = { refusedAt, reason: 'The scheduled run was skipped: a run started from the dashboard moments ago is starting' };
        }
      }
    } catch (error) {
      refusal = {
        refusedAt,
        reason: `The scheduled run was skipped: whether a run is underway couldn't be read (${error instanceof Error ? error.message : error})`,
      };
    }
    if (!refusal) {
      return undefined;
    }
    try {
      await this.deps.refusals?.record(refusal);
    } catch (error) {
      console.warn(`The skipped scheduled start couldn't be recorded: ${error instanceof Error ? error.message : error}`);
    }
    return refusal.reason;
  }

  /** A refusal read back from its record; undefined when the value isn't one */
  static parseRefusal(value: string | undefined): ScheduledStartRefusal | undefined {
    try {
      const parsed = value ? JSON.parse(value) : undefined;
      return parsed && typeof parsed.refusedAt === 'string' && typeof parsed.reason === 'string'
        ? { refusedAt: parsed.refusedAt, reason: parsed.reason, ...(typeof parsed.syncRunId === 'string' ? { syncRunId: parsed.syncRunId } : {}) }
        : undefined;
    } catch {
      return undefined;
    }
  }
}
