import {
  CreateScheduleCommand, DeleteScheduleCommand, GetScheduleCommand, GetScheduleCommandOutput, ListSchedulesCommand,
  SchedulerClient, Target, UpdateScheduleCommand, UpdateScheduleCommandInput
} from '@aws-sdk/client-scheduler';
import { CronSchedule } from './CronSchedule';
import { ScheduleError } from './ScheduleError';

export type IEventBridgeScheduleParms = {
  /** The schedule's name, e.g. chunker-api-schedule-mock */
  name: string;
  /** Its schedule group (default 'default') */
  groupName?: string;
  /** The region it is in (default the SDK's, from the environment) */
  region?: string;
  /** A client to use instead of one made from the region (tests inject a mocked one) */
  client?: SchedulerClient;
};

/** What a new schedule is: when it runs, in which zone, what it invokes and with what */
export type ScheduleDefinition = {
  /** An AWS schedule expression, e.g. cron(0 4 ? * MON-FRI *) */
  expression: string;
  /** IANA time zone the expression is read in */
  timezone: string;
  enabled: boolean;
  description?: string;
  /** The target's ARN (e.g. a Lambda function's) */
  targetArn: string;
  /** The role EventBridge Scheduler assumes to invoke the target */
  roleArn: string;
  /** The target's input, as JSON text */
  input: string;
};

/** The fields UpdateSchedule takes that a change may set; everything else is carried over as it is */
export type ScheduleChange = Partial<Omit<UpdateScheduleCommandInput, 'Name' | 'GroupName' | 'ClientToken'>>;

/** A schedule as GetSchedule read it */
export class ScheduleSnapshot {
  constructor(readonly output: GetScheduleCommandOutput) {}

  get name(): string {
    return this.output.Name ?? '';
  }

  get enabled(): boolean {
    return this.output.State === 'ENABLED';
  }

  get expression(): string {
    return this.output.ScheduleExpression ?? '';
  }

  /** The zone the expression is read in (UTC when none is set) */
  get timezone(): string {
    return this.output.ScheduleExpressionTimezone ?? 'UTC';
  }

  get description(): string | undefined {
    return this.output.Description;
  }

  get target(): Target | undefined {
    return this.output.Target;
  }

  /** The target's input, parsed from its JSON text; undefined when it has none or it isn't JSON */
  get input(): unknown {
    const text = this.output.Target?.Input;
    if (!text) {
      return undefined;
    }
    try {
      return JSON.parse(text);
    } catch {
      return undefined;
    }
  }

  get created(): Date | undefined {
    return this.output.CreationDate;
  }

  get lastModified(): Date | undefined {
    return this.output.LastModificationDate;
  }

  /** When it runs, if its expression is a cron (a rate() or at() expression gives undefined) */
  get cronSchedule(): CronSchedule | undefined {
    if (!/^cron\(/i.test(this.expression)) {
      return undefined;
    }
    try {
      return new CronSchedule(this.expression, this.timezone);
    } catch {
      return undefined;
    }
  }
}

export type IEventBridgeSchedule = {
  get(): Promise<ScheduleSnapshot | undefined>;
  getCronSchedule(): Promise<CronSchedule | undefined>;
  create(definition: ScheduleDefinition): Promise<void>;
  update(change: ScheduleChange | ((current: ScheduleSnapshot) => ScheduleChange)): Promise<void>;
  setEnabled(enabled: boolean, change?: ScheduleChange): Promise<void>;
  delete(): Promise<boolean>;
};

/**
 * One named EventBridge Scheduler schedule, through the SDK. Knows nothing about what the schedule is for: the
 * landscape's bulk sync schedule is ScheduledRun, which uses this.
 *
 * **UpdateSchedule replaces the whole schedule**: any field it isn't sent goes back to its default. So update() reads
 * the schedule first and sends every field back, with only the change applied. Never call UpdateScheduleCommand
 * anywhere else.
 *
 * New schedules invoke their target once, on time: no retries (a throttled invocation would otherwise be retried for
 * up to a day, starting a run hours late) and no flexible time window.
 */
export class EventBridgeSchedule implements IEventBridgeSchedule {
  readonly name: string;
  readonly groupName: string;
  private readonly client: SchedulerClient;

  constructor(parms: IEventBridgeScheduleParms) {
    const { name, groupName = 'default', region, client } = parms;
    this.name = name;
    this.groupName = groupName;
    this.client = client ?? new SchedulerClient(region ? { region } : {});
  }

  /** The schedule, or undefined when there is none by this name */
  async get(): Promise<ScheduleSnapshot | undefined> {
    try {
      const output = await this.client.send(new GetScheduleCommand({ Name: this.name, GroupName: this.groupName }));
      return new ScheduleSnapshot(output);
    } catch (error) {
      if (EventBridgeSchedule.isNamed(error, 'ResourceNotFoundException')) {
        return undefined;
      }
      throw error;
    }
  }

  /** When the schedule runs, or undefined when there is none or its expression isn't a cron */
  async getCronSchedule(): Promise<CronSchedule | undefined> {
    return (await this.get())?.cronSchedule;
  }

  /**
   * @throws ScheduleError EXISTS when a schedule has this name; INVALID_CRON when AWS refuses the expression
   */
  async create(definition: ScheduleDefinition): Promise<void> {
    const { expression, timezone, enabled, description, targetArn, roleArn, input } = definition;
    try {
      await this.client.send(new CreateScheduleCommand({
        Name: this.name,
        GroupName: this.groupName,
        ScheduleExpression: expression,
        ScheduleExpressionTimezone: timezone,
        State: enabled ? 'ENABLED' : 'DISABLED',
        Description: description,
        FlexibleTimeWindow: { Mode: 'OFF' },
        Target: { Arn: targetArn, RoleArn: roleArn, Input: input, RetryPolicy: { MaximumRetryAttempts: 0 } },
      }));
    } catch (error) {
      if (EventBridgeSchedule.isNamed(error, 'ConflictException')) {
        throw new ScheduleError('EXISTS', `The schedule ${this.name} already exists`);
      }
      throw EventBridgeSchedule.translated(error);
    }
  }

  /**
   * Change the schedule, leaving everything else as it is: read it, then send every field back with the change.
   * @param change The fields to set, or a function of the schedule as read that gives them (to change part of the
   *   target, say)
   * @throws ScheduleError NOT_FOUND when there is no schedule; INVALID_CRON when AWS refuses the expression
   */
  async update(change: ScheduleChange | ((current: ScheduleSnapshot) => ScheduleChange)): Promise<void> {
    const current = await this.get();
    if (!current) {
      throw new ScheduleError('NOT_FOUND', `There is no schedule ${this.name}`);
    }
    // Everything GetSchedule gives that UpdateSchedule takes: all but the read-only fields
    const { Arn, CreationDate, LastModificationDate, $metadata, ...writable } = current.output;
    const changes = typeof change === 'function' ? change(current) : change;
    const input = { ...writable, ...changes, Name: this.name, GroupName: this.groupName } as UpdateScheduleCommandInput;
    try {
      await this.client.send(new UpdateScheduleCommand(input));
    } catch (error) {
      throw EventBridgeSchedule.translated(error);
    }
  }

  /** Turn the schedule on or off, with any other change at the same time (e.g. its description) */
  async setEnabled(enabled: boolean, change: ScheduleChange = {}): Promise<void> {
    await this.update({ ...change, State: enabled ? 'ENABLED' : 'DISABLED' });
  }

  /** @returns false when there was no schedule to delete */
  async delete(): Promise<boolean> {
    try {
      await this.client.send(new DeleteScheduleCommand({ Name: this.name, GroupName: this.groupName }));
      return true;
    } catch (error) {
      if (EventBridgeSchedule.isNamed(error, 'ResourceNotFoundException')) {
        return false;
      }
      throw error;
    }
  }

  /** The names of the schedules in a group whose names begin with a prefix, every page of them */
  static async list(namePrefix: string, parms: { groupName?: string; region?: string; client?: SchedulerClient } = {}): Promise<string[]> {
    const { groupName = 'default', region, client = new SchedulerClient(region ? { region } : {}) } = parms;
    const names: string[] = [];
    let NextToken: string | undefined;
    do {
      const page = await client.send(new ListSchedulesCommand({ NamePrefix: namePrefix, GroupName: groupName, NextToken }));
      for (const { Name } of page.Schedules ?? []) {
        if (Name) {
          names.push(Name);
        }
      }
      NextToken = page.NextToken;
    } while (NextToken);
    return names;
  }

  private static isNamed(error: unknown, name: string): boolean {
    return !!error && typeof error === 'object' && (error as { name?: string }).name === name;
  }

  /** AWS refusing an expression becomes INVALID_CRON; anything else is passed on */
  private static translated(error: unknown): unknown {
    if (EventBridgeSchedule.isNamed(error, 'ValidationException') && /expression/i.test((error as Error).message ?? '')) {
      return new ScheduleError('INVALID_CRON', `AWS refused the schedule expression: ${(error as Error).message}`);
    }
    return error;
  }
}
