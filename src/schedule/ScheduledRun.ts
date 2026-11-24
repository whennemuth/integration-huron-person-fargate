import { SchedulerClient } from '@aws-sdk/client-scheduler';
import { TestEnvironment } from 'integration-core';
import { SyncPopulation } from '../../docker/chunkTypes';
import type { ApiChunkerEvent } from '../chunking/ChunkerSubscriber';
import { RunStarter } from '../chunking/StartedBy';
import type { PreviousStateSource } from '../chunking/PreviousStateSource';
import { CHUNKER_SUBSCRIBER_BASE_NAME } from '../Utils';
import { CronExpressionBuilder } from './CronExpressionBuilder';
import { CronSchedule } from './CronSchedule';
import { EventBridgeSchedule, ScheduleSnapshot } from './EventBridgeSchedule';
import { ScheduleChoices, ScheduleChoicesParser } from './ScheduleChoices';
import { ScheduleError, ScheduleErrorCode, ScheduleFieldError } from './ScheduleError';
import { TimeZone } from './TimeZone';

/** A landscape's schedule as the dashboard shows it */
export type ScheduleInfo = {
  name: string;
  enabled: boolean;
  /** cron(...) as AWS has it */
  expression: string;
  timezone: string;
  /** In words, with the zone; the expression itself when it can't be read */
  description: string;
  /** The next three runs, formatted in its zone (whether or not it is on) */
  nextRuns: string[];
  /** The choices that show its expression on the page (CronExpressionBuilder.toChoices()) */
  choices?: ScheduleChoices;
  /** The chunker event it sends */
  input?: ApiChunkerEvent;
  /** Whether it reads the real source (not the source simulator) */
  realSource: boolean;
  /** Who changed it last, and when (ISO), from its description; absent for a schedule not changed this way */
  changedBy?: string;
  changedAt?: string;
  created?: Date;
  lastModified?: Date;
};

/** Another landscape's schedule, as the spacing rule sees it */
export type OtherSchedule = {
  landscape: string;
  name: string;
  enabled: boolean;
  realSource: boolean;
  /** When it runs; undefined when its expression isn't a cron */
  schedule?: CronSchedule;
  /** In words, with the zone */
  description: string;
};

/** What a schedule's when would be, worked out without saving anything (ScheduledRun.preview()) */
export type SchedulePreview = {
  /** The choices as understood, normalized; absent when they couldn't be read */
  choices?: ScheduleChoices;
  expression?: string;
  description?: string;
  nextRuns?: string[];
  /** Invalid choices or cron fields, or too frequent */
  errors?: ScheduleFieldError[];
  /** The kind of problem the errors are: INVALID_CHOICES, INVALID_CRON or TOO_FREQUENT */
  errorCode?: ScheduleErrorCode;
  /** The other landscape's schedule it would start too close to */
  conflict?: ScheduleConflict;
  /** Daylight saving, or an expression that never runs */
  notes?: string[];
};

/** A schedule whose next run starts soon (ScheduledRun.upcomingStarts()) */
export type UpcomingStart = {
  landscape: string;
  /** Whether it is the landscape asking's own */
  thisLandscape: boolean;
  /** When, formatted in the schedule's zone */
  at: string;
  minutesAway: number;
  description: string;
};

export type ScheduleConflict = {
  landscape: string;
  description: string;
  /** This schedule's run and the other's, formatted in the zone */
  at: string;
  otherAt: string;
  minutesApart: number;
};

export type ScheduledRunParms = {
  landscape: string;
  region?: string;
  /** The role EventBridge Scheduler assumes to invoke the chunker subscriber (needed to create a schedule) */
  roleArn?: string;
  /** The chunker subscriber's ARN (needed to create a schedule) */
  targetArn?: string;
  /** A client to use instead of one made from the region (tests inject a mocked one) */
  client?: SchedulerClient;
};

/**
 * A landscape's bulk sync schedule: EventBridge Scheduler's chunker-api-schedule-<landscape>, which invokes the
 * chunker subscriber with a scheduled run's event. The schedule itself is the record: nothing about it is stored
 * anywhere else, and it is found by name.
 *
 * **Static** (what a scheduled run is, with no AWS access; CDK, the dashboard's Bulk sync defaults and tests use it):
 * a full sync with stored hashes trusted. The source has no endpoint that returns only changed people yet, so a run
 * fetches everyone; trusting the stored hashes keeps people whose hash matches from reaching the target, which is
 * the effect a delta run would have, and the full sync deactivates people missing from the source.
 *
 * Where a run's previous state comes from is one choice, bulk reset or not (previousStateSource(): `target` or
 * `storage`, src/chunking/PreviousStateSource.ts), sent as the event's previousStateSource. It replaced the
 * bulkReset / trustPreviousStorage pair, which only ever acted together.
 *
 * **Instance** (one landscape's schedule, through EventBridgeSchedule), with the bulk sync rules:
 * - one schedule per landscape: the name is fixed, and a second create is refused (EXISTS);
 * - at most once a day (TOO_FREQUENT);
 * - schedules of different landscapes that read the real source start at least MIN_SPACING_MINUTES apart, since the
 *   landscapes share one source API (TOO_CLOSE). Checked against every other landscape's enabled schedule when a
 *   schedule is created enabled, turned on, or changed while on. A schedule reading the source simulator neither
 *   conflicts nor is conflicted with.
 * Every schedule is read in TIMEZONE. Who changed it last goes in its description.
 */
export class ScheduledRun {
  static readonly POPULATION = SyncPopulation.PersonFull;
  static readonly BULK_RESET = false;
  /** What the schedule's static input says in place of the time a run is started */
  static readonly PROCESSED_AT_PLACEHOLDER = 'Placeholder. Override at runtime with ISO timestamp';
  /** Every landscape's schedule is read in this zone, so their times can be compared */
  static readonly TIMEZONE = 'America/New_York';
  /** How far apart (minutes) schedules of different landscapes that read the real source must start */
  static readonly MIN_SPACING_MINUTES = 60;
  /** Every landscape's schedule name begins with this */
  static readonly NAME_PREFIX = 'chunker-api-schedule-';
  /** How many next runs get() and preview() give */
  static readonly NEXT_RUNS = 3;

  /**
   * Where the previous state of a run that does or doesn't bulk reset comes from: a bulk reset looks each person up in
   * the target (`target`); without one, people whose stored hash matches are skipped (`storage`).
   */
  static previousStateSource(bulkReset: boolean): PreviousStateSource {
    return bulkReset ? 'target' : 'storage';
  }

  /**
   * The scheduled run's event, for the landscape's configured endpoint: what CDK's schedule sent, and what the harness
   * creates a schedule with. The dashboard builds its schedule's event from the reviewed choices instead.
   *
   * @param params.baseUrl, params.fetchPath - the integration config's people endpoint
   * @param params.iterationLimit - the context's ECS.chunkerService.chunksPerTask
   * @param params.personRecordProcessorAddons - addons for scheduled runs (none yet)
   */
  static scheduleInput(params: {
    baseUrl: string;
    fetchPath: string;
    iterationLimit: number;
    personRecordProcessorAddons?: string;
  }): ApiChunkerEvent {
    const { baseUrl, fetchPath, iterationLimit, personRecordProcessorAddons } = params;
    return {
      baseUrl,
      fetchPath,
      populationType: ScheduledRun.POPULATION,
      iterationLimit,
      offset: 0,
      previousStateSource: ScheduledRun.previousStateSource(ScheduledRun.BULK_RESET),
      ...(personRecordProcessorAddons !== undefined ? { personRecordProcessorAddons } : {}),
      startedBy: RunStarter.SCHEDULE,
      processingMetadata: {
        processedAt: ScheduledRun.PROCESSED_AT_PLACEHOLDER,
        processorVersion: '1.0.0'
      }
    };
  }

  /** The landscape's schedule name: chunker-api-schedule-<landscape> */
  static scheduleName(landscape: string): string {
    return `${ScheduledRun.NAME_PREFIX}${landscape.toLowerCase()}`;
  }

  /** The role the schedule invokes the chunker subscriber with: chunker-schedule-<landscape> (CDK creates it) */
  static roleName(landscape: string): string {
    return `chunker-schedule-${landscape.toLowerCase()}`;
  }

  static roleArn(account: string, landscape: string): string {
    return `arn:aws:iam::${account}:role/${ScheduledRun.roleName(landscape)}`;
  }

  static functionArn(region: string, account: string, functionName: string): string {
    return `arn:aws:lambda:${region}:${account}:function:${functionName}`;
  }

  /**
   * Whether a scheduled run's event reads the real source: an event reading the source simulator carries the
   * simulator's terms endpoint (termsEndpoint, set only then), so anything without one reads the real source.
   */
  static usesRealSource(input: unknown): boolean {
    return !(input && typeof input === 'object' && (input as ApiChunkerEvent).termsEndpoint);
  }

  readonly landscape: string;
  readonly name: string;
  private readonly roleArn?: string;
  private readonly targetArn?: string;
  private readonly client: SchedulerClient;
  private readonly schedule: EventBridgeSchedule;
  private readonly timeZone = new TimeZone(ScheduledRun.TIMEZONE);

  constructor(parms: ScheduledRunParms) {
    const { landscape, region, roleArn, targetArn, client } = parms;
    this.landscape = landscape.toLowerCase();
    this.name = ScheduledRun.scheduleName(landscape);
    this.roleArn = roleArn;
    this.targetArn = targetArn;
    this.client = client ?? new SchedulerClient(region ? { region } : {});
    this.schedule = new EventBridgeSchedule({ name: this.name, client: this.client });
  }

  /** The landscape's schedule, or undefined when it has none */
  async get(): Promise<ScheduleInfo | undefined> {
    const snapshot = await this.schedule.get();
    return snapshot ? this.info(snapshot) : undefined;
  }

  /**
   * What a schedule made from these choices would be, saving nothing: the expression, its words and next runs, and
   * anything wrong (invalid choices or fields, too frequent, too close to another landscape's schedule). Never throws
   * for bad choices: they come back as errors, for the page to show beside its fields.
   *
   * @param body The page's choices (ScheduleChoicesParser.parse() reads them)
   * @param options.realSource Whether the schedule would read the real source (default true): only then is it
   *   checked against the other landscapes'
   */
  async preview(body: unknown, options: { realSource?: boolean; now?: Date } = {}): Promise<SchedulePreview> {
    const { realSource = true, now = new Date() } = options;
    let choices: ScheduleChoices | undefined;
    let schedule: CronSchedule;
    try {
      choices = ScheduleChoicesParser.parse(body);
      schedule = this.scheduleFor(choices);
    } catch (error) {
      if (!ScheduleError.isScheduleError(error)) {
        throw error;
      }
      return { ...(choices ? { choices } : {}), errors: error.errors, errorCode: error.code };
    }

    const preview: SchedulePreview = {
      choices,
      expression: schedule.getAwsExpression(),
      description: schedule.describe(),
      nextRuns: schedule.nextExecutions(ScheduledRun.NEXT_RUNS, now).map(run => this.timeZone.format(run)),
    };
    const notes = schedule.daylightSavingNotes(now);
    if (preview.nextRuns!.length === 0) {
      notes.push('It never runs: no day matches its expression.');
    }
    if (notes.length) {
      preview.notes = notes;
    }
    if (!schedule.isAtMostOnceADay()) {
      preview.errors = [ScheduledRun.tooFrequent(schedule).errors[0]];
      preview.errorCode = 'TOO_FREQUENT';
      return preview;
    }
    if (realSource) {
      const conflict = await this.conflict(schedule, now);
      if (conflict) {
        preview.conflict = conflict;
      }
    }
    return preview;
  }

  /**
   * Create the landscape's schedule.
   * @throws ScheduleError INVALID_CHOICES, INVALID_CRON, TOO_FREQUENT, TOO_CLOSE (created on, reading the real
   *   source), EXISTS
   */
  async create(params: { choices: unknown; input: ApiChunkerEvent; enabled: boolean; changedBy?: string; now?: Date }): Promise<void> {
    const { choices, input, enabled, changedBy, now = new Date() } = params;
    if (!this.roleArn || !this.targetArn) {
      throw new Error(`Creating ${this.name} needs the scheduler role's and the chunker subscriber's ARNs`);
    }
    const schedule = this.scheduleFor(ScheduleChoicesParser.parse(choices));
    if (enabled && ScheduledRun.usesRealSource(input)) {
      await this.checkSpacing(schedule, now);
    }
    await this.schedule.create({
      expression: schedule.getAwsExpression(),
      timezone: ScheduledRun.TIMEZONE,
      enabled,
      description: this.description(changedBy, now),
      targetArn: this.targetArn,
      roleArn: this.roleArn,
      input: JSON.stringify(input),
    });
  }

  /**
   * Turn the schedule on or off (2b). Turning on a schedule that reads the real source checks it against the other
   * landscapes'.
   * @throws ScheduleError NOT_FOUND, TOO_CLOSE
   */
  async setEnabled(enabled: boolean, changedBy?: string, now: Date = new Date()): Promise<void> {
    const current = await this.current();
    const schedule = current.cronSchedule;
    if (enabled && schedule && ScheduledRun.usesRealSource(current.input)) {
      await this.checkSpacing(schedule, now);
    }
    await this.schedule.setEnabled(enabled, { Description: this.description(changedBy, now) });
  }

  /**
   * Change when the schedule runs (2c), in the schedule's zone.
   * @throws ScheduleError NOT_FOUND, INVALID_CHOICES, INVALID_CRON, TOO_FREQUENT, TOO_CLOSE (while on, reading the
   *   real source)
   */
  async setCron(choices: unknown, changedBy?: string, now: Date = new Date()): Promise<void> {
    const current = await this.current();
    const schedule = this.scheduleFor(ScheduleChoicesParser.parse(choices));
    if (current.enabled && ScheduledRun.usesRealSource(current.input)) {
      await this.checkSpacing(schedule, now);
    }
    await this.schedule.update({
      ScheduleExpression: schedule.getAwsExpression(),
      ScheduleExpressionTimezone: ScheduledRun.TIMEZONE,
      Description: this.description(changedBy, now),
    });
  }

  /**
   * Change the run the schedule starts: its chunker event.
   * @throws ScheduleError NOT_FOUND, TOO_CLOSE (while on, when the new event reads the real source)
   */
  async replaceInput(input: ApiChunkerEvent, changedBy?: string, now: Date = new Date()): Promise<void> {
    const current = await this.current();
    const schedule = current.cronSchedule;
    if (current.enabled && schedule && ScheduledRun.usesRealSource(input)) {
      await this.checkSpacing(schedule, now);
    }
    await this.schedule.update(snapshot => ({
      Target: { ...snapshot.target!, Input: JSON.stringify(input) },
      Description: this.description(changedBy, now),
    }));
  }

  /**
   * Change when the schedule runs, the run it starts and whether it is on, all at once (the dashboard's Edit schedule):
   * checked once, then one update, so a refusal leaves the schedule as it was.
   * @throws ScheduleError NOT_FOUND, INVALID_CHOICES, INVALID_CRON, TOO_FREQUENT, TOO_CLOSE (on, reading the real
   *   source)
   */
  async replace(params: { choices: unknown; input: ApiChunkerEvent; enabled: boolean; changedBy?: string; now?: Date }): Promise<void> {
    const { choices, input, enabled, changedBy, now = new Date() } = params;
    await this.current();
    const schedule = this.scheduleFor(ScheduleChoicesParser.parse(choices));
    if (enabled && ScheduledRun.usesRealSource(input)) {
      await this.checkSpacing(schedule, now);
    }
    await this.schedule.update(snapshot => ({
      ScheduleExpression: schedule.getAwsExpression(),
      ScheduleExpressionTimezone: ScheduledRun.TIMEZONE,
      State: enabled ? 'ENABLED' : 'DISABLED',
      Target: { ...snapshot.target!, Input: JSON.stringify(input) },
      Description: this.description(changedBy, now),
    }));
  }

  /**
   * Create the landscape's schedule, or replace the one it has (create() or replace()).
   * @returns Which it did
   */
  async save(params: { choices: unknown; input: ApiChunkerEvent; enabled: boolean; changedBy?: string; now?: Date }): Promise<'created' | 'replaced'> {
    if (await this.schedule.get()) {
      await this.replace(params);
      return 'replaced';
    }
    await this.create(params);
    return 'created';
  }

  /** @returns false when there was no schedule */
  async delete(): Promise<boolean> {
    return this.schedule.delete();
  }

  /**
   * The enabled schedules that read the real source, this landscape's and the others', whose next run starts within
   * the window: what a run started by hand now could run into (Bulk sync's warning).
   */
  async upcomingStarts(withinMinutes: number = ScheduledRun.MIN_SPACING_MINUTES, now: Date = new Date()): Promise<UpcomingStart[]> {
    const own = await this.schedule.get();
    const candidates: { landscape: string; snapshot: ScheduleSnapshot }[] = own ? [{ landscape: this.landscape, snapshot: own }] : [];
    for (const name of (await EventBridgeSchedule.list(ScheduledRun.NAME_PREFIX, { client: this.client })).filter(name => name !== this.name)) {
      const snapshot = await new EventBridgeSchedule({ name, client: this.client }).get();
      if (snapshot) {
        candidates.push({ landscape: name.slice(ScheduledRun.NAME_PREFIX.length), snapshot });
      }
    }
    const starts: UpcomingStart[] = [];
    for (const { landscape, snapshot } of candidates) {
      const schedule = snapshot.cronSchedule;
      if (!snapshot.enabled || !schedule || !ScheduledRun.usesRealSource(snapshot.input)) {
        continue;
      }
      const next = schedule.getNextExecutionDate(now);
      const minutesAway = next ? Math.round((next.getTime() - now.getTime()) / 60_000) : undefined;
      if (next && minutesAway !== undefined && minutesAway <= withinMinutes) {
        starts.push({
          landscape, thisLandscape: landscape === this.landscape, at: schedule.timeZone.format(next), minutesAway, description: schedule.describe(),
        });
      }
    }
    return starts.sort((a, b) => a.minutesAway - b.minutesAway);
  }

  /** Every other landscape's schedule in this account and region */
  async otherSchedules(): Promise<OtherSchedule[]> {
    const names = (await EventBridgeSchedule.list(ScheduledRun.NAME_PREFIX, { client: this.client }))
      .filter(name => name !== this.name);
    const others: OtherSchedule[] = [];
    for (const name of names) {
      const snapshot = await new EventBridgeSchedule({ name, client: this.client }).get();
      if (!snapshot) {
        continue; // deleted since it was listed
      }
      const schedule = snapshot.cronSchedule;
      others.push({
        landscape: name.slice(ScheduledRun.NAME_PREFIX.length),
        name,
        enabled: snapshot.enabled,
        realSource: ScheduledRun.usesRealSource(snapshot.input),
        schedule,
        description: schedule ? schedule.describe() : snapshot.expression,
      });
    }
    return others;
  }

  /** The first enabled real-source schedule of another landscape this one would start too close to, if any */
  private async conflict(schedule: CronSchedule, now: Date): Promise<ScheduleConflict | undefined> {
    const buffer = ScheduledRun.MIN_SPACING_MINUTES * 60_000;
    for (const other of await this.otherSchedules()) {
      if (!other.enabled || !other.realSource || !other.schedule) {
        continue;
      }
      const details = schedule.overlapsWith(other.schedule, buffer, now);
      if (details.overlaps) {
        return {
          landscape: other.landscape,
          description: other.description,
          at: this.timeZone.format(details.at!),
          otherAt: this.timeZone.format(details.otherAt!),
          minutesApart: Math.round(details.gap! / 60_000),
        };
      }
    }
    return undefined;
  }

  /** @throws ScheduleError TOO_CLOSE */
  private async checkSpacing(schedule: CronSchedule, now: Date): Promise<void> {
    const conflict = await this.conflict(schedule, now);
    if (conflict) {
      const { landscape, description, at, otherAt, minutesApart } = conflict;
      throw new ScheduleError('TOO_CLOSE',
        `${landscape}'s schedule (${description}) starts at ${otherAt}, ${minutesApart} minutes from this one's run at ${at}: ` +
        `schedules that read the source must start at least ${ScheduledRun.MIN_SPACING_MINUTES} minutes apart`,
        { conflict });
    }
  }

  /**
   * The schedule for the choices, in TIMEZONE, if a bulk sync may have it.
   * @throws ScheduleError INVALID_CHOICES, INVALID_CRON, TOO_FREQUENT
   */
  private scheduleFor(choices: ScheduleChoices): CronSchedule {
    const schedule = new CronSchedule(CronExpressionBuilder.fromChoices(choices), this.timeZone);
    if (!schedule.isAtMostOnceADay()) {
      throw ScheduledRun.tooFrequent(schedule);
    }
    return schedule;
  }

  private static tooFrequent(schedule: CronSchedule): ScheduleError {
    return ScheduleError.of('TOO_FREQUENT', [{
      field: 'expression',
      message: `${schedule.getAwsExpression()} runs more than once a day: a bulk sync schedule needs one minute and one hour`,
    }], 'Too frequent');
  }

  /** @throws ScheduleError NOT_FOUND */
  private async current(): Promise<ScheduleSnapshot> {
    const snapshot = await this.schedule.get();
    if (!snapshot) {
      throw new ScheduleError('NOT_FOUND', `${this.landscape} has no bulk sync schedule (${this.name})`);
    }
    return snapshot;
  }

  /** The schedule's description, saying who changed it and when */
  private description(changedBy: string | undefined, now: Date): string {
    const by = changedBy?.trim() ? `by ${changedBy.trim()} ` : '';
    return `Bulk sync schedule for ${this.landscape}. Changed ${by}at ${now.toISOString()}`;
  }

  private info(snapshot: ScheduleSnapshot): ScheduleInfo {
    const schedule = snapshot.cronSchedule;
    const input = snapshot.input as ApiChunkerEvent | undefined;
    const changed = /Changed (?:by (.+) )?at (\S+)$/.exec(snapshot.description ?? '');
    let choices: ScheduleChoices | undefined;
    if (schedule) {
      choices = CronExpressionBuilder.toChoices(schedule.expression);
    }
    return {
      name: snapshot.name,
      enabled: snapshot.enabled,
      expression: snapshot.expression,
      timezone: snapshot.timezone,
      description: schedule ? schedule.describe() : snapshot.expression,
      nextRuns: schedule
        ? schedule.nextExecutions(ScheduledRun.NEXT_RUNS).map(run => schedule.timeZone.format(run))
        : [],
      ...(choices ? { choices } : {}),
      ...(input ? { input } : {}),
      realSource: ScheduledRun.usesRealSource(input),
      ...(changed?.[1] ? { changedBy: changed[1] } : {}),
      ...(changed ? { changedAt: changed[2] } : {}),
      created: snapshot.created,
      lastModified: snapshot.lastModified,
    };
  }
}

/** The active landscape's context (context/context.json), for the harness's defaults */
function activeContext(): any {
  try {
    return JSON.parse(require('fs').readFileSync(require('path').resolve(process.cwd(), 'context/context.json'), 'utf8'));
  } catch {
    return {};
  }
}

/**
 * Harness SCHEDULED_RUN: a landscape's bulk sync schedule in AWS, through the same rules as the dashboard.
 *
 *   TASK=name | get | list | input | delete
 *   TASK=preview | create | cron   when: TIME and DAYS, or CRON_EXPRESSION (default: the context's SCHEDULE)
 *   TASK=enable | disable
 *   create also takes ENABLED (default false), and sends scheduleInput() for the landscape's integration config
 *
 * LANDSCAPE, REGION, ACCOUNT and CONFIG_PATH default to the active context's (context/context.json).
 */
async function main(): Promise<void> {
  const context = activeContext();
  const {
    TASK: task,
    LANDSCAPE: landscape = context.TAGS?.Landscape,
    REGION: region = context.REGION,
    ACCOUNT: account = context.ACCOUNT,
    CONFIG_PATH: configPath = context.HURON_PERSON_CONFIG?.configPath,
    TIME: time,
    DAYS: days,
    CRON_EXPRESSION: cronExpression,
    ENABLED: enabled = 'false',
  } = process.env;
  if (!landscape || !region) {
    throw new Error('LANDSCAPE and REGION are required (or an active context that has them)');
  }

  const when = (): ScheduleChoices => {
    if (cronExpression) {
      return { mode: 'cron', expression: cronExpression };
    }
    if (time) {
      return ScheduleChoicesParser.parse({ mode: 'simple', time, days: days?.includes(',') ? days.split(',') : days ?? 'every-day' });
    }
    const contextCron = context.SCHEDULE?.cronExpression;
    if (!contextCron) {
      throw new Error('Say when: TIME and DAYS, or CRON_EXPRESSION (the active context has no SCHEDULE)');
    }
    return { mode: 'cron', expression: contextCron };
  };
  const input = (): ApiChunkerEvent => {
    if (!configPath) {
      throw new Error('CONFIG_PATH is required (or an active context whose HURON_PERSON_CONFIG has a configPath)');
    }
    // Only the endpoint is needed: read the file as it is, not through ConfigManager, which validates the whole config
    const config: any = JSON.parse(require('fs').readFileSync(require('path').resolve(process.cwd(), configPath), 'utf8'));
    const { fetchPath, endpointConfig: { baseUrl } = {} as any } = config.dataSource?.people ?? {};
    if (!baseUrl || !fetchPath) {
      throw new Error(`${configPath} has no dataSource.people endpointConfig.baseUrl and fetchPath`);
    }
    return ScheduledRun.scheduleInput({ baseUrl, fetchPath, iterationLimit: context.ECS?.chunkerService?.chunksPerTask ?? 0 });
  };

  const scheduledRun = new ScheduledRun({
    landscape, region,
    roleArn: account ? ScheduledRun.roleArn(account, landscape) : undefined,
    targetArn: account ? ScheduledRun.functionArn(region, account, `${CHUNKER_SUBSCRIBER_BASE_NAME}-${landscape.toLowerCase()}`) : undefined,
  });
  const changedBy = 'the SCHEDULED_RUN harness';
  const print = (value: unknown) => console.log(JSON.stringify(value, null, 2));

  switch (task) {
    case 'name':
      console.log(scheduledRun.name);
      break;
    case 'get':
      print((await scheduledRun.get()) ?? `${landscape} has no schedule (${scheduledRun.name})`);
      break;
    case 'list':
      for (const other of await scheduledRun.otherSchedules()) {
        console.log(`${other.landscape}: ${other.enabled ? 'ON ' : 'off'} ${other.description}, ${other.realSource ? 'real source' : 'source simulator'}`);
      }
      print((await scheduledRun.get()) ?? `${landscape}: no schedule`);
      break;
    case 'input':
      print(input());
      break;
    case 'preview':
      print(await scheduledRun.preview(when()));
      break;
    case 'create':
      await scheduledRun.create({ choices: when(), input: input(), enabled: enabled === 'true', changedBy });
      print(await scheduledRun.get());
      break;
    case 'enable':
    case 'disable':
      await scheduledRun.setEnabled(task === 'enable', changedBy);
      print(await scheduledRun.get());
      break;
    case 'cron':
      await scheduledRun.setCron(when(), changedBy);
      print(await scheduledRun.get());
      break;
    case 'delete':
      console.log(await scheduledRun.delete() ? `Deleted ${scheduledRun.name}` : `${landscape} had no schedule`);
      break;
    default:
      throw new Error(`Unknown TASK value: ${task}. Valid values: name, get, list, input, preview, create, enable, disable, cron, delete`);
  }
}

if (require.main === module) {
  const testEnvironment = TestEnvironment('SCHEDULED_RUN');

  [
    'TASK',
    'LANDSCAPE',
    'REGION',
    'ACCOUNT',
    'CONFIG_PATH',
    'TIME',
    'DAYS',
    'CRON_EXPRESSION',
    'ENABLED'
  ].forEach(testEnvironment.getVar);

  main().catch((error: unknown) => {
    if (ScheduleError.isScheduleError(error)) {
      console.error(`ScheduledRun harness: ${error.code}: ${error.message}`);
    } else {
      console.error(`ScheduledRun harness failed: ${error instanceof Error ? error.message : String(error)}`);
    }
    process.exitCode = 1;
  });
}
