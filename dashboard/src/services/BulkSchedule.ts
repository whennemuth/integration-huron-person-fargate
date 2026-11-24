import { Config } from 'integration-huron-person';
import type { RequestAuthorizer } from '../authorization/Authorizer';
import type { ApiChunkerEvent } from '../../../src/chunking/ChunkerSubscriber';
import { previousStateSourceIn } from '../../../src/chunking/PreviousStateSource';
import { SyncPopulation } from '../../../docker/chunkTypes';
import { CronExpressionBuilder } from '../../../src/schedule/CronExpressionBuilder';
import { ScheduleChoices } from '../../../src/schedule/ScheduleChoices';
import { ScheduledRun, ScheduleInfo, SchedulePreview } from '../../../src/schedule/ScheduledRun';
import { ScheduleError } from '../../../src/schedule/ScheduleError';
import { ScheduledStartRefusal, ScheduledStartRefusalRecord, SsmScheduledStartRefusalRecord } from '../../../src/schedule/ScheduledStartGuard';
import { BulkSync, BulkSyncError } from './BulkSync';
import {
  BulkSchedulePlan, BulkScheduleRequestBody, BulkScheduleSaved, BulkScheduleService, BulkScheduleState, BulkScheduleView,
  BulkSyncRequestBody, RunReportingService
} from './ServiceTypes';

/**
 * The landscape's bulk sync schedule, for the Bulk sync schedule page: a thin layer over ScheduledRun
 * (src/schedule/ScheduledRun.ts), which owns the schedule and its rules (one per landscape, at most once a day, an hour
 * from other landscapes' schedules that read the real source), and over Bulk sync, whose form and checks the run's
 * settings reuse (BulkSync.scheduleEvent(): no test, no Start from scratch). The schedule is the record: everything
 * shown is read from EventBridge Scheduler.
 *
 * When the schedule runs comes from the page as ScheduleChoices (a time and days, or a cron expression); the backend
 * builds and checks the cron, so the page never does.
 */
export class BulkSchedule implements BulkScheduleService {
  /** Where a new schedule's form starts when the context has no SCHEDULE (or one that doesn't parse) */
  static readonly FALLBACK_WHEN: ScheduleChoices = { mode: 'simple', time: '04:00', days: 'every-day' };

  constructor(private readonly deps: {
    bulkSync: BulkSync;
    scheduledRun: ScheduledRun;
    landscape: string;
    /** The context's SCHEDULE cron (SCHEDULE_DEFAULT_CRON), where a new schedule's form starts */
    defaultCron?: string;
    /** The environment variables creating or changing a schedule needs that are missing */
    missing?: string[];
    /** The last scheduled start the chunker subscriber skipped (ScheduledStartGuard); absent: none shown */
    refusals?: ScheduledStartRefusalRecord;
    now?: () => Date;
  }) {}

  /** The real service, from the dashboard Lambda's environment (dashboard/lib/FunctionUrl.ts sets it) */
  static fromEnvironment(config: Config, runs: Pick<RunReportingService, 'listRunPage' | 'getRunReport'>): BulkSchedule {
    const { LANDSCAPE, REGION, SCHEDULE_ROLE_ARN, SCHEDULE_TARGET_ARN, SCHEDULE_DEFAULT_CRON } = process.env;
    const missing = Object.entries({ LANDSCAPE, REGION, SCHEDULE_ROLE_ARN, SCHEDULE_TARGET_ARN })
      .filter(([, value]) => !value).map(([name]) => name);
    const landscape = LANDSCAPE || 'unknown';
    return new BulkSchedule({
      bulkSync: BulkSync.fromEnvironment(config, runs),
      scheduledRun: new ScheduledRun({ landscape, region: REGION, roleArn: SCHEDULE_ROLE_ARN, targetArn: SCHEDULE_TARGET_ARN }),
      landscape,
      defaultCron: SCHEDULE_DEFAULT_CRON,
      missing,
      ...(LANDSCAPE && REGION ? { refusals: new SsmScheduledStartRefusalRecord(LANDSCAPE, REGION) } : {}),
    });
  }

  private get now(): Date {
    return (this.deps.now ?? (() => new Date()))();
  }

  async describe(): Promise<BulkScheduleState> {
    const { scheduledRun, bulkSync, landscape, missing = [] } = this.deps;
    const [info, form, others, lastRefusal] = await Promise.all([
      scheduledRun.get(), bulkSync.describe(), scheduledRun.otherSchedules(), this.lastRefusal()
    ]);
    return {
      landscape,
      ...(info ? { schedule: this.view(info) } : {}),
      ...(lastRefusal ? { lastRefusal } : {}),
      when: { defaults: this.defaultWhen(), timezone: ScheduledRun.TIMEZONE, minSpacingMinutes: ScheduledRun.MIN_SPACING_MINUTES },
      form,
      otherSchedules: others.map(({ landscape, enabled, realSource, description }) => ({ landscape, enabled, realSource, description })),
      ...(missing.length ? { blocked: { code: 'NOT_CONFIGURED', message: BulkSchedule.notConfigured(missing).message } } : {}),
    };
  }

  /** The last scheduled start the chunker subscriber skipped; none when there was none or it can't be read */
  private async lastRefusal(): Promise<ScheduledStartRefusal | undefined> {
    try {
      return await this.deps.refusals?.read();
    } catch (error) {
      console.warn(`The last skipped scheduled start couldn't be read: ${error instanceof Error ? error.message : error}`);
      return undefined;
    }
  }

  async preview(body: BulkScheduleRequestBody): Promise<SchedulePreview> {
    const realSource = (body?.run as BulkSyncRequestBody | undefined)?.source !== 'simulator';
    return this.deps.scheduledRun.preview(body?.when, { realSource, now: this.now });
  }

  /**
   * @throws ScheduleError for invalid or too frequent choices, or (for a schedule to be on) too close to another
   *   landscape's; BulkSyncError for the run's settings; ForbiddenError for a choice the authorizer doesn't grant
   */
  async plan(body: BulkScheduleRequestBody, authorizer?: RequestAuthorizer): Promise<BulkSchedulePlan> {
    const { scheduledRun, bulkSync, landscape } = this.deps;
    const enabled = BulkSchedule.enabled(body);
    const run = await bulkSync.scheduleEvent(body?.run ?? {}, authorizer);
    const when = await scheduledRun.preview(body?.when, { realSource: ScheduledRun.usesRealSource(run.event), now: this.now });
    BulkSchedule.refuse(when, enabled);
    return {
      landscape,
      when,
      run: { choices: run.choices, event: run.event as unknown as Record<string, unknown>, settings: run.settings },
      enabled,
      replaces: !!(await scheduledRun.get()),
    };
  }

  /** @throws as plan(), and ScheduleError EXISTS for a schedule created meanwhile, BulkSyncError NOT_CONFIGURED */
  async save(body: BulkScheduleRequestBody, changedBy?: string, authorizer?: RequestAuthorizer): Promise<BulkScheduleSaved> {
    const { scheduledRun, bulkSync } = this.deps;
    this.checkConfigured();
    const enabled = BulkSchedule.enabled(body);
    const run = await bulkSync.scheduleEvent(body?.run ?? {}, authorizer);
    const status = await scheduledRun.save({ choices: body?.when, input: run.event, enabled, changedBy, now: this.now });
    console.log(`Bulk sync schedule ${status} in ${this.deps.landscape} by ${changedBy ?? 'no one signed in'}`);
    return { status, schedule: await this.current() };
  }

  /** @throws ScheduleError NOT_FOUND, or TOO_CLOSE turning it on */
  async setEnabled(enabled: boolean, changedBy?: string): Promise<BulkScheduleView> {
    await this.deps.scheduledRun.setEnabled(enabled, changedBy, this.now);
    return this.current();
  }

  async delete(): Promise<{ deleted: boolean }> {
    return { deleted: await this.deps.scheduledRun.delete() };
  }

  private async current(): Promise<BulkScheduleView> {
    const info = await this.deps.scheduledRun.get();
    if (!info) {
      throw new ScheduleError('NOT_FOUND', `${this.deps.landscape} has no bulk sync schedule`);
    }
    return this.view(info);
  }

  /** The schedule as shown: its run as Bulk sync's form values, and an endpoint the config no longer names */
  private view(info: ScheduleInfo): BulkScheduleView {
    const configured = this.deps.bulkSync.peopleEndpoint();
    const event = info.input;
    const scheduled = event ? { baseUrl: event.baseUrl, fetchPath: event.fetchPath } : undefined;
    const stale = info.realSource && configured && scheduled
      && (scheduled.baseUrl !== configured.baseUrl || scheduled.fetchPath !== configured.fetchPath);
    return {
      ...info,
      run: BulkSchedule.runFromEvent(event),
      ...(stale ? { staleEndpoint: { scheduled: scheduled!, configured: configured! } } : {}),
    };
  }

  private defaultWhen(): ScheduleChoices {
    const { defaultCron } = this.deps;
    if (defaultCron) {
      try {
        return CronExpressionBuilder.toChoices(defaultCron);
      } catch {
        console.warn(`SCHEDULE_DEFAULT_CRON isn't a cron expression: ${defaultCron}`);
      }
    }
    return BulkSchedule.FALLBACK_WHEN;
  }

  private checkConfigured(): void {
    const { missing = [] } = this.deps;
    if (missing.length) {
      throw BulkSchedule.notConfigured(missing);
    }
  }

  private static notConfigured(missing: string[]): BulkSyncError {
    return new BulkSyncError('NOT_CONFIGURED',
      `This dashboard can't create or change the schedule: its environment lacks ${missing.join(', ')} (redeploy its stack)`);
  }

  /** Whether the schedule is to be on (default true) @throws BulkSyncError INVALID_REQUEST for anything but a boolean */
  static enabled(body: BulkScheduleRequestBody): boolean {
    const enabled = body?.enabled;
    if (enabled === undefined) {
      return true;
    }
    if (typeof enabled !== 'boolean') {
      throw new BulkSyncError('INVALID_REQUEST', 'enabled must be true or false');
    }
    return enabled;
  }

  /** A preview's problems as the refusal a save would make @throws ScheduleError */
  static refuse(preview: SchedulePreview, enabled: boolean): void {
    const { errors, errorCode, conflict } = preview;
    if (errors?.length) {
      throw ScheduleError.of(errorCode ?? 'INVALID_CHOICES', errors, 'The schedule choices aren\'t valid');
    }
    if (enabled && conflict) {
      const { landscape, description, at, otherAt, minutesApart } = conflict;
      throw new ScheduleError('TOO_CLOSE',
        `${landscape}'s schedule (${description}) starts at ${otherAt}, ${minutesApart} minutes from this one's run at ${at}: ` +
        `schedules that read the source must start at least ${ScheduledRun.MIN_SPACING_MINUTES} minutes apart`,
        { conflict });
    }
  }

  /** A schedule's run read back as Bulk sync's form values (what Edit schedule fills the form with) */
  static runFromEvent(event?: ApiChunkerEvent): BulkSyncRequestBody {
    if (!event) {
      return {};
    }
    const simulated = !ScheduledRun.usesRealSource(event);
    let simulator: Record<string, number> | undefined;
    if (simulated) {
      const query = new URLSearchParams(event.fetchPath.split('?')[1] ?? '');
      simulator = {};
      for (const name of ['totalPopulation', 'simulatedDelaySeconds', 'errorRate', 'nonCurrentTermRate']) {
        const value = query.get(name);
        if (value !== null && value !== '' && Number.isFinite(Number(value))) {
          simulator[name] = Number(value);
        }
      }
    }
    return {
      runType: event.populationType === SyncPopulation.PersonFull ? 'full' : 'full-no-deactivations',
      bulkReset: previousStateSourceIn(event) === 'target',
      iterationLimit: event.iterationLimit ?? 0,
      parallelStart: event.parallelStart ?? 0,
      ...(event.processorScalingCapacityOverride !== undefined ? { processorScalingCapacityOverride: event.processorScalingCapacityOverride } : {}),
      ...(simulated ? { source: 'simulator', simulator } : {}),
    };
  }
}
