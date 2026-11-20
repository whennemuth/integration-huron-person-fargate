/**
 * Tests the dashboard's bulk sync schedule (dashboard/src/services/BulkSchedule.ts) over the real ScheduledRun and an
 * in-memory EventBridge Scheduler (dashboard/src/handlers/OfflineScheduler.ts, seeded with staging on weekdays at
 * 3:00 AM, preview every day at 5:00 AM and mock1 at 4:30 AM reading the source simulator), with the offline Bulk
 * sync for the run's settings: what the page is told, the preview and the review, saving, turning on and off,
 * deleting, the refusals, Bulk sync's warning of a schedule starting within the hour, and the routes' answers.
 */
import { BulkSchedule } from '../dashboard/src/services/BulkSchedule';
import { BulkSyncError } from '../dashboard/src/services/BulkSync';
import { InMemorySchedulerClient } from '../dashboard/src/handlers/OfflineScheduler';
import { OfflineBulkSyncService } from '../dashboard/src/handlers/OfflineServices';
import { ScheduledRun } from '../src/schedule/ScheduledRun';
import { ScheduleError } from '../src/schedule/ScheduleError';
import { COOKIE, event, json, loadHandler, signedInEnvironment } from './support/OfflineDashboard';
import { TestSignIn } from './support/TestSignIn';

/** Thursday Nov 19 2026, 7:00 AM in New York */
const now = new Date('2026-11-19T12:00:00Z');
const roleArn = 'arn:aws:iam::123456789012:role/chunker-schedule-mock';
const targetArn = 'arn:aws:lambda:us-east-2:123456789012:function:chunker-subscriber-mock';
const everyDayAt = (time: string) => ({ mode: 'simple', time, days: 'every-day' });

/** A mock landscape's schedule service, offline */
const make = (options: { missing?: string[]; defaultCron?: string } = {}) => {
  const client = new InMemorySchedulerClient('mock', () => now);
  const scheduledRun = new ScheduledRun({ landscape: 'mock', client: client.asClient(), roleArn, targetArn });
  const offline = new OfflineBulkSyncService({
    settings: { landscape: 'mock' }, now: () => now.getTime(),
    upcomingSchedules: () => scheduledRun.upcomingStarts(60, now),
  });
  const service = new BulkSchedule({
    bulkSync: offline.bulkSync, scheduledRun, landscape: 'mock',
    defaultCron: 'defaultCron' in options ? options.defaultCron : 'cron(0 4 * * ? *)',
    missing: options.missing, now: () => now,
  });
  return { service, scheduledRun, offline };
};

const thrown = async (action: () => Promise<unknown>): Promise<any> => {
  try {
    await action();
  } catch (error) {
    return error;
  }
  throw new Error('nothing was thrown');
};

describe('BulkSchedule', () => {
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  describe('describe', () => {
    it('tells the page there is no schedule, where When starts, the form, and the other landscapes\' schedules', async () => {
      const state = await make().service.describe();
      expect(state.schedule).toBeUndefined();
      expect(state.when).toEqual({ defaults: everyDayAt('04:00'), timezone: 'America/New_York', minSpacingMinutes: 60 });
      expect(state.form.defaults.runType).toBe('full');
      expect(state.otherSchedules).toEqual([
        { landscape: 'staging', enabled: true, realSource: true, description: 'At 03:00 AM, Monday through Friday (America/New_York)' },
        { landscape: 'preview', enabled: true, realSource: true, description: 'At 05:00 AM (America/New_York)' },
        { landscape: 'mock1', enabled: true, realSource: false, description: 'At 04:30 AM (America/New_York)' },
      ]);
      expect(state.blocked).toBeUndefined();
    });

    it("starts When at 4:00 AM every day when the context's cron is missing or doesn't parse", async () => {
      expect((await make({ defaultCron: undefined }).service.describe()).when.defaults).toEqual(BulkSchedule.FALLBACK_WHEN);
      expect((await make({ defaultCron: 'every morning' }).service.describe()).when.defaults).toEqual(BulkSchedule.FALLBACK_WHEN);
    });

    it("says why the schedule can't be changed when the environment lacks what that needs", async () => {
      const state = await make({ missing: ['SCHEDULE_ROLE_ARN'] }).service.describe();
      expect(state.blocked).toEqual({ code: 'NOT_CONFIGURED', message: expect.stringMatching(/SCHEDULE_ROLE_ARN/) });
    });
  });

  describe('preview', () => {
    it('answers with the expression, its words and next runs', async () => {
      const preview = await make().service.preview({ when: { mode: 'simple', time: '04:00', days: 'weekdays' } });
      expect(preview).toMatchObject({ expression: 'cron(0 4 ? * MON-FRI *)', description: 'At 04:00 AM, Monday through Friday (America/New_York)' });
      expect(preview.nextRuns).toHaveLength(3);
      expect(preview.conflict).toBeUndefined();
    });

    it("names the schedule it would start too close to, unless the run reads the source simulator", async () => {
      const { service } = make();
      expect((await service.preview({ when: everyDayAt('03:30') })).conflict).toMatchObject({ landscape: 'staging', minutesApart: 30 });
      expect((await service.preview({ when: everyDayAt('03:30'), run: { runType: 'full', source: 'simulator' } })).conflict).toBeUndefined();
    });
  });

  describe('plan', () => {
    it('reviews when and the run, started by the schedule, without saving', async () => {
      const { service, scheduledRun } = make();
      const plan = await service.plan({ when: everyDayAt('04:00'), run: { runType: 'full', parallelStart: 5 } });
      expect(plan).toMatchObject({ landscape: 'mock', enabled: true, replaces: false, when: { expression: 'cron(0 4 * * ? *)' } });
      expect(plan.run.event).toMatchObject({
        baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: 'person-full', parallelStart: 5,
        bulkReset: false, trustPreviousStorage: true, startedBy: { by: 'schedule' },
        processingMetadata: { processedAt: ScheduledRun.PROCESSED_AT_PLACEHOLDER },
      });
      expect(await scheduledRun.get()).toBeUndefined();
    });

    it("refuses what a recurring run mustn't do: a test, or Start from scratch", async () => {
      const { service } = make();
      const test = await thrown(() => service.plan({ when: everyDayAt('04:00'), run: { runType: 'test', buid: 'U123' } }));
      expect(test).toBeInstanceOf(BulkSyncError);
      expect(test.message).toMatch(/can't run a test/);
      const scratch = await thrown(() => service.plan({ when: everyDayAt('04:00'), run: { runType: 'full', resetState: true } }));
      expect(scratch.message).toMatch(/can't start from scratch/);
    });

    it('refuses invalid choices, too frequent ones, and (for a schedule to be on) one too close to another landscape\'s', async () => {
      const { service } = make();
      const invalid = await thrown(() => service.plan({ when: { mode: 'simple', time: '25:00', days: 'weekdays' }, run: { runType: 'full' } }));
      expect(invalid).toBeInstanceOf(ScheduleError);
      expect(invalid).toMatchObject({ code: 'INVALID_CHOICES', errors: [{ field: 'time', message: expect.any(String) }] });
      const frequent = await thrown(() => service.plan({ when: { mode: 'cron', expression: 'cron(0 */6 * * ? *)' }, run: { runType: 'full' } }));
      expect(frequent.code).toBe('TOO_FREQUENT');
      const close = await thrown(() => service.plan({ when: everyDayAt('03:30'), run: { runType: 'full' } }));
      expect(close).toMatchObject({ code: 'TOO_CLOSE', details: { conflict: { landscape: 'staging' } } });
      await expect(service.plan({ when: everyDayAt('03:30'), run: { runType: 'full' }, enabled: false })).resolves.toMatchObject({ enabled: false });
    });

    it('refuses an enabled that isn\'t true or false', async () => {
      const error = await thrown(() => make().service.plan({ when: everyDayAt('04:00'), run: { runType: 'full' }, enabled: 'yes' }));
      expect(error).toMatchObject({ code: 'INVALID_REQUEST' });
    });
  });

  describe('save', () => {
    it('creates the schedule, then replaces it, with the run read back as the form\'s values and who changed it', async () => {
      const { service } = make();
      const created = await service.save({ when: { mode: 'simple', time: '04:00', days: 'weekdays' }, run: { runType: 'full', iterationLimit: 10 } }, 'jdoe');
      expect(created.status).toBe('created');
      expect(created.schedule).toMatchObject({
        name: 'chunker-api-schedule-mock', enabled: true, expression: 'cron(0 4 ? * MON-FRI *)',
        choices: { mode: 'simple', time: '04:00', days: 'weekdays' }, changedBy: 'jdoe', realSource: true,
        run: { runType: 'full', bulkReset: false, iterationLimit: 10, parallelStart: 0 },
      });
      const replaced = await service.save({ when: everyDayAt('04:15'), run: { runType: 'full-no-deactivations', bulkReset: true }, enabled: false }, 'amy');
      expect(replaced.status).toBe('replaced');
      expect(replaced.schedule).toMatchObject({
        enabled: false, expression: 'cron(15 4 * * ? *)', changedBy: 'amy', run: { runType: 'full-no-deactivations', bulkReset: true },
      });
    });

    it("reads back a source simulator run's settings", async () => {
      const { service } = make();
      const saved = await service.save({ when: everyDayAt('03:30'), run: { runType: 'full', source: 'simulator', simulator: { totalPopulation: 500, errorRate: 0.1 } } });
      expect(saved.schedule.realSource).toBe(false);
      expect(saved.schedule.run).toMatchObject({ source: 'simulator', simulator: { totalPopulation: 500, errorRate: 0.1 } });
    });

    it('refuses to save when the environment lacks what that needs, changing nothing', async () => {
      const { service, scheduledRun } = make({ missing: ['SCHEDULE_TARGET_ARN'] });
      const error = await thrown(() => service.save({ when: everyDayAt('04:00'), run: { runType: 'full' } }));
      expect(error).toMatchObject({ code: 'NOT_CONFIGURED' });
      expect(await scheduledRun.get()).toBeUndefined();
    });

    it("flags a schedule calling an endpoint the integration config no longer names", async () => {
      const { service, scheduledRun } = make();
      await scheduledRun.create({
        choices: everyDayAt('04:00'), enabled: false, now,
        input: ScheduledRun.scheduleInput({ baseUrl: 'https://old.example.bu.edu', fetchPath: '/api/people', iterationLimit: 10 }),
      });
      expect((await service.describe()).schedule!.staleEndpoint).toEqual({
        scheduled: { baseUrl: 'https://old.example.bu.edu', fetchPath: '/api/people' },
        configured: { baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people' },
      });
    });
  });

  describe('setEnabled and delete', () => {
    it('turns the schedule off and on, refusing on when too close, and deletes it', async () => {
      const { service } = make();
      await service.save({ when: everyDayAt('03:30'), run: { runType: 'full' }, enabled: false });
      expect(await thrown(() => service.setEnabled(true, 'jdoe'))).toMatchObject({ code: 'TOO_CLOSE' });
      await service.save({ when: everyDayAt('04:00'), run: { runType: 'full' }, enabled: false });
      expect((await service.setEnabled(true, 'jdoe')).enabled).toBe(true);
      expect((await service.setEnabled(false, 'jdoe')).enabled).toBe(false);
      expect(await service.delete()).toEqual({ deleted: true });
      expect(await service.delete()).toEqual({ deleted: false });
      expect(await thrown(() => service.setEnabled(true))).toMatchObject({ code: 'NOT_FOUND' });
    });
  });

  describe("Bulk sync's warning", () => {
    it('lists a schedule that reads the real source and starts within the hour, in the state and the review', async () => {
      const { service, offline } = make();
      expect((await offline.describe()).upcomingSchedules).toBeUndefined();
      await service.save({ when: everyDayAt('07:30'), run: { runType: 'full' } });
      const expected = [{ landscape: 'mock', thisLandscape: true, at: 'Thu, Nov 19, 2026, 7:30 AM', minutesAway: 30, description: 'At 07:30 AM (America/New_York)' }];
      expect((await offline.describe()).upcomingSchedules).toEqual(expected);
      expect((await offline.plan({ runType: 'full' })).upcomingSchedules).toEqual(expected);
    });

    it("leaves the warning out when the schedules can't be read", async () => {
      const offline = new OfflineBulkSyncService({ settings: { landscape: 'mock' }, upcomingSchedules: async () => { throw new Error('denied'); } });
      expect((await offline.describe()).upcomingSchedules).toBeUndefined();
    });
  });
});

describe('the bulk sync schedule routes, offline', () => {
  const signIn = new TestSignIn();
  const jdoe = signIn.cookie({ buPrincipal: ['jdoe'], givenName: ['Jane'], sn: ['Doe'] }, COOKIE);
  let loaded: ReturnType<typeof loadHandler>;

  jest.setTimeout(60_000);
  beforeAll(() => {
    loaded = loadHandler(signedInEnvironment(signIn));
    process.env.USER_ROLES = 'Admin';
  });
  afterAll(() => loaded.restore());
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'info').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  const call = (method: string, path: string, body?: object) => loaded.handler(event(method, path, { body, cookie: jdoe }));

  it('answers each operation with its status', async () => {
    loaded.ServiceProvider.resetOfflineState();
    expect(json(await call('GET', '/api/bulk-schedule')).schedule).toBeUndefined();

    const invalid = await call('POST', '/api/bulk-schedule/when', { when: { mode: 'simple', time: 'noon', days: 'weekdays' } });
    expect(invalid.statusCode).toBe(200);
    expect(json(invalid).errors[0].field).toBe('time');

    const tooClose = await call('PUT', '/api/bulk-schedule', { when: everyDayAt('03:30'), run: { runType: 'full' } });
    expect(tooClose.statusCode).toBe(409);
    expect(json(tooClose)).toMatchObject({ code: 'TOO_CLOSE', conflict: { landscape: 'staging' } });

    const created = await call('PUT', '/api/bulk-schedule', { when: everyDayAt('04:00'), run: { runType: 'full' } });
    expect(created.statusCode).toBe(201);
    expect(json(created).schedule).toMatchObject({ enabled: true, changedBy: 'jdoe' });
    expect((await call('PUT', '/api/bulk-schedule', { when: everyDayAt('06:00'), run: { runType: 'full' } })).statusCode).toBe(200);

    expect((await call('POST', '/api/bulk-schedule/state', {})).statusCode).toBe(400);
    expect(json(await call('POST', '/api/bulk-schedule/state', { enabled: false })).enabled).toBe(false);
    expect(json(await call('GET', '/api/bulk-schedule')).schedule).toMatchObject({ expression: 'cron(0 6 * * ? *)', enabled: false });

    expect(json(await call('DELETE', '/api/bulk-schedule'))).toEqual({ deleted: true });
    expect((await call('POST', '/api/bulk-schedule/state', { enabled: true })).statusCode).toBe(404);
  });

  it("answers a route's wrong method 405, and a body that isn't JSON 400", async () => {
    expect((await call('GET', '/api/bulk-schedule/when')).statusCode).toBe(405);
    const notJson = await loaded.handler({ ...event('POST', '/api/bulk-schedule/plan', { cookie: jdoe }), body: '{' } as any);
    expect(notJson.statusCode).toBe(400);
  });
});
