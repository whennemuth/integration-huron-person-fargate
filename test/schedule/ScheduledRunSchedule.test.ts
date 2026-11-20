/**
 * Tests a landscape's bulk sync schedule (src/schedule/ScheduledRun.ts, the instance side) against a mocked
 * EventBridge Scheduler: what get() and preview() report, and the bulk sync rules when creating, turning on, and
 * changing a schedule: at most once a day, and an hour from other landscapes' enabled schedules that read the real
 * source (the landscapes share one source API), never checked for a schedule reading the source simulator.
 */
import {
  CreateScheduleCommand, DeleteScheduleCommand, GetScheduleCommand, GetScheduleCommandOutput, ListSchedulesCommand,
  ResourceNotFoundException, SchedulerClient, UpdateScheduleCommand
} from '@aws-sdk/client-scheduler';
import { mockClient } from 'aws-sdk-client-mock';
import { ScheduledRun } from '../../src/schedule/ScheduledRun';

const scheduler = mockClient(SchedulerClient);
const client = new SchedulerClient({ region: 'us-east-2' });

/** Thursday Nov 19 2026, 7:00 AM in New York */
const now = new Date('2026-11-19T12:00:00Z');
const roleArn = 'arn:aws:iam::123456789012:role/chunker-schedule-mock';
const targetArn = 'arn:aws:lambda:us-east-2:123456789012:function:chunker-subscriber-mock';
const realSource = ScheduledRun.scheduleInput({ baseUrl: 'https://source.example.edu', fetchPath: '/people', iterationLimit: 10 });
const simulatorSource = { ...realSource, termsEndpoint: { baseUrl: 'https://sim.lambda-url.us-east-2.on.aws', fetchPath: '/terms' } };

const stored = (landscape: string, params: { expression: string; enabled: boolean; input?: object; description?: string }): GetScheduleCommandOutput => ({
  $metadata: {},
  Arn: `arn:aws:scheduler:us-east-2:123456789012:schedule/default/chunker-api-schedule-${landscape}`,
  Name: `chunker-api-schedule-${landscape}`,
  GroupName: 'default',
  ScheduleExpression: params.expression,
  ScheduleExpressionTimezone: 'America/New_York',
  State: params.enabled ? 'ENABLED' : 'DISABLED',
  Description: params.description,
  FlexibleTimeWindow: { Mode: 'OFF' },
  Target: { Arn: targetArn, RoleArn: roleArn, Input: JSON.stringify(params.input ?? realSource), RetryPolicy: { MaximumRetryAttempts: 0 } },
});

/** The schedules EventBridge has, by landscape: what ListSchedules lists and GetSchedule reads */
const given = (schedules: Record<string, GetScheduleCommandOutput>) => {
  scheduler.on(ListSchedulesCommand).resolves({ Schedules: Object.values(schedules).map(({ Name }) => ({ Name })) } as any);
  scheduler.on(GetScheduleCommand).rejects(new ResourceNotFoundException({ message: 'not found', Message: 'not found', $metadata: {} }));
  for (const schedule of Object.values(schedules)) {
    scheduler.on(GetScheduleCommand, { Name: schedule.Name }).resolves(schedule);
  }
};

/** staging, on, reading the real source, at 4:30 AM every day */
const stagingAt430 = stored('staging', { expression: 'cron(30 4 * * ? *)', enabled: true });

describe("ScheduledRun: a landscape's schedule", () => {
  const mock = new ScheduledRun({ landscape: 'mock', roleArn, targetArn, client });

  beforeEach(() => {
    scheduler.reset();
    scheduler.on(CreateScheduleCommand).resolves({ ScheduleArn: 'arn' });
    scheduler.on(UpdateScheduleCommand).resolves({ ScheduleArn: 'arn' });
  });

  describe('get', () => {
    it('reports the schedule in words, with its next runs, the choices that show it, and who changed it', async () => {
      given({ mock: stored('mock', { expression: 'cron(0 4 ? * MON-FRI *)', enabled: true, description: 'Bulk sync schedule for mock. Changed by jdoe at 2026-11-18T15:00:00.000Z' }) });
      const info = await mock.get();
      expect(info).toMatchObject({
        name: 'chunker-api-schedule-mock',
        enabled: true,
        expression: 'cron(0 4 ? * MON-FRI *)',
        timezone: 'America/New_York',
        description: 'At 04:00 AM, Monday through Friday (America/New_York)',
        choices: { mode: 'simple', time: '04:00', days: 'weekdays' },
        input: realSource,
        realSource: true,
        changedBy: 'jdoe',
        changedAt: '2026-11-18T15:00:00.000Z',
      });
      expect(info!.nextRuns).toHaveLength(3);
    });

    it('gives undefined when the landscape has no schedule', async () => {
      given({});
      expect(await mock.get()).toBeUndefined();
    });
  });

  describe('preview', () => {
    it('works out the expression, its words and next runs from the choices, saving nothing', async () => {
      given({});
      const preview = await mock.preview({ mode: 'simple', time: '4:00', days: 'weekdays' }, { now });
      expect(preview).toEqual({
        choices: { mode: 'simple', time: '04:00', days: 'weekdays' },
        expression: 'cron(0 4 ? * MON-FRI *)',
        description: 'At 04:00 AM, Monday through Friday (America/New_York)',
        nextRuns: ['Fri, Nov 20, 2026, 4:00 AM', 'Mon, Nov 23, 2026, 4:00 AM', 'Tue, Nov 24, 2026, 4:00 AM'],
      });
      expect(scheduler.commandCalls(CreateScheduleCommand)).toHaveLength(0);
    });

    it('gives bad choices back as errors by field, without throwing', async () => {
      expect(await mock.preview({ mode: 'simple', time: '25:00', days: [] }, { now })).toEqual({
        errors: [
          { field: 'time', message: expect.any(String) },
          { field: 'days', message: expect.any(String) },
        ],
      });
      const badCron = await mock.preview({ mode: 'cron', expression: 'cron(0 4 * * MON *)' }, { now });
      expect(badCron.choices).toEqual({ mode: 'cron', expression: 'cron(0 4 * * MON *)' });
      expect(badCron.errors![0].message).toMatch(/must be '\?'/);
    });

    it('says a schedule that runs more than once a day is too frequent', async () => {
      given({});
      const preview = await mock.preview({ mode: 'cron', expression: 'cron(0 4,16 * * ? *)' }, { now });
      expect(preview.errors).toEqual([{ field: 'expression', message: expect.stringMatching(/more than once a day/) }]);
    });

    it("names another landscape's enabled schedule it would start within the hour of", async () => {
      given({ staging: stagingAt430 });
      const preview = await mock.preview({ mode: 'simple', time: '04:00', days: 'every-day' }, { now });
      expect(preview.conflict).toEqual({
        landscape: 'staging',
        description: 'At 04:30 AM (America/New_York)',
        at: 'Fri, Nov 20, 2026, 4:00 AM',
        otherAt: 'Fri, Nov 20, 2026, 4:30 AM',
        minutesApart: 30,
      });
    });

    it('ignores its own schedule, disabled schedules and schedules reading the source simulator', async () => {
      given({
        mock: stored('mock', { expression: 'cron(0 4 * * ? *)', enabled: true }),
        preview: stored('preview', { expression: 'cron(15 4 * * ? *)', enabled: false }),
        mock1: stored('mock1', { expression: 'cron(45 4 * * ? *)', enabled: true, input: simulatorSource }),
      });
      expect((await mock.preview({ mode: 'simple', time: '04:00', days: 'every-day' }, { now })).conflict).toBeUndefined();
    });

    it("doesn't look for conflicts for a schedule that would read the source simulator", async () => {
      given({ staging: stagingAt430 });
      const preview = await mock.preview({ mode: 'simple', time: '04:00', days: 'every-day' }, { now, realSource: false });
      expect(preview.conflict).toBeUndefined();
      expect(scheduler.commandCalls(ListSchedulesCommand)).toHaveLength(0);
    });

    it('notes daylight saving, and an expression that never runs', async () => {
      given({});
      const skipped = await mock.preview({ mode: 'simple', time: '02:30', days: 'every-day' }, { now });
      expect(skipped.notes).toEqual([expect.stringMatching(/Mar 14, 2027 the clocks go forward past 2:30 AM/)]);
      const never = await mock.preview({ mode: 'cron', expression: 'cron(0 4 30 2 ? *)' }, { now });
      expect(never.notes).toEqual(['It never runs: no day matches its expression.']);
    });
  });

  describe('create', () => {
    it('creates the schedule in the landscape\'s zone, invoking the chunker subscriber with the event', async () => {
      given({});
      await mock.create({ choices: { mode: 'simple', time: '04:00', days: 'weekdays' }, input: realSource, enabled: true, changedBy: 'jdoe', now });
      expect(scheduler.commandCalls(CreateScheduleCommand)[0].args[0].input).toMatchObject({
        Name: 'chunker-api-schedule-mock',
        ScheduleExpression: 'cron(0 4 ? * MON-FRI *)',
        ScheduleExpressionTimezone: 'America/New_York',
        State: 'ENABLED',
        Description: 'Bulk sync schedule for mock. Changed by jdoe at 2026-11-19T12:00:00.000Z',
        Target: { Arn: targetArn, RoleArn: roleArn, Input: JSON.stringify(realSource) },
      });
    });

    it('refuses to create it on within the hour of another landscape\'s, creating nothing', async () => {
      given({ staging: stagingAt430 });
      await expect(mock.create({ choices: { mode: 'simple', time: '05:00', days: 'every-day' }, input: realSource, enabled: true, now }))
        .rejects.toMatchObject({ code: 'TOO_CLOSE', details: { conflict: { landscape: 'staging', minutesApart: 30 } } });
      expect(scheduler.commandCalls(CreateScheduleCommand)).toHaveLength(0);
    });

    it('creates it off, or reading the source simulator, without looking at the others', async () => {
      given({ staging: stagingAt430 });
      await mock.create({ choices: { mode: 'simple', time: '05:00', days: 'every-day' }, input: realSource, enabled: false, now });
      await mock.create({ choices: { mode: 'simple', time: '05:00', days: 'every-day' }, input: simulatorSource, enabled: true, now });
      expect(scheduler.commandCalls(CreateScheduleCommand)).toHaveLength(2);
      expect(scheduler.commandCalls(ListSchedulesCommand)).toHaveLength(0);
    });

    it('refuses choices that run more than once a day, or are invalid', async () => {
      await expect(mock.create({ choices: { mode: 'cron', expression: 'cron(0 */6 * * ? *)' }, input: realSource, enabled: false, now }))
        .rejects.toMatchObject({ code: 'TOO_FREQUENT' });
      await expect(mock.create({ choices: { mode: 'simple', time: 'noon', days: 'weekdays' }, input: realSource, enabled: false, now }))
        .rejects.toMatchObject({ code: 'INVALID_CHOICES' });
    });

    it('needs the role and the target to create one', async () => {
      await expect(new ScheduledRun({ landscape: 'mock', client }).create({ choices: { mode: 'simple', time: '04:00', days: 'weekdays' }, input: realSource, enabled: false }))
        .rejects.toThrow(/needs the scheduler role's and the chunker subscriber's ARNs/);
    });
  });

  describe('setEnabled', () => {
    it('turns the schedule on, checking it against the others first', async () => {
      given({ mock: stored('mock', { expression: 'cron(0 4 * * ? *)', enabled: false }), staging: stagingAt430 });
      await expect(mock.setEnabled(true, 'jdoe', now)).rejects.toMatchObject({ code: 'TOO_CLOSE' });
      expect(scheduler.commandCalls(UpdateScheduleCommand)).toHaveLength(0);
    });

    it('turns it on when it is clear, and off without checking, leaving everything else as it was', async () => {
      given({ mock: stored('mock', { expression: 'cron(0 2 * * ? *)', enabled: false }), staging: stagingAt430 });
      await mock.setEnabled(true, 'jdoe', now);
      await mock.setEnabled(false, undefined, now);
      const [on, off] = scheduler.commandCalls(UpdateScheduleCommand).map(call => call.args[0].input);
      expect(on).toMatchObject({ State: 'ENABLED', ScheduleExpression: 'cron(0 2 * * ? *)', Target: { Arn: targetArn, RoleArn: roleArn } });
      expect(on.Description).toBe('Bulk sync schedule for mock. Changed by jdoe at 2026-11-19T12:00:00.000Z');
      expect(off).toMatchObject({ State: 'DISABLED', Description: 'Bulk sync schedule for mock. Changed at 2026-11-19T12:00:00.000Z' });
    });

    it('turns on a schedule reading the source simulator whatever the others are', async () => {
      given({ mock: stored('mock', { expression: 'cron(0 4 * * ? *)', enabled: false, input: simulatorSource }), staging: stagingAt430 });
      await mock.setEnabled(true, 'jdoe', now);
      expect(scheduler.commandCalls(UpdateScheduleCommand)).toHaveLength(1);
    });

    it('refuses when the landscape has no schedule', async () => {
      given({});
      await expect(mock.setEnabled(true, 'jdoe', now)).rejects.toMatchObject({ code: 'NOT_FOUND' });
    });
  });

  describe('setCron', () => {
    it("changes when it runs, in the landscape's zone", async () => {
      given({ mock: stored('mock', { expression: 'cron(0 2 * * ? *)', enabled: true }) });
      await mock.setCron({ mode: 'simple', time: '03:15', days: ['MON', 'THU'] }, 'jdoe', now);
      expect(scheduler.commandCalls(UpdateScheduleCommand)[0].args[0].input).toMatchObject({
        State: 'ENABLED',
        ScheduleExpression: 'cron(15 3 ? * MON,THU *)',
        ScheduleExpressionTimezone: 'America/New_York',
      });
    });

    it('checks the new time against the others while the schedule is on, and not while it is off', async () => {
      given({ mock: stored('mock', { expression: 'cron(0 2 * * ? *)', enabled: true }), staging: stagingAt430 });
      await expect(mock.setCron({ mode: 'simple', time: '05:00', days: 'every-day' }, 'jdoe', now)).rejects.toMatchObject({ code: 'TOO_CLOSE' });
      given({ mock: stored('mock', { expression: 'cron(0 2 * * ? *)', enabled: false }), staging: stagingAt430 });
      await mock.setCron({ mode: 'simple', time: '05:00', days: 'every-day' }, 'jdoe', now);
      expect(scheduler.commandCalls(UpdateScheduleCommand)).toHaveLength(1);
    });
  });

  describe('replaceInput', () => {
    it('changes the event it sends, keeping the rest of the target', async () => {
      given({ mock: stored('mock', { expression: 'cron(0 2 * * ? *)', enabled: true }) });
      const input = { ...realSource, parallelStart: 15 };
      await mock.replaceInput(input, 'jdoe', now);
      expect(scheduler.commandCalls(UpdateScheduleCommand)[0].args[0].input.Target).toEqual({
        Arn: targetArn, RoleArn: roleArn, Input: JSON.stringify(input), RetryPolicy: { MaximumRetryAttempts: 0 },
      });
    });

    it('checks the spacing when an enabled schedule switches from the simulator to the real source', async () => {
      given({ mock: stored('mock', { expression: 'cron(0 4 * * ? *)', enabled: true, input: simulatorSource }), staging: stagingAt430 });
      await expect(mock.replaceInput(realSource, 'jdoe', now)).rejects.toMatchObject({ code: 'TOO_CLOSE' });
    });
  });

  it('deletes the schedule, saying whether there was one', async () => {
    scheduler.on(DeleteScheduleCommand).resolvesOnce({}).rejectsOnce(new ResourceNotFoundException({ message: 'gone', Message: 'gone', $metadata: {} }));
    expect(await mock.delete()).toBe(true);
    expect(await mock.delete()).toBe(false);
  });

  it("lists the other landscapes' schedules", async () => {
    given({ mock: stored('mock', { expression: 'cron(0 0 * * ? *)', enabled: false }), staging: stagingAt430 });
    expect(await mock.otherSchedules()).toEqual([expect.objectContaining({
      landscape: 'staging', name: 'chunker-api-schedule-staging', enabled: true, realSource: true, description: 'At 04:30 AM (America/New_York)',
    })]);
  });
});
