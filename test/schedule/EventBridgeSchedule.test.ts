/**
 * Tests the SDK adapter for one EventBridge Scheduler schedule (src/schedule/EventBridgeSchedule.ts): reading it,
 * creating it to invoke once on time, and above all that update() sends back every field it read with only the change
 * (UpdateSchedule resets anything it isn't sent), plus not-found, conflict and validation handling.
 */
import {
  ConflictException, CreateScheduleCommand, DeleteScheduleCommand, GetScheduleCommand, GetScheduleCommandOutput,
  ListSchedulesCommand, ResourceNotFoundException, SchedulerClient, UpdateScheduleCommand, ValidationException
} from '@aws-sdk/client-scheduler';
import { mockClient } from 'aws-sdk-client-mock';
import { EventBridgeSchedule } from '../../src/schedule/EventBridgeSchedule';
import { ScheduleError } from '../../src/schedule/ScheduleError';

const scheduler = mockClient(SchedulerClient);
const client = new SchedulerClient({ region: 'us-east-2' });
const NAME = 'chunker-api-schedule-mock';

/** A schedule as GetSchedule returns it, with every field set */
const stored: GetScheduleCommandOutput = {
  $metadata: {},
  Arn: `arn:aws:scheduler:us-east-2:123456789012:schedule/default/${NAME}`,
  GroupName: 'default',
  Name: NAME,
  ScheduleExpression: 'cron(0 4 ? * MON-FRI *)',
  ScheduleExpressionTimezone: 'America/New_York',
  StartDate: new Date('2026-01-01T00:00:00Z'),
  EndDate: new Date('2030-01-01T00:00:00Z'),
  Description: 'Bulk sync schedule for mock',
  State: 'ENABLED',
  KmsKeyArn: 'arn:aws:kms:us-east-2:123456789012:key/abc',
  CreationDate: new Date('2026-11-01T00:00:00Z'),
  LastModificationDate: new Date('2026-11-02T00:00:00Z'),
  FlexibleTimeWindow: { Mode: 'OFF' },
  ActionAfterCompletion: 'NONE',
  Target: {
    Arn: 'arn:aws:lambda:us-east-2:123456789012:function:chunker-subscriber-mock',
    RoleArn: 'arn:aws:iam::123456789012:role/chunker-schedule-mock',
    Input: JSON.stringify({ baseUrl: 'https://source.example.edu', fetchPath: '/people' }),
    RetryPolicy: { MaximumRetryAttempts: 0, MaximumEventAgeInSeconds: 86400 },
    DeadLetterConfig: { Arn: 'arn:aws:sqs:us-east-2:123456789012:dlq' },
  },
};

/** Everything UpdateSchedule should be sent to leave the stored schedule as it is */
const { Arn, CreationDate, LastModificationDate, $metadata, ...unchanged } = stored;

const notFound = () => new ResourceNotFoundException({ message: 'Schedule not found', Message: 'Schedule not found', $metadata: {} });

describe('EventBridgeSchedule', () => {
  const schedule = new EventBridgeSchedule({ name: NAME, client });

  beforeEach(() => {
    scheduler.reset();
  });

  describe('get', () => {
    it('reads the schedule', async () => {
      scheduler.on(GetScheduleCommand, { Name: NAME, GroupName: 'default' }).resolves(stored);
      const snapshot = await schedule.get();
      expect(snapshot).toBeDefined();
      expect(snapshot!.enabled).toBe(true);
      expect(snapshot!.expression).toBe('cron(0 4 ? * MON-FRI *)');
      expect(snapshot!.timezone).toBe('America/New_York');
      expect(snapshot!.input).toEqual({ baseUrl: 'https://source.example.edu', fetchPath: '/people' });
      expect(snapshot!.cronSchedule!.describe()).toBe('At 04:00 AM, Monday through Friday (America/New_York)');
      expect((await schedule.getCronSchedule())!.getAwsExpression()).toBe('cron(0 4 ? * MON-FRI *)');
    });

    it('gives undefined when there is no schedule by the name', async () => {
      scheduler.on(GetScheduleCommand).rejects(notFound());
      expect(await schedule.get()).toBeUndefined();
      expect(await schedule.getCronSchedule()).toBeUndefined();
    });

    it('reads a schedule without a zone in UTC, and gives no cron schedule for a rate() expression', async () => {
      scheduler.on(GetScheduleCommand).resolves({ ...stored, ScheduleExpressionTimezone: undefined, ScheduleExpression: 'rate(1 day)' });
      const snapshot = await schedule.get();
      expect(snapshot!.timezone).toBe('UTC');
      expect(snapshot!.cronSchedule).toBeUndefined();
    });
  });

  describe('create', () => {
    const definition = {
      expression: 'cron(0 4 ? * MON-FRI *)',
      timezone: 'America/New_York',
      enabled: false,
      description: 'Bulk sync schedule for mock',
      targetArn: 'arn:aws:lambda:us-east-2:123456789012:function:chunker-subscriber-mock',
      roleArn: 'arn:aws:iam::123456789012:role/chunker-schedule-mock',
      input: '{"populationType":"person-full"}',
    };

    it('creates a schedule that invokes once, on time: no retries and no flexible window', async () => {
      scheduler.on(CreateScheduleCommand).resolves({ ScheduleArn: 'arn' });
      await schedule.create(definition);
      expect(scheduler.commandCalls(CreateScheduleCommand)[0].args[0].input).toEqual({
        Name: NAME,
        GroupName: 'default',
        ScheduleExpression: 'cron(0 4 ? * MON-FRI *)',
        ScheduleExpressionTimezone: 'America/New_York',
        State: 'DISABLED',
        Description: 'Bulk sync schedule for mock',
        FlexibleTimeWindow: { Mode: 'OFF' },
        Target: {
          Arn: definition.targetArn,
          RoleArn: definition.roleArn,
          Input: definition.input,
          RetryPolicy: { MaximumRetryAttempts: 0 },
        },
      });
    });

    it('refuses a name that is taken, as EXISTS', async () => {
      scheduler.on(CreateScheduleCommand).rejects(new ConflictException({ message: 'exists', Message: 'exists', $metadata: {} }));
      await expect(schedule.create(definition)).rejects.toMatchObject({ code: 'EXISTS' });
    });

    it("passes on AWS refusing the expression as INVALID_CRON, and anything else as it is", async () => {
      const badExpression = new ValidationException({ message: 'Invalid Schedule Expression cron(x).', Message: 'Invalid Schedule Expression cron(x).', $metadata: {} });
      const roleProblem = new ValidationException({ message: 'The role is not authorized', Message: 'The role is not authorized', $metadata: {} });
      scheduler.on(CreateScheduleCommand).rejectsOnce(badExpression).rejectsOnce(roleProblem);
      await expect(schedule.create(definition)).rejects.toMatchObject({ code: 'INVALID_CRON' });
      await expect(schedule.create(definition)).rejects.toBe(roleProblem);
    });
  });

  describe('update', () => {
    beforeEach(() => {
      scheduler.on(GetScheduleCommand).resolves(stored);
      scheduler.on(UpdateScheduleCommand).resolves({ ScheduleArn: Arn });
    });

    it('sends every field it read back, with only the change, and none of the read-only ones', async () => {
      await schedule.update({ ScheduleExpression: 'cron(0 5 * * ? *)' });
      const sent = scheduler.commandCalls(UpdateScheduleCommand)[0].args[0].input;
      expect(sent).toEqual({ ...unchanged, ScheduleExpression: 'cron(0 5 * * ? *)' });
      expect(sent).not.toHaveProperty('Arn');
      expect(sent).not.toHaveProperty('CreationDate');
    });

    it('takes a change worked out from the schedule as read, e.g. part of the target', async () => {
      await schedule.update(current => ({ Target: { ...current.target!, Input: '{"new":true}' } }));
      const sent = scheduler.commandCalls(UpdateScheduleCommand)[0].args[0].input;
      expect(sent).toEqual({ ...unchanged, Target: { ...stored.Target, Input: '{"new":true}' } });
    });

    it('turns the schedule off, with any other change at the same time', async () => {
      await schedule.setEnabled(false, { Description: 'Changed' });
      const sent = scheduler.commandCalls(UpdateScheduleCommand)[0].args[0].input;
      expect(sent).toEqual({ ...unchanged, State: 'DISABLED', Description: 'Changed' });
    });

    it('refuses to change a schedule that isn\'t there', async () => {
      scheduler.on(GetScheduleCommand).rejects(notFound());
      await expect(schedule.update({ State: 'DISABLED' })).rejects.toBeInstanceOf(ScheduleError);
      await expect(schedule.update({ State: 'DISABLED' })).rejects.toMatchObject({ code: 'NOT_FOUND' });
      expect(scheduler.commandCalls(UpdateScheduleCommand)).toHaveLength(0);
    });
  });

  describe('delete', () => {
    it('says whether there was a schedule to delete', async () => {
      scheduler.on(DeleteScheduleCommand).resolvesOnce({}).rejectsOnce(notFound());
      expect(await schedule.delete()).toBe(true);
      expect(await schedule.delete()).toBe(false);
    });
  });

  describe('list', () => {
    it('lists the names with a prefix, every page of them', async () => {
      scheduler.on(ListSchedulesCommand, { NextToken: undefined }).resolves({ Schedules: [{ Name: 'chunker-api-schedule-mock' }], NextToken: 'page-2' } as any);
      scheduler.on(ListSchedulesCommand, { NextToken: 'page-2' }).resolves({ Schedules: [{ Name: 'chunker-api-schedule-staging' }] } as any);
      expect(await EventBridgeSchedule.list('chunker-api-schedule-', { client })).toEqual([
        'chunker-api-schedule-mock', 'chunker-api-schedule-staging',
      ]);
      expect(scheduler.commandCalls(ListSchedulesCommand)[0].args[0].input).toMatchObject({ NamePrefix: 'chunker-api-schedule-', GroupName: 'default' });
    });
  });
});
