import {
  ConflictException, CreateScheduleCommand, DeleteScheduleCommand, GetScheduleCommand, GetScheduleCommandOutput,
  ListSchedulesCommand, ResourceNotFoundException, SchedulerClient, UpdateScheduleCommand
} from '@aws-sdk/client-scheduler';
import { ScheduledRun } from '../../../src/schedule/ScheduledRun';

/**
 * An in-memory EventBridge Scheduler for the offline services: it answers the five commands ScheduledRun sends (get,
 * list, create, update, delete) as the service does, including its not-found and conflict errors, so offline the real
 * ScheduledRun and its rules run unchanged. It starts with a few other landscapes' schedules (OTHER_LANDSCAPES), so
 * the spacing rule and the other landscapes' list have something to show.
 */
export class InMemorySchedulerClient {
  /**
   * Other landscapes' schedules to start with: staging on weekdays at 3:00 AM and preview every day at 5:00 AM, both
   * on and reading the real source; mock1 every day at 4:30 AM reading the source simulator (never in the way).
   */
  static readonly OTHER_LANDSCAPES: { landscape: string; expression: string; enabled: boolean; simulator?: boolean }[] = [
    { landscape: 'staging', expression: 'cron(0 3 ? * MON-FRI *)', enabled: true },
    { landscape: 'preview', expression: 'cron(0 5 * * ? *)', enabled: true },
    { landscape: 'mock1', expression: 'cron(30 4 * * ? *)', enabled: true, simulator: true },
  ];

  private readonly schedules = new Map<string, GetScheduleCommandOutput>();

  /** @param landscape The landscape the offline services play, whose own schedule isn't seeded */
  constructor(landscape: string, private readonly now: () => Date = () => new Date()) {
    for (const { landscape: other, expression, enabled, simulator } of InMemorySchedulerClient.OTHER_LANDSCAPES) {
      if (other === landscape.toLowerCase()) {
        continue;
      }
      const name = ScheduledRun.scheduleName(other);
      const simulatorUrl = 'https://example-simulator.lambda-url.us-east-2.on.aws';
      const input = simulator
        ? { ...ScheduledRun.scheduleInput({ baseUrl: simulatorUrl, fetchPath: '/', iterationLimit: 10 }), termsEndpoint: { baseUrl: simulatorUrl, fetchPath: '/terms' } }
        : ScheduledRun.scheduleInput({ baseUrl: 'https://source.example.edu', fetchPath: '/people', iterationLimit: 10 });
      this.schedules.set(name, this.stored({
        Name: name,
        GroupName: 'default',
        ScheduleExpression: expression,
        ScheduleExpressionTimezone: ScheduledRun.TIMEZONE,
        State: enabled ? 'ENABLED' : 'DISABLED',
        Description: `Bulk sync schedule for ${other}. Changed by jdoe at 2026-11-18T15:00:00.000Z`,
        FlexibleTimeWindow: { Mode: 'OFF' },
        Target: {
          Arn: `arn:aws:lambda:us-east-2:123456789012:function:chunker-subscriber-${other}`,
          RoleArn: `arn:aws:iam::123456789012:role/${ScheduledRun.roleName(other)}`,
          Input: JSON.stringify(input),
          RetryPolicy: { MaximumRetryAttempts: 0 },
        },
      }));
    }
  }

  /** As a SchedulerClient, for ScheduledRun */
  asClient(): SchedulerClient {
    return this as unknown as SchedulerClient;
  }

  async send(command: unknown): Promise<unknown> {
    const input = (command as { input: any }).input;
    const existing = input?.Name ? this.schedules.get(input.Name) : undefined;
    if (command instanceof GetScheduleCommand) {
      if (!existing) {
        throw InMemorySchedulerClient.notFound(input.Name);
      }
      return structuredClone(existing);
    }
    if (command instanceof ListSchedulesCommand) {
      const prefix = input?.NamePrefix ?? '';
      return {
        Schedules: [...this.schedules.values()]
          .filter(({ Name }) => Name!.startsWith(prefix))
          .map(({ Name, GroupName, State }) => ({ Name, GroupName, State })),
      };
    }
    if (command instanceof CreateScheduleCommand) {
      if (existing) {
        throw new ConflictException({ message: `Schedule ${input.Name} already exists.`, Message: `Schedule ${input.Name} already exists.`, $metadata: {} });
      }
      const created = this.stored(input);
      this.schedules.set(input.Name, created);
      return { ScheduleArn: created.Arn };
    }
    if (command instanceof UpdateScheduleCommand) {
      if (!existing) {
        throw InMemorySchedulerClient.notFound(input.Name);
      }
      const updated = { ...this.stored(input), CreationDate: existing.CreationDate };
      this.schedules.set(input.Name, updated);
      return { ScheduleArn: updated.Arn };
    }
    if (command instanceof DeleteScheduleCommand) {
      if (!existing) {
        throw InMemorySchedulerClient.notFound(input.Name);
      }
      this.schedules.delete(input.Name);
      return {};
    }
    throw new Error(`The offline scheduler doesn't answer ${(command as object)?.constructor?.name}`);
  }

  /** A schedule as GetSchedule returns it, from what CreateSchedule or UpdateSchedule was sent */
  private stored(input: Record<string, any>): GetScheduleCommandOutput {
    const { ClientToken, ...fields } = input;
    const at = this.now();
    return {
      $metadata: {},
      ...fields,
      GroupName: fields.GroupName ?? 'default',
      Arn: `arn:aws:scheduler:us-east-2:123456789012:schedule/default/${fields.Name}`,
      CreationDate: at,
      LastModificationDate: at,
    };
  }

  private static notFound(name: string): ResourceNotFoundException {
    const message = `Schedule ${name} does not exist.`;
    return new ResourceNotFoundException({ message, Message: message, $metadata: {} });
  }
}
