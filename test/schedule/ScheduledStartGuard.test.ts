/**
 * Tests skipping a start the bulk sync schedule makes while a run is underway in its landscape
 * (src/schedule/ScheduledStartGuard.ts, through scheduledStartRefusal() in src/chunking/ChunkerSubscriber.ts): which
 * events are checked, what holds a scheduled start back (a run underway, a dashboard launch up, a run state that can't
 * be read) and what doesn't (a stalled, finished or failed run), the refusal's record for the dashboard, and the
 * chunker subscriber Lambda's environment and grants for it.
 */
import { GetParameterCommand, ParameterNotFound, PutParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import { App, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { Vpc } from 'aws-cdk-lib/aws-ec2';
import { mockClient } from 'aws-sdk-client-mock';
import { ChunkerSubscribingLambda } from '../../lib/services/chunker/ChunkerSubscribingLambda';
import { scheduledStartRefusal } from '../../src/chunking/ChunkerSubscriber';
import type { RunActivityState } from '../../src/reporting/RunActivity';
import { ScheduledRun } from '../../src/schedule/ScheduledRun';
import {
  MemoryScheduledStartRefusalRecord, ScheduledStartGuard, SsmScheduledStartRefusalRecord
} from '../../src/schedule/ScheduledStartGuard';

const NOW = Date.parse('2026-11-20T07:00:00.000Z');
const scheduledStart = ScheduledRun.scheduleInput({ baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', iterationLimit: 10 });
const dashboardStart = { ...scheduledStart, startedBy: { by: 'dashboard' as const, principal: 'jdoe' } };

/** A guard over a given latest run and launch flag, recording refusals in memory */
const guarded = (latest: RunActivityState | Error, launchExpiresAt?: string) => {
  const refusals = new MemoryScheduledStartRefusalRecord();
  const latestRun = jest.fn(async () => {
    if (latest instanceof Error) {
      throw latest;
    }
    return latest;
  });
  const guard = new ScheduledStartGuard({ latestRun, launchExpiresAt: async () => launchExpiresAt, refusals, now: () => NOW });
  return { guard, refusals, latestRun };
};

describe('ScheduledStartGuard', () => {
  beforeEach(() => {
    jest.spyOn(console, 'warn').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  it('checks only a run start the schedule makes', async () => {
    const { guard, latestRun } = guarded({ kind: 'underway', syncRunId: '2026-11-20T06:00:00.000Z' });
    expect(await guard.refusal(dashboardStart)).toBeUndefined();
    expect(await guard.refusal({ ...scheduledStart, startedBy: undefined })).toBeUndefined();
    // A chained chunker message of a scheduled run, not its start
    expect(await guard.refusal({ ...scheduledStart, offset: 2000 })).toBeUndefined();
    expect(await guard.refusal({ ...scheduledStart, chunkDirectory: 'chunks/person-full/x' })).toBeUndefined();
    expect(latestRun).not.toHaveBeenCalled();
  });

  it('skips a scheduled start while a run is underway, and records why', async () => {
    const { guard, refusals } = guarded({ kind: 'underway', syncRunId: '2026-11-20T06:00:00.000Z' });
    const reason = await guard.refusal(scheduledStart);
    expect(reason).toBe('The scheduled run was skipped: run 2026-11-20T06:00:00.000Z is underway in this landscape');
    expect(await refusals.read()).toEqual({ refusedAt: '2026-11-20T07:00:00.000Z', reason, syncRunId: '2026-11-20T06:00:00.000Z' });
  });

  it("lets it start after a stalled, finished or failed run, or none, so a dead run can't stop every scheduled run", async () => {
    for (const kind of ['stalled', 'finished', 'failed', 'none'] as const) {
      const { guard, refusals } = guarded({ kind, syncRunId: '2026-11-19T06:00:00.000Z' });
      expect(await guard.refusal(scheduledStart)).toBeUndefined();
      expect(await refusals.read()).toBeUndefined();
    }
  });

  it('skips it while a dashboard launch is up, and not once the launch has expired', async () => {
    expect(await guarded({ kind: 'finished' }, '2026-11-20T07:03:00.000Z').guard.refusal(scheduledStart))
      .toMatch(/a run started from the dashboard moments ago is starting/);
    expect(await guarded({ kind: 'finished' }, '2026-11-20T07:00:00.000Z').guard.refusal(scheduledStart)).toBeUndefined();
  });

  it("skips it when the run state can't be read: a missed day is safer than two runs at once", async () => {
    const { guard, refusals } = guarded(new Error('AccessDeniedException'));
    expect(await guard.refusal(scheduledStart)).toMatch(/couldn't be read \(AccessDeniedException\)/);
    expect((await refusals.read())!.reason).toMatch(/couldn't be read/);
  });

  it('still skips it when the refusal can\'t be recorded', async () => {
    const guard = new ScheduledStartGuard({
      latestRun: async () => ({ kind: 'underway', syncRunId: 'x' }), launchExpiresAt: async () => undefined, now: () => NOW,
      refusals: { record: async () => { throw new Error('denied'); }, read: async () => undefined },
    });
    expect(await guard.refusal(scheduledStart)).toMatch(/is underway/);
    expect(console.warn).toHaveBeenCalledWith(expect.stringMatching(/couldn't be recorded: denied/));
  });

  it('names its parameters beside the dashboard\'s launch flags', () => {
    expect(ScheduledStartGuard.launchFlagParameterName('mock')).toBe('/hp-dashboard/launch-flags/mock');
    expect(ScheduledStartGuard.refusalParameterName('mock')).toBe('/hp-dashboard/schedule-refusals/mock');
  });

  it('is absent, so nothing is checked, where the environment lacks the run list', () => {
    expect(ScheduledStartGuard.fromEnvironment({ LANDSCAPE: 'mock', REGION: 'us-east-2' })).toBeUndefined();
    expect(ScheduledStartGuard.fromEnvironment({ LANDSCAPE: 'mock', REGION: 'us-east-2', DYNAMODB_STATISTICS_TABLE_NAME: 'stats' }))
      .toBeInstanceOf(ScheduledStartGuard);
  });

  it('reads a refusal back, and nothing that isn\'t one', () => {
    expect(ScheduledStartGuard.parseRefusal('{"refusedAt":"2026-11-20T07:00:00.000Z","reason":"r","extra":1}'))
      .toEqual({ refusedAt: '2026-11-20T07:00:00.000Z', reason: 'r' });
    expect(ScheduledStartGuard.parseRefusal('{"reason":"r"}')).toBeUndefined();
    expect(ScheduledStartGuard.parseRefusal('not json')).toBeUndefined();
    expect(ScheduledStartGuard.parseRefusal(undefined)).toBeUndefined();
  });
});

describe('SsmScheduledStartRefusalRecord', () => {
  const ssm = mockClient(SSMClient);
  const record = new SsmScheduledStartRefusalRecord('mock', 'us-east-2', new SSMClient({ region: 'us-east-2' }));
  const refusal = { refusedAt: '2026-11-20T07:00:00.000Z', reason: 'The scheduled run was skipped: run x is underway in this landscape', syncRunId: 'x' };

  beforeEach(() => ssm.reset());

  it('replaces the last refusal in its parameter', async () => {
    ssm.on(PutParameterCommand).resolves({});
    await record.record(refusal);
    expect(ssm.commandCalls(PutParameterCommand)[0].args[0].input).toMatchObject({
      Name: '/hp-dashboard/schedule-refusals/mock', Value: JSON.stringify(refusal), Type: 'String', Overwrite: true,
    });
  });

  it('reads it back, or nothing when there has been none', async () => {
    ssm.on(GetParameterCommand).resolvesOnce({ Parameter: { Value: JSON.stringify(refusal) } })
      .rejectsOnce(new ParameterNotFound({ message: 'not found', $metadata: {} }));
    expect(await record.read()).toEqual(refusal);
    expect(await record.read()).toBeUndefined();
  });
});

describe('scheduledStartRefusal (the chunker subscriber)', () => {
  it("asks the guard about a scheduled start only, and lets it start when there's no guard", async () => {
    const guard = { refusal: jest.fn(async () => 'skipped') } as unknown as ScheduledStartGuard;
    expect(await scheduledStartRefusal(scheduledStart, () => guard)).toBe('skipped');
    expect(await scheduledStartRefusal(dashboardStart, () => guard)).toBeUndefined();
    expect(guard.refusal).toHaveBeenCalledTimes(1);
    expect(await scheduledStartRefusal(scheduledStart, () => undefined)).toBeUndefined();
  });
});

describe('the chunker subscriber Lambda, for the guard', () => {
  const app = new App({ context: { 'aws:cdk:bundling-stacks': [] } });
  const stack = new Stack(app, 'TestStack', { env: { account: '111111111111', region: 'us-east-2' } });
  new ChunkerSubscribingLambda(stack, 'Chunker', {
    vpc: new Vpc(stack, 'Vpc'), chunkerQueueUrl: 'https://sqs.us-east-2.amazonaws.com/111111111111/chunker', inputBucketName: 'input',
    region: 'us-east-2', landscape: 'preview', stackId: 'huron-person-fargate-processor',
    chunkerClusterName: 'huron-person-cluster-preview', chunkerServiceName: 'Chunker', timeoutSeconds: 600, memorySizeMb: 256,
    processorServiceName: 'Processor', processorMaxScalingCapacity: 3, storageType: 'dynamodb'
  });
  const template = Template.fromStack(stack);
  const statements = Object.values(template.findResources('AWS::IAM::Policy'))
    .flatMap((policy: any) => policy.Properties.PolicyDocument.Statement);
  const of = (action: string) => statements.filter((s: any) => [s.Action].flat().includes(action));

  it('names the statistics table and the storage mode in its environment', () => {
    const [fn] = Object.values(template.findResources('AWS::Lambda::Function', { Properties: { FunctionName: 'chunker-subscriber-preview' } }));
    expect(fn.Properties.Environment.Variables).toMatchObject({
      DYNAMODB_STATISTICS_TABLE_NAME: 'huron-person-fargate-processor-statistics-preview', PREVIOUS_STORAGE_TYPE: 'dynamodb',
    });
  });

  it('may read the statistics table and its indexes, and nothing else of it', () => {
    const reads = statements.filter((s: any) => JSON.stringify(s.Resource).includes('statistics-preview'));
    expect(reads.map((s: any) => s.Action)).toEqual([['dynamodb:GetItem', 'dynamodb:Query']]);
    expect(JSON.stringify(reads[0].Resource)).toContain('statistics-preview/index/*');
  });

  it("may read its landscape's launch flag and write its refusal record, and no other parameter", () => {
    expect(JSON.stringify(of('ssm:GetParameter').map((s: any) => s.Resource))).toContain('parameter/hp-dashboard/launch-flags/preview');
    expect(JSON.stringify(of('ssm:PutParameter').map((s: any) => s.Resource))).toContain('parameter/hp-dashboard/schedule-refusals/preview');
    expect(of('ssm:GetParameter')).toHaveLength(1);
    expect(of('ssm:PutParameter')).toHaveLength(1);
    expect(statements.flatMap((s: any) => [s.Action].flat()).filter((a: string) => a.startsWith('ssm:')).sort())
      .toEqual(['ssm:GetParameter', 'ssm:PutParameter']);
  });
});
