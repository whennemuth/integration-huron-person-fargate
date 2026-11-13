/**
 * Tests resetting the chunker's counters when a run starts: the shared reset (src/chunking/RunCounters.ts), the rule
 * for which events start a run, and the chunker subscriber Lambda's reset before it queues a run's first event
 * (resetCountersForRunStart() and handler() in src/chunking/ChunkerSubscriber.ts), plus that Lambda's environment and
 * grant (lib/services/chunker/ChunkerSubscribingLambda.ts).
 */
import { App, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { Vpc } from 'aws-cdk-lib/aws-ec2';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { ChunkerSubscribingLambda } from '../lib/services/chunker/ChunkerSubscribingLambda';
import { CHUNKER_COUNTER_NAME, CHUNK_ORDINAL_COUNTER_NAME } from '../src/chunking/ChunkerQueue';
import { handler, resetCountersForRunStart } from '../src/chunking/ChunkerSubscriber';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { isRunStartEvent, resetRunCounters, RUN_COUNTER_NAMES } from '../src/chunking/RunCounters';

jest.mock('../src/chunking/fetch/ChunkerApiSubscriber', () => ({
  ...jest.requireActual('../src/chunking/fetch/ChunkerApiSubscriber'),
  handleApiEvent: jest.fn(async () => ({ statusCode: 200, body: 'Message sent to chunker queue' }))
}));

const runStart = { baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: 'person-full' } as any;
const ENV = { STACK_ID: 'huron-person-fargate-processor', LANDSCAPE: 'preview', REGION: 'us-east-2', DRY_RUN: 'false' };

describe('the shared counter reset', () => {
  const dynamo = mockClient(DynamoDBDocumentClient);
  beforeEach(() => {
    dynamo.reset();
    dynamo.on(UpdateCommand).resolves({});
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  it("resets the chunker's own two counters", () => {
    expect(RUN_COUNTER_NAMES).toEqual([CHUNKER_COUNTER_NAME, CHUNK_ORDINAL_COUNTER_NAME]);
    expect(RUN_COUNTER_NAMES).toEqual(['chunker-offset-counter', 'chunker-chunk-ordinal-counter']);
  });

  it('sets each to zero in the landscape\'s atomic counter table', async () => {
    await resetRunCounters({ stackId: 'huron-person-fargate-processor', region: 'us-east-2', landscape: 'preview' });
    const inputs = dynamo.commandCalls(UpdateCommand).map(call => call.args[0].input);
    expect(inputs.map(input => input.Key)).toEqual([
      { counter_name: 'chunker-offset-counter' }, { counter_name: 'chunker-chunk-ordinal-counter' }
    ]);
    for (const input of inputs) {
      expect(input).toMatchObject({
        TableName: 'huron-person-fargate-processor-atomic-counter-preview',
        UpdateExpression: 'SET #counterValue = :zero, LastUpdated = :timestamp',
        ExpressionAttributeValues: expect.objectContaining({ ':zero': 0 })
      });
    }
  });

  it("knows a run's start: offset 0 and no chunk directory", () => {
    expect(isRunStartEvent(runStart)).toBe(true);
    expect(isRunStartEvent({ ...runStart, offset: 0 })).toBe(true);
    // The Runner's seeded messages carry the run's chunk directory, all but the first a later offset
    expect(isRunStartEvent({ ...runStart, offset: 0, chunkDirectory: 'chunks/person-full/2026-11-10T12:00:00.000Z' })).toBe(false);
    expect(isRunStartEvent({ ...runStart, offset: 400 })).toBe(false);
  });
});

describe("the chunker subscriber's reset", () => {
  beforeEach(() => jest.spyOn(console, 'warn').mockImplementation(() => undefined));
  afterEach(() => jest.restoreAllMocks());

  it("resets for a run's start, in the landscape the environment names", async () => {
    const reset = jest.fn(async () => undefined);
    expect(await resetCountersForRunStart(runStart, ENV, reset)).toBe(true);
    expect(reset).toHaveBeenCalledWith({ stackId: 'huron-person-fargate-processor', region: 'us-east-2', landscape: 'preview' });
  });

  it('leaves the counters alone for a continuing event, in DRY_RUN, or without the table named', async () => {
    const reset = jest.fn(async () => undefined);
    expect(await resetCountersForRunStart({ ...runStart, offset: 400, chunkDirectory: 'chunks/x' }, ENV, reset)).toBe(false);
    expect(await resetCountersForRunStart(runStart, { ...ENV, DRY_RUN: 'TRUE' }, reset)).toBe(false);
    expect(await resetCountersForRunStart(runStart, { ...ENV, STACK_ID: undefined }, reset)).toBe(false);
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining("Not resetting the chunker's counters"));
    expect(reset).not.toHaveBeenCalled();
  });

  it("fails rather than queue a run's start against counters it couldn't reset", async () => {
    await expect(resetCountersForRunStart(runStart, ENV, async () => { throw new Error('AccessDenied'); })).rejects.toThrow('AccessDenied');
  });

  it("resets before it queues an API event, and doesn't for one in DRY_RUN", async () => {
    const dynamo = mockClient(DynamoDBDocumentClient);
    dynamo.on(UpdateCommand).resolves({});
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    const saved = { ...process.env };
    Object.assign(process.env, ENV);
    try {
      expect(await handler(runStart)).toEqual({ statusCode: 200, body: 'Message sent to chunker queue' });
      expect(dynamo.commandCalls(UpdateCommand)).toHaveLength(2);
      expect(handleApiEvent).toHaveBeenCalledWith(runStart);
    } finally {
      process.env = saved;
      dynamo.restore();
    }
  });
});

describe('the chunker subscriber Lambda', () => {
  const app = new App({ context: { 'aws:cdk:bundling-stacks': [] } });
  const stack = new Stack(app, 'TestStack', { env: { account: '111111111111', region: 'us-east-2' } });
  new ChunkerSubscribingLambda(stack, 'Chunker', {
    vpc: new Vpc(stack, 'Vpc'), chunkerQueueUrl: 'https://sqs.us-east-2.amazonaws.com/111111111111/chunker', inputBucketName: 'input',
    region: 'us-east-2', landscape: 'preview', stackId: 'huron-person-fargate-processor',
    chunkerClusterName: 'huron-person-cluster-preview', chunkerServiceName: 'Chunker', timeoutSeconds: 600, memorySizeMb: 256
  });
  const template = Template.fromStack(stack);
  const statements = () => Object.values(template.findResources('AWS::IAM::Policy'))
    .flatMap((policy: any) => policy.Properties.PolicyDocument.Statement);

  describe('for a parallel start (src/chunking/ParallelStart.ts)', () => {
    it('has the timeout its context gives it, and names the chunker service in its environment', () => {
      template.hasResourceProperties('AWS::Lambda::Function', {
        FunctionName: 'chunker-subscriber-preview', Timeout: 600,
        Environment: { Variables: { CHUNKER_CLUSTER_NAME: 'huron-person-cluster-preview', CHUNKER_SERVICE_NAME: 'Chunker' } }
      });
    });

    it('is never retried automatically, so a failed run start is never repeated into a run already started', () => {
      template.hasResourceProperties('AWS::Lambda::EventInvokeConfig', { MaximumRetryAttempts: 0, MaximumEventAgeInSeconds: 900, Qualifier: '$LATEST' });
    });

    it('may scale only the chunker service, and read its scaling and alarms', () => {
      const ecs = statements().find((s: any) => [s.Action].flat().includes('ecs:UpdateService'));
      expect([ecs.Action].flat().sort()).toEqual(['ecs:DescribeServices', 'ecs:UpdateService']);
      expect(ecs.Resource).toBe('arn:aws:ecs:us-east-2:111111111111:service/huron-person-cluster-preview/Chunker');
      const scaling = statements().find((s: any) => [s.Action].flat().includes('application-autoscaling:DescribeScalingPolicies'));
      expect([scaling.Action].flat().sort()).toEqual(['application-autoscaling:DescribeScalableTargets', 'application-autoscaling:DescribeScalingPolicies']);
      const alarms = statements().find((s: any) => [s.Action].flat().includes('cloudwatch:DescribeAlarms'));
      expect(alarms.Resource).toBe('arn:aws:cloudwatch:us-east-2:111111111111:alarm:*');
      // Nothing that changes scaling or alarms themselves
      const actions = statements().flatMap((s: any) => [s.Action].flat());
      expect(actions.filter((a: string) => /^(application-autoscaling|cloudwatch):(Put|Delete|Register|Set)/.test(a))).toEqual([]);
    });
  });

  it('names the atomic counter table in its environment', () => {
    const [fn] = Object.values(template.findResources('AWS::Lambda::Function', { Properties: { FunctionName: 'chunker-subscriber-preview' } }));
    expect(fn.Properties.Environment.Variables).toMatchObject({ STACK_ID: 'huron-person-fargate-processor', LANDSCAPE: 'preview', REGION: 'us-east-2' });
  });

  it('may update that one table, and no other', () => {
    const statements = Object.values(template.findResources('AWS::IAM::Policy'))
      .flatMap((policy: any) => policy.Properties.PolicyDocument.Statement)
      .filter((s: any) => [s.Action].flat().some((a: string) => a.startsWith('dynamodb:')));
    expect(statements.map((s: any) => s.Action)).toEqual(['dynamodb:UpdateItem']);
    expect(JSON.stringify(statements[0].Resource)).toContain('table/huron-person-fargate-processor-atomic-counter-preview');
  });
});
