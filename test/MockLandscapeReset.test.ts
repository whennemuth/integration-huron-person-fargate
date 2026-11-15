/**
 * Tests a mock landscape's run state reset at a run's start: the shared reset (src/chunking/MockLandscapeReset.ts), the
 * chunker subscriber's refusal, reset and order (resetRefusal(), resetMockLandscapeForRunStart(), runStartRefusal() and
 * handler() in src/chunking/ChunkerSubscriber.ts), the source simulator's counter reset with the run's counters in a
 * mock landscape (src/chunking/RunCounters.ts), and the subscriber Lambda's environment and grants, which only a mock
 * landscape's has (lib/services/chunker/ChunkerSubscribingLambda.ts).
 */
import { App, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { Vpc } from 'aws-cdk-lib/aws-ec2';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { ChunkerSubscribingLambda } from '../lib/services/chunker/ChunkerSubscribingLambda';
import { BulkPurgerTables } from '../src/dynamodb/BulkPurger';
import { MockLandscapeReset, MockLandscapeResetDeps } from '../src/chunking/MockLandscapeReset';
import {
  handler, resetMockLandscapeForRunStart, resetRefusal, runStartRefusal
} from '../src/chunking/ChunkerSubscriber';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { resetRunCounters, runCounterNames } from '../src/chunking/RunCounters';
import { SIMULATOR_COUNTER_NAME } from '../src/chunking/fetch/SourceSimulator';

jest.mock('../src/chunking/fetch/ChunkerApiSubscriber', () => ({
  ...jest.requireActual('../src/chunking/fetch/ChunkerApiSubscriber'),
  handleApiEvent: jest.fn(async () => ({ statusCode: 200, body: 'Message sent to chunker queue' }))
}));

const STACK_ID = 'huron-person-fargate-processor';
const runStart = { baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: 'person-full' } as any;
const MOCK_ENV = { STACK_ID, LANDSCAPE: 'mock', REGION: 'us-east-2', CHUNKS_BUCKET_NAME: 'huron-person-chunks-mock', DRY_RUN: 'false' };

beforeEach(() => {
  jest.spyOn(console, 'log').mockImplementation(() => undefined);
  jest.spyOn(console, 'error').mockImplementation(() => undefined);
});
afterEach(() => jest.restoreAllMocks());

describe('the shared reset (MockLandscapeReset)', () => {
  const depsFor = (storageMode: 'dynamodb' | 's3') => {
    const calls: string[] = [];
    const deps: MockLandscapeResetDeps = {
      detectStorageMode: jest.fn(async () => { calls.push('detect'); return storageMode; }),
      truncateTables: jest.fn(async (tables: BulkPurgerTables[]) => { calls.push(`truncate ${tables.join(',')}`); }),
      deleteS3Baseline: jest.fn(async () => { calls.push('delete baseline'); return 2; }),
    };
    return { deps, calls };
  };

  it('DynamoDB mode: truncates the mock target, both person tables and the statistics, and leaves S3 alone', async () => {
    const { deps, calls } = depsFor('dynamodb');
    await expect(new MockLandscapeReset(deps).run()).resolves.toEqual({
      storageMode: 'dynamodb',
      tablesTruncated: ['mock_target_person', 'person_current_state', 'person_history', 'statistics']
    });
    expect(calls).toEqual(['detect', 'truncate mock_target_person,person_current_state,person_history,statistics']);
  });

  it('S3 mode: truncates the mock target and the statistics, then deletes the S3 baseline', async () => {
    const { deps, calls } = depsFor('s3');
    await expect(new MockLandscapeReset(deps).run()).resolves.toEqual({
      storageMode: 's3', tablesTruncated: ['mock_target_person', 'statistics'], baselineObjectsDeleted: 2
    });
    expect(calls).toEqual(['detect', 'truncate mock_target_person,statistics', 'delete baseline']);
  });

  it('refuses a landscape that is not a mock landscape', () => {
    expect(MockLandscapeReset.refusal('mock')).toBeUndefined();
    expect(MockLandscapeReset.refusal('mock2')).toBeUndefined();
    expect(MockLandscapeReset.refusal('staging')).toMatch(/"staging" is not a mock landscape/);
    expect(MockLandscapeReset.refusal(undefined)).toMatch(/not a mock landscape/);
  });
});

describe("the chunker subscriber's reset at a run's start", () => {
  it('has nothing to refuse when no reset is asked for, in any landscape', () => {
    expect(resetRefusal(runStart, { ...MOCK_ENV, LANDSCAPE: 'staging' })).toBeUndefined();
    expect(resetRefusal({ ...runStart, resetState: false }, { ...MOCK_ENV, LANDSCAPE: 'staging' })).toBeUndefined();
  });

  it("refuses one for an event that doesn't start a run, outside a mock landscape, or without its environment", () => {
    const reset = { ...runStart, resetState: true };
    expect(resetRefusal(reset, MOCK_ENV)).toBeUndefined();
    expect(resetRefusal({ ...reset, offset: 400, chunkDirectory: 'chunks/x' }, MOCK_ENV)).toMatch(/only for a run's first event/);
    expect(resetRefusal(reset, { ...MOCK_ENV, LANDSCAPE: 'staging' })).toMatch(/"staging" is not a mock landscape/);
    expect(resetRefusal(reset, { ...MOCK_ENV, CHUNKS_BUCKET_NAME: undefined })).toMatch(/CHUNKS_BUCKET_NAME isn't set/);
  });

  it('checks the reset before the parallel start', () => {
    const both = { ...runStart, resetState: true, parallelStart: 99 };
    expect(runStartRefusal(both, { ...MOCK_ENV, LANDSCAPE: 'staging' })).toMatch(/not a mock landscape/);
    expect(runStartRefusal(both, MOCK_ENV)).toMatch(/parallelStart must be a whole number from 1 to 20/);
    expect(runStartRefusal(runStart, MOCK_ENV)).toBeUndefined();
  });

  it('resets the landscape the environment names, only when asked, and not in DRY_RUN', async () => {
    const reset = jest.fn(async () => undefined);
    expect(await resetMockLandscapeForRunStart({ ...runStart, resetState: true }, MOCK_ENV, reset)).toBe(true);
    expect(reset).toHaveBeenCalledWith({ stackId: STACK_ID, landscape: 'mock', region: 'us-east-2', chunksBucketName: 'huron-person-chunks-mock' });
    reset.mockClear();
    expect(await resetMockLandscapeForRunStart(runStart, MOCK_ENV, reset)).toBe(false);
    expect(await resetMockLandscapeForRunStart({ ...runStart, resetState: true }, { ...MOCK_ENV, DRY_RUN: 'true' }, reset)).toBe(false);
    expect(reset).not.toHaveBeenCalled();
  });

  describe('in the handler', () => {
    const dynamo = mockClient(DynamoDBDocumentClient);
    let saved: NodeJS.ProcessEnv;
    let calls: string[];

    beforeEach(() => {
      saved = { ...process.env };
      Object.assign(process.env, MOCK_ENV);
      calls = [];
      dynamo.reset();
      dynamo.on(UpdateCommand).callsFake(input => { calls.push(`reset counter ${input.Key.counter_name}`); return {}; });
      jest.spyOn(MockLandscapeReset.prototype, 'run').mockImplementation(async () => {
        calls.push('reset run state');
        return { storageMode: 'dynamodb', tablesTruncated: [] };
      });
      (handleApiEvent as jest.Mock).mockClear();
      (handleApiEvent as jest.Mock).mockImplementation(async () => {
        calls.push('queue');
        return { statusCode: 200, body: 'Message sent to chunker queue' };
      });
    });
    afterEach(() => { process.env = saved; });
    afterAll(() => dynamo.restore());

    it('resets the run state, then the counters (the simulator\'s too), then queues the run', async () => {
      expect(await handler({ ...runStart, resetState: true })).toEqual({ statusCode: 200, body: 'Message sent to chunker queue' });
      expect(calls).toEqual([
        'reset run state',
        'reset counter chunker-offset-counter',
        'reset counter chunker-chunk-ordinal-counter',
        `reset counter ${SIMULATOR_COUNTER_NAME}`,
        'queue'
      ]);
    });

    it('refuses a reset outside a mock landscape with a 400, resetting and queuing nothing', async () => {
      process.env.LANDSCAPE = 'staging';
      const response = await handler({ ...runStart, resetState: true });
      expect(response.statusCode).toBe(400);
      expect(response.body).toMatch(/not a mock landscape/);
      expect(calls).toEqual([]);
    });

    it('resets nothing beyond the counters when no reset is asked for', async () => {
      await handler(runStart);
      expect(calls).not.toContain('reset run state');
      expect(calls[calls.length - 1]).toBe('queue');
    });
  });
});

describe("the run's counters in a mock landscape", () => {
  const dynamo = mockClient(DynamoDBDocumentClient);
  beforeEach(() => { dynamo.reset(); dynamo.on(UpdateCommand).resolves({}); });
  afterAll(() => dynamo.restore());

  it("include the source simulator's depletion counter, only in a mock landscape", async () => {
    expect(runCounterNames('mock1')).toEqual(['chunker-offset-counter', 'chunker-chunk-ordinal-counter', 'simulator-offset-counter']);
    expect(runCounterNames('staging')).toEqual(['chunker-offset-counter', 'chunker-chunk-ordinal-counter']);
    await resetRunCounters({ stackId: STACK_ID, region: 'us-east-2', landscape: 'mock' });
    expect(dynamo.commandCalls(UpdateCommand).map(call => call.args[0].input)).toEqual([
      expect.objectContaining({ TableName: `${STACK_ID}-atomic-counter-mock`, Key: { counter_name: 'chunker-offset-counter' } }),
      expect.objectContaining({ TableName: `${STACK_ID}-atomic-counter-mock`, Key: { counter_name: 'chunker-chunk-ordinal-counter' } }),
      expect.objectContaining({ TableName: `${STACK_ID}-atomic-counter-mock`, Key: { counter_name: 'simulator-offset-counter' } }),
    ]);
  });
});

describe("the chunker subscriber Lambda's reset grants", () => {
  const templateFor = (landscape: string) => {
    const app = new App({ context: { 'aws:cdk:bundling-stacks': [] } });
    const stack = new Stack(app, 'TestStack', { env: { account: '111111111111', region: 'us-east-2' } });
    new ChunkerSubscribingLambda(stack, 'Chunker', {
      vpc: new Vpc(stack, 'Vpc'), chunkerQueueUrl: 'https://sqs.us-east-2.amazonaws.com/111111111111/chunker', inputBucketName: 'input',
      region: 'us-east-2', landscape, stackId: STACK_ID, chunksBucketName: `huron-person-chunks-${landscape}`,
      chunkerClusterName: `huron-person-cluster-${landscape}`, chunkerServiceName: 'Chunker', timeoutSeconds: 600, memorySizeMb: 256
    });
    return Template.fromStack(stack);
  };
  const statementsOf = (template: Template) => Object.values(template.findResources('AWS::IAM::Policy'))
    .flatMap((policy: any) => policy.Properties.PolicyDocument.Statement);
  const environmentOf = (template: Template) => (Object.values(template.findResources('AWS::Lambda::Function'))[0] as any)
    .Properties.Environment.Variables;

  it('in a mock landscape: the chunks bucket, the four tables it truncates, and delta-storage/ only', () => {
    const template = templateFor('mock');
    expect(environmentOf(template).CHUNKS_BUCKET_NAME).toBe('huron-person-chunks-mock');
    const statements = statementsOf(template);

    const tables = statements.find((s: any) => [s.Action].flat().includes('dynamodb:Scan'));
    expect([tables.Action].flat().sort()).toEqual(['dynamodb:BatchWriteItem', 'dynamodb:DescribeTable', 'dynamodb:Scan']);
    const tableNames = JSON.stringify(tables.Resource);
    for (const table of ['mock-target-person', 'statistics', 'person-current-state', 'person-history']) {
      expect(tableNames).toContain(`table/${STACK_ID}-${table}-mock`);
    }
    expect(tables.Resource).toHaveLength(4);

    const list = statements.find((s: any) => [s.Action].flat().includes('s3:ListBucket'));
    expect(list.Resource).toBe('arn:aws:s3:::huron-person-chunks-mock');
    expect(list.Condition).toEqual({ StringLike: { 's3:prefix': ['delta-storage/*'] } });
    const remove = statements.find((s: any) => [s.Action].flat().includes('s3:DeleteObject'));
    expect(remove.Resource).toBe('arn:aws:s3:::huron-person-chunks-mock/delta-storage/*');
  });

  it('in any other landscape: none of them', () => {
    const template = templateFor('staging');
    expect(environmentOf(template).CHUNKS_BUCKET_NAME).toBeUndefined();
    const actions = statementsOf(template).flatMap((s: any) => [s.Action].flat());
    expect(actions.filter((a: string) => /^s3:|^dynamodb:(Scan|BatchWriteItem|DescribeTable)$/.test(a))).toEqual([]);
  });
});
