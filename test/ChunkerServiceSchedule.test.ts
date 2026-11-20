/**
 * Tests what the chunker service deploys for the landscape's bulk sync schedule (lib/services/chunker/ChunkerService.ts):
 * never the schedule itself, which ScheduledRun manages at run time (bulk-sync-schedule-plan.md, Option A), whatever
 * the context or the integration config says; only the role the schedule invokes the chunker subscriber with, named
 * so ScheduledRun can pass it, trusted by EventBridge Scheduler for this account, and allowed nothing but invoking the
 * subscriber. The task definitions are stand-ins, as in EcsInfrastructureNetwork.test.ts.
 */
import { App, Stack } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { Code, Function, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { IContext } from '../context/IContext';
import { EcsInfrastructure } from '../lib/EcsInfrastructure';
import { ScheduledRun } from '../src/schedule/ScheduledRun';

jest.mock('../lib/TaskDefinitions', () => {
  const { ContainerImage, FargateTaskDefinition } = jest.requireActual('aws-cdk-lib/aws-ecs');
  const { Construct } = jest.requireActual('constructs');
  return {
    TaskDefinitions: class extends Construct {
      constructor(scope: any, id: string) {
        super(scope, id);
        for (const name of ['chunker', 'processor', 'postProcessor']) {
          const taskDefinition = new FargateTaskDefinition(this, `${name}TaskDefinition`);
          taskDefinition.addContainer('app', { image: ContainerImage.fromRegistry('example/app') });
          (this as any)[name] = { taskDefinition };
        }
      }
    }
  };
});

const people = {
  endpointConfig: { baseUrl: 'https://source.example.edu', apiKey: 'not-a-key' },
  fetchPath: '/people',
  fetchSchedule: { enabled: true, cronExpression: 'cron(0 2 * * ? *)' },
};

const context = {
  STACK_ID: 'huron-person-fargate-processor',
  REGION: 'us-east-2',
  ACCOUNT: '123456789012',
  TAGS: { Landscape: 'Staging', Service: 'research-admin', Function: 'huron-person' },
  ECS: { chunkerService: { maxScalingCapacity: 1, chunksPerTask: 10 } },
  HURON_PERSON_CONFIG: { dataSource: { people } },
  SCHEDULE: { cronExpression: 'cron(0 3 * * ? *)' },
} as unknown as IContext;

/** Synthesize a stack with the chunker service, with or without the chunker subscriber */
const synthesize = (withSubscriber = true): { template: Template; stack: Stack } => {
  const app = new App();
  const stack = new Stack(app, 'huron-person-fargate-processor-staging', { env: { account: context.ACCOUNT, region: context.REGION } });
  const ecs = new EcsInfrastructure(stack, 'Ecs', {
    repository: {} as any, context, huronPersonSecrets: {} as any, config: {} as any, stackScope: stack, dynamoDbTables: {} as any,
  });
  const chunkerLambda = withSubscriber
    ? new Function(stack, 'ChunkerSubscriber', {
      functionName: 'chunker-subscriber-staging',
      runtime: Runtime.NODEJS_20_X, handler: 'index.handler', code: Code.fromInline('exports.handler = async () => {}'),
    })
    : undefined;
  ecs.createChunkerService(new Queue(stack, 'ChunkerQueue'), new Queue(stack, 'ChunkerDlq'), chunkerLambda);
  return { template: Template.fromStack(stack), stack };
};

/** The logical ID of the stack's one resource of a type that matches */
const logicalIdOf = (template: Template, type: string, properties: object): string => {
  const found = Object.keys(template.findResources(type, { Properties: properties }));
  expect(found).toHaveLength(1);
  return found[0];
};

describe("the chunker service and the landscape's bulk sync schedule", () => {
  beforeAll(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterAll(() => {
    jest.restoreAllMocks();
  });

  it('never deploys the schedule, whatever SCHEDULE or the integration config\'s fetchSchedule say', () => {
    synthesize().template.resourceCountIs('AWS::Scheduler::Schedule', 0);
  });

  it("creates the schedule's role, named as ScheduledRun names it, trusted by EventBridge Scheduler for this account only", () => {
    const { template } = synthesize();
    template.hasResourceProperties('AWS::IAM::Role', {
      RoleName: ScheduledRun.roleName('Staging'),
      AssumeRolePolicyDocument: {
        Statement: [{
          Action: 'sts:AssumeRole',
          Effect: 'Allow',
          Principal: { Service: 'scheduler.amazonaws.com' },
          Condition: { StringEquals: { 'aws:SourceAccount': context.ACCOUNT } },
        }],
      },
    });
    expect(ScheduledRun.roleName('Staging')).toBe('chunker-schedule-staging');
  });

  it('allows the role nothing but invoking the chunker subscriber', () => {
    const { template } = synthesize();
    const role = logicalIdOf(template, 'AWS::IAM::Role', { RoleName: 'chunker-schedule-staging' });
    const subscriber = logicalIdOf(template, 'AWS::Lambda::Function', { FunctionName: 'chunker-subscriber-staging' });
    const policies = template.findResources('AWS::IAM::Policy', { Properties: { Roles: [{ Ref: role }] } });
    expect(Object.values(policies)).toHaveLength(1);
    expect((Object.values(policies)[0] as any).Properties.PolicyDocument.Statement).toEqual([{
      Action: 'lambda:InvokeFunction',
      Effect: 'Allow',
      Resource: { 'Fn::GetAtt': [subscriber, 'Arn'] },
    }]);
    template.hasResourceProperties('AWS::IAM::Role', { RoleName: 'chunker-schedule-staging', ManagedPolicyArns: Match.absent() });
  });

  it('creates no role without the chunker subscriber', () => {
    synthesize(false).template.resourcePropertiesCountIs('AWS::IAM::Role', { RoleName: 'chunker-schedule-staging' }, 0);
  });
});
