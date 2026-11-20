/**
 * Tests the EventBridge schedule the chunker service deploys (lib/services/chunker/ChunkerService.ts,
 * createApiChunkingSchedule()): its cron comes from the context's SCHEDULE, it is always disabled, its endpoint comes
 * from the integration config, and without SCHEDULE there is none (bulk-sync-schedule-plan.md, step 1). The task
 * definitions are stand-ins, as in EcsInfrastructureNetwork.test.ts.
 */
import { App, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { Code, Function, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { IContext } from '../context/IContext';
import { EcsInfrastructure } from '../lib/EcsInfrastructure';

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
  fetchPath: '/people'
};

const baseContext = {
  STACK_ID: 'huron-person-fargate-processor',
  REGION: 'us-east-2',
  ACCOUNT: '123456789012',
  TAGS: { Landscape: 'Staging', Service: 'research-admin', Function: 'huron-person' },
  ECS: { chunkerService: { maxScalingCapacity: 1, chunksPerTask: 10 } },
  HURON_PERSON_CONFIG: { dataSource: { people } },
};

/** Synthesize a stack with the chunker service, given the context's SCHEDULE (or none) */
const synthesize = (overrides: Partial<IContext> = {}): Template => {
  const context = { ...baseContext, ...overrides } as unknown as IContext;
  const app = new App();
  const stack = new Stack(app, 'huron-person-fargate-processor-staging', { env: { account: context.ACCOUNT, region: context.REGION } });
  const ecs = new EcsInfrastructure(stack, 'Ecs', {
    repository: {} as any, context, huronPersonSecrets: {} as any, config: {} as any, stackScope: stack, dynamoDbTables: {} as any,
  });
  const chunkerLambda = new Function(stack, 'ChunkerSubscriber', {
    runtime: Runtime.NODEJS_20_X, handler: 'index.handler', code: Code.fromInline('exports.handler = async () => {}'),
  });
  ecs.createChunkerService(new Queue(stack, 'ChunkerQueue'), new Queue(stack, 'ChunkerDlq'), chunkerLambda);
  return Template.fromStack(stack);
};

describe("the chunker service's EventBridge schedule", () => {
  beforeAll(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterAll(() => {
    jest.restoreAllMocks();
  });

  it("takes its cron from the context's SCHEDULE, and is disabled", () => {
    const template = synthesize({ SCHEDULE: { cronExpression: 'cron(0 3 * * ? *)' } } as Partial<IContext>);
    template.resourceCountIs('AWS::Scheduler::Schedule', 1);
    template.hasResourceProperties('AWS::Scheduler::Schedule', {
      Name: 'chunker-api-schedule-staging',
      ScheduleExpression: 'cron(0 3 * * ? *)',
      State: 'DISABLED',
    });
  });

  it("sends ScheduledRun's input for the integration config's endpoint", () => {
    const template = synthesize({ SCHEDULE: { cronExpression: 'cron(0 3 * * ? *)' } } as Partial<IContext>);
    const [schedule] = Object.values(template.findResources('AWS::Scheduler::Schedule')) as any[];
    const input = JSON.parse(schedule.Properties.Target.Input);
    expect(input).toMatchObject({
      baseUrl: people.endpointConfig.baseUrl,
      fetchPath: people.fetchPath,
      populationType: 'person-full',
      iterationLimit: 10,
      bulkReset: false,
      trustPreviousStorage: true,
      startedBy: { by: 'schedule' },
    });
  });

  it('is not deployed without SCHEDULE', () => {
    synthesize().resourceCountIs('AWS::Scheduler::Schedule', 0);
  });

  it("ignores a fetchSchedule left in the integration config", () => {
    const fetchSchedule = { enabled: true, cronExpression: 'cron(0 2 * * ? *)' };
    const template = synthesize({ HURON_PERSON_CONFIG: { dataSource: { people: { ...people, fetchSchedule } } } } as unknown as Partial<IContext>);
    template.resourceCountIs('AWS::Scheduler::Schedule', 0);
  });

  it('is not deployed without an endpoint in the integration config', () => {
    const template = synthesize({
      SCHEDULE: { cronExpression: 'cron(0 3 * * ? *)' },
      HURON_PERSON_CONFIG: { dataSource: {} },
    } as unknown as Partial<IContext>);
    template.resourceCountIs('AWS::Scheduler::Schedule', 0);
  });
});
