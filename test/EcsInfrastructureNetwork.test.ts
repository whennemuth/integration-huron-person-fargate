/**
 * Tests that a pipeline stack's ECS infrastructure (lib/EcsInfrastructure.ts) runs in the shared network from the
 * network stack's SSM parameters (lib/NetworkStack.ts): it creates no VPC or subnets of its own and looks nothing up
 * at synth, and its cluster and services take the VPC and private subnets from the parameters, which CloudFormation
 * resolves at deploy. The task definitions are stand-ins: what they hold isn't what's tested here.
 */
import { App, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { IContext } from '../context/IContext';
import { EcsInfrastructure } from '../lib/EcsInfrastructure';
import { NetworkParameters } from '../lib/NetworkStack';

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

const context = {
  STACK_ID: 'huron-person-fargate-processor',
  REGION: 'us-east-2',
  ACCOUNT: '123456789012',
  TAGS: { Landscape: 'staging', Service: 'research-admin', Function: 'huron-person' },
  ECS: { processorService: { maxScalingCapacity: 2 } },
} as unknown as IContext;

describe('the pipeline stack in the shared network', () => {
  const app = new App();
  const stack = new Stack(app, 'huron-person-fargate-processor-staging', { env: { account: context.ACCOUNT, region: context.REGION } });
  const ecs = new EcsInfrastructure(stack, 'Ecs', {
    repository: {} as any, context, huronPersonSecrets: {} as any, config: {} as any, stackScope: stack, dynamoDbTables: {} as any,
  });
  ecs.createProcessorService(new Queue(stack, 'ProcessorQueue'), new Queue(stack, 'ProcessorDlq'));
  const template = Template.fromStack(stack);
  const json = template.toJSON();
  /** The logical ID of the CloudFormation parameter that reads an SSM parameter */
  const parameterFor = (name: string): string => {
    const found = Object.entries(json.Parameters ?? {}).find(([, parameter]: any) => parameter.Default === name);
    expect(found).toBeDefined();
    return found![0];
  };

  it('creates no network of its own', () => {
    for (const type of ['AWS::EC2::VPC', 'AWS::EC2::Subnet', 'AWS::EC2::InternetGateway', 'AWS::EC2::NatGateway', 'AWS::EC2::RouteTable']) {
      template.resourceCountIs(type, 0);
    }
  });

  it('reads the VPC and its subnets from the network stack\'s parameters, so nothing is looked up at synth', () => {
    expect(ecs.vpc.availabilityZones).toEqual(['us-east-2a', 'us-east-2b']);
    const ssmParameters = Object.values(json.Parameters ?? {})
      .filter((parameter: any) => parameter.Type === 'AWS::SSM::Parameter::Value<String>')
      .map((parameter: any) => parameter.Default);
    expect(ssmParameters).toEqual(expect.arrayContaining([
      NetworkParameters.VPC_ID,
      NetworkParameters.subnetId('private', 'us-east-2a'),
      NetworkParameters.subnetId('private', 'us-east-2b'),
    ]));
    // A lookup would have asked for (and cached) the VPC's description in the app's context
    expect(app.node.tryGetContext(`vpc-provider:account=${context.ACCOUNT}:filter.vpc-id=vpc-08291eb088cf2e883:region=${context.REGION}:returnAsymmetricSubnets=true`)).toBeUndefined();
    expect(stack.node.metadata.filter(entry => String(entry.data).includes('vpc-provider'))).toEqual([]);
  });

  it("runs the service's tasks in the private subnets, with a security group in the VPC, from the parameters", () => {
    const [service] = Object.values(template.findResources('AWS::ECS::Service')) as any[];
    expect(service.Properties.NetworkConfiguration.AwsvpcConfiguration.Subnets).toEqual([
      { Ref: parameterFor(NetworkParameters.subnetId('private', 'us-east-2a')) },
      { Ref: parameterFor(NetworkParameters.subnetId('private', 'us-east-2b')) },
    ]);
    expect(service.Properties.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp).toBe('DISABLED');
    template.hasResourceProperties('AWS::EC2::SecurityGroup', { VpcId: { Ref: parameterFor(NetworkParameters.VPC_ID) } });
  });
});
