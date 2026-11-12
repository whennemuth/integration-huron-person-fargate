/**
 * Tests the shared network stack (lib/NetworkStack.ts): the layout it synthesizes, which must match the network
 * already in the account for CloudFormation to import it; that everything in it is retained and the stack protected;
 * the SSM parameters it publishes its IDs in, and how a pipeline stack reads them; its tags; and that a pipeline stack
 * depending on it leaves it out of a destroy.
 */
import { App, Aspects, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { CfnSecurityGroup, SecurityGroup } from 'aws-cdk-lib/aws-ec2';
import { IContext } from '../context/IContext';
import { NETWORK_STACK_NAME, NetworkLayout, NetworkParameters, NetworkStack } from '../lib/NetworkStack';
import { BU_NameTagAspect } from '../lib/Tagging';

const context = {
  STACK_ID: 'huron-person-fargate-processor',
  REGION: 'us-east-2',
  ACCOUNT: '123456789012',
  TAGS: { Landscape: 'staging', Service: 'research-admin', Function: 'huron-person', CostCenter: 'cc', Ticket: 'T-1' }
} as unknown as IContext;

describe('NetworkStack', () => {
  const app = new App();
  const network = NetworkStack.create(app, context);
  const template = Template.fromStack(network);
  const resources = (type: string) => Object.values(template.findResources(type)) as any[];
  const subnets = () => resources('AWS::EC2::Subnet').map(subnet => ({
    cidr: subnet.Properties.CidrBlock,
    az: subnet.Properties.AvailabilityZone,
    public: subnet.Properties.MapPublicIpOnLaunch,
  })).sort((a, b) => a.cidr.localeCompare(b.cidr));

  it('is named for the account, not a landscape, and is protected from termination', () => {
    expect(network.stackName).toBe(NETWORK_STACK_NAME);
    expect(network.stackName).toBe('huron-integration-network');
    expect(network.terminationProtection).toBe(true);
  });

  it('lays the network out as the network in the account is laid out', () => {
    template.hasResourceProperties('AWS::EC2::VPC', {
      CidrBlock: '10.0.0.0/16', EnableDnsHostnames: true, EnableDnsSupport: true, InstanceTenancy: 'default'
    });
    expect(subnets()).toEqual([
      { cidr: '10.0.0.0/24', az: 'us-east-2a', public: true },
      { cidr: '10.0.1.0/24', az: 'us-east-2b', public: true },
      { cidr: '10.0.2.0/24', az: 'us-east-2a', public: false },
      { cidr: '10.0.3.0/24', az: 'us-east-2b', public: false },
    ]);
    template.resourceCountIs('AWS::EC2::InternetGateway', 1);
    template.resourceCountIs('AWS::EC2::VPCGatewayAttachment', 1);
    template.resourceCountIs('AWS::EC2::NatGateway', 1);
    template.resourceCountIs('AWS::EC2::EIP', 1);
    // A route table per subnet: the public ones route to the internet gateway, the private ones to the NAT gateway
    template.resourceCountIs('AWS::EC2::RouteTable', 4);
    const defaultRoutes = resources('AWS::EC2::Route').filter(route => route.Properties.DestinationCidrBlock === '0.0.0.0/0');
    expect(defaultRoutes.filter(route => route.Properties.GatewayId)).toHaveLength(2);
    expect(defaultRoutes.filter(route => route.Properties.NatGatewayId)).toHaveLength(2);
  });

  it('puts the NAT gateway in the first public subnet', () => {
    const [nat] = resources('AWS::EC2::NatGateway');
    const subnetLogicalId = nat.Properties.SubnetId.Ref;
    expect(template.toJSON().Resources[subnetLogicalId].Properties).toMatchObject({ CidrBlock: '10.0.0.0/24', AvailabilityZone: 'us-east-2a' });
  });

  it("has nothing CloudFormation can't import: no custom resource restricting the default security group", () => {
    const types = Object.values(template.toJSON().Resources).map((resource: any) => resource.Type);
    expect(types.filter(type => type.startsWith('Custom::') || type === 'AWS::Lambda::Function' || type === 'AWS::CDK::Metadata')).toEqual([]);
  });

  it('retains every resource when the stack is deleted or a resource replaced', () => {
    const all = Object.entries(template.toJSON().Resources) as [string, any][];
    expect(all.length).toBeGreaterThan(0);
    for (const [, resource] of all) {
      expect([resource.Type, resource.DeletionPolicy, resource.UpdateReplacePolicy]).toEqual([resource.Type, 'Retain', 'Retain']);
    }
  });

  it('publishes the VPC and each subnet in an SSM parameter', () => {
    const parameters = resources('AWS::SSM::Parameter').map(parameter => parameter.Properties.Name).sort();
    expect(parameters).toEqual([
      '/huron-integration-network/private-subnet-ids/us-east-2a',
      '/huron-integration-network/private-subnet-ids/us-east-2b',
      '/huron-integration-network/public-subnet-ids/us-east-2a',
      '/huron-integration-network/public-subnet-ids/us-east-2b',
      '/huron-integration-network/vpc-id',
    ]);
    const [vpcLogicalId] = Object.keys(template.findResources('AWS::EC2::VPC'));
    template.hasResourceProperties('AWS::SSM::Parameter', { Name: NetworkParameters.VPC_ID, Type: 'String', Value: { Ref: vpcLogicalId } });
  });

  it("is tagged as the pipeline stacks are, but with no Landscape: it belongs to none", () => {
    expect(NetworkStack.tags(context)).toEqual({ Service: 'research-admin', Function: 'huron-person', CostCenter: 'cc', Ticket: 'T-1' });
    const [vpc] = resources('AWS::EC2::VPC');
    const tags = Object.fromEntries(vpc.Properties.Tags.map((tag: any) => [tag.Key, tag.Value]));
    expect(tags).toMatchObject({ Service: 'research-admin', Function: 'huron-person', CostCenter: 'cc', Ticket: 'T-1' });
    expect(tags).not.toHaveProperty('Landscape');
  });
});

describe('a pipeline stack in the shared network', () => {
  const app = new App();
  const network = NetworkStack.create(app, context);
  const pipeline = new Stack(app, 'huron-person-fargate-processor-staging', { env: { account: context.ACCOUNT, region: context.REGION } });
  const vpc = NetworkParameters.vpc(pipeline, 'Vpc', context.REGION);
  new SecurityGroup(pipeline, 'ServiceSecurityGroup', { vpc });
  pipeline.addDependency(network, 'the pipeline runs in the shared network');
  const template = Template.fromStack(pipeline).toJSON();

  it('reads the VPC and subnets from the parameters at deploy, so nothing is looked up at synth', () => {
    const parameterDefaults = Object.values(template.Parameters ?? {})
      .filter((parameter: any) => parameter.Type === 'AWS::SSM::Parameter::Value<String>')
      .map((parameter: any) => parameter.Default)
      .filter((name: string) => name.startsWith(NetworkParameters.PREFIX)) // not CDK's own bootstrap version
      .sort();
    expect(parameterDefaults).toEqual([
      '/huron-integration-network/private-subnet-ids/us-east-2a',
      '/huron-integration-network/private-subnet-ids/us-east-2b',
      '/huron-integration-network/public-subnet-ids/us-east-2a',
      '/huron-integration-network/public-subnet-ids/us-east-2b',
      '/huron-integration-network/vpc-id',
    ]);
    expect(vpc.availabilityZones).toEqual(NetworkLayout.availabilityZones('us-east-2'));
    expect(vpc.privateSubnets).toHaveLength(2);
    expect(vpc.publicSubnets).toHaveLength(2);
    // A resource in the VPC takes its ID from the parameter
    const [group] = Object.values(template.Resources).filter((resource: any) => resource.Type === 'AWS::EC2::SecurityGroup') as any[];
    const vpcParameter = Object.entries(template.Parameters).find(([, parameter]: any) => parameter.Default === NetworkParameters.VPC_ID)![0];
    expect(group.Properties.VpcId).toEqual({ Ref: vpcParameter });
    expect(app.node.tryGetContext('vpc-provider')).toBeUndefined();
  });

  it('depends on the network stack, so a deploy brings it first and a destroy of the pipeline leaves it', () => {
    expect(pipeline.dependencies).toContain(network);
    expect(network.dependencies).toEqual([]);
  });
});

describe('BU_NameTagAspect', () => {
  const nameOf = (tags: { [key: string]: string }) => {
    const stack = new Stack(new App(), 'TagStack');
    const group = new CfnSecurityGroup(stack, 'Group', { groupDescription: 'test' });
    Aspects.of(stack).add(new BU_NameTagAspect(tags));
    const resource = Template.fromStack(stack).toJSON().Resources[stack.getLogicalId(group)];
    return resource.Properties.Tags?.find((tag: any) => tag.Key === 'Name')?.Value;
  };

  it("names a resource Service-Function-Landscape-<type>, leaving out a landscape a stack doesn't have", () => {
    expect(nameOf({ Service: 'svc', Function: 'fn', Landscape: 'staging' })).toBe('svc-fn-staging-sg');
    expect(nameOf({ Service: 'svc', Function: 'fn' })).toBe('svc-fn-sg');
  });
});
