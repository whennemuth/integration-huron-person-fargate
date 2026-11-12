import { App, CfnResource, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { IpAddresses, IVpc, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { BU_NameTagAspect, TaggingAspect } from './Tagging';

/** The one network stack per account and region, shared by every landscape's pipeline stack */
export const NETWORK_STACK_NAME = 'huron-integration-network';

/**
 * The shared network's layout. In code, not in any landscape's context: every landscape's CDK app synthesizes the
 * same network stack, so a deploy from any of them must leave it as it is. It is the layout lib/EcsInfrastructure.ts
 * made with `new Vpc` before the network had a stack of its own, so the network that layout built matches it.
 */
export class NetworkLayout {
  static readonly CIDR = '10.0.0.0/16';
  /** Availability zones, by their suffix to the region's name */
  static readonly AZ_SUFFIXES = ['a', 'b'];
  static readonly SUBNET_CIDR_MASK = 24;
  /** One NAT gateway, in the first public subnet, for the private subnets' internet access (one for cost) */
  static readonly NAT_GATEWAYS = 1;

  static availabilityZones(region: string): string[] {
    return NetworkLayout.AZ_SUFFIXES.map(suffix => `${region}${suffix}`);
  }
}

export type SubnetKind = 'public' | 'private';

/**
 * The SSM parameters the network stack publishes its IDs in, and how a pipeline stack reads them. The pipeline stacks
 * take the network from these, resolved by CloudFormation at each deploy, rather than looking it up at synth: in a
 * brand-new account the network doesn't exist yet when the pipeline stack is synthesized (it is created by the same
 * deploy, first), and a lookup's cdk.context.json cache would keep subnet IDs after the subnets are gone. Unlike
 * CloudFormation exports, reading a parameter never stops the network stack from changing.
 */
export class NetworkParameters {
  static readonly PREFIX = `/${NETWORK_STACK_NAME}`;
  static readonly VPC_ID = `${NetworkParameters.PREFIX}/vpc-id`;

  static subnetId(kind: SubnetKind, availabilityZone: string): string {
    return `${NetworkParameters.PREFIX}/${kind}-subnet-ids/${availabilityZone}`;
  }

  /** In a pipeline stack: the shared network's VPC and subnets, from the parameters (their values known at deploy) */
  static vpc(scope: Construct, id: string, region: string): IVpc {
    const availabilityZones = NetworkLayout.availabilityZones(region);
    const read = (name: string) => StringParameter.valueForStringParameter(scope, name);
    return Vpc.fromVpcAttributes(scope, id, {
      vpcId: read(NetworkParameters.VPC_ID),
      availabilityZones,
      publicSubnetIds: availabilityZones.map(az => read(NetworkParameters.subnetId('public', az))),
      privateSubnetIds: availabilityZones.map(az => read(NetworkParameters.subnetId('private', az))),
    });
  }
}

export type NetworkStackProps = StackProps & { env: { account: string; region: string } };

/**
 * The network every landscape's pipeline runs in: the VPC, its public and private subnets, the internet gateway, the
 * route tables and the NAT gateway. No landscape owns it: each pipeline stack depends on it (bin/App.ts), so deploying
 * a pipeline stack deploys this one first (creating it in a new account, a no-op when unchanged), and destroying a
 * pipeline stack leaves it alone (`cdk destroy` adds a stack's dependents, never its dependencies). Termination
 * protection, and RETAIN on every resource, make removing the network deliberate.
 */
export class NetworkStack extends Stack {
  public readonly vpc: Vpc;

  constructor(scope: Construct, props: NetworkStackProps) {
    super(scope, NETWORK_STACK_NAME, {
      description: 'The network shared by every landscape of the Huron person integration',
      ...props,
      stackName: NETWORK_STACK_NAME,
      terminationProtection: true,
      // No AWS::CDK::Metadata resource: an import creates nothing besides what it imports, and this stack's first
      // deploy in an existing account is an import of the network already there
      analyticsReporting: false,
    });
    const { region } = props.env;

    this.vpc = new Vpc(this, 'Vpc', {
      ipAddresses: IpAddresses.cidr(NetworkLayout.CIDR),
      availabilityZones: NetworkLayout.availabilityZones(region),
      natGateways: NetworkLayout.NAT_GATEWAYS,
      subnetConfiguration: [
        { name: 'Public', subnetType: SubnetType.PUBLIC, cidrMask: NetworkLayout.SUBNET_CIDR_MASK },
        { name: 'Private', subnetType: SubnetType.PRIVATE_WITH_EGRESS, cidrMask: NetworkLayout.SUBNET_CIDR_MASK },
      ],
      // Its default is a custom resource, which CloudFormation can't import; nothing uses the default security group
      restrictDefaultSecurityGroup: false,
    });

    new StringParameter(this, 'VpcIdParameter', {
      parameterName: NetworkParameters.VPC_ID,
      stringValue: this.vpc.vpcId,
      description: 'The shared network\'s VPC',
    });
    const publish = (kind: SubnetKind, subnets: { subnetId: string; availabilityZone: string }[]) => {
      for (const subnet of subnets) {
        new StringParameter(this, `${kind}-${subnet.availabilityZone}-SubnetIdParameter`, {
          parameterName: NetworkParameters.subnetId(kind, subnet.availabilityZone),
          stringValue: subnet.subnetId,
          description: `The shared network's ${kind} subnet in ${subnet.availabilityZone}`,
        });
      }
    };
    publish('public', this.vpc.publicSubnets);
    publish('private', this.vpc.privateSubnets);

    // Every resource survives the stack's deletion, the parameters too: a pipeline keeps resolving them
    for (const child of this.node.findAll()) {
      if (child instanceof CfnResource) {
        child.applyRemovalPolicy(RemovalPolicy.RETAIN);
      }
    }
  }

  /** The tags the pipeline stacks get from the context's TAGS, without Landscape: the network belongs to none */
  static tags(context: IContext): { [key: string]: string } {
    const { Service, Function, CostCenter = '', Ticket = '' } = context.TAGS;
    return { Service, Function, CostCenter, Ticket };
  }

  /** Add the network stack to the app, tagged, for the pipeline stack to depend on */
  static create(app: App, context: IContext): NetworkStack {
    const network = new NetworkStack(app, { env: { account: context.ACCOUNT, region: context.REGION } });
    const tags = NetworkStack.tags(context);
    new TaggingAspect(network, tags).applyTags({ aspect: new BU_NameTagAspect(tags) });
    return network;
  }
}
