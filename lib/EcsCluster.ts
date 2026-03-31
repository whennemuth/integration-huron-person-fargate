import { Tags } from 'aws-cdk-lib';
import { IVpc, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import { Cluster, ClusterProps, ContainerInsights } from 'aws-cdk-lib/aws-ecs';
import { Construct } from 'constructs';

export interface EcsClusterProps {
  clusterName: string;
  useDefaultVpc?: boolean;  // If true, looks up default VPC; if false, creates new VPC
  tags?: { [key: string]: string };
}

/**
 * Creates an ECS cluster with Fargate capacity providers
 */
export class EcsCluster extends Construct {
  public readonly cluster: Cluster;
  public readonly vpc: IVpc;

  constructor(scope: Construct, id: string, props: EcsClusterProps) {
    super(scope, id);

    // Create or lookup VPC based on configuration
    if (props.useDefaultVpc) {
      // Use default VPC (requires AWS credentials for lookup)
      this.vpc = Vpc.fromLookup(this, 'Vpc', {
        isDefault: true,
      });
    } else {
      // Create new VPC with minimal configuration for cost optimization
      this.vpc = new Vpc(this, 'Vpc', {
        maxAzs: 2,  // Use 2 AZs for high availability
        natGateways: 0,  // No NAT gateways to save costs (use public subnets)
        subnetConfiguration: [
          {
            name: 'Public',
            subnetType: SubnetType.PUBLIC,
            cidrMask: 24,
          },
        ],
      });
    }

    // Create ECS cluster
    this.cluster = new Cluster(this, 'Cluster', {
      clusterName: props.clusterName,
      vpc: this.vpc,
      containerInsightsV2: ContainerInsights.ENHANCED
    } satisfies ClusterProps);

    // Add Fargate capacity providers (for ARM64/Graviton2 support)
    this.cluster.enableFargateCapacityProviders();

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.cluster).add(key, value);
      });
    }
  }
}
