import { Tags } from 'aws-cdk-lib';
import { IVpc, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { Cluster, ClusterProps, ContainerInsights } from 'aws-cdk-lib/aws-ecs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { IQueue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { ChunkerService } from './services/chunker/ChunkerService';
import { MergerService } from './services/merger/MergerService';
import { ProcessorService } from './services/processor/ProcessorService';
import { TaskDefinitions } from './TaskDefinitions';

export interface EcsInfrastructureProps {
  repository: IRepository;
  context: IContext;
  stackScope: Construct;  // Stack reference for escape hatches
  tags?: { [key: string]: string };
}

/**
 * Wrapper construct for all ECS infrastructure
 * Creates logical grouping: Cluster, TaskDefinitions, and 3 QueueProcessingFargateServices
 */
export class EcsInfrastructure extends Construct {
  public readonly cluster: Cluster;
  public readonly vpc: IVpc;
  public readonly taskDefinitions: TaskDefinitions;
  public chunkerService?: ChunkerService;
  public processorService?: ProcessorService;
  public mergerService?: MergerService;
  private context: IContext;
  private stackScope: Construct;
  private tags?: { [key: string]: string };
  private servicesConstruct: Construct; // Logical grouping for all services

  constructor(scope: Construct, id: string, props: EcsInfrastructureProps) {
    super(scope, id);

    const { repository, context: ctx, stackScope, tags } = props;
    this.context = ctx;
    this.stackScope = stackScope;
    this.tags = tags;

    // Create a logical grouping construct for all services
    this.servicesConstruct = new Construct(this, 'Services');

    // Create VPC with private subnets and NAT gateway for security
    this.vpc = new Vpc(this, 'Vpc', {
      maxAzs: 2,  // Use 2 AZs for high availability
      natGateways: 1,  // NAT gateway for private subnet internet access (1 for cost optimization, 2 for HA)
      subnetConfiguration: [
        {
          name: 'Public',
          subnetType: SubnetType.PUBLIC,
          cidrMask: 24,
        },
        {
          name: 'Private',
          subnetType: SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 24,
        },
      ],
    });

    // Create ECS Cluster
    this.cluster = new Cluster(this, 'Cluster', {
      clusterName: ctx.ECS.clusterName,
      vpc: this.vpc,
      containerInsightsV2: ContainerInsights.ENHANCED
    } satisfies ClusterProps);

    // Add Fargate capacity providers
    this.cluster.enableFargateCapacityProviders();

    // Apply tags to cluster
    if (tags) {
      Object.entries(tags).forEach(([key, value]) => {
        Tags.of(this.cluster).add(key, value);
      });
    }

    // Task Definitions
    this.taskDefinitions = new TaskDefinitions(this, 'TaskDefs', {
      repository,
      context: ctx,
      tags,
    });
  }

  /**
   * Creates the ChunkerService (QueueProcessingFargateService)
   * Must be called after chunker queue is created
   */
  public createChunkerService(
    queue: IQueue,
    deadLetterQueue: IQueue
  ): ChunkerService {
    this.chunkerService = new ChunkerService(this.servicesConstruct, {
      cluster: this.cluster,
      taskDefinition: this.taskDefinitions.chunker.taskDefinition,
      vpc: this.vpc,
      queue,
      deadLetterQueue,
      minScalingCapacity: 0,
      maxScalingCapacity: 1, // Chunking is less frequent, lower max
      stackScope: this.stackScope,  // Pass stack reference for escape hatches
      tags: this.tags,
    });

    return this.chunkerService;
  }

  /**
   * Creates the ProcessorService (QueueProcessingFargateService)
   * Must be called after queue and chunks bucket are created
   */
  public createProcessorService(
    queue: IQueue,
    deadLetterQueue: IQueue,
    chunksBucket: Bucket
  ): ProcessorService {
    this.processorService = new ProcessorService(this.servicesConstruct, {
      cluster: this.cluster,
      taskDefinition: this.taskDefinitions.processor.taskDefinition,
      vpc: this.vpc,
      queue,
      deadLetterQueue,
      chunksBucket,
      minScalingCapacity: this.context.ECS.processorService.minScalingCapacity,
      maxScalingCapacity: this.context.ECS.processorService.maxScalingCapacity,
      stackScope: this.stackScope,  // Pass stack reference for escape hatches
      tags: this.tags,
    });

    return this.processorService;
  }

  /**
   * Creates the MergerService (QueueProcessingFargateService)
   * Must be called after merger queue is created
   */
  public createMergerService(
    queue: IQueue,
    deadLetterQueue: IQueue
  ): MergerService {
    this.mergerService = new MergerService(this.servicesConstruct, {
      cluster: this.cluster,
      taskDefinition: this.taskDefinitions.merger.taskDefinition,
      vpc: this.vpc,
      queue,
      deadLetterQueue,
      minScalingCapacity: 0,
      maxScalingCapacity: 1, // Merging is less frequent, lower max
      stackScope: this.stackScope,  // Pass stack reference for escape hatches
      tags: this.tags,
    });

    return this.mergerService;
  }
}
