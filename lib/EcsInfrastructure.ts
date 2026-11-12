import { Tags } from 'aws-cdk-lib';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { Cluster, ClusterProps, ContainerInsights } from 'aws-cdk-lib/aws-ecs';
import { IFunction } from 'aws-cdk-lib/aws-lambda';
import { IQueue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { ChunkerService } from './services/chunker/ChunkerService';
import { PostProcessorService } from './services/post-processor/PostProcessorService';
import { ProcessorService } from './services/processor/ProcessorService';
import { TaskDefinitions } from './TaskDefinitions';
import { Config } from 'integration-huron-person';
import { DynamoDbTables } from './DynamoDB';
import { HuronPersonSecrets } from './Secrets';
import { NetworkParameters } from './NetworkStack';

export const CLUSTER_BASE_NAME = 'huron-person-cluster';

export interface EcsInfrastructureProps {
  repository: IRepository;
  context: IContext;
  huronPersonSecrets: HuronPersonSecrets;  
  config: Config;
  stackScope: Construct;  // Stack reference for escape hatches
  dynamoDbTables: DynamoDbTables;
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
  public readonly huronPersonSecrets: HuronPersonSecrets;
  
  public chunkerService?: ChunkerService;
  public processorService?: ProcessorService;
  public postProcessorService?: PostProcessorService;
  private context: IContext;
  private stackScope: Construct;
  private tags?: { [key: string]: string };
  private servicesConstruct: Construct; // Logical grouping for all services

  constructor(scope: Construct, id: string, props: EcsInfrastructureProps) {
    super(scope, id);

    const { repository, context: ctx, config, huronPersonSecrets, stackScope, dynamoDbTables, tags } = props;
    this.context = ctx;
    this.huronPersonSecrets = huronPersonSecrets;
    this.stackScope = stackScope;
    this.tags = tags;

    // Create a logical grouping construct for all services
    this.servicesConstruct = new Construct(this, 'Services');

    /**
     * The shared network (lib/NetworkStack.ts), whose IDs CloudFormation reads from its SSM parameters at deploy.
     * A pipeline stack never creates or looks up a VPC. It used to: with ECS.vpcId in the context it looked one up
     * (Vpc.fromLookup), whose cdk.context.json cache kept subnet IDs after the subnets were gone; without it, it
     * created one, and the network every landscape used then belonged to that landscape's stack, so its teardown
     * deleted the subnets the others ran in (2026-11-12). The network stack is deployed first, as this stack's
     * dependency (bin/App.ts), so its parameters exist when this stack is deployed, a brand-new account included.
     */
    this.vpc = NetworkParameters.vpc(this, 'Vpc', ctx.REGION);

    // Create ECS Cluster
    const { Landscape } = ctx.TAGS;
    this.cluster = new Cluster(this, 'Cluster', {
      clusterName: `${CLUSTER_BASE_NAME}-${Landscape.toLowerCase()}`,
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
      config,
      huronPersonSecrets, // Pass HuronPersonSecrets for task definitions
      dynamoDbTables, // Pass DynamoDB tables for environment variables and IAM permissions
      tags,
    });
  }

  /**
   * Creates the ChunkerService (QueueProcessingFargateService)
   * Must be called after chunker queue is created
   */
  public createChunkerService(
    queue: IQueue,
    deadLetterQueue: IQueue,
    chunkerLambda?: IFunction
  ): ChunkerService {
    this.chunkerService = new ChunkerService(this.servicesConstruct, {
      cluster: this.cluster,
      taskDefinition: this.taskDefinitions.chunker.taskDefinition,
      vpc: this.vpc,
      queue,
      deadLetterQueue,
      maxScalingCapacity: this.context.ECS.chunkerService?.maxScalingCapacity ?? 1, // Chunking is less frequent, lower max
      stackScope: this.stackScope,  // Pass stack reference for escape hatches
      context: this.context,  // Required for AbstractService (landscape suffix)
      tags: this.tags,
      chunkerLambda,
      chunksPerTask: this.context.ECS.chunkerService?.chunksPerTask ?? 0,
    });

    return this.chunkerService;
  }

  /**
   * Creates the ProcessorService (QueueProcessingFargateService)
   * Must be called after queue and chunks bucket are created
   */
  public createProcessorService(
    queue: IQueue,
    deadLetterQueue: IQueue
  ): ProcessorService {
    this.processorService = new ProcessorService(this.servicesConstruct, {
      cluster: this.cluster,
      taskDefinition: this.taskDefinitions.processor.taskDefinition,
      vpc: this.vpc,
      queue,
      deadLetterQueue,
      maxScalingCapacity: this.context.ECS.processorService?.maxScalingCapacity ?? 1,
      stackScope: this.stackScope,  // Pass stack reference for escape hatches
      context: this.context,  // Required for AbstractService (landscape suffix)
      tags: this.tags,
    });

    return this.processorService;
  }

  /**
   * Creates the PostProcessorService (QueueProcessingFargateService)
   * Must be called after post-processor queue is created
   */
  public createPostProcessorService(
    queue: IQueue,
    deadLetterQueue: IQueue
  ): PostProcessorService {
    this.postProcessorService = new PostProcessorService(this.servicesConstruct, {
      cluster: this.cluster,
      taskDefinition: this.taskDefinitions.postProcessor.taskDefinition,
      vpc: this.vpc,
      queue,
      deadLetterQueue,
      maxScalingCapacity: 1, // Post-processing is less frequent, lower max
      stackScope: this.stackScope,  // Pass stack reference for escape hatches
      context: this.context,  // Required for AbstractService (landscape suffix)
      tags: this.tags,
    });

    return this.postProcessorService;
  }
}
