import { RemovalPolicy, Tags } from 'aws-cdk-lib';
import { IVpc, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { Cluster, ClusterProps, ContainerInsights } from 'aws-cdk-lib/aws-ecs';
import { IFunction } from 'aws-cdk-lib/aws-lambda';
import { IQueue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { ChunkerService } from './services/chunker/ChunkerService';
import { MergerService } from './services/merger/MergerService';
import { ProcessorService } from './services/processor/ProcessorService';
import { TaskDefinitions } from './TaskDefinitions';
import { Config } from 'integration-huron-person';
import { DynamoDbTables } from './DynamoDB';
import { HuronPersonSecrets } from './Secrets';

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
  public mergerService?: MergerService;
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
     * IMPORTANT GOTCHA: If you start importing existing vpcs that the CDK formerly created 
     * (with RETAIN on delete policy), you may run into cloudformation errors like the following:
     * 
     * The error message is: Resource handler returned message: "Invalid request provided: 
     * Error retrieving subnet information for [subnet-0d62741a9b460cc61, 
     * subnet-058326610d44d9229]: The subnet ID 'subnet-058326610d44d9229' does not exist 
     * (ErrorCode: InvalidSubnetID.NotFound) (Service: Ecs, Status Code: 400, Request ID: 
     * ae376d15-f8f6-4a6c-b715-2f1efd5cfa5a) (SDK Attempt Count: 1)" (RequestToken: 
     * 7815edf1-d718-b924-fe0a-e25209ebad2d, HandlerErrorCode: InvalidRequest)
     * 
     * CDK's Vpc.fromLookup() writes results to cdk.context.json and doesn't re-lookup on 
     * subsequent deployments. So even though you're importing the VPC correctly, CDK is using 
     * stale subnet information from when those subnets existed.
     * 
     * The problem is the CDK is still using the old cached context that references the deleted 
     * subnets, so you must perform the following remedy:
     * 
     * Clear CDK Context
     * -----------------------------------
     * Clear the context cache:
     * cdk context --clear
     * 
     * Or delete the specific VPC context:
     * Look for your VPC in cdk.context.json and remove that entry
     * Or delete the entire cdk.context.json file
     * 
     * Force a fresh lookup:
     * cdk synth --force
     * 
     * Then deploy:
     * cdk deploy
     */
    if(ctx.ECS.vpcId) {
      // Use existing VPC if vpcId is provided
      this.vpc = Vpc.fromLookup(this, 'Vpc', { vpcId: ctx.ECS.vpcId });
    } 
    else {
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
      this.vpc.applyRemovalPolicy(RemovalPolicy.RETAIN); // Retain VPC on stack deletion for cleanup
    }

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
      dynamoDbTables, // Pass DynamoDB table name for processor task definition
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
      maxScalingCapacity: 1, // Merging is less frequent, lower max
      stackScope: this.stackScope,  // Pass stack reference for escape hatches
      context: this.context,  // Required for AbstractService (landscape suffix)
      tags: this.tags,
    });

    return this.mergerService;
  }
}
