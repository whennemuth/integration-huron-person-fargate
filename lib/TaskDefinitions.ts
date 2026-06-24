import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { ChunkerTaskDefinition } from './services/chunker/ChunkerTaskDefinition';
import { SERVICE_LOGICAL_ID } from './services/chunker/ChunkerService';
import { MergerTaskDefinition } from './services/merger/MergerTaskDefinition';
import { ProcessorTaskDefinition } from './services/processor/ProcessorTaskDefinition';
import { HuronPersonSecrets } from './Secrets';
import { Config, TargetPersonDeleteType } from 'integration-huron-person';
import { S3Config as S3FolderConfig } from 'integration-core';
import { DynamoDbTables } from './DynamoDB';
import { CLUSTER_BASE_NAME } from './EcsInfrastructure';

export interface TaskDefinitionsProps {
  repository: IRepository;
  context: IContext;
  config?: Config;
  dynamoDbTables: DynamoDbTables;
  tags?: { [key: string]: string };
}

/**
 * Wrapper construct for all task definitions
 * Creates a logical grouping in CloudFormation
 */
export class TaskDefinitions extends Construct {
  public readonly chunker: ChunkerTaskDefinition;
  public readonly processor: ProcessorTaskDefinition;
  public readonly merger: MergerTaskDefinition;

  constructor(scope: Construct, id: string, props: TaskDefinitionsProps) {
    super(scope, id);

    const { config, repository, context: ctx, dynamoDbTables, tags } = props;
    let sharedDeltaStorageDir = 'delta-storage'; // Default value
    const { storage, storage: { type: storageType, config: storageConfig } = {} } = props.config || {};
    if(storage && storageType === 's3') {
      const { keyPrefix } = storageConfig as S3FolderConfig;
      if(keyPrefix) {
        sharedDeltaStorageDir = keyPrefix.endsWith('/') ? keyPrefix.slice(0, -1) : keyPrefix; // Remove trailing slash if present
      }
    }

    // Create Secrets Manager secret for huron-person configuration
    const huronPersonSecrets = new HuronPersonSecrets(this, props.context);

    // Chunker task definition
    this.chunker = new ChunkerTaskDefinition(this, 'chunker', {
      repository,
      cpu: ctx.ECS.chunkerTaskDefinition.cpu,
      memoryLimitMiB: ctx.ECS.chunkerTaskDefinition.memoryLimitMiB,
      memoryReservationMiB: ctx.ECS.chunkerTaskDefinition.memoryReservationMiB,
      logRetentionDays: ctx.ECS.chunkerTaskDefinition.logRetentionDays,
      inputBucketName: ctx.S3.inputBucket,
      chunksBucketName: `${ctx.S3.chunksBucket}-${ctx.TAGS.Landscape.toLowerCase()}`,
      queueUrl: '', // Will be set after queue is created
      dynamoDbTables,
      stackId: ctx.STACK_ID,
      itemsPerChunk: ctx.ITEMS_PER_CHUNK,
      huronPersonSecrets,
      sharedDeltaStorageDir,
      region: ctx.REGION,
      ecsClusterName: `${CLUSTER_BASE_NAME}-${ctx.TAGS.Landscape.toLowerCase()}`,
      maxScalingCapacity: ctx.ECS.chunkerService?.maxScalingCapacity ?? 1,
      ecsChunkerServiceName: SERVICE_LOGICAL_ID,
      landscape: ctx.TAGS.Landscape.toLowerCase(),
      dryRun: ctx.DRY_RUN?.taskdef?.chunker,
      tags,
    });

    // Processor task definition (queue URL will be set separately in Stack.ts)
    this.processor = new ProcessorTaskDefinition(this, 'processor', {
      repository,
      cpu: ctx.ECS.processorTaskDefinition.cpu,
      memoryLimitMiB: ctx.ECS.processorTaskDefinition.memoryLimitMiB,
      memoryReservationMiB: ctx.ECS.processorTaskDefinition.memoryReservationMiB,
      logRetentionDays: ctx.ECS.processorTaskDefinition.logRetentionDays,
      chunksBucketName: `${ctx.S3.chunksBucket}-${ctx.TAGS.Landscape.toLowerCase()}`,
      queueUrl: '', // Will be set after queue is created
      dynamoDbTables,
      huronPersonSecrets,
      sharedDeltaStorageDir,
      context: ctx,
      region: ctx.REGION,
      landscape: ctx.TAGS.Landscape.toLowerCase(),
      dryRun: ctx.DRY_RUN?.taskdef?.processor,
      tags,
    });

    // Merger task definition
    this.merger = new MergerTaskDefinition(this, 'merger', {
      repository,
      cpu: ctx.ECS.mergerTaskDefinition.cpu,
      memoryLimitMiB: ctx.ECS.mergerTaskDefinition.memoryLimitMiB,
      memoryReservationMiB: ctx.ECS.mergerTaskDefinition.memoryReservationMiB,
      logRetentionDays: ctx.ECS.mergerTaskDefinition.logRetentionDays,
      inputBucketName: ctx.S3.inputBucket,
      chunksBucketName: `${ctx.S3.chunksBucket}-${ctx.TAGS.Landscape.toLowerCase()}`,
      dynamoDbTables,
      huronPersonSecrets,
      sharedDeltaStorageDir,
      personDeleteType: props.config?.dataTarget?.personDeleteType || TargetPersonDeleteType.SOFT,
      region: ctx.REGION,
      landscape: ctx.TAGS.Landscape.toLowerCase(),
      dryRun: ctx.DRY_RUN?.taskdef?.merger,
      tags,
    });
  }
}
