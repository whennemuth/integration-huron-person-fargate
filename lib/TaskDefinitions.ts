import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { Construct } from 'constructs';
import { DatabaseConfig, FileConfig, S3Config as S3FolderConfig } from 'integration-core';
import { Config, TargetPersonDeleteType } from 'integration-huron-person';
import { IContext } from '../context/IContext';
import { DynamoDbTables } from './DynamoDB';
import { CLUSTER_BASE_NAME } from './EcsInfrastructure';
import { HuronPersonSecrets } from './Secrets';
import { SERVICE_LOGICAL_ID } from './services/chunker/ChunkerService';
import { ChunkerTaskDefinition } from './services/chunker/ChunkerTaskDefinition';
import { MergerTaskDefinition } from './services/merger/MergerTaskDefinition';
import { ProcessorTaskDefinition } from './services/processor/ProcessorTaskDefinition';

export interface TaskDefinitionsProps {
  repository: IRepository;
  context: IContext;
  huronPersonSecrets: HuronPersonSecrets;
  config: Config;
  dynamoDbTables: DynamoDbTables;
  tags?: { [key: string]: string };
}

export type StorageParams = {
  previousStorageType: IContext['PREVIOUS_STORAGE_TYPE'];
  storageConfig: {
    sharedDeltaStorageDir?: string;
    s3?: S3FolderConfig;
    dynamodb?: DynamoDbTables;
    database?: DatabaseConfig;
  }
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

    let { config, repository, context: ctx, dynamoDbTables, huronPersonSecrets, tags } = props;

    const { storage, storage: { config: storageCfg } = {} } = props.config || {};
    const previousStorageType = ctx.PREVIOUS_STORAGE_TYPE || storage?.type;

    if( ! previousStorageType) {
      throw new Error('context.HURON_PERSON_CONFIG.storage and config.storage.type are not defined. Please provide a valid storage configuration.');
    }

    const storageParams: StorageParams = { previousStorageType, storageConfig: { } }

    // For any storage type, we assume that we can also provide DynamoDB tables for statistics 
    // tracking, atomic counters, and mock target person tables.
    storageParams.storageConfig.dynamodb = dynamoDbTables;

    switch(previousStorageType) {
      case 's3':
        const { keyPrefix } = storageCfg as S3FolderConfig;
        if(keyPrefix) {
          storageParams.storageConfig.sharedDeltaStorageDir = keyPrefix.endsWith('/') ? 
            keyPrefix.slice(0, -1) : 
            keyPrefix; // Remove trailing slash if present
        }
        storageParams.storageConfig.s3 = storageCfg as S3FolderConfig || 'delta-storage';
        break;
      case 'dynamodb':
        break;
      case 'database':
        // Not supported yet, but we can provide a DatabaseConfig for future use.
        storageParams.storageConfig.database = storageCfg as DatabaseConfig;
        break;
      case 'file':
        // Not supported yet, but we can provide a FileConfig for future use.
        let { path, outputPath } = storageCfg as FileConfig;
        path = outputPath ? outputPath(path) : path;
        if(path) {
          storageParams.storageConfig.sharedDeltaStorageDir = path.endsWith('/') ? 
            path.slice(0, -1) : 
            path; // Remove trailing slash if present
        }
        break;
      default:
        throw new Error(`Unsupported storage type: ${previousStorageType}`);
    }
    
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
      stackId: ctx.STACK_ID,
      itemsPerChunk: ctx.ITEMS_PER_CHUNK,
      huronPersonSecrets,
      region: ctx.REGION,
      ecsClusterName: `${CLUSTER_BASE_NAME}-${ctx.TAGS.Landscape.toLowerCase()}`,
      maxScalingCapacity: ctx.ECS.chunkerService?.maxScalingCapacity ?? 1,
      ecsChunkerServiceName: SERVICE_LOGICAL_ID,
      landscape: ctx.TAGS.Landscape.toLowerCase(),
      retries: ctx.ECS.chunkerTaskDefinition.retries,
      storageParams,
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
      storageParams,
      huronPersonSecrets,
      context: ctx,
      config,
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
      storageParams,
      huronPersonSecrets,
      personDeleteType: config?.dataTarget?.personDeleteType || TargetPersonDeleteType.SOFT,
      region: ctx.REGION,
      landscape: ctx.TAGS.Landscape.toLowerCase(),
      dryRun: ctx.DRY_RUN?.taskdef?.merger,
      tags,
    });
  }
}
