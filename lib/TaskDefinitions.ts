import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { ChunkerTaskDefinition } from './services/chunker/ChunkerTaskDefinition';
import { MergerTaskDefinition } from './services/merger/MergerTaskDefinition';
import { ProcessorTaskDefinition } from './services/processor/ProcessorTaskDefinition';
import { HuronPersonSecrets } from './Secrets';

export interface TaskDefinitionsProps {
  repository: IRepository;
  context: IContext;
  dynamoDbTableName: string;
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

    const { repository, context: ctx, dynamoDbTableName, tags } = props;

    // Create Secrets Manager secret for huron-person configuration
    const huronPersonSecrets = new HuronPersonSecrets(this, props.context);

    // Chunker task definition
    this.chunker = new ChunkerTaskDefinition(this, 'chunker', {
      repository,
      cpu: ctx.ECS.chunkerTaskDefinition.cpu,
      memoryLimitMiB: ctx.ECS.chunkerTaskDefinition.memoryLimitMiB,
      logRetentionDays: ctx.ECS.chunkerTaskDefinition.logRetentionDays,
      inputBucketName: ctx.S3.inputBucket,
      chunksBucketName: ctx.S3.chunksBucket,
      queueUrl: '', // Will be set after queue is created
      itemsPerChunk: ctx.ITEMS_PER_CHUNK,
      huronPersonSecrets,
      region: ctx.REGION,
      dryRun: ctx.DRY_RUN?.taskdef?.chunker,
      tags,
    });

    // Processor task definition (queue URL will be set separately in Stack.ts)
    this.processor = new ProcessorTaskDefinition(this, 'processor', {
      repository,
      cpu: ctx.ECS.processorTaskDefinition.cpu,
      memoryLimitMiB: ctx.ECS.processorTaskDefinition.memoryLimitMiB,
      logRetentionDays: ctx.ECS.processorTaskDefinition.logRetentionDays,
      chunksBucketName: ctx.S3.chunksBucket,
      queueUrl: '', // Will be set after queue is created
      dynamoDbTableName, // DynamoDB table for error tracking and statistics
      huronPersonSecrets,
      context: ctx,
      region: ctx.REGION,
      dryRun: ctx.DRY_RUN?.taskdef?.processor,
      tags,
    });

    // Merger task definition
    this.merger = new MergerTaskDefinition(this, 'merger', {
      repository,
      cpu: ctx.ECS.mergerTaskDefinition.cpu,
      memoryLimitMiB: ctx.ECS.mergerTaskDefinition.memoryLimitMiB,
      logRetentionDays: ctx.ECS.mergerTaskDefinition.logRetentionDays,
      inputBucketName: ctx.S3.inputBucket,
      chunksBucketName: ctx.S3.chunksBucket,
      region: ctx.REGION,
      dryRun: ctx.DRY_RUN?.taskdef?.merger,
      tags,
    });
  }
}
