import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Duration, RemovalPolicy, Tags } from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import { EcrRepository } from './EcrRepository';
import { EcsInfrastructure } from './EcsInfrastructure';
import { IContext } from '../context/IContext';
import { QueueInfrastructure } from './QueueInfrastructure';
import { SubscribingLambdas } from './SubscribingLambdas';

export interface AppConstructProps {
  context: IContext;
  tags?: { [key: string]: string };
}

/**
 * Top-level application construct
 * Groups all infrastructure components except custom resource providers
 */
export class AppConstruct extends Construct {
  public readonly ecr: EcrRepository;
  public readonly ecs: EcsInfrastructure;
  public readonly queue: QueueInfrastructure;
  public readonly chunksBucket: Bucket;
  public readonly subscribingLambdas: SubscribingLambdas;

  constructor(scope: Construct, id: string, props: AppConstructProps) {
    super(scope, id);

    const { context: ctx, tags } = props;

    // ========================================
    // 1. ECR Repository
    // ========================================
    this.ecr = new EcrRepository(this, 'Ecr', {
      repositoryName: ctx.ECR.repositoryName,
      registryId: ctx.ECR.registryId,
      tags,
    });

    // ========================================
    // 2. ECS Infrastructure (Cluster + Task Definitions)
    // ========================================
    this.ecs = new EcsInfrastructure(this, 'Ecs', {
      repository: this.ecr.repository,
      context: ctx,
      stackScope: scope,  // Pass stack reference for escape hatches
      tags,
    });

    // ========================================
    // 3. Queue Infrastructure
    // ========================================
    this.queue = new QueueInfrastructure(this, 'Queue', {
      context: ctx,
      tags,
    });

    // Update task definition with their corresponding queue URLs
    this.ecs.taskDefinitions.chunker.taskDefinition.defaultContainer!.addEnvironment(
      'SQS_QUEUE_URL',
      this.queue.chunkerQueue.queueUrl
    );
    this.ecs.taskDefinitions.processor.taskDefinition.defaultContainer!.addEnvironment(
      'SQS_QUEUE_URL',
      this.queue.processorQueue.queueUrl
    );
    this.ecs.taskDefinitions.merger.taskDefinition.defaultContainer!.addEnvironment(
      'SQS_QUEUE_URL',
      this.queue.mergerQueue.queueUrl
    );

    // ========================================
    // 4. Chunks Bucket
    // ========================================
    this.chunksBucket = new Bucket(this, 'ChunksBucket', {
      bucketName: ctx.S3.chunksBucket,
      // Lifecycle rules:
      // 1. Temporary chunk files (processed immediately) → configured expiration
      // 2. Delta files (merged by merger, but need failsafe) → longer expiration
      //    - Protects frequently-updated shared files (previous-input.ndjson)
      //    - Cleans up orphaned delta chunks from failed mergers
      lifecycleRules: [
        {
          id: 'expire-chunks',
          prefix: 'chunks/',
          expiration: Duration.days(ctx.S3.chunkExpirationDays),
        },
        {
          id: 'expire-deltas',
          prefix: 'deltas/',
          expiration: Duration.days(ctx.S3.deltaExpirationDays),
          // Longer expiration allows failsafe cleanup while protecting active shared files
          // (previous-input.ndjson is overwritten frequently, resetting its age)
        },
      ],
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // Apply tags to chunks bucket
    if (tags) {
      Object.entries(tags).forEach(([key, value]) => {
        Tags.of(this.chunksBucket).add(key, value);
      });
    }

    // ========================================
    // 5. Chunker Service (Phase 1)
    // ========================================
    this.ecs.createChunkerService(
      this.queue.chunkerQueue,
      this.queue.chunkerDeadLetterQueue
    );

    // ========================================
    // 6. Processor Service (Phase 2)
    // ========================================
    // Create processor service as child of ECS infrastructure
    this.ecs.createProcessorService(
      this.queue.processorQueue,
      this.queue.processorDeadLetterQueue,
      this.chunksBucket
    );

    // ========================================
    // 7. Merger Service (Phase 3)
    // ========================================
    this.ecs.createMergerService(
      this.queue.mergerQueue,
      this.queue.mergerDeadLetterQueue
    );

    // ========================================
    // 8. Subscribing Lambdas (Chunker & Merger)
    // ========================================
    this.subscribingLambdas = new SubscribingLambdas(this, 'SubscribingLambdas', {
      ecsInfra: this.ecs,
      chunksBucket: this.chunksBucket,
      chunkerQueueUrl: this.queue.chunkerQueue.queueUrl,
      mergerQueueUrl: this.queue.mergerQueue.queueUrl,
      context: ctx,
      tags,
    });
  }
}
