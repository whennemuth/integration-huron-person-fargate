import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Duration, RemovalPolicy, Tags } from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import { EcrRepository } from './EcrRepository';
import { EcsInfrastructure } from './EcsInfrastructure';
import { IContext } from '../context/IContext';
import { QueueInfrastructure } from './QueueInfrastructure';
import { SubscribingLambdas } from './SubscribingLambdas';
import { DynamoDbTables } from './DynamoDB';
import { Config } from 'integration-huron-person';
import { SourceSimulator } from './services/chunker/SourceSimulator';
import { HuronPersonSecrets } from './Secrets';

export interface AppConstructProps {
  context: IContext;
  config?: Config;
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
  public readonly dynamoDbTables: DynamoDbTables;
  public readonly sourceSimulator?: SourceSimulator;
  public readonly huronPersonSecrets: HuronPersonSecrets;

  constructor(scope: Construct, id: string, props: AppConstructProps) {
    super(scope, id);

    const { context: ctx, config, tags } = props;
    const { S3: { chunksBucket }, TAGS: { Landscape } } = ctx;

    // ========================================
    // 1. ECR Repository
    // ========================================
    this.ecr = new EcrRepository(this, 'Ecr', {
      landscape: Landscape,
      registryId: ctx.ACCOUNT,
      tags,
    });

    // ========================================
    // 2. DynamoDB Table for Processor Statistics
    // ========================================
    this.dynamoDbTables = new DynamoDbTables({ scope: this, id: 'DynamoDb', props: {
      context: ctx,
      tags,
    }});

    // ========================================
    // 3. Secrets Manager Secret for Huron Person Integration
    // ========================================
    this.huronPersonSecrets = new HuronPersonSecrets(this, props.context);
    
    // ========================================
    // 4. ECS Infrastructure (Cluster + Task Definitions)
    // ========================================
    this.ecs = new EcsInfrastructure(this, 'Ecs', {
      repository: this.ecr.repository,
      context: ctx,
      config,
      huronPersonSecrets: this.huronPersonSecrets,
      stackScope: scope,  // Pass stack reference for escape hatches
      dynamoDbTables: this.dynamoDbTables, // Pass DynamoDB tables to ECS infrastructure for task definitions
      tags,
    });

    // ========================================
    // 4. Queue Infrastructure
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
    // 5. Chunks Bucket
    // ========================================
    this.chunksBucket = new Bucket(this, 'ChunksBucket', {
      bucketName: `${chunksBucket}-${Landscape.toLowerCase()}`,
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
    // 6. Subscribing Lambdas (Chunker & Merger)
    // ========================================
    // Create subscribing lambdas before services so chunker lambda can be
    // passed to ChunkerService for EventBridge schedule configuration
    this.subscribingLambdas = new SubscribingLambdas(this, 'SubscribingLambdas', {
      ecsInfra: this.ecs,
      chunksBucket: this.chunksBucket,
      chunkerQueueUrl: this.queue.chunkerQueue.queueUrl,
      processorQueueUrl: this.queue.processorQueue.queueUrl,
      processorQueueArn: this.queue.processorQueue.queueArn,
      mergerQueueUrl: this.queue.mergerQueue.queueUrl,
      stackScope: scope,
      context: ctx,
      tags,
    });

    // ========================================
    // 7. Chunker Service (Phase 1)
    // ========================================
    // Pass chunker lambda for EventBridge schedule configuration
    this.ecs.createChunkerService(
      this.queue.chunkerQueue,
      this.queue.chunkerDeadLetterQueue,
      this.subscribingLambdas.chunker.function
    );

    // ========================================
    // 8. Processor Service (Phase 2)
    // ========================================
    // Create processor service as child of ECS infrastructure
    this.ecs.createProcessorService(
      this.queue.processorQueue,
      this.queue.processorDeadLetterQueue
    );

    // ========================================
    // 9. Merger Service (Phase 3)
    // ========================================
    this.ecs.createMergerService(
      this.queue.mergerQueue,
      this.queue.mergerDeadLetterQueue
    );

    
    // Source Simulator (Optional) - Mock API for testing without 30-minute cooldown
    if (ctx.LAMBDA.sourceSimulator?.enabled) {
      this.sourceSimulator = new SourceSimulator(this, 'SourceSimulator', {
        huronPersonSecrets: this.huronPersonSecrets,
        landscape: ctx.TAGS.Landscape.toLowerCase(),
        stackId: ctx.STACK_ID,
        region: ctx.REGION,
        account: ctx.ACCOUNT,
        timeoutSeconds: ctx.LAMBDA.sourceSimulator.timeoutSeconds,
        memorySizeMb: ctx.LAMBDA.sourceSimulator.memorySizeMb,
        mockTotalPopulation: ctx.LAMBDA.sourceSimulator.mockTotalPopulation,
        mockErrorRate: ctx.LAMBDA.sourceSimulator.mockErrorRate,
        simulatedDelaySeconds: ctx.LAMBDA.sourceSimulator.simulatedDelaySeconds,
        secretArn: this.huronPersonSecrets.secretArn,
        tags,
      });

      console.log('[SubscribingLambdas] Source Simulator enabled - Function URL will be created');
    } else {
      console.log('[SubscribingLambdas] Source Simulator disabled - Skipping creation');
    }
    
  }
}
