import { Duration, Tags } from 'aws-cdk-lib';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';

export const QUEUE_BASE_NAME = 'huron-person';

export interface QueueInfrastructureProps {
  context: IContext;
  tags?: { [key: string]: string };
}

/**
 * Wrapper construct for SQS queue infrastructure
 * Creates 3 queue pairs (main + DLQ) for the 3 QueueProcessingFargateService tasks:
 * 1. Chunker - breaks large JSON files into chunks
 * 2. Processor - processes each chunk (delta computation)
 * 3. Post-processor - merges delta chunks into final output
 */
export class QueueInfrastructure extends Construct {
  // Chunker queues
  public readonly chunkerQueue: Queue;
  public readonly chunkerDeadLetterQueue: Queue;
  
  // Processor queues
  public readonly processorQueue: Queue;
  public readonly processorDeadLetterQueue: Queue;
  
  // Post-processor queues
  public readonly postProcessorQueue: Queue;
  public readonly postProcessorDeadLetterQueue: Queue;

  constructor(scope: Construct, id: string, props: QueueInfrastructureProps) {
    super(scope, id);

    const { context: ctx } = props;
    const { Landscape } = ctx.TAGS;

    // ========================================
    // 1. Chunker Queues
    // ========================================
    this.chunkerDeadLetterQueue = new Queue(this, 'ChunkerDeadLetterQueue', {
      queueName: `${Landscape.toLowerCase()}-${QUEUE_BASE_NAME}-chunker-queue-dlq`,
      retentionPeriod: Duration.days(14),
    });
    // Or create a fifo queue.
    // this.chunkerDeadLetterQueue = new Queue(this, 'ChunkerDeadLetterQueue', {
    //   queueName: 'huron-person-chunker-queue-dlq.fifo',
    //   retentionPeriod: Duration.days(14),
    //   fifo: true,
    //   contentBasedDeduplication: true
    // });

    this.chunkerQueue = new Queue(this, 'ChunkerQueue', {
      queueName: `${Landscape.toLowerCase()}-${QUEUE_BASE_NAME}-chunker-queue`,
      visibilityTimeout: Duration.seconds(ctx.SQS.visibilityTimeoutSeconds),
      retentionPeriod: Duration.days(ctx.SQS.retentionPeriodDays),
      deadLetterQueue: {
        queue: this.chunkerDeadLetterQueue,
        maxReceiveCount: ctx.SQS.deadLetterQueueMaxReceiveCount,
      },
    });

    // ========================================
    // 2. Processor Queues
    // ========================================
    this.processorDeadLetterQueue = new Queue(this, 'ProcessorDeadLetterQueue', {
      queueName: `${Landscape.toLowerCase()}-${QUEUE_BASE_NAME}-processor-queue-dlq`,
      retentionPeriod: Duration.days(14),
    });

    this.processorQueue = new Queue(this, 'ProcessorQueue', {
      queueName: `${Landscape.toLowerCase()}-${QUEUE_BASE_NAME}-processor-queue`,
      visibilityTimeout: Duration.seconds(ctx.SQS.visibilityTimeoutSeconds),
      retentionPeriod: Duration.days(ctx.SQS.retentionPeriodDays),
      deadLetterQueue: {
        queue: this.processorDeadLetterQueue,
        maxReceiveCount: ctx.SQS.deadLetterQueueMaxReceiveCount,
      },
    });

    // ========================================
    // 3. Post-Processor Queues
    // ========================================
    this.postProcessorDeadLetterQueue = new Queue(this, 'PostProcessorDeadLetterQueue', {
      queueName: `${Landscape.toLowerCase()}-${QUEUE_BASE_NAME}-post-processor-queue-dlq`,
      retentionPeriod: Duration.days(14),
    });

    this.postProcessorQueue = new Queue(this, 'PostProcessorQueue', {
      queueName: `${Landscape.toLowerCase()}-${QUEUE_BASE_NAME}-post-processor-queue`,
      visibilityTimeout: Duration.seconds(ctx.SQS.visibilityTimeoutSeconds),
      retentionPeriod: Duration.days(ctx.SQS.retentionPeriodDays),
      deadLetterQueue: {
        queue: this.postProcessorDeadLetterQueue,
        maxReceiveCount: ctx.SQS.deadLetterQueueMaxReceiveCount,
      },
    });

    // ========================================
    // Apply tags to all queues
    // ========================================
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.chunkerQueue).add(key, value);
        Tags.of(this.chunkerDeadLetterQueue).add(key, value);
        Tags.of(this.processorQueue).add(key, value);
        Tags.of(this.processorDeadLetterQueue).add(key, value);
        Tags.of(this.postProcessorQueue).add(key, value);
        Tags.of(this.postProcessorDeadLetterQueue).add(key, value);
      });
    }
  }
}
