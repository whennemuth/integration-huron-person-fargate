import { Duration, Tags } from 'aws-cdk-lib';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';

export interface QueueInfrastructureProps {
  context: IContext;
  tags?: { [key: string]: string };
}

/**
 * Wrapper construct for SQS queue infrastructure
 * Creates 3 queue pairs (main + DLQ) for the 3 QueueProcessingFargateService tasks:
 * 1. Chunker - breaks large JSON files into chunks
 * 2. Processor - processes each chunk (delta computation)
 * 3. Merger - merges delta chunks into final output
 */
export class QueueInfrastructure extends Construct {
  // Chunker queues
  public readonly chunkerQueue: Queue;
  public readonly chunkerDeadLetterQueue: Queue;
  
  // Processor queues
  public readonly processorQueue: Queue;
  public readonly processorDeadLetterQueue: Queue;
  
  // Merger queues
  public readonly mergerQueue: Queue;
  public readonly mergerDeadLetterQueue: Queue;

  constructor(scope: Construct, id: string, props: QueueInfrastructureProps) {
    super(scope, id);

    const { context: ctx } = props;

    // ========================================
    // 1. Chunker Queues
    // ========================================
    this.chunkerDeadLetterQueue = new Queue(this, 'ChunkerDeadLetterQueue', {
      queueName: 'huron-person-chunker-queue-dlq',
      retentionPeriod: Duration.days(14),
    });

    this.chunkerQueue = new Queue(this, 'ChunkerQueue', {
      queueName: 'huron-person-chunker-queue',
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
      queueName: 'huron-person-processor-queue-dlq',
      retentionPeriod: Duration.days(14),
    });

    this.processorQueue = new Queue(this, 'ProcessorQueue', {
      queueName: 'huron-person-processor-queue',
      visibilityTimeout: Duration.seconds(ctx.SQS.visibilityTimeoutSeconds),
      retentionPeriod: Duration.days(ctx.SQS.retentionPeriodDays),
      deadLetterQueue: {
        queue: this.processorDeadLetterQueue,
        maxReceiveCount: ctx.SQS.deadLetterQueueMaxReceiveCount,
      },
    });

    // ========================================
    // 3. Merger Queues
    // ========================================
    this.mergerDeadLetterQueue = new Queue(this, 'MergerDeadLetterQueue', {
      queueName: 'huron-person-merger-queue-dlq',
      retentionPeriod: Duration.days(14),
    });

    this.mergerQueue = new Queue(this, 'MergerQueue', {
      queueName: 'huron-person-merger-queue',
      visibilityTimeout: Duration.seconds(ctx.SQS.visibilityTimeoutSeconds),
      retentionPeriod: Duration.days(ctx.SQS.retentionPeriodDays),
      deadLetterQueue: {
        queue: this.mergerDeadLetterQueue,
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
        Tags.of(this.mergerQueue).add(key, value);
        Tags.of(this.mergerDeadLetterQueue).add(key, value);
      });
    }
  }
}
