import { Construct } from 'constructs';
import { IBucket } from 'aws-cdk-lib/aws-s3';
import { ChunkerSubscribingLambda } from './services/chunker/ChunkerSubscribingLambda';
import { EcsInfrastructure } from './EcsInfrastructure';
import { IContext } from '../context/IContext';
import { MergerSubscribingLambda } from './services/merger/MergerSubscribingLambda';

/**
 * Construct that defines all Lambda functions that are triggered by events (S3 or SQS)
 * Triggers:
 * 
 * - Chunker 
 *      1) Large JSON file appears in the input bucket.
 *      2) S3 event notification of input bucket triggers ChunkerSubscribingLambda created below.
 *      3) ChunkerSubscribingLambda sends message to SQS queue.
 *      4) QueueProcessingFargateService detects queue depth increase and auto-scales up, 
 *         launching chunker Fargate task(s).
 * 
 * - Processor
 *      1) Small data "chunk" NDJSON file appears in the chunks bucket at "/chunks/..."
 *      2) S3 event notification of chunks bucket sends associated message to SQS queue DIRECTLY.
 *         (Note: This is different from the chunker/merger trigger flows - the processor trigger 
 *          is S3 → SQS directly, without a Lambda in between - HENCE NO LAMBDA CREATED HERE).
 *      3) QueueProcessingFargateService detects queue depth increase and auto-scales up, 
 *         launching processor Fargate task(s).
 * 
 * - Merger
 *      1) Small delta "chunk" NDJSON appears in the chunks bucket at "/deltas/..."
 *      2) S3 event notification of chunks bucket triggers MergerSubscribingLambda created below.
 *      3) MergerSubscribingLambda checks if all expected delta chunks are present. If so, sends 
 *         message to SQS queue.
 *      4) QueueProcessingFargateService detects queue depth increase and auto-scales up,
 *         launching merger Fargate task(s).
 * 
 * This construct groups these related Lambda functions together for better organization in the CDK app.
 */
export interface SubscribingLambdasProps {
  ecsInfra: EcsInfrastructure;
  chunksBucket: IBucket;
  chunkerQueueUrl: string;
  mergerQueueUrl: string;
  context: IContext;
  tags?: { [key: string]: string };
}

/**
 * Wrapper construct for subscribing Lambda functions
 * Groups Chunker and Merger subscribing lambdas
 */
export class SubscribingLambdas extends Construct {
  public readonly chunker: ChunkerSubscribingLambda;
  public readonly merger: MergerSubscribingLambda;

  constructor(scope: Construct, id: string, props: SubscribingLambdasProps) {
    super(scope, id);

    const { ecsInfra, chunksBucket, chunkerQueueUrl, mergerQueueUrl, context: ctx, tags } = props;

    // Chunker Subscribing Lambda (Phase 1)
    this.chunker = new ChunkerSubscribingLambda(this, 'Chunker', {
      vpc: ecsInfra.vpc,
      chunkerQueueUrl,
      inputBucketName: ctx.S3.inputBucket,
      region: ctx.REGION,
      timeoutSeconds: ctx.LAMBDA.chunkerSubscriber.timeoutSeconds,
      memorySizeMb: ctx.LAMBDA.chunkerSubscriber.memorySizeMb,
      dryRun: ctx.DRY_RUN?.lambda?.chunker,
      tags,
    });

    // Processor Subscribing Lambda (Phase 2) - SQS event-driven - implemented in ProcessorService construct

    // Merger Subscribing Lambda (Phase 3) - S3 event-driven
    this.merger = new MergerSubscribingLambda(this, 'Merger', {
      vpc: ecsInfra.vpc,
      mergerQueueUrl,
      chunksBucket,
      chunksBucketName: ctx.S3.chunksBucket,
      region: ctx.REGION,
      timeoutSeconds: ctx.LAMBDA.mergerSubscriber.timeoutSeconds,
      memorySizeMb: ctx.LAMBDA.mergerSubscriber.memorySizeMb,
      dryRun: ctx.DRY_RUN?.lambda?.merger,
      tags,
    });
  }
}
