import { Duration, RemovalPolicy, Tags } from 'aws-cdk-lib';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import { Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { EventType, IBucket } from 'aws-cdk-lib/aws-s3';
import { LambdaDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { Construct } from 'constructs';

export interface MergerSubscribingLambdaProps {
  vpc: IVpc;
  mergerQueueUrl: string;
  chunksBucket: IBucket;
  chunksBucketName: string;
  region: string;
  timeoutSeconds: number;
  memorySizeMb: number;
  dryRun?: boolean;
  tags?: { [key: string]: string };
}

/**
 * Creates a Lambda function that triggers the merger Fargate task by sending messages to SQS.
 * 
 * Sequence of events:
 * - 1) The chunker Fargate task creates delta chunk files in the chunks S3 bucket.
 * - 2) An S3 event notification for the chunks bucket, responding to the creation of delta chunk 
 *      files, triggers this MergerSubscribingLambda (once per file).
 * - 3) This MergerSubscribingLambda uses the event parameters to identify and inspect the S3 
 *      chunks bucket.
 * - 4) If all expected delta chunks are present in the chunks bucket, as per a metadata file it 
 *      reads from the same bucket, this Lambda sends a message to the merger SQS queue with the 
 *      S3 key of the uploaded chunk file - thus "passing on" the subscription to Fargate as follows: 
 * - 5) The desiredCount of the merger Fargate service is then adjusted upward by the
 *      QueueProcessingFargateService construct internals responding to the temporarily heightened
 *      depth of the SQS queue, which in turn causes a new Fargate task for merging to be created.
 * - 6) The merger task polls for a message in the queue that it expects to find with the S3
 *      bucket details for inspecting content as follows:.
 * - 7) If all expected delta chunks are present in the chunks bucket, it sends a message to the 
 *      merger SQS queue to trigger the merger Fargate task.
 * - 8) The merger task concatenates all NDJSON chunk files into a single "previous-input.ndjson" file, which overwrites the prior version in the same bucket, and then deletes the chunk files.
 */
export class MergerSubscribingLambda extends Construct {
  public readonly function: NodejsFunction;

  constructor(scope: Construct, id: string, props: MergerSubscribingLambdaProps) {
    super(scope, id);

    // Create Lambda log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/aws/lambda/merger-subscriber`,
      retention: RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY
    });

    // Create IAM role with predictable name for Lambda function
    const lambdaRole = new Role(this, 'FunctionRole', {
      roleName: 'merger-subscriber-lambda-role',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      description: 'Role for subscribing merger lambda function',
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Create Lambda function
    this.function = new NodejsFunction(this, 'Function', {
      functionName: 'merger-subscriber',
      description: 
        `Subscribes to S3 event where a "chunk" file comprising hashed person delta data lands in 
        ${props.chunksBucketName}. (SEE DESCRIPTION environment variable(s) for task performed)`,      runtime: Runtime.NODEJS_20_X,
      handler: 'handler',
      entry: 'src/MergerSubscriber.ts',
      timeout: Duration.seconds(props.timeoutSeconds),
      memorySize: props.memorySizeMb,
      role: lambdaRole,
      logGroup,
      environment: {
        REGION: props.region,
        MERGER_QUEUE_URL: props.mergerQueueUrl,
        CHUNKS_BUCKET_NAME: props.chunksBucketName,
        DRY_RUN: props.dryRun ? 'true' : 'false',
        DESCRIPTION1:
          `Sends SQS message to merger queue to trigger Fargate task that checks completeness 
          of the delta chunks by referencing a metadata file created by the chunker.`,
        DESCRIPTION2:
          `If all expected delta chunks are present in the ${props.chunksBucketName} bucket, 
          it triggers the merger task to concatenate all NDJSON chunk files into a single 
          "previous-input.ndjson" file.`,
        DESCRIPTION3:
          `This "previous-input.ndjson" file will overwrite the prior version in the same bucket, 
          and then the chunk files are deleted.`
      },
      bundling: {
        externalModules: [
          '@aws-sdk/*',
        ]
      }
    });

    // Grant permissions to list and read S3 objects (for metadata check)
    this.function.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:GetObject',
          's3:ListBucket',
        ],
        resources: [
          `arn:aws:s3:::${props.chunksBucketName}`,
          `arn:aws:s3:::${props.chunksBucketName}/*`,
        ],
      })
    );

    // Grant permissions to send messages to SQS queue
    const queueArn = `arn:aws:sqs:${props.region}:*:*`; // Will be restricted by VPC endpoint
    this.function.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'sqs:SendMessage',
        ],
        resources: [queueArn],
      })
    );

    // Add S3 event notification to trigger Lambda when delta chunk files are created
    // This replaces the previous EventBridge schedule approach
    props.chunksBucket.addEventNotification(
      EventType.OBJECT_CREATED,
      new LambdaDestination(this.function),
      {
        prefix: 'deltas/',  // Filter to delta files only
        suffix: '.ndjson',  // Only NDJSON files
      }
    );

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.function).add(key, value);
      });
    }
  }
}
