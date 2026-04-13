import { Duration, RemovalPolicy, Tags } from 'aws-cdk-lib';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import { Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';

export interface ChunkerSubscribingLambdaProps {
  vpc: IVpc;
  chunkerQueueUrl: string;
  inputBucketName: string;
  region: string;
  timeoutSeconds: number;
  memorySizeMb: number;
  dryRun?: boolean;
  tags?: { [key: string]: string };
}

/**
 * Creates a Lambda function that triggers chunker Fargate tasks by sending messages to SQS.
 * 
 * This Lambda acts as a dispatcher for two types of chunking events:
 * 
 * **S3-based chunking sequence:**
 * - 1) A large JSON file containing person records is uploaded to the foreign input S3 bucket.
 * - 2) A foreign Lambda, subscribed to S3 events for the input bucket, calls this 
 *      ChunkerSubscribingLambda with S3 file details (bucket, key).
 * - 3) This Lambda delegates to ChunkerS3Subscriber which sends a message to the chunker SQS queue
 *      with the S3 file location.
 * - 4) QueueProcessingFargateService detects the heightened queue depth and auto-scales.
 * - 5) Fargate task reads the message, streams the S3 file, and creates NDJSON chunks.
 * 
 * **API-based chunking sequence:**
 * - 1) EventBridge schedule triggers this Lambda on a cron schedule.
 * - 2) Lambda delegates to ChunkerApiSubscriber which sends a message to the chunker SQS queue
 *      with API endpoint configuration (baseUrl, fetchPath, populationType).
 * - 3) QueueProcessingFargateService detects the heightened queue depth and auto-scales.
 * - 4) Fargate task reads the message, fetches data from the API, and creates NDJSON chunks.
 * 
 * Note: The Lambda entry point (src/chunking/ChunkerSubscriber.ts) analyzes the event structure
 * and routes to the appropriate handler (S3 or API).
 */
export class ChunkerSubscribingLambda extends Construct {
  public readonly function: NodejsFunction;

  constructor(scope: Construct, id: string, props: ChunkerSubscribingLambdaProps) {
    super(scope, id);

    // Create Lambda log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/aws/lambda/chunker-subscriber`,
      retention: RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY
    });

    // Create IAM role with predictable name for Lambda function
    const lambdaRole = new Role(this, 'FunctionRole', {
      roleName: 'chunker-subscriber-lambda-role',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      description: 'Role for subscribing chunker lambda function',
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Create Lambda function
    this.function = new NodejsFunction(this, 'Function', {
      functionName: 'chunker-subscriber',
      description: `Dispatcher for chunker events: S3 file uploads to ${props.inputBucketName} or EventBridge schedule for API fetch (SEE DESCRIPTION environment variable(s) for task performed)`,
      runtime: Runtime.NODEJS_20_X,
      handler: 'handler',
      entry: 'src/chunking/ChunkerSubscriber.ts',
      timeout: Duration.seconds(props.timeoutSeconds),
      memorySize: props.memorySizeMb,
      role: lambdaRole,
      logGroup,
      environment: {
        REGION: props.region,
        CHUNKER_QUEUE_URL: props.chunkerQueueUrl,
        DRY_RUN: props.dryRun ? 'true' : 'false',
        DESCRIPTION1: 
          `Analyzes incoming event (S3 or API) and delegates to appropriate handler (ChunkerS3Subscriber 
          or ChunkerApiSubscriber). Both handlers send SQS messages to trigger Fargate tasks that create 
          NDJSON chunk files from person data.`
      },
      bundling: {
        externalModules: [
          '@aws-sdk/*',
        ]
      },
    });

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

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.function).add(key, value);
        Tags.of(lambdaRole).add(key, value);
      });
    }
  }
}
