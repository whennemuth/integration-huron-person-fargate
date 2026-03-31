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
 * Creates a Lambda function that triggers the chunker Fargate task by sending messages to SQS.
 * 
 * Sequence of events:
 * - 1) A large JSON file containing person records is uploaded to the foreign input S3 bucket.
 * - 2) A foreign Lambda, subscribed to S3 events for the input bucket, is activated to call this 
 *      ChunkerSubscribingLambda (technically not "subscribing").
 * - 3) This ChunkerSubscribingLambda sends a message to the chunker SQS queue with the S3 key of 
 *      the uploaded file - thus "passing on" the subscription to Fargate as follows:
 * - 4) The desiredCount of the chunker Fargate service is then adjusted upward by the 
 *      QueueProcessingFargateService construct internals responding to the temporarily heightened
 *      depth of the SQS queue, which in turn causes a new Fargate task for chunking to be created.
 * - 5) The chunking task polls for a message in the queue that it expects to find with the S3
 *      bucket name and key of the uploaded file. 
 * - 6) The task processes the file and creates smaller NDJSON chunk files in the chunks S3 bucket.
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
      description: `Triggered by S3 event where big person JSON file lands in 
        ${props.inputBucketName} (SEE DESCRIPTION environment variable(s) for task performed)`,
      runtime: Runtime.NODEJS_20_X,
      handler: 'handler',
      entry: 'src/ChunkerSubscriber.ts',
      timeout: Duration.seconds(props.timeoutSeconds),
      memorySize: props.memorySizeMb,
      role: lambdaRole,
      logGroup,
      environment: {
        REGION: props.region,
        CHUNKER_QUEUE_URL: props.chunkerQueueUrl,
        DRY_RUN: props.dryRun ? 'true' : 'false',
        DESCRIPTION1: 
          `Sends SQS message to chunker queue to trigger Fargate task that breaks the big person 
          JSON file into smaller "bite sized" .NDJSON chunk files.`
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
