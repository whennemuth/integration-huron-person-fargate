import { Duration, RemovalPolicy, Stack, Tags } from 'aws-cdk-lib';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import { Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { FUNCTION_BASE_NAME } from '../../../src/chunking/fetch/ChunkerApiSubscriber';
import { DYNAMODB_TABLE_NAME as atomicCounterTableName } from '../../../src/dynamodb/AtomicCounter';
import { IContext } from '../../../context/IContext';

export interface ChunkerSubscribingLambdaProps {
  vpc: IVpc;
  chunkerQueueUrl: string;
  inputBucketName: string;
  region: string;
  landscape: string;
  /** The pipeline stack's STACK_ID, which names the atomic counter table it resets when a run starts */
  stackId: string;
  /** The chunker service's cluster and service names, which a parallel start scales (src/chunking/ParallelStart.ts) */
  chunkerClusterName: string;
  chunkerServiceName: string;
  /** Long enough for a parallel start's wait for the scale-in alarm (up to 6 minutes): see the context's LAMBDA */
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
  /** How long an asynchronous invocation may wait in Lambda's queue before it is dropped rather than run late */
  static readonly MAX_EVENT_AGE = Duration.minutes(15);

  public readonly function: NodejsFunction;

  constructor(scope: Construct, id: string, props: ChunkerSubscribingLambdaProps) {
    super(scope, id);

    // Create Lambda log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/aws/lambda/${FUNCTION_BASE_NAME}-${props.landscape}`,
      retention: RetentionDays.THREE_MONTHS,
      removalPolicy: RemovalPolicy.DESTROY
    });

    // Create IAM role with predictable name for Lambda function
    const lambdaRole = new Role(this, 'FunctionRole', {
      roleName: `${FUNCTION_BASE_NAME}-lambda-role-${props.landscape}`,
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      description: 'Role for subscribing chunker lambda function',
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Create Lambda function
    this.function = new NodejsFunction(this, 'Function', {
      functionName: `${FUNCTION_BASE_NAME}-${props.landscape}`,
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
        // Name the atomic counter table, whose counters a run's first event resets (resetCountersForRunStart())
        STACK_ID: props.stackId,
        LANDSCAPE: props.landscape,
        // The chunker service a parallel start scales (ParallelStart.fromEnvironment())
        CHUNKER_CLUSTER_NAME: props.chunkerClusterName,
        CHUNKER_SERVICE_NAME: props.chunkerServiceName,
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

    // Reset the chunker's counters when a run starts: UpdateItem on the landscape's atomic counter table only
    this.function.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['dynamodb:UpdateItem'],
        resources: [Stack.of(this).formatArn({
          service: 'dynamodb', resource: 'table',
          resourceName: atomicCounterTableName({ STACK_ID: props.stackId, TAGS: { Landscape: props.landscape } } as IContext)
        })],
      })
    );

    // A parallel start (src/chunking/ParallelStart.ts) reads the chunker service's maximum and scaling policies, waits
    // on its scale-in alarm, then sets its desired count. Application Auto Scaling's Describe actions don't support
    // resource-level permissions; the alarm's name is generated, so any alarm in the region.
    const { account, region } = Stack.of(this);
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['ecs:UpdateService', 'ecs:DescribeServices'],
      resources: [`arn:aws:ecs:${region}:${account}:service/${props.chunkerClusterName}/${props.chunkerServiceName}`],
    }));
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['application-autoscaling:DescribeScalableTargets', 'application-autoscaling:DescribeScalingPolicies'],
      resources: ['*'],
    }));
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['cloudwatch:DescribeAlarms'],
      resources: [`arn:aws:cloudwatch:${region}:${account}:alarm:*`],
    }));

    // No automatic retry: Lambda retries a failed or timed-out asynchronous invocation twice by default, and a retried
    // run start would reset the counters and seed again into a run already started (duplicate chunks). A failed start
    // is started again by a person. An event that waited longer than this in Lambda's queue is dropped, not started late.
    this.function.configureAsyncInvoke({ retryAttempts: 0, maxEventAge: ChunkerSubscribingLambda.MAX_EVENT_AGE });

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.function).add(key, value);
        Tags.of(lambdaRole).add(key, value);
      });
    }
  }
}
