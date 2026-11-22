import { Duration, RemovalPolicy, Stack, Tags } from 'aws-cdk-lib';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import { Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { FUNCTION_BASE_NAME } from '../../../src/chunking/fetch/ChunkerApiSubscriber';
import { DYNAMODB_TABLE_NAME as atomicCounterTableName } from '../../../src/dynamodb/AtomicCounter';
import { DYNAMODB_TABLE_NAME as mockTargetPersonTableName } from '../../../src/dynamodb/MockTargetPersonTable';
import { DYNAMODB_TABLE_NAME as statisticsTableName } from '../../../src/dynamodb/StatisticsTable';
import { DYNAMODB_TABLE_NAME as personCurrentStateTableName } from '../../../src/dynamodb/PersonCurrentStateTable';
import { DYNAMODB_TABLE_NAME as personHistoryTableName } from '../../../src/dynamodb/PersonHistoryTable';
import { MockLandscapeReset } from '../../../src/chunking/MockLandscapeReset';
import { isMockLandscape } from '../../../src/Utils';
import { ScheduledStartGuard } from '../../../src/schedule/ScheduledStartGuard';
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
  /**
   * The processor service (in the chunker's cluster), whose live maximum tasks every run start sets
   * (src/chunking/ProcessorScalingCapacity.ts), and the landscape's maxScalingCapacity it goes back to without an override
   */
  processorServiceName: string;
  processorMaxScalingCapacity: number;
  /**
   * The landscape's chunks bucket. In a mock landscape only, it goes in the environment (CHUNKS_BUCKET_NAME) with the
   * grants a run that starts from scratch needs (ApiChunkerEvent.resetState, src/chunking/MockLandscapeReset.ts);
   * any other landscape's subscriber gets neither, so it can't reset anything.
   */
  chunksBucketName?: string;
  /** Long enough for a parallel start's wait for the scale-in alarm (up to 6 minutes): see the context's LAMBDA */
  timeoutSeconds: number;
  memorySizeMb: number;
  /** The context's PREVIOUS_STORAGE_TYPE: how the scheduled start guard reads the latest run (default dynamodb) */
  storageType?: string;
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
        // The processor service whose live maximum a run start sets, and the maximum it goes back to without an
        // override (ProcessorScalingCapacity.fromEnvironment())
        PROCESSOR_SERVICE_NAME: props.processorServiceName,
        PROCESSOR_MAX_SCALING_CAPACITY: String(props.processorMaxScalingCapacity),
        // A start the bulk sync schedule makes is skipped while a run is underway (ScheduledStartGuard): the run list
        // it reads
        DYNAMODB_STATISTICS_TABLE_NAME: statisticsTableName({ STACK_ID: props.stackId, TAGS: { Landscape: props.landscape } } as IContext),
        PREVIOUS_STORAGE_TYPE: props.storageType ?? 'dynamodb',
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

    // Every run start sets the processor service's live maximum tasks (src/chunking/ProcessorScalingCapacity.ts) with
    // RegisterScalableTarget. A scalable target's ARN carries an ID Application Auto Scaling assigns, so it can't be
    // named here: any target, narrowed by the conditions to an ECS service's desired count. AWS's example for ECS
    // targets also grants DescribeServices and UpdateService, here on the processor service only.
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['application-autoscaling:RegisterScalableTarget'],
      resources: ['*'],
      conditions: {
        StringEquals: {
          'application-autoscaling:service-namespace': 'ecs',
          'application-autoscaling:scalable-dimension': 'ecs:service:DesiredCount'
        }
      }
    }));
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['ecs:UpdateService', 'ecs:DescribeServices'],
      resources: [`arn:aws:ecs:${region}:${account}:service/${props.chunkerClusterName}/${props.processorServiceName}`],
    }));

    // A start the bulk sync schedule makes (src/schedule/ScheduledStartGuard.ts) is skipped while a run is underway or
    // a dashboard launch is up: read the statistics table (the run list index, the newest run's report) and the
    // dashboard's launch flag, and record a skipped start where the dashboard reads it
    const statisticsTableArn = Stack.of(this).formatArn({
      service: 'dynamodb', resource: 'table',
      resourceName: statisticsTableName({ STACK_ID: props.stackId, TAGS: { Landscape: props.landscape } } as IContext)
    });
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['dynamodb:GetItem', 'dynamodb:Query'],
      resources: [statisticsTableArn, `${statisticsTableArn}/index/*`],
    }));
    const parameterArn = (name: string) => Stack.of(this).formatArn({ service: 'ssm', resource: 'parameter', resourceName: name.slice(1) });
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['ssm:GetParameter'],
      resources: [parameterArn(ScheduledStartGuard.launchFlagParameterName(props.landscape))],
    }));
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['ssm:PutParameter'],
      resources: [parameterArn(ScheduledStartGuard.refusalParameterName(props.landscape))],
    }));

    if (isMockLandscape(props.landscape) && props.chunksBucketName) {
      this.grantMockLandscapeReset(props.chunksBucketName, props);
    }

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

  /**
   * A mock landscape's run state reset (src/chunking/MockLandscapeReset.ts), for a run that starts from scratch: the
   * chunks bucket in the environment; scan and batch-delete on the four tables it truncates (BulkPurger), and describe
   * on them (the person current-state table's existence decides the storage mode, and the person tables are absent in
   * S3 mode); list and delete under the chunks bucket's delta-storage/ only.
   */
  private grantMockLandscapeReset(chunksBucketName: string, props: ChunkerSubscribingLambdaProps): void {
    this.function.addEnvironment('CHUNKS_BUCKET_NAME', chunksBucketName);
    const context = { STACK_ID: props.stackId, TAGS: { Landscape: props.landscape } } as IContext;
    const tableArn = (name: string) => Stack.of(this).formatArn({ service: 'dynamodb', resource: 'table', resourceName: name });
    const tables = [mockTargetPersonTableName, statisticsTableName, personCurrentStateTableName, personHistoryTableName]
      .map(name => tableArn(name(context)));
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['dynamodb:Scan', 'dynamodb:BatchWriteItem', 'dynamodb:DescribeTable'],
      resources: tables,
    }));
    const prefix = `${MockLandscapeReset.SHARED_DELTA_STORAGE_DIR}/`;
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['s3:ListBucket'],
      resources: [`arn:aws:s3:::${chunksBucketName}`],
      conditions: { StringLike: { 's3:prefix': [`${prefix}*`] } },
    }));
    this.function.addToRolePolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['s3:DeleteObject'],
      resources: [`arn:aws:s3:::${chunksBucketName}/${prefix}*`],
    }));
  }
}
