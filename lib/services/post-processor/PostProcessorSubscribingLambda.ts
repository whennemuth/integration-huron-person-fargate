import { Duration, RemovalPolicy, Tags } from 'aws-cdk-lib';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import { Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { EventType, IBucket } from 'aws-cdk-lib/aws-s3';
import { LambdaDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { Construct } from 'constructs';
import { FUNCTION_BASE_NAME } from '../../../src/post-processing/PostProcessorSubscriber';
import { IContext } from '../../../context/IContext';
import { DYNAMODB_TABLE_NAME as STATISTICS_TABLE_NAME } from '../../../src/dynamodb/StatisticsTable';

export interface PostProcessorSubscribingLambdaProps {
  vpc: IVpc;
  postProcessorQueueUrl: string;
  chunksBucket: IBucket;
  chunksBucketName: string;
  region: string;
  landscape: string;
  timeoutSeconds: number;
  memorySizeMb: number;
  dryRun?: boolean;
  context: IContext;
  tags?: { [key: string]: string };
}

/**
 * Creates the Lambda function that, in S3 storage mode, triggers the post-processor Fargate task by
 * sending a message to the post-processor queue.
 *
 * Sequence of events (S3 mode):
 * - 1) Each S3-mode processor task writes its chunk's mini-delta under deltas/{population}/{timestamp}/
 *      in the chunks bucket, then a chunk-NNNN_processing_complete.json marker (success or failure).
 * - 2) The marker's S3 event notification invokes this Lambda (once per marker).
 * - 3) The Lambda does nothing for a run with a terminal error. Otherwise it reads the run's metadata
 *      (chunkCount) and lists the run's markers.
 * - 4) Once the markers' ordinals run 0..N with no gaps and number chunkCount, it sends
 *      { chunksBucket, chunkDirectory, createdAt } to the post-processor queue (unless DRY_RUN).
 * - 5) The QueueProcessingFargateService scales up on the queue depth and starts the post-processor
 *      task, which reads that message (see PostProcessorTaskDefinition).
 *
 * In DynamoDB mode the Lambda is deployed but never invoked: processors write no markers, and the
 * processor that completes a run's last chunk queues the post-processor itself.
 */
export class PostProcessorSubscribingLambda extends Construct {
  public readonly function: NodejsFunction;

  constructor(scope: Construct, id: string, props: PostProcessorSubscribingLambdaProps) {
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
      description: 'Role for subscribing post-processor lambda function',
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Create Lambda function
    this.function = new NodejsFunction(this, 'Function', {
      functionName: `${FUNCTION_BASE_NAME}-${props.landscape}`,
      description:
        `S3 mode: queues the post-processor once every chunk's processing-complete marker is in ` +
        `${props.chunksBucketName}. (SEE DESCRIPTION environment variable)`,
      runtime: Runtime.NODEJS_20_X,
      handler: 'handler',
      entry: 'src/post-processing/PostProcessorSubscriber.ts',
      timeout: Duration.seconds(props.timeoutSeconds),
      memorySize: props.memorySizeMb,
      role: lambdaRole,
      logGroup,
      environment: {
        REGION: props.region,
        POST_PROCESSOR_QUEUE_URL: props.postProcessorQueueUrl,
        CHUNKS_BUCKET_NAME: props.chunksBucketName,
        DRY_RUN: props.dryRun ? 'true' : 'false',
        PREVIOUS_STORAGE_TYPE: props.context.PREVIOUS_STORAGE_TYPE || 's3',
        // Conditionally add the statistics table name when in DynamoDB mode
        ...(props.context.PREVIOUS_STORAGE_TYPE === 'dynamodb' && {
          DYNAMODB_STATISTICS_TABLE_NAME: STATISTICS_TABLE_NAME(props.context),
        }),
        // What this Lambda does. Lambda allows 4 KB for all environment variables together.
        DESCRIPTION:
          `S3 mode only. Invoked for each chunk-NNNN_processing_complete.json marker that an S3-mode ` +
          `processor writes (success or failure) under deltas/{population}/{timestamp}/ in ${props.chunksBucketName}. ` +
          `Does nothing for a run with a terminal error. Otherwise reads the run's metadata (chunkCount) and ` +
          `lists the run's markers; once they run 0..N with no gaps and number chunkCount, it queues the post-processor. ` +
          `The message (chunksBucket, chunkDirectory, createdAt) scales up the post-processor service, which ` +
          `starts its task (see its task definition's DESCRIPTION variable). DRY_RUN=true checks but sends nothing. ` +
          `DynamoDB mode: never invoked, since processors write no markers. The processor that completes a run's ` +
          `last chunk and wins its POST_PROCESSOR_TRIGGER_CLAIM (statistics table) queues the post-processor itself.`,
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

    // Grant DynamoDB permissions if in DynamoDB mode - GetItem only (read-only Lambda), for
    // FLAGS/METADATA/TERMINAL_ERROR lookups on the statistics table
    if (props.context.PREVIOUS_STORAGE_TYPE === 'dynamodb') {
      const statisticsTableName = STATISTICS_TABLE_NAME(props.context);

      this.function.addToRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:GetItem',
          ],
          resources: [
            `arn:aws:dynamodb:${props.region}:*:table/${statisticsTableName}`,
          ],
        })
      );
    }

    // Add S3 event notification to trigger Lambda when marker files are created
    // Marker files indicate a chunk has finished processing and prevent race conditions
    // where delta files are deleted before S3 events can process (6-15 second window issue)
    props.chunksBucket.addEventNotification(
      EventType.OBJECT_CREATED,
      new LambdaDestination(this.function),
      {
        prefix: 'deltas/',                  // Filter to delta directory
        suffix: '_processing_complete.json', // Only marker files
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
