import { CfnResource, Duration, RemovalPolicy, Tags } from 'aws-cdk-lib';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import { Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { EventType, IBucket } from 'aws-cdk-lib/aws-s3';
import { LambdaDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { Construct } from 'constructs';

export interface ProcessorSubscribingLambdaProps {
  vpc: IVpc;
  chunksBucket: IBucket;
  processorQueueUrl: string;
  processorQueueArn: string;
  region: string;
  timeoutSeconds: number;
  memorySizeMb: number;
  pauseMessaging?: boolean;
  dryRun?: boolean;
  stackScope: Construct;
  tags?: { [key: string]: string };
}

/**
 * Creates a Lambda function that forwards S3 chunk notifications to the processor SQS 
 * queue when new person data chunks are deposited into the chunks bucket. This is 
 * needed to trigger the processor service to start processing these chunks as they 
 * arrive - one process triggered per arrival.
 *
 * Flow:
 * - 1) New chunk file lands in chunks bucket under chunks/*.ndjson
 * - 2) S3 notification invokes this Lambda
 * - 3) Lambda optionally skips forwarding when PAUSE_MESSAGING=true
 * - 4) Lambda forwards the original S3 event payload to processor queue
 */
export class ProcessorSubscribingLambda extends Construct {
  public readonly function: NodejsFunction;

  constructor(scope: Construct, id: string, props: ProcessorSubscribingLambdaProps) {
    super(scope, id);

    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: '/aws/lambda/processor-subscriber',
      retention: RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY
    });

    const lambdaRole = new Role(this, 'FunctionRole', {
      roleName: 'processor-subscriber-lambda-role',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      description: 'Role for subscribing processor lambda function',
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    this.function = new NodejsFunction(this, 'Function', {
      functionName: 'processor-subscriber',
      description: 'Subscribes to chunks/*.ndjson S3 events and forwards unchanged S3 event payload to processor SQS queue.',
      runtime: Runtime.NODEJS_20_X,
      handler: 'handler',
      entry: 'src/processing/ProcessorSubscriber.ts',
      timeout: Duration.seconds(props.timeoutSeconds),
      memorySize: props.memorySizeMb,
      role: lambdaRole,
      logGroup,
      environment: {
        REGION: props.region,
        PROCESSOR_QUEUE_URL: props.processorQueueUrl,
        PAUSE_MESSAGING: props.pauseMessaging ? 'true' : 'false',
        DRY_RUN: props.dryRun ? 'true' : 'false',
        DESCRIPTION1: 'Forwards S3 chunk creation events to processor queue unless PAUSE_MESSAGING=true.',
      },
      bundling: {
        externalModules: [
          '@aws-sdk/*',
        ]
      },
    });

    // Least privilege: only allow send to the processor queue.
    this.function.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'sqs:SendMessage',
        ],
        resources: [props.processorQueueArn],
      })
    );

    // S3 notifications for created chunks.
    props.chunksBucket.addEventNotification(
      EventType.OBJECT_CREATED,
      new LambdaDestination(this.function),
      {
        prefix: 'chunks/',
        suffix: '.ndjson',
      }
    );

    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.function).add(key, value);
        Tags.of(lambdaRole).add(key, value);
      });
    }
  }
}