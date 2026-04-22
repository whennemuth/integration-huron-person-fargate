import { RemovalPolicy, Stack, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, Secret as EcsSecret, FargateTaskDefinition, LogDriver, OperatingSystemFamily } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { IContext } from '../../../context/IContext';
import { HuronPersonSecrets } from '../../Secrets';

export interface ProcessorTaskDefinitionProps {
  repository: IRepository;
  imageTag?: string;
  cpu: number;
  memoryLimitMiB: number;
  logRetentionDays: number;
  chunksBucketName: string;
  queueUrl: string;
  dynamoDbTableName: string;
  context: IContext;
  region: string;
  huronPersonSecrets: HuronPersonSecrets;
  dryRun?: boolean;
  tags?: { [key: string]: string };
}

/**
 * Creates a Fargate task definition for the processor (Phase 2)
 * Reads NDJSON chunks and syncs persons to Huron API
 */
export class ProcessorTaskDefinition extends Construct {
  public readonly taskDefinition: FargateTaskDefinition;

  constructor(scope: Construct, id: string, props: ProcessorTaskDefinitionProps) {
    super(scope, id);

    const { 
      huronPersonSecrets: { secret, secretArn , secretName } = {}, logRetentionDays, 
      memoryLimitMiB, cpu, region, queueUrl, dynamoDbTableName, chunksBucketName, 
      context, repository, imageTag, dryRun, tags 
    } = props;

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-processor`,
      retention: logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: 'Processor',
      cpu,
      memoryLimitMiB,
      // Use ARM64 for Graviton2 (20% cost savings)
      runtimePlatform: {
        cpuArchitecture: CpuArchitecture.ARM64,
        operatingSystemFamily: OperatingSystemFamily.LINUX,
      },
    });

    // Add container with environment variables
    const environment: { [key: string]: string } = {
      REGION: region,
      SQS_QUEUE_URL: queueUrl,
      DYNAMODB_TABLE_NAME: dynamoDbTableName,
      // CHUNKS_BUCKET and CHUNK_KEY are set from SQS messages at runtime (not env vars)
      STATIC_MAP_USAGE: '{ "orgMap": true, "stateMap": true, "countryMap": true }', // Used by processor to determine which static maps to load in data mapper
      SECRET_ARN: secretArn!, // ARN of the Secrets Manager secret to read config from
      IS_ECS_TASK: 'true', // Used by the application code to determine if running in ECS context (vs local dev)
      DRY_RUN: dryRun ? 'true' : 'false',
      DESCRIPTION1: `Container run by lambda function responding to S3 events when a new "chunk" 
        file comprising person data is deposited into ${chunksBucketName}.`,
      DESCRIPTION2: 
        `It processes the chunk by syncing all person records in it to the Huron API.`
    };

    // Check if the context includes retry strategy configuration and add it to environment variables if present.
    const { retries } = context.ECS.processorTaskDefinition;
    if(retries) {
      const { retryStrategyOptions, retryStrategyType } = retries;
      if(retryStrategyOptions || retryStrategyType) {
        environment.RETRY_STRATEGY = JSON.stringify(retryStrategyType);
      }
    }

    // ECS secrets (injected at runtime from Secrets Manager)
    // These take precedence over environment variables in Fargate context
    const secrets = {
      // Inject entire huron-person config as JSON from Secrets Manager
      // This is retrieved by ECS at container startup and never appears in CloudFormation or logs
      HURON_PERSON_CONFIG_JSON: EcsSecret.fromSecretsManager(secret!),
    };

    const container = this.taskDefinition.addContainer('ProcessorContainer', {
      containerName: 'processor',
      image: ContainerImage.fromEcrRepository(
        repository,
        imageTag || 'latest'
      ),
      // Override CMD in Dockerfile to run processor
      command: ['node', 'dist/docker/processor.js'],
      logging: LogDriver.awsLogs({
        streamPrefix: 'processor',
        logGroup,
      }),
      environment,
      secrets, // ECS secrets injected at runtime
    });

    // Grant S3 permissions for processor operations on chunks bucket only
    // Processor.ts overrides storage.config.bucketName to use chunks bucket, so processor
    // does not need access to input bucket at all. Input bucket is only for chunker.ts.
    
    // Grant object-level permissions (GetObject, PutObject, DeleteObject, CopyObject)
    // - GetObject: Read chunk NDJSON files from chunksBucket
    // - PutObject/DeleteObject: Write/manage delta storage files in chunks bucket
    // - CopyObject: Move/reorganize files (used by S3StreamProvider.moveResource)
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:GetObject',
          's3:GetObjectVersion',
          's3:PutObject',
          's3:DeleteObject',
          's3:CopyObject',
        ],
        resources: [`arn:aws:s3:::${chunksBucketName}/*`],
      })
    );
    
    // Grant bucket-level permissions (ListBucket)
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:ListBucket',
        ],
        resources: [`arn:aws:s3:::${chunksBucketName}`],
      })
    );

    // Grant SQS permissions for reading and deleting messages
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'sqs:ReceiveMessage',
          'sqs:DeleteMessage',
          'sqs:GetQueueAttributes',
        ],
        resources: [
          `arn:aws:sqs:${region}:${Stack.of(this).account}:*`,
        ],
      })
    );

    // Grant DynamoDB permissions for writing error events and statistics
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'dynamodb:PutItem',
          'dynamodb:UpdateItem',
          'dynamodb:Query',
          'dynamodb:GetItem',
        ],
        resources: [
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamoDbTableName}`,
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamoDbTableName}/index/*`,
        ],
      })
    );

    // Grant Secrets Manager read access for huron-person configuration
    // IMPORTANT: Secrets are retrieved by the EXECUTION ROLE at container startup,
    // not the task role. The execution role is used by ECS agent to pull images,
    // retrieve secrets, and write logs before the container even starts.
    secret!.grantRead(this.taskDefinition.executionRole!);

    // Grant the task role permission to read the configuration secret from Secrets Manager
    // This is necessary for the application code to access the secret at runtime using the SDK, 
    // even though the secret is also injected as an environment variable.
    secret!.grantRead(this.taskDefinition.taskRole); // Grant read access to the secret for the task role (used by the application code at runtime)

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (tags) {
      Object.entries(tags).forEach(([key, value]) => {
        Tags.of(this.taskDefinition).add(key, value);
      });
    }
  }
}
