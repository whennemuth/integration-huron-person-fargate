import { RemovalPolicy, Stack, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, Secret as EcsSecret, FargateTaskDefinition, LogDriver, OperatingSystemFamily } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { RetryStrategyConfig } from '../../../src/ApiErrorRetryStrategy';
import { HuronPersonSecrets } from '../../Secrets';
import { StorageParams } from '../../TaskDefinitions';
import { SERVICE_LOGICAL_ID } from './ChunkerService';

export interface ChunkerTaskDefinitionProps {
  repository: IRepository;
  imageTag?: string;
  cpu: number;
  memoryLimitMiB: number;
  memoryReservationMiB: number;
  logRetentionDays: number;
  queueUrl: string;
  inputBucketName: string;
  chunksBucketName: string;
  stackId: string;
  itemsPerChunk: number;
  storageParams: StorageParams;
  region: string;
  ecsClusterName: string;
  maxScalingCapacity: number;
  huronPersonSecrets: HuronPersonSecrets;
  ecsChunkerServiceName: string;
  landscape: string;
  retries?: RetryStrategyConfig;
  dryRun?: boolean;
  tags?: { [key: string]: string };
}

/**
 * Creates a Fargate task definition for the chunker (Phase 1)
 * Reads large JSON files from S3 and creates NDJSON chunks
 */
export class ChunkerTaskDefinition extends Construct {
  public readonly taskDefinition: FargateTaskDefinition;

  constructor(scope: Construct, id: string, props: ChunkerTaskDefinitionProps) {
    super(scope, id);

    const { 
      huronPersonSecrets: { secret, secretArn } = {}, logRetentionDays, 
      memoryLimitMiB, memoryReservationMiB, cpu, region, queueUrl, itemsPerChunk, chunksBucketName, 
      inputBucketName, repository, imageTag, ecsClusterName, maxScalingCapacity, stackId, 
      ecsChunkerServiceName, landscape, dryRun, tags, retries, 
      storageParams: { previousStorageType, storageConfig: { sharedDeltaStorageDir, dynamodb } = {} }
    } = props;

    const environment: { [key: string]: string } = {
      DESCRIPTION1:
        `Container run by a lambda function responding to S3 events when a new large person
          data file is uploaded to the ${inputBucketName} bucket.`,
      DESCRIPTION2:
        `It splits the file into smaller NDJSON chunk files, and writes the chunks back to
          the ${chunksBucketName} bucket for parallel processing.`,
      REGION: region,
      ECS_CLUSTER_NAME: ecsClusterName,
      ECS_SERVICE_NAME: SERVICE_LOGICAL_ID,
      MAX_SCALING_CAPACITY: maxScalingCapacity.toString(),
      SQS_QUEUE_URL: queueUrl,
      CHUNKS_BUCKET: chunksBucketName,
      PREVIOUS_STORAGE_TYPE: previousStorageType!,
      DYNAMODB_ATOMIC_COUNTER_TABLE_NAME: dynamodb!.atomicCounterTable.tableName,
      ITEMS_PER_CHUNK: itemsPerChunk.toString(),
      PERSON_ID_FIELD: 'personid',
      STACK_ID: stackId,
      LANDSCAPE: landscape,
      SECRET_ARN: secretArn!,
      IS_ECS_TASK: 'true',
      PAUSE_BEFORE_EARLY_EXIT: 'true', // Number of seconds to pause before early exit
      DRY_RUN: dryRun ? 'true' : 'false'
    };

    if (retries && (retries.retryStrategyOptions || retries.retryStrategyType)) {
      environment.RETRY_STRATEGY = JSON.stringify(retries);
    }

    // Set storage-mode-specific environment variables
    switch(previousStorageType) {
      case 's3':
        if(sharedDeltaStorageDir) {
          environment.SHARED_DELTA_STORAGE_DIR = sharedDeltaStorageDir;
        }
        break;
      case 'dynamodb':
        // Add DynamoDB-specific table names when using DynamoDB mode
        const { 
          personCurrentStateTable: { tableName: personCurrentStateTableName } = {},
          mockTargetPersonTable: { tableName: mockTargetPersonTableName } = {},
          statisticsTable: { tableName: statisticsTableName } = {},
          mockStatisticsTable: { tableName: mockStatisticsTableName } = {},
        } = dynamodb || {};
        if (personCurrentStateTableName) {
          environment.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME = personCurrentStateTableName;
        }
        if (mockTargetPersonTableName) {
          environment.DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME = mockTargetPersonTableName;
        }
        // Required by MetadataFactory.create() to route _flags.json/_metadata.json writes to DynamoDB
        if (statisticsTableName) {
          environment.DYNAMODB_STATISTICS_TABLE_NAME = statisticsTableName;
        }
        if (mockStatisticsTableName) {
          environment.DYNAMODB_MOCK_STATISTICS_TABLE_NAME = mockStatisticsTableName;
        }
        break;
    }

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-chunker-${landscape}`,
      retention: logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: `Chunker-${landscape}`,
      cpu,
      memoryLimitMiB,
      // Use ARM64 for Graviton2 (20% cost savings)
      runtimePlatform: {
        cpuArchitecture: CpuArchitecture.ARM64,
        operatingSystemFamily: OperatingSystemFamily.LINUX,
      },
    });

    // ECS secrets (injected at runtime from Secrets Manager)
    // These take precedence over environment variables in Fargate context
    const secrets = {
      // Inject entire huron-person config as JSON from Secrets Manager
      // This is retrieved by ECS at container startup and never appears in CloudFormation or logs
      HURON_PERSON_CONFIG_JSON: EcsSecret.fromSecretsManager(secret!),
    };

    // Add container
    this.taskDefinition.addContainer('ChunkerContainer', {
      containerName: 'chunker',
      image: ContainerImage.fromEcrRepository(
        repository,
        imageTag || 'latest'
      ),
      // Override CMD in Dockerfile to run chunker
      command: ['node', 'dist/docker/chunker.js'],
      logging: LogDriver.awsLogs({
        streamPrefix: 'chunker',
        logGroup,
      }),
      memoryLimitMiB, // Hard limit for container memory - if the container exceeds this, it will be killed. This is required to prevent runaway memory usage in case of issues.
      memoryReservationMiB, // Soft limit for container memory - the container can use more memory if available.
      environment,
      secrets
    });

    // Grant S3 read permissions for input bucket
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:GetObject',
          's3:GetObjectVersion',
        ],
        resources: [
          `arn:aws:s3:::${inputBucketName}/*`,
        ],
      })
    );
    
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:ListBucket',
        ],
        resources: [
          `arn:aws:s3:::${inputBucketName}`,
        ],
      })
    );

    // Grant S3 read+write permissions for chunks bucket
    // Read is needed to check if delta-storage/previous-input.ndjson exists
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:GetObject',
          's3:GetObjectVersion',
          's3:PutObject',
          's3:PutObjectAcl',
          's3:DeleteObject',
          's3:DeleteObjectVersion',
          's3:headObject',
        ],
        resources: [
          `arn:aws:s3:::${chunksBucketName}/*`,
        ],
      })
    );
    
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:ListBucket',
        ],
        resources: [
          `arn:aws:s3:::${chunksBucketName}`,
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

    // Grant SQS SendMessage permission for chunker queue
    // This allows chunker tasks to send the next message for parallel chunking
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'sqs:SendMessage',
        ],
        resources: [
          `arn:aws:sqs:${region}:${Stack.of(this).account}:*`,
        ],
      })
    );

    // Grant ECS UpdateService permission for chunker service
    // This allows chunker tasks to scale down the service when processing completes
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'ecs:UpdateService',
        ],
        resources: [
          `arn:aws:ecs:${region}:${Stack.of(this).account}:service/${ecsClusterName}/${ecsChunkerServiceName}`,
        ],
      })
    );

    // Grant ECS task protection permissions
    // This allows the running task to enable/disable scale-in protection via ECS agent endpoint
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'ecs:GetTaskProtection',
          'ecs:UpdateTaskProtection',
        ],
        resources: ['*'],
      })
    );

    
    // Grant DynamoDB permissions for reading and incrementing the atomic counter table.
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
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.atomicCounterTable.tableName}`,
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.atomicCounterTable.tableName}/index/*`,
        ],
      })
    );

    // Grant DynamoDB write permissions for PersonCurrentStateTable
    // Used for storing current sync state of each person in DynamoDB mode
    // Also grant read permission (Scan with Limit=1) to check if baseline data exists
    if (dynamodb!.personCurrentStateTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:Scan',       // Read: Used by sharedDeltaStorageExists() to check if any baseline data exists
            'dynamodb:PutItem',
            'dynamodb:UpdateItem',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.personCurrentStateTable.tableName}`,
          ],
        })
      );
    }

    // Grant DynamoDB read permissions for MockTargetPersonTable
    // Used when PersonCache needs to fetch population from mock target (instead of real API)
    if (dynamodb!.mockTargetPersonTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:Scan',
            'dynamodb:Query',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.mockTargetPersonTable.tableName}`,
          ],
        })
      );
    }

    // Grant DynamoDB read/write permissions for StatisticsTable
    // Used for reading/writing METADATA, FLAGS, and TERMINAL_ERROR event records in DynamoDB mode.
    // BatchWriteItem is required because DynamoDBTable.putItem() routes through batchWrite() internally.
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'dynamodb:GetItem',
          'dynamodb:Query',
          'dynamodb:PutItem',
          'dynamodb:UpdateItem',
          'dynamodb:BatchWriteItem',
        ],
        resources: [
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.statisticsTable.tableName}`,
        ],
      })
    );

    // Grant DynamoDB read/write permissions for the isolated mock statistics table
    // Used when the run is mock (flags.useMockTarget=true) so its entire statistics-table trail
    // (FLAGS/METADATA/TERMINAL_ERROR/STATISTICS/ERROR/CHUNK_STATUS) stays in this table only
    if (dynamodb!.mockStatisticsTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:GetItem',
            'dynamodb:Query',
            'dynamodb:PutItem',
            'dynamodb:UpdateItem',
            'dynamodb:BatchWriteItem',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.mockStatisticsTable.tableName}`,
          ],
        })
      );
    }

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (tags) {
      Object.entries(tags).forEach(([key, value]) => {
        Tags.of(this.taskDefinition).add(key, value);
      });
    }
  }

}
