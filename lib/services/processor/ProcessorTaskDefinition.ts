import { RemovalPolicy, Stack, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, Secret as EcsSecret, FargateTaskDefinition, LogDriver, OperatingSystemFamily } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { IContext } from '../../../context/IContext';
import { Config } from 'integration-huron-person';
import { HuronPersonSecrets } from '../../Secrets';
import { StorageParams } from '../../TaskDefinitions';

export interface ProcessorTaskDefinitionProps {
  repository: IRepository;
  imageTag?: string;
  cpu: number;
  memoryLimitMiB: number;
  memoryReservationMiB: number;
  logRetentionDays: number;
  chunksBucketName: string;
  queueUrl: string;
  storageParams: StorageParams;
  context: IContext;
  config: Config;
  region: string;
  landscape: string;
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
      memoryLimitMiB, memoryReservationMiB, cpu, region, queueUrl,  chunksBucketName, 
      context, repository, imageTag, landscape, dryRun, tags,
      storageParams: { previousStorageType, storageConfig: { sharedDeltaStorageDir, dynamodb } = {} },
      config: { preLoadedMaps: { orgMap=false, stateMap=false, countryMap=false } = {} },
    } = props;

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-processor-${landscape}`,
      retention: logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: `Processor-${landscape}`,
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
      PREVIOUS_STORAGE_TYPE: previousStorageType!,
      DYNAMODB_STATISTICS_TABLE_NAME: dynamodb!.statisticsTable.tableName,
      // CHUNKS_BUCKET and CHUNK_KEY are set from SQS messages at runtime (not env vars)
      STATIC_MAP_USAGE: `{ "orgMap": ${orgMap}, "stateMap": ${stateMap}, "countryMap": ${countryMap} }`, // Used by processor to determine which static maps to load in data mapper
      DYNAMODB_MOCK_STATISTICS_TABLE_NAME: dynamodb!.mockStatisticsTable.tableName, // Isolated statistics table for mocked runs (flags.useMockTarget)
      SECRET_ARN: secretArn!, // ARN of the Secrets Manager secret to read config from
      IS_ECS_TASK: 'true', // Used by the application code to determine if running in ECS context (vs local dev)
      DRY_RUN: dryRun ? 'true' : 'false',
      DESCRIPTION1: `Container run by lambda function responding to S3 events when a new "chunk" 
        file comprising person data is deposited into ${chunksBucketName}.`,
      DESCRIPTION2: 
        `It processes the chunk by syncing all person records in it to the Huron API.`
    };

    switch(previousStorageType) {
      case 's3':
        if(sharedDeltaStorageDir) {
          environment.SHARED_DELTA_STORAGE_DIR = sharedDeltaStorageDir;
        }
        break;
      case 'dynamodb':
        // Add DynamoDB-specific table names when using DynamoDB mode
        // These tables only exist in DynamoDB mode and are used to distinguish between S3 and DynamoDB storage modes
        const { 
          personCurrentStateTable: { tableName: personCurrentStateTableName } = {}, 
          personHistoryTable: { tableName: personHistoryTableName } = {},
          mockTargetPersonTable: { tableName: mockTargetPersonTableName } = {},
          mockPersonCurrentStateTable: { tableName: mockPersonCurrentStateTableName } = {},
          mockPersonHistoryTable: { tableName: mockPersonHistoryTableName } = {},
        } = dynamodb || {};
        if (personCurrentStateTableName) {
          environment.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME = personCurrentStateTableName;
        }
        if (personHistoryTableName) {
          environment.DYNAMODB_PERSON_HISTORY_TABLE_NAME = personHistoryTableName;
        }
        if (mockTargetPersonTableName) {
          environment.DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME = mockTargetPersonTableName;
        }
        if (mockPersonCurrentStateTableName) {
          environment.DYNAMODB_MOCK_PERSON_CURRENT_STATE_TABLE_NAME = mockPersonCurrentStateTableName;
        }
        if (mockPersonHistoryTableName) {
          environment.DYNAMODB_MOCK_PERSON_HISTORY_TABLE_NAME = mockPersonHistoryTableName;
        }
        break;
      case 'database':
        // Not supported yet.
        break;
      default:
        throw new Error(`Unsupported storage type: ${previousStorageType}`);
    }

    // Check if the context includes retry strategy configuration and add it to environment variables if present.
    const { retries } = context.ECS.processorTaskDefinition;
    if(retries) {
      const { retryStrategyOptions, retryStrategyType } = retries;
      if(retryStrategyOptions || retryStrategyType) {
        environment.RETRY_STRATEGY = JSON.stringify(retries);
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
      memoryLimitMiB, // Hard limit for container memory - if the container exceeds this, it will be killed. This is required to prevent runaway memory usage in case of issues.
      memoryReservationMiB, // Soft limit for container memory - the container can use more memory if available.
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
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.statisticsTable.tableName}`,
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.statisticsTable.tableName}/index/*`,
        ],
      })
    );

    // Grant DynamoDB permissions for incrementing and reading atomic counters (used for generating unique chunk IDs and other operations)
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

    // Grant DynamoDB read permissions for PersonCurrentStateTable
    // Used for reading current person sync state during processing in DynamoDB mode
    if (dynamodb!.personCurrentStateTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:GetItem',
            'dynamodb:Query',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.personCurrentStateTable.tableName}`,
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.personCurrentStateTable.tableName}/index/*`,
          ],
        })
      );
    }

    // Grant DynamoDB write permissions for PersonHistoryTable
    // Used for writing person change history records during processing in DynamoDB mode
    if (dynamodb!.personHistoryTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:PutItem',
            'dynamodb:UpdateItem',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.personHistoryTable.tableName}`,
          ],
        })
      );
    }

    // Grant DynamoDB read/write permissions for mockTargetPersonTable
    // Used when flags.useMockTarget is true to simulate target system without calling real API
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'dynamodb:GetItem',
          'dynamodb:PutItem',
          'dynamodb:UpdateItem',
          'dynamodb:DeleteItem',
          'dynamodb:Query',
          'dynamodb:Scan',
          'dynamodb:BatchGetItem',
        ],
        resources: [
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.mockTargetPersonTable.tableName}`,
        ],
      })
    );

    // Grant DynamoDB read/write permissions for mockStatisticsTable
    // Used when flags.useMockTarget is true so bulk STATISTICS/ERROR/CHUNK_STATUS records never mix with production data
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
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.mockStatisticsTable.tableName}`,
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.mockStatisticsTable.tableName}/index/*`,
        ],
      })
    );

    // Grant DynamoDB read/write permissions for mockPersonCurrentStateTable and mockPersonHistoryTable
    // Used when flags.useMockTarget is true so DeltaStrategyForDynamoDB never mixes mocked hash/history state with production data
    if (dynamodb!.mockPersonCurrentStateTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:GetItem',
            'dynamodb:PutItem',
            'dynamodb:UpdateItem',
            'dynamodb:Query',
            'dynamodb:BatchGetItem',
            'dynamodb:BatchWriteItem',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.mockPersonCurrentStateTable.tableName}`,
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.mockPersonCurrentStateTable.tableName}/index/*`,
          ],
        })
      );
    }

    if (dynamodb!.mockPersonHistoryTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:PutItem',
            'dynamodb:UpdateItem',
            'dynamodb:BatchWriteItem',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.mockPersonHistoryTable.tableName}`,
          ],
        })
      );
    }

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
