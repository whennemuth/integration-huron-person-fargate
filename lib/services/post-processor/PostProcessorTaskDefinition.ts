import { RemovalPolicy, Stack, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, Secret as EcsSecret, FargateTaskDefinition, LogDriver, OperatingSystemFamily } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { TargetPersonDeleteType } from 'integration-huron-person/dist/types/src/config/Config';
import { HuronPersonSecrets } from '../../Secrets';
import { StorageParams } from '../../TaskDefinitions';


export interface PostProcessorTaskDefinitionProps {
  repository: IRepository;
  imageTag?: string;
  cpu: number;
  memoryLimitMiB: number;
  memoryReservationMiB: number;
  logRetentionDays: number;
  inputBucketName: string;
  chunksBucketName: string;
  storageParams: StorageParams;
  personDeleteType: TargetPersonDeleteType;
  huronPersonSecrets: HuronPersonSecrets;
  region: string;
  landscape: string;
  dryRun?: boolean;
  tags?: { [key: string]: string };
}

/**
 * Creates a Fargate task definition for the post-processor (Phase 3), run once per run after every
 * chunk is processed: deletions for a full sync in both storage modes, plus, in S3 mode, merging the
 * run's delta files into the delta-storage/previous-input.ndjson baseline. See the DESCRIPTION environment variable.
 */
export class PostProcessorTaskDefinition extends Construct {
  public readonly taskDefinition: FargateTaskDefinition;

  constructor(scope: Construct, id: string, props: PostProcessorTaskDefinitionProps) {
    super(scope, id);

    const { 
      cpu, memoryLimitMiB, memoryReservationMiB, region, inputBucketName,  personDeleteType, 
      repository, imageTag, landscape, dryRun, tags, logRetentionDays, chunksBucketName,
      huronPersonSecrets: { secret, secretArn , secretName } = {},
      storageParams: { previousStorageType, storageConfig: { sharedDeltaStorageDir, dynamodb } = {} }
    } = props;

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-post-processor-${landscape}`,
      retention: logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: `PostProcessor-${landscape}`,
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

    const environment: { [key: string]: string } = {
      REGION: region,
      INPUT_BUCKET: inputBucketName,
      // CHUNKS_BUCKET will be provided at runtime by Lambda
      IS_ECS_TASK: 'true', // Used by the application code to determine if running in ECS context (vs local dev)
      PERSON_DELETE_TYPE: personDeleteType,
      DRY_RUN: dryRun ? 'true' : 'false',
      DYNAMODB_STATISTICS_TABLE_NAME: dynamodb!.statisticsTable.tableName,
      PREVIOUS_STORAGE_TYPE: previousStorageType!,
      SECRET_ARN: secretArn!, // ARN of the Secrets Manager secret to read config from
      // What this task does.
      DESCRIPTION:
        `Phase 3, once per run: started when the post-processor service scales up on a message ` +
        `(chunksBucket, chunkDirectory, createdAt) queued after every chunk of a run in ${chunksBucketName} is processed. ` +
        `DynamoDB mode: the processor completing the run's last chunk queues it, once (by claim). Nothing to ` +
        `merge: the processors already wrote each changed person's hash to the current-state and history tables. ` +
        `Deletions, for a full sync (person-full) only and never for a run with a terminal error: people in ` +
        `the current-state table but not in the run's raw chunk files (chunks/...) are deleted in Huron. ` +
        `PERSON_DELETE_TYPE (default soft: PATCH active=false) sets how. Each soft delete adds a DELETED history ` +
        `record and a DELETED:-prefixed hash with deletedAt, so later runs skip the person and reactivate a returning one. ` +
        `S3 mode: queued by the post-processor subscriber Lambda. Concatenates the run's delta files under ` +
        `deltas/{population}/{timestamp}/, merges them into delta-storage/previous-input.ndjson, then deletes them. ` +
        `Both modes: chunk files under chunks/ are left in place, and the run's RUN_SUMMARY is written to the ` +
        `statistics table at the end, whatever the outcome.`,
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
        } = dynamodb || {};
        if (personCurrentStateTableName) {
          environment.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME = personCurrentStateTableName;
        }
        if (personHistoryTableName) {
          environment.DYNAMODB_PERSON_HISTORY_TABLE_NAME = personHistoryTableName;
        }
        break;
      case 'database':
        // Not supported yet.
        break;
      default:
        throw new Error(`Unsupported storage type: ${previousStorageType}`);
    }

    // Add container
    const container = this.taskDefinition.addContainer('PostProcessorContainer', {
      containerName: 'post-processor',
      image: ContainerImage.fromEcrRepository(repository,imageTag || 'latest'),
      // Override CMD in Dockerfile to run post-processor
      command: ['node', 'dist/docker/post-processor.js'],
      logging: LogDriver.awsLogs({
        streamPrefix: 'post-processor',
        logGroup,
      }),
      memoryLimitMiB, // Hard limit for container memory - if the container exceeds this, it will be killed. This is required to prevent runaway memory usage in case of issues.
      memoryReservationMiB, // Soft limit for container memory - the container can use more memory if available.
      environment,
      secrets, // ECS secrets injected at runtime
    });

    // Grant S3 read permissions for chunks bucket
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:GetObject',
          's3:ListBucket',
        ],
        resources: [
          `arn:aws:s3:::${chunksBucketName}`,
          `arn:aws:s3:::${chunksBucketName}/*`,
        ],
      })
    );

    // Grant S3 write/delete permissions for chunks bucket (merge output and cleanup)
    // S3StreamProvider.moveResource() uses CopyObject + DeleteObject
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:PutObject',
          's3:DeleteObject',
          's3:CopyObject',
        ],
        resources: [
          `arn:aws:s3:::${chunksBucketName}/*`,
        ],
      })
    );

    // Grant DynamoDB permissions for writing error events and statistics
    // BatchWriteItem is required because DynamoDBTable.putItem() routes through batchWrite() internally.
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'dynamodb:PutItem',
          'dynamodb:UpdateItem',
          'dynamodb:Query',
          'dynamodb:GetItem',
          'dynamodb:BatchWriteItem',
        ],
        resources: [
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.statisticsTable.tableName}`,
          `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.statisticsTable.tableName}/index/*`,
        ],
      })
    );

    // Grant DynamoDB read/write permissions for PersonCurrentStateTable
    // Used for deletion detection (finding persons in storage but not in source) in DynamoDB mode,
    // and for marking soft-deleted persons so later runs don't select them again
    // (PersonCurrentStateTable.markDeleted() routes through DynamoDBTable.batchWrite()).
    if (dynamodb!.personCurrentStateTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:Query',
            'dynamodb:GetItem',
            'dynamodb:Scan', // Needed for full table scan to detect deletions
            'dynamodb:PutItem',
            'dynamodb:BatchWriteItem',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.personCurrentStateTable.tableName}`,
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.personCurrentStateTable.tableName}/index/*`,
          ],
        })
      );
    }

    // Grant DynamoDB write permissions for PersonHistoryTable
    // Used for writing DELETED event records during merge in DynamoDB mode.
    // BatchWriteItem is required because PersonHistoryTable.writeHistory() routes through
    // DynamoDBTable.putItem() -> batchWrite() internally.
    if (dynamodb!.personHistoryTable) {
      this.taskDefinition.addToTaskRolePolicy(
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'dynamodb:PutItem',
            'dynamodb:UpdateItem',
            'dynamodb:BatchWriteItem',
          ],
          resources: [
            `arn:aws:dynamodb:${region}:${Stack.of(this).account}:table/${dynamodb!.personHistoryTable.tableName}`,
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
