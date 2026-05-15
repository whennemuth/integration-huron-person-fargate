import { RemovalPolicy, Stack, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, Secret as EcsSecret, FargateTaskDefinition, LogDriver, OperatingSystemFamily } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { TargetPersonDeleteType } from 'integration-huron-person/dist/types/src/config/Config';
import { HuronPersonSecrets } from '../../Secrets';


export interface MergerTaskDefinitionProps {
  repository: IRepository;
  imageTag?: string;
  cpu: number;
  memoryLimitMiB: number;
  memoryReservationMiB: number;
  logRetentionDays: number;
  inputBucketName: string;
  chunksBucketName: string;
  sharedDeltaStorageDir: string;
  personDeleteType: TargetPersonDeleteType;
  dynamoDbTableName: string;
  huronPersonSecrets: HuronPersonSecrets;
  region: string;
  dryRun?: boolean;
  tags?: { [key: string]: string };
}

/**
 * Creates a Fargate task definition for the merger (Phase 3)
 * Concatenates all NDJSON chunks into previous-input.ndjson
 */
export class MergerTaskDefinition extends Construct {
  public readonly taskDefinition: FargateTaskDefinition;

  constructor(scope: Construct, id: string, props: MergerTaskDefinitionProps) {
    super(scope, id);

    const { 
      cpu, memoryLimitMiB, memoryReservationMiB, region, inputBucketName, sharedDeltaStorageDir, personDeleteType, 
      repository, imageTag, dryRun, tags, logRetentionDays, chunksBucketName,
      dynamoDbTableName, huronPersonSecrets: { secret, secretArn , secretName } = {} 
    } = props;

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-merger`,
      retention: logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: 'Merger',
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
    const container = this.taskDefinition.addContainer('MergerContainer', {
      containerName: 'merger',
      image: ContainerImage.fromEcrRepository(repository,imageTag || 'latest'),
      // Override CMD in Dockerfile to run merger
      command: ['node', 'dist/docker/merger.js'],
      logging: LogDriver.awsLogs({
        streamPrefix: 'merger',
        logGroup,
      }),
      memoryLimitMiB, // Hard limit for container memory - if the container exceeds this, it will be killed. This is required to prevent runaway memory usage in case of issues.
      memoryReservationMiB, // Soft limit for container memory - the container can use more memory if available.
      environment: {
        REGION: region,
        INPUT_BUCKET: inputBucketName,
        // CHUNKS_BUCKET will be provided at runtime by Lambda
        SHARED_DELTA_STORAGE_DIR: sharedDeltaStorageDir,
        IS_ECS_TASK: 'true', // Used by the application code to determine if running in ECS context (vs local dev)
        PERSON_DELETE_TYPE: personDeleteType,
        DRY_RUN: dryRun ? 'true' : 'false',
        DYNAMODB_TABLE_NAME: dynamoDbTableName,
        SECRET_ARN: secretArn!, // ARN of the Secrets Manager secret to read config from
        DESCRIPTION1: 
          `Container run by lambda function responding to S3 events when a new "chunk" 
          file comprising person delta (hashes) data is deposited into the ${chunksBucketName} 
          bucket.`,
        DESCRIPTION2: 
          `It checks the completeness of the delta chunks by referencing a metadata file created 
          by the chunker. If all expected delta chunks are present in the ${chunksBucketName}`,
        DESCRIPTION3:
          `bucket, it triggers the merger task to concatenate all NDJSON chunk files into a single file named previous-input.ndjson 
          in the same bucket, and then deletes the chunk files.`
      },
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
