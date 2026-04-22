import { RemovalPolicy, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, Secret as EcsSecret, FargateTaskDefinition, LogDriver, OperatingSystemFamily } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { HuronPersonSecrets } from '../../Secrets';

export interface ChunkerTaskDefinitionProps {
  repository: IRepository;
  imageTag?: string;
  cpu: number;
  memoryLimitMiB: number;
  logRetentionDays: number;
  queueUrl: string;
  inputBucketName: string;
  chunksBucketName: string;
  itemsPerChunk: number;
  region: string;
  huronPersonSecrets: HuronPersonSecrets;
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
      huronPersonSecrets: { secret, secretArn , secretName } = {}, logRetentionDays, 
      memoryLimitMiB, cpu, region, queueUrl, itemsPerChunk, chunksBucketName, inputBucketName,
      repository, imageTag, dryRun, tags 
    } = props;

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-chunker`,
      retention: logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: 'Chunker',
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
    const container = this.taskDefinition.addContainer('ChunkerContainer', {
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
      environment: {
        DESCRIPTION1: 
          `Container run by a lambda function responding to S3 events when a new large person 
          data file is uploaded to the ${inputBucketName} bucket.`,
        DESCRIPTION2: 
          `It splits the file into smaller NDJSON chunk files, and writes the chunks back to 
          the ${chunksBucketName} bucket for parallel processing.`,
        REGION: region,
        SQS_QUEUE_URL: queueUrl,
        CHUNKS_BUCKET: chunksBucketName,
        ITEMS_PER_CHUNK: itemsPerChunk.toString(),
        PERSON_ID_FIELD: 'personid',
        SECRET_ARN: secretArn!, // ARN of the Secrets Manager secret to read config from
        // INPUT_BUCKET and INPUT_KEY will be provided at runtime by Lambda
        DRY_RUN: dryRun ? 'true' : 'false',
      },
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

    // Grant S3 write permissions for chunks bucket
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:PutObject',
          's3:PutObjectAcl',
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

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (tags) {
      Object.entries(tags).forEach(([key, value]) => {
        Tags.of(this.taskDefinition).add(key, value);
      });
    }
  }
}
