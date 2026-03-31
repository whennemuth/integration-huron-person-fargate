import { SQS } from '@aws-sdk/client-sqs';
import { RemovalPolicy, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, FargateTaskDefinition, LogDriver, OperatingSystemFamily } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';

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

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-chunker`,
      retention: props.logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: 'Chunker',
      cpu: props.cpu,
      memoryLimitMiB: props.memoryLimitMiB,
      // Use ARM64 for Graviton2 (20% cost savings)
      runtimePlatform: {
        cpuArchitecture: CpuArchitecture.ARM64,
        operatingSystemFamily: OperatingSystemFamily.LINUX,
      },
    });

    // Add container
    const container = this.taskDefinition.addContainer('ChunkerContainer', {
      containerName: 'chunker',
      image: ContainerImage.fromEcrRepository(
        props.repository,
        props.imageTag || 'latest'
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
          data file is uploaded to the ${props.inputBucketName} bucket.`,
        DESCRIPTION2: 
          `It splits the file into smaller NDJSON chunk files, and writes the chunks back to 
          the ${props.chunksBucketName} bucket for parallel processing.`,
        REGION: props.region,
        SQS_QUEUE_URL: props.queueUrl,
        CHUNKS_BUCKET: props.chunksBucketName,
        ITEMS_PER_CHUNK: props.itemsPerChunk.toString(),
        PERSON_ID_FIELD: 'personid',
        // INPUT_BUCKET and INPUT_KEY will be provided at runtime by Lambda
        DRY_RUN: props.dryRun ? 'true' : 'false',
      },
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
          `arn:aws:s3:::${props.inputBucketName}/*`,
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
          `arn:aws:s3:::${props.inputBucketName}`,
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
          `arn:aws:s3:::${props.chunksBucketName}/*`,
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
          `arn:aws:s3:::${props.chunksBucketName}`,
        ],
      })
    );

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.taskDefinition).add(key, value);
      });
    }
  }
}
