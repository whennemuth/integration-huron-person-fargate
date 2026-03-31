import { RemovalPolicy, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, FargateTaskDefinition, LogDriver, OperatingSystemFamily } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';

export interface MergerTaskDefinitionProps {
  repository: IRepository;
  imageTag?: string;
  cpu: number;
  memoryLimitMiB: number;
  logRetentionDays: number;
  inputBucketName: string;
  chunksBucketName: string;
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

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-merger`,
      retention: props.logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: 'Merger',
      cpu: props.cpu,
      memoryLimitMiB: props.memoryLimitMiB,
      // Use ARM64 for Graviton2 (20% cost savings)
      runtimePlatform: {
        cpuArchitecture: CpuArchitecture.ARM64,
        operatingSystemFamily: OperatingSystemFamily.LINUX,
      },
    });

    // Add container
    const container = this.taskDefinition.addContainer('MergerContainer', {
      containerName: 'merger',
      image: ContainerImage.fromEcrRepository(
        props.repository,
        props.imageTag || 'latest'
      ),
      // Override CMD in Dockerfile to run merger
      command: ['node', 'dist/docker/merger.js'],
      logging: LogDriver.awsLogs({
        streamPrefix: 'merger',
        logGroup,
      }),
      environment: {
        REGION: props.region,
        INPUT_BUCKET: props.inputBucketName,
        // CHUNKS_BUCKET will be provided at runtime by Lambda
        DRY_RUN: props.dryRun ? 'true' : 'false',
        DESCRIPTION1: 
          `Container run by lambda function responding to S3 events when a new "chunk" 
          file comprising person delta (hashes) data is deposited into the ${props.chunksBucketName} 
          bucket.`,
        DESCRIPTION2: 
          `It checks the completeness of the delta chunks by referencing a metadata file created 
          by the chunker. If all expected delta chunks are present in the ${props.chunksBucketName}`,
        DESCRIPTION3:
          `bucket, it triggers the merger task to concatenate all NDJSON chunk files into a single file named previous-input.ndjson 
          in the same bucket, and then deletes the chunk files.`
      },
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
          `arn:aws:s3:::${props.chunksBucketName}`,
          `arn:aws:s3:::${props.chunksBucketName}/*`,
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
          `arn:aws:s3:::${props.chunksBucketName}/*`,
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
