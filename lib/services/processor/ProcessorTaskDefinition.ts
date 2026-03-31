import { RemovalPolicy, Stack, Tags } from 'aws-cdk-lib';
import { IRepository } from 'aws-cdk-lib/aws-ecr';
import { ContainerImage, CpuArchitecture, FargateTaskDefinition, LogDriver, OperatingSystemFamily, Secret as EcsSecret, LogDrivers } from 'aws-cdk-lib/aws-ecs';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
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
  context: IContext;
  region: string;
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

    // Create Secrets Manager secret for huron-person configuration
    const huronPersonSecrets = new HuronPersonSecrets(this, props.context);

    // Create SSM Parameter for BULK_RESET flag (allows runtime toggling without stack updates)
    const { STACK_ID, BULK_RESET = false } = props.context;
    const bulkResetParameter = new StringParameter(this, 'BulkResetParameter', {
      parameterName: `/huron-person-integration/${STACK_ID}/bulk-reset`,
      description: 'Enable bulk reset mode for huron person integration processor tasks (true/false). When true, queries target system for each person instead of using delta storage.',
      stringValue: BULK_RESET.toString(),
      simpleName: false, // Use full parameter name with path
    });

    // Create CloudWatch log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/ecs/huron-person-processor`,
      retention: props.logRetentionDays as RetentionDays,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create task definition
    this.taskDefinition = new FargateTaskDefinition(this, 'TaskDefinition', {
      family: 'Processor',
      cpu: props.cpu,
      memoryLimitMiB: props.memoryLimitMiB,
      // Use ARM64 for Graviton2 (20% cost savings)
      runtimePlatform: {
        cpuArchitecture: CpuArchitecture.ARM64,
        operatingSystemFamily: OperatingSystemFamily.LINUX,
      },
    });

    // Add container with environment variables
    const environment: { [key: string]: string } = {
      REGION: props.region,
      SQS_QUEUE_URL: props.queueUrl,
      STACK_ID: props.context.STACK_ID, // Used for SSM parameter name lookup
      // CHUNKS_BUCKET and CHUNK_KEY are set from SQS messages at runtime (not env vars)
      STATIC_MAP_USAGE: '{ "orgMap": true, "stateMap": true, "countryMap": true }', // Used by processor to determine which static maps to load in data mapper
      SECRET_ARN: huronPersonSecrets.secretArn, // ARN of the Secrets Manager secret to read config from
      DRY_RUN: props.dryRun ? 'true' : 'false',
      DESCRIPTION1: `Container run by lambda function responding to S3 events when a new "chunk" 
        file comprising person data is deposited into ${props.chunksBucketName}.`,
      DESCRIPTION2: 
        `It processes the chunk by syncing all person records in it to the Huron API.`
    };

    // ECS secrets (injected at runtime from Secrets Manager)
    // These take precedence over environment variables in Fargate context
    const secrets = {
      // Inject entire huron-person config as JSON from Secrets Manager
      // This is retrieved by ECS at container startup and never appears in CloudFormation or logs
      HURON_PERSON_CONFIG_JSON: EcsSecret.fromSecretsManager(huronPersonSecrets.secret),
    };

    const container = this.taskDefinition.addContainer('ProcessorContainer', {
      containerName: 'processor',
      image: ContainerImage.fromEcrRepository(
        props.repository,
        props.imageTag || 'latest'
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

    // Second method for the lambda handler to get the secret - an alternative for environment 
    // variable injection.
    huronPersonSecrets.secret.grantRead(this.taskDefinition.executionRole!); // Grant read access to the secret for the execution role (used by ECS agent at startup)

    // Grant S3 permissions for processor operations on chunks bucket only
    // Processor.ts overrides storage.config.bucketName to use chunks bucket, so processor
    // does not need access to input bucket at all. Input bucket is only for chunker.ts.
    const chunksBucket = props.chunksBucketName;
    
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
        resources: [`arn:aws:s3:::${chunksBucket}/*`],
      })
    );
    
    // Grant bucket-level permissions (ListBucket)
    this.taskDefinition.addToTaskRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:ListBucket',
        ],
        resources: [`arn:aws:s3:::${chunksBucket}`],
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
          `arn:aws:sqs:${props.region}:${Stack.of(this).account}:*`,
        ],
      })
    );

    // Grant Secrets Manager read access for huron-person configuration
    // IMPORTANT: Secrets are retrieved by the EXECUTION ROLE at container startup,
    // not the task role. The execution role is used by ECS agent to pull images,
    // retrieve secrets, and write logs before the container even starts.
    huronPersonSecrets.secret.grantRead(this.taskDefinition.executionRole!);

    // Grant SSM Parameter Store read access for BULK_RESET flag
    // This is read by the TASK ROLE at runtime (not execution role)
    bulkResetParameter.grantRead(this.taskDefinition.taskRole);

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.taskDefinition).add(key, value);
      });
    }
  }
}
