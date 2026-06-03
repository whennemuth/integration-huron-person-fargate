# integration-huron-person-fargate/lib: CDK Infrastructure Patterns

## Purpose
Defines AWS infrastructure-as-code using CDK. Orchestrates Lambda, ECS Fargate, S3, SQS, and other services for the three-phase pipeline.

## Key Components

### Stack.ts
**Purpose**: Main CDK stack orchestrating all infrastructure

**Responsibilities**:
- Define VPC and networking
- Create Lambda functions (chunker orchestrator)
- Create ECS cluster and task definitions
- Create S3 buckets (input, chunks, processed, output)
- Create SQS queues (phase communication)
- Create IAM roles and permissions
- Wire all components together

**Pattern**:
```typescript
export class HuronStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    
    // 1. Networking
    const vpc = new ec2.Vpc(this, 'VPC', { /* ... */ });
    
    // 2. Storage
    const s3Bucket = new s3.Bucket(this, 'S3Bucket', { /* ... */ });
    
    // 3. Queuing
    const sqsQueue = new sqs.Queue(this, 'SQSQueue', { /* ... */ });
    
    // 4. Compute
    const chunkerLambda = new lambda.Function(this, 'Chunker', { /* ... */ });
    const processorCluster = new ecs.Cluster(this, 'Cluster', { /* ... */ });
    
    // 5. Permissions
    chunkerLambda.addToRolePolicy(/* S3 access */);
    processorCluster.addToRolePolicy(/* S3/SQS access */);
  }
}
```

### ChunkerService (lib/services/chunker/ChunkerService.ts)
**Purpose**: Lambda service that orchestrates Phase 1 (chunking)

**Environment Prefix**: CHUNKER_SERVICE

**Responsibilities**:
- Triggered by scheduled event or manual invocation
- Load chunking configuration
- Instantiate appropriate chunker (API, S3, etc.)
- Execute chunking
- Signal processors via SQS

**Test Harness**: Can be tested as module standalone with TestEnvironment

## Infrastructure Patterns

### Separation of Concerns

```
lib/
  ├─ Stack.ts           (main CDK orchestrator)
  └─ services/
      └─ chunker/
          └─ ChunkerService.ts  (Phase 1 orchestration)

docker/                 (container images)
  ├─ chunker.ts         (Phase 1 container entry)
  ├─ processor.ts       (Phase 2 container entry)
  └─ merger.ts          (Phase 3 container entry)

src/
  ├─ chunking/          (Phase 1 logic)
  ├─ processing/        (Phase 2 logic)
  └─ merging/           (Phase 3 logic)
```

### Context Configuration

**Files**: `context/context.json`, `context/IContext.ts`

**Purpose**: Environment-specific infrastructure parameters

```json
{
  "cidr": "10.0.0.0/16",
  "vpcSubnets": ["us-east-2a", "us-east-2b"],
  "ecrImageUri": "123456789.dkr.ecr.us-east-2.amazonaws.com/processor:latest",
  "s3BucketName": "huron-pipeline-dev",
  "sqsQueueUrl": "https://sqs.us-east-2.amazonaws.com/..."
}
```

**Usage in CDK**:
```typescript
const context = new Context();
const imageUri = context.getValue('ecrImageUri');
const bucket = new s3.Bucket(this, 'Bucket', {
  bucketName: context.getValue('s3BucketName')
});
```

**Tier-Specific Contexts**:
- `context.json` – Development
- `context.staging.json` – Staging
- `context.prod.json` – Production

## IAM Permissions Pattern

### Least Privilege Principle

```typescript
// ChunkerLambda role: S3, SQS write, Secrets Manager read
const chunkerRole = new iam.Role(this, 'ChunkerRole', {
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
});

chunkerRole.addToPrincipalPolicy(
  new iam.PolicyStatement({
    actions: ['s3:GetObject', 's3:PutObject'],
    resources: [bucket.arnForObjects('chunks/*')]
  })
);

chunkerRole.addToPrincipalPolicy(
  new iam.PolicyStatement({
    actions: ['sqs:SendMessage'],
    resources: [queue.queueArn]
  })
);

chunkerRole.addToPrincipalPolicy(
  new iam.PolicyStatement({
    actions: ['secretsmanager:GetSecretValue'],
    resources: [secretArn]
  })
);
```

### ProcessorTask role: Broader permissions (processing logic needs them)

```typescript
const processorRole = new iam.Role(this, 'ProcessorRole', {
  assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com')
});

// S3: read chunks, write processed
processorRole.addToPrincipalPolicy(
  new iam.PolicyStatement({
    actions: ['s3:GetObject', 's3:PutObject'],
    resources: [bucket.arnForObjects('*')]
  })
);

// SQS: receive messages
processorRole.addToPrincipalPolicy(
  new iam.PolicyStatement({
    actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage'],
    resources: [queue.queueArn]
  })
);

// CloudWatch: log output
processorRole.addToPrincipalPolicy(
  new iam.PolicyStatement({
    actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
    resources: [logGroupArn]
  })
);
```

## Testing Infrastructure Code

### ChunkerService Harness
**Validation**: Service can load configuration and execute chunking logic

```bash
npx ts-node lib/services/chunker/ChunkerService.ts
```

### CDK Synthesis
**Validation**: CloudFormation template generates without errors

```bash
cdk synthesize
# Outputs: cdk.out/HuronStack.template.json
```

### Dry-Run Deployment
**Validation**: Show what will be created

```bash
cdk diff
# Shows: [+] New S3 bucket
#        [+] New SQS queue
#        [+] etc.
```

## Deployment Workflow

1. **Update context.json** with environment-specific values
2. **Run ChunkerService harness** to validate configuration
3. **Test Docker images** locally before deployment
4. **Run `cdk diff`** to review changes
5. **Run `cdk deploy`** to deploy infrastructure
6. **Verify resources created** in AWS console
7. **Monitor logs** in CloudWatch

## Common CDK Patterns

### Conditional Resources
```typescript
if (this.isProd) {
  // Production-only resources
  new rds.Database(this, 'Database', { /* ... */ });
}
```

### Cross-Stack References
```typescript
// Stack A exports
export const bucketName = bucket.bucketName;

// Stack B imports
const bucketName = cdk.Fn.importValue('HuronStack-BucketName');
```

### Parameterization
```typescript
const config = new Context();
const imageUri = config.getValue('ecrImageUri');
const taskDefinition = new ecs.FargateTaskDefinition(
  this, 'ProcessorTask',
  { cpu: 1024, memoryLimitMiB: 2048 }
);
```

## Infrastructure Concerns by Phase

### Phase 1 (Chunking)
- Lambda function with S3 access
- Input S3 bucket (data source)
- SQS queue (signal processors)

### Phase 2 (Processing)
- ECS cluster and task definition
- ECR image for processor
- S3 access (read chunks, write processed)
- SQS consumer

### Phase 3 (Merging)
- ECS task definition
- ECR image for merger
- S3 access (read all processed chunks)
- Output bucket (write delta storage)

