# Huron Person Fargate Processor - CDK Infrastructure

This document provides an overview of the CDK infrastructure created for the Fargate-based Huron Person File Processor.

## Quick Start

### 1. Deploy Infrastructure

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Deploy CDK stack
npm run cdk deploy

# Or use the deployment script
npm run deploy
```

### 2. Build and Push Docker Image

```bash
# Build and push to ECR
npm run docker-publish
```

### 3. Test

```bash
# Upload a JSON file to trigger processing
aws s3 cp test-data/sample.json s3://huron-person-file-drop-dev/
```

## Infrastructure Components

### Created Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `huron-person-processor` | ECR Repository | Stores Docker images |
| `huron-person-cluster` | ECS Cluster | Runs Fargate tasks |
| `huron-person-chunks-dev` | S3 Bucket | Stores NDJSON chunks (7-day lifecycle) |
| `huron-person-chunks-queue` | SQS Queue | Distributes chunks to processors |
| `huron-person-chunks-queue-dlq` | SQS DLQ | Failed message handling |
| `ChunkerTaskDefinition` | ECS Task Def | Phase 1: Creates chunks |
| `ProcessorTaskDefinition` | ECS Task Def | Phase 2: Processes chunks |
| `ChunkerSubscriberLambda` | Lambda Function | Triggers chunker on file upload |
| `ProcessorService` | Fargate Service | Auto-scales processors (0-10 tasks) |

### IAM Roles

- **Chunker Task Role**: Read from input bucket, write to chunks bucket
- **Processor Task Role**: Read from chunks bucket, read SSM parameter (API key)
- **Lambda Execution Role**: RunTask permission for chunker

### Event Flow

```
S3 Upload (JSON) → Lambda → RunTask (Chunker)
                                ↓
                          Create Chunks → S3
                                ↓
                          S3 Event → SQS
                                ↓
                    Fargate Processors (0-10 tasks)
                                ↓
                          Huron API Sync
```

## Configuration

### context.json

Key settings in [context/context.json](../context/context.json):

```json
{
  "ITEMS_PER_CHUNK": 200,
  "ECS": {
    "chunkerTaskDefinition": {
      "cpu": 2048,
      "memoryLimitMiB": 4096
    },
    "processorService": {
      "maxScalingCapacity": 10
    }
  },
  "SQS": {
    "visibilityTimeoutSeconds": 900
  }
}
```

**Tuning Guide:**
- `ITEMS_PER_CHUNK`: Higher = fewer chunks, lower memory per processor task
- `chunkerTaskDefinition.memoryLimitMiB`: Increase if chunker OOMs on large files
- `processorService.maxScalingCapacity`: Max parallel tasks (10 = ~5min for 80K persons)
- `visibilityTimeoutSeconds`: Must exceed max processor task runtime

## CDK Constructs

All constructs are in [lib/constructs/](../lib/constructs/):

| File | Construct | Description |
|------|-----------|-------------|
| `EcrRepository.ts` | `EcrRepository` | ECR repo with lifecycle rules |
| `EcsCluster.ts` | `EcsCluster` | Cluster + VPC (creates or lookup) |
| `ChunkerTaskDefinition.ts` | `ChunkerTaskDefinition` | Chunker task (ARM64, 2-4 vCPU) |
| `ProcessorTaskDefinition.ts` | `ProcessorTaskDefinition` | Processor task (ARM64, 1-2 vCPU) |
| `ChunkerTriggerLambda.ts` | `ChunkerTriggerLambda` | Lambda inline code |
| `ProcessorService.ts` | `ProcessorService` | Queue + auto-scaling service |

## Deployment Scripts

| Script | Purpose |
|--------|---------|
| [docker/publish.sh](../docker/publish.sh) | Build ARM64 image, push to ECR |
| [bin/deploy.sh](../bin/deploy.sh) | Deploy CDK stack |

## Monitoring

### CloudWatch Logs

```bash
# Chunker logs
aws logs tail /ecs/huron-person-chunker --follow

# Processor logs
aws logs tail /ecs/huron-person-processor --follow

# Lambda logs
aws logs tail /aws/lambda/*ChunkerTriggerLambda* --follow
```

### SQS Metrics

```bash
# Get queue URL from stack outputs
QUEUE_URL=$(npm run cdk -- output huron-person-fargate-processor ProcessorQueueUrl --json | jq -r '.["huron-person-fargate-processor"].ProcessorQueueUrl')

# Check queue depth
aws sqs get-queue-attributes --queue-url $QUEUE_URL --attribute-names ApproximateNumberOfMessages
```

### ECS Tasks

```bash
# List running tasks
aws ecs list-tasks --cluster huron-person-cluster

# Get task details
aws ecs describe-tasks --cluster huron-person-cluster --tasks <task-arn>
```

## Cost Optimization

- **ARM64/Graviton2**: ~20% savings vs x86_64
- **Auto-scaling**: Processors scale 0→N (pay only when processing)
- **No NAT Gateway**: Public subnets only (saves ~$32/month per AZ)
- **S3 Lifecycle**: Chunks auto-delete after 7 days
- **Log Retention**: 7 days (configurable in context.json)

**Estimated Costs** (80K person file, 10 parallel processors):
- Chunker task: 1 task × 5 min × $0.04048/vCPU-hr × 2 vCPU = $0.007
- Processor tasks: 10 tasks × 5 min × $0.04048/vCPU-hr × 1 vCPU = $0.034
- Total per file: ~$0.04

## Troubleshooting

See [CDK_DEPLOYMENT.md](../docs/CDK_DEPLOYMENT.md) for detailed troubleshooting guide.

**Common Issues:**

1. **Build errors (TS5055)**: Run `rm -rf dist && npm run build`
2. **VPC lookup fails**: Set `useDefaultVpc: false` in `EcsCluster` props
3. **Task not starting**: Check image exists in ECR (`aws ecr list-images`)
4. **DLQ messages**: Check processor logs for errors

## Next Steps

1. **Implement actual Huron API sync** in [src/PersonProcessor.ts](../src/PersonProcessor.ts)
2. **Create SSM parameter** for API key
3. **Test with real data**
4. **Set up CloudWatch alarms** for failures
5. **Configure production settings** (retention policies, etc.)

## Documentation

- [CDK Deployment Guide](./CDK_DEPLOYMENT.md) - Full deployment instructions
- [Docker README](../docker/README.md) - Docker build/run guide
- [Infrastructure Variants](./infrastructure-variants.md) - Architecture decisions

## Stack Outputs

After deployment, key outputs:

```
EcrRepositoryUri: 770203350335.dkr.ecr.us-east-2.amazonaws.com/huron-person-processor
EcsClusterName: huron-person-cluster
ChunkerTaskDefinitionArn: arn:aws:ecs:...
ProcessorTaskDefinitionArn: arn:aws:ecs:...
ProcessorQueueUrl: https://sqs.us-east-2.amazonaws.com/.../huron-person-chunks-queue
ChunksBucketName: huron-person-chunks-dev
```
