# CDK Deployment Guide

This guide covers deploying the Fargate-based Huron Person File Processor.

## Architecture Overview

**Two-Phase Parallel Processing:**

1. **Phase 1 (Chunker)**: 
   - S3 file drop → Lambda Subscriber → Fargate task (chunker)
   - Reads large JSON file, creates NDJSON chunks
   - Stores chunks in S3 `chunks/` folder

2. **Phase 2 (Processor)**:
   - S3 chunk created → SQS message → Fargate tasks (processors, auto-scaled 0-10)
   - Each task reads one chunk, syncs persons to Huron API
   - Parallel processing: 10 tasks = ~5 min vs ~16 min serial

## Prerequisites

1. **AWS Account & Credentials**
   ```bash
   aws configure --profile infnprd
   export AWS_PROFILE=infnprd
   ```

2. **Node.js & Dependencies**
   ```bash
   npm install
   ```

3. **Docker** (for building ARM64 images)
   ```bash
   docker --version  # Requires Docker 20.10+
   ```

4. **CDK Bootstrap** (one-time per account/region)
   ```bash
   npm run cdk bootstrap aws://770203350335/us-east-2
   ```

## Configuration

Edit [context/context.json](./context/context.json):

```json
{
  "ACCOUNT": "770203350335",
  "REGION": "us-east-2",
  "S3": {
    "inputBucket": "huron-person-file-drop-dev",
    "chunksBucket": "huron-person-chunks-dev"
  },
  "ITEMS_PER_CHUNK": 200,
  "ECS": {
    "processorService": {
      "maxScalingCapacity": 10
    }
  }
}
```

**Key Settings:**
- `ITEMS_PER_CHUNK`: Number of persons per chunk (default: 200)
- `maxScalingCapacity`: Max parallel processor tasks (default: 10)
- `inputBucket`: S3 bucket for JSON file uploads
- `chunksBucket`: S3 bucket for chunk storage (created by CDK)

## Deployment Steps

### 1. Deploy Infrastructure (First Time)

```bash
# Synthesize CloudFormation template
npm run build
npm run cdk synth

# Deploy stack (creates ECR, ECS, SQS, Lambda, S3)
npm run cdk deploy

# Or use the script
npm run deploy
```

**Resources Created:**
- ECR Repository: `huron-person-processor`
- ECS Cluster: `huron-person-cluster`
- S3 Bucket: `huron-person-chunks-dev`
- SQS Queue: `huron-person-chunks-queue` + DLQ
- Lambda: Chunker subscriber function
- Task Definitions: Chunker + Processor
- IAM Roles: Task execution and task roles

### 2. Build and Push Docker Image

```bash
# Build ARM64 image and push to ECR
npm run docker-publish

# Or with custom tag
cd docker && ./publish.sh v1.0.0
```

**What This Does:**
1. Logs in to ECR
2. Builds Docker image for `linux/arm64` (Graviton2)
3. Tags image with version + `latest`
4. Pushes to ECR repository

### 3. Update Task Definitions (After Image Push)

After pushing a new image, ECS tasks need to reference it:

```bash
# Force new deployment (picks up latest image)
npm run cdk deploy
```

Alternatively, update the image tag in task definitions:
```typescript
// lib/constructs/ChunkerTaskDefinition.ts
image: ecs.ContainerImage.fromEcrRepository(
  props.repository,
  'v1.0.0'  // Specify version instead of 'latest'
)
```

## Testing

### Upload Test File

```bash
# Copy test file to input bucket
aws s3 cp test-data/20260303111609986.json \
  s3://huron-person-file-drop-dev/

# Monitor Lambda logs (chunker subscriber)
aws logs tail /aws/lambda/huron-person-fargate-processor-ChunkerSubscriberLambda --follow

# Monitor Chunker logs
aws logs tail /ecs/huron-person-chunker --follow

# Monitor Processor logs
aws logs tail /ecs/huron-person-processor --follow
```

### Check SQS Queue Depth

```bash
# Get queue URL from stack outputs
QUEUE_URL=$(npm run cdk -- output huron-person-fargate-processor ProcessorQueueUrl --json | jq -r '."huron-person-fargate-processor".ProcessorQueueUrl')

# Check queue metrics
aws sqs get-queue-attributes \
  --queue-url $QUEUE_URL \
  --attribute-names All
```

### Monitor ECS Tasks

```bash
# List running tasks
aws ecs list-tasks --cluster huron-person-cluster

# Describe task
aws ecs describe-tasks \
  --cluster huron-person-cluster \
  --tasks <task-arn>
```

## Stack Outputs

After deployment, `npm run cdk -- output --all` shows:

```
huron-person-fargate-processor.EcrRepositoryUri = 770203350335.dkr.ecr.us-east-2.amazonaws.com/huron-person-processor
huron-person-fargate-processor.EcsClusterName = huron-person-cluster
huron-person-fargate-processor.ChunkerTaskDefinitionArn = arn:aws:ecs:us-east-2:770203350335:task-definition/...
huron-person-fargate-processor.ProcessorTaskDefinitionArn = arn:aws:ecs:us-east-2:770203350335:task-definition/...
huron-person-fargate-processor.ProcessorQueueUrl = https://sqs.us-east-2.amazonaws.com/770203350335/huron-person-chunks-queue
huron-person-fargate-processor.ChunksBucketName = huron-person-chunks-dev
```

## Troubleshooting

### Chunker Task Not Starting

**Check Lambda logs:**
```bash
aws logs tail /aws/lambda/*ChunkerSubscriberLambda* --follow
```

**Common issues:**
- Task definition not found → Redeploy CDK stack
- Image not found → Push Docker image to ECR
- Permissions error → Check IAM roles

### Processor Tasks Not Starting

**Check SQS messages:**
```bash
# Receive message from queue
aws sqs receive-message --queue-url $QUEUE_URL
```

**Common issues:**
- Queue empty → Chunker didn't create chunks (check chunker logs)
- Tasks failing → Check processor logs in CloudWatch
- Image pull error → Verify ECR image exists

### Tasks Failing

**Check CloudWatch logs:**
```bash
# Chunker logs
aws logs tail /ecs/huron-person-chunker --follow

# Processor logs
aws logs tail /ecs/huron-person-processor --follow
```

**Common issues:**
- S3 permissions → Check task role policies
- Out of memory → Increase `memoryLimitMiB` in context.json
- Timeout → Increase SQS `visibilityTimeoutSeconds`

### Dead Letter Queue Messages

**Check DLQ:**
```bash
DLQ_URL=$(aws sqs list-queues --queue-name-prefix huron-person-chunks-queue-dlq --query 'QueueUrls[0]' --output text)
aws sqs receive-message --queue-url $DLQ_URL
```

**Common reasons:**
- Task crashes → Check logs for errors
- Max receive count exceeded (3) → Check processor logic
- Invalid chunk file → Verify chunker output format

## Cost Optimization

**ARM64/Graviton2:**
- Saves ~20% vs x86_64
- Defined in task definitions: `cpuArchitecture: ecs.CpuArchitecture.ARM64`

**Auto-Scaling:**
- Processor tasks scale 0→10 based on queue depth
- Idle time = $0 (no tasks running)
- Max cost: 10 tasks × 0.04048 vCPU/hr + 0.004445 GB/hr

**S3 Lifecycle:**
- Chunks auto-delete after 7 days: `lifecycleRules: [{ expiration: Duration.days(7) }]`

## Updates & Redeployment

### Code Changes

1. **Modify source code** (src/chunker.ts, src/processor.ts)
2. **Rebuild and push image:**
   ```bash
   cd docker && ./publish.sh v1.0.1
   ```
3. **Update task definitions** (if using versioned tags)
4. **Redeploy:**
   ```bash
   npm run deploy
   ```

### Infrastructure Changes

1. **Modify CDK code** (lib/constructs/*.ts)
2. **Redeploy:**
   ```bash
   npm run build
   npm run cdk deploy
   ```

### Configuration Changes

1. **Modify context.json**
2. **Redeploy:**
   ```bash
   npm run cdk deploy
   ```

## Cleanup

**Delete stack:**
```bash
npm run cdk destroy
```

**Manual cleanup required:**
- ECR images (if repository not empty)
- S3 chunks bucket (if `autoDeleteObjects: false`)
- CloudWatch log groups (if `removalPolicy: RETAIN`)

## Next Steps

1. **Implement Huron API sync logic** in [src/PersonProcessor.ts](./src/PersonProcessor.ts)
2. **Create SSM Parameter** for Huron API key:
   ```bash
   aws ssm put-parameter \
     --name "REPLACE_WITH_SSM_PARAMETER_NAME" \
     --value "your-api-key" \
     --type SecureString
   ```
3. **Update context.json** with real Huron API endpoint
4. **Test with small files** before production deployment
5. **Set up CloudWatch alarms** for DLQ messages and task failures
