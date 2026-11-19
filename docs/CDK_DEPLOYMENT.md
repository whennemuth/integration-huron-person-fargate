# CDK Deployment Guide

This guide covers deploying the Fargate-based Huron Person pipeline.

## Architecture Overview

**Three-phase parallel processing**, each phase an ECS Fargate service fed by its own SQS queue:

1. **Chunker**: a run starts from the schedule, the dashboard's Bulk sync, the Runner, the `npm run start-*` scripts or a
   dropped file; the chunker subscriber Lambda queues it, and chunker tasks stream the source into NDJSON chunk files in
   S3 under `chunks/<population>/<syncRunId>/`.
2. **Processor**: each new chunk file reaches the processor queue through the processor subscriber Lambda; processor
   tasks (scaled on queue depth, from 0 to the context's `ECS.processorService.maxScalingCapacity`) each sync one
   chunk's people to the Huron API.
3. **Post-processor**: once every chunk is processed, consolidates the run's results (S3 mode), handles people missing
   from the source, and writes the run summary.

Logs: `/ecs/huron-person-chunker-<landscape>`, `/ecs/huron-person-processor-<landscape>`,
`/ecs/huron-person-post-processor-<landscape>`, and `/aws/lambda/<function>-<landscape>` for the subscriber Lambdas.
The event flow in full: [INFRASTRUCTURE_README.md](INFRASTRUCTURE_README.md#event-flow).

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

Edit [context/context.json](../context/context.json):

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

**Stacks:** the CDK app synthesizes the pipeline stack (`huron-person-fargate-processor-<landscape>`)
and, when the context has a `DASHBOARD` section, the landscape's dashboard stack
(`hp-dashboard-<landscape>`) plus the dashboard's us-east-1 edge-lambda stack (Lambda@Edge for
Shibboleth). Because there is more than one stack, a bare `cdk deploy` / `cdk destroy` fails -
name a stack or use `--all`, or use the npm scripts:

| Script | Stacks |
|---|---|
| `npm run deploy` | all (writes `cdk.out/synth.output.fargate.yaml` and `synth.output.dashboard.yaml`) |
| `npm run deploy-fargate` | pipeline, after the shared network stack it depends on (writes `cdk.out/synth.output.fargate.yaml`) |
| `npm run deploy-dashboard` | dashboard + edge-lambda (writes `cdk.out/synth.output.dashboard.yaml`) |
| `npm run teardown` | all |
| `npm run teardown-fargate` | pipeline |
| `npm run teardown-dashboard` | dashboard + edge-lambda |

**The shared network:** every landscape's pipeline runs in the network of `huron-integration-network`
(`lib/NetworkStack.ts`), one stack per account and region, which the pipeline stack depends on. Deploying a pipeline
deploys it first (creating it in a new account); no teardown script touches it, and it is termination-protected.
The pipeline reads the network's IDs from its SSM parameters at deploy time. See "Shared Network Stack" in
[CLAUDE.md](../CLAUDE.md) for the rules, and [SHARED_NETWORK.md](SHARED_NETWORK.md) for why it exists and how it was imported.

**Order:** the dashboard stack takes nothing from the pipeline stack's outputs - it finds the
pipeline's config secret, tables and chunks bucket by name - so the stacks deploy in any order.
But the dashboard only works once its landscape's pipeline stack exists; until then its pages
load and its data calls fail. Deploying both separately: `deploy-fargate` first, then
`deploy-dashboard`.

**Edge-lambda teardown:** CloudFront removes its replicas of the Lambda@Edge functions some time
(up to hours) after the distribution is deleted, and the edge-lambda stack can't be deleted
before that. If `teardown` / `teardown-dashboard` fails on that stack, rerun it later.

### 1. Deploy Infrastructure (First Time)

```bash
# Synthesize CloudFormation templates
npm run build
npm run synth

# Deploy every stack (pipeline: ECR, ECS, SQS, Lambda, S3; dashboard: CloudFront, Lambda, A record)
npm run deploy
```

**Resources Created:**
- ECR Repository: `huron-person-integration`
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
npm run deploy-fargate
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

### Stack creation/update known issues

**Transient AWS Lambda Service-Side Error:**

You may encounter a stack creation/update error that looks like this: 
```
Resource handler returned message: "The role defined for the function cannot be assumed by Lambda. (Service: Lambda, Status Code: 400, Request ID: feefaedf-ed97-40f4-832f-171cd0ed3c08) (SDK Attempt Count: 1)" (RequestToken: 3e22193a-2bde-1510-e55e-f168363a1832, HandlerErrorCode: InvalidRequest)
```

The Lambda API itself occasionally returns 400 - The role defined for the function cannot be assumed as a transient error even when the role is fully valid and propagated. This is a known intermittent behavior in the Lambda control plane — the error message is misleading and doesn't always mean the role is actually unassumable.

There is not much you can do to rule out this transient error. However, retrying the stack creation/update should resolve the issue. 

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
   npm run deploy-fargate
   ```

### Configuration Changes

1. **Modify context.json**
2. **Redeploy:**
   ```bash
   npm run deploy
   ```

## Cleanup

**Delete stacks:**
```bash
npm run teardown            # all stacks
npm run teardown-fargate    # pipeline stack only
npm run teardown-dashboard  # dashboard + edge-lambda stacks (see "Edge-lambda teardown" above)
```

**Manual cleanup required:**
- ECR images (if repository not empty)
- S3 chunks bucket (if `autoDeleteObjects: false`)
- CloudWatch log groups (if `removalPolicy: RETAIN`)

## Next Steps

1. **Publish the task image** to the landscape's ECR repository: `npm run docker-publish`
2. **Start a run**: the dashboard's Bulk sync, `npm run start-full-sync` (or another `start-*` script), or the Runner
   (`src/runner/Runner.ts`); the person sync itself is in `src/processing/` (`ProcessorForS3.ts`,
   `ProcessorForDynamoDb.ts`)
3. **Test with a mock landscape or a small population** before a production run
4. **Set up CloudWatch alarms** for DLQ messages and task failures
