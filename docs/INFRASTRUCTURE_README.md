# Huron Person Fargate Processor - CDK Infrastructure

This document provides an overview of the CDK infrastructure for the Fargate-based Huron Person sync pipeline. Resource
names below end in the landscape (`<landscape>`, lower case, e.g. `staging`) from the active context's
`TAGS.Landscape`.

## Quick Start

### 1. Deploy Infrastructure

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Deploy every stack: the shared network (unchanged if it exists), the pipeline and, if the context has a DASHBOARD
# section, the landscape's dashboard
npm run deploy

# Or one stack (deploy the pipeline before the dashboard - the dashboard reads its resources)
npm run deploy-fargate
npm run deploy-dashboard
```

A bare `cdk deploy` fails because the app has more than one stack - see
[CDK_DEPLOYMENT.md](./CDK_DEPLOYMENT.md#deployment-steps).

### 2. Build and Push Docker Image

```bash
# Build and push to the active landscape's ECR repository
npm run docker-publish
```

### 3. Test

```bash
# Trigger a run from the source API: set the "RUNNER_" prefixed variables in .env first (see src/runner/README.md)
npx ts-node src/runner/Runner.ts

# Or drop a JSON file into the file-drop subdirectory of the input bucket (context S3.inputBucket) that is wired to
# this landscape's chunker-subscriber-<landscape> Lambda
```

## Infrastructure Components

### Created Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `huron-integration-network` | CloudFormation stack | Shared VPC, subnets and NAT gateway for every landscape ([SHARED_NETWORK.md](SHARED_NETWORK.md)) |
| `huron-person-integration-<landscape>` | ECR Repository | Stores the task image |
| `huron-person-cluster-<landscape>` | ECS Cluster | Runs the three Fargate services |
| `Chunker`, `Processor`, `PostProcessor` | Fargate Services | Queue-driven, auto-scaling from 0 to `ECS.<phase>Service.maxScalingCapacity` |
| `Chunker-<landscape>`, `Processor-<landscape>`, `PostProcessor-<landscape>` | ECS Task Defs | The phases' tasks (ARM64) |
| `<landscape>-huron-person-{chunker,processor,post-processor}-queue` (+ `-dlq`) | SQS Queues | One queue and dead-letter queue per phase |
| `huron-person-chunks-<landscape>` | S3 Bucket | Chunk files, delta chunks and (S3 mode) the baseline; `chunks/` expire after `S3.chunkExpirationDays`, `deltas/` after `S3.deltaExpirationDays` |
| `chunker-subscriber-<landscape>` | Lambda | Starts runs: from the schedule, the dashboard or a dropped file |
| `processor-subscriber-<landscape>` | Lambda | Forwards chunk files' S3 events to the processor queue |
| `post-processor-subscriber-<landscape>` | Lambda | S3 mode: queues the post-processor once every chunk is done |
| `chunker-api-schedule-<landscape>` | EventBridge Scheduler schedule | Invokes the chunker subscriber on the landscape's bulk sync schedule. Not deployed: managed at run time through `ScheduledRun` (dashboard or harness), deleted by `bin/teardown.sh` |
| `chunker-schedule-<landscape>` | IAM role | The schedule's role: invokes the chunker subscriber, nothing else (`ChunkerService`) |
| Statistics, atomic counter, addon log, PersonCurrentState, PersonHistory tables | DynamoDB | Run metadata and state; the person tables in DynamoDB storage mode (`lib/DynamoDB.ts`) |
| The integration config secret | Secrets Manager | The integration config, with API credentials (`lib/Secrets.ts`) |
| `source-simulator-<landscape>`, `target-simulator-<landscape>` | Lambdas | Mock landscapes only ([MOCK_LANDSCAPES.md](MOCK_LANDSCAPES.md)) |

### IAM Roles

- **Task roles**: read/write the chunks bucket, read the config secret, read/write their DynamoDB tables
- **Chunker subscriber role**: send to the chunker queue; update/describe the chunker and processor services (parallel
  start, the processor maximum); in a mock landscape, the reset's table and bucket grants
- **Processor / post-processor subscriber roles**: send to their phase's queue

### Event Flow

```
Schedule / dashboard Bulk sync / Runner / dropped file
        ↓
chunker-subscriber Lambda → chunker queue → Chunker service (scales on queue depth)
        ↓
Chunk files → S3 → processor-subscriber Lambda → processor queue → Processor service (scales on queue depth)
        ↓                                                                ↓
        ↓                                                         Huron API sync
        ↓
S3 mode: completion markers → post-processor-subscriber Lambda ─┐
DynamoDB mode: the last processor ───────────────────────────────┴→ post-processor queue → PostProcessor service
                                                                       (consolidation, deletions, run summary)
```

## Configuration

### context.json

Key settings in [context/context.json](../context/context.json) (the active landscape's context; each landscape has its
own `context.<landscape>.json`, switched with `context/swap.sh`):

```json
{
  "ITEMS_PER_CHUNK": 200,
  "S3": { "chunksBucket": "huron-person-chunks", "chunkExpirationDays": 14, "deltaExpirationDays": 30 },
  "ECS": {
    "chunkerTaskDefinition": { "cpu": 2048, "memoryLimitMiB": 4096, "logRetentionDays": 90 },
    "chunkerService": { "maxScalingCapacity": 15, "chunksPerTask": 0 },
    "processorService": { "maxScalingCapacity": 15 }
  },
  "SQS": { "visibilityTimeoutSeconds": 900 }
}
```

**Tuning Guide:**
- `ITEMS_PER_CHUNK`: higher = fewer chunks, higher memory per processor task
- `chunkerTaskDefinition.memoryLimitMiB`: increase if the chunker OOMs on large files
- `processorService.maxScalingCapacity`: max parallel processor tasks; above a point Huron throttles them. A run can set
  its own (Bulk sync's Processor tasks, the Runner's `PROCESSOR_SCALING_CAPACITY_OVERRIDE`)
- `visibilityTimeoutSeconds`: must exceed max processor task runtime

## CDK Constructs

| File | Construct | Description |
|------|-----------|-------------|
| `lib/Stack.ts` | `IntegrationHuronPersonLambdaStack` | The pipeline stack and its outputs |
| `lib/AppConstruct.ts` | `AppConstruct` | Everything in the pipeline stack, wired together |
| `lib/NetworkStack.ts` | `NetworkStack` | The shared network stack and its SSM parameters |
| `lib/EcrRepository.ts` | `EcrRepository` | ECR repo with lifecycle rules |
| `lib/EcsInfrastructure.ts` | `EcsInfrastructure` | Cluster, task definitions and the three services (VPC from `NetworkParameters`) |
| `lib/TaskDefinitions.ts`, `lib/services/*/…TaskDefinition.ts` | | The phases' task definitions |
| `lib/services/*/…Service.ts` | | The phases' Fargate services (and the chunker's schedule) |
| `lib/QueueInfrastructure.ts` | `QueueInfrastructure` | The queues and DLQs |
| `lib/SubscribingLambdas.ts`, `lib/services/*/…SubscribingLambda.ts` | | The subscriber Lambdas |
| `lib/DynamoDB.ts` | `DynamoDbTables` | The DynamoDB tables |
| `lib/Secrets.ts` | `HuronPersonSecrets` | The integration config secret |
| `lib/Simulator.ts` | `Simulator` | A mock landscape's simulators |

## Deployment Scripts

| Script | Purpose |
|--------|---------|
| [docker/publish.sh](../docker/publish.sh) | Build ARM64 image, push to ECR |
| [bin/deploy.sh](../bin/deploy.sh) | Deploy CDK stacks |
| [bin/teardown.sh](../bin/teardown.sh) | Destroy CDK stacks (never the shared network) |

## Monitoring

### CloudWatch Logs

```bash
# Chunker, processor and post-processor logs
aws logs tail /ecs/huron-person-chunker-<landscape> --follow
aws logs tail /ecs/huron-person-processor-<landscape> --follow
aws logs tail /ecs/huron-person-post-processor-<landscape> --follow

# Chunker subscriber Lambda logs
aws logs tail /aws/lambda/chunker-subscriber-<landscape> --follow
```

The dashboard's Run history shows each run's progress, errors and summary.

### SQS Metrics

```bash
QUEUE_URL=$(aws sqs get-queue-url --queue-name <landscape>-huron-person-processor-queue --query QueueUrl --output text)

# Check queue depth
aws sqs get-queue-attributes --queue-url $QUEUE_URL --attribute-names ApproximateNumberOfMessages
```

### ECS Tasks

```bash
# List running tasks
aws ecs list-tasks --cluster huron-person-cluster-<landscape>

# Get task details
aws ecs describe-tasks --cluster huron-person-cluster-<landscape> --tasks <task-arn>
```

## Cost Optimization

- **ARM64/Graviton2**: ~20% savings vs x86_64
- **Auto-scaling**: every service scales 0→N (pay only when processing)
- **One NAT Gateway**, shared: every landscape runs in the private subnets of one network stack (`lib/NetworkStack.ts`), whose single NAT gateway (~$32/month) serves them all
- **S3 Lifecycle**: chunks expire after `S3.chunkExpirationDays` (14), deltas after `S3.deltaExpirationDays` (30)
- **Log Retention**: `ECS.*TaskDefinition.logRetentionDays` (90 in every context)

**Estimated Costs** (80K person file, 10 parallel processors):
- Chunker task: 1 task × 5 min × $0.04048/vCPU-hr × 2 vCPU = $0.007
- Processor tasks: 10 tasks × 5 min × $0.04048/vCPU-hr × 1 vCPU = $0.034
- Total per file: ~$0.04

## Troubleshooting

See [CDK_DEPLOYMENT.md](../docs/CDK_DEPLOYMENT.md) for detailed troubleshooting guide.

**Common Issues:**

1. **Build errors (TS5055)**: Run `rm -rf dist && npm run build`
2. **Network parameters missing**: the pipeline reads its VPC and subnets from the SSM parameters under `/huron-integration-network/`; deploy the network stack (`npm run deploy-fargate` does it first)
3. **Task not starting**: Check image exists in ECR (`aws ecr list-images --repository-name huron-person-integration-<landscape>`)
4. **DLQ messages**: Check processor logs for errors

## Documentation

- [CDK Deployment Guide](./CDK_DEPLOYMENT.md) - Full deployment instructions
- [Docker README](../docker/README.md) - Docker build/run guide
- [Shared network](./SHARED_NETWORK.md), [storage modes](./STORAGE_MODES.md), [starting a run](./RUN_START.md)

## Stack Outputs

After deployment, the pipeline stack outputs (each id suffixed with `-<landscape>`): `EcrRepositoryUri`,
`EcsClusterName`, `ChunkerSubsriberLambdaArn` and `ChunkerSubsriberLambdaRoleArn` (sic), `ChunkerTaskDefinitionArn`,
`ProcessorTaskDefinitionArn`, `ProcessorQueueUrl`, `ChunksBucketName`, `StatisticsTableName`; and in a mock landscape
with the source simulator, `SourceSimulatorUrl` and `SourceSimulatorUsage`. List them with:

```bash
aws cloudformation describe-stacks --stack-name huron-person-fargate-processor-<Landscape> --query "Stacks[0].Outputs"
```
