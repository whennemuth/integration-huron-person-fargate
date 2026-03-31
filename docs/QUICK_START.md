# Quick Start: Chunked Parallel Processing

## Overview

Process large datasets in parallel using a 3-phase pipeline that prevents race conditions.

## Architecture in 60 Seconds

```
Phase 1: Chunker      →  Phase 2: Processors (parallel)  →  Phase 3: Merger
Split input file     →  Process each chunk              →  Combine results
chunks/chunk-*.ndjson →  Update chunk in place           →  previous-input.ndjson
```

## Prerequisites

- Docker images built:
  - `integration-chunker`
  - `integration-processor`
  - `integration-merger`
- S3 bucket with input data
- SQS queue created
- Huron API credentials

## Step 1: Run Chunker

**Split input into chunks:**

```bash
docker run \
  --env-file .env \
  -e INPUT_SOURCE_KEY=example-client/input-data.json \
  -e CHUNK_SIZE=1000 \
  -e QUEUE_URL=https://sqs.us-east-2.amazonaws.com/.../integration-queue \
  integration-chunker
```

**Output:**
- Creates `chunks/chunk-0000.ndjson`, `chunk-0001.ndjson`, etc.
- Publishes SQS message for each chunk

**Verify:**
```bash
aws s3 ls s3://integration-bucket/example-client/chunks/
# Should see: chunk-0000.ndjson, chunk-0001.ndjson, ...
```

## Step 2: Run Processors (Auto-scaled)

**Fargate service consumes SQS messages:**

Each message triggers a Fargate task that:
1. Reads chunk file from S3
2. Processes records (calls Huron API)
3. Writes updated chunk back to S3

**No manual intervention needed** - Fargate auto-scales based on queue depth.

**Monitor progress:**
```bash
# Check queue depth (should decrease to 0)
aws sqs get-queue-attributes \
  --queue-url https://sqs.us-east-2.amazonaws.com/.../integration-queue \
  --attribute-names ApproximateNumberOfMessages

# Check running tasks
aws ecs list-tasks --cluster integration-cluster --service-name processor
```

**Wait for:**
- Queue depth = 0
- All tasks = STOPPED status

## Step 3: Run Merger

**Combine chunks into final output:**

```bash
docker run \
  --env-file .env \
  -e CLIENT_ID=example-client \
  integration-merger
```

**Output:**
- Creates `example-client/previous-input.ndjson`
- Deletes all `chunks/chunk-*.ndjson` files

**Verify:**
```bash
# Check final output exists
aws s3 ls s3://integration-bucket/example-client/previous-input.ndjson

# Verify chunks deleted
aws s3 ls s3://integration-bucket/example-client/chunks/
# Should be empty
```

## Environment Variables

### All Phases

```bash
AWS_REGION=us-east-2
INPUT_BUCKET=integration-bucket
CHUNKS_BUCKET=integration-bucket
CLIENT_ID=example-client
```

### Chunker Only

```bash
INPUT_SOURCE_KEY=example-client/input-data.json
CHUNK_SIZE=1000
QUEUE_URL=https://sqs.us-east-2.amazonaws.com/123456789/integration-queue
```

### Processor Only

```bash
# Auto-provided by SQS message:
# CHUNK_KEY=example-client/chunks/chunk-0042.ndjson

# Huron API credentials:
HURON_API_ENDPOINT=https://api.huron.com/v1
JWT_PRIVATE_KEY_BASE64=...
JWT_ALGORITHM=RS256
JWT_KEY_ID=...
JWT_ISSUER=...
JWT_SUBJECT=...
JWT_AUDIENCE=...
```

### Merger Only

```bash
# CLIENT_ID is sufficient (reuses from "All Phases")
```

## Troubleshooting

### Chunker fails

**Symptom**: No chunks created in S3
**Check**:
```bash
docker logs <container-id>
```
**Common causes**:
- Invalid INPUT_SOURCE_KEY
- S3 permissions
- Invalid JSON structure

**Fix**: Verify input file exists and is valid JSON

### Processor fails

**Symptom**: SQS queue not emptying, tasks in FAILED state
**Check**:
```bash
aws ecs describe-tasks --cluster ... --tasks <task-arn>
# Check stopReason and exitCode

# Check CloudWatch logs
aws logs tail /ecs/processor --follow
```
**Common causes**:
- Huron API credentials invalid
- Network timeout
- Missing environment variables

**Fix**: 
- Verify credentials
- Check Huron API status
- Review environment configuration

### Merger fails

**Symptom**: No previous-input.ndjson created, chunks remain
**Check**:
```bash
docker logs <container-id>
```
**Common causes**:
- Chunks still being processed (wait for queue depth = 0)
- S3 permissions
- Network timeout

**Fix**: Ensure all processors complete before running merger

## Monitoring Commands

```bash
# Count chunks created
aws s3 ls s3://$CHUNKS_BUCKET/$CLIENT_ID/chunks/ | wc -l

# Check SQS queue depth
aws sqs get-queue-attributes \
  --queue-url $QUEUE_URL \
  --attribute-names ApproximateNumberOfMessages

# List running processor tasks
aws ecs list-tasks \
  --cluster integration-cluster \
  --service-name processor \
  --desired-status RUNNING

# Check final output size
aws s3 ls s3://$CHUNKS_BUCKET/$CLIENT_ID/previous-input.ndjson --human-readable
```

## Performance Tuning

### Chunk Size

| Records per Chunk | Use Case | Parallelism | API Calls |
|-------------------|----------|-------------|-----------|
| 500 | Small datasets, high parallelism | High | Many |
| 1000 | **Recommended default** | Medium | Moderate |
| 5000 | Large datasets, fewer API calls | Low | Few |

**Formula**: 
```
Total chunks = Total records / Chunk size
Parallel tasks = Total chunks (up to Fargate limit)
```

### Fargate Scaling

**Service configuration**:
```json
{
  "desiredCount": 0,
  "minimumTaskCount": 0,
  "maximumTaskCount": 100,
  "targetTrackingScaling": {
    "targetValue": 100.0,
    "scaleInCooldown": 60,
    "scaleOutCooldown": 60,
    "metric": "SQSQueueDepth"
  }
}
```

**Recommendations**:
- Start with `maximumTaskCount` = 10-20 for testing
- Increase to 50-100 for production
- Monitor Huron API rate limits

## Cost Estimation

### Example: 100,000 records

**Assumptions**:
- Chunk size: 1000 records
- Total chunks: 100
- Processing time: 30 seconds per chunk
- Fargate: 0.5 vCPU, 1 GB memory

**Costs**:
- Fargate: 100 tasks × 30s × $0.04048/hour/vCPU ≈ $0.03
- S3: 100 chunk files × $0.005/1000 PUT ≈ $0.0005
- SQS: 100 messages × $0.40/million ≈ $0.00004
- **Total: ~$0.03** (excluding API calls)

**Scaling**: Linear with number of records (roughly $0.30 per million records)

## Testing Locally

### Small dataset test

```bash
# 1. Create test input (10 records)
echo '{"people": [{"id": "1"}, {"id": "2"}, ..., {"id": "10"}]}' > test-input.json

# 2. Upload to S3
aws s3 cp test-input.json s3://$INPUT_BUCKET/$CLIENT_ID/test-input.json

# 3. Run chunker (chunk size = 2 for testing)
docker run --env-file .env \
  -e INPUT_SOURCE_KEY=$CLIENT_ID/test-input.json \
  -e CHUNK_SIZE=2 \
  integration-chunker

# 4. Verify 5 chunks created
aws s3 ls s3://$CHUNKS_BUCKET/$CLIENT_ID/chunks/

# 5. Process one chunk manually
docker run --env-file .env \
  -e CHUNK_KEY=$CLIENT_ID/chunks/chunk-0000.ndjson \
  integration-processor

# 6. Process remaining chunks (or let Fargate handle)

# 7. Run merger
docker run --env-file .env integration-merger

# 8. Verify output
aws s3 cp s3://$CHUNKS_BUCKET/$CLIENT_ID/previous-input.ndjson - | wc -l
# Should see 10 lines
```

## Next Steps

1. Review [CHUNKING_ARCHITECTURE.md](CHUNKING_ARCHITECTURE.md) for detailed design
2. Set up CloudWatch dashboards for monitoring
3. Configure SQS Dead Letter Queue for failed messages
4. Implement S3 lifecycle policy for orphaned chunks (delete after 7 days)
5. Load test with production-size datasets
