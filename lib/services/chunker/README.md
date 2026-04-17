# Chunker Service

## What is Chunking?

**Chunking** is the first phase (Phase 1) of a three-phase data processing pipeline that handles large-scale person record synchronization from Boston University's CDM (Common Data Model) system to a target system.

The chunking process breaks down large datasets into smaller, manageable pieces called "chunks":

- **Input**: Large JSON file containing thousands of person records (from S3) or bulk API response from CDM
- **Output**: Multiple smaller NDJSON (newline-delimited JSON) files, typically 200 records each
- **Purpose**: Enable parallel processing in Phase 2 by distributing work across multiple Fargate tasks

### Why Chunking?

1. **Parallel Processing**: Multiple Fargate tasks can process chunks simultaneously
2. **Memory Efficiency**: Smaller chunks prevent memory exhaustion when processing large datasets
3. **Fault Tolerance**: If one chunk fails, others can still succeed
4. **Progress Tracking**: Each chunk can be tracked independently through the pipeline

---

## Architecture Overview

The Chunker Service uses AWS QueueProcessingFargateService with two triggers:

### 1. **S3 File Upload Trigger** (Manual/External)
- A large JSON file is uploaded to the input S3 bucket
- External Lambda detects S3 event and invokes ChunkerSubscribingLambda
- Lambda sends message to SQS queue with S3 location
- Fargate task streams the file and creates chunks

### 2. **EventBridge Schedule Trigger** (Automated/Periodic)
- EventBridge schedule runs on configured cron expression
- Directly invokes ChunkerSubscribingLambda with API parameters
- Lambda sends message to SQS queue with API endpoint details
- Fargate task fetches data from CDM API and creates chunks

---

## EventBridge Schedule Configuration

The ChunkerService automatically creates an EventBridge schedule for periodic API-based data fetching when properly configured in IContext.

### Configuration Requirements

In `context/IContext.ts`, configure the `HURON_PERSON_CONFIG` as follows:

```typescript
HURON_PERSON_CONFIG: {
  dataSource: {
    people: {
      endpointConfig: {
        baseUrl: 'https://prod-budev-fm.snaplogic.io',  // CDM API base URL
        apiKey: 'your-api-key-here'          // API authentication key
      },
      fetchPath: '/api/1/rest/feed/run/task/BUDev/Admin-Integration-Services/GenericGets/huronIRBPersonByPopulation',                // API endpoint path
      fetchSchedule: {
        enabled: true,                        // Enable the schedule
        cronExpression: 'cron(0 2 * * ? *)'  // Run daily at 2 AM UTC
      }
    }
  }
}
```

### Cron Expression Examples

AWS EventBridge uses a 6-field cron format: `cron(Minutes Hours Day-of-month Month Day-of-week Year)`

| Schedule | Cron Expression | Description |
|----------|----------------|-------------|
| Daily at 2 AM | `cron(0 2 * * ? *)` | Every day at 2:00 AM UTC |
| Every 6 hours | `cron(0 */6 * * ? *)` | At minutes 0, 6, 12, 18 |
| Weekly on Monday | `cron(0 3 ? * MON *)` | Every Monday at 3:00 AM UTC |
| Monthly on 1st | `cron(0 4 1 * ? *)` | 1st of month at 4:00 AM UTC |
| Hourly | `cron(0 * * * ? *)` | Every hour at minute 0 |

**Note**: EventBridge uses UTC timezone. Convert local times to UTC when scheduling.

### How the Schedule Works

1. **Deployment Time**:
   - ChunkerService constructor checks `HURON_PERSON_CONFIG.dataSource.people.fetchSchedule`
   - If `enabled: true` and valid `cronExpression` exists, creates EventBridge Rule
   - Rule is configured with Lambda target and API event payload

2. **Runtime**:
   - EventBridge invokes ChunkerSubscribingLambda on schedule
   - Lambda receives event:
     ```json
     {
       "baseUrl": "https://prod-budev-fm.snaplogic.io",
       "fetchPath": "/api/1/rest/feed/run/task/BUDev/Admin-Integration-Services/GenericGets/huronIRBPersonByPopulation",
       "populationType": "person-delta",
       "bulkReset": false,
       "processingMetadata": {
         "processedAt": "2026-04-13T14:30:00.000Z",
         "processorVersion": "1.0.0"
       }
     }
     ```
   - Lambda delegates to ChunkerApiSubscriber
   - ChunkerApiSubscriber sends SQS message to chunker queue
   - QueueProcessingFargateService auto-scales and launches task
   - Fargate task fetches data from API and creates chunks

### Population Types

- **`person-full`**: Fetch all person records (full sync)
  - Used for initial data load
  - Used when as an override of the default (e.g. via manual invocation)
  - Resyncs complete person dataset

- **`person-delta`**: Fetch only changed records (incremental sync)
  - Used for ongoing updates
  - Used as the default (e.g. for scheduled syncs)
  - More efficient for daily syncs

---

## Manual Lambda Invocation (Off-Schedule)

### Prerequisites

1. AWS CLI installed and configured
2. Appropriate IAM permissions to invoke Lambda functions
3. Lambda function name (typically: `chunker-subscriber`)

### CLI Invocation Examples

#### Example 1: Full Person Sync

```bash
aws lambda invoke \
  --function-name chunker-subscriber \
  --region us-east-2 \
  --payload '{
    "baseUrl": "https://prod-budev-fm.snaplogic.io",
    "fetchPath": "/api/1/rest/feed/run/task/BUDev/Admin-Integration-Services/GenericGets/huronIRBPersonByPopulation",
    "populationType": "person-full",
    "bulkReset": true,
    "processingMetadata": {
      "processedAt": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "processorVersion": "1.0.0"
    }
  }' \
  response.json
```
#### Example 2: Full Person Sync (using configured defaults)

```bash
aws lambda invoke \
  --function-name chunker-subscriber \
  --region us-east-2 \
  --payload '{
    "baseUrl": "from_config",
    "fetchPath": "from_config",
    "populationType": "person-full",
    "bulkReset": true,
    "processingMetadata": {
      "processedAt": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "processorVersion": "1.0.0"
    }
  }' \
  response.json

# or...

npm run start-full-reset your_aws_profile
```

#### Example 3: Delta (Incremental) Sync

```bash
aws lambda invoke \
  --function-name chunker-subscriber \
  --region us-east-2 \
  --payload '{
    "baseUrl": "https://prod-budev-fm.snaplogic.io",
    "fetchPath": "/api/1/rest/feed/run/task/BUDev/Admin-Integration-Services/GenericGets/huronIRBPersonByPopulation",
    "populationType": "person-delta",
    "bulkReset": false,
    "processingMetadata": {
      "processedAt": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "processorVersion": "1.0.0"
    }
  }' \
  response.json
```

#### Example 4: Delta (Incremental) Sync (using configured defaults)

```bash
aws lambda invoke \
  --function-name chunker-subscriber \
  --region us-east-2 \
  --payload '{
    "baseUrl": "from_config",
    "fetchPath": "from_config",
    "populationType": "person-delta",
    "bulkReset": false,
    "processingMetadata": {
      "processedAt": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "processorVersion": "1.0.0"
    }
  }' \
  response.json

# or...

npm run start-delta-sync your_aws_profile
```

### Checking Invocation Results

After invoking the Lambda, check the response:

```bash
# View Lambda response
cat response.json

# Expected success response:
# {
#   "statusCode": 200,
#   "body": "Message sent to chunker queue"
# }
```

### Monitoring Progress

1. **CloudWatch Logs**: Check Lambda logs
   ```bash
   aws logs tail /aws/lambda/chunker-subscriber --follow
   ```

2. **SQS Queue Depth**: Monitor chunker queue
   ```bash
   aws sqs get-queue-attributes \
     --queue-url <CHUNKER_QUEUE_URL> \
     --attribute-names ApproximateNumberOfMessages
   ```

3. **ECS Tasks**: Watch Fargate tasks start
   ```bash
   aws ecs list-tasks --cluster <CLUSTER_NAME> --service-name Chunker
   ```

4. **S3 Chunks**: Check for output chunks
   ```bash
   aws s3 ls s3://<CHUNKS_BUCKET>/chunks/person-full/ --recursive
   ```

---

## Troubleshooting

### Schedule Not Created

**Symptoms**: EventBridge schedule doesn't exist after deployment

**Possible Causes**:
1. `fetchSchedule.enabled` is `false`
2. `fetchSchedule.cronExpression` is missing or invalid
3. `baseUrl` or `fetchPath` not configured
4. ChunkerService didn't receive Lambda function reference

**Solution**: Check CloudFormation stack events during deployment for ChunkerService logs

### Manual Invocation Fails

**Error**: `AccessDeniedException`
```
User is not authorized to perform: lambda:InvokeFunction
```

**Solution**: Add IAM permission:
```json
{
  "Effect": "Allow",
  "Action": "lambda:InvokeFunction",
  "Resource": "arn:aws:lambda:us-east-2:*:function:chunker-subscriber"
}
```

### No Chunks Created

**Symptoms**: Lambda succeeds but no chunks appear in S3

**Debugging Steps**:
1. Check CloudWatch logs for Fargate task
2. Verify SQS message was sent (check queue metrics)
3. Confirm Fargate task scaled up (check ECS service)
4. Check task exit code and logs
5. Verify API endpoint is reachable from Fargate

---

## Related Files

- **ChunkerService.ts**: Creates QueueProcessingFargateService and EventBridge schedule
- **ChunkerSubscribingLambda.ts**: Lambda construct definition
- **src/chunking/ChunkerSubscriber.ts**: Lambda handler (dispatcher)
- **src/chunking/fetch/ChunkerApiSubscriber.ts**: API event handler
- **src/chunking/filedrop/ChunkerS3Subscriber.ts**: S3 event handler
- **src/chunker.ts**: Fargate task entry point
- **src/chunking/fetch/ChunkFromAPI.ts**: API-based chunking implementation
- **src/chunking/filedrop/ChunkFromS3.ts**: S3-based chunking implementation

---

## See Also

- [IContext Configuration](../../../context/IContext.ts)
- [Processor Service (Phase 2)](../processor/)
- [Merger Service (Phase 3)](../merger/)
- [EventBridge Cron Expressions](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-create-rule-schedule.html)
