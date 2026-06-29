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
      fetchPath: '/api/1/rest/feed/run/task/BUTest/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation',                // API endpoint path
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
       "fetchPath": "/api/1/rest/feed/run/task/BUTest/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation",
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
    "fetchPath": "/api/1/rest/feed/run/task/BUTest/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation",
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
    "fetchPath": "/api/1/rest/feed/run/task/BUTest/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation",
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

## Source Simulator (Mock API for Testing)

The **Source Simulator** is an optional Lambda Function URL that simulates the source person API for testing purposes. It eliminates the 30-minute cooldown constraint of the real API, enabling rapid testing and development of the parallel chunking flow.

### Why Use the Source Simulator?

**Problem**: The real source API maintains private state and requires a 30-minute cooldown between calls. It ignores the `offset` parameter and returns chunks in call order, making rapid testing impractical.

**Solution**: A stateless mock API that:
- ✅ Responds immediately to any `offset` value (no cooldown)
- ✅ Supports parallel requests (tests actual production flow)
- ✅ Generates deterministic person IDs using formula-based approach
- ✅ Returns minimal field set (only fields DataMapper uses)
- ✅ Validates API key (secure even when public)
- ✅ Matches real API response structure

### Configuration

Enable in `context/IContext.ts`:

```typescript
LAMBDA: {
  // ... other Lambda configs ...
  sourceSimulator: {
    enabled: true,                    // Enable source simulator
    timeoutSeconds: 30,                // Lambda timeout
    memorySizeMb: 512,                 // Lambda memory
    mockTotalPopulation: 10000,        // Total mock persons
    mockErrorRate: 0.0,                // Simulated error rate (0.0-1.0)
    apiKey: 'your-api-key-here'        // Same key as real API
  }
}
```

After deploying, the CDK will output the Function URL.

### Usage in Runner.ts

Set environment variables to point to the mock API:

```bash
# In your .env file for Runner.ts harness
RUNNER_MOCK_API_BASE_URL=https://abc123xyz.lambda-url.us-east-2.on.aws
RUNNER_MOCK_API_FETCH_PATH=/
```

Then in Runner.ts code:

```typescript
const mockApiBaseUrl = testEnvironment.getVar('MOCK_API_BASE_URL');
const mockApiFetchPath = testEnvironment.getVar('MOCK_API_FETCH_PATH');

if (mockApiBaseUrl) {
  messageBody.baseUrl = mockApiBaseUrl;
  messageBody.fetchPath = mockApiFetchPath || '/';
}
```

The mock API will automatically be used instead of the real API.

### Query Parameters

The mock API supports the same query parameters as the real API:

- **`recordCount`**: Number of records per batch (default: 200)
- **`offset`**: Batch number for pagination (default: 0)

Example:
```
https://your-function-url.lambda-url.us-east-2.on.aws/?recordCount=500&offset=2
```
Returns persons 1000-1499 (offset 2 × 500 records)

### Mock Data Structure

Generated persons include minimal fields for DataMapper:

```json
{
  "personid": "U0000001",
  "bu_id": "00000001",
  "firstName": "FirstName1",
  "lastName": "LastName1",
  "email": "U0000001@bu.edu",
  "employeeInfo": { ... },      // 33% of population
  "studentInfo": { ... },        // 33% of population
  "affiliateInfo": { ... }       // 33% of population
}
```

### Health Check

Check simulator status:

```bash
curl -H "X-API-Key: your-api-key-here" \
  https://your-function-url.lambda-url.us-east-2.on.aws/health
```

Response:
```json
{
  "status": "healthy",
  "totalPopulation": 10000,
  "errorRate": 0.0
}
```

### Test Harness

The Source Simulator includes a test harness for local development and remote testing. Run it directly with `npx ts-node`:

**Local Task** (calls handler directly with mocked event):
```bash
# Configure environment variables in .env (see example-env.md)
SOURCE_SIMULATOR_TASK=local
SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION=1000
SOURCE_SIMULATOR_SECRET_ARN=<your-secret-arn>
SOURCE_SIMULATOR_RECORD_COUNT=10
SOURCE_SIMULATOR_OFFSET=0
SOURCE_SIMULATOR_API_KEY=test-api-key

# Run the harness
npx ts-node src/chunking/fetch/SourceSimulator.ts
```

**Remote Task** (calls deployed Lambda Function URL):
```bash
# Configure environment variables in .env
SOURCE_SIMULATOR_TASK=remote
SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION=1000
SOURCE_SIMULATOR_SECRET_ARN=<your-secret-arn>
SOURCE_SIMULATOR_FUNCTION_URL=https://your-function-url.lambda-url.us-east-2.on.aws/
SOURCE_SIMULATOR_API_KEY=<your-api-key>
SOURCE_SIMULATOR_RECORD_COUNT=10
SOURCE_SIMULATOR_OFFSET=0

# Run the harness
npx ts-node src/chunking/fetch/SourceSimulator.ts
```

Output shows:
- Query parameters used
- HTTP status code and headers
- Parsed response body
- Number of persons returned
- Sample person record

### Architecture

```
Runner.ts (with MOCK_API_BASE_URL)
  ↓ sends message with mock baseUrl/fetchPath
ChunkerSubscriber Lambda
  ↓ puts message on queue
ChunkFromAPI (reads message)
  ↓ ChunkConfigOverride mutates config
BigJsonFetch
  ↓ BuCdmPeopleDataSource
    ↓ BuCdmDataSource.fetchRaw()
      ↓ ApiClientForApiKey.get()
        ↓ **Source Simulator Lambda** (stateless, offset-based)
```

### Security

- Function URL is **public** but requires valid API key in headers
- Supports two authentication methods:
  - `Authorization: Bearer <api-key>`
  - `X-API-Key: <api-key>`
- API key must match `MOCK_API_KEY` environment variable
- Generates fake data only (no real PII)

---

## Related Files

- **ChunkerService.ts**: Creates QueueProcessingFargateService and EventBridge schedule
- **ChunkerSubscribingLambda.ts**: Lambda construct definition
- **SourceSimulator.ts** (lib): Source Simulator Lambda construct
- **src/chunking/ChunkerSubscriber.ts**: Lambda handler (dispatcher)
- **src/chunking/fetch/ChunkerApiSubscriber.ts**: API event handler
- **src/chunking/filedrop/ChunkerS3Subscriber.ts**: S3 event handler
- **src/chunker.ts**: Fargate task entry point
- **src/chunking/fetch/ChunkFromAPI.ts**: API-based chunking implementation
- **src/chunking/filedrop/ChunkFromS3.ts**: S3-based chunking implementation
- **src/chunking/fetch/SourceSimulator.ts**: Mock API Lambda handler

---

## See Also

- [IContext Configuration](../../../context/IContext.ts)
- [Processor Service (Phase 2)](../processor/)
- [Merger Service (Phase 3)](../merger/)
- [EventBridge Cron Expressions](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-create-rule-schedule.html)
