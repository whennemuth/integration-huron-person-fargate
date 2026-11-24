# Chunker Service

## What is Chunking?

**Chunking** is the first phase (Phase 1) of a three-phase data processing pipeline that handles large-scale person record synchronization from Boston University's CDM (Common Data Model) system to a target system.

The chunking process breaks down large datasets into smaller, manageable pieces called "chunks":

- **Input**: the CDM person API, paged across chained chunker tasks, or a large JSON file dropped in S3
- **Output**: Multiple smaller NDJSON (newline-delimited JSON) files, `ITEMS_PER_CHUNK` records each (typically 200), under `chunks/<population>/<syncRunId>/`
- **Purpose**: Enable parallel processing in Phase 2 by distributing work across multiple Fargate tasks

### Why Chunking?

1. **Parallel Processing**: Multiple Fargate tasks can process chunks simultaneously
2. **Memory Efficiency**: Smaller chunks prevent memory exhaustion when processing large datasets
3. **Fault Tolerance**: If one chunk fails, others can still succeed
4. **Progress Tracking**: Each chunk can be tracked independently through the pipeline

---

## Architecture Overview

The Chunker Service is a QueueProcessingFargateService that scales on its queue. Messages reach the queue through the
chunker subscriber Lambda (`chunker-subscriber-<landscape>`, handler `src/chunking/ChunkerSubscriber.ts`), which takes:

1. **API events** (`ChunkerApiSubscriber`): from the EventBridge Scheduler schedule, or from the dashboard's Bulk sync
   (which may add `parallelStart`, `resetState`, `processorScalingCapacityOverride` and `startedBy`)
2. **S3 file-drop events** (`ChunkerS3Subscriber`): a large JSON file uploaded to the input bucket; the file's location
   goes on the queue message and the task streams the file into chunks
3. **Mock-landscape reset events** (`{ mockLandscapeReset: true }`): the dashboard's Reset landscape; nothing is queued

The manual Runner (`src/runner/Runner.ts`) doesn't invoke the Lambda: it calls `handleApiEvent()` in-process to queue
the message.

For a run's first API event, before queuing, the handler: applies its refusals (returned as a 400 result, not thrown);
sets the processor service's live maximum (the run's override, else `ECS.processorService.maxScalingCapacity`); resets
a mock landscape's state if `resetState`; resets the run counters; and, with `parallelStart` N, seeds N messages and
sets the chunker service's desired count. Details: [docs/RUN_START.md](../../../docs/RUN_START.md).

---

## The Bulk Sync Schedule

A landscape has at most one schedule, EventBridge Scheduler's `chunker-api-schedule-<landscape>`, which invokes the
chunker subscriber with a scheduled run's event. It is **not deployed**: it is created, turned on or off, changed and
deleted at run time through `ScheduledRun` (`src/schedule/ScheduledRun.ts`, from the dashboard or the
`SCHEDULED_RUN` harness), so a deploy can't put back a schedule someone changed. CDK only creates the role the
schedule invokes the subscriber with, `chunker-schedule-<landscape>` (`ChunkerService`), trusted by EventBridge
Scheduler for this account and allowed nothing but invoking the chunker subscriber. `bin/teardown.sh` deletes the
schedule before destroying the pipeline stack. The rules and the cron model: [src/schedule/CLAUDE.md](../../../src/schedule/CLAUDE.md).

### When it runs

AWS cron, `cron(minutes hours day-of-month month day-of-week year)`, read in `America/New_York` (day-of-week 1 is
Sunday). A bulk sync schedule runs at most once a day, and schedules of different landscapes that read the real source
start at least an hour apart, since they share the source API. The context's `SCHEDULE` is the cron a new schedule
starts from (`context/context.<landscape>.json`):

```json
"SCHEDULE": {
  "cronExpression": "cron(0 4 * * ? *)"
}
```

| Schedule | Cron Expression |
|----------|----------------|
| Every day at 4 AM | `cron(0 4 * * ? *)` |
| Weekdays at 4 AM | `cron(0 4 ? * MON-FRI *)` |
| Mondays at 3 AM | `cron(0 3 ? * MON *)` |
| The 1st of the month at 4 AM | `cron(0 4 1 * ? *)` |

Try an expression without AWS: `CRON_SCHEDULE_TASK=parse CRON_SCHEDULE_EXPRESSION="cron(0 4 ? * MON-FRI *)" npx ts-node src/schedule/CronSchedule.ts`.

### What it sends

A full sync, stored hashes trusted (`ScheduledRun.scheduleInput()` for the integration config's endpoint; a schedule
made in the dashboard carries the run settings reviewed there):

```json
{
  "baseUrl": "<the config's people baseUrl>",
  "fetchPath": "<the config's people fetchPath>",
  "populationType": "person-full",
  "iterationLimit": "<ECS.chunkerService.chunksPerTask>",
  "offset": 0,
  "previousStateSource": "storage",
  "startedBy": { "by": "schedule" },
  "processingMetadata": {
    "processedAt": "Placeholder. Override at runtime with ISO timestamp",
    "processorVersion": "1.0.0"
  }
}
```

The handler prepares the run (above) and delegates to ChunkerApiSubscriber, which sends the message to the chunker
queue; QueueProcessingFargateService auto-scales and launches tasks; each task fetches its pages from the API, writes
chunks, and chains the next message.

### Population Types

The source has no "changed records only" endpoint, so every run fetches everyone; the population type decides only
whether people missing from the source are deactivated:

- **`person-full`**: full fetch, with deactivations. The default: the schedule and the dashboard's Bulk sync use it.
- **`person-delta`**: full fetch, no deactivations.

People whose record hasn't changed are skipped by trusting the stored hashes (`previousStateSource: storage`), not by
the population type. `previousStateSource: target` is a bulk reset: every person is looked up in Huron. Every event must
say which; one that doesn't is refused.

---

## Manual Lambda Invocation (Off-Schedule)

### Prerequisites

1. AWS CLI installed and configured
2. Appropriate IAM permissions to invoke Lambda functions
3. Lambda function name: `chunker-subscriber-<landscape>`

The dashboard's Bulk sync, or the Runner, is the usual way to start an off-schedule run.

### CLI Invocation Examples

#### Example 1: Full Person Sync, bulk reset

```bash
aws lambda invoke \
  --function-name chunker-subscriber-<landscape> \
  --region us-east-2 \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "baseUrl": "https://prod-budev-fm.snaplogic.io",
    "fetchPath": "/api/1/rest/feed/run/task/BUTest/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation",
    "populationType": "person-full",
    "previousStateSource": "target",
    "processingMetadata": {
      "processedAt": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "processorVersion": "1.0.0"
    }
  }' \
  response.json
```

#### Example 2: Full Person Sync, bulk reset (using configured defaults)

```bash
aws lambda invoke \
  --function-name chunker-subscriber-<landscape> \
  --region us-east-2 \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "baseUrl": "from_config",
    "fetchPath": "from_config",
    "populationType": "person-full",
    "previousStateSource": "target",
    "processingMetadata": {
      "processedAt": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "processorVersion": "1.0.0"
    }
  }' \
  response.json
```

`npm run start-full-sync` / `start-full-reset` / `start-delta-sync` / `start-delta-reset`
(`bin/start-off-schedule-sync.sh`) invoke the Lambda the same way: the function is `chunker-subscriber-<landscape>`,
with the landscape and region read from `context/context.json` (override with `--landscape` / `--region`), and
`--bulkReset` sent as `previousStateSource` (`true`: `target`, `false`: `storage`). Add `--dry-run` (after `--`, e.g.
`npm run start-full-sync -- --dry-run`) to print the invocation without running it. AWS CLI v2 reads `--payload` as base64
unless given `--cli-binary-format raw-in-base64-out`, as in the examples above.

#### Example 3: Full sync trusting stored hashes (what the schedule runs)

```bash
aws lambda invoke \
  --function-name chunker-subscriber-<landscape> \
  --region us-east-2 \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "baseUrl": "from_config",
    "fetchPath": "from_config",
    "populationType": "person-full",
    "previousStateSource": "storage",
    "processingMetadata": {
      "processedAt": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "processorVersion": "1.0.0"
    }
  }' \
  response.json
```

#### Example 4: Full fetch, no deactivations

As Example 3 with `"populationType": "person-delta"`.

### Checking Invocation Results

After invoking the Lambda, check the response:

```bash
# View Lambda response
cat response.json
```

A refused start (e.g. an invalid `parallelStart`) answers with a 400 result rather than an error.

### Monitoring Progress

1. **CloudWatch Logs**: Check Lambda logs
   ```bash
   aws logs tail /aws/lambda/chunker-subscriber-<landscape> --follow
   ```

2. **SQS Queue Depth**: Monitor chunker queue
   ```bash
   aws sqs get-queue-attributes \
     --queue-url <CHUNKER_QUEUE_URL> \
     --attribute-names ApproximateNumberOfMessages
   ```

3. **ECS Tasks**: Watch Fargate tasks start
   ```bash
   aws ecs list-tasks --cluster huron-person-cluster-<landscape> --service-name Chunker
   ```

4. **S3 Chunks**: Check for output chunks
   ```bash
   aws s3 ls s3://<CHUNKS_BUCKET>/chunks/person-full/ --recursive
   ```

5. **The dashboard's Run history** shows the run once its first chunker task has written its metadata.

---

## Troubleshooting

### No Schedule

**Symptoms**: `chunker-api-schedule-<landscape>` doesn't exist

It is never deployed: create it from the dashboard's Bulk sync schedule page, or with the harness
(`SCHEDULED_RUN_TASK=create`, `src/schedule/ScheduledRun.ts`). Creating it needs the role
`chunker-schedule-<landscape>`, which a deploy of the pipeline stack creates. `SCHEDULED_RUN_TASK=list` shows every
landscape's schedule in the account.

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
  "Resource": "arn:aws:lambda:us-east-2:*:function:chunker-subscriber-*"
}
```

### No Chunks Created

**Symptoms**: Lambda succeeds but no chunks appear in S3

**Debugging Steps**:
1. Check the Lambda's result and logs: a refusal, or `DRY_RUN=true` on the subscriber, queues nothing
2. Check CloudWatch logs for the Fargate task
3. Verify SQS message was sent (check queue metrics)
4. Confirm Fargate task scaled up (check ECS service)
5. Check task exit code and logs
6. Verify API endpoint is reachable from Fargate

---

## Source Simulator (Mock API for Testing)

The **Source Simulator** is an optional Lambda Function URL that simulates the source person API for testing purposes. It eliminates the 30-minute cooldown constraint of the real API, enabling rapid testing and development of the parallel chunking flow. Its full description: [docs/SOURCE_SIMULATOR.md](../../../docs/SOURCE_SIMULATOR.md).

### Why Use the Source Simulator?

**Problem**: The real source API maintains private state and requires a 30-minute cooldown between calls. It ignores the `offset` parameter and returns chunks in call order, making rapid testing impractical.

**Solution**: A mock API that behaves the same way, without the cooldown:
- ✅ Stateful depletion, like the real API: each request to `/` claims the next `recordCount` people; `offset` is accepted but ignored
- ✅ Supports parallel requests (tests actual production flow)
- ✅ Deterministic payloads: a person's record is the same on every request
- ✅ Returns minimal field set (only fields DataMapper uses)
- ✅ Validates API key (secure even when public)
- ✅ Matches real API response structure

### Configuration

**Mock landscapes only**: the simulator may only be enabled for a landscape matching `^mock\d*$`
(e.g. `context/context.mock.json`), whose target is always mocked. Synthesis fails if it is enabled
in any other landscape, so simulated data can never reach the real target system.

Enable it in the mock landscape's context (`context/context.mock.json`; the type is `LAMBDA.sourceSimulator` in
`context/IContext.ts`):

```json
"LAMBDA": {
  "sourceSimulator": {
    "enabled": true,
    "timeoutSeconds": 30,
    "memorySizeMb": 512,
    "mockTotalPopulation": 10000,
    "mockErrorRate": 0.0,
    "simulatedDelaySeconds": 0
  }
}
```

After deploying, the CDK will output the Function URL (`SourceSimulatorUrl`).

### Usage in Runner.ts

Set `RUNNER_SOURCE_SIMULATOR=true` (mock landscape only), optionally with `RUNNER_SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`,
`_MOCK_SIMULATED_DELAY_SECONDS`, `_MOCK_ERROR_RATE` and `_MOCK_NON_CURRENT_TERM_RATE`. `SourceSimulatorRunnerDecorator`
looks up the deployed Function URL, sends the run's settings as query parameters of its `fetchPath`
(`simulatorFetchPath()`), and points the run's terms at the simulator's `/terms`. The dashboard's Bulk sync offers the
same in a mock landscape (Source: simulator).

### Query Parameters

- **`recordCount`**: Number of records per batch (default: 200)
- **`offset`**: accepted for compatibility but ignored; each request returns the next `recordCount` people not yet
  served
- **`totalPopulation`**, **`simulatedDelaySeconds`** (at most 30), **`errorRate`**, **`nonCurrentTermRate`**: a run's
  own settings, overriding the Lambda's defaults for that request

Example:
```
https://your-function-url.lambda-url.us-east-2.on.aws/?recordCount=500&totalPopulation=1950
```

### Single Person Endpoint

`/person?buid=U0000042` returns one person, mirroring the real person API: the same payload the
people endpoint produces for that person, without consuming anything from a run. A BUID beyond
`MOCK_TOTAL_POPULATION` returns `{ "response": [] }`; a missing or malformed one returns 400.
Runner.ts uses it when `RUNNER_SINGLE_PERSON_BUID` is set with `RUNNER_SOURCE_SIMULATOR=true`.

### Mock Data Structure

Generated persons include minimal fields for DataMapper (`MockPerson` in `src/chunking/fetch/SourceSimulator.ts`):

```json
{
  "personid": "U0000001",
  "bu_id": "00000001",
  "personBasic": {
    "names": [ { "firstName": "FirstName1", "lastName": "LastName1", "nameType": "...", "source": "..." } ]
  },
  "email": [ { "address": "U0000001@bu.edu", "type": "...", "source": "..." } ],
  "employeeInfo": { ... },      // a third of the population (by index % 3)
  "studentInfo": { ... },        // a third
  "affiliateInfo": { ... }       // a third
}
```

### Health Check

`/health` needs no API key:

```bash
curl https://your-function-url.lambda-url.us-east-2.on.aws/health
```

Response:
```json
{
  "status": "healthy",
  "totalPopulation": 10000,
  "errorRate": 0.0,
  "simulatedDelaySeconds": 0
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
# SOURCE_SIMULATOR_BUID=U0000042   # optional: call /person?buid= instead
SOURCE_SIMULATOR_PEOPLE_API_KEY=test-api-key
# SOURCE_SIMULATOR_OUTPUT_FILE_PATH=./scrap/source-simulator-output.json   # the default

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
SOURCE_SIMULATOR_PEOPLE_API_KEY=<your-api-key>
SOURCE_SIMULATOR_RECORD_COUNT=10
SOURCE_SIMULATOR_OFFSET=0
# SOURCE_SIMULATOR_BUID=U0000042   # optional: call /person?buid= instead

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
Runner.ts (RUNNER_SOURCE_SIMULATOR=true), or Bulk sync with the simulator
  ↓ the run's baseUrl/fetchPath point at the simulator
handleApiEvent() queues the message (the Runner in-process; Bulk sync through the chunker subscriber Lambda)
  ↓
ChunkFromAPI (reads message)
  ↓ ChunkConfigOverride mutates config
BigJsonFetch
  ↓ BuCdmPeopleDataSource
    ↓ BuCdmDataSource.fetchRaw()
      ↓ ApiClientForApiKey.get()
        ↓ **Source Simulator Lambda** (stateful depletion)
```

### Security

- Function URL is **public** but requires a valid API key in headers (except `/health`)
- Supports two authentication methods:
  - `Authorization: Bearer <api-key>`
  - `X-API-Key: <api-key>`
- The key must be one of the config secret's people, person or terms `endpointConfig.apiKey` values (read via
  `SECRET_ARN`)
- Generates fake data only (no real PII)

---

## Related Files

- **ChunkerService.ts**: Creates QueueProcessingFargateService and the EventBridge Scheduler schedule
- **ChunkerSubscribingLambda.ts**: Lambda construct definition (timeout, no automatic retry, grants)
- **ChunkerTaskDefinition.ts**: the chunker task definition
- **SourceSimulator.ts** (lib): Source Simulator Lambda construct
- **src/chunking/ChunkerSubscriber.ts**: Lambda handler (dispatcher)
- **src/chunking/fetch/ChunkerApiSubscriber.ts**: API event handler
- **src/chunking/filedrop/ChunkerS3Subscriber.ts**: S3 event handler
- **src/schedule/ScheduledRun.ts**: the schedule's input
- **docker/chunker.ts**: Fargate task entry point
- **src/chunking/fetch/ChunkFromAPI.ts**: API-based chunking implementation
- **src/chunking/filedrop/ChunkFromS3.ts**: S3-based chunking implementation
- **src/chunking/fetch/SourceSimulator.ts**: Mock API Lambda handler

---

## See Also

- [IContext Configuration](../../../context/IContext.ts)
- [Starting a run](../../../docs/RUN_START.md)
- [Processor Service (Phase 2)](../processor/)
- [Post-Processor Service (Phase 3)](../post-processor/)
- [EventBridge Cron Expressions](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-create-rule-schedule.html)
