# integration-huron-person-fargate/src: Utilities & Test Harnesses

## Purpose
Provides utility modules and test harnesses for validating infrastructure components, orchestrating manual operations, and testing individual service behaviors outside of the full CDK deployment.

## Directory Structure

```
src/
  ├─ Runner.ts                    (Manual chunking orchestration harness)
  ├─ DesiredCount.ts              (ECS service scaling utility)
  ├─ PersonCache.ts               (Person data caching utility)
  ├─ Queue.ts                     (SQS queue utilities)
  ├─ TaskProtection.ts            (ECS task protection management)
  ├─ AtomicCounter.ts             (DynamoDB-backed atomic counter)
  ├─ Utils.ts                     (General utilities)
  ├─ statistics/                  (Statistics table utilities)
  ├─ storage/                     (S3 storage helpers)
  ├─ chunking/                    (Chunking phase utilities)
  ├─ processing/                  (Processing phase utilities)
  └─ merging/                     (Merging phase utilities)
```

## Runner.ts - Manual Chunking Orchestration

**Purpose**: Test harness for manually triggering the chunking service outside of the EventBridge schedule. Allows for:
- Testing the full chunking flow end-to-end
- Initiating off-schedule sync runs
- Pre-seeding the processor queue for parallel testing
- Single-person testing for debugging

### Key Features

1. **Configuration Loading**: Supports multiple config sources with precedence
2. **Queue Seeding**: Pre-populate SQS queue with multiple messages for parallel processing
3. **ECS Scaling**: Automatically scale up chunker service desired count
4. **Single Person Mode**: Test with just one person (via SINGLE_PERSON_BUID)
5. **Dry Run Support**: Test without triggering downstream processors

### Environment Variables

**Required**:
- `CHUNKER_QUEUE_URL` - SQS queue URL for chunker messages
- `REGION` - AWS region
- `STACK_ID` - Stack identifier (e.g., `huron-person-fargate-processor`)
- `LANDSCAPE` - Deployment landscape (dev, test, prod)

**Optional**:
- `SECRET_ARN` - Secrets Manager ARN for configuration
- `HURON_PERSON_CONFIG_PATH` - Path to local config file
- `POPULATION_TYPE` - `person-delta` or `person-full` (default: person-full)
- `SINGLE_PERSON_BUID` - Test with single person (e.g., `U12345678`)
- `DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT` - Limit number of people fetched
- `BULK_RESET` - Set to `true` to force full resync
- `TRUST_PREVIOUS_STORAGE` - Set to `true` to trust delta storage
- `MESSAGES_TO_PREPOPULATE` - Number of messages to seed (default: 0)
- `DESIRED_COUNT` - Target ECS task count after seeding
- `ECS_CLUSTER_NAME` - ECS cluster name (required if setting desired count)
- `ECS_SERVICE_NAME` - ECS service name (required if setting desired count)

### Usage Patterns

#### Simple Manual Trigger (One Message)
```bash
# Trigger a single chunking run
export CHUNKER_QUEUE_URL="https://sqs.us-east-2.amazonaws.com/123/chunker-queue"
export REGION="us-east-2"
export STACK_ID="huron-person-fargate-processor"
export LANDSCAPE="dev"

npx ts-node src/Runner.ts
```

#### Queue Seeding for Parallel Testing
```bash
# Seed queue with 50 messages, then scale service to 10 tasks
export MESSAGES_TO_PREPOPULATE="50"
export DESIRED_COUNT="10"
export ECS_CLUSTER_NAME="huron-person-cluster-dev"
export ECS_SERVICE_NAME="Chunker"

npx ts-node src/Runner.ts
```

#### Single Person Testing
```bash
# Test with just one person
export SINGLE_PERSON_BUID="U12345678"
export POPULATION_TYPE="person-delta"

npx ts-node src/Runner.ts
```

#### Delta vs Full Sync
```bash
# Delta sync (default - only changed records)
export POPULATION_TYPE="person-delta"
export BULK_RESET="false"

# Full sync with reset
export POPULATION_TYPE="person-full"
export BULK_RESET="true"

npx ts-node src/Runner.ts
```

### How It Works

1. **Load Configuration**: Merges environment, file system, Secrets Manager
2. **Validate Inputs**: Checks required environment variables
3. **Build API Request**: Constructs baseUrl and fetchPath
4. **Queue Seeding (Optional)**:
   - Resets atomic counter to 0
   - Seeds queue with N messages (each with unique offset)
   - Scales up ECS service to process messages in parallel
5. **Single Message (Default)**:
   - Sends one message to chunker queue
   - Triggers normal one-in/one-out processing

### TestEnvironment Integration

Uses `TestEnvironment('RUNNER')` pattern for prefix-based variable loading:

```typescript
if (require.main === module) {
  const testEnvironment = TestEnvironment('RUNNER');
  
  [
    'CHUNKER_QUEUE_URL',
    'REGION',
    'STACK_ID',
    'LANDSCAPE'
  ].forEach(testEnvironment.getVar);
  
  // ... execution
}
```

### Related Infrastructure

- **ChunkerService** (`lib/services/chunker/ChunkerService.ts`): CDK construct that creates the ECS service
- **ChunkerSubscriber Lambda** (`lib/services/chunker/ChunkerSubscribingLambda.ts`): Lambda that sends messages to chunker queue
- **QueueSeeder** (`src/chunking/fetch/QueueSeeder.ts`): Utility for pre-populating queue with messages

### VS Code Debugging

Add to `.vscode/launch.json`:

```json
{
  "type": "node",
  "request": "launch",
  "name": "Debug Runner",
  "runtimeArgs": ["-r", "ts-node/register"],
  "args": ["${workspaceFolder}/src/Runner.ts"],
  "envFile": "${workspaceFolder}/.env",
  "cwd": "${workspaceFolder}",
  "internalConsoleOptions": "openOnSessionStart"
}
```

### Dry Run Mode

To test chunking without triggering downstream processing, set the ChunkerSubscriber Lambda to dry run mode:

```bash
aws lambda update-function-configuration \
  --region us-east-2 \
  --function-name chunker-subscriber-dev \
  --environment "Variables={DRY_RUN=true}"
```

This prevents the chunker from sending messages to the processor queue when chunk files are uploaded to S3.

## Other Utilities

### DesiredCount.ts
**Purpose**: Utility for scaling ECS services up or down

```bash
npx ts-node src/DesiredCount.ts
```

### PersonCache.ts
**Purpose**: Cache person data for testing and debugging

```bash
npx ts-node src/PersonCache.ts
```

### TaskProtection.ts
**Purpose**: Manage ECS task protection to prevent premature termination

```bash
npx ts-node src/TaskProtection.ts
```

### AtomicCounter.ts
**Purpose**: DynamoDB-backed atomic counter for offset generation

```bash
npx ts-node src/AtomicCounter.ts
```

## Execution Methods

### VS Code Launch Configuration (Recommended)
**File**: `.vscode/launch.json` (provided by this project)

Configuration: "Debug current file"
- Automatically loads `.env`
- Provides breakpoints and step-through debugging
- Allows variable inspection

**Usage**:
1. Open Runner.ts (or other utility)
2. Press F5 or Run > Start Debugging
3. Select "Debug current file"

### Command Line (npx)
```bash
npx ts-node src/Runner.ts
npx ts-node src/DesiredCount.ts
npx ts-node src/PersonCache.ts
```

## Common Patterns

### Adding a New Utility Harness
1. Create module in `src/` directory
2. Import TestEnvironment from 'integration-core'
3. Add `if (require.main === module)` block with TestEnvironment instantiation
4. Add environment variables to `.env` under harness-specific section
5. Add placeholders to `example-env.md`
6. Update this README with usage documentation

### Example Harness Pattern
```typescript
import { TestEnvironment } from 'integration-core';

async function main() {
  // ... implementation
}

if (require.main === module) {
  const testEnvironment = TestEnvironment('MY_HARNESS');
  
  [
    'REQUIRED_VAR_1',
    'REQUIRED_VAR_2'
  ].forEach(testEnvironment.getVar);
  
  [
    'OPTIONAL_VAR_1',
    'OPTIONAL_VAR_2'
  ].forEach(testEnvironment.getVarOrEmptyString);
  
  main();
}

export { main };
```

## Relationship to CDK Infrastructure

The utilities in this directory are **test harnesses** and **operational tools** that work with deployed infrastructure but are **not part of the CDK stack itself**. They:

- **Test deployed services** (Runner.ts tests ChunkerService ECS tasks)
- **Manage operations** (DesiredCount.ts scales ECS services)
- **Debug issues** (PersonCache.ts inspects cached data)

They complement the infrastructure defined in `lib/` but do not define infrastructure themselves.
