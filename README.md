# Huron Person Integration - Fargate Infrastructure

This CDK TypeScript project deploys the Huron Person integration as a serverless ECS Fargate task that processes batches of people synchronization in parallel.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Architecture

- **ECS Cluster**: Manages Fargate task instances
- **Fargate Task Definition**: Configures processor container with environment variables
- **EventBridge Scheduled Rule**: Optional trigger for recurring sync operations
- **S3 Integration**: Reads source data from S3 for parallel chunk processing
- **Secrets Manager**: Stores API credentials and configuration securely

## Configuration

The Fargate task is configured via the `HURON_PERSON_CONFIG` environment variable, which contains a JSON configuration object. The configuration structure matches the `integration-huron-person` package.

### Dry Run Mode

Dry run mode allows testing the synchronization logic without making changes to the target Huron API or delta storage. This is particularly useful when:
- Testing new configurations in production-like environments
- Validating data mappings before actual synchronization
- Debugging issues without affecting live data
- Auditing planned synchronization operations

#### Enabling Dry Run in Fargate

Set `dryRun: true` in the `dataTarget` section of your configuration:

```json
{
  "dataTarget": {
    "endpointConfig": { ... },
    "personsPath": "/api/v2/persons",
    "organizationsPath": "/api/v2/organizations",
    "dryRun": true
  }
}
```

When dry run is enabled:
- The Fargate task sets the `DRY_RUN=true` environment variable for processor containers
- All write operations (POST/PATCH to Huron API) are logged but not executed
- Delta storage updates are skipped (previous data is not persisted)
- Read operations continue normally (source fetching, delta computation, HRN lookups)
- All parallel chunks process without writing any data
- Console logs show what operations would have been performed

**Important**: Dry run mode affects all parallel processing chunks, so the entire batch runs in test mode.

## Test Harnesses

Test harnesses are executable modules that verify individual components using environment-based configuration via the `TestEnvironment` utility from `integration-core`. Each harness loads its own prefixed environment variables and validates component behavior in isolation.

Harness configuration is documented in [example-env.md](./example-env.md). The file contains grouped environment variables for test harnesses covering the major processing pipelines:

- **CDK Service Layer**: `ChunkerService` (Lambda-based chunking orchestration)
- **Chunking Pipeline**: `ChunkFromAPI`, `ChunkFromS3`, `BigJsonFetch`, `BigJsonFile`, `PersonArrayWrapper`
- **Merging Pipeline**: `DeferredDeleteHandler`, `PersonCache`, `StatisticsTable`
- **Docker Entry Points**: `chunker.ts`, `processor.ts`, `merger.ts`
- **Storage & Error Handling**: `S3StorageAdapter`, `ApiErrorTracking`, `EcrChecker`

### Running Test Harnesses

**Option 1: Using VS Code Launch Configuration (Recommended)**

1. Open the harness file in the editor (e.g., `src/chunking/fetch/ChunkFromAPI.ts`)
2. Press `F5` or go to **Run > Start Debugging**
3. Select "Debug current file" from the launch configuration dropdown
4. The harness will execute with your `.env` file automatically loaded

**Option 2: Command Line with npx**

```bash
# Example: Run the ChunkFromAPI harness
npx ts-node src/chunking/fetch/ChunkFromAPI.ts

# Example: Run the PersonCache harness
npx ts-node src/PersonCache.ts

# Example: Run the docker chunker harness
npx ts-node docker/chunker.ts
```

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `npx cdk deploy`  deploy this stack to your default AWS account/region
* `npx cdk diff`    compare deployed stack with current state
* `npx cdk synth`   emits the synthesized CloudFormation template
