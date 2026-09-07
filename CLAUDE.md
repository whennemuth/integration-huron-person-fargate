# integration-huron-person-fargate: Serverless Pipeline & Infrastructure

## Project Purpose
AWS CDK infrastructure and serverless implementation of the three-phase streaming delta synchronization pipeline. Implements chunking, processing, and merging phases using Lambda, ECS Fargate, S3, and SQS.

## Repository Relationship Model

This project is an independently versioned npm package with its own source repository.

It composes with sibling repositories through dependency relationships (especially `integration-core`) rather than workspace-level source control.

## Shared Skills Repository

Cross-repository Copilot skills are maintained in a separate repository at `integration-workspace-skills/skills/`.

VS Code discovers these skills using the `chat.agentSkillsLocations` setting in your `.code-workspace` file. In multi-root `.code-workspace` configurations, `chat.agentSkillsLocations` paths are resolved relative to each workspace root folder (not from the `.code-workspace` file location).

Canonical settings entry:

```json
"chat.agentSkillsLocations": {
  "../integration-workspace-skills/skills": true
}
```

Core-only and core+person+fargate workspace examples are documented in this repository's `README.md`.

## Workspace-Scoped Memory Files

The `.copilot/memories/` directory (visible in the workspace as "workspace-memories") stores workspace-scoped Copilot memory files that apply to all projects.

**Purpose**: Stores coding preferences, task verification protocols, and workflow requirements that should be consistently applied across all integration projects (core, huron-person, fargate, dashboard, file-drop, etc.).

**Key file**: `task-verification-protocol.md` - Defines requirements for build verification, test execution, and completion reporting on all code implementation tasks.

**Discovery**: VS Code Copilot automatically loads memory files from `.copilot/memories/` when the directory is included as a workspace folder.

## Implementation Verification Protocol

**CRITICAL**: When implementing code that depends on unfamiliar abstractions, control flow directives, or domain-specific patterns, you MUST verify their actual behavior before proceeding.

### High-Risk Abstractions Requiring Verification

- **Control flow directives**: `__arrayFieldOperations`, `__metadata`, behavioral flags
- **Update semantics**: append vs replace, merge vs overwrite patterns
- **Authentication patterns**: JWT token management, credential resolution
- **Pipeline coordination**: Phase transitions, queue message handling
- **Docker entrypoint patterns**: Environment loading, error propagation
- **CDK infrastructure patterns**: Stack dependencies, resource references

### Mandatory Verification Steps

Before implementing code that uses an unfamiliar abstraction:

1. **Search for definition**: Use `grep_search` to find where it's defined
2. **Find consumers**: Search for where it's processed/interpreted
3. **Read usage examples**: Look at tests and similar patterns
4. **State your understanding**: Explicitly describe what you think it does
5. **Think through interactions**: Consider edge cases and combinations
6. **Only then implement**: Proceed with verified understanding

### When You're Uncertain

If you cannot fully verify an abstraction's behavior:

- **State explicitly what you don't know**
- **Ask whether to search for implementation first**
- **Do NOT proceed on "educated guesses"**

### Real Example: config.preLoadedMaps Dead Code

A runner decorator (`MockTargetRunnerDecorator`) mutated `config.preLoadedMaps = {orgMap:false, ...}` believing this would prevent organization/state/country lookups from calling the real Huron API in mock target mode:
- **Assumption**: Setting this field on the Runner's `Config` object would reach the running processor task
- **Reality**: `config.preLoadedMaps` is only ever read by CDK at deploy time (`ProcessorTaskDefinition.ts`) to bake `STATIC_MAP_USAGE` into the ECS task's environment variables - from an entirely different `Config` object loaded from disk, not the one the Runner mutated at invocation time
- **Result**: The override compiled and ran with zero errors, logged nothing wrong, and had absolutely no effect - mock-mode runs could still trigger real organization API calls

This was fixed by forcing `StaticMapUsage` at the point it's actually consumed - inside the processor, immediately after `flags.useMockTarget` is read from chunk metadata (the same channel already used to select the mock data target) - rather than trying to inject it upstream through a Config object that never reaches the running task. The lesson: a runtime override that "looks right" and produces no errors can still be a no-op if it mutates the wrong instance of a config object that exists in two different lifecycles (deploy-time vs invocation-time).

### Source Simulator

The source simulator (`src/chunking/fetch/SourceSimulator.ts`) is a Lambda Function URL that provides mock API responses for full 3-phase integration testing without the real API's 30-minute cooldown constraint.

**Endpoints** (path-based routing):
- `/` (default): mock person data, using a stateful depletion model (see repo memory `source-simulator-semantics.md` for allocator semantics)
- `/terms`: mock current-terms data (6 static terms: 2 current, 2 past, 2 future) - required because `DataMapperOrg.isCurrentSemester()` needs terms data to filter student semesters, and running with `RUNNER_CHUNKING_ONLY=false` exercises this path
- Optional `nonCurrentTermRate` query parameter (0.0-1.0, default 0.0): probability that a simulated student is assigned a non-current term, for testing semester-filtering logic

**Safety enforcement - source simulation entails target simulation**: `RUNNER_SOURCE_SIMULATOR=true` automatically forces `useMockTarget=true` (in `Runner.ts` and, redundantly, in `MockTargetRunnerDecorator`) whenever the processor phase is enabled (`RUNNER_CHUNKING_ONLY=false`). There is no supported way to run simulated source data against the real Huron target API - this is enforced in code, not just documented as a convention.

### User Override

You can skip verification by saying:
- "Skip verification and proceed"
- "Use inference for this"

**See Also**: `verify-abstractions-before-implementation` skill in workspace skills repository

## Architecture: Three-Phase Pipeline

### Phase 1: Chunking
**Purpose**: Stream large JSON payloads into NDJSON chunks for parallel processing

**Components**:
- ChunkerService (Lambda-triggered via CDK)
- ChunkFromAPI (fetch data from source APIs)
- ChunkFromS3 (ingest from S3 file drop)
- BigJsonFetch, BigJsonFile (streaming utilities)
- PersonArrayWrapper (array chunking logic)

**TestEnvironment Integration**: All chunking modules use `TestEnvironment` imported from `integration-core`

**Harness Prefixes**:
- CHUNK_FROM_API_*, CHUNK_FROM_S3_*, CHUNKER_SERVICE_*
- See `.env` and `example-env.md` for grouped configuration

### Phase 2: Processing
**Purpose**: Apply data mapping and validation transformations in parallel ECS Fargate tasks

**Components**:
- Docker entrypoint: `docker/processor.ts`
- Task orchestration via CDK
- Batch processing of chunks

**Harness Prefix**: DOCKER_PROCESSOR_*

### Phase 3: Merging  
**Purpose**: Consolidate processing results and write final delta storage

**Components**:
- DeferredDeleteHandler (soft delete coordination)
- StatisticsTable (merge metrics)
- Implementation classes: `src/merging/AbstractMerger.ts` (base class), `src/merging/MergerForS3.ts`, `src/merging/MergerForDynamoDB.ts`
- Docker entrypoint: `docker/merger.ts` (router)

**Architecture**: Template Method pattern with abstract base class
- `AbstractMerger` (src/merging/): Base class with shared logic (getTaskParameters, processDeferredDeletes, main template method)
- `MergerForS3` (src/merging/): S3-specific implementation (file consolidation, baseline merging)
- `MergerForDynamoDB` (src/merging/): DynamoDB-specific implementation (minimal/no-op merge, state already in tables)
- `merger.ts` (docker/): Router that detects storage mode and delegates to appropriate implementation

**Critical Design Note**: BOTH storage modes require the merger service
- S3 mode: File consolidation + deletion handling
- DynamoDB mode: Deletion handling only (no file consolidation needed)

**Harness Prefixes**:
- DEFERRED_DELETE_HANDLER_*, STATISTICS_TABLE_*
- DOCKER_MERGER_*

## Storage Modes: DynamoDB vs S3

The pipeline supports two storage modes for delta state and metadata, controlled by `config.storage.type`:
- `'s3'` | `'file'` | `'database'` → **S3 Mode** (traditional file-based storage)
- `'dynamodb'` → **DynamoDB Mode** (database-backed state tracking)

**IMPORTANT**: S3 chunk NDJSON files (`chunk-0000.ndjson`, `chunk-0001.ndjson`, etc.) remain in S3 regardless of storage mode. Only state/metadata files migrate to DynamoDB.

### S3 Mode (Default)

**What's stored in S3**:
1. **Chunk files**: `s3://bucket/chunks/{population}/{timestamp}/chunk-XXXX.ndjson`
2. **Metadata file**: `s3://bucket/chunks/{population}/{timestamp}/_metadata.json`
3. **Flags file**: `s3://bucket/chunks/{population}/{timestamp}/_flags.json`
4. **Terminal error marker**: `s3://bucket/chunks/{population}/{timestamp}/_terminal_error.json`
5. **Delta storage**: `s3://bucket/delta-storage/{population}/previous-input.ndjson`
6. **Hash storage**: `s3://bucket/delta-storage/{population}/hashes.ndjson`

**Characteristics**:
- Simple file-based architecture
- Easy to inspect with AWS Console or CLI
- Lower cost for infrequent access patterns
- Sequential file I/O (streaming, line-by-line reading)

### DynamoDB Mode (Parallel Implementation)

**What's stored in DynamoDB**:
1. **PersonCurrentStateTable** - Current person sync state
   - PK: `personId` (BUID)
   - Attributes: `hash`, `sourceIdentifier`, `lastSyncTime`, `syncRunId`
   - GSI: `syncRunId-personId` (for querying all persons in a sync run)
   
2. **PersonHistoryTable** - Historical change audit trail
   - PK: `personId`, SK: `syncRunId` (composite key for versioning)
   - Attributes: `changeType` (CREATED | UPDATED | DELETED), `hash`, `timestamp`
   - GSI1: `syncRunId-changeType` (query all changes in a run by type)
   - GSI2: `changeType-syncRunId` (query changes across runs by type)

3. **StatisticsTable** - Metadata, flags, and error events
   - PK: `integrationTimestamp` (syncRunId), SK: `eventType`
   - Event types: `METADATA`, `FLAGS`, `TERMINAL_ERROR`, `STATISTICS`, `ERROR:*`
   - Replaces `_metadata.json` and `_flags.json` files from S3 mode

**What remains in S3** (even in DynamoDB mode):
- Chunk NDJSON files (parallel processing dependency)
- Person cache file (10K-100K BUIDs, better in S3 than DynamoDB item batches)

**Characteristics**:
- Atomic updates with conditional expressions
- Point-in-time recovery and backups
- Query-based access patterns (GSI flexibility)
- Better for high-frequency state lookups
- Supports resumption after failures (via PersonCurrentStateTable)

### Migration Path (Parallel Implementation)

**DynamoDB mode does NOT remove S3 features**. Both modes coexist:

**Template Pattern Abstractions**:
1. **HashStorage** (integration-huron-person/src/delta-storage/)
   - `AbstractHashStorage` base class
   - `HashStorageResetForS3` (S3 implementation)
   - `HashStorageResetForDynamoDb` (DynamoDB implementation)
   - `HashStorageResetFactory` switches on `config.storage.type`

2. **PersonCache** (src/person-cache/)
   - `AbstractPersonCache` base class
   - `PersonCacheForS3` (direct S3 implementation)
   - `PersonCacheForDynamoDb` (facade delegating to S3 - optimal for bulk data)
   - `PersonCacheFactory` switches on `config.storage.type`
   
   **Mock Target Support** (Strategy Pattern):
   - `AbstractPersonTarget` interface: Abstracts person data source
   - `PersonTargetReal`: Fetches from real Huron API via ListPeople
   - `PersonTargetMocked`: Scans MockTargetPersonTable (DynamoDB) for test data
   - Factory injects appropriate PersonTarget based on `useMockTarget` flag
   - Design: Dependency injection enables testing without environment coupling
   - `MockPersonDataTarget.getPersonByBuid()` (integration-huron-person): single-person existence check used by `UpsertDeltaStrategy` when `flags.useMockTarget` is true, so the create-vs-update lookup never hits the real Huron API in mock mode
   - DELETE against the mock target is a soft-delete (`deactivated`/`deactivatedAt` attributes), matching Huron's soft-delete-only requirement - records are never removed from MockTargetPersonTable, only marked inactive
   - `StaticMapUsage` is forced to `{orgMap:false, stateMap:false, countryMap:false}` at runtime (via `resolveStaticMapUsage()` in ProcessorForS3.ts/ProcessorForDynamoDb.ts) whenever `flags.useMockTarget` is true, overriding whatever `STATIC_MAP_USAGE` was baked into the task at deploy time

3. **Metadata** (src/chunking/metadata/)
   - `AbstractMetadata` with 19 methods (5 static, 14 abstract)
   - `MetadataForS3` (file-based implementation)
   - `MetadataForDynamoDb` (StatisticsTable with `eventType` field)
   - `MetadataFactory` switches on `config.storage.type`

**Switching Between Modes**:
```typescript
// In Secrets Manager or context.json:
{
  "storage": {
    "type": "s3"        // Traditional file-based mode
    // OR
    "type": "dynamodb"  // DynamoDB state tracking mode (default)
  }
}
```

**IAM Permissions** (automatically configured in task definitions):
- S3 mode: S3 bucket read/write only
- DynamoDB mode: S3 bucket + DynamoDB table read/write (conditionally granted)

**CDK Infrastructure** (`lib/DynamoDB.ts`):
- Tables created when `context.PREVIOUS_STORAGE_TYPE === 'dynamodb'` or when `context.PREVIOUS_STORAGE_TYPE` is undefined (defaults to 'dynamodb')
- Task definitions check `dynamoDbTables.personCurrentStateTable` before granting permissions
- Zero infrastructure impact when using S3 mode

### When to Use Each Mode

**Use S3 Mode when**:
- Simple deployment with minimal infrastructure
- Infrequent sync operations (daily/weekly)
- File-based introspection preferred (AWS Console browsing)
- Lower cost priority for small-scale operations

**Use DynamoDB Mode when**:
- Frequent sync operations requiring fast state lookups
- Audit trail and history tracking critical
- Resumption and retry logic needed (pipeline interruptions)
- Query-based analytics desired (change type filtering, time-series analysis)
- Point-in-time recovery and backups required

## TestEnvironment Pattern Implementation

### Migration Strategy
**Challenge**: Initially had local `src/Utils.ts` with TestEnvironment definitions.

**Solution**: Migrated all 14 harness modules to import TestEnvironment directly from `integration-core` package.

**Files Migrated** (14 total):
- Docker entries: chunker.ts, processor.ts, merger.ts
- Chunking: BigJsonFile.ts, BigJsonFetch.ts, ChunkFromAPI.ts, ChunkFromS3.ts, PersonArrayWrapper.ts
- Merging: DeferredDeleteHandler.ts, StatisticsTable.ts, PersonCache.ts
- Error handling: ApiErrorTracking.ts
- Orchestration: Runner.ts (extracted from lib/services/chunker/ChunkerService.ts)
- Utilities: scrap/EcrChecker.ts

### Import Pattern
```typescript
// Before migration:
import { TestEnvironment } from '../Utils';

// After migration:
import { TestEnvironment } from 'integration-core';

// If combined with other imports:
import { Timer, TestEnvironment } from 'integration-core';
```

### Harness Initialization Pattern
```typescript
import { TestEnvironment } from 'integration-core';

// ... implementation

if (require.main === module) {
  const env = new TestEnvironment('HARNESS_PREFIX');
  const variable = env.getVar('DOWNSTREAM_KEY');
  // ... run logic
  main();
}
```

**No wrapper layer** in fargate (unlike huron-person) because harnesses are simpler and don't share a common set of downstream keys across all 14 modules.

## Environment Configuration

**File**: `.env` (git-ignored, local development only)

**Structure** (parallel to huron-person):
```
# Shared variables (no prefix)
DATASOURCE_BASE_URL=...
DATATARGET_BASE_URL=...

# Harness groups by pipeline phase
# ---------- Use these for src/Runner.ts ---------- #
RUNNER_CHUNKER_QUEUE_URL=...

# ---------- Use these for src/chunking/fetch/ChunkFromAPI.ts ---------- #
CHUNK_FROM_API_BASE_URL=...

# ... etc for all 14 harnesses
```

**Template**: See `example-env.md` (~270 lines, sanitized with placeholders)

## Docker Entry Points

### chunker.ts
**Purpose**: Container entry point for ECS Fargate task running chunking phase

**Environment Variables**: Loaded via TestEnvironment('DOCKER_CHUNKER')
- S3 bucket configuration
- Source API credentials (via DATASOURCE_*)
- Chunk size parameters

**Harness Execution**: Run with npx to validate chunking before ECS deployment

### processor.ts
**Purpose**: Router entry point for parallel processing tasks

**Architecture**: Switchboard pattern - detects storage mode and delegates to appropriate processor
- ProcessorForS3 (src/processing/): S3-based processing with mini-deltas and marker files
- ProcessorForDynamoDB (src/processing/): DynamoDB-based processing with direct table writes
- processor.ts (docker/): Router that detects storage mode and delegates

**Mode Detection**: Checks for DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME

**Harness Prefix**: DOCKER_PROCESSOR

### merger.ts
**Purpose**: Router entry point for result consolidation

**Architecture**: Template Method pattern with mode-specific implementations
- AbstractMerger (src/merging/): Base class with shared logic (getTaskParameters, processDeferredDeletes, main)
- MergerForS3 (src/merging/): S3-specific file consolidation and baseline merging
- MergerForDynamoDB (src/merging/): Minimal/no-op merge (state already in DynamoDB tables)
- merger.ts (docker/): Router that detects storage mode and delegates

**Critical**: Both modes require merger service for DeferredDeleteHandler

**Mode Detection**: Checks for DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME

**Harness Prefix**: DOCKER_MERGER

## Test Harnesses (14+ total)

**Categories**:
- **Orchestration** (1): Runner (Manual chunking service invocation)
- **Chunking Pipeline** (5): ChunkFromAPI, ChunkFromS3, BigJsonFetch, BigJsonFile, PersonArrayWrapper
- **Merging Pipeline** (3): DeferredDeleteHandler, PersonCache, StatisticsTable
- **Docker Entry Points** (3): chunker.ts, processor.ts, merger.ts
- **Error Handling** (1): ApiErrorTracking
- **Utilities** (1): EcrChecker

### Execution

**VS Code Launch Configuration (Recommended)**:
```
1. Open harness file (e.g., src/chunking/fetch/ChunkFromAPI.ts)
2. Press F5 or Run > Start Debugging
3. Select "Debug current file"
```

**Command Line (npx)**:
```bash
npx ts-node src/chunking/fetch/ChunkFromAPI.ts
npx ts-node docker/chunker.ts
npx ts-node src/Runner.ts
```

## CDK Infrastructure Patterns

### Context Configuration
**Files**: `context/context.json`, `context/IContext.ts`

Defines infrastructure parameters (VPC, subnet, image URIs, etc.) per deployment tier.

### Stack Management
**Pattern**: `lib/Stack.ts` orchestrates Lambda, ECS, S3, SQS resources

**Harness Testing**: Use Runner harness (`src/Runner.ts`) to validate chunking orchestration before deployment

## Patterns to Follow

### Adding a New Harness
1. Create module in appropriate src/ subdirectory or docker/
2. Import TestEnvironment from 'integration-core'
3. Add `if (require.main === module)` block with TestEnvironment instantiation
4. Add environment variables to `.env` under harness-specific section
5. Add placeholders to `example-env.md`
6. Update README test harnesses list

### Integrating with CDK
1. Harness should test individual component functionality
2. CDK Lambda/ECS configurations should reference the same environment variables (via Secrets Manager or ECS TaskDef)
3. Use ConfigManager chain pattern for flexibility across local/staging/production

### Docker Image Deployment
1. Test entrypoint harness locally with `.env`
2. Verify in docker/Dockerfile build
3. Deploy to ECR and reference in CDK
4. Test in ECS task before full pipeline run

## Dependencies
- `integration-core`: TestEnvironment, base classes, utilities
- AWS services: Lambda, ECS Fargate, S3, SQS, Secrets Manager
- CDK libraries: cdk, aws-ec2, aws-lambda, aws-ecs, aws-s3, aws-sqs
- Node ecosystem: ts-node, Docker, TypeScript

## Common Issues & Debugging

### Environment Variable Not Found
1. Check `.env` has the prefixed variable in correct harness group
2. Verify `TestEnvironment('PREFIX')` matches the `.env` group prefix
3. Use getVar() without the prefix (TestEnvironment adds it)

### Docker Build Failures
1. Check docker/Dockerfile references correct src files
2. Ensure .env variables align with docker/entrypoint.ts expectations
3. Test harness locally before building Docker image

### ECS Task Failures
1. Verify IAM role has S3, SQS, Secrets Manager permissions
2. Check context.json has correct resource ARNs
3. Validate ConfigManager chain loads credentials correctly (TaskDef → Secrets Manager → environment)

