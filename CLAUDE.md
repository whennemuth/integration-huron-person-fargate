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
- Docker entrypoint: `docker/merger.ts`

**Harness Prefixes**:
- DEFERRED_DELETE_HANDLER_*, STATISTICS_TABLE_*
- DOCKER_MERGER_*

## TestEnvironment Pattern Implementation

### Migration Strategy
**Challenge**: Initially had local `src/Utils.ts` with TestEnvironment definitions.

**Solution**: Migrated all 14 harness modules to import TestEnvironment directly from `integration-core` package.

**Files Migrated** (14 total):
- Docker entries: chunker.ts, processor.ts, merger.ts
- Chunking: BigJsonFile.ts, BigJsonFetch.ts, ChunkFromAPI.ts, ChunkFromS3.ts, PersonArrayWrapper.ts
- Merging: DeferredDeleteHandler.ts, StatisticsTable.ts, PersonCache.ts
- Error handling: ApiErrorTracking.ts
- Service layer: lib/services/chunker/ChunkerService.ts
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
# ---------- Use these for lib/services/chunker/ChunkerService.ts ---------- #
CHUNKER_SERVICE_TIMEOUT=...

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
**Purpose**: Container entry point for parallel processing tasks

**Harness Prefix**: DOCKER_PROCESSOR

### merger.ts
**Purpose**: Container entry point for result consolidation

**Harness Prefix**: DOCKER_MERGER

## Test Harnesses (14+ total)

**Categories**:
- **CDK Service Layer** (1): ChunkerService
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
npx ts-node lib/services/chunker/ChunkerService.ts
```

## CDK Infrastructure Patterns

### Context Configuration
**Files**: `context/context.json`, `context/IContext.ts`

Defines infrastructure parameters (VPC, subnet, image URIs, etc.) per deployment tier.

### Stack Management
**Pattern**: `lib/Stack.ts` orchestrates Lambda, ECS, S3, SQS resources

**Harness Testing**: Use ChunkerService harness to validate context loading before cdk deploy

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

