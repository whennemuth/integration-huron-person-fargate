# Test Harnesses, Environment and Docker Entry Points

How this project's test harnesses are built and run, how their `.env` is organized, what the Docker entry points do,
and how the harnesses came to import `TestEnvironment` from integration-core. The harness pattern itself is in the
workspace skills `testenvironment-pattern`, `harness-initialization-patterns` and
`environment-variable-organization`; the dashboard's own harnesses are in `dashboard/CLAUDE.md`. Read this when adding
a harness or a Docker entry point, or when a harness can't find its variables.

## Harness pattern

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

If combined with other imports: `import { Timer, TestEnvironment } from 'integration-core';`.

There is **no wrapper layer** in fargate (unlike huron-person), because the harnesses are simpler and don't share a
common set of downstream keys.

## Harness catalog

- **Orchestration**: Runner (`src/runner/Runner.ts`, manual chunking service invocation; see `src/runner/CLAUDE.md`)
- **Chunking pipeline**: ChunkFromAPI, ChunkFromS3, BigJsonFetch, BigJsonFile, PersonArrayWrapper
  (prefixes `CHUNK_FROM_API_*`, `CHUNK_FROM_S3_*`, `CHUNKER_SERVICE_*`, ...)
- **Post-processing pipeline**: DeferredDeleteHandler (`DEFERRED_DELETE_HANDLER_*`), PersonCache, StatisticsTable
  (`STATISTICS_TABLE_*`)
- **Docker entry points**: `docker/chunker.ts` (`DOCKER_CHUNKER`), `docker/processor.ts` (`DOCKER_PROCESSOR`),
  `docker/post-processor.ts` (`DOCKER_POST_PROCESSOR`)
- **Error handling**: ApiErrorTracking (`src/ApiErrorTracking.ts`)
- **Utilities**: EcrChecker

## Running a harness

VS Code (recommended): open the harness file (e.g. `src/chunking/fetch/ChunkFromAPI.ts`), press F5 (Run > Start
Debugging), and select "Debug current file".

Command line:

```bash
npx ts-node src/chunking/fetch/ChunkFromAPI.ts
npx ts-node docker/chunker.ts
npx ts-node src/runner/Runner.ts
```

Use the Runner harness to validate chunking orchestration before a deployment.

## Environment configuration

`.env` (git-ignored, local development only) is structured like huron-person's: shared variables without a prefix,
then one group per harness, headed by the file it serves.

```
# Shared variables (no prefix)
DATASOURCE_BASE_URL=...
DATATARGET_BASE_URL=...

# Harness groups by pipeline phase
# ---------- Use these for src/runner/Runner.ts ---------- #
RUNNER_CHUNKER_QUEUE_URL=...

# ---------- Use these for src/chunking/fetch/ChunkFromAPI.ts ---------- #
CHUNK_FROM_API_BASE_URL=...

# ... etc for every harness
```

The template is `example-env.md` (sanitized, with placeholders).

## Docker entry points

- **`docker/chunker.ts`**: container entry point for the ECS Fargate task running the chunking phase. Environment
  loaded via `TestEnvironment('DOCKER_CHUNKER')`: S3 bucket configuration, source API credentials (via
  `DATASOURCE_*`), chunk size parameters. Run it with npx to validate chunking before an ECS deployment.
- **`docker/processor.ts`**: router (switchboard) for the parallel processing tasks; detects the storage mode and
  delegates to `ProcessorForS3` (`src/processing/`: S3-based processing with mini-deltas and marker files) or
  `ProcessorForDynamoDB` (`src/processing/`: direct table writes).
- **`docker/post-processor.ts`**: router for result consolidation; delegates to `PostProcessorForS3` (file
  consolidation and baseline merging) or `PostProcessorForDynamoDB` (minimal/no-op merge, state already in the
  tables), both extending `AbstractPostProcessor` (`src/post-processing/`: `getTaskParameters`,
  `processDeferredDeletes`, the `main` template method). Both modes require the post-processor for the
  `DeferredDeleteHandler`.

The processor and post-processor routers detect DynamoDB mode by the presence of
`DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME` or `DYNAMODB_PERSON_HISTORY_TABLE_NAME`. `docker/CLAUDE.md` has the detail.

## History: moving `TestEnvironment` to integration-core

Initially fargate had a local `src/Utils.ts` with its own `TestEnvironment` definitions. All 14 harness modules were
migrated to import `TestEnvironment` directly from the `integration-core` package:

- Docker entries: chunker.ts, processor.ts, post-processor.ts
- Chunking: BigJsonFile.ts, BigJsonFetch.ts, ChunkFromAPI.ts, ChunkFromS3.ts, PersonArrayWrapper.ts
- Post-processing: DeferredDeleteHandler.ts, StatisticsTable.ts, PersonCache.ts
- Error handling: ApiErrorTracking.ts
- Orchestration: Runner.ts (extracted from `lib/services/chunker/ChunkerService.ts`)
- Utilities: scrap/EcrChecker.ts

```typescript
// Before migration:
import { TestEnvironment } from '../Utils';

// After migration:
import { TestEnvironment } from 'integration-core';
```
