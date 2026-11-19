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

Every module with a `require.main === module` block is a harness; its prefix is the one its `TestEnvironment(...)`
call names. Each folder's CLAUDE.md lists its own:

- **Orchestration**: the Runner (`src/runner/Runner.ts`, `RUNNER`), `MetricsCatchupDelay` (`METRICS_CATCHUP_DELAY`),
  `ServiceToggler` (`SERVICE_TOGGLER`); see `src/runner/CLAUDE.md`.
- **Chunking**: `ChunkFromAPI` (`CHUNK_FROM_API`), `ChunkFromS3` (`CHUNK_FROM_S3`), `BigJsonFetch` (`BIG_JSON_FETCH`),
  `BigJsonFile` (`BIG_JSON_FILE`), `PersonArrayWrapper` (`PERSON_ARRAY_WRAPPER`), `ChunkerQueue` (`CHUNKER_QUEUE`),
  `QueueSeeder` (`QUEUE_SEEDER`), `ProcessorServiceBooster` (`PROCESSOR_SERVICE_BOOSTER`), `SourceSimulator`
  (`SOURCE_SIMULATOR`); see `src/chunking/CLAUDE.md`.
- **Processing**: `ProcessorForS3` (`DOCKER_PROCESSOR`), `ProcessorForDynamoDb` (`DOCKER_PROCESSOR_DYNAMODB`).
- **Post-processing**: `DeferredDeleteHandler` and `DeferredDeleteHandlerForS3` (`DEFERRED_DELETE`),
  `ChunkPopulationReader` (`CHUNK_POPULATION_READER`).
- **Tables and caches** (`src/dynamodb/`, `src/person-cache/`): `STATISTICS_TABLE`, `ATOMIC_COUNTER`, `OCC_FLAG`,
  `PERSON_CURRENT_STATE_TABLE`, `PERSON_HISTORY_TABLE`, `MOCK_TARGET_PERSON_TABLE`, `BULK_PURGER`,
  `INTEGRATION_RUN_PRUNER`, `PERSON_CACHE`.
- **Docker entry points**: `docker/chunker.ts` (`DOCKER_CHUNKER`), `docker/processor.ts` (`DOCKER_PROCESSOR`),
  `docker/post-processor.ts` (`DOCKER_POST_PROCESSOR`).
- **Utilities** (`src/`): `DesiredCount` (`DESIRED_COUNT`), `Alarm` (`ALARM`), `ApiErrorTracking`
  (`API_ERROR_TRACKING`).

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

`docker/chunker.ts`, `docker/processor.ts` and `docker/post-processor.ts` are harnesses too: run one with npx and its
`.env` group to validate a phase before an ECS deployment. The processor and post-processor are routers that branch on
`PREVIOUS_STORAGE_TYPE`. What each does, the image, and the routers' rules: `docker/CLAUDE.md`.

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
