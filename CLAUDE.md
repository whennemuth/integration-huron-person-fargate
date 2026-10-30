# integration-huron-person-fargate: Serverless Pipeline & Infrastructure

## Project Purpose
AWS CDK infrastructure and serverless implementation of the three-phase streaming delta synchronization pipeline. Implements chunking, processing, and post-processing phases using Lambda, ECS Fargate, S3, and SQS.

## Repository Relationship Model

This project is an independently versioned npm package with its own source repository.

It composes with sibling repositories through dependency relationships (especially `integration-core`) rather than workspace-level source control.

## Shared Skills Repository

Cross-repository agent skills are maintained in a separate repository (https://github.com/whennemuth/integration-skills) that is cloned as the `.claude` directory of the workspace root, so skills live at `<workspace root>/.claude/skills/`. This is a native skill location that both VS Code (GitHub Copilot) and Claude Code discover without any custom settings.

- **Install**: from the workspace root, run `git clone https://github.com/whennemuth/integration-skills.git .claude`.
- **VS Code**: include the workspace root itself (`"path": "."`) as a root folder in your `.code-workspace` file. VS Code discovers `.claude/skills/` in every workspace root folder and makes those skills available across the whole workspace.
- **Claude Code**: start Claude Code in the workspace root. When starting in a package repository instead, add the workspace root with `--add-dir` (e.g. `claude --add-dir ..` from the repository root, or `/add-dir` in a running session).

See `<workspace root>/.claude/README.md` for full setup details.

Core-only and core+person+fargate workspace examples are documented in this repository's `README.md`.

## Workspace Instructions

Workspace-wide agent instructions live in `<workspace root>/.claude/CLAUDE.md`, versioned in the integration-skills repository alongside the shared skills. That file holds the workspace architecture overview, pointers to the shared-pattern skills and project CLAUDE.md files, change and commit preferences, and the task verification protocol (build, test, and completion reporting requirements for all code implementation tasks) that apply to all integration projects (core, huron-person, fargate, dashboard, file-drop, etc.).

**Discovery**: Claude Code and GitHub Copilot CLI load it automatically when started in the workspace root. VS Code Copilot Chat loads it when the workspace root (`"path": "."`) is a root folder of the `.code-workspace` file and the `chat.useClaudeMdFile` setting is enabled. When starting Claude Code in a package repository with `--add-dir` pointing at the workspace root, also set `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` so the workspace instructions are loaded.

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

A runner decorator (`MockTargetRunnerDecorator`, since replaced by `MockLandscapeRunnerDecorator`) mutated `config.preLoadedMaps = {orgMap:false, ...}` believing this would prevent organization/state/country lookups from calling the real Huron API in mock target mode:
- **Assumption**: Setting this field on the Runner's `Config` object would reach the running processor task
- **Reality**: `config.preLoadedMaps` is only ever read by CDK at deploy time (`ProcessorTaskDefinition.ts`) to bake `STATIC_MAP_USAGE` into the ECS task's environment variables - from an entirely different `Config` object loaded from disk, not the one the Runner mutated at invocation time
- **Result**: The override compiled and ran with zero errors, logged nothing wrong, and had absolutely no effect - mock-mode runs could still trigger real organization API calls

This was fixed by forcing `StaticMapUsage` at the point it's actually consumed - inside the processor, immediately after `flags.useMockTarget` is read from chunk metadata (the same channel already used to select the mock data target) - rather than trying to inject it upstream through a Config object that never reaches the running task. The lesson: a runtime override that "looks right" and produces no errors can still be a no-op if it mutates the wrong instance of a config object that exists in two different lifecycles (deploy-time vs invocation-time).

(Since superseded: mock mode is now a deploy-time property of a mock landscape - see "Mock Landscapes" below - whose target simulator answers the same calls the real Huron API would, so no static-map override is needed at all; the runtime override `resolveStaticMapUsage()` was removed.)

### Source Simulator

The source simulator (`src/chunking/fetch/SourceSimulator.ts`) is a Lambda Function URL that provides mock API responses for full 3-phase integration testing without the real API's 30-minute cooldown constraint.

**Endpoints** (path-based routing):
- `/` (default): mock person data, using a stateful depletion model (see repo memory `source-simulator-semantics.md` for allocator semantics)
- `/person?buid=U0000042`: one person, mirroring the real person API. Stateless (the depletion counter is untouched) and idempotent: `U0000042` is index 41, the same payload `/` produces for that person. A BUID beyond `MOCK_TOTAL_POPULATION` returns `{ response: [] }`; a missing or malformed one (not `U` + digits) returns 400. It must be selected by a `buid` query parameter, because that is what makes the chunker do a single, non-paged request (`BigJsonFetch.isNotBatchable()`). With `RUNNER_SINGLE_PERSON_BUID` set, `SourceSimulatorRunnerDecorator` points the source here.
- `/terms`: mock current-terms data (6 static terms: 2 current, 2 past, 2 future) - required because `DataMapperOrg.isCurrentSemester()` needs terms data to filter student semesters, and running with `RUNNER_CHUNKING_ONLY=false` exercises this path.
- **API keys**: every endpoint accepts any of the three source keys in the config secret (people, person, terms), and rejects anything else. Callers pick keys by URL and the simulator's URLs can't satisfy all of them while hybrid runs need the secret's real source URLs: the chunker never matches the simulator's `/person` to the configured person URL, so it sends the people key there, whereas a client given the simulator's URLs directly (integration-huron-person's `SyncPerson.ts`) sends the person key, and the processor always sends the terms key. See "API keys" in the `SourceSimulator.ts` header.
- **How simulated terms reach the processor** (only for simulated-source runs): terms are fetched by the processor tasks (integration-huron-person's `getDataMapper()`/`DataMapperOrg`), from the config they load from the secret - so the Runner cannot redirect them by changing its own config object (it used to, with no effect: mock runs fetched terms from the real terms API). Instead `SourceSimulatorRunnerDecorator` returns a `termsEndpoint` with the source endpoint, which travels like `personRecordProcessorCustomizations`: chunker SQS message (`ApiChunkerEvent.termsEndpoint`) -> `TaskParameters` -> re-forwarded by `ChunkerQueue.sendNextChunkingMessage()` -> `docker/chunker.ts` writes it to the run's Flags (`Flags.termsEndpoint`) -> `ProcessorForS3`/`ProcessorForDynamoDb` apply it via `applyTermsEndpoint()` (ProcessorCommon.ts: replaces `dataSource.terms` baseUrl/fetchPath only; the apiKey stays). Hybrid runs (real source -> mock target) and scheduled runs never set it, so they keep the configured (real) terms endpoint. The field itself is generic, not mock-specific.
- Optional `nonCurrentTermRate` query parameter (0.0-1.0, default 0.0), on `/` and `/person`: the proportion of simulated students assigned a non-current term, for testing semester-filtering logic. Which students get one is decided by a hash of the person's index (not `Math.random()`), so a person's payload is identical on every request and from either endpoint

**Safety enforcement - source simulation entails target simulation**: the source simulator may only exist in a mock landscape (see "Mock Landscapes" below), whose target is always mocked. This is enforced twice: `lib/AppConstruct.ts` refuses to synthesize a stack with `LAMBDA.sourceSimulator.enabled` in a non-mock landscape (so real stacks have no simulator Lambda at all), and `Runner.ts` aborts when `RUNNER_SOURCE_SIMULATOR=true` targets a non-mock landscape. There is no supported way to run simulated source data against the real Huron target API.

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

**End-of-records detection (`stopAtFirstPartial`)**: The source API can return a "partial"
batch (0 < length < `recordCount`) mid-population (a source-side bug), so by default only an
**empty** batch marks the end. `IContext` `ECS.chunkerTaskDefinition.stopAtFirstPartial`
(optional, default `false`) -> `ChunkerTaskDefinition` env `STOP_AT_FIRST_PARTIAL` ->
`ChunkFromAPI.runChunking()` -> `BigJsonFetchConfig.stopAtFirstPartial` ->
`BuCdmPeopleDataSourceBatch` (integration-huron-person). `true` restores the legacy
stop-at-first-partial behavior. Consequences:
- `ChunkResult.endOfRecordsDetected` (renamed from `partialChunkEncountered`) / metadata
  `partialOrEmptyChunkEncountered` (persisted, so NOT renamed) mean "end of source records
  detected" (empty batch, or partial only if `stopAtFirstPartial`). All consumers (chain-stop,
  `isAlreadyFinished()`, post-processor-trigger gate, `ProcessorServiceBooster` final check) need exactly
  that signal, so they are unchanged.
- Counts are never inferred from `recordCount`: `totalRecords` = sum of actual response lengths,
  `chunkCount` = chunk files actually written.
- A task whose `iterationLimit` is met on a partial batch does NOT signal the end; the next
  chained task then gets an empty first batch, which `BigJsonFetch` treats as a normal end
  (warn, not error).
- With `iterationLimit=0` (e.g. `maxScalingCapacity=1`), the default mode relies on the source
  eventually returning an empty batch (true of the depleting real API and SourceSimulator).
  The safety cutoff below bounds the damage if it never does.

**Glitched-source safety cutoff (`maxTotalRecords`)**: If the source never returns an empty
batch, chunking would never end - an endless fetch loop when `iterationLimit=0`, or endless
chunker message/task chaining when `iterationLimit>0`. `IContext`
`ECS.chunkerTaskDefinition.maxTotalRecords` (optional, default `DEFAULT_MAX_TOTAL_RECORDS` =
500,000 in `docker/chunkTypes.ts`; the real population is ~145,000; `<= 0` disables) ->
env `MAX_TOTAL_RECORDS` -> `ChunkFromAPI.runChunking()` (also reads the run's accumulated
`totalRecords` via `MetadataBroker.getRunningTotalRecords()`) -> `BigJsonFetchConfig.maxTotalRecords`
/ `runTotalRecordsAtStart` -> `BigJsonFetch.enforceMaxTotalRecords()`, checked before the first
fetch (fail fast) and on every batch before it is written. Exceeding it throws, surfacing through
the existing terminal-error path (`runFailed` metadata + `markRunFailed`), so later chained tasks
bail on `isTerminalErrorEncountered()` and the S3-mode post-processor is blocked. Precision caveat:
`totalRecords` only includes tasks that have finished, so the cutoff can be overshot by up to
the in-flight records of concurrently running tasks.

**Failed runs never reach deactivations**: A run is "failed" when its TERMINAL_ERROR record exists
(DynamoDB: PK=syncRunId, SK=`TERMINAL_ERROR` in the statistics table;
S3: `_terminal_error.json`). Gates: `PostProcessorSubscriber` (S3 mode, blocks post-processor),
`ProcessorForDynamoDb`'s last-processor check (does not trigger the post-processor), and
`PostProcessorForS3`/`PostProcessorForDynamoDB.runDeferredDeletes()` (skip deletion handling, both modes).
Note: until this was fixed, `MetadataForDynamoDb.readTerminalError()` read the METADATA record
(and `StatisticsTable.readMetadata()` strips `eventType`), so `terminalErrorExists()` was always
`false` in DynamoDB mode - chunker fast-fail on a failed run silently never happened there. Unit
tests mocked `readMetadata` to return `eventType: 'TERMINAL_ERROR'`, hiding it. It now uses
dedicated `StatisticsTable.writeTerminalError()`/`readTerminalError()`.

### Phase 2: Processing
**Purpose**: Apply data mapping and validation transformations in parallel ECS Fargate tasks

**Components**:
- Docker entrypoint: `docker/processor.ts`
- Task orchestration via CDK
- Batch processing of chunks

**Harness Prefix**: DOCKER_PROCESSOR_*

### Phase 3: Post-processing  
**Purpose**: Consolidate processing results and write final delta storage

**Components**:
- DeferredDeleteHandler (soft delete coordination)
- StatisticsTable (merge metrics)
- Implementation classes: `src/post-processing/AbstractPostProcessor.ts` (base class), `src/post-processing/PostProcessorForS3.ts`, `src/post-processing/PostProcessorForDynamoDB.ts`
- Docker entrypoint: `docker/post-processor.ts` (router)

**Architecture**: Template Method pattern with abstract base class
- `AbstractPostProcessor` (src/post-processing/): Base class with shared logic (getTaskParameters, processDeferredDeletes, main template method)
- `PostProcessorForS3` (src/post-processing/): S3-specific implementation (file consolidation, baseline merging)
- `PostProcessorForDynamoDB` (src/post-processing/): DynamoDB-specific implementation (minimal/no-op merge, state already in tables)
- `post-processor.ts` (docker/): Router that detects storage mode and delegates to appropriate implementation

**Critical Design Note**: BOTH storage modes require the post-processor service
- S3 mode: File consolidation + deletion handling
- DynamoDB mode: Deletion handling only (no file consolidation needed)

**Soft-deleted persons are marked in the baseline** (`src/post-processing/DeletedHashMarker.ts`): both
baselines (PersonCurrentStateTable, `previous-input.ndjson`) retain every person ever seen, so a
person absent from the source would otherwise be selected for deletion on every full sync. After a
successful soft-delete, `onSoftDeleteSuccess` rewrites that person's baseline hash with a
`DELETED:` prefix (DynamoDB mode also sets `deletedAt`). Deletion detection skips marked entries,
and a marked hash never matches a fresh source hash, so a person who reappears is classified
UPDATED and reactivated (via `__active`); the processor's write then clears the marker.

**Harness Prefixes**:
- DEFERRED_DELETE_HANDLER_*, STATISTICS_TABLE_*
- DOCKER_POST_PROCESSOR_*

## Storage Modes: DynamoDB vs S3

The pipeline supports two storage modes for delta state and metadata, selected at deploy time by
`context.PREVIOUS_STORAGE_TYPE` (CDK bakes it into every task definition as the `PREVIOUS_STORAGE_TYPE`
env var, which the Docker routers branch on; the runtime factories below then switch on the resulting
`config.storage.type`):
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
   - Event types: `METADATA`, `FLAGS`, `TERMINAL_ERROR`, `STATISTICS-chunk-*`, `ERROR:*`,
     `CHUNK_STATUS_*`, `PROCESSOR_BOOST_CLAIM`, `POST_PROCESSOR_TRIGGER_CLAIM`, `RUN`, `RUN_SUMMARY`
     (no aggregate `STATISTICS` item is ever written: processors always pass a chunkId)
   - `PROCESSOR_BOOST_CLAIM` / `POST_PROCESSOR_TRIGGER_CLAIM` are `OCCFlag`-guarded claim records ensuring exactly
     one chunker task runs the processor scaling check, and exactly one processor task triggers the post-processor
     (see `docs/CONCURRENCY_CONTROL.md`); the boost claim is written in S3 storage mode too
   - Replaces `_metadata.json` and `_flags.json` files from S3 mode
   - `RUN` marker item (one per run, attribute `distinctRunId` = PK) is the only item in the sparse,
     KEYS_ONLY `distinct-run-index` GSI, so `getUniqueIntegrationTimestamps()` ("select distinct
     syncRunId") scans one tiny item per run instead of every STATISTICS/chunk item. It is written
     idempotently by `writeFlags()`, `addToMetadataTotals()` (every chunker task, both storage modes)
     and `writeTerminalError()`; runs that predate the index need the harness task `backfill-runs`
   - `STATISTICS-chunk-*` items carry the chunk's `IntegrationResult` counts (success/failure/skipped,
     added/updated/removed) as well as its error counts.
   - `RUN_SUMMARY` (one per run) is written by `AbstractPostProcessor.main()`'s `finally` block, in both
     storage modes: METADATA plus the summed chunk statistics plus the deletion outcome
     (`DeletionOutcome`, returned by `runDeferredDeletes()`: COMPLETED with counts, SKIPPED with a
     reason, or FAILED). Status: `SUCCEEDED`, `FAILED` (deletes skipped because the run carries a
     terminal error) or `POST_PROCESSING_FAILED`. Writing it never fails post-processing. Runs that
     never reach the post-processor have no summary; `src/reporting/RunReporting.ts` reports them as
     FAILED/INCOMPLETE from METADATA, FLAGS and the terminal error (S3 files in S3 mode, located via
     METADATA's `chunkDirectory`).
   - `src/reporting/RunReporting.ts` is the read side (no transport): `listRuns`, `getRunReport`,
     `getRunChunkStatistics`, paged `getRunErrors` and `getRunChanges` (person-history changes,
     DynamoDB mode only; S3 mode keeps no per-person change record per run). Page cursors are
     base64url `LastEvaluatedKey`s, rejected unless their key attributes match the requested run
     (and change type).

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
   
   **Person target** (Strategy Pattern):
   - `AbstractPersonTarget` interface: Abstracts the target system the cache is built from
   - `PersonTargetReal`: Fetches from the target API (config.dataTarget) via ListPeople - the real
     Huron API, or in a mock landscape the target simulator (see "Mock Landscapes" below)
   - Design: Dependency injection enables testing without environment coupling

3. **Metadata** (src/chunking/metadata/)
   - `AbstractMetadata` with 19 methods (5 static, 14 abstract)
   - `MetadataForS3` (file-based implementation)
   - `MetadataForDynamoDb` (StatisticsTable with `eventType` field)
   - `MetadataFactory` switches on `config.storage.type`

### Mock Landscapes

**The signal**: any landscape whose name matches `^mock\d*$` (`mock`, `mock1`, `mock2`, ...) is a
*mock landscape* - a stack dedicated to mocked runs. `isMockLandscape()` (src/Utils.ts) is the single
test for it, used only by CDK (with the lower-cased `TAGS.Landscape`), the Runner and `BulkPurger`.
There is no per-run "mock" flag, and **the pipeline's runtime code (chunker, processor, post-processor) has
no mock awareness at all**: mock-ness is a deploy-time property of the stack.

**Why a landscape, not isolation tables**: nearly every resource name already carries the
landscape (`${STACK_ID}-<kind>-${landscape}` tables, queues, cluster, Lambdas, config secret, chunks
bucket, the stack itself), so a dedicated mock stack isolates everything by construction - including
the S3 delta storage and the chunker's baseline scan, which the old per-run isolation tables never
covered. A mock stack uses the **standard** tables; they simply never hold real data.

**The target simulator** (`src/processing/TargetSimulator.ts`, CDK:
`lib/services/processor/TargetSimulator.ts`): a Lambda behind an API Gateway HTTP API that impersonates
the subset of the Huron person API the pipeline uses - token request, `POST`/`PATCH`/`GET /api/v2/persons[/{hrn}]`
with filters/paging - storing persons in the mock target table. CDK injects its endpoint (and a generated
external token, from a separate `.../target-simulator/token/<landscape>` secret, via a CloudFormation
dynamic reference) into the mock stack's integration config secret as
`dataTarget.endpointConfig.baseUrl`/`externalToken`. So the ECS tasks run the ordinary
`HuronPersonDataTarget`/`ReadPerson`/`ListPeople` code against it, unaware it is a fake.
- Contract quirks it must honor (see the file header and `test/TargetSimulator.test.ts`, which drives
  the real client classes against it): "not found" is `200 {data: []}`; `pagination[offset]` is a
  page index and a page past the end is `200 {data: []}`; `filter[...includeInactive...]` is a flag;
  hrns (`hrn:hrs:persons:<sourceIdentifier>`) are derived, so `PATCH /{hrn}` needs no index.
- DELETE (`PATCH {active:false}`) is a soft delete (`deactivated`/`deactivatedAt` + `data.active=false`);
  records are never removed, matching Huron's soft-delete-only behavior.
- The simulator reads its token from its own secret, never the config secret, keeping the CloudFormation
  dependency one-way (config secret -> simulator).
- **Why an HTTP API, not a Lambda Function URL**: the Huron client deliberately sends UNENCODED `[ ]`
  in query strings (`pagination[offset]=0`, `filter[0!sourceIdentifier!and]=...` - see
  integration-huron-person `UrlSerializer.ts`; the real Huron API rejects the encoded form). A Function
  URL rejects such requests with `400 {"message":null}` before ever invoking the function (the first
  mock run failed this way). The local contract test cannot catch front-door strictness like this
  (Node's HTTP server accepts anything), so after any change to the simulator's front door, probe the
  deployed endpoint: `curl -g "<endpoint>/api/v2/persons?pagination[offset]=0"` must return the
  simulator's own `401 {"errors":[...]}`, not an AWS-generated 400.
- `MockPersonDataTarget`, `DataTargetFlags.useMockTarget` and Upsert's mock branch have been removed
  from integration-huron-person; the target simulator is the only mock target.

**What differs in a mock stack**:
- Exactly one extra table: `${STACK_ID}-mock-target-person-${landscape}` (MockTargetPersonTable),
  backing the target simulator. Non-mock stacks have no `-mock-` tables at all.
- The target simulator (always) and source simulator (if enabled - synthesis fails if enabled
  in a non-mock landscape). Neither exists in any other landscape.
- Its integration config (`integration-huron-person/config/config.mock.json`) sets
  `dataTarget.endpointConfig.baseUrl` to an unroutable placeholder, which CDK replaces with the
  simulator's URL in the deployed secret (so a local harness pointed at the file still fails loudly
  instead of reaching a real target), and `preLoadedMaps` like preview (`orgMap` false; state/country
  maps come from CSV files, not Huron).

**Runner**: when `LANDSCAPE` is a mock landscape, `Runner.ts` wraps the runner in
`MockLandscapeRunnerDecorator`, which verifies the mock stack is deployed (mock target table exists),
optionally resets its run state (`RUNNER_MOCK_TARGET_RESET_STATE=true` truncates the mock target and
statistics tables via `BulkPurger`, then clears the delta baseline of the stack's storage mode: the person
current-state and history tables in DynamoDB mode, or the `delta-storage/` files of the chunks bucket in
S3 mode; the mode is detected from whether the person current-state table exists), and warns about
"hybrid" runs (real source API -> mock target) - which remain supported (`RUNNER_SOURCE_SIMULATOR=false`
against a mock landscape), but consume from the real, depleting source API like any other landscape.
`BulkPurger` refuses to purge a non-mock landscape unless `force: true`.

**Deploying one**: `context/context.mock.json` (+ git-ignored `context/mock.env`) -> `./context/swap.sh mock`,
then `cdk deploy`. Add `context.mock1.json` etc. for more mock stacks.

**Switching Between Modes**: set `PREVIOUS_STORAGE_TYPE` (`"s3"` or `"dynamodb"`) in the active
`context/context.json` and update the stack. That one value is the switch: the integration config
files (`integration-huron-person/config/config.*.json`) need no change, because the config secret's
storage block is derived from it (`resolveSecretStorage()` in `lib/Secrets.ts`):
- **Type**: `context.PREVIOUS_STORAGE_TYPE`, else the integration config's `storage.type`, else `'s3'`
  (`DEFAULT_STORAGE_TYPE`, the last resort). So the secret always agrees with the mode the tasks run in.
- **Settings**: the integration config's `storage.config` is kept only if it belongs to the resolved
  type (e.g. an S3 `keyPrefix`); settings for another type are dropped, not mixed in. Settings the
  stack determines come from the context: the landscape's chunks bucket (`bucketName`, S3 mode) or
  the person current-state/history table names and region (DynamoDB mode).
- Environment variables (`.env`, loaded by `bin/App.ts`) don't set storage in the secret.

Keep `PREVIOUS_STORAGE_TYPE` set in every `context.*.json`: when it is missing, CDK's fallbacks
disagree (`lib/DynamoDB.ts` creates the DynamoDB tables, `TaskDefinitions` takes the type from the
locally loaded integration config). Switching a stack to S3 mode deletes its PersonCurrentState and
PersonHistory tables (removal policy DESTROY); switching back creates them empty. Switching does not
touch an S3 baseline (`delta-storage/` in the chunks bucket) left by an earlier S3-mode period; in a mock
landscape the runner's reset deletes it.

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
- Docker entries: chunker.ts, processor.ts, post-processor.ts
- Chunking: BigJsonFile.ts, BigJsonFetch.ts, ChunkFromAPI.ts, ChunkFromS3.ts, PersonArrayWrapper.ts
- Post-processing: DeferredDeleteHandler.ts, StatisticsTable.ts, PersonCache.ts
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

### post-processor.ts
**Purpose**: Router entry point for result consolidation

**Architecture**: Template Method pattern with mode-specific implementations
- AbstractPostProcessor (src/post-processing/): Base class with shared logic (getTaskParameters, processDeferredDeletes, main)
- PostProcessorForS3 (src/post-processing/): S3-specific file consolidation and baseline merging
- PostProcessorForDynamoDB (src/post-processing/): Minimal/no-op merge (state already in DynamoDB tables)
- post-processor.ts (docker/): Router that detects storage mode and delegates

**Critical**: Both modes require post-processor service for DeferredDeleteHandler

**Mode Detection**: Checks for DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME

**Harness Prefix**: DOCKER_POST_PROCESSOR

## Test Harnesses (14+ total)

**Categories**:
- **Orchestration** (1): Runner (Manual chunking service invocation)
- **Chunking Pipeline** (5): ChunkFromAPI, ChunkFromS3, BigJsonFetch, BigJsonFile, PersonArrayWrapper
- **Post-processing Pipeline** (3): DeferredDeleteHandler, PersonCache, StatisticsTable
- **Docker Entry Points** (3): chunker.ts, processor.ts, post-processor.ts
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

### Landscape Dashboard Stack
The dashboard (`dashboard/`, see `dashboard/CLAUDE.md` and `docs/pan-landscape-data-exchange-plan.md`)
is a second stack in this CDK app: `bin/App.ts` calls `createDashboardStack()` when the context has a
`DASHBOARD` section, creating `${DASHBOARD.STACK_ID}-${landscape}` (`hp-dashboard-<landscape>`, at
`<landscape>.person.huron.cssnprd.warhen.work`; the default landscape's stack also redirects
`person.huron.cssnprd.warhen.work` to it). It shares no CloudFormation exports with the pipeline
stack: it reads the landscape's config secret, tables and chunks bucket by the same deterministic
names (`integrationSecretName()`, `dashboard/lib/LandscapeResources.ts`). `npm run deploy` deploys
every stack (`--all`); `deploy-fargate` / `deploy-dashboard` deploy one. All run `bin/deploy.sh`,
which synthesizes once and dumps each deployed stack's template to
`cdk.out/synth.output.<fargate|dashboard>.yaml` from the cloud assembly (`cdk synth --app cdk.out -e`).
`teardown` / `teardown-fargate` / `teardown-dashboard` mirror them. Every synth still synthesizes both, so it needs AWS
credentials (the Shibboleth construct reads its secret, the certificate and Route 53 at synth, and
the default landscape's redirect checks its certificate and record the same way) and
`build`/`synth` first generate the dashboard's `TemplateConstants.ts`. The dashboard reports on
the landscape's runs in-process (`/api/runs*`, `/api/capabilities`, over `src/reporting/RunReporting.ts`)
and lists every registered landscape (`/api/landscapes`) from SSM parameters under
`/hp-dashboard/landscapes`, one owned by each dashboard stack (see `dashboard/CLAUDE.md`).

**Keep the dashboard out of the task image.** The fargate tasks take no part in the dashboard,
but share this repo's `package.json`, lockfile and `context/IContext.ts`. So:
- Dashboard/CDK-synth-only packages are `optionalDependencies` (`shibboleth-sp-at-edge`, from the
  `simple-npm-package-bucket` registry URL, `express`, `mustache`, and the synth-time
  `@aws-sdk/client-acm` / `@aws-sdk/client-route-53`); `docker/Dockerfile` runs
  `npm ci --omit=optional`, which never fetches them. Add new dashboard-only packages with
  `npm install --save-optional`.
- The Dockerfile compiles with `npx tsc`, not `npm run build` (which builds the dashboard templates).
- `context/IContext.ts` types `DASHBOARD` loosely and imports nothing from `dashboard/`; the
  precise `DashboardSettings` is applied in `dashboard/lib/DashboardStacks.ts`. Code under
  `src/`, `docker/` and `context/` must never import from `dashboard/` or `shibboleth-sp-at-edge`.
- **When to run `npm run docker-build`**: the image is built only from `package.json`,
  `package-lock.json`, `tsconfig.json`, `src/`, `context/*.ts` and `docker/` (it never copies
  `dashboard/`). Check what a change touches with `git diff --name-only`:
  - `tsconfig.json`, `src/`, `context/*.ts` or `docker/` (including a dashboard change that edits
    `context/IContext.ts`): run `docker-build`, not only `tsc`/`jest`, since the image's
    `npx tsc` compiles them.
  - Only `package.json` / `package-lock.json` (e.g. adding a dashboard-only optional package):
    skip `docker-build` and run the image's install in a scratch folder instead (about 30
    seconds, against about 6 minutes). Never run it in the repo: `npm ci` deletes `node_modules`.
    ```bash
    mkdir /tmp/npm-ci-check && cp package.json package-lock.json /tmp/npm-ci-check/ && cd /tmp/npm-ci-check
    npm ci --omit=optional --ignore-scripts --no-audit --no-fund
    ```
    It must succeed (that proves the lockfile matches `package.json`), and `node_modules` must
    not contain the optional packages. Compare versions the pipeline uses (e.g.
    `node_modules/@aws-sdk/core/package.json`) with the repo's to confirm nothing shared moved.
    Don't use `--dry-run` for this: it lists the whole tree, optional packages included, so it
    can't show what `--omit=optional` leaves out.
  - Only `dashboard/`, `lib/`, `bin/`, `test/` or docs: can't affect the image, so skip both.

## personRecordProcessor Customization Framework

**Location**: `src/processing/custom/` (concrete implementations live under `impl/`)

Wires `integration-huron-person`'s `personRecordProcessor` hook (see that repo's
`CLAUDE.md`/`src/data-mapper/CLAUDE.md`) into the processor tasks, letting custom per-person
async logic (e.g. outlier logging/analysis) run during a live sync without touching
`HuronPersonIntegration` itself. See `src/processing/custom/README.md` for the framework's
design (dependency injection, multi-customization composition) and how to add a new
implementation.

**Files**: `AbstractCustomPersonProcessor.ts` (base class + `Customization` enum),
`CustomizationParser.ts` (parses the comma-delimited config string - see below),
`PersonRecordProcessorComposite.ts` (combines multiple active customizations),
`PersonRecordProcessorFactory.ts` (registry/switchboard), `impl/OrgComparisonLogging.ts`
(first concrete customization: for students - mapped `title === 'Student'`, set in
`integration-huron-person`'s `src/data-mapper/DataMapperTitle.ts` from the *same*
`orgAssignments.personType` that drove the `organization`/`secondaryUnit` assignment, a more
accurate signal than an independent raw `studentInfo` check - whose primary/secondary org HRNs
differ, logs `{ personid, primaryOrg, secondaryOrg }`).

**Specifying customizations - key name or numeric value, either works**: `Customization` is a
plain (numeric) TS enum with no explicit initializers, so config always identifies a
customization by its enum **key name** (e.g. `'ORG_COMPARISON_LOGGING'`) - the underlying number
is an implementation detail. `CustomizationParser.ts`'s `parseCustomizations()` additionally
accepts the raw numeric value as a string (e.g. `'0'`) as an equivalent alternative, since some
callers may find a stable numeric value more convenient to configure than a name. A token is
tried as a number first (matched via the enum's reverse mapping) only if it looks like an
integer; otherwise it's looked up by key name. Mixing both forms for the same customization in
one comma-delimited list (e.g. `'ORG_COMPARISON_LOGGING,0'`) de-duplicates to a single instance
rather than double-counting it. See `test/CustomizationParser.test.ts` for the full set of
parsing permutations this covers.

**Shared log table**: `src/dynamodb/PersonRecordProcessorLogTable.ts` + CDK table in
`lib/DynamoDB.ts` (`personRecordProcessorLogTable`, single table, no mock-mode variant unlike
most other tables here). Schema: PK=`customization` (groups entries by which customization wrote
them), SK=`sortKey` (`${isoTimestamp}#${personid}`, chronologically browsable and collision-free
per person), plus a generic `data` JSON attribute whose shape is defined entirely by the writing
customization. This design lets any number of future customizations share one table with zero
CDK/schema changes - only `AbstractCustomPersonProcessor`, `Customization`, and
`PersonRecordProcessorFactory` need updating to add one. The log table name IS still baked into
the task definition as `PERSON_RECORD_PROCESSOR_LOG_TABLE_NAME` (it's an infrastructure resource
name, not a per-run behavioral toggle, so CDK-time baking is appropriate here - unlike *which*
customization runs, see below).

**Selecting which customization(s) run - Flags, not task-definition env vars**: Which
customization is active is deliberately **not** an `IContext`/task-definition environment
variable (that would require a full stack redeploy to change). Instead it follows the same
runtime-configuration precedent as `flags.bulkReset`: a `personRecordProcessorCustomizations`
field (comma-delimited `Customization` keys) flows through
`src/chunking/metadata/IMetadataStorage.ts`'s `Flags` type, which is written to S3/DynamoDB
*before* chunking starts and read back by each processor task - so it can be changed per-run
without redeploying anything. The full pipeline:
1. **Manual invocation** (`src/runner/Runner.ts`, "FOR MANUAL INVOCATION ONLY"): reads the
   `PERSON_RECORD_PROCESSOR_CUSTOMIZATIONS` env var via `RunnerTypes.ts`'s `extractEnvironment()`
   into `RunnerEnv.personRecordProcessorCustomizations`.
2. Each runner (`RunnerForSingleMessage.ts`, `RunnerForSinglePerson.ts`, and
   `RunnerForQueueSeeding.ts` via `QueueSeeder.ts`) copies that value into the `ApiChunkerEvent`
   it sends (`ChunkerSubscriber.ts`'s `ApiChunkerEvent.personRecordProcessorCustomizations`).
3. **Scheduled production invocation** (`lib/services/chunker/ChunkerService.ts`'s
   `createApiChunkingSchedule()`): the EventBridge schedule's static `ScheduleTargetInput` also
   includes a `personRecordProcessorCustomizations` field (currently left `undefined` as a
   placeholder pending a decision on how to source a real value for scheduled runs - see the
   `// TODO LATER` comment there) - the consuming side described below already handles it
   whenever a value is present.
4. `ChunkerApiSubscriber.ts`'s `handleApiEvent()` extracts it from the incoming event and forwards
   it into the `TaskParameters` SQS message it sends to the chunker queue.
5. `ChunkFromAPI.ts` parses it off the queue message into `TaskParameters.personRecordProcessorCustomizations`
   and exposes it via `getPersonRecordProcessorCustomizations()`; `ChunkerQueue.ts`'s
   `sendNextChunkingMessage()` re-forwards it into subsequent messages for parallel chunking.
6. `docker/chunker.ts` reads that getter and includes it when calling `metadataManager.writeFlags(...)`
   - so it's persisted to the run's Flags record before any processor task starts.
7. `ProcessorForS3.ts`/`ProcessorForDynamoDb.ts` read it back via
   `flags.personRecordProcessorCustomizations` (from the same `readFlagsFromChunkKey()` call
   already used for `flags.bulkReset` etc.) and pass it to `personRecordProcessorFactory()`.

**Wiring/DI**: `docker/processor.ts` (the S3-vs-DynamoDB router) accepts an optional
`personRecordProcessor: PersonRecordProcessor` parameter and threads it through to whichever of
`ProcessorForS3.ts`/`ProcessorForDynamoDb.ts`'s `main()` it routes to - both of which now also
accept that same optional parameter. If provided (e.g. by a test, or a future caller that already
knows which processor to use), it's used as-is; otherwise each `main()` falls back to resolving
it from its own chunk's Flags via `personRecordProcessorFactory()`. This exists so the
factory-invocation logic isn't duplicated across processor variants (DRY), while still allowing
Flags-based resolution to work correctly - the router itself can't resolve from Flags because
those are chunk-specific and not known until deep inside each `main()`.

**Adding a new customization**:
1. Add a value to the `Customization` enum in `AbstractCustomPersonProcessor.ts`.
2. Create a new file under `impl/` extending `AbstractPersonRecordProcessor`, setting
   `customization` and implementing `processRecord`; call
   `this.logEntry(this.customization, personid, data)` to persist findings.
3. Register it in the `switch` in `PersonRecordProcessorFactory.ts`.
4. Set `PERSON_RECORD_PROCESSOR_CUSTOMIZATIONS` (env var read by `Runner.ts`, or the
   `personRecordProcessorCustomizations` Flags field directly) to the enum *key* to activate it -
   comma-delimit multiple keys to run several customizations at once (combined automatically via
   `PersonRecordProcessorComposite`).

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

