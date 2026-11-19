# integration-huron-person-fargate: Serverless Pipeline & Infrastructure

AWS CDK infrastructure and the ECS Fargate implementation of the three-phase streaming delta sync (chunking,
processing, post-processing) on Lambda, ECS Fargate, S3, SQS and DynamoDB, plus the landscape's management dashboard
(`dashboard/`, with its own `CLAUDE.md`). An independently versioned npm package in its own repository, composed with
its siblings (especially `integration-core`) through dependencies, not workspace-level source control.

Folder-level CLAUDE.md files hold more detail: `lib/`, `docker/`, `src/`, `src/chunking/`, `src/processing/`,
`src/post-processing/`, `src/runner/`, `dashboard/`. Reference docs are in [docs/](docs/).

**Workspace setup**: the shared skills are the integration-skills repository cloned as `<workspace root>/.claude`, and
the workspace-wide instructions are its `CLAUDE.md`. How to install them and get VS Code, Copilot CLI or Claude Code
to load them (including `--add-dir ..` with `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1`): the `README.md`,
"Shared Skills Setup" and "Workspace Instructions" (and `<workspace root>/.claude/README.md`).

## Verify before you rely on an abstraction

Before using an unfamiliar abstraction, find its definition and its consumers, read its tests, state what you think it
does, and only then implement; if you can't verify it, say what you don't know and ask rather than guess (skill
`verify-abstractions-before-implementation`; the user can say "skip verification and proceed" or "use inference for
this"). High-risk here: control-flow directives (`__arrayFieldOperations`, `__metadata`, behavioral flags), update
semantics (append vs replace, merge vs overwrite), JWT and credential resolution, phase transitions and queue
messages, Docker entry-point environment loading and error propagation, CDK stack dependencies and resource
references.

**Pitfall: two lifecycles of `Config`.** The Runner's `Config` object never reaches the running tasks; values CDK reads
from config are baked into task definitions at deploy time, and the tasks load their own config from the secret. A
runtime override set on the wrong instance compiles, runs and does nothing. A per-run value must travel the run's
messages: `ApiChunkerEvent` -> the chunker queue message -> `TaskParameters` -> each chained message
(`ChunkerQueue.sendNextChunkingMessage()`) -> the run's Flags (`docker/chunker.ts`) -> the processors, as
`termsEndpoint`, `startedBy`, `processorScalingCapacity` and the addons do. The story behind this rule (the
`config.preLoadedMaps` dead code): [docs/MOCK_LANDSCAPES.md](docs/MOCK_LANDSCAPES.md#history-the-configpreloadedmaps-dead-code).

## Architecture: Three-Phase Pipeline

### Phase 1: Chunking

Streams large JSON payloads from the source APIs (or an S3 file drop) into NDJSON chunk files for parallel processing.
Components: the chunker subscriber Lambda (`src/chunking/ChunkerSubscriber.ts`, CDK
`lib/services/chunker/ChunkerSubscribingLambda.ts`), the chunker ECS service (`lib/services/chunker/ChunkerService.ts`,
entry point `docker/chunker.ts`), `ChunkFromAPI`, `ChunkFromS3`, `BigJsonFetch`, `BigJsonFile`, `PersonArrayWrapper`.
All chunking modules use `TestEnvironment` from `integration-core`; harness prefixes `CHUNK_FROM_API_*`,
`CHUNK_FROM_S3_*`, `CHUNKER_SERVICE_*`. How chunker tasks chain and avoid races: [docs/CHUNKING_ARCHITECTURE.md](docs/CHUNKING_ARCHITECTURE.md).

**How a run ends** (details, consequences and the terminal-error bug story:
[docs/CHUNKING_END_AND_FAILURE.md](docs/CHUNKING_END_AND_FAILURE.md)):
- **End of records**: only an **empty** batch marks the end, because the source can return a partial batch
  mid-population. `ECS.chunkerTaskDefinition.stopAtFirstPartial` (default `false`) restores stopping at the first
  partial batch. `ChunkResult.endOfRecordsDetected` and metadata `partialOrEmptyChunkEncountered` (persisted, so never
  renamed) both mean "end of source records detected". Counts are never inferred from `recordCount`.
- **Safety cutoff**: `ECS.chunkerTaskDefinition.maxTotalRecords` (default 500,000, `DEFAULT_MAX_TOTAL_RECORDS` in
  `docker/chunkTypes.ts`; `<= 0` disables) stops a source that never returns an empty batch, by failing the run.
- **Failed runs never reach deactivations**: a run with a TERMINAL_ERROR record (statistics table, or
  `_terminal_error.json` in S3 mode) blocks the post-processor trigger and skips deletion handling in both modes. Read
  and write it only with `StatisticsTable.writeTerminalError()`/`readTerminalError()`.

**How a run starts** (paths, grants, refusals, timings and the reasoning: [docs/RUN_START.md](docs/RUN_START.md)):
- **The schedule** (`ScheduledRun`, `src/chunking/ScheduledRun.ts`) sends a full sync (`person-full`) with stored
  hashes trusted (`bulkReset: false`, `trustPreviousStorage: true`): it fetches everyone, sends only people whose hash
  changed, and deactivates people missing from the source. The two previous-state flags only act together; set them
  with `ScheduledRun.previousStateFlags(bulkReset)`. The context's `BULK_RESET` / `TRUST_PREVIOUS_STORAGE` are only
  the chunker task's environment fallback.
- **Who started it** (`startedBy`, `src/chunking/StartedBy.ts`): `{ by: 'dashboard', principal? }`, `{ by: 'schedule' }`
  or `{ by: 'runner' }`, carried along the run's messages into Flags and the run summary.
- **The run counters** (`resetRunCounters()`, `src/chunking/RunCounters.ts`: the chunker's offset and chunk-ordinal
  counters, plus the source simulator's in a mock landscape) are reset by the chunker subscriber's `handler()` for an
  event that starts a run, and by the manual Runner. **Never in `handleApiEvent()`**: the Runner's queue seeding calls
  it for every seeded message.
- **Parallel start** (`ApiChunkerEvent.parallelStart` 1-20, `src/chunking/ParallelStart.ts`, shared with the Runner's
  queue seeding): seeds N messages, waits for the chunker's scale-in alarm to leave `ALARM`, then sets the desired
  count. Refusals are **returned** as 400 results, not thrown. The subscriber Lambda has a 600-second timeout and
  **no automatic retry** (a retried start would reset counters and seed again into a run underway).
- **A run's own processor maximum** (`processorScalingCapacityOverride` 1-20, `src/chunking/ProcessorScalingCapacity.ts`):
  every run start sets the processor service's live `MaxCapacity` to the override or back to the context's
  `ECS.processorService.maxScalingCapacity`, which never changes at run time. An override that can't be set fails the
  start; failing to restore only warns. Recorded as `Flags.processorScalingCapacity`. Cluster and processor service
  names live in `src/Utils.ts` (the task image compiles `src/` without `lib/`).
- **Start from scratch** (mock landscapes only, `ApiChunkerEvent.resetState`, or a reset-only
  `MockLandscapeResetEvent`): see "Mock Landscapes" below.

### Phase 2: Processing

Parallel ECS Fargate tasks apply mapping and push changes to the target. Entry point `docker/processor.ts` (a router
to `ProcessorForS3` / `ProcessorForDynamoDb` in `src/processing/`); harness prefix `DOCKER_PROCESSOR_*`.

**Addons** (`src/processing/addon/`): integration-huron-person's `personRecordProcessor` hook, wired into the
processors so per-person async logic (e.g. `ORG_COMPARISON_LOGGING`) can run during a live sync. Which addons run is a
per-run Flags field (`personRecordProcessorAddons`, from the Runner's `PERSON_RECORD_PROCESSOR_ADDONS`), never a task
definition variable. Design, the Flags path, the shared log table and how to add one:
[src/processing/addon/README.md](src/processing/addon/README.md).

### Phase 3: Post-processing

Consolidates results and handles deletions. `AbstractPostProcessor` (`src/post-processing/`, template method) with
`PostProcessorForS3` (file consolidation, baseline merging) and `PostProcessorForDynamoDB` (deletions only; state is
already in the tables); router `docker/post-processor.ts`. **Both storage modes need the post-processor service**, for
`DeferredDeleteHandler`. `AbstractPostProcessor.main()` writes the run's `RUN_SUMMARY` in its `finally` block. Harness
prefixes `DEFERRED_DELETE_HANDLER_*`, `STATISTICS_TABLE_*`, `DOCKER_POST_PROCESSOR_*`.

**Soft-deleted persons are marked in the baseline** (`src/post-processing/DeletedHashMarker.ts`): a successful soft
delete rewrites the person's baseline hash with a `DELETED:` prefix (DynamoDB mode also sets `deletedAt`), so deletion
detection skips them and a returning person is classified UPDATED and reactivated. How it works, and the DynamoDB
matching bug fixed 2026-11-17: [docs/STORAGE_MODES.md](docs/STORAGE_MODES.md#soft-deleted-persons-in-the-baseline).

**Taking a new integration-core build**: update `node_modules/integration-core`'s `integrity` in `package-lock.json`
and run a plain `npm install`, which changes nothing else (see "Publishing the package" in integration-core's
`CLAUDE.md`). The task image installs it from the lockfile, so push the image to each landscape afterwards.

## Storage Modes

`context.PREVIOUS_STORAGE_TYPE` selects the mode at deploy time (`'dynamodb'` → DynamoDB mode; `'s3'`, `'file'`,
`'database'` → S3 mode); CDK bakes it into every task definition, and the factories (`HashStorageResetFactory`,
`PersonCacheFactory`, `MetadataFactory`) switch on the resulting `config.storage.type`. Chunk NDJSON files and the
person cache stay in S3 in both modes; DynamoDB mode keeps delta state in the PersonCurrentState and PersonHistory
tables and run metadata in the statistics table.

- **Switching**: change `PREVIOUS_STORAGE_TYPE` in the context and update the stack; the config secret's storage block
  is derived from it (`resolveSecretStorage()` in `lib/Secrets.ts`), so the integration config files need no change.
- **Keep `PREVIOUS_STORAGE_TYPE` set in every `context.*.json`**: when missing, CDK's fallbacks disagree.
- **Switching to S3 mode deletes** the PersonCurrentState and PersonHistory tables (DESTROY); switching back creates
  them empty. An old S3 baseline (`delta-storage/`) is left alone, except by a mock landscape's reset.

Table schemas, S3 layout, the abstractions, switching detail and when to choose which mode:
[docs/STORAGE_MODES.md](docs/STORAGE_MODES.md).

**The statistics table** (`src/dynamodb/StatisticsTable.ts`, PK syncRunId, SK `eventType`) holds each run's METADATA,
FLAGS, TERMINAL_ERROR, per-chunk statistics, claim records ([docs/CONCURRENCY_CONTROL.md](docs/CONCURRENCY_CONTROL.md)),
RUN marker and RUN_SUMMARY; `src/reporting/RunReporting.ts` is its read side for the dashboard. Rules:
- **Change the statistics table's GSIs one per deploy**: CloudFormation refuses an update that adds or removes more
  than one.
- Runs are listed through the sparse `run-list-index` (the RUN marker). A table updated in place to add it needs the
  harness task `backfill-runs` before the dashboard that reads it is deployed; a table created with it doesn't.
- A chunk's `totalRecords` counts only the people whose record changed, so it can't measure a run's progress.

Event types, the run list, which landscapes have the index, chunk-statistics semantics, the summary and `RunReporting`:
[docs/STATISTICS_TABLE.md](docs/STATISTICS_TABLE.md).

## Mock Landscapes

A landscape named `^mock\d*$` (`isMockLandscape()`, `src/Utils.ts`) is a stack dedicated to mocked runs: its target is
always the **target simulator** (`src/processing/TargetSimulator.ts`, a Lambda behind an API Gateway HTTP API storing
persons in the mock target table), injected into its config secret by CDK, and its source may be the **source
simulator** (`src/chunking/fetch/SourceSimulator.ts`, a Lambda Function URL). Mock-ness is a deploy-time property of
the stack: there is no per-run mock flag, and the pipeline's runtime code has no mock awareness. Why a landscape, what
differs in a mock stack, the reset in full: [docs/MOCK_LANDSCAPES.md](docs/MOCK_LANDSCAPES.md); the end-to-end
runbook: [docs/MOCK_INTEGRATION_TESTING.md](docs/MOCK_INTEGRATION_TESTING.md).

Rules:
- **Source simulation entails target simulation.** `lib/AppConstruct.ts` refuses to synthesize a stack with
  `LAMBDA.sourceSimulator.enabled` in a non-mock landscape, and `Runner.ts` aborts when `RUNNER_SOURCE_SIMULATOR=true`
  targets one. There is no supported way to run simulated source data against the real Huron target.
- **Hybrid runs** (real source -> mock target) are supported but consume the real, depleting source API.
- **Resets only in a mock landscape**: `BulkPurger` refuses to purge a non-mock landscape unless `force: true`; only a
  mock landscape's chunker subscriber gets the reset's grants. The reset (`src/chunking/MockLandscapeReset.ts`, shared
  by the Runner's `RUNNER_MOCK_TARGET_RESET_STATE`, Bulk sync's Start from scratch and the dashboard's Reset landscape)
  empties the mock target and statistics tables (so Run history) and the delta baseline of the stack's storage mode.
- **The reset record borrows the atomic counter table** (`counter_name` `not-a-counter:mock-landscape-reset`,
  `NOT_A_COUNTER_PREFIX` in `src/dynamodb/AtomicCounter.ts`): anything that clears the counters must leave such items
  alone.
- **After any change to the target simulator's front door**, probe the deployed endpoint:
  `curl -g "<endpoint>/api/v2/persons?pagination[offset]=0"` must return the simulator's own `401 {"errors":[...]}`,
  not an AWS-generated 400. Why (unencoded `[ ]` in Huron query strings, and why not a Function URL), and the contract
  quirks it must honor: [docs/TARGET_SIMULATOR.md](docs/TARGET_SIMULATOR.md).
- **The source simulator** serves `/` (stateful, depleting), `/person?buid=` (stateless), `/terms` and `/health`;
  per-run settings travel as query parameters of the run's `fetchPath` (`simulatorFetchPath()`), never by changing the
  Lambda's environment. Simulated terms reach the processors through `Flags.termsEndpoint`. Endpoints, API keys and
  settings: [docs/SOURCE_SIMULATOR.md](docs/SOURCE_SIMULATOR.md).

**Deploying one**: `context/context.mock.json` (+ git-ignored `context/mock.env`) -> `./context/swap.sh mock`, then
`cdk deploy`. Add `context.mock1.json` etc. for more.

## CDK Infrastructure

- **Context**: `context/context*.json` and `context/IContext.ts` define each landscape's infrastructure parameters
  (image URIs, task sizes, buckets, ...). No context names a VPC.
- **Stacks**: `bin/App.ts` builds the pipeline stack (`lib/Stack.ts`, orchestrating Lambda, ECS, S3, SQS), the
  landscape's dashboard stack, and the shared network stack the pipeline depends on.
- **Deploy and tear down** (from the repo root): `npm run deploy` (every stack) / `deploy-fargate` / `deploy-dashboard`,
  all through `bin/deploy.sh`, which synthesizes once and writes each deployed stack's template to
  `cdk.out/synth.output.<fargate|dashboard>.yaml` from the cloud assembly (`cdk synth --app cdk.out -e`); `npm run teardown` / `teardown-fargate` / `teardown-dashboard`
  through `bin/teardown.sh`. More: [docs/CDK_DEPLOYMENT.md](docs/CDK_DEPLOYMENT.md).

### Shared Network Stack

Every landscape's pipeline runs in the network of one stack, `huron-integration-network` (`lib/NetworkStack.ts`, one
per account and region; layout in code, `NetworkLayout`): VPC, public and private subnets in zones `a` and `b`, the
internet gateway, route tables, one NAT gateway. It is termination-protected and every resource is retained. It is the
pipeline stack's dependency: `cdk deploy` of a pipeline deploys it first; `cdk destroy` of a pipeline never selects it.
Pipelines read its IDs from SSM parameters (`/huron-integration-network/...`, `NetworkParameters`) resolved at each
deploy; the ECS services run in the private subnets.

- **Never let a landscape create or look up a VPC** (no `Vpc.fromLookup`, no VPC in `EcsInfrastructure`, no
  CloudFormation exports of the network). A landscape that created its own VPC once took every other landscape's
  network down with its teardown.
- Check what depends on the network before any teardown.
- Tests: `test/NetworkStack.test.ts`, `test/EcsInfrastructureNetwork.test.ts`.

Why it exists (the 2026-11-12 incident), and how a network was imported into it (`cdk import` details):
[docs/SHARED_NETWORK.md](docs/SHARED_NETWORK.md).

### Landscape Dashboard Stack

The dashboard (`dashboard/`, see `dashboard/CLAUDE.md`) is a second stack in this CDK app: `bin/App.ts` calls
`createDashboardStack()` when the context has a `DASHBOARD` section, creating `${DASHBOARD.STACK_ID}-${landscape}`
(`hp-dashboard-<landscape>`, at `<landscape>.person.huron.cssnprd.warhen.work`; the default landscape's stack also
redirects `person.huron.cssnprd.warhen.work` to it). It shares no CloudFormation exports with the pipeline stack: it
reads the landscape's config secret, tables and chunks bucket by the same deterministic names
(`integrationSecretName()`, `dashboard/lib/LandscapeResources.ts`). It reports on runs in-process over
`src/reporting/RunReporting.ts`, and lists landscapes from SSM parameters under `/hp-dashboard/landscapes`.

- After a deploy that includes the dashboard, `bin/deploy.sh` deletes the Lambda@Edge versions earlier deploys left
  behind; `bin/teardown.sh` destroys the dashboard in three steps so its edge-lambda stack goes in one run (both in
  [dashboard/docs/DEPLOYMENT.md](dashboard/docs/DEPLOYMENT.md)).
- Every synth synthesizes both stacks, so it needs AWS credentials (the Shibboleth construct and the default
  landscape's redirect read secrets, the certificate and Route 53 at synth), and `build`/`synth` first generate the
  dashboard's `TemplateConstants.ts`.

**Keep the dashboard out of the task image.** The fargate tasks take no part in the dashboard, but share this repo's
`package.json`, lockfile and `context/IContext.ts`. So:
- Dashboard/CDK-synth-only packages are `optionalDependencies` (`shibboleth-sp-at-edge`, from the
  `simple-npm-package-bucket` registry URL, `express`, `mustache`, and the synth-time `@aws-sdk/client-acm` /
  `@aws-sdk/client-route-53`); `docker/Dockerfile` runs `npm ci --omit=optional`, which never fetches them. Add new
  dashboard-only packages with `npm install --save-optional`.
- The Dockerfile compiles with `npx tsc`, not `npm run build` (which builds the dashboard templates).
- `context/IContext.ts` types `DASHBOARD` loosely and imports nothing from `dashboard/`; the precise `DashboardSettings`
  is applied in `dashboard/lib/DashboardStacks.ts`. Code under `src/`, `docker/` and `context/` must never import from
  `dashboard/` or `shibboleth-sp-at-edge`.
- **When to run `npm run docker-build`**: the image is built only from `package.json`, `package-lock.json`,
  `tsconfig.json`, `src/`, `context/*.ts` and `docker/` (never `dashboard/`). Check what a change touches with
  `git diff --name-only`:
  - `tsconfig.json`, `src/`, `context/*.ts` or `docker/` (including a dashboard change that edits
    `context/IContext.ts`): run `docker-build`, not only `tsc`/`jest`, since the image's `npx tsc` compiles them.
  - Only `package.json` / `package-lock.json` (e.g. adding a dashboard-only optional package): skip `docker-build` and
    run the image's install in a scratch folder instead (about 30 seconds, against about 6 minutes). Never run it in the
    repo: `npm ci` deletes `node_modules`.
    ```bash
    mkdir /tmp/npm-ci-check && cp package.json package-lock.json /tmp/npm-ci-check/ && cd /tmp/npm-ci-check
    npm ci --omit=optional --ignore-scripts --no-audit --no-fund
    ```
    It must succeed (that proves the lockfile matches `package.json`), and `node_modules` must not contain the optional
    packages. Compare versions the pipeline uses (e.g. `node_modules/@aws-sdk/core/package.json`) with the repo's to
    confirm nothing shared moved. Don't use `--dry-run`: it lists the whole tree, optional packages included.
  - Only `dashboard/`, `lib/`, `bin/`, `test/` or docs: can't affect the image, so skip both.

## Harnesses and Environment

Harnesses follow the workspace pattern (skills `testenvironment-pattern`, `harness-initialization-patterns`,
`environment-variable-organization`): `TestEnvironment` imported from `integration-core` (no local wrapper), an
`if (require.main === module)` block naming its variables, a prefixed group in the git-ignored `.env`, and placeholders
in `example-env.md`. Run one with VS Code's "Debug current file" or `npx ts-node <file>` (e.g. `src/runner/Runner.ts`,
documented in `src/runner/CLAUDE.md`). Catalog, `.env` layout, Docker entry points and the history of the pattern here:
[docs/HARNESSES.md](docs/HARNESSES.md). Tests: [docs/TESTING.md](docs/TESTING.md).

**Adding a harness**: create the module under `src/` or `docker/`, import `TestEnvironment` from `integration-core`,
add the `require.main` block, add its variables to `.env` under its own group and placeholders to `example-env.md`,
and list it in the README's test harnesses.

**Integrating with CDK**: a harness tests one component; CDK Lambda/ECS configuration should feed the same variables
(Secrets Manager or the task definition), resolved through the ConfigManager chain (skill
`configmanager-chain-pattern`).

**Docker image**: test the entry-point harness locally with `.env`, build with `docker/Dockerfile`, push to ECR
(`npm run docker-push`), and test in an ECS task before a full pipeline run.

## Dependencies

`integration-core` (TestEnvironment, base classes, utilities) and `integration-huron-person`; AWS Lambda, ECS Fargate,
S3, SQS, DynamoDB, Secrets Manager; CDK; ts-node, Docker, TypeScript.

## Common Issues

- **Environment variable not found**: check `.env` has the prefixed variable in the right harness group, that
  `TestEnvironment('PREFIX')` matches the group's prefix, and that `getVar()` is called without the prefix.
- **Docker build failures**: check `docker/Dockerfile` references the right source files and that `.env` matches the
  entry point's expectations; run the harness locally before building.
- **ECS task failures**: check the task role's S3, SQS, Secrets Manager (and DynamoDB) permissions, the context's
  resource ARNs, and that the ConfigManager chain loads credentials (task definition → Secrets Manager → environment).
