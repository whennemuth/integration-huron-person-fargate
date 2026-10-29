# TODO: configuration sprawl and conflicting settings

**Status: postponed.** Recorded 2026-10-19 so that work can resume later without repeating the research.
File and line references are as of commit `b1e6b72` on the `post-processor` branch (2026-10-17). Re-check them before relying on exact lines.

## The problem

A fargate run's settings come from many sources:
- the local `.env` (Runner and harness env vars, also loaded at CDK synth by `bin/App.ts`)
- `context/context.json`
- the integration config file named by `context.HURON_PERSON_CONFIG.configPath` (e.g. `../integration-huron-person/config/config.mock.json`)
- a second config file named by `.env` `HURON_PERSON_CONFIG_PATH`
- the deployed config secret (`${STACK_ID}/integration/_config/${landscape}`)
- task-definition and Lambda env vars baked at synth
- values the Runner passes downstream in SQS messages and run Flags

Each execution context (Runner, local harness, Lambda, ECS task, EventBridge schedule) chooses among these with its own precedence and fallbacks. So settings can conflict, and huron-person's `ConfigValidator` cannot see it: it only checks one merged `Config`.

### Rejected approach (don't repeat it)

The first idea was one central `ConfigConflictChecker` class, called at each kick-off point (Runner, manual component launches, EventBridge schedule). It would have:
1. gathered every source, including the deployed secret, task-definition env vars and Lambda env vars
2. worked out which contexts a launch reaches
3. rebuilt each context's resolved configuration
4. aborted on conflicts

It was rejected because it **mirrors every context's precedence rules in a second place**. That duplicates logic, and the copy drifts from the real code. It treats the symptom, not the cause. (The rejected plan is in `C:\Users\wrh\.claude\plans\pasted-content-id-3e0c-it-has-cozy-swing.md` if that file still exists.)

Term defined by the user, worth keeping:
- **Resolved configuration:** all configuration values together, after a per-variable selection process that is specific to the execution context. The selection decides which source of each variable takes precedence.

## Diagnosis: why the sprawl happens

1. **Settings have several homes.**
   - storage type: context, config file, env and secret
   - delete type: config file and the `PERSON_DELETE_TYPE` env var
   - static maps: `preLoadedMaps` and `STATIC_MAP_USAGE`
   - landscape: context, `.env` and the config file

   Any value that can be set in two places can be set two ways.
2. **Each context re-resolves.**
   - The Runner, docker harnesses, ECS tasks and CDK each build config with their own `ConfigManager` chain order.
   - `deepMerge` lets the first source win, so chain order *is* precedence, and it differs from file to file.
   - Precedence is spread across the code instead of defined anywhere.
3. **One deployment is described by two or three files**: `context.json`, the integration config it points to, and a second path in `.env`.
   - CDK splits their contents into two channels: the secret and task-definition env vars.
   - Tasks merge the two back together at runtime.
   - `context/swap.sh` has to juggle all of this, which is itself a symptom.
4. **Per-run parameters share channels with configuration.**
   - `bulkReset`, `trustPreviousStorage`, population and endpoint overrides travel in SQS messages and Flags, but can also come from env or config.
   - Defaults differ by place: `trustPreviousStorage` is `false` in some and `true` in others.
5. **Dead knobs.** Context `BULK_RESET` and config `dataTarget.dryRun` look like settings, but nothing reads them.

Some of this grew naturally: simulators and mock landscapes for testing, the S3-to-DynamoDB migration running both modes side by side, and harness convenience. The result is still a design smell.

## Recommended approaches

1. **One home per setting.** Each setting belongs to exactly one category and can come only from that category's source:

   | Category | Examples | Only source |
   |---|---|---|
   | Deployment config (fixed per stack) | storage mode, delete type, static maps, scaling, per-component dry run | one per-landscape file |
   | Secrets | API keys, target credentials | Secrets Manager |
   | Run parameters (per run) | bulkReset, trust, population, simulator endpoint, customizations | the run request (SQS message → Flags), with one set of defaults |
   | Local dev overrides | harness conveniences | an explicit local-only override layer |

   This is the core fix. A value that cannot be specified twice cannot conflict.
2. **Resolve once, then pass the result along.**
   - Compute the full deployment config at synth and put it all in the secret.
   - Compute the full run parameters at kick-off and put them all in the Flags.
   - Downstream contexts read those values and never override them.
   - Task-definition env vars shrink to pointers (`SECRET_ARN`, landscape).
   - No per-context precedence is left, so nothing needs mirroring and nothing drifts.
3. **Derive instead of configure.**
   - Bucket, table and secret names are already computed from `STACK_ID` + landscape. Extend that.
   - The Runner should read queue URLs, the secret and the storage mode from the deployed stack (stack outputs or the secret) instead of `.env` (`CHUNKER_QUEUE_URL`, `CHUNKS_BUCKET`, …).
   - Two values derived by one shared function cannot disagree.
4. **One file per landscape.** Merge `context.<landscape>.json` and its integration config into one document, or let the context own the reference so `.env` names no config path at all. Then `swap.sh` switches one thing.
5. **Validate where values are created.**
   - Use a typed schema for deployment config and one for run parameters.
   - Cross-field rules (e.g. "S3 mode needs a bucket") live in the schema.
   - Validate at synth (deployment config) and at kick-off (run parameters); parse where values are consumed.
   - This doesn't duplicate any precedence logic.
6. **Remove dead and duplicate knobs.** Cheap, with immediate value:
   - delete context `BULK_RESET` and `dataTarget.dryRun`
   - pick one of `PERSON_DELETE_TYPE` / `personDeleteType`
   - pick one of `STATIC_MAP_USAGE` / `preLoadedMaps`
7. **Print provenance at startup.** Each kick-off prints its resolved values and the source of each. It prevents nothing, but it makes surprises obvious, and that is all most local overrides need.

After 1–4, very little is left to check. If a gate is still wanted, it reduces to "the local override layer disagrees with the deployed stack": a few lines, not a mirror of every context.

## Suggested order

1. **Classify.** Build a setting-by-setting table: setting, category, current homes, proposed single home. Use the inventory below as the starting point. Do this first; every later step is easier with the table in hand, and steps 2–3 may remove most of the risk on their own.
2. **Remove dead and duplicate knobs** (approach 6). Small changes, each independently shippable.
3. **Resolve once:** the secret carries the full deployment config, Flags carry the full run parameters, task-definition env vars become pointers.
4. **The Runner derives from landscape + the deployed stack** instead of `.env`.
5. **Add schema validation and provenance printing.**

### Constraints

- **Dependency direction:** integration-core ← integration-huron-person ← this project. Upstream packages must not name their importers, in code, comments, docs or commits.
- **huron-person's `ConfigManager` stays.** It also serves huron-person's own standalone launches (`SyncPerson`, `SyncPersonBatch`, …). This project would add its own small loader built on the categories above.
- **The user changed the scope.** They first included huron-person's `SyncPerson`/`SyncPersonBatch` launch configurations, then dropped them: the work is **fargate only**.
- **Runtime checks are enough for deploy issues.** Validating how CloudFormation synthesizes resources is out of scope. The user accepts that synth-only issues surface at runtime, where the messages tell them to update the stack.
- **Mirror existing checks; don't remove them.** Existing scattered checks stay where they are; any central list repeats them.
- **Errors abort; acknowledged errors still print.** Error-level conflicts should abort. A per-rule acknowledgement (e.g. `CONFIG_CONFLICTS_ALLOW=<rule-id>`) may downgrade one to a warning, but the warning is still printed.
- **Workspace conventions:** problem-first commit messages (`integration-workspace-skills/.claude/skills/commit-message-conventions/SKILL.md`). Pause for the user before any S3 tgz publish and at each commit.

## Research inventory

### How huron-person assembles config

- **`ConfigManager`** (`integration-huron-person/src/config/ConfigManager.ts`)
  - A singleton with a lazy, queued chain: `fromPartial`, `fromFileSystem`, `fromEnvironment`, `fromJsonString` (`HURON_PERSON_CONFIG_JSON`), `fromSecretManager`.
  - `deepMerge(source, target)` (~:380) lets **target (already accumulated) win**. So the earliest source in the chain wins; later sources only fill missing keys. `null` and `''` from an earlier source also win.
  - Each source swallows its own errors with a `console.warn`. A missing file, a bad secret, or an invalid `PREVIOUS_STORAGE_TYPE` inside `fromEnvironment` is **never fatal**.
  - `ignoreValidation` is fixed by the first `getInstance()` call. `isValidated` is cached across different `executionMode`s until `reset()`.
  - Validation is **skipped entirely** when a `config` object is injected: `new HuronPersonIntegration({config})`, `SyncPersonBatch.main({config})`.
- **`ConfigFromEnvironment`** (`src/config/ConfigFromEnvironment.ts`) maps env vars to config fields:

  | Env vars | Config field |
  |---|---|
  | `DATASOURCE_ENDPOINTCONFIG_{PERSON,PEOPLE,CURRENT_TERMS}_{BASE_URL,API_KEY,PATH}` | `dataSource.{person,people,terms}` endpoint, key, path |
  | `DATASOURCE_PEOPLE_FETCH_BATCHED` | `dataSource.people.fetchBatched` |
  | `DATASOURCE_PEOPLE_FETCH_SCHEDULE_{ENABLED,CRON_EXPRESSION}` | `dataSource.people.fetchSchedule` |
  | `S3_BUCKET`, `S3_KEY` | `dataSource.people.bucketName`, `key` |
  | `DATATARGET_ENDPOINTCONFIG_{BASE_URL,USERNAME,PASSWORD,LOGIN_SVC_PATH,LOGIN_USERID,EXTERNAL_TOKEN,AUTH_METHOD}` | `dataTarget.endpointConfig.*` |
  | `DATATARGET_PERSONS_PATH`, `DATATARGET_ORGANIZATIONS_PATH` | `dataTarget` paths |
  | `CLIENT_ID`, `BATCH_SIZE`, `TIMEOUT` | `integration.*` |
  | `CACHE_ENABLED`, `CACHE_PATH` | `cache.*` |

  Storage env vars (`PREVIOUS_STORAGE_TYPE`, `DYNAMODB_PERSON_*_TABLE_NAME`, `REGION`) are read only with `fromEnvironment({storage: true})`. **This project never passes that option.**
- **`ConfigValidator`** checks:
  - required fields per `executionMode`
  - the auth-method branch
  - storage shape per type
  - S3 CSV configs
  - URL parsing

  It does **no** cross-source or cross-field conflict checks. It is not exported, and there is no hook for extra validation.
- **Env vars read outside config:**
  - `DRY_RUN`: `PersonDataTarget.ts:84`, `DeltaStrategyFactory.ts:142`
  - `CACHE_ENABLED` / `CACHE_PATH`: `Cache.ts`, where env silently beats `config.cache`
- **`SyncPeople.ts:172-179`** forces `personDeleteType` to SOFT. Its warning wrongly says cleanup is enabled; cleanup only happens for HARD (:227).

### Where this project overrides or shadows the config file

**CDK synth**

- **`bin/App.ts`**
  - `dotenv.config()` (:11), then `fromEnvironment().fromFileSystem(HURON_PERSON_CONFIG_PATH || getLocalConfig())`. `.env` overrides the file.
  - **This file is not context `configPath`.**
  - `getLocalConfig()` (`src/Utils.ts:70-85`) returns `<workspaceFolder>/../integration-huron-person/config.json` (missing the `config/` subfolder), and only when a `workspaceFolder=` argument is passed.
  - This `config` drives:
    - the storage-type fallback and `keyPrefix` → `SHARED_DELTA_STORAGE_DIR` (`lib/TaskDefinitions.ts:47-88`)
    - `STATIC_MAP_USAGE` (`ProcessorTaskDefinition.ts:75`)
    - `PERSON_DELETE_TYPE` (`TaskDefinitions.ts:147` → `PostProcessorTaskDefinition.ts:79`)
- **`lib/Secrets.ts`**
  - `buildSecretValue()`: `fromEnvironment()` first (so `.env` overrides the file in the secret, except storage), then the resolved storage, then the `configPath` file.
  - `resolveSecretStorage()`: storage type = context `PREVIOUS_STORAGE_TYPE` → config `storage.type` → `'s3'`. The config's `storage.config` is kept only if its type matches.
  - S3 `bucketName` is forced to the chunks bucket. DynamoDB table names come from context, and region is filled with `??=`.
  - In mock landscapes, `dataTargetOverride` replaces the target `baseUrl`, `externalToken` and `authMethod` with the target simulator's.
- **`lib/TaskDefinitions.ts:62, :78`** destructure `storageCfg` without a guard. If the type is `s3` and App's config has no `storage` block, synth throws a TypeError.
- **`lib/services/chunker/ChunkerService.ts:71-74`** calls `ConfigManager.getInstance().fromFileSystem(configPath)` **without `reset()`**. It reuses operations already queued by `buildSecretValue` (env first). The schedule input hard-codes `PersonDelta`, `bulkReset: false`, and `personRecordProcessorCustomizations: undefined`.
- **Task-definition env vars:**

  | Task | Env vars |
  |---|---|
  | Chunker | `REGION`, `LANDSCAPE`, `STACK_ID`, `CHUNKS_BUCKET`, `PREVIOUS_STORAGE_TYPE`, `ITEMS_PER_CHUNK`, `DRY_RUN`, `BOOST_PROCESSOR`, `STOP_AT_FIRST_PARTIAL`, `MAX_TOTAL_RECORDS`, `MAX_SCALING_CAPACITY`, `RETRY_STRATEGY`, `SHARED_DELTA_STORAGE_DIR`, table names, secret `HURON_PERSON_CONFIG_JSON` |
  | Processor | `STATIC_MAP_USAGE`, `PREVIOUS_STORAGE_TYPE`, `SHARED_DELTA_STORAGE_DIR`, table names, `RETRY_STRATEGY`, `DRY_RUN` |
  | Post-processor | `PERSON_DELETE_TYPE`, `DRY_RUN`, `PREVIOUS_STORAGE_TYPE`, `SHARED_DELTA_STORAGE_DIR`, table names, `INPUT_BUCKET` |

- **Lambda env vars:**
  - `DRY_RUN` (context `DRY_RUN.lambda.*`) on every subscriber.
  - `PostProcessorSubscribingLambda.ts:89` sets `PREVIOUS_STORAGE_TYPE: context.PREVIOUS_STORAGE_TYPE || 's3'`.
  - Source simulator: `MOCK_*`. Target simulator: `TABLE_NAME`, token secret.
- **`context/IContext.ts` fields that shadow config fields:**
  - `PREVIOUS_STORAGE_TYPE` shadows `storage.type`
  - `BULK_RESET`: unused
  - `TRUST_PREVIOUS_STORAGE`: used only for the schedule
  - `DRY_RUN.*` vs `dataTarget.dryRun`: the latter is unused
  - `S3.chunksBucket` vs S3 `bucketName`
  - `TAGS.Landscape` vs `config.landscape`: the latter is never read here
  - `REGION` vs storage region
  - `ITEMS_PER_CHUNK`: fetch batch size, separate from `integration.batchSize`

**ECS runtime**

- **Config load chain** (`src/Utils.ts:95-112 getConfig`, `ProcessorForS3.ts:89-98`, `ProcessorForDynamoDb.ts:149-158`, `AbstractDeferredDeleteHandler.ts:363-379`, `PersonCacheForS3.ts:52-68`): `fromJsonString` → `fromSecretManager(SECRET_ARN)` → `fromEnvironment` → `fromFileSystem`. The secret wins.
- **`docker/chunker.ts`**
  - The SQS message beats env.
  - With no baseline, `bulkReset` is forced to true (:366-378).
  - `trustPreviousStorage` = message flag `|| false` (:380).
  - Writes Flags, including `dryRun` from `DRY_RUN`.
  - The person cache type comes from the secret's `storage.type`.
- **`ChunkFromAPI.ts`**
  - Local env task parameters (:135-218); SQS message fields (:226-270).
  - `MAX_SCALING_CAPACITY=1` forces offset and iteration limit to 0 (:447-464).
  - `bulkReset`/`trust` are OR-ed with overrides (:470-492).
- **`ChunkConfigOverride.ts`**: the message `baseUrl`/`fetchPath` replace `dataSource.people`. The people `apiKey` is swapped for the person key only when the URL matches the person endpoint.
- **`ProcessorForS3.buildChunkConfig`**
  - Replaces the data source, `integration.clientId` (`deltas/...`), `integratedDeltaClientId` (`SHARED_DELTA_STORAGE_DIR`), and storage (`type: 's3'`, chunks bucket, `keyPrefix: ''`).
  - `STATIC_MAP_USAGE` beats `preLoadedMaps`.
- **`ProcessorForDynamoDb`**: replaces storage completely from env (tables, `REGION`, the GSI, `syncRunId`); `clientId` becomes `'dynamodb-processor'`.
- **`ProcessorCommon.resolveCommonFlags`**: `bulkReset = flags ?? BULK_RESET env` (no task definition sets that env var); `trust = flags ?? true`; population `?? PersonFull`. `applyTermsEndpoint` replaces the terms URL and path.
- **Post-processor**: the `PERSON_DELETE_TYPE` env var beats the secret's `personDeleteType`, and `NONE` skips deletes. It uses `SHARED_DELTA_STORAGE_DIR` and ignores `keyPrefix`.

**Runner** (`src/runner/`)

- **`RunnerTypes.extractEnvironment`** reads: `SECRET_ARN`, `HURON_PERSON_CONFIG_PATH`, `CHUNKER_QUEUE_URL`, `POPULATION_TYPE`, `DATASOURCE_ENDPOINTCONFIG_ITERATION_LIMIT`, `SINGLE_PERSON_BUID`, `REGION`, `STACK_ID`, `LANDSCAPE`, `BULK_RESET`, `TRUST_PREVIOUS_STORAGE`, `SKIP_ATOMIC_COUNTER_RESET`, `MESSAGES_TO_PREPOPULATE`, `DESIRED_COUNT`, `ECS_CLUSTER_NAME`, `ECS_SERVICE_NAME`, `MESSAGING_ONLY`, `CHUNKING_ONLY`, `SOURCE_SIMULATOR`, `SOURCE_SIMULATOR_MOCK_*`, `MOCK_TARGET_RESET_STATE`, `PERSON_RECORD_PROCESSOR_CUSTOMIZATIONS`.
- **`setTestEnvironment`** copies `RUNNER_*` over these. `LANDSCAPE` is not in its list.
- **`AbstractRunner.loadConfiguration`** order: env → local file → JSON → secret. This is the **reverse of ECS**. Locally resolved endpoints go into the SQS message and override the secret's endpoint in the chunker.
- **Decorators**
  - `SourceSimulatorRunnerDecorator` rewrites the simulator Lambda's env vars (they drift from context until the next deploy) and passes `termsEndpoint` through Flags.
  - `ServiceToggler` flips subscriber Lambdas' `DRY_RUN` (drift until the next deploy).
  - `MockLandscapeRunnerDecorator` detects the storage mode by whether the person current-state table exists; its baseline dir is hard-coded to `delta-storage`, and the bucket base name comes from local `context.json`.

### Existing scattered conflict checks (keep them)

**Synth**
- `AppConstruct.ts:43-48`: the source simulator outside a mock landscape throws.
- `TaskDefinitions.ts:50-52`: no storage type throws.
- Unsupported storage type throws in `TaskDefinitions`, `ProcessorTaskDefinition` and `PostProcessorTaskDefinition` (the latter two include `'file'`).
- `DynamoDB.ts:435-472`: accessing a table that wasn't created throws.

**ECS**
- `chunker.ts`: `PREVIOUS_STORAGE_TYPE` missing or unsupported; DynamoDB mode without a table; the no-baseline override of `bulkReset`.
- `processor.ts` / `post-processor.ts`: storage env var or tables missing.
- `MetadataFactory`: a missing statistics table, unsupported type, or bootstrap type.
- `ChunkConfigOverride`: logs overrides.
- `ChunkFromAPI`: `MAX_SCALING_CAPACITY=1` override; `bulkReset` override warning; bad `POPULATION_SCOPE`.
- `ProcessorForDynamoDb`: exits if a table env var is missing.
- `PostProcessorForDynamoDB`: skips deletes when tables are missing.
- `AbstractDeferredDeleteHandler`: an invalid `PERSON_DELETE_TYPE` falls back to SOFT with a warning.
- Post-processors: skip deletes for failed or PersonDelta runs.
- huron-person `SyncPeople`: forces SOFT deletes.

**Lambda**
- `PostProcessorSubscriber`: blocked by a terminal error, or by missing metadata or `chunkCount`.

**Runner**
- `Runner.ts`: a bad `MESSAGES_TO_PREPOPULATE`; `SOURCE_SIMULATOR` on a non-mock landscape (not lower-cased, unlike CDK).
- `MockLandscapeRunnerDecorator`: a non-mock landscape; hybrid-run warnings; a missing mock target table.
- `BulkPurger`: the non-mock guard.
- Mutually exclusive modes in the `MessagingOnly`/`ChunkingOnly`/`RestoreToFull` decorators.
- `SourceSimulatorRunnerDecorator`: no function URL; bad BUID; overseeding.
- `RunnerForQueueSeeding`: BUID with seeding; desired count > seeds; seeds > population; no counter table.
- `swap.sh:76-88`: an informational note when `.env` `LANDSCAPE` differs from the context.

### Fallbacks that disagree

**`PREVIOUS_STORAGE_TYPE` missing from context:**

| Where | Falls back to |
|---|---|
| `DynamoDB.ts` | `'dynamodb'` |
| `PostProcessorSubscribingLambda` | `'s3'` |
| `TaskDefinitions` | App config `storage.type` |
| `Secrets` | the `configPath` file, then `'s3'` |
| `IContext` comment | says `'dynamodb'` |
| `MockLandscapeRunnerDecorator` | whether the table exists |
| `MetadataFactory` | env, then the secret |
| `PersonCacheFactory` | the secret only |

**Other disagreements:**
- **`trustPreviousStorage` defaults:** `false` in the Runner, `ChunkFromAPI`, `ChunkerApiSubscriber` and chunker Flags; `true` in `resolveCommonFlags` and the `SyncPeople` constructor.
- **S3 `keyPrefix`:** honored for `SHARED_DELTA_STORAGE_DIR`, dropped by `ProcessorForS3`, hard-coded to `delta-storage` in the mock reset.
- **DynamoDB region:** the secret keeps the file's region; the processor uses the `REGION` env var; `MetadataBroker` passes none.
- **Ignored at runtime:** the secret's `preLoadedMaps` and `personDeleteType` lose to the synth-baked `STATIC_MAP_USAGE` and `PERSON_DELETE_TYPE`.

### Contradictions found in the files (2026-10-17)

- `context/staging.env` and `context/migration.env` set `LANDSCAPE=preview` and `HURON_PERSON_CONFIG_PATH=config.preview.json`, while their context files point at their own landscapes. **The user reports having corrected this.**
- `.env` `DATASOURCE_*`, `DATATARGET_*`, `CLIENT_ID` and `CACHE_*` override the config file in the deployed secret at synth. For example, `cache.path` becomes `"."` instead of `/tmp/cache`.
- `docker/post-processor.env`: `PERSON_DELETE_TYPE=deferred` is invalid; it falls back to SOFT with a warning.
- `docker/chunker.env`: mixes landscapes (`CHUNKS_BUCKET=…-staging`, `LANDSCAPE=dev`).

### Small bugs found along the way (unfixed)

- `ChunkingOnlyRunnerDecorator`: the BUID check prints `MESSAGING_ONLY and SINGLE_PERSON_BUID cannot both be true` even though it fires for `CHUNKING_ONLY`.
- `ChunkerService.ts:71`: `fromFileSystem()` without `reset()` (see above).
- `src/Utils.ts getLocalConfig()`: the path lacks the `config/` subfolder.
- `TaskDefinitions.ts:62/:78`: unguarded destructuring of `storageCfg`.
- `PostProcessorSubscribingLambda`: in DynamoDB mode it gets no person-table env vars, so bootstrap would throw if it ever fired. This is latent: DynamoDB mode writes no markers.
- huron-person:
  - `SyncPerson` / `SyncPersonBatch` don't list `HURON_PERSON_CONFIG_PATH` in their `TestEnvironment` keys, so their prefixed `.env` values are ignored.
  - `SyncPersonBatch` and `UnassignedOrgEnforcer` disagree on `SYNC_BUIDS` vs `SYNC_BUIDS_FILE_PATH` precedence.

## Related recent work

These commits are on the `post-processor` branch (not yet merged to master):
- `f45422e`: renamed the merger phase to post-processor.
- `407360c`: the config secret's storage is derived from context `PREVIOUS_STORAGE_TYPE` (`resolveSecretStorage()` in `lib/Secrets.ts`). This is a small instance of "resolve once from one home".
- `b1e6b72`: the mock landscape reset detects the deployed storage mode (`MockLandscapeRunnerDecorator.detectStorageMode()`). This is an instance of "derive from the deployed stack".
