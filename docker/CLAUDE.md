# docker/: The Task Image and Its Entry Points

One image runs all three pipeline phases on ECS Fargate (ARM64, Graviton); each task definition picks the entry point.
How to build, run locally with Docker Compose, and the variables: [README.md](README.md); image hardening:
[SECURITY.md](SECURITY.md).

## Files

- **`chunker.ts`** (Phase 1, harness `DOCKER_CHUNKER`): runs in a chunker task started from the chunker queue (fed by
  the chunker subscriber Lambda); streams the source (API or a dropped file) into NDJSON chunk files and writes the
  run's metadata and Flags (`termsEndpoint`, `startedBy`, `processorScalingCapacity`, addons, ...).
- **`processor.ts`** (Phase 2, harness `DOCKER_PROCESSOR`): a router to `src/processing/ProcessorForS3.ts` or
  `ProcessorForDynamoDb.ts`; accepts an optional `personRecordProcessor` for tests
  ([src/processing/addon/README.md](../src/processing/addon/README.md)).
- **`post-processor.ts`** (Phase 3, harness `DOCKER_POST_PROCESSOR`): a router to `PostProcessorForS3` or
  `PostProcessorForDynamoDB` (`src/post-processing/`).
- **`chunkTypes.ts`**: shared chunking types (`SyncPopulation`, `SourceEndpoint`, `IChunkFromSource`, ...) and
  `DEFAULT_MAX_TOTAL_RECORDS`.
- **`Dockerfile`**: two stages on `node:22-alpine` for `linux/arm64`. The build stage runs `npm ci --omit=optional` and
  `npx tsc` over `src/`, `context/*.ts` and `docker/*.ts`; the production stage installs with
  `npm ci --omit=dev --omit=optional`, copies `dist/`, runs as the non-root `nodejs` user, and defaults to
  `node dist/docker/chunker.js` (the task definitions set the command).
- **Scripts** (run through npm from the repo root): `build.sh` (`npm run docker-build`, buildx), `push.sh`
  (`npm run docker-push`: ECR login, tag, push), `publish.sh` (`npm run docker-publish`: build then push), `run.sh`
  (`npm run chunker` / `processor` / `post-processor`). **`run.sh` doesn't run a container**: its `docker-compose up`
  lines are commented out, so it only checks its variables and prints the command; run
  `docker-compose [-f docker-compose.yml -f docker-compose.manual.yml] up <phase>` yourself from `docker/`. The compose
  services take their variables only from `chunker.env`, `processor.env` and `post-processor.env` (templates:
  `example-env-s3.md` / `example-env-dynamodb.md`), not from the shell.

## Rules and pitfalls

- **The image never contains the dashboard**: it builds only from `package.json`, the lockfile, `tsconfig.json`, `src/`,
  `context/*.ts` and `docker/`, compiles with `npx tsc` (not `npm run build`), and omits optional (dashboard-only)
  packages. When a change needs `docker-build` and when it doesn't: "Keep the dashboard out of the task image" in the
  project [CLAUDE.md](../CLAUDE.md).
- **`push.sh` pushes to the active context's landscape**: the repository is `huron-person-integration-<landscape>`,
  with the account, region and landscape read from `context/context.json` (`bin/get-context.js`). Swap the context
  (`./context/swap.sh <landscape>`) before pushing to another landscape, and push to each landscape after a change
  that affects the image (a new integration-core build included).
- **The routers branch on `PREVIOUS_STORAGE_TYPE`**, which CDK bakes into each task definition: `s3` or `dynamodb`; it
  must be set, any other value throws, and `dynamodb` also requires both `DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME` and
  `DYNAMODB_PERSON_HISTORY_TABLE_NAME`. (`MetadataFactoryForBootstrap` in `src/chunking/metadata/MetadataFactory.ts`,
  which reads metadata before config is loaded, decides by the person tables' variables instead.) Storage modes in
  full: [docs/STORAGE_MODES.md](../docs/STORAGE_MODES.md).
- **Tables in both modes**: the statistics table (errors, metadata, claims) and the atomic counter table (chunk
  ordinals and offsets; the chunker's) exist in either mode; only the two person tables are DynamoDB mode's.
- **Configuration** reaches a task as `HURON_PERSON_CONFIG_JSON` (the config secret, injected by ECS) or `SECRET_ARN`;
  locally, `HURON_PERSON_CONFIG_PATH`. `IS_ECS_TASK` tells the code it runs in Fargate; `DRY_RUN`, `CACHE_ENABLED` and
  `CACHE_PATH` are shared switches. Never bake credentials into the image or the task definition's plain environment.
- **Test an entry point before building**: `npx ts-node docker/chunker.ts` (or VS Code's "Debug current file") with
  its `.env` group; then build and run the image locally (`docker-compose up <phase>`); then push and test in an ECS
  task before a full
  pipeline run.
- Every entry point's `require.main` block lists the variables it reads; keep it in step with the task definitions
  in `lib/services/*/`.
