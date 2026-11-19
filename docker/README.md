# Docker Setup for Huron Person Processing

This directory contains the Docker configuration for running the Huron Person pipeline in containers on AWS Fargate
with Graviton2 (ARM64) processors.

## Architecture

The system uses a single Docker image with three entry points:

1. **Chunker** (`dist/docker/chunker.js`): streams person records from the source API, or from a JSON file dropped in
   S3, into NDJSON chunk files
2. **Processor** (`dist/docker/processor.js`): reads a chunk file and syncs its people to the Huron API
3. **Post-processor** (`dist/docker/post-processor.js`): consolidates the run's results and handles deletions

### Chunker Operation

The chunker's source comes from its SQS message: the source API (`ChunkFromAPI`; the scheduled run, the dashboard's
Bulk sync and the Runner) or a dropped file (`ChunkFromS3`, with the file's bucket and key in the message).

- Writes chunk files to `{CHUNKS_BUCKET}/chunks/<population>/<syncRunId>/chunk-NNNN.ndjson`
- Writes the run's metadata and flags to `_metadata.json` / `_flags.json` in that directory (S3 mode), or to the
  statistics table (DynamoDB mode)

### Processor Dual-Mode Operation

The processor router (`docker/processor.ts`) delegates to the storage mode's implementation, both of which are modules
exporting `main()`:

**`src/processing/ProcessorForS3.ts` (S3 mode):**
- Writes a mini-delta to S3 for each chunk, under `deltas/<population>/<syncRunId>/`, with a completion marker
  `chunk-NNNN_processing_complete.json`
- The post-processor consolidates the deltas into `previous-input.ndjson`

**`src/processing/ProcessorForDynamoDb.ts` (DynamoDB mode):**
- Writes directly to the PersonCurrentState and PersonHistory tables, with atomic DynamoDB operations
- No mini-deltas or marker files; the last processor to finish triggers the post-processor

**Mode selection:** both routers switch on `PREVIOUS_STORAGE_TYPE` (`s3` or `dynamodb`), which CDK bakes into every
task definition. It must be set, and any other value throws. `dynamodb` also requires both
`DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME` and `DYNAMODB_PERSON_HISTORY_TABLE_NAME`. (`MetadataFactoryForBootstrap`, in
`src/chunking/metadata/MetadataFactory.ts`, switches on it too, but in `dynamodb` mode needs only one of the two.)

**Local Development (docker-compose):**
- Reads `CHUNKS_BUCKET` and `CHUNK_KEY` environment variables
- Processes a single specified chunk file
- Uses `EnvironmentBasedQueueSimulator`

**AWS Fargate:**
- Reads `SQS_QUEUE_URL` environment variable
- Polls SQS for the S3 event notifications the processor subscriber forwards
- Parses the chunk's bucket and key from each notification
- Uses `SQSQueueReader`

This dual-mode design allows identical code to run locally for testing and in Fargate for production.

### Post-Processor Operation (Template Method Pattern)

**AbstractPostProcessor (src/post-processing/AbstractPostProcessor.ts - Base Class):**
- `main()`: the template method: task parameters, `merge()`, then `runDeferredDeletes()`; its `finally` writes the
  run's `RUN_SUMMARY` to the statistics table
- `getMessageFromSQS()`: reads the trigger message
- Abstract: `getTaskParameters()`, `getPostProcessorContext()`, `merge()`, `runDeferredDeletes()`

**PostProcessorForS3 (src/post-processing/PostProcessorForS3.ts - S3 Mode Implementation):**
- Consolidates delta chunk files from `deltas/{population}/{syncRunId}/`
- Merges with the existing baseline (`previous-input.ndjson`)
- Writes the merged result to `{SHARED_DELTA_STORAGE_DIR}/previous-input.ndjson` (default `delta-storage/`)
- Cleans up the temporary delta chunk files

**PostProcessorForDynamoDB (src/post-processing/PostProcessorForDynamoDB.ts - DynamoDB Mode Implementation):**
- No file consolidation or baseline merging (the processors wrote the tables directly)
- `merge()` is essentially a no-op
- Still runs the deferred deletes (soft-deletions)

**post-processor.ts (docker/post-processor.ts - Router):** switches on `PREVIOUS_STORAGE_TYPE`, as the processor router
does.

**Both modes require the post-processor service**: S3 mode for file consolidation and deletion handling, DynamoDB mode
for deletion handling.

**Environment:**
- ECS: reads its trigger message from `SQS_QUEUE_URL`, which carries the chunks bucket, chunk directory and `createdAt`
- Local: `CHUNKS_BUCKET` plus `CHUNK_DIRECTORY` (e.g. `chunks/person-full/<syncRunId>`) or `INPUT_KEY`

**Trigger:**
- S3 mode: each processor's completion marker fires an S3 notification (suffix `_processing_complete.json`) to the
  post-processor subscriber Lambda (`src/post-processing/PostProcessorSubscriber.ts`), which queues the post-processor
  once every chunk is done (and never for a run with a terminal error)
- DynamoDB mode: the last processor to finish sends the trigger message itself (`POST_PROCESSOR_QUEUE_URL`)
- Locally: run it yourself after processing all chunks

## Storage Modes

### S3 Mode

- Chunk NDJSON files: `s3://bucket/chunks/{population}/{syncRunId}/chunk-NNNN.ndjson`
- Metadata, flags and terminal error: `_metadata.json`, `_flags.json`, `_terminal_error.json` in the chunk directory
- Delta chunks and completion markers: `s3://bucket/deltas/{population}/{syncRunId}/`
- Baseline: `s3://bucket/{SHARED_DELTA_STORAGE_DIR}/previous-input.ndjson` (default `delta-storage/`)

**Environment variables:** `PREVIOUS_STORAGE_TYPE=s3`, `DYNAMODB_STATISTICS_TABLE_NAME` (errors, claims, the run
summary).

### DynamoDB Mode

- **PersonCurrentStateTable** - current hash state per person (PK: personId)
- **PersonHistoryTable** - historical change audit trail (PK: personId, SK: syncRunId)
- **StatisticsTable** - metadata, flags, errors and the run summary (PK: integrationTimestamp, SK: eventType)
- Chunk NDJSON files remain in S3 (required for parallel processing)

**Environment variables:** `PREVIOUS_STORAGE_TYPE=dynamodb`, `DYNAMODB_STATISTICS_TABLE_NAME`,
`DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME`, `DYNAMODB_PERSON_HISTORY_TABLE_NAME`.

The statistics and atomic counter tables exist in BOTH modes; only PersonCurrentStateTable and PersonHistoryTable are
DynamoDB mode's. More: [docs/STORAGE_MODES.md](../docs/STORAGE_MODES.md).

## Files

- `Dockerfile` - multi-stage build for ARM64/Graviton2
- `docker-compose.yml` - local development/testing with docker-compose
- `docker-compose.manual.yml` - override for manual/debug mode
- `chunker.env`, `processor.env`, `post-processor.env` - the containers' variables (compose's `env_file`); templates in
  `example-env-s3.md` and `example-env-dynamodb.md`
- `build.sh` - builds the image with `docker buildx` (ARM64, Docker v2 manifest); `npm run docker-build`
- `push.sh` - ECR login, tag and push; `npm run docker-push`. `publish.sh` builds then pushes; `npm run docker-publish`
- `run.sh` - `npm run chunker` / `processor` / `post-processor` (and `*-debug` for manual mode): checks the phase's
  env file names `PREVIOUS_STORAGE_TYPE` (and warns without `AWS_PROFILE`), then runs that phase alone with
  `docker compose up --no-deps`, exiting with the container's exit code
- `../.dockerignore` - files to exclude from the build context
- `SECURITY.md` - security notes and vulnerability mitigation

## Quick Start

### Build the Image

```bash
npm run docker-build     # from the repo root; or: cd docker && ./build.sh
```

This builds an ARM64 image for Graviton2 processors (~20% cost savings on AWS Fargate).

### Run Locally (Normal Mode)

Set the variables in `docker/chunker.env`, `processor.env` or `post-processor.env`: the compose services read only
those files, so variables exported in your shell don't reach the container. Then, from the repo root:

```bash
npm run chunker
npm run processor        # with CHUNK_KEY=chunks/person-full/<syncRunId>/chunk-0000.ndjson in processor.env
npm run post-processor   # with CHUNK_DIRECTORY=chunks/person-full/<syncRunId> in post-processor.env
```

or, from `docker/`, `./run.sh <phase>` or `docker compose up --no-deps <phase>`. Without `--no-deps`, compose also
starts the phases the service `depends_on` (the processor starts the chunker; the post-processor starts both).

### Run in Manual/Debug Mode

Manual mode keeps the container running without executing the node command, so you can shell in and run commands
interactively.

```bash
# Start in manual mode (or: npm run processor-debug)
docker-compose -f docker-compose.yml -f docker-compose.manual.yml up -d --no-deps processor

# Shell in
docker exec -it huron-processor sh

# Run commands inside container
node dist/docker/processor.js

# Test with a different chunk
CHUNK_KEY=chunks/person-full/<syncRunId>/chunk-0001.ndjson node dist/docker/processor.js

# Stop
docker-compose down
```

**Manual mode is useful for:**
- Debugging code behavior
- Testing with different parameters
- Inspecting container filesystem
- Troubleshooting AWS credential issues

## Environment Variables

### AWS Credentials
**For Local Development:** set `AWS_PROFILE` in the phase's `.env` file. Compose mounts your `~/.aws` directory
read-only at `/home/nodejs/.aws`, the home of the image's non-root user `nodejs`, where the SDK looks for it. (Not
`/root/.aws`: the SDK wouldn't look there, and `nodejs` can't read `/root`.) Being read-only, the mount can't take a
refreshed SSO token: for an SSO profile, log in on the host (`aws sso login`) before running.

**For AWS Fargate:**
- Credentials are provided via IAM task roles
- No environment variables or volume mounts needed
- CDK stack configures proper IAM permissions

### Common Variables
- `PREVIOUS_STORAGE_TYPE` - `s3` or `dynamodb` (required)
- `REGION` - AWS region (default: us-east-2)
- `PERSON_ID_FIELD` - Field identifying person records (default: personid)
- `AWS_PROFILE` - AWS profile name from ~/.aws/credentials (default: default)

### Chunker-Specific
- `SQS_QUEUE_URL`, `REGION`, `LANDSCAPE` - required, also locally (`ChunkerQueue`)
- `CHUNKS_BUCKET` - destination bucket for chunk files (required)
- `INPUT_BUCKET`, `INPUT_KEY` - a dropped file's bucket and key, for a local file-drop run (in ECS they come in the SQS
  message)
- `ITEMS_PER_CHUNK` - number of persons per chunk file (default: 200)
- `DYNAMODB_ATOMIC_COUNTER_TABLE_NAME` - with `STACK_ID`, atomic chunk ordinals and offsets; without them, ordinals are
  derived from offsets (non-atomically). CDK always sets it
- `SECRET_ARN` - Secrets Manager ARN for configuration (optional, for API mode)
- `ECS_CLUSTER_NAME`, `ECS_SERVICE_NAME`, `MAX_SCALING_CAPACITY` - ECS service scaling parameters

### Processor-Specific
- `CHUNKS_BUCKET`, `CHUNK_KEY` - the chunk to process, locally (without both, the processor reads SQS)
- `SQS_QUEUE_URL` - the processor queue (ECS)
- `POST_PROCESSOR_QUEUE_URL` - DynamoDB mode: the last processor triggers the post-processor through it
- `DYNAMODB_STATISTICS_TABLE_NAME` - error tracking and statistics (both modes)
- `DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME`, `DYNAMODB_PERSON_HISTORY_TABLE_NAME` - DynamoDB mode
- `SECRET_ARN` - Secrets Manager ARN for configuration (optional)
- `STATIC_MAP_USAGE` - JSON object specifying which static maps to load (e.g., `{ "orgMap": true }`)
- `SHARED_DELTA_STORAGE_DIR` - S3 directory for baseline delta files (default: delta-storage)

### Post-Processor-Specific
- `CHUNKS_BUCKET` - bucket containing chunk files (required locally)
- `CHUNK_DIRECTORY` or `INPUT_KEY` - the run's chunk directory, locally
- `SQS_QUEUE_URL` - the post-processor queue (ECS)
- `DYNAMODB_STATISTICS_TABLE_NAME` - statistics and the run summary (both modes)
- `SECRET_ARN` - Secrets Manager ARN for configuration (optional)
- `SHARED_DELTA_STORAGE_DIR` - S3 directory for baseline delta files (default: delta-storage)
- `PERSON_DELETE_TYPE` - `soft` (default; also for an invalid value), `hard`, `log` or `none` (no deletion handling)

## AWS Fargate Deployment

This image is designed for AWS Fargate with Graviton2. The infrastructure is deployed with CDK (`../lib/`); the image is
pushed with `npm run docker-push` (or `docker-publish` to build first) to the active landscape's repository,
`huron-person-integration-<landscape>`, with the account, region and landscape read from `context/context.json`.

### Manual ECR Push (if needed)

```bash
# Login to ECR
aws ecr get-login-password --region us-east-2 | \
  docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-2.amazonaws.com

# Tag image
docker tag huron-person-integration:latest \
  <account>.dkr.ecr.us-east-2.amazonaws.com/huron-person-integration-<landscape>:latest

# Push to ECR
docker push <account>.dkr.ecr.us-east-2.amazonaws.com/huron-person-integration-<landscape>:latest
```

### Task Definitions

ECS task definitions are created via CDK (`lib/services/*/*TaskDefinition.ts`), each on `ARM64`, `LINUX`, with CPU and
memory from the landscape's context (`ECS.<phase>TaskDefinition`):

**Chunker Task:** command `["node", "dist/docker/chunker.js"]`. Environment baked in by CDK includes `CHUNKS_BUCKET`,
`SQS_QUEUE_URL`, `PREVIOUS_STORAGE_TYPE`, the DynamoDB table names, `LANDSCAPE` and `STACK_ID`; a file drop's
`INPUT_BUCKET` / `INPUT_KEY` come in the SQS message.

**Processor Task:** command `["node", "dist/docker/processor.js"]`. Environment includes `SQS_QUEUE_URL`,
`PREVIOUS_STORAGE_TYPE` and the table names; each chunk's bucket and key come in its SQS message.

**Post-Processor Task:** command `["node", "dist/docker/post-processor.js"]`. The bucket, chunk directory and
`createdAt` come in the SQS message. (CDK also sets `INPUT_BUCKET`, which nothing reads.)

## Image Details

The multi-stage build produces a lean, secure production image:
- Base: `node:22-alpine3.22` on `linux/arm64`, both stages
- Build stage: `npm ci --omit=optional` (no dashboard packages) and `npx tsc`
- Production stage: `npm ci --omit=dev --omit=optional`, `dist/` only
- Security: non-root user (`nodejs:1001`), Alpine packages upgraded
- Default command: `node dist/docker/chunker.js` (the task definitions set their own)

See `SECURITY.md` for vulnerability analysis and mitigation strategies.

## Troubleshooting

### Build Issues

**Build fails on Apple Silicon (M1/M2):**
- The `--platform linux/arm64` flag ensures ARM64 compatibility
- Should build natively on M-series Macs

**Build fails on x86_64:**
- Enable Docker BuildKit: `export DOCKER_BUILDKIT=1`
- Or use: `docker buildx build --platform linux/arm64 ...`

### AWS Credentials

**Credentials not working:**
1. Check your `~/.aws/credentials` file exists and contains profiles
2. Verify `AWS_PROFILE` matches a profile name in credentials file
3. Test credentials outside Docker: `aws s3 ls --profile infnprd`
4. Check the volume mount is working: `docker exec -it huron-chunker ls /home/nodejs/.aws`

**Profile-specific region not used:**
- Set `REGION` environment variable explicitly, or
- Ensure `~/.aws/config` has region for the profile

### Runtime Issues

**Container exits immediately:**
- Check logs: `docker logs <container-id>` or `docker-compose logs chunker`
- Verify the required variables are set in the phase's `.env` file:
  - Chunker: `PREVIOUS_STORAGE_TYPE`, `SQS_QUEUE_URL`, `REGION`, `LANDSCAPE`, `CHUNKS_BUCKET` (and `INPUT_BUCKET`,
    `INPUT_KEY` for a file drop)
  - Processor: `PREVIOUS_STORAGE_TYPE`, `CHUNKS_BUCKET`, `CHUNK_KEY`
  - Post-processor: `PREVIOUS_STORAGE_TYPE`, `CHUNKS_BUCKET`, `CHUNK_DIRECTORY` (or `INPUT_KEY`)
- Try manual mode to debug (above)

**S3 access denied:**
- Verify your AWS profile has S3 read permissions
- Check bucket names and keys are correct
- Test with AWS CLI chunker: `aws s3 cp s3://$INPUT_BUCKET/$INPUT_KEY . --profile $AWS_PROFILE`
- Test with AWS CLI processor: `aws s3 cp s3://$CHUNKS_BUCKET/$CHUNK_KEY . --profile $AWS_PROFILE`

**Memory issues / OOM kills:**
- Increase Docker desktop memory allocation
- For large files (>2GB), ensure at least 8GB RAM available

### Manual Mode Debugging

```bash
# Start in manual mode (from docker/; or: npm run chunker-debug)
docker-compose -f docker-compose.yml -f docker-compose.manual.yml up -d --no-deps chunker

# Shell in
docker exec -it huron-chunker sh

# Check environment
env | grep -E '(AWS|S3|REGION)'

# Test AWS connectivity
node -e "const {S3Client} = require('@aws-sdk/client-s3'); console.log('OK')"

# Run with debug output
DEBUG=* node dist/docker/chunker.js
```
