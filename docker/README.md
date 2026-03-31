# Docker Setup for Huron Person Processing

This directory contains Docker configuration for running the Huron Person processor in containers, optimized for AWS Fargate with Graviton2 (ARM64) processors.

## Architecture

The system uses a single Docker image with three entry points:

1. **Chunker** (`dist/docker/chunker.js`): Reads large JSON files from S3 and breaks them into NDJSON chunks
2. **Processor** (`dist/docker/processor.js`): Reads NDJSON chunks and syncs person records to Huron API
3. **Merger** (`dist/docker/merger.js`): Concatenates processed chunk outputs into a single merged file

### Chunker Operation

The chunker reads a large JSON file from S3, streams the person array, and writes smaller NDJSON chunk files to enable parallel processing.

**Environment:**
- Reads `INPUT_BUCKET`, `INPUT_KEY`, and `CHUNKS_BUCKET` environment variables
- Writes chunk files to: `{CHUNKS_BUCKET}/{INPUT_BUCKET}/chunks/chunk-XXXX.ndjson`
- Creates metadata file: `{CHUNKS_BUCKET}/{INPUT_BUCKET}/chunks/_metadata.json`

### Processor Dual-Mode Operation

The processor adapts to its environment automatically:

**Local Development (docker-compose):**
- Reads `CHUNKS_BUCKET` and `CHUNK_KEY` environment variables
- Processes a single specified chunk file
- Uses `EnvironmentBasedQueueSimulator`

**AWS Fargate:**
- Reads `SQS_QUEUE_URL` environment variable
- Polls SQS for S3 event notifications
- Parses S3 bucket/key from event notifications
- Processes chunks as they arrive
- Uses `SQSQueueReader`

This dual-mode design allows identical code to run locally for testing and in Fargate for production.

### Merger Operation

The merger runs after all processor tasks complete. It concatenates chunk output files into a single merged file for the next sync cycle.

**Environment:**
- Reads `CHUNKS_BUCKET` and `INPUT_BUCKET` environment variables
- Lists all chunks: `{CHUNKS_BUCKET}/{INPUT_BUCKET}/chunks/chunk-*.ndjson`
- Writes merged output: `{CHUNKS_BUCKET}/{INPUT_BUCKET}/previous-input.ndjson`
- Deletes chunk files after successful merge

**Trigger:**
- In AWS: EventBridge rule polls metadata file and triggers merger when all chunks are processed
- Locally: Run manually via `./run.sh merger` after processing all chunks

## Files

- `Dockerfile` - Multi-stage build for ARM64/Graviton2
- `docker-compose.yml` - Local development/testing with docker-compose
- `docker-compose.manual.yml` - Override for manual/debug mode
- `build.sh` - Script to build the Docker image (uses docker-compose)
- `run.sh` - Script to run containers locally (uses docker-compose)
- `../.dockerignore` - Files to exclude from Docker build context
- `SECURITY.md` - Security notes and vulnerability mitigation

## Quick Start

### Build the Image

```bash
cd docker
./build.sh
```

This uses Docker Compose to build an ARM64 image optimized for Graviton2 processors (~20% cost savings on AWS Fargate).

### Run Locally (Normal Mode)

#### Option 1: Using run.sh script (Recommended)

**Run Chunker:**
```bash
AWS_PROFILE=infnprd INPUT_BUCKET=input-bucket INPUT_KEY=data/people.json CHUNKS_BUCKET=chunks-bucket ./run.sh chunker
```

**Run Processor:**
```bash
AWS_PROFILE=infnprd CHUNKS_BUCKET=chunks-bucket CHUNK_KEY=client/chunks/chunk-0000.ndjson ./run.sh processor
```

**Run Merger:**
```bash
AWS_PROFILE=infnprd CHUNKS_BUCKET=chunks-bucket INPUT_BUCKET=input-bucket ./run.sh merger
```

#### Option 2: Using docker-compose directly

```bash
# Set environment variables first
export AWS_PROFILE=infnprd
export INPUT_BUCKET=input-bucket
export INPUT_KEY=data/people.json
export CHUNKS_BUCKET=chunks-bucket

# Run chunker
docker-compose up chunker

# Run processor (different CHUNK_KEY for chunk file)
export CHUNK_KEY=client/chunks/chunk-0000.ndjson
docker-compose up processor

# Run merger
docker-compose up merger
```

Edit environment variables in `docker-compose.yml` or create a `.env` file.

### Run in Manual/Debug Mode

Manual mode keeps the container running without executing the node command, allowing you to shell in and run commands interactively.

**Option 1: Using run.sh with --manual flag**
```bash
# Start container in background
AWS_PROFILE=infnprd ./run.sh processor --manual

# Shell into container
docker exec -it huron-processor sh

# Inside container, run commands manually
export CHUNKS_BUCKET=chunks-bucket
export CHUNK_KEY=client/chunks/chunk-0000.ndjson
node dist/docker/processor.js

# Test with different chunk
CHUNK_KEY=client/chunks/chunk-0001.ndjson node dist/docker/processor.js

# Or for merger
export INPUT_BUCKET=input-bucket
node dist/docker/merger.js

# Stop when done
docker-compose down
```

**Option 2: Using docker-compose with override file**
```bash
# Start in manual mode
docker-compose -f docker-compose.yml -f docker-compose.manual.yml up -d processor

# Shell in
docker exec -it huron-processor sh

# Run commands inside container
node dist/docker/processor.js

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
**For Local Development:**
```bash
export AWS_PROFILE=infnprd  # or dev, prod, etc.
```

The container mounts your `~/.aws` directory (read-only), which contains:
- `~/.aws/credentials` - Access keys organized by profile
- `~/.aws/config` - Region and other settings per profile

The AWS SDK automatically uses the specified profile. No need to set `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY` explicitly.

**For AWS Fargate:**
- Credentials are provided via IAM task roles
- No environment variables or volume mounts needed
- CDK stack configures proper IAM permissions

### Common Variables
- `REGION` - AWS region (default: us-east-2)
- `PERSON_ID_FIELD` - Field identifying person records (default: personid)
- `AWS_PROFILE` - AWS profile name from ~/.aws/credentials (default: default)

### Chunker-Specific
- `INPUT_BUCKET` - Source bucket containing input JSON file (required)
- `INPUT_KEY` - Key of input JSON file to process (required)
- `CHUNKS_BUCKET` - Destination bucket for chunk files (required)
- `ITEMS_PER_CHUNK` - Number of persons per chunk file (default: 200)

### Processor-Specific
- `CHUNKS_BUCKET` - Bucket containing chunk files (required)
- `CHUNK_KEY` - Key of specific chunk to process (required)
- `HURON_API_ENDPOINT` - Huron API endpoint URL (optional, for future use)

### Merger-Specific
- `CHUNKS_BUCKET` - Bucket containing chunk files (required)
- `INPUT_BUCKET` - Original input bucket name, used as top-level folder prefix (required)

## AWS Fargate Deployment

This image is designed for AWS Fargate with Graviton2. Deployment is handled via CDK (see `publish.sh` in this directory and `../lib/` directory).

### Manual ECR Push (if needed)

```bash
# Login to ECR
aws ecr get-login-password --region us-east-2 | \
  docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-2.amazonaws.com

# Tag image
docker tag huron-person-processor:latest \
  <account>.dkr.ecr.us-east-2.amazonaws.com/huron-person-processor:latest

# Push to ECR
docker push <account>.dkr.ecr.us-east-2.amazonaws.com/huron-person-processor:latest
```

### Task Definitions

ECS task definitions are created via CDK with these settings:

**Chunker Task:**
- Command: `["node", "dist/docker/chunker.js"]`
- Platform: `ARM64`, `LINUX`
- CPU: 2048, Memory: 4096 MB
- Environment: `INPUT_BUCKET`, `INPUT_KEY`, `CHUNKS_BUCKET` (set by Lambda at runtime)

**Processor Task:**
- Command: `["node", "dist/docker/processor.js"]`
- Platform: `ARM64`, `LINUX`
- CPU: 1024, Memory: 2048 MB
- Environment: `CHUNKS_BUCKET`, `CHUNK_KEY` (from SQS message)

**Merger Task:**
- Command: `["node", "dist/docker/merger.js"]`
- Platform: `ARM64`, `LINUX`
- CPU: 1024, Memory: 2048 MB
- Environment: `CHUNKS_BUCKET`, `INPUT_BUCKET` (set by Lambda at runtime)

## Image Details

The multi-stage build produces a lean, secure production image:
- Base: `node:current-alpine3.22` (Node.js 25.8.1, ~150MB)
- With app code: ~200-250MB
- Security: Non-root user (nodejs:1001), Alpine packages upgraded
- Vulnerabilities: 4 high CVEs (down from 13 with older base images)

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
4. Check the volume mount is working: `docker exec -it huron-chunker cat /root/.aws/credentials`

**Profile-specific region not used:**
- Set `REGION` environment variable explicitly, or
- Ensure `~/.aws/config` has region for the profile

### Runtime Issues

**Container exits immediately:**
- Check logs: `docker logs <container-id>` or `docker-compose logs chunker`
- Verify required environment variables are set:
  - Chunker: `INPUT_BUCKET`, `INPUT_KEY`, `CHUNKS_BUCKET`
  - Processor: `CHUNKS_BUCKET`, `CHUNK_KEY`
  - Merger: `CHUNKS_BUCKET`, `INPUT_BUCKET`
- Try manual mode to debug: `./run.sh chunker --manual`

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
# Start in manual mode
./run.sh chunker --manual

# Shell in
docker exec -it huron-chunker sh

# Check environment
env | grep -E '(AWS|S3|REGION)'

# Test AWS connectivity
node -e "const {S3Client} = require('@aws-sdk/client-s3'); console.log('OK')"

# Run with debug output
DEBUG=* node dist/docker/chunker.js
```
