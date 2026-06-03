# integration-huron-person-fargate/docker: Container Entry Points & Orchestration

## Purpose
Defines Docker container entry points for ECS Fargate tasks. Each container is a standalone executable that encapsulates phase-specific logic.

## Container Structure

```
docker/
  ├─ Dockerfile              (base image configuration)
  ├─ chunker.ts              (Phase 1: streaming chunking)
  ├─ processor.ts             (Phase 2: parallel transformation)
  └─ merger.ts                (Phase 3: consolidation)
```

## Dockerfile Pattern

**Purpose**: Multi-stage build optimizing for production

```dockerfile
FROM node:18-alpine AS builder

WORKDIR /build

# Install dependencies
COPY package*.json ./
RUN npm ci

# Build TypeScript
COPY tsconfig.json .
COPY src ./src
COPY docker ./docker
RUN npm run build

# Production image
FROM node:18-alpine

WORKDIR /app

# Copy only production dependencies
COPY --from=builder /build/node_modules ./node_modules
COPY --from=builder /build/dist ./dist

# Copy source (for ts-node execution)
COPY src ./src
COPY docker ./docker
COPY tsconfig.json .

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "console.log('healthy')"

# Entry point determined by ECS task definition
CMD ["npx", "ts-node", "docker/processor.ts"]
```

## Container Entry Point Pattern

All three entry points follow same pattern:

```typescript
// docker/{phase}.ts
import { ConfigManager, TestEnvironment } from 'integration-core';

interface PhaseConfig {
  s3Bucket: string;
  s3Region: string;
  // ... phase-specific config
}

async function loadConfiguration(): Promise<PhaseConfig> {
  const config = new ConfigManager()
    .fromJsonString(process.env.TASK_SECRETS)    // ECS injected
    .fromSecretManager()                          // AWS Secrets Manager
    .fromEnvironment();                           // .env for testing

  return {
    s3Bucket: config.getVariable('S3_BUCKET'),
    s3Region: config.getVariable('AWS_REGION') || 'us-east-2',
    // ...
  };
}

async function main() {
  try {
    console.log('[PHASE] Starting...');
    
    // Load config
    const config = await loadConfiguration();
    
    // Initialize service
    const service = new PhaseService(config);
    
    // Execute phase
    const result = await service.run();
    
    // Report success
    console.log('[PHASE] Complete:', result);
    process.exit(0);
    
  } catch (error) {
    console.error('[PHASE] Failed:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

export { main };
```

## Phase 1: Chunker Container

**File**: `docker/chunker.ts`

**Environment Prefix**: DOCKER_CHUNKER

**Responsibilities**:
1. Load source configuration (API or S3)
2. Instantiate chunker service
3. Execute chunking
4. Signal processors (SQS message)
5. Report metrics

**Execution Context**:
- Triggered by Lambda (ChunkerService)
- Runs once per sync cycle
- Sequential (not parallel)
- Can be long-running (hours for large datasets)

## Phase 2: Processor Container

**File**: `docker/processor.ts`

**Environment Prefix**: DOCKER_PROCESSOR

**Responsibilities**:
1. Load SQS message (chunk to process)
2. Fetch chunk from S3
3. Apply transformations (data mapper, validators)
4. Write processed chunk to S3
5. Acknowledge SQS message
6. Report completion

**Execution Context**:
- Triggered by SQS message
- Multiple tasks run in parallel (one per chunk or batch)
- Should complete in seconds/minutes per chunk
- Failures trigger automatic retry via SQS

**Parallelism**:
```
SQS Queue with 100 messages (100 chunks)
  ↓
ECS Task 1 → processes chunk-0001
ECS Task 2 → processes chunk-0002
ECS Task 3 → processes chunk-0003
... (up to desired count)
ECS Task N → processes chunk-NNNN
```

## Phase 3: Merger Container

**File**: `docker/merger.ts`

**Environment Prefix**: DOCKER_MERGER

**Responsibilities**:
1. Wait for all processor tasks to complete
2. Fetch all processed chunks from S3
3. Consolidate into single delta
4. Coordinate soft deletes
5. Write delta storage
6. Record statistics
7. Clean up intermediate S3 objects (optional)

**Execution Context**:
- Triggered after all processors complete
- Runs once per sync cycle
- Final consolidation step
- Can be long-running (consolidating results)

## Testing Entry Points Locally

### Test Without Docker

```bash
# Set up .env
export DOCKER_CHUNKER_S3_BUCKET=test-bucket
export DATASOURCE_API_KEY=test-key
# ... more vars

# Run entry point directly
npx ts-node docker/chunker.ts
```

### Test With Docker Locally

```bash
# Build Docker image
docker build -t huron-processor:test -f docker/Dockerfile .

# Run container with environment
docker run \
  -e DOCKER_PROCESSOR_S3_BUCKET=test-bucket \
  -e AWS_REGION=us-east-2 \
  -v ~/.aws:/.aws:ro \
  huron-processor:test npx ts-node docker/processor.ts
```

### Test With LocalStack (Mock AWS)

```bash
# Start LocalStack
docker-compose up localstack

# Run processor container against LocalStack
docker run \
  -e AWS_ENDPOINT_URL=http://localstack:4566 \
  -e DOCKER_PROCESSOR_S3_BUCKET=test-bucket \
  --network host \
  huron-processor:test npx ts-node docker/processor.ts
```

## Container Logging

### CloudWatch Integration
```typescript
// In entry point
console.log(`[${timestamp}] Phase starting`);
console.log(`[${timestamp}] Loaded configuration`);
console.error(`[${timestamp}] Error occurred: ...`);

// ECS redirects stdout/stderr to CloudWatch Logs
// Accessible at: /ecs/huron-{phase}
```

### Structured Logging (Optional)
```typescript
const logger = {
  info: (msg: string, data?: any) =>
    console.log(JSON.stringify({ level: 'INFO', msg, ...data })),
  error: (msg: string, data?: any) =>
    console.error(JSON.stringify({ level: 'ERROR', msg, ...data }))
};

logger.info('Processing chunk', { chunkKey: 'chunks/chunk-001.ndjson' });
```

## Container Health Checks

### Dockerfile Health Check
```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "process.exit(process.env.HEALTHY ? 0 : 1)"
```

### In Entry Point
```typescript
// Set environment variable indicating health
process.env.HEALTHY = 'true';

// Or periodically update
setInterval(() => {
  process.env.LAST_HEARTBEAT = Date.now().toString();
}, 30000);
```

## Error Handling in Containers

### Exit Codes
```typescript
if (error instanceof ConfigurationError) {
  console.error('Configuration invalid:', error);
  process.exit(1);  // Container exits with error
} else if (error instanceof S3Error) {
  console.error('S3 access failed:', error);
  process.exit(2);  // Different error code for S3
} else {
  console.error('Unknown error:', error);
  process.exit(1);
}
```

### ECS Task Failure Handling
- Exit code 0 → Task successful
- Exit code 1-255 → Task failed
- ECS can retry failed tasks automatically

## Container Resource Configuration

### Memory & CPU
```typescript
// For Phase 2 (processor)
const taskDef = new ecs.FargateTaskDefinition(this, 'Processor', {
  cpu: 1024,           // 1 vCPU
  memoryLimitMiB: 2048 // 2 GB
});
```

**Sizing Guidance**:
- **Chunker** (simple streaming): 512 CPU, 1024 MB memory
- **Processor** (data transformation): 1024 CPU, 2048 MB memory
- **Merger** (consolidation): 1024 CPU, 2048 MB memory

## Container Security Best Practices

1. **Never embed credentials**:
   ```dockerfile
   # ❌ Wrong
   ENV API_KEY=sk-1234567890
   
   # ✅ Correct: Load from Secrets Manager at runtime
   ```

2. **Use read-only root filesystem** (optional):
   ```typescript
   const taskDef = new ecs.FargateTaskDefinition(
     this, 'Task',
     { readonlyRootFilesystem: true }
   );
   ```

3. **Run as non-root user** (if not using Alpine):
   ```dockerfile
   RUN useradd -m -u 1000 appuser
   USER appuser
   ```

4. **Scan for vulnerabilities**:
   ```bash
   docker scan huron-processor:latest
   ```

## Deployment Pipeline

1. **Local testing**:
   ```bash
   npx ts-node docker/chunker.ts
   ```

2. **Docker build & test**:
   ```bash
   docker build -t huron-processor:v1.0 -f docker/Dockerfile .
   docker run ... huron-processor:v1.0
   ```

3. **Push to ECR**:
   ```bash
   aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
   docker tag huron-processor:v1.0 <account>.dkr.ecr.<region>.amazonaws.com/huron-processor:v1.0
   docker push <account>.dkr.ecr.<region>.amazonaws.com/huron-processor:v1.0
   ```

4. **Deploy CDK**:
   ```bash
   cdk deploy
   ```

5. **Monitor in CloudWatch**:
   ```bash
   aws logs tail /ecs/huron-processor --follow
   ```

