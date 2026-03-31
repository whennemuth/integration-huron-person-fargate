# Chunking Architecture for Parallel Processing

## Table of Contents
- [Problem Statement](#problem-statement)
- [Solution Overview](#solution-overview)
- [Architecture](#architecture)
- [Implementation Details](#implementation-details)
- [Testing](#testing)
- [Deployment Considerations](#deployment-considerations)

## Problem Statement

### The Race Condition

In the original design, multiple Fargate tasks running in parallel would encounter a race condition when updating the `previous-input.ndjson` file:

```
Task A reads previous-input.ndjson (10,000 records)
Task B reads previous-input.ndjson (10,000 records)
Task C reads previous-input.ndjson (10,000 records)

Task A processes chunk 0 (1,000 records) → writes previous-input.ndjson
Task B processes chunk 1 (1,000 records) → writes previous-input.ndjson (overwrites Task A)
Task C processes chunk 2 (1,000 records) → writes previous-input.ndjson (overwrites Task B)

Result: Only chunk 2 data is preserved. Chunks 0 and 1 data is lost.
```

### Why This Happened

The `updatePreviousData()` function in the delta strategy performs:
1. Read current `previous-input.ndjson`
2. Merge with newly processed records
3. Write back to `previous-input.ndjson`

When multiple tasks execute this simultaneously, the last write wins and previous writes are lost.

## Solution Overview

The solution implements a **3-phase chunked processing pipeline**:

```
Phase 1: Chunker (Sequential)
├─ Splits input into numbered chunk files
└─ Writes: chunks/chunk-0000.ndjson, chunk-0001.ndjson, ..., chunk-NNNN.ndjson

Phase 2: Processor (Parallel)
├─ Each Fargate task processes ONE chunk
├─ Writes to same chunk file (chunks/chunk-XXXX.ndjson)
└─ No file conflicts because each task has unique chunk ID

Phase 3: Merger (Sequential)
├─ Reads all chunks/chunk-*.ndjson files
├─ Concatenates into single previous-input.ndjson
└─ Deletes chunk files
```

### Key Benefits

1. **No race conditions**: Each task writes to a unique file
2. **Reuses existing code**: `integration-huron-person` handles processing
3. **Scalable**: N chunks = N parallel tasks
4. **Fault tolerant**: Failed chunks can be retried independently
5. **Agnostic core**: `integration-core` remains unaware of chunking

## Architecture

### Components

#### 1. Chunker (`docker/chunker.ts`)
**Purpose**: Split large NDJSON input into manageable chunks

**Input**:
- S3 source file (e.g., `example-client/input-data.json`)
- Chunk size (default: 1000 records)

**Output**:
- `chunks/chunk-0000.ndjson` (records 0-999)
- `chunks/chunk-0001.ndjson` (records 1000-1999)
- `chunks/chunk-NNNN.ndjson` (remaining records)

**Key Features**:
- Zero-padded chunk IDs ensure lexicographic sorting
- Deterministic chunk assignment (record N always in same chunk)
- Streaming to minimize memory usage

#### 2. Processor (`docker/processor.ts`)
**Purpose**: Process a single chunk using HuronPersonIntegration

**Input**:
- SQS message with S3 chunk location
- Environment variables for Huron API credentials

**Processing**:
1. Extract chunk ID from S3 key: `/chunk-(\d+)\.ndjson$/`
2. Build S3DataSourceConfig pointing to chunk file
3. Call `HuronPersonIntegration.run(taskName, chunkId)`
4. Delta strategy writes to `chunks/chunk-{chunkId}.ndjson`

**Output**:
- Updated chunk file with processed records
- Logs of sync operations

**Key Features**:
- Completely reuses `integration-huron-person` code
- Zero code duplication
- Each task processes exactly one chunk

#### 3. Merger (`docker/merger.ts`)
**Purpose**: Combine all processed chunks into final output

**Input**:
- All chunk files in `chunks/` prefix

**Processing**:
1. List all `chunks/chunk-*.ndjson` files
2. Sort by chunk ID for deterministic order
3. Stream each chunk to concatenate NDJSON
4. Write to `previous-input.ndjson`
5. Delete all chunk files (batched for S3 limits)

**Output**:
- Single `previous-input.ndjson` with all records
- Empty `chunks/` directory

**Key Features**:
- Streaming to handle large files
- Batched deletes (1000 files per API call)
- Comprehensive error handling
- Validates chunk file pattern

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1: Chunker (Single Task)                                  │
│                                                                  │
│  input-data.json (10,000 records)                               │
│         │                                                        │
│         ├─> chunks/chunk-0000.ndjson (1,000 records)           │
│         ├─> chunks/chunk-0001.ndjson (1,000 records)           │
│         ├─> chunks/chunk-0002.ndjson (1,000 records)           │
│         └─> ... chunks/chunk-0009.ndjson (1,000 records)       │
│                                                                  │
│  Publishes SQS messages for each chunk                          │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 2: Processors (10 Parallel Fargate Tasks)                 │
│                                                                  │
│  Task 1: chunks/chunk-0000.ndjson → Process → chunk-0000.ndjson│
│  Task 2: chunks/chunk-0001.ndjson → Process → chunk-0001.ndjson│
│  Task 3: chunks/chunk-0002.ndjson → Process → chunk-0002.ndjson│
│  ...                                                             │
│  Task 10: chunks/chunk-0009.ndjson → Process → chunk-0009.ndjson
│                                                                  │
│  Each task writes to unique chunk file (no conflicts)           │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 3: Merger (Single Task)                                   │
│                                                                  │
│  Read all chunks/chunk-*.ndjson files                           │
│         │                                                        │
│         ├─> Concatenate NDJSON lines                            │
│         ├─> Write to previous-input.ndjson                      │
│         └─> Delete chunk files                                  │
│                                                                  │
│  Output: previous-input.ndjson (10,000 records)                 │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Details

### Core Changes (integration-core)

To keep the core agnostic to chunking, we introduced **injectable output paths**:

#### FileStorage Changes
```typescript
export interface DeltaStrategyParams {
  // Existing fields...
  
  /**
   * Optional function to customize output file path
   * Allows chunked storage without core knowing about chunks
   */
  outputPath?: (defaultPath: string) => string;
}
```

**Pattern**:
```typescript
// Without chunking:
const path = 'example-client/previous-input.ndjson';

// With chunking (chunkId = '0042'):
const outputPath = (defaultPath: string) => 
  defaultPath.replace('previous-input.ndjson', 'chunks/chunk-0042.ndjson');

const path = outputPath('example-client/previous-input.ndjson');
// Result: 'example-client/chunks/chunk-0042.ndjson'
```

#### S3Storage Changes
```typescript
export interface DeltaStrategyParams {
  /**
   * Optional function to customize S3 key prefix
   * Allows chunked storage without core knowing about chunks
   */
  outputKeyPrefix?: (defaultPrefix: string) => string;
}
```

**Pattern**:
```typescript
// Without chunking:
const key = 'example-client/previous-input.ndjson';

// With chunking (chunkId = '0042'):
const outputKeyPrefix = (defaultKey: string) =>
  defaultKey.replace('previous-input.ndjson', 'chunks/chunk-0042.ndjson');

const key = outputKeyPrefix('example-client/previous-input.ndjson');
// Result: 'example-client/chunks/chunk-0042.ndjson'
```

### Huron-Person Changes

#### DeltaStrategyFactory Changes
```typescript
export const createStrategy = (
  config: Config,
  chunkId?: string  // Optional chunk ID for parallel processing
): DeltaStrategy => {
  // Create output path transformer when chunkId provided
  const outputPath = chunkId
    ? (defaultPath: string) => defaultPath.replace(
        'previous-input.ndjson',
        `chunks/chunk-${chunkId}.ndjson`
      )
    : undefined;
    
  const outputKeyPrefix = chunkId
    ? (defaultKey: string) => defaultKey.replace(
        'previous-input.ndjson',
        `chunks/chunk-${chunkId}.ndjson`
      )
    : undefined;

  return new DeltaByComparingNdjsonArrays({
    ...params,
    outputPath,
    outputKeyPrefix
  });
};
```

#### SyncPeople Changes
```typescript
export class SyncPeople {
  async run(taskName?: string, chunkId?: string): Promise<void> {
    // Pass chunkId to factory for chunked storage
    const strategy = DeltaStrategyFactory.createStrategy(
      this.config,
      chunkId
    );
    
    // Rest of processing unchanged
    await endToEnd.run();
  }
}
```

### Chunk ID Extraction

The processor extracts chunk IDs using a consistent regex pattern:

```typescript
export const extractChunkId = (s3Key: string): string | undefined => {
  const match = s3Key.match(/chunk-(\d+)\.ndjson$/);
  return match ? match[1] : undefined;
};

// Examples:
extractChunkId('data/chunk-0042.ndjson')         // '0042'
extractChunkId('bu/data/chunk-0001.ndjson')      // '0001'
extractChunkId('data/file.ndjson')               // undefined
extractChunkId('chunk0042.ndjson')                // undefined (no hyphen)
```

**Key Requirements**:
- Must end with `.ndjson`
- Must have hyphen separator: `chunk-`
- Must have numeric ID (any length, zero-padded)
- Pattern must be at end of string

### Configuration Building

The processor builds chunk-specific configurations:

```typescript
export const buildChunkConfig = (
  bucketName: string,
  s3Key: string,
  region?: string
): Config => {
  // Load base config from environment/filesystem
  const baseConfig = ConfigManager.getInstance()
    .reset()
    .fromEnvironment()
    .fromFileSystem()
    .getConfig('people');

  // Override data source to point to chunk file
  const s3DataSource: S3DataSourceConfig = {
    bucketName,
    key: s3Key,
    region: region || baseConfig.dataSource.people?.region || 'us-east-1',
    fieldsOfInterest: baseConfig.dataSource.people?.fieldsOfInterest
  };

  return {
    ...baseConfig,
    dataSource: {
      ...baseConfig.dataSource,
      people: s3DataSource
    }
  };
};
```

**Result**: Each processor task:
1. Reads from its assigned chunk file
2. Processes all records in that chunk
3. Writes output back to the same chunk file
4. No file conflicts with other tasks

## Testing

### Test Coverage Summary

| Module | Test File | Tests | Status |
|--------|-----------|-------|--------|
| chunker.ts | chunker.test.ts | 14 | ✅ Passing |
| processor.ts | processor.test.ts | 20 | ✅ Passing |
| merger.ts | merger.test.ts | 42 | ✅ Passing |
| BigJsonFile.ts | BigJsonFile.test.ts | Multiple | ✅ Passing |
| PersonArrayWrapper.ts | PersonArrayWrapper.test.ts | Multiple | ✅ Passing |
| **Total** | **5 suites** | **103** | **✅ All Passing** |

Additionally:
- integration-core: 302/302 tests passing (18 new for chunking)
- integration-huron-person: 789/789 tests passing (7 new for chunking)

### Chunker Tests

**File**: `test/chunker.test.ts` (14 tests)

Tests cover:
- Queue publishing to SQS
- Chunk size calculations
- Record distribution
- S3 key formatting
- Zero-padding consistency
- Error handling

### Processor Tests

**File**: `test/processor.test.ts` (20 tests)

Tests cover:
- Chunk ID extraction from S3 keys
  - Standard patterns
  - Nested paths
  - Edge cases (no hyphen, wrong extension)
- Input validation
  - Missing bucket name
  - Missing S3 key
  - Valid inputs
- Configuration building
  - S3 data source creation
  - Region handling
  - Base config preservation
- Output path transformations

### Merger Tests

**File**: `test/merger.test.ts` (42 tests)

Tests cover:
- Chunk file discovery
  - Pattern matching
  - Correct S3 prefix
  - Filtering non-chunk files
- Sorting
  - Lexicographic order
  - Zero-padding preservation
- Streaming
  - NDJSON concatenation
  - Large file handling (2500+ chunks)
  - Empty chunks
- Deletion
  - Batching (1000 files per call)
  - Error handling
- End-to-end merge scenarios

## Deployment Considerations

### Environment Variables

All three phases require standard environment variables:

```bash
# AWS Configuration
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...

# S3 Storage
INPUT_BUCKET=integration-bucket
CHUNKS_BUCKET=integration-bucket
CLIENT_ID=example-client

# Huron API (for processor only)
HURON_API_ENDPOINT=https://api.huron.com
JWT_PRIVATE_KEY_BASE64=...
JWT_ALGORITHM=RS256
```

### Chunker-Specific

```bash
# Input file location
INPUT_SOURCE_KEY=example-client/input-data.json

# Chunk configuration
CHUNK_SIZE=1000  # Records per chunk

# SQS Queue for coordination
QUEUE_URL=https://sqs.us-east-2.amazonaws.com/123456789/integration-queue
```

### Processor-Specific

```bash
# Provided by SQS message or environment
CHUNK_KEY=example-client/chunks/chunk-0042.ndjson
```

### Merger-Specific

```bash
# No additional variables required
# Uses CLIENT_ID to determine chunk prefix
```

### Orchestration

**Recommended flow**:

1. **Start Chunker**
   ```bash
   docker run --env-file .env chunker
   ```
   - Splits input into chunks
   - Publishes SQS messages (one per chunk)

2. **Fargate auto-scales Processors**
   - ECS service consumes SQS messages
   - Each message triggers one Fargate task
   - Tasks process chunks in parallel
   - Auto-scales based on queue depth

3. **Start Merger (after all processors complete)**
   ```bash
   docker run --env-file .env merger
   ```
   - Waits for all chunks to be processed
   - Merges into final output
   - Cleans up chunk files

**Determining completion**:
- Monitor SQS queue depth = 0
- All Fargate tasks in STOPPED state
- OR implement completion detection (all chunk files present)

### Scaling Recommendations

| Input Size | Chunk Size | Parallel Tasks | Estimated Time |
|------------|------------|----------------|----------------|
| 10,000 records | 1,000 | 10 | 5-10 min |
| 100,000 records | 1,000 | 100 | 10-20 min |
| 100,000 records | 5,000 | 20 | 8-15 min |
| 1,000,000 records | 5,000 | 200 | 30-60 min |

**Trade-offs**:
- Smaller chunks = more parallelism, more overhead
- Larger chunks = less parallelism, less overhead
- Optimal: 1000-5000 records per chunk

### Monitoring

**Key metrics**:
- SQS queue depth (processing progress)
- Fargate task count (parallelism)
- S3 chunk file count (completion tracking)
- API calls to Huron (rate limiting)
- Memory/CPU usage per task

**CloudWatch Logs**:
- Chunker: Chunk count, SQS publish success
- Processor: Chunk ID, processing time, sync results
- Merger: Chunk count, merge time, deletion success

### Error Handling

**Chunker failures**:
- Retry: Chunker is idempotent (overwrites chunks)
- Impact: No chunks produced, entire pipeline stalled

**Processor failures**:
- Retry: Process only failed chunks
- Impact: Partial data loss if not retried
- Detection: Missing chunk files

**Merger failures**:
- Retry: Merger is idempotent (overwrites previous-input.ndjson)
- Impact: No final output, chunk files remain

**Recommendations**:
- Enable SQS message visibility timeout (e.g., 300s)
- Configure SQS DLQ for failed messages
- Implement chunk completion tracking
- Monitor for orphaned chunk files

### Cost Optimization

**S3 costs**:
- Chunk files are temporary (cleaned up by merger)
- Use S3 Lifecycle policy for orphaned chunks (delete after 7 days)

**Fargate costs**:
- Right-size task CPU/memory based on chunk processing time
- Use Spot instances if processing can tolerate interruptions

**API costs**:
- Bulk sync API calls (one per chunk) reduce total API calls
- Monitor rate limits and throttling

## Configuration Example

### Complete .env file

```bash
# AWS Configuration
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# S3 Storage
INPUT_BUCKET=integration-huron-person-data
CHUNKS_BUCKET=integration-huron-person-data
CLIENT_ID=example-client

# Chunking Configuration
INPUT_SOURCE_KEY=example-client/input-data.json
CHUNK_SIZE=1000

# SQS Queue
QUEUE_URL=https://sqs.us-east-2.amazonaws.com/123456789012/integration-queue

# Huron API Configuration
HURON_API_ENDPOINT=https://api.huron.com/v1
JWT_PRIVATE_KEY_BASE64=<base64-encoded-private-key>
JWT_ALGORITHM=RS256
JWT_KEY_ID=key-12345
JWT_ISSUER=integration-service
JWT_AUDIENCE=huron-api
JWT_SUBJECT=service-account@integration
JWT_EXPIRATION_MINUTES=60
```

## Summary

The chunking architecture solves the race condition in parallel Fargate processing by:

1. **Isolation**: Each task writes to a unique chunk file
2. **Reusability**: Leverages existing `integration-huron-person` code
3. **Scalability**: Processes N chunks in parallel
4. **Simplicity**: Core remains agnostic via injectable output paths
5. **Testability**: 103 tests validate all components

The solution is production-ready with comprehensive test coverage and deployment guidance.
