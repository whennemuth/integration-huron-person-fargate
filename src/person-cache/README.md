# Person Cache Module

Person cache stores sourceIdentifiers (BUIDs) of all persons currently in the target system. Used during `bulkReset=true` mode to determine CREATE vs PATCH operations without querying target API for each person.

## Architecture

### Template Method Pattern

Abstract base class (`AbstractPersonCache`) defines interface, concrete implementations provide storage-specific behavior.

**Class Hierarchy**:
```
AbstractPersonCache (abstract)
├── PersonCacheForS3 (concrete - production)
└── PersonCacheForDynamoDb (facade - delegates to S3)
```

### Strategy Pattern for Target Access

**PersonTarget Strategy** injects person data source via dependency injection:

```
AbstractPersonTarget (interface)
├── PersonTargetReal → Queries real Huron API via ListPeople
└── PersonTargetMocked → Scans mockTargetPersonTable (DynamoDB) for test data
```

**Benefits**:
- **Single Responsibility**: PersonTargetReal handles API, PersonTargetMocked handles DynamoDB
- **Open/Closed**: Add new sources without modifying cache classes
- **Dependency Inversion**: Cache depends on abstraction, not concrete implementations
- **Testability**: Easily mock personTarget without environment setup
- **Separation of Concerns**: Target access logic separated from cache storage

**Factory Wiring**:
```typescript
// PersonCacheFactory.create(config, useMockTarget)
const personTarget = useMockTarget ? 
  new PersonTargetMocked() :   // ← Test mode: DynamoDB
  new PersonTargetReal();       // ← Production: Huron API

return new PersonCacheForS3({ config, personTarget });
```

### Why S3 for Cache Storage?

After architectural analysis, S3 was chosen over DynamoDB:

| Factor | S3 | DynamoDB |
|--------|----|-----------| 
| **Data Size** | ✅ Ideal for 10K-200K BUIDs | ⚠️ Requires 10K+ individual records |
| **Access Pattern** | ✅ Write once, read once per sync | ⚠️ Optimized for random access |
| **Cost** | ✅ Single PUT + GET | ⚠️ Write cost × record count |
| **Simplicity** | ✅ Plain text, easy to debug | ⚠️ Batch ops, pagination |
| **Performance** | ✅ Sequential read is fast | ⚠️ Batch queries needed |

### PersonCacheForDynamoDb Facade

`PersonCacheForDynamoDb` currently delegates to `PersonCacheForS3`. This maintains abstraction layer while using optimal storage.

**Future Implementation**: If requirements change (incremental updates, cross-sync persistence, query filtering), replace facade with true DynamoDB operations. See class comments for implementation guidance.

## Race Condition Prevention

### Problem
When queue seeding sends 15+ messages simultaneously, multiple chunker tasks start concurrently. Without coordination, all tasks would attempt to create the cache simultaneously, causing:
- Redundant expensive API calls (fetching 10K-200K person records multiple times)
- Wasted compute resources
- Potential API rate limiting

### Solution: Marker File Lock Pattern

**Lock File**: `_personCache.creating`
- Created before cache creation begins
- Contains metadata: `{ taskId, timestamp, expiresAt }` (10-minute TTL)
- Deleted after cache creation completes

**Coordination Logic**:
```
Task 1: Cache exists? No → Lock exists? No → Acquire lock → Create cache → Release lock ✅
Task 2: Cache exists? No → Lock exists? Yes → Skip (wait for Task 1) ⏳
Task 3: Cache exists? Yes → Skip (Task 1 finished) ✅
```

**Key Methods**:
- `ensureCache()` - High-level: Check cache, coordinate creation if needed
- `acquireCacheLock()` - Low-level: Try to acquire lock marker file
- `releaseCacheLock()` - Low-level: Delete lock marker file
- `isLockActive()` - Low-level: Check if lock exists and hasn't expired

### ensureCache() - Thread-Safe Cache Initialization

**Recommended API** for cache creation:

```typescript
const result = await cache.ensureCache({ 
  bucketName: 'my-bucket', 
  key: 'chunks/person-full/2026-08-13/_personCache.txt', 
  region: 'us-east-2' 
});

// Result: { existed: boolean, created: boolean, skipped: boolean }
if (result.created) {
  console.log('✅ This task created the cache');
} else if (result.existed) {
  console.log('✅ Cache already existed');
} else if (result.skipped) {
  console.log('⏳ Another task is creating cache');
}
```

**Behavior**:
1. **Cache exists**: Returns immediately `{ existed: true }`
2. **No cache, lock active**: Skips gracefully `{ skipped: true }`
3. **No cache, no lock**: Acquires lock → Creates cache → Releases lock `{ created: true }`
4. **Error during creation**: Lock is released in finally block (self-healing)

**Benefits**:
- Encapsulates all lock coordination logic
- Thread-safe for concurrent callers
- Simple one-line API for chunker
- Automatic lock cleanup on errors

## Usage

### Factory Pattern (Recommended)

```typescript
import { PersonCacheFactory } from './person-cache/PersonCacheFactory';

// Production mode: Query real Huron API
const cache = PersonCacheFactory.create(config, false);

// Mock target mode: Query mockTargetPersonTable (DynamoDB)
const mockCache = PersonCacheFactory.create(config, true);

// Thread-safe cache creation (handles concurrent tasks)
await cache.ensureCache({ 
  bucketName: 'my-bucket', 
  key: 'chunks/person-full/2026-08-13/_personCache.txt', 
  region: 'us-east-2' 
});

// Read cache
const buids = await cache.getCache({ 
  bucketName: 'my-bucket', 
  key: 'chunks/person-full/2026-08-13/_personCache.txt', 
  region: 'us-east-2' 
});

console.log(`Cache contains ${buids.size} BUIDs`);
```

### Direct Instantiation with Injected Strategy

```typescript
import { PersonCacheForS3 } from './person-cache/PersonCacheForS3';
import { PersonTargetReal, PersonTargetMocked } from './person-cache/PersonTarget*';

// With real target
const personTarget = new PersonTargetReal();
const cache = new PersonCacheForS3({ config, personTarget });

// With mocked target
const mockTarget = new PersonTargetMocked();
const mockCache = new PersonCacheForS3({ config, personTarget: mockTarget });

await cache.ensureCache({ bucketName, key, region });
```

### Low-Level Lock Management (Advanced)

For custom coordination scenarios (not needed in normal usage):

```typescript
// Check if lock is active
if (await cache.isLockActive({ bucketName, key, region })) {
  console.log('Another task is creating cache');
}

// Manual lock acquisition (use ensureCache() instead)
if (await cache.acquireCacheLock({ bucketName, key, region })) {
  try {
    await cache.setCache({ bucketName, key, region });
  } finally {
    await cache.releaseCacheLock({ bucketName, key, region });
  }
}
```

## Cache Lifecycle

1. **Chunker Phase**: Ensures cache exists at start if `bulkReset=true`
   - **Concurrent coordination**: 15+ tasks may start simultaneously
   - **One task creates**: Uses `ensureCache()` with marker file locks
   - **Other tasks skip**: Wait for cache to be ready or see it already exists
   - Fetches full population from target API (expensive, done once)
   - Writes sourceIdentifiers to S3 as newline-delimited text

2. **Processor Phase**: Reads cache for CREATE vs PATCH decisions
   - **Exponential backoff**: If cache creation in progress, waits up to 2 minutes
   - Loads cache into `Set<string>` for O(1) lookup
   - For each person: `cache.has(buid)` → PATCH, else → CREATE

3. **Cleanup**: Cache is sync-scoped (stored under chunkDirectory)
   - No need to persist between syncs
   - Can be deleted after merger completes
   - Lock files auto-expire after 10 minutes (self-healing)

## File Format

**S3 Cache File** (`_personCache.txt`):
```
U12345678
U12345679
U12345680
...
```

**Characteristics**:
- One BUID per line
- Plain text UTF-8
- Typical size: ~10 bytes × record count
- Example: 50,000 BUIDs = ~500KB file

**S3 Metadata**:
```json
{
  "recordCount": "50000",
  "createdAt": "2026-08-13T10:30:00.000Z"
}
```

**S3 Lock File** (`_personCache.creating`):
```json
{
  "taskId": "arn:aws:ecs:us-east-2:123456789:task/cluster/abc123",
  "timestamp": "2026-08-13T10:30:00.000Z",
  "expiresAt": 1723550400000
}
```

**Lock File Characteristics**:
- Marker file indicates cache creation in progress
- TTL: 10 minutes (600,000 ms)
- Auto-expires if task crashes (self-healing)
- Deleted after successful cache creation

## Error Handling

### Cache Not Found (Processor)
```
⚠️  Cache file not found at s3://bucket/key
💡 Waiting for cache creation (checking for lock)...
```

**Behavior**: Waits with exponential backoff if lock active, throws error after 2 minutes

### Cache Empty
```
⚠️  No people retrieved from target API - cache will be empty
```

**Behavior**: Writes empty cache file, all persons treated as CREATE

### Missing sourceIdentifiers
```
⚠️  Skipped 5 people without sourceIdentifier
```

**Behavior**: Logs warning, continues with remaining records

### Lock Expired
```
⏱️  Cache lock expired (created at 2026-08-13T10:30:00Z)
```

**Behavior**: Lock file ignored, treated as if no lock exists (self-healing)

### Cache Creation Error
```
❌ Error creating cache: API request failed
✓ Released cache lock after error
```

**Behavior**: Lock released in finally block, other tasks can retry

## Test Harness

**File**: `PersonCacheForS3.ts` (harness at end of file)

**Environment Variables**:
```bash
PERSON_CACHE_PERSON_CACHE_BUCKET_NAME=my-bucket
PERSON_CACHE_PERSON_CACHE_KEY=chunks/person-full/2026-08-13/cache.txt
PERSON_CACHE_REGION=us-east-2
PERSON_CACHE_OUTPUT_FILE_PATH=./cache-output.txt  # Optional for local file
```

**Execution**:
```bash
npx ts-node src/person-cache/PersonCacheForS3.ts
```

## API Reference

### AbstractPersonCache Methods

#### High-Level API (Recommended)
- `ensureCache(params)` - Thread-safe cache initialization with lock coordination

#### Storage Operations
- `cacheExists(params)` - Check if cache file exists
- `setCache(params)` - Write cache file (expensive: fetches from API)
- `getCache(params)` - Read cache file, returns `Set<string>`
- `writeToFile(filePath)` - Local file export (testing/debugging)

#### Lock Management (Low-Level)
- `acquireCacheLock(params)` - Try to create lock marker file
- `releaseCacheLock(params)` - Delete lock marker file
- `isLockActive(params)` - Check lock exists and hasn't expired

**Note**: Use `ensureCache()` instead of manual lock management unless you have a specific advanced use case.

## Design Decisions

### Why Not DynamoDB?

**Considered Options**:
1. Individual records: `{ PK: syncRunId, SK: "CACHE_{buid}", personId: buid }`
2. Chunked arrays: `{ PK: syncRunId, SK: "CACHE_CHUNK_0000", personIds: [array] }`
3. Compressed JSON: `{ PK: syncRunId, SK: "CACHE", buids: [array] }`

**Rejected because**:
- Cache is large bulk data (10K-200K records)
- Sequential access pattern (no random lookups)
- Temporary (sync-scoped, no persistence needed)
- S3 is simpler, cheaper, faster for this use case

### Why Maintain DynamoDB Facade?

**Rationale**:
1. **Architectural consistency**: All storage switching uses same pattern
2. **Future flexibility**: Requirements may change (e.g., incremental updates)
3. **Clean abstraction**: Consumers depend on interface, not implementation
4. **Zero cost**: Facade adds negligible overhead

**Trade-off**: Extra layer of indirection vs architectural flexibility → flexibility wins

## Related Files

- `docker/chunker.ts` - Calls `ensureCache()` during Phase 1 (simplified to 1 line)
- `docker/processor.ts` - Reads cache during Phase 2 with exponential backoff
- `PersonCacheLookup.ts` - Processor-side lazy loading with wait logic
- `AbstractHashStorage.ts` - Similar template pattern for hash storage
- `lib/DynamoDB.ts` - CDK table definitions (no cache table needed)

## Design Evolution

**v1**: Simple `setCache()` - No coordination, race conditions occurred  
**v2**: Lock methods added - Manual lock management in chunker (50+ lines)  
**v3**: `ensureCache()` method - Encapsulated coordination, chunker simplified to 1 line ✅
