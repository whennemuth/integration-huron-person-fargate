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
└── PersonTargetMocked → Scans MockTargetStateTable (DynamoDB) for test data
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
| **Data Size** | ✅ Ideal for 10K-100K BUIDs | ⚠️ Requires 10K+ individual records |
| **Access Pattern** | ✅ Write once, read once per sync | ⚠️ Optimized for random access |
| **Cost** | ✅ Single PUT + GET | ⚠️ Write cost × record count |
| **Simplicity** | ✅ Plain text, easy to debug | ⚠️ Batch ops, pagination |
| **Performance** | ✅ Sequential read is fast | ⚠️ Batch queries needed |

### PersonCacheForDynamoDb Facade

`PersonCacheForDynamoDb` currently delegates to `PersonCacheForS3`. This maintains abstraction layer while using optimal storage.

**Future Implementation**: If requirements change (incremental updates, cross-sync persistence, query filtering), replace facade with true DynamoDB operations. See class comments for implementation guidance.

## Usage

### Factory Pattern (Recommended)

```typescript
import { PersonCacheFactory } from './person-cache/PersonCacheFactory';

// Production mode: Query real Huron API
const cache = PersonCacheFactory.create(config, false);

// Mock target mode: Query MockTargetStateTable (DynamoDB)
const mockCache = PersonCacheFactory.create(config, true);

// Write cache
await cache.setCache({ 
  bucketName: 'my-bucket', 
  key: 'chunks/person-full/2026-08-13/cache.txt', 
  region: 'us-east-2' 
});

// Read cache
const buids = await cache.getCache({ 
  bucketName: 'my-bucket', 
  key: 'chunks/person-full/2026-08-13/cache.txt', 
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

await cache.setCache({ bucketName, key, region });
```

## Cache Lifecycle

1. **Chunker Phase**: Writes cache at start if `bulkReset=true`
   - Fetches full population from target API
   - Writes sourceIdentifiers to S3 as newline-delimited text

2. **Processor Phase**: Reads cache for CREATE vs PATCH decisions
   - Loads cache into `Set<string>` for O(1) lookup
   - For each person: `cache.has(buid)` → PATCH, else → CREATE

3. **Cleanup**: Cache is sync-scoped (stored under chunkDirectory)
   - No need to persist between syncs
   - Can be deleted after merger completes

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

## Error Handling

### Cache Not Found
```
⚠️  Cache file not found at s3://bucket/key
💡 This is expected if bulkReset was not enabled or chunker did not complete
```

**Behavior**: Returns empty `Set<string>`, all persons treated as CREATE

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

## Design Decisions

### Why Not DynamoDB?

**Considered Options**:
1. Individual records: `{ PK: syncRunId, SK: "CACHE_{buid}", personId: buid }`
2. Chunked arrays: `{ PK: syncRunId, SK: "CACHE_CHUNK_0000", personIds: [array] }`
3. Compressed JSON: `{ PK: syncRunId, SK: "CACHE", buids: [array] }`

**Rejected because**:
- Cache is large bulk data (10K-100K records)
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

- `docker/chunker.ts` - Writes cache during Phase 1
- `docker/processor.ts` - Reads cache during Phase 2  
- `AbstractHashStorage.ts` - Similar template pattern for hash storage
- `lib/DynamoDB.ts` - CDK table definitions (no cache table needed)
