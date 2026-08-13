# Metadata Management Abstraction

## Purpose

Manages two critical metadata types used in the 3-phase ECS Fargate chunking pipeline:
1. **FLAGS**: Early configuration flags for processors (written before chunking begins)
2. **METADATA**: Run manifest and configuration for post-run diagnostics (written after chunking completes)
3. **TERMINAL_ERROR**: Catastrophic failure markers preventing normal pipeline completion

## Template Pattern Implementation

The metadata module uses the template pattern with abstract base class and concrete implementations:

```
AbstractMetadata (abstract base)
├── MetadataForS3 (file-based implementation)
└── MetadataForDynamoDb (table-based implementation)
```

### AbstractMetadata (19 methods)

**5 Static Path Helpers**:
- `deriveDeltaStoragePath(chunkDirectory): string` - Extract delta storage path from chunk directory
- `deriveChunkDirectory(deltaStoragePath): string` - Derive chunk directory from delta storage path
- `getMetadataKey(chunkDirectory): string` - Get S3 key or DynamoDB PK for metadata
- `getFlagsKey(chunkDirectory): string` - Get S3 key or DynamoDB PK for flags
- `getTerminalErrorKey(chunkDirectory): string` - Get S3 key or DynamoDB PK for terminal error
- `extractSyncRunId(chunkDirectory): string` - Extract ISO timestamp from chunk directory for DynamoDB PK

**14 Abstract Instance Methods**:
- `write(params: WriteMetadataParams): Promise<void>` - Write metadata record
- `writeFlags(params: WriteFlagsParams): Promise<void>` - Write flags record (early in chunking)
- `read(params: ReadMetadataParams): Promise<ChunkMetadata>` - Read metadata record
- `readFlags(params: ReadFlagsParams): Promise<Flags>` - Read flags record
- `markRunFailed(params: MarkRunFailedParams): Promise<void>` - Write terminal error marker
- `isRunFailed(params: ReadFlagsParams): Promise<boolean>` - Check if run failed
- `terminalErrorExists(params: ReadTerminalErrorParams): Promise<boolean>` - Check terminal error existence
- `readTerminalError(params: ReadTerminalErrorParams): Promise<TerminalError | null>` - Read terminal error details
- `readFlagsFromChunkKey(bucketName: string, chunkS3Key: string, region?: string): Promise<Flags>` - Read flags from chunk S3 key
- `readFromChunkKey(bucketName: string, chunkS3Key: string, region?: string): Promise<ChunkMetadata>` - Read metadata from chunk S3 key
- `listChunkFiles(bucketName: string, chunkDirectory: string, region?: string): Promise<string[]>` - List all chunk files in directory
- `computeTotalRecords(bucketName: string, chunkKeys: string[], region?: string): Promise<number>` - Count total records across chunks
- `buildAggregatedMetadata(bucketName: string, chunkDirectory: string, region?: string): Promise<{chunkCount, totalRecords, chunkKeys}>` - Build aggregated metadata from chunks
- `validateMetadata(metadata: ChunkMetadata): boolean` - Validate metadata structure

## MetadataForS3 (File-Based Implementation)

### Storage Format

**Metadata File** (`_metadata.json`):
```json
{
  "itemsPerChunk": 500,
  "source": "BU CDM People API",
  "target": "Huron Target System",
  "chunkDirectory": "chunks/PersonFull/2024-01-15T10:30:00.000Z",
  "deltaStoragePath": "delta-storage/PersonFull",
  "bulkReset": false,
  "trustPreviousStorage": true,
  "syncPopulation": "PersonFull",
  "createdAt": "2024-01-15T10:30:00.000Z"
}
```

**Flags File** (`_flags.json`):
```json
{
  "bulkReset": false,
  "trustPreviousStorage": true,
  "syncPopulation": "PersonFull"
}
```

**Terminal Error File** (`_terminal_error.json`):
```json
{
  "timestamp": "2024-01-15T10:35:00.000Z",
  "errorMessage": "Failed to fetch source data",
  "errorStack": "Error: Network timeout\n  at ...",
  "phase": "chunking",
  "chunkDirectory": "chunks/PersonFull/2024-01-15T10:30:00.000Z"
}
```

### File Locations

All files stored under: `s3://{bucket}/chunks/{populationType}/{timestamp}/`

Example:
- `s3://my-bucket/chunks/PersonFull/2024-01-15T10:30:00.000Z/_metadata.json`
- `s3://my-bucket/chunks/PersonFull/2024-01-15T10:30:00.000Z/_flags.json`
- `s3://my-bucket/chunks/PersonFull/2024-01-15T10:30:00.000Z/_terminal_error.json`
- `s3://my-bucket/chunks/PersonFull/2024-01-15T10:30:00.000Z/chunk-0000.ndjson`
- `s3://my-bucket/chunks/PersonFull/2024-01-15T10:30:00.000Z/chunk-0001.ndjson`
- ...

### Implementation Details

- Uses `S3Client` for file operations (GetObject, PutObject, ListObjectsV2)
- `listChunkFiles()`: Paginates through S3 objects matching `chunk-*.ndjson` pattern
- `computeTotalRecords()`: Streams NDJSON files and counts lines (memory-efficient)
- `buildAggregatedMetadata()`: Discovers all chunks via S3 ListObjectsV2
- **Static wrapper methods**: All 13 instance methods have static wrappers for backward compatibility

### Backward Compatibility

Original code called `MetadataManager.write()` as static methods. MetadataForS3 provides static wrappers:

```typescript
// Old code (still works):
import { MetadataForS3 } from './metadata';
const MetadataManager = MetadataForS3;
await MetadataManager.write({ bucketName, chunkDirectory, ... });

// New code (instance-based):
const metadata = new MetadataForS3({ config });
await metadata.write({ bucketName, chunkDirectory, ... });
```

## MetadataForDynamoDb (Table-Based Implementation)

### Storage Format

Uses **StatisticsTable** with `eventType` field to differentiate record types:

**Metadata Record** (eventType = "METADATA"):
```
PK: "2024-01-15T10:30:00.000Z"  // syncRunId (integrationTimestamp)
SK: "METADATA"
Attributes: {
  itemsPerChunk: 500,
  source: "BU CDM People API",
  target: "Huron Target System",
  chunkDirectory: "chunks/PersonFull/2024-01-15T10:30:00.000Z",
  deltaStoragePath: "delta-storage/PersonFull",
  bulkReset: false,
  trustPreviousStorage: true,
  syncPopulation: "PersonFull",
  createdAt: "2024-01-15T10:30:00.000Z"
}
```

**Flags Record** (eventType = "FLAGS"):
```
PK: "2024-01-15T10:30:00.000Z"
SK: "FLAGS"
Attributes: {
  bulkReset: false,
  trustPreviousStorage: true,
  syncPopulation: "PersonFull"
}
```

**Terminal Error Record** (eventType = "TERMINAL_ERROR"):
```
PK: "2024-01-15T10:30:00.000Z"
SK: "TERMINAL_ERROR"
Attributes: {
  timestamp: "2024-01-15T10:35:00.000Z",
  errorMessage: "Failed to fetch source data",
  errorStack: "Error: Network timeout\n  at ...",
  phase: "chunking",
  chunkDirectory: "chunks/PersonFull/2024-01-15T10:30:00.000Z"
}
```

### Key Extraction

The `syncRunId` (DynamoDB PK) is extracted from the chunk directory timestamp:

```typescript
chunkDirectory: "chunks/PersonFull/2024-01-15T10:30:00.000Z"
                                    ^^^^^^^^^^^^^^^^^^^^^^^^
                                    syncRunId (ISO timestamp)
```

This is handled by `AbstractMetadata.extractSyncRunId(chunkDirectory)`.

### Implementation Details

- Uses `StatisticsTable` utility class (wraps DynamoDB Document Client)
- `write()`: Calls `StatisticsTable.writeEventRecord({ eventType: 'METADATA', ... })`
- `writeFlags()`: Calls `StatisticsTable.writeEventRecord({ eventType: 'FLAGS', ... })`
- `markRunFailed()`: Calls `StatisticsTable.writeEventRecord({ eventType: 'TERMINAL_ERROR', ... })`
- `read()`: Queries StatisticsTable with PK=syncRunId, SK="METADATA"
- `readFlags()`: Queries StatisticsTable with PK=syncRunId, SK="FLAGS"

### Not Applicable Methods

Three methods are **not applicable** in DynamoDB mode and throw errors:
- `listChunkFiles()` - Chunk files always remain in S3, not DynamoDB
- `computeTotalRecords()` - Must read NDJSON files from S3 regardless of storage mode
- `buildAggregatedMetadata()` - Requires S3 file listing

These methods are S3-specific utilities for chunk discovery and are not needed when metadata is in DynamoDB (chunk files still in S3).

## Factory Pattern

### MetadataFactory

Switches between implementations based on `config.storage.type`:

```typescript
import { MetadataFactory } from './metadata';

const metadata = MetadataFactory.create(config);
// Returns: MetadataForS3 if config.storage.type is 's3'|'file'|'database'
// Returns: MetadataForDynamoDb if config.storage.type is 'dynamodb'
```

### Consumer Pattern

Consumers (chunker.ts, processor.ts, merger.ts) use backward compatibility aliasing:

```typescript
import { MetadataForS3 } from '../src/chunking/metadata';
const MetadataManager = MetadataForS3;  // Alias for minimal code change

// Usage (still works as before):
await MetadataManager.write({ bucketName, chunkDirectory, ... });
await MetadataManager.writeFlags({ bucketName, chunkDirectory, bulkReset, ... });
const flags = await MetadataManager.readFlags({ bucketName, chunkDirectory });
```

**Migration Path**: Replace `MetadataManager` with `MetadataFactory.create(config)` to enable dynamic switching.

## Pipeline Flow

### Phase 1: Chunker (docker/chunker.ts)

1. **Write Flags Early** (before chunking begins):
   ```typescript
   await MetadataManager.writeFlags({
     bucketName, chunkDirectory,
     bulkReset, trustPreviousStorage, syncPopulation
   });
   ```
   
2. **Create Chunk Files** (`chunk-0000.ndjson`, `chunk-0001.ndjson`, ...)

3. **Write Metadata** (after chunking completes):
   ```typescript
   await MetadataManager.write({
     bucketName, chunkDirectory, itemsPerChunk,
     source, target, bulkReset, trustPreviousStorage, syncPopulation
   });
   ```

4. **On Failure** (catastrophic error):
   ```typescript
   await MetadataManager.markRunFailed({
     bucketName, chunkDirectory,
     runFailureMessage: error.message,
     runFailureTimestamp: new Date().toISOString()
   });
   ```

### Phase 2: Processor (docker/processor.ts)

1. **Read Flags** (for sync configuration):
   ```typescript
   const flags = await MetadataManager.readFlagsFromChunkKey(
     bucketName, chunkS3Key, region
   );
   // Use flags.bulkReset, flags.syncPopulation in processing logic
   ```

2. **Process Chunk** (apply data mapping, delta logic)

3. **Write Results** (to S3 or DynamoDB depending on storage mode)

### Phase 3: Merger (docker/merger.ts)

1. **Read Metadata** (to get run context):
   ```typescript
   const metadata = await MetadataManager.read({
     bucketName, chunkDirectory
   });
   ```

2. **Read Flags** (for bulkReset behavior):
   ```typescript
   const flags = await MetadataManager.readFlags({
     bucketName, chunkDirectory
   });
   ```

3. **Check Terminal Error** (before proceeding):
   ```typescript
   const hasFailed = await MetadataManager.terminalErrorExists({
     bucketName, chunkDirectory
   });
   if (hasFailed) {
     const error = await MetadataManager.readTerminalError({
       bucketName, chunkDirectory
     });
     // Handle failure scenario
   }
   ```

4. **Merge Results** (consolidate processing output)

## Testing

### Running Metadata Tests

```bash
# Run all metadata tests
npm test -- Metadata.test.ts

# Run specific test
npm test -- Metadata.test.ts -t "should write and read metadata"
```

### Test Coverage

- Metadata write/read operations
- Flags write/read operations
- Terminal error marking and detection
- Chunk file listing and record counting
- Aggregated metadata building
- Error handling (missing files, invalid JSON)

## Common Issues

### "Metadata file not found"

**Cause**: Processor tasks reading flags before chunker has written them.

**Solution**: Chunker writes flags **immediately** (before chunking begins) to avoid this race condition.

### "Cannot read property 'bulkReset' of undefined"

**Cause**: Flags structure mismatch or corrupted JSON.

**Solution**: Validate flags structure with `AbstractMetadata.validateFlags()` (if implemented).

### "chunkDirectory required but not provided"

**Cause**: Missing `chunkDirectory` parameter in read operations.

**Solution**: Ensure all read operations pass `chunkDirectory` from chunk S3 key or environment variable.

## Future Enhancements

1. **Validation Methods**: Add `validateMetadata()` and `validateFlags()` implementations
2. **Retry Logic**: Add exponential backoff for DynamoDB throttling
3. **Caching**: Cache frequently-read flags in memory (processor optimization)
4. **Compression**: Compress large metadata payloads (S3 mode)
5. **Versioning**: Support metadata schema versioning for backward compatibility
