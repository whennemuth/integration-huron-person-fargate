# integration-huron-person-fargate/src/chunking: Chunk Generation Patterns

## Purpose
Implements streaming chunk generation from large data sources into sized NDJSON chunks for parallel processing.

## Harnesses (5 total)

### 1. ChunkFromAPI
**Purpose**: Stream data from REST API into chunks

**Environment Prefix**: CHUNK_FROM_API

**Location**: `fetch/ChunkFromAPI.ts`

**Pattern**:
1. Make paginated API requests
2. Stream response as NDJSON
3. Split into chunks of configured size
4. Upload each chunk to S3

**Configuration**:
```bash
CHUNK_FROM_API_BASE_URL=<datasource-url>       # Shared: DATASOURCE_BASE_URL
CHUNK_FROM_API_API_KEY=<api-key>               # Shared: DATASOURCE_API_KEY
CHUNK_FROM_API_CHUNK_SIZE=100                  # Records per chunk
CHUNK_FROM_API_TIMEOUT=30000                   # Request timeout
CHUNK_FROM_API_PAGE_SIZE=1000                  # Records per API page
```

### 2. ChunkFromS3
**Purpose**: Load data from S3 file drop into chunks

**Environment Prefix**: CHUNK_FROM_S3

**Location**: `filedrop/ChunkFromS3.ts`

**Pattern**:
1. List S3 files in drop location
2. Read file (JSON array or NDJSON)
3. Parse and split into chunks
4. Upload chunks to processing location

**Configuration**:
```bash
CHUNK_FROM_S3_BUCKET=<drop-bucket>             # Source bucket
CHUNK_FROM_S3_KEY_PREFIX=incoming/             # Drop location
CHUNK_FROM_S3_CHUNK_SIZE=100
CHUNK_FROM_S3_TIMEOUT=60000
```

### 3. BigJsonFetch
**Purpose**: Stream API response as JSON to avoid memory overload

**Environment Prefix**: BIG_JSON_FETCH

**Location**: `fetch/BigJsonFetch.ts`

**Implementation**:
- Use streaming HTTP response
- Parse JSON incrementally
- Yield records as they're parsed
- Don't load full response into memory

### 4. BigJsonFile
**Purpose**: Stream file reading for large JSON/NDJSON files

**Environment Prefix**: BIG_JSON_FILE

**Location**: `filedrop/BigJsonFile.ts`

**Supports**:
- JSON arrays: `[{...}, {...}, ...]`
- NDJSON: `{...}\n{...}\n{...}`
- Streaming from S3 or local filesystem

### 5. PersonArrayWrapper
**Purpose**: Convert arrays to NDJSON and manage chunking logic

**Environment Prefix**: PERSON_ARRAY_WRAPPER

**Location**: `PersonArrayWrapper.ts`

**Operations**:
```typescript
// Input: Array
const persons = [{id: '1', ...}, {id: '2', ...}, ...];

// Process: Convert to NDJSON chunks
const chunks = wrapper.splitIntoChunks(persons, 100);

// Output: NDJSON format
// chunk-0001.ndjson: {"id":"1",...}\n{"id":"2",...}\n...
// chunk-0002.ndjson: {"id":"101",...}\n...
```

## Chunking Workflow

```
Data Source (API or S3)
        ↓
Stream/Read Data
        ↓
Parse Records
        ↓
Accumulate to Chunk Size
        ↓
Convert to NDJSON
        ↓
Upload to S3
        ↓
Signal Ready (SQS message)
```

## Implementation: ChunkFromAPI

```typescript
export class ChunkFromAPI {
  async chunk(): Promise<{ chunkCount: number; totalRecords: number }> {
    const chunkSize = parseInt(process.env.CHUNK_FROM_API_CHUNK_SIZE || '100');
    const pageSize = parseInt(process.env.CHUNK_FROM_API_PAGE_SIZE || '1000');
    
    let chunkCount = 0;
    let totalRecords = 0;
    let currentChunk: Person[] = [];
    
    // Pagination loop
    for (let offset = 0; ; offset += pageSize) {
      console.log(`Fetching page at offset ${offset}...`);
      
      const persons = await this.fetchPage(offset, pageSize);
      
      if (persons.length === 0) {
        break;  // No more data
      }
      
      // Add persons to current chunk
      currentChunk.push(...persons);
      totalRecords += persons.length;
      
      // Flush chunk if full
      while (currentChunk.length >= chunkSize) {
        const chunk = currentChunk.splice(0, chunkSize);
        await this.uploadChunk(chunk, chunkCount++);
      }
    }
    
    // Flush remaining records
    if (currentChunk.length > 0) {
      await this.uploadChunk(currentChunk, chunkCount++);
    }
    
    return { chunkCount, totalRecords };
  }
  
  private async fetchPage(offset: number, limit: number): Promise<Person[]> {
    const response = await axios.get(
      `${process.env.DATASOURCE_BASE_URL}/api/persons`,
      {
        params: { offset, limit },
        headers: {
          'x-api-key': process.env.DATASOURCE_API_KEY
        },
        timeout: parseInt(process.env.CHUNK_FROM_API_TIMEOUT || '30000')
      }
    );
    
    return response.data;
  }
  
  private async uploadChunk(persons: Person[], chunkIndex: number): Promise<void> {
    const ndjson = persons
      .map(p => JSON.stringify(p))
      .join('\n');
    
    const key = `chunks/chunk-${String(chunkIndex + 1).padStart(4, '0')}.ndjson`;
    
    console.log(`Uploading ${persons.length} records to ${key}`);
    
    await this.s3Client.putObject({
      Bucket: process.env.S3_BUCKET,
      Key: key,
      Body: ndjson,
      ContentType: 'application/x-ndjson'
    });
    
    // Signal processor that chunk is ready
    await this.sqs.sendMessage({
      QueueUrl: process.env.SQS_QUEUE_URL,
      MessageBody: JSON.stringify({
        type: 'chunk_ready',
        s3Key: key,
        recordCount: persons.length
      })
    });
  }
}
```

## NDJSON Format Reference

```
Each line is a complete JSON object:
{"id":"1","name":"Alice","email":"alice@example.com"}
{"id":"2","name":"Bob","email":"bob@example.com"}
{"id":"3","name":"Charlie","email":"charlie@example.com"}
```

**Advantages**:
- Streaming-friendly (parse line by line)
- Splittable (split on newlines)
- Human-readable
- Single record per line

## Testing Chunking Harnesses

```bash
# Test API chunking
npx ts-node src/chunking/fetch/ChunkFromAPI.ts

# Test S3 file drop chunking
npx ts-node src/chunking/filedrop/ChunkFromS3.ts

# Test array wrapper
npx ts-node src/chunking/PersonArrayWrapper.ts
```

### Validation Checklist
- [ ] All source records processed
- [ ] No duplicate records across chunks
- [ ] Chunk size respected (≤ configured size)
- [ ] NDJSON format valid
- [ ] S3 upload successful
- [ ] SQS signals sent
- [ ] Statistics logged

## Chunk Size Considerations

**Small chunks** (10-50 records):
- ✅ More parallelism
- ✅ Faster worker task completion
- ❌ More S3 uploads
- ❌ More SQS messages
- ❌ Higher overhead

**Large chunks** (1000+ records):
- ✅ Fewer S3/SQS operations
- ✅ Lower overhead
- ❌ Less parallelism
- ❌ Longer worker task time
- ❌ Risk of task timeout

**Recommended**: 100-500 records per chunk

## Error Handling

### API Fetch Failures
```typescript
try {
  const persons = await this.fetchPage(offset, pageSize);
} catch (error) {
  if (error.response?.status === 401) {
    throw new Error('Invalid API credentials');
  } else if (error.response?.status === 429) {
    // Rate limited - back off
    await sleep(5000);
    return this.fetchPage(offset, pageSize);
  } else {
    throw error;
  }
}
```

### S3 Upload Failures
```typescript
// Retry with exponential backoff
let retries = 0;
while (retries < 3) {
  try {
    await this.s3Client.putObject(...);
    break;
  } catch (error) {
    retries++;
    if (retries >= 3) throw error;
    await sleep(Math.pow(2, retries) * 1000);
  }
}
```

## Integration with Chunker Service

CDK ChunkerService Lambda triggers chunking:

```typescript
// lib/services/chunker/ChunkerService.ts
export class ChunkerService {
  async run(): Promise<void> {
    const chunker = new ChunkFromAPI();
    const result = await chunker.chunk();
    
    console.log(`Chunking complete: ${result.chunkCount} chunks, ${result.totalRecords} records`);
  }
}
```

