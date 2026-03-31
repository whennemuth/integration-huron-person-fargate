# BigJsonFile with Storage Adapter Pattern

## Overview

The `BigJsonFile` class has been refactored to support multiple storage backends through the **Adapter Pattern**. This allows you to process large JSON files from different sources without changing the core chunking logic.

## Architecture

```
IStorageAdapter (interface)
    ├── S3StorageAdapter (AWS S3)
    └── FileSystemStorageAdapter (local filesystem)

BigJsonFile uses IStorageAdapter
    → Works with any storage backend
```

## Storage Adapters

### 1. S3StorageAdapter (Default)

Works with AWS S3 buckets. This is the default behavior when you don't provide a storage adapter.

```typescript
import { BigJsonFile } from './BigJsonFile';

// Automatic S3 adapter (original behavior)
const chunker = new BigJsonFile({
  itemsPerChunk: 1000,
  bucketName: 'my-bucket'  // Required for S3
});

await chunker.breakup('data/people.json');
// Reads from: s3://my-bucket/data/people.json
// Writes to: s3://my-bucket/data/people/chunk-0000.ndjson, etc.
```

### 2. FileSystemStorageAdapter (New!)

Works with local filesystem. Perfect for testing and development without AWS.

```typescript
import { BigJsonFile } from './BigJsonFile';
import { FileSystemStorageAdapter } from './storage';

const chunker = new BigJsonFile({
  itemsPerChunk: 1000,
  storage: new FileSystemStorageAdapter({ basePath: './data' })
});

await chunker.breakup('people.json');
// Reads from: ./data/people.json
// Writes to: ./data/people/chunk-0000.ndjson, etc.
```

## Complete Examples

### Example 1: Development Mode (Filesystem)

Process a local file during development:

```typescript
import { BigJsonFile } from './BigJsonFile';
import { FileSystemStorageAdapter } from './storage';

async function testLocal() {
  const chunker = new BigJsonFile({
    itemsPerChunk: 200,
    personIdField: 'personid',  // Field to identify person objects
    storage: new FileSystemStorageAdapter({ 
      basePath: './test-data' 
    })
  });

  try {
    const result = await chunker.breakup('sample-people.json');
    console.log(`Created ${result.chunkCount} chunks`);
    console.log(`Processed ${result.totalRecords} person records`);
    console.log('Chunk files:', result.chunkKeys);
  } catch (error) {
    console.error('Error:', error.message);
  }
}
```

### Example 2: Production Mode (S3)

Process files from S3 in Lambda:

```typescript
import { BigJsonFile } from './BigJsonFile';

export async function handler(event: any) {
  const chunker = new BigJsonFile({
    itemsPerChunk: 1000,
    bucketName: process.env.BUCKET_NAME!
  });

  const result = await chunker.breakup(event.s3Key);
  
  return {
    statusCode: 200,
    body: JSON.stringify({
      chunks: result.chunkCount,
      records: result.totalRecords,
      files: result.chunkKeys
    })
  };
}
```

### Example 3: Testing with Mocked S3

Use the S3 adapter with mocked clients:

```typescript
import { BigJsonFile } from './BigJsonFile';
import { S3Client } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';

const s3Mock = mockClient(S3Client);

const chunker = new BigJsonFile({
  itemsPerChunk: 1000,
  bucketName: 'test-bucket',
  s3Client: s3Mock as any  // Pass mocked client
});
```

## Configuration Options

```typescript
interface BigJsonFileConfig {
  /** Number of records per chunk (required) */
  itemsPerChunk: number;
  
  /** S3 bucket name (required if storage not provided) */
  bucketName?: string;
  
  /** S3 client for testing (optional) */
  s3Client?: S3Client;
  
  /** Field to identify person objects (default: 'personid') */
  personIdField?: string;
  
  /** Storage adapter (defaults to S3StorageAdapter) */
  storage?: IStorageAdapter;
}
```

## Return Value

```typescript
interface ChunkResult {
  /** Storage keys of created chunk files */
  chunkKeys: string[];
  
  /** Total person records found */
  totalRecords: number;
  
  /** Number of chunk files created */
  chunkCount: number;
}
```

## Supported JSON Structures

The chunker works with ANY JSON structure containing person objects:

```typescript
// Plain array
[
  { personid: 'U001', name: 'John' },
  { personid: 'U002', name: 'Jane' }
]

// API response wrapper
[{
  response_code: 200,
  response: [
    { personid: 'U001', name: 'John' },
    { personid: 'U002', name: 'Jane' }
  ]
}]

// Deeply nested
{
  data: {
    results: {
      people: [
        { personid: 'U001', name: 'John' }
      ]
    }
  }
}
```

## Creating Custom Storage Adapters

Implement the `IStorageAdapter` interface:

```typescript
import { IStorageAdapter } from './storage';
import { Readable } from 'stream';

class MyStorageAdapter implements IStorageAdapter {
  async getReadStream(key: string): Promise<Readable> {
    // Return a readable stream for the file
  }

  async writeFile(key: string, content: string, contentType?: string): Promise<void> {
    // Write content to storage
  }
}

// Use your custom adapter
const chunker = new BigJsonFile({
  itemsPerChunk: 1000,
  storage: new MyStorageAdapter()
});
```

## Benefits of the Adapter Pattern

1. **Testability**: Test with local files instead of mocking S3
2. **Flexibility**: Swap storage backends without changing core logic
3. **Development Speed**: Work locally without AWS credentials
4. **Extensibility**: Easily add support for Azure Blob, GCS, etc.
5. **Backward Compatible**: Existing code works without changes

## Running the Test Harness

```bash
# Test both S3 and filesystem modes
npx ts-node src/BigJsonFile.ts
```

The test harness demonstrates both storage modes:
- S3 mode: Points to an S3 file  
- Filesystem mode: Points to `./test-data/sample-people.json`
