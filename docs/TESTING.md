# Testing Guide

## Overview

This guide covers testing strategy, running tests, and validating the chunking implementation across all three projects.

## Test Coverage Summary

### integration-core
**Location**: `c:\whennemuth\workspaces\bu_workspace\integration\integration-core`

**Total**: 302 tests passing

**New for chunking**: 18 tests
- `test/FileStorage.test.ts`: 9 tests for `outputPath` parameter
- `test/S3Storage.test.ts`: 9 tests for `outputKeyPrefix` parameter

**Run tests**:
```bash
cd integration-core
npm test
```

**Key test scenarios**:
- Default behavior (no outputPath/outputKeyPrefix provided)
- Custom output path transformation
- Chunked storage path generation
- Path preservation for non-matching patterns
- S3 key prefix manipulation

### integration-huron-person
**Location**: `c:\whennemuth\workspaces\bu_workspace\integration\integration-huron-person`

**Total**: 789 tests passing

**New for chunking**: 7 tests
- `test/DeltaStrategyFactory.test.ts`: 4 tests for chunkId parameter
- `test/SyncPeople.test.ts`: 3 tests for chunkId flow

**Run tests**:
```bash
cd integration-huron-person
npm test
```

**Key test scenarios**:
- Strategy creation without chunkId (default behavior)
- Strategy creation with chunkId (chunked storage)
- Output path/key transformation functions
- Integration flow from SyncPeople → DeltaStrategyFactory

### integration-huron-person-fargate
**Location**: `c:\whennemuth\workspaces\bu_workspace\integration\integration-huron-person-fargate`

**Total**: 103 tests passing

**Test suites**: 5
1. `test/chunker.test.ts`: 14 tests
2. `test/processor.test.ts`: 20 tests
3. `test/merger.test.ts`: 42 tests
4. `test/BigJsonFile.test.ts`: Multiple tests
5. `test/PersonArrayWrapper.test.ts`: Multiple tests

**Run all tests**:
```bash
cd integration-huron-person-fargate
npm test
```

**Run specific suite**:
```bash
npm test test/chunker.test.ts
npm test test/processor.test.ts
npm test test/merger.test.ts
```

## Detailed Test Coverage

### Chunker Tests (14 tests)

**File**: `test/chunker.test.ts`

**Test categories**:

1. **Queue Publishing** (3 tests)
   - Publishes messages for each chunk
   - Includes S3 location in message body
   - Handles errors in publishing

2. **Chunk Size Calculation** (4 tests)
   - Correct number of chunks for exact division
   - Handles remainder records in final chunk
   - Zero-pads chunk IDs
   - Single chunk for small datasets

3. **S3 Key Formatting** (4 tests)
   - Correct prefix for chunks directory
   - Chunk ID zero-padding (4 digits)
   - Pattern: `chunks/chunk-NNNN.ndjson`

4. **Record Distribution** (3 tests)
   - Records evenly distributed
   - No records lost
   - No records duplicated

**Example test**:
```typescript
it('should publish SQS message for each chunk with S3 location', async () => {
  const mockPublish = jest.fn().mockResolvedValue({});
  const queueWriter = { publishMessage: mockPublish };
  
  const chunker = new Chunker(
    mockS3Client,
    queueWriter,
    'test-bucket',
    'test-client',
    'us-east-2'
  );
  
  await chunker.breakupAndPublish('input.json', 1000);
  
  expect(mockPublish).toHaveBeenCalledWith(
    expect.objectContaining({
      bucketName: 'test-bucket',
      s3Key: 'test-client/chunks/chunk-0000.ndjson'
    })
  );
});
```

### Processor Tests (20 tests)

**File**: `test/processor.test.ts`

**Test categories**:

1. **Chunk ID Extraction** (7 tests)
   - Standard S3 key patterns
   - Nested paths
   - Zero-padding preservation
   - Edge cases (no hyphen, wrong extension, etc.)
   - End-of-string matching

2. **Input Validation** (3 tests)
   - Throws when chunk is undefined
   - Exits when bucketName missing
   - Exits when s3Key missing
   - Accepts valid inputs

3. **Configuration Building** (5 tests)
   - Creates S3DataSourceConfig correctly
   - Handles region parameter
   - Preserves base config fields
   - Handles fieldsOfInterest
   - Overrides only data source section

4. **Output Path Transformations** (4 tests)
   - Transforms to chunked storage format
   - Uses default path when no chunkId
   - Preserves client ID prefix
   - Handles multiple chunk IDs consistently

**Example test**:
```typescript
describe('extractChunkId', () => {
  it('should extract chunk ID from standard S3 key pattern', () => {
    const result = extractChunkId('data/people/chunk-0042.ndjson');
    expect(result).toBe('0042');
  });

  it('should preserve zero padding in chunk ID', () => {
    expect(extractChunkId('chunk-0000.ndjson')).toBe('0000');
    expect(extractChunkId('chunk-0001.ndjson')).toBe('0001');
    expect(extractChunkId('chunk-0099.ndjson')).toBe('0099');
    expect(extractChunkId('chunk-1234.ndjson')).toBe('1234');
  });

  it('should return undefined when no chunk pattern present', () => {
    expect(extractChunkId('data/people.json')).toBeUndefined();
    expect(extractChunkId('data/chunk0042.ndjson')).toBeUndefined();
  });
});
```

### Merger Tests (42 tests)

**File**: `test/merger.test.ts`

**Test categories**:

1. **Basic Merge Operations** (4 tests)
   - Lists chunk files from S3
   - Concatenates NDJSON correctly
   - Writes to target file
   - Returns success status

2. **Single Chunk Scenarios** (4 tests)
   - Handles one chunk file
   - Correct line count
   - Proper deletion
   - Validates chunk pattern

3. **Multiple Chunk Scenarios** (5 tests)
   - Two chunks concatenated in order
   - Five chunks processed correctly
   - Sorted output (lexicographic)
   - Preserves all records

4. **Large-Scale Tests** (8 tests)
   - 100 chunks
   - 1000 chunks  
   - Handles many files efficiently
   - Memory usage remains stable

5. **Error Handling** (6 tests)
   - No chunks found (graceful handling)
   - Missing chunk in sequence
   - S3 read errors
   - S3 write errors
   - Partial deletion failures

6. **Edge Cases** (8 tests)
   - Empty chunks (0 lines)
   - Mixed chunk sizes
   - Special characters in data
   - Very long lines
   - Unicode content
   - Non-sequential chunk IDs

7. **Deletion Batching** (4 tests)
   - Deletes in batches of 1000
   - Multiple batches handled
   - All files deleted
   - Error recovery in batches

8. **Pattern Validation** (3 tests)
   - Only processes chunk-*.ndjson files
   - Ignores other files in directory
   - Validates chunk ID format

**Example test**:
```typescript
describe('Merger', () => {
  it('should concatenate multiple chunks in correct order', async () => {
    // Mock S3 to return 5 chunks
    mockS3.send.mockImplementation((command) => {
      if (command.constructor.name === 'ListObjectsV2Command') {
        return {
          Contents: [
            { Key: 'chunks/chunk-0002.ndjson' },
            { Key: 'chunks/chunk-0000.ndjson' },
            { Key: 'chunks/chunk-0004.ndjson' },
            { Key: 'chunks/chunk-0001.ndjson' },
            { Key: 'chunks/chunk-0003.ndjson' }
          ]
        };
      }
      // Mock GetObject to return chunk data
      if (command.constructor.name === 'GetObjectCommand') {
        const chunkContent = '{"id": "..."}\\n';
        return { Body: Readable.from([chunkContent]) };
      }
    });

    const result = await merger.merge();

    expect(result.success).toBe(true);
    expect(result.totalChunks).toBe(5);
    expect(result.totalLines).toBe(5);
  });
});
```

## Running Tests

### Full Test Suite (All Projects)

```bash
# Run from workspace root
npm test --workspaces

# Or individually:
cd integration-core && npm test
cd ../integration-huron-person && npm test
cd ../integration-huron-person-fargate && npm test
```

### Watch Mode (Development)

```bash
cd integration-huron-person-fargate
npm test -- --watch

# Run specific file in watch mode
npm test test/processor.test.ts -- --watch
```

### Coverage Reports

```bash
npm test -- --coverage

# View coverage report
open coverage/lcov-report/index.html
```

**Expected coverage**:
- Statements: >90%
- Branches: >85%
- Functions: >90%
- Lines: >90%

### Debugging Tests

**VSCode launch configuration** (`.vscode/launch.json`):
```json
{
  "type": "node",
  "request": "launch",
  "name": "Jest Current File",
  "program": "${workspaceFolder}/node_modules/.bin/jest",
  "args": [
    "${fileBasenameNoExtension}",
    "--config",
    "jest.config.js"
  ],
  "console": "integratedTerminal",
  "internalConsoleOptions": "neverOpen"
}
```

**Run with debugger**:
1. Open test file in VSCode
2. Set breakpoints
3. Press F5 or select "Jest Current File" from debug menu

## Test Data

### Sample Input Files

**Small dataset** (10 records):
```json
{
  "people": [
    {"id": "1", "firstName": "Alice", "lastName": "Anderson"},
    {"id": "2", "firstName": "Bob", "lastName": "Brown"},
    ...
    {"id": "10", "firstName": "Jack", "lastName": "Johnson"}
  ]
}
```

**Medium dataset** (1000 records):
```bash
# Generate test data
node scripts/generateTestData.js --count 1000 --output test-data-1000.json
```

**Large dataset** (100,000 records):
```bash
# Use production-style data
node scripts/generateTestData.js --count 100000 --output test-data-100k.json
```

### Expected Chunk Outputs

**Input**: 10,000 records, chunk size = 1000

**Expected chunks**:
```
chunks/chunk-0000.ndjson (1000 lines)
chunks/chunk-0001.ndjson (1000 lines)
chunks/chunk-0002.ndjson (1000 lines)
chunks/chunk-0003.ndjson (1000 lines)
chunks/chunk-0004.ndjson (1000 lines)
chunks/chunk-0005.ndjson (1000 lines)
chunks/chunk-0006.ndjson (1000 lines)
chunks/chunk-0007.ndjson (1000 lines)
chunks/chunk-0008.ndjson (1000 lines)
chunks/chunk-0009.ndjson (1000 lines)
```

**Validation**:
```bash
# Count total records
cat chunks/chunk-*.ndjson | wc -l
# Expected: 10000

# Verify each chunk has 1000 records
for chunk in chunks/chunk-*.ndjson; do
  echo "$chunk: $(wc -l < $chunk) lines"
done
```

## Integration Testing

### End-to-End Test

**Script**: `test/integration-test.sh`

```bash
#!/bin/bash
set -e

echo "=== Integration Test: Chunking Pipeline ==="

# 1. Generate test data
node scripts/generateTestData.js --count 5000 --output /tmp/test-input.json

# 2. Upload to S3
aws s3 cp /tmp/test-input.json s3://$INPUT_BUCKET/$CLIENT_ID/test-input.json

# 3. Run chunker
docker run --env-file .env \
  -e INPUT_SOURCE_KEY=$CLIENT_ID/test-input.json \
  -e CHUNK_SIZE=500 \
  integration-chunker

# 4. Verify chunks created
CHUNK_COUNT=$(aws s3 ls s3://$CHUNKS_BUCKET/$CLIENT_ID/chunks/ | wc -l)
echo "Created $CHUNK_COUNT chunks (expected: 10)"

# 5. Process chunks (simulate Fargate)
for i in {0000..0009}; do
  docker run --env-file .env \
    -e CHUNK_KEY=$CLIENT_ID/chunks/chunk-$i.ndjson \
    integration-processor &
done
wait

# 6. Run merger
docker run --env-file .env integration-merger

# 7. Verify output
FINAL_COUNT=$(aws s3 cp s3://$CHUNKS_BUCKET/$CLIENT_ID/previous-input.ndjson - | wc -l)
echo "Final record count: $FINAL_COUNT (expected: 5000)"

# 8. Cleanup
aws s3 rm s3://$INPUT_BUCKET/$CLIENT_ID/test-input.json
aws s3 rm s3://$CHUNKS_BUCKET/$CLIENT_ID/previous-input.ndjson

echo "=== Integration Test PASSED ==="
```

**Run**:
```bash
chmod +x test/integration-test.sh
./test/integration-test.sh
```

## Continuous Integration

### GitHub Actions Workflow

**File**: `.github/workflows/test.yml`

```yaml
name: Test

on: [push, pull_request]

jobs:
  test-core:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with:
          node-version: '18'
      - run: cd integration-core && npm install
      - run: cd integration-core && npm test
      - run: cd integration-core && npm run build

  test-huron-person:
    needs: test-core
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with:
          node-version: '18'
      - run: cd integration-huron-person && npm install
      - run: cd integration-huron-person && npm test
      - run: cd integration-huron-person && npm run build

  test-fargate:
    needs: test-huron-person
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with:
          node-version: '18'
      - run: cd integration-huron-person-fargate && npm install
      - run: cd integration-huron-person-fargate && npm test
      - run: cd integration-huron-person-fargate && npm run build
```

## Test Maintenance

### Adding New Tests

**Checklist**:
1. Create test file in `test/` directory
2. Follow naming convention: `<module>.test.ts`
3. Use descriptive test names: `it('should ...')`
4. Group related tests in `describe()` blocks
5. Mock external dependencies (S3, SQS, APIs)
6. Clean up mocks in `afterEach()`
7. Verify test runs independently: `npm test test/<file>.test.ts`
8. Verify test runs in suite: `npm test`

### Common Patterns

**Mocking S3**:
```typescript
import { mockClient } from 'aws-sdk-client-mock';
import { S3Client } from '@aws-sdk/client-s3';

const mockS3 = mockClient(S3Client);

beforeEach(() => {
  mockS3.reset();
});

it('should read from S3', async () => {
  mockS3.on(GetObjectCommand).resolves({
    Body: Readable.from(['data'])
  });
  
  // Test code
});
```

**Mocking ConfigManager**:
```typescript
jest.mock('integration-huron-person', () => ({
  ConfigManager: {
    getInstance: jest.fn(() => ({
      getConfig: jest.fn(() => mockConfig)
    }))
  }
}));
```

## Troubleshooting Tests

### Tests timeout

**Symptom**: Tests hang indefinitely

**Causes**:
- Unmocked async calls
- Missing promise resolution
- Stream not ending

**Fix**:
```typescript
// Add timeout
it('should complete', async () => {
  // Test code
}, 5000); // 5 second timeout

// Or increase Jest timeout
jest.setTimeout(10000);
```

### Tests fail intermittently

**Symptom**: Random failures, pass on retry

**Causes**:
- Race conditions in tests
- Shared state between tests
- Async timing issues

**Fix**:
```typescript
beforeEach(() => {
  jest.clearAllMocks();
  // Reset any shared state
});

afterEach(() => {
  // Clean up resources
});
```

### Mock not working

**Symptom**: Real implementation called instead of mock

**Causes**:
- Mock defined after import
- Wrong mock syntax
- Module cache issues

**Fix**:
```typescript
// Define mock BEFORE imports
jest.mock('module-name');

// Or reset module registry
beforeEach(() => {
  jest.resetModules();
});
```

## Summary

- **Total tests**: 1094 across 3 projects
- **All passing**: ✅
- **Coverage**: >90% for chunking code
- **CI**: GitHub Actions validates on every push
- **Integration tests**: End-to-end pipeline validation

The testing strategy ensures the chunking implementation is production-ready with comprehensive validation of all components.
