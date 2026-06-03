# integration-huron-person-fargate/src/processing: Data Transformation Pipeline

## Purpose
Implements the Phase 2 processing logic that applies data transformations, validations, and mappings to each chunk in parallel.

## Components

### Key Responsibilities
1. **Data Mapping** – Transform source field format to target format
2. **Validation** – Ensure data quality and correctness
3. **Deactivation Strategy** – Identify records to soft delete
4. **Error Handling** – Gracefully handle invalid records
5. **Metrics** – Track success/failure counts

## Harness (1 total)

### ApiErrorTracking
**Purpose**: Track and log API errors during processing

**Environment Prefix**: API_ERROR_TRACKING

**Location**: `ApiErrorTracking.ts`

**Responsibilities**:
- Categorize errors (auth, validation, network, etc.)
- Count error frequencies
- Log structured error information
- Report error statistics

## Processing Workflow

```
Input: Chunk from S3 (NDJSON)
  ↓
Read chunk line-by-line
  ↓
For each record:
  1. Parse JSON
  2. Map source → target format
  3. Validate against schema
  4. Check for deactivation
  ↓
Accumulate valid records
  ↓
Write processed chunk to S3
```

### Processing Service Pattern

```typescript
export class ProcessingService {
  constructor(
    private s3Client: S3Client,
    private dataMapper: DataMapper,
    private validator: PersonValidator,
    private deactivationStrategy: DeactivationStrategy
  ) {}

  async processChunk(chunkKey: string): Promise<ProcessingResult> {
    const startTime = Date.now();
    const result: ProcessingResult = {
      processedCount: 0,
      validCount: 0,
      invalidCount: 0,
      deactivationCount: 0,
      errors: []
    };

    try {
      // 1. Fetch chunk from S3
      console.log(`Processing ${chunkKey}...`);
      const chunk = await this.s3Client.getObject(chunkKey);
      const lines = chunk.split('\n').filter(l => l.trim());

      // 2. Process each record
      const validRecords: TargetPerson[] = [];

      for (const line of lines) {
        result.processedCount++;

        try {
          // Parse JSON
          const sourceRecord = JSON.parse(line);

          // Map to target format
          const targetRecord = this.dataMapper.map(sourceRecord);

          // Validate
          const validation = this.validator.validate(targetRecord);
          if (!validation.isValid) {
            console.warn(`Invalid record ${sourceRecord.id}: ${validation.errors}`);
            result.invalidCount++;
            continue;
          }

          // Check for deactivation
          const shouldDeactivate = await this.deactivationStrategy.shouldDeactivate(
            targetRecord
          );
          if (shouldDeactivate) {
            result.deactivationCount++;
            // Handle deactivation (queued for Phase 3)
            continue;
          }

          // Accumulate valid record
          validRecords.push(targetRecord);
          result.validCount++;

        } catch (error) {
          console.error(`Error processing record: ${error.message}`);
          result.errors.push({
            line: result.processedCount,
            error: error.message
          });
          result.invalidCount++;
        }
      }

      // 3. Write processed chunk to S3
      const outputKey = this.getOutputKey(chunkKey);
      await this.writeProcessedChunk(outputKey, validRecords);

      // 4. Report metrics
      const duration = Date.now() - startTime;
      console.log(`Processed ${chunkKey}: ${result.validCount}/${result.processedCount} valid in ${duration}ms`);

      return result;

    } catch (error) {
      console.error(`Failed to process ${chunkKey}:`, error);
      throw error;
    }
  }

  private async writeProcessedChunk(
    outputKey: string,
    records: TargetPerson[]
  ): Promise<void> {
    const ndjson = records
      .map(r => JSON.stringify(r))
      .join('\n');

    await this.s3Client.putObject({
      Bucket: process.env.S3_BUCKET,
      Key: outputKey,
      Body: ndjson,
      ContentType: 'application/x-ndjson'
    });

    console.log(`Wrote ${records.length} records to ${outputKey}`);
  }

  private getOutputKey(inputKey: string): string {
    // Transform: chunks/chunk-0001.ndjson → processed/chunk-0001.ndjson
    return inputKey.replace('chunks/', 'processed/');
  }
}
```

## Validation Patterns

### Schema Validation
```typescript
interface PersonValidator {
  validate(person: TargetPerson): ValidationResult;
}

export class PersonValidator {
  validate(person: TargetPerson): ValidationResult {
    const errors: string[] = [];

    if (!person.id) errors.push('id is required');
    if (!person.name) errors.push('name is required');
    if (person.name.length > 255) errors.push('name exceeds max length');
    if (person.email && !this.isValidEmail(person.email)) {
      errors.push('invalid email format');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  private isValidEmail(email: string): boolean {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }
}
```

### Business Rule Validation
```typescript
class DeactivationStrategy {
  async shouldDeactivate(person: TargetPerson): Promise<boolean> {
    // Rule: deactivate if marked inactive in source
    if (!person.isActive) {
      return true;
    }

    // Rule: deactivate if no email (required in target)
    if (!person.email) {
      return true;
    }

    return false;
  }
}
```

## Error Categorization (ApiErrorTracking)

```typescript
enum ErrorCategory {
  PARSE_ERROR = 'parse_error',           // Invalid JSON
  VALIDATION_ERROR = 'validation_error', // Schema violation
  MAPPING_ERROR = 'mapping_error',       // Transformation failed
  DEACTIVATION_ERROR = 'deactivation_error',
  NETWORK_ERROR = 'network_error',       // S3/API error
  UNKNOWN_ERROR = 'unknown_error'
}

class ApiErrorTracking {
  private errorCounts: Map<ErrorCategory, number> = new Map();

  categorizeError(error: Error): ErrorCategory {
    if (error instanceof SyntaxError) {
      return ErrorCategory.PARSE_ERROR;
    } else if (error instanceof ValidationError) {
      return ErrorCategory.VALIDATION_ERROR;
    } else if (error instanceof MappingError) {
      return ErrorCategory.MAPPING_ERROR;
    } else if (error instanceof NetworkError) {
      return ErrorCategory.NETWORK_ERROR;
    }
    return ErrorCategory.UNKNOWN_ERROR;
  }

  trackError(category: ErrorCategory): void {
    const count = this.errorCounts.get(category) || 0;
    this.errorCounts.set(category, count + 1);
  }

  getReport(): Map<ErrorCategory, number> {
    return new Map(this.errorCounts);
  }
}
```

## Testing Processing Logic

### Harness Test
```bash
npx ts-node src/processing/ApiErrorTracking.ts
```

### Unit Test Example
```typescript
describe('ProcessingService', () => {
  it('should process valid chunk', async () => {
    const chunk = '{"id":"1","name":"Test"}\n{"id":"2","name":"Test2"}\n';
    
    const result = await processor.processChunk(chunk);
    
    expect(result.processedCount).toBe(2);
    expect(result.validCount).toBe(2);
  });

  it('should skip invalid records', async () => {
    const chunk = '{"id":"1","name":"Test"}\n{"id":"2"}\n';  // Missing name
    
    const result = await processor.processChunk(chunk);
    
    expect(result.processedCount).toBe(2);
    expect(result.validCount).toBe(1);
    expect(result.invalidCount).toBe(1);
  });
});
```

## Performance Considerations

### Streaming Processing
- **Don't load entire chunk into memory** – parse line by line
- **Batch writes to S3** – write processed records in batches
- **Parallel processing** – multiple ECS tasks process different chunks simultaneously

### Error Resilience
- **Continue on single record error** – don't fail entire chunk
- **Log errors structured** – categorize for analysis
- **Report partial success** – metrics show how many processed despite errors

## Integration with Pipeline

### Input
- **Source**: Chunk files from S3 (chunks/chunk-NNNN.ndjson)
- **Trigger**: SQS message with S3 key
- **Format**: NDJSON (one record per line)

### Output
- **Destination**: S3 (processed/chunk-NNNN.ndjson)
- **Signal**: SQS acknowledgment (tells merger chunk complete)
- **Format**: NDJSON (mapped/validated records)

### Failure Handling
- **Failed chunk**: Logged to error bucket, can be retried
- **Partial failure**: Processed records still written, errors logged
- **Timeout**: ECS task killed, SQS message re-queued (auto-retry)

## Configuration

**Environment Variables** (Phase 2):
```bash
DATASOURCE_BASE_URL=...           # Shared (if fetching from API)
DATATARGET_BASE_URL=...           # Shared (if validating against target schema)
DOCKER_PROCESSOR_TIMEOUT=300000   # Processing timeout
DOCKER_PROCESSOR_MAX_WORKERS=4    # Parallelism (set by ECS)
DATA_MAPPER_FIELD_MAP=...         # Transformation rules
```

