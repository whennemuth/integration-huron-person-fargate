# integration-huron-person-fargate/src/merging: Delta Consolidation & Storage

## Purpose
Implements Phase 3 merging logic that consolidates processed chunks into final delta records and manages soft deletes.

## Components

### Key Responsibilities
1. **Chunk Consolidation** – Merge all processed chunks into single delta
2. **Deactivation Coordination** – Handle soft deletes without conflicts
3. **Delta Storage** – Write final delta to filesystem hierarchy
4. **Statistics** – Record counts of creates, updates, deletions
5. **Cleanup** – Optional cleanup of intermediate S3 objects

## Harnesses (3 total)

### 1. DeferredDeleteHandler
**Purpose**: Coordinate and queue soft delete operations

**Environment Prefix**: DEFERRED_DELETE_HANDLER

**Location**: `DeferredDeleteHandler.ts`

**Responsibilities**:
- Queue deletion requests with deferred timestamps
- Deduplicate deletion requests (same ID from multiple processors)
- Wait for all processors to complete
- Execute deactivations with conflict resolution

**Pattern**:
```typescript
class DeferredDeleteHandler {
  private deleteQueue: Set<string> = new Set();

  async queueForDeletion(personId: string, reason: string): Promise<void> {
    // Add to queue with defer time (wait 5 seconds for other requests)
    this.deleteQueue.add(personId);
  }

  async processDeferredDeletes(): Promise<number> {
    // Wait for all processors to queue deletes
    await this.waitForProcessorCompletion();

    // Deduplicate
    const uniqueIds = Array.from(this.deleteQueue);

    // Execute deactivations
    let count = 0;
    for (const id of uniqueIds) {
      await this.dataTarget.deactivate(id, 'Deactivated by sync');
      count++;
    }

    return count;
  }

  private async waitForProcessorCompletion(): Promise<void> {
    // Check SQS: wait until no messages in queue
    const isQueueEmpty = await this.sqsMonitor.isEmpty();
    while (!isQueueEmpty) {
      await sleep(5000);
    }
  }
}
```

### 2. PersonCache
**Purpose**: In-memory cache of person records for merge deduplication

**Environment Prefix**: PERSON_CACHE

**Location**: `PersonCache.ts`

**Responsibilities**:
- Load all processed persons into memory
- Deduplicate by ID (keep last version)
- Provide fast lookup for conflict detection
- Cache statistics (hit rate, memory usage)

**Pattern**:
```typescript
class PersonCache {
  private cache: Map<string, TargetPerson> = new Map();

  addPerson(person: TargetPerson): void {
    const existing = this.cache.get(person.id);

    if (existing) {
      // Keep later version (better to have newer data)
      this.cache.set(person.id, person);
    } else {
      this.cache.set(person.id, person);
    }
  }

  getPerson(id: string): TargetPerson | undefined {
    return this.cache.get(id);
  }

  getAllPersons(): TargetPerson[] {
    return Array.from(this.cache.values());
  }

  getStatistics() {
    return {
      count: this.cache.size,
      memoryUsage: this.estimateMemory()
    };
  }
}
```

### 3. StatisticsTable
**Purpose**: Record and report merge statistics

**Environment Prefix**: STATISTICS_TABLE

**Location**: `StatisticsTable.ts`

**Responsibilities**:
- Track sync metrics (counts, timing, errors)
- Persist statistics to database or file
- Generate reports
- Monitor for anomalies

**Pattern**:
```typescript
class StatisticsTable {
  async recordSyncStatistics(stats: SyncStatistics): Promise<void> {
    const record = {
      timestamp: new Date().toISOString(),
      syncId: process.env.SYNC_ID,
      processedCount: stats.processedCount,
      createdCount: stats.createdCount,
      updatedCount: stats.updatedCount,
      deactivatedCount: stats.deactivatedCount,
      errorCount: stats.errorCount,
      successRate: (stats.processedCount - stats.errorCount) / stats.processedCount,
      duration: stats.endTime - stats.startTime
    };

    // Store in DynamoDB, S3, or database
    await this.store(record);

    console.log(`Statistics recorded for sync ${process.env.SYNC_ID}`);
  }
}
```

## Merging Workflow

```
Phase 2 Complete (all chunks processed)
  ↓
Merger starts
  ↓
1. Load all processed chunks from S3
   processed/chunk-0001.ndjson
   processed/chunk-0002.ndjson
   ...
   processed/chunk-NNNN.ndjson
  ↓
2. Deduplicate records (by ID)
   Keep last version if duplicates
  ↓
3. Build delta:
   - New persons (not in target)
   - Updated persons (different from target)
   - Deactivated persons (in target, not in source)
  ↓
4. Process deferred deletes
   Soft delete (deactivate) persons
  ↓
5. Write delta storage
   data/delta-storage/{TIMESTAMP}/
   ├─ delta-persons.ndjson
   ├─ delta-deletions.ndjson
   └─ metadata.json
  ↓
6. Record statistics
   Update metrics table
  ↓
Sync complete
```

## Delta Storage Structure

**Location**: `data/delta-storage/{TIMESTAMP}/`

**Format**:
```
20260531_143022/
  ├─ delta-persons.ndjson      # New and updated persons
  ├─ delta-deletions.ndjson    # Soft-deleted persons
  ├─ metadata.json             # Sync statistics and manifest
  └─ manifest.json             # File inventory (optional)
```

**Delta Persons (NDJSON)**:
```json
{"id":"P001","name":"Alice","email":"alice@example.com","status":"active"}
{"id":"P002","name":"Bob","email":"bob@example.com","status":"active"}
```

**Delta Deletions (NDJSON)**:
```json
{"id":"P999","reason":"Not in current enrollment"}
{"id":"P998","reason":"Graduated"}
```

**Metadata**:
```json
{
  "syncId": "sync-20260531-143022",
  "timestamp": "2026-05-31T14:30:22Z",
  "statistics": {
    "totalProcessed": 10000,
    "created": 2500,
    "updated": 5000,
    "deactivated": 500,
    "errors": 0
  },
  "source": {
    "type": "api",
    "endpoint": "https://api.example.com/api/persons",
    "recordsCount": 10000
  },
  "duration": {
    "startTime": "2026-05-31T14:00:00Z",
    "endTime": "2026-05-31T14:30:22Z",
    "durationMs": 1822000
  },
  "processorTasks": {
    "count": 100,
    "successCount": 100,
    "failureCount": 0
  }
}
```

## Merger Service Implementation

```typescript
export class MergerService {
  constructor(
    private s3Client: S3Client,
    private dataTarget: PersonDataTarget,
    private personCache: PersonCache,
    private statisticsTable: StatisticsTable,
    private deferredDeleteHandler: DeferredDeleteHandler
  ) {}

  async merge(): Promise<void> {
    const startTime = Date.now();
    const stats = {
      processedCount: 0,
      createdCount: 0,
      updatedCount: 0,
      deactivatedCount: 0,
      errorCount: 0
    };

    try {
      console.log('Merger starting...');

      // 1. Load all processed chunks
      console.log('Loading processed chunks...');
      const chunks = await this.loadProcessedChunks();
      console.log(`Loaded ${chunks.length} processed chunks`);

      // 2. Deduplicate and build cache
      console.log('Building person cache...');
      for (const chunk of chunks) {
        for (const person of chunk) {
          this.personCache.addPerson(person);
          stats.processedCount++;
        }
      }
      console.log(`Cache complete: ${this.personCache.getAllPersons().length} unique persons`);

      // 3. Process each person
      const targetPersons = await this.dataTarget.fetchAll();
      const targetMap = new Map(targetPersons.map(p => [p.id, p]));

      for (const sourcePerson of this.personCache.getAllPersons()) {
        const targetPerson = targetMap.get(sourcePerson.id);

        if (!targetPerson) {
          // New person
          stats.createdCount++;
        } else if (JSON.stringify(sourcePerson) !== JSON.stringify(targetPerson)) {
          // Different
          stats.updatedCount++;
        }
      }

      // 4. Process deferred deletes
      console.log('Processing deferred deletes...');
      stats.deactivatedCount = await this.deferredDeleteHandler.processDeferredDeletes();

      // 5. Write delta storage
      console.log('Writing delta storage...');
      await this.writeDeltaStorage(this.personCache.getAllPersons(), stats);

      // 6. Record statistics
      const duration = Date.now() - startTime;
      await this.statisticsTable.recordSyncStatistics({
        ...stats,
        duration,
        successRate: (stats.processedCount - stats.errorCount) / stats.processedCount
      });

      console.log('Merger complete:', stats);

    } catch (error) {
      console.error('Merger failed:', error);
      throw error;
    }
  }

  private async loadProcessedChunks(): Promise<TargetPerson[][]> {
    const files = await this.s3Client.listObjects({
      Bucket: process.env.S3_BUCKET,
      Prefix: 'processed/'
    });

    const chunks = [];
    for (const file of files) {
      const content = await this.s3Client.getObject(file.Key);
      const persons = content.split('\n')
        .filter(l => l.trim())
        .map(l => JSON.parse(l));
      chunks.push(persons);
    }

    return chunks;
  }

  private async writeDeltaStorage(persons: TargetPerson[], stats: any): Promise<void> {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').substring(0, 19);
    const storageKey = `delta-storage/${timestamp}`;

    // Write persons
    const personNdjson = persons
      .map(p => JSON.stringify(p))
      .join('\n');

    await this.s3Client.putObject({
      Key: `${storageKey}/delta-persons.ndjson`,
      Body: personNdjson
    });

    // Write metadata
    const metadata = {
      timestamp: new Date().toISOString(),
      statistics: stats,
      processorCount: 100  // From phase 2
    };

    await this.s3Client.putObject({
      Key: `${storageKey}/metadata.json`,
      Body: JSON.stringify(metadata, null, 2)
    });

    console.log(`Delta storage written to ${storageKey}`);
  }
}
```

## Testing Merging Logic

### Harnesses
```bash
npx ts-node src/merging/DeferredDeleteHandler.ts
npx ts-node src/merging/PersonCache.ts
npx ts-node src/merging/StatisticsTable.ts
```

### Validation Checklist
- [ ] All chunks loaded from S3
- [ ] Deduplication working (no ID duplicates)
- [ ] Deactivation coordination prevents conflicts
- [ ] Delta storage created with correct structure
- [ ] Statistics accurate (counts match actual)
- [ ] Metadata complete and valid JSON

## Configuration

**Environment Variables** (Phase 3):
```bash
DATATARGET_BASE_URL=...              # Shared
DOCKER_MERGER_TIMEOUT=600000         # Long timeout (merge takes time)
DEFERRED_DELETE_HANDLER_BATCH_SIZE=100  # Delete batch size
STATISTICS_TABLE_UPDATE_INTERVAL=5000   # Metrics update frequency
```

