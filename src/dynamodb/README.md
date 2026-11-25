# DynamoDB Table Utilities

This directory contains utility classes for interacting with DynamoDB tables used throughout the integration pipeline. All classes follow a composition pattern where domain-specific wrappers compose a generic `DynamoDBTable` base class.

## Architecture Pattern

**Composition Over Inheritance**: Domain-specific table classes (StatisticsTable, PersonCurrentStateTable, PersonHistoryTable) wrap a generic `DynamoDBTable` instance rather than inheriting from it. This provides:
- Clear separation between generic DynamoDB operations and domain logic
- Easy reusability of the generic table class
- Type-safe domain-specific methods
- Single source of truth for common operations (batch operations, pagination, GSI queries)

Also here, not covered below: `OCCFlag` (first-writer-wins claim records; [docs/CONCURRENCY_CONTROL.md](../../docs/CONCURRENCY_CONTROL.md)),
`AtomicCounter` (the run counters; the table also holds `not-a-counter:` items, `NOT_A_COUNTER_PREFIX`, which anything
that clears counters must leave alone), `MockTargetPersonTable` (the target simulator's store), `PersonRecordProcessorLogTable`
(the addons' log) and `BulkPurger` (empties a mock landscape's tables).

## DynamoDBTable (Generic Base Class)

**File**: `DynamoDBTable.ts`

Generic wrapper for DynamoDB operations. Provides common functionality used by all table-specific wrappers.

### Features
- **Batch operations**: `batchWrite()`, `truncateTable()` (a parallel scan, several delete batches in flight; returns the count)
- **Single-item operations**: `getItem()`, `putItem()`
- **Query operations**: `queryByPartitionKey()`, `queryGSI()`
- **Pagination handling**: Automatically handles LastEvaluatedKey for large result sets

### Usage
```typescript
const table = new DynamoDBTable({
  region: 'us-east-2',
  tableName: 'my-table',
  partitionKey: 'id',
  sortKey: 'timestamp' // optional
});

// Get single item
const item = await table.getItem({ partitionKeyValue: 'partition-value', sortKeyValue: 'sort-value' });

// Query by partition key
const items = await table.queryByPartitionKey({ partitionKeyValue: 'partition-value', sortKeyPrefix: 'sort-prefix' });

// Query GSI
const results = await table.queryGSI({
  indexName: 'gsi-name',
  gsiPartitionKey: 'gsi-partition-key',
  partitionKeyValue: 'value',
  gsiSortKey: 'gsi-sort-key',   // optional
  sortKeyValue: 'value',        // optional
  sortKeyOperator: '>='         // optional operator
});

// Batch write
await table.batchWrite({ items: [item1, item2, item3], operation: 'put' });
```

## StatisticsTable

**File**: `StatisticsTable.ts`

Tracks integration run statistics, error events, and sync run metadata.

### Table Schema
- **PK**: `integrationTimestamp` (ISO timestamp)
- **SK**: `eventType` (STATISTICS-chunk-XXXX | ERROR:<statusCode>:<timestamp> | FLAGS | METADATA | TERMINAL_ERROR | CHUNK_STATUS_nnnn | PROCESSOR_BOOST_CLAIM | POST_PROCESSOR_TRIGGER_CLAIM | SINGLE_REQUEST_FETCH_CLAIM | RUN | RUN_SUMMARY)
- **GSI1**: `errorType-timestamp-index` (for querying errors by status code)
- **GSI2**: `run-list-index` (sparse, KEYS_ONLY; partition key `runList`, always `RUNS`, sort key `distinctRunId`): holds
  only the RUN marker items, so it lists every run in time order (`getRunIdsPage()`, `getAllRunIds()`). A table updated
  in place to add it needs the harness task `backfill-runs`. More: [docs/STATISTICS_TABLE.md](../../docs/STATISTICS_TABLE.md)

### Record Types

**1. RUN_SUMMARY**
- SK: "RUN_SUMMARY"
- Contains: the run's status, summed results, deletion outcome, `startedBy`, `processorScalingCapacity`
- Written by: the post-processor (`AbstractPostProcessor`'s `finally`, `writeRunSummary()`), in both storage modes
- Read by: run reporting (`readRunSummary()`)

**2. STATISTICS-chunk-XXXX** (Per-Chunk)
- SK: "STATISTICS-chunk-0009"
- Contains: Chunk-specific metrics
- Written by: Processor tasks (no aggregate `STATISTICS` item is written)

**3. ERROR:<statusCode>:<timestamp>**
- SK: e.g. "ERROR:429:2026-03-03T20:01:12.003Z"; `errorType` "ERROR:429" (the GSI1 key)
- Contains: timestamp, statusCode, statusText, message, incidentId, isThrottled, detailsObj (no stack trace)
- Written by: `ApiErrorTracking`'s tracker, for target API errors during processing

**4. FLAGS**
- SK: "FLAGS"
- Contains: previousStateSource, syncPopulation, and optionally personRecordProcessorAddons, termsEndpoint,
  startedBy, processorScalingCapacity (and runFailed* fields, which nothing writes). Runs recorded before
  previousStateSource existed have bulkReset and trustPreviousStorage in its place (only Run history reads them)
- Written by: Chunker tasks
- Read by: Processor and Post-processor phases

**5. METADATA**
- SK: "METADATA"
- Contains: source, target, chunkDirectory, itemsPerChunk, previousStateSource (bulkReset and trustPreviousStorage
  before it existed), syncPopulation,
  deltaStoragePath, createdAt; the run totals chunkCount and totalRecords (accumulated by `addToMetadataTotals()`) and
  partialOrEmptyChunkEncountered; postProcessorTriggered / postProcessorTriggeredAt / postProcessorTriggeredBy
- Written by: Chunker tasks (`MetadataBroker.write`, `addToMetadataTotals`) and the processor that triggers the
  post-processor (`updateMetadata`)
- Read by: the processors' completion check, run reporting

**6. CHUNK_STATUS_nnnn**
- SK: "CHUNK_STATUS_0009"
- Contains: status (COMPLETED|FAILED), endTime, recordCount, error
- Written by: Processor tasks (DynamoDB mode)
- Read by: processors (`getCompletedChunkCount()`), to detect that every chunk is done and claim
  POST_PROCESSOR_TRIGGER_CLAIM

**7. TERMINAL_ERROR**
- SK: "TERMINAL_ERROR"
- Contains: `{ stage: 'chunking', chunkDirectory, errorMessage, errorTimestamp }`
- Written by: the chunker, when a run fails (`markRunFailed()` → `writeTerminalError()`)
- Read by: chunker tasks (abort), the processor (before triggering the post-processor) and the post-processor (before
  deletions) (`readTerminalError()`)

**RUN** (the run marker)
- SK: "RUN"; attributes `runList` = `RUNS`, `distinctRunId` = the syncRunId
- Written idempotently with FLAGS, the metadata totals and the terminal error (`registerRun()`); the only item in
  `run-list-index`

**8. PROCESSOR_BOOST_CLAIM**
- SK: "PROCESSOR_BOOST_CLAIM"
- Contains: claimedAt, claimedByChunk (optional - identifies the winning chunker task/offset)
- Written by: Chunker tasks via `claimProcessorBoost()` (ProcessorServiceBooster) - an `OCCFlag`
  guarding `claimedAt` ensures exactly one concurrent chunker task wins the right to run the
  processor scaling check. Used in both S3 and DynamoDB storage modes.
- Released by: `releaseProcessorBoostClaim()` (deletes the record) once the winner is done
- Staleness: a claim older than 10 minutes (`PROCESSOR_BOOST_CLAIM_STALE_AFTER_MS`) is treated as
  abandoned (winner killed before releasing it), cleared, and retried

**9. POST_PROCESSOR_TRIGGER_CLAIM**
- SK: "POST_PROCESSOR_TRIGGER_CLAIM"
- Contains: claimedAt, claimedByChunk (optional - identifies the winning processor task/chunk)
- Written by: Processor tasks via `claimPostProcessorTrigger()` once the completion condition is met and
  no TERMINAL_ERROR exists - an `OCCFlag` (first-writer-wins) ensures exactly one processor ever
  sends the post-processor-trigger SQS message
- Never released or treated as stale: once the post-processor has been triggered it must never be
  triggered again (see `docs/CONCURRENCY_CONTROL.md`)

**10. SINGLE_REQUEST_FETCH_CLAIM**
- SK: "SINGLE_REQUEST_FETCH_CLAIM"
- Contains: claimedAt, claimedByChunk (optional - the offset of the chunker task that fetched)
- Written by: Chunker tasks via `claimSingleRequestFetch()` (`DepletingSingleRequestDataSource`), only in a run
  whose source is a single request (one person by BUID): the winner fetches the person, every later fetch in the run
  returns an empty batch, so the source depletes like the real one. Used in both S3 and DynamoDB storage modes.
- Never released or treated as stale: the person is fetched at most once per run

### Usage
```typescript
const statsTable = new StatisticsTable(context);

// Get the run's summary
const summary = await statsTable.readRunSummary('2026-03-03T19:58:41.277Z');

// Get chunk-specific statistics
const chunkStats = await statsTable.getChunkStatistics('2026-03-03T19:58:41.277Z', 'chunk-0009');

// Write flags for sync run
await statsTable.writeFlags('2026-03-03T19:58:41.277Z', {
  previousStateSource: 'target',
  syncPopulation: 'person-full'
});

// Read flags
const flags = await statsTable.readFlags('2026-03-03T19:58:41.277Z');

// Write metadata
await statsTable.writeMetadata('2026-03-03T19:58:41.277Z', {
  source: '...',
  chunkDirectory: 'chunks/person-full/2026-03-03T19:58:41.277Z',
  itemsPerChunk: 200,
  createdAt: '2026-03-03T19:58:41.277Z'
});

// Write chunk status
await statsTable.writeChunkStatus('2026-03-03T19:58:41.277Z', 'chunk-0009', {
  status: 'COMPLETED',
  endTime: '2026-03-03T20:05:30.000Z',
  recordCount: 200
});

// Get completed chunk count (has every chunk been processed?)
const completedCount = await statsTable.getCompletedChunkCount('2026-03-03T19:58:41.277Z');

// Claim the processor boost (chunker tasks) - true only for the single winner
if (await statsTable.claimProcessorBoost('2026-03-03T19:58:41.277Z', '1000')) {
  try { /* run scaling check */ } finally {
    await statsTable.releaseProcessorBoostClaim('2026-03-03T19:58:41.277Z');
  }
}

// Claim the post-processor trigger (processor tasks) - true only for the single winner, never released
const shouldTriggerPostProcessor = await statsTable.claimPostProcessorTrigger('2026-03-03T19:58:41.277Z', '0009');
```

### Harness Execution
The StatisticsTable includes a test harness for interactive operations:

```bash
# Truncate table
STATISTICS_TABLE_TASK=truncate npx ts-node src/dynamodb/StatisticsTable.ts

# Write the RUN marker for runs that lack one (a table updated in place to add run-list-index)
STATISTICS_TABLE_TASK=backfill-runs npx ts-node src/dynamodb/StatisticsTable.ts

# Get the run summary for a specific run
STATISTICS_TABLE_TASK=statistics \
STATISTICS_TABLE_INTEGRATION_TIMESTAMP=2026-03-03T19:58:41.277Z \
npx ts-node src/dynamodb/StatisticsTable.ts

# List all integration runs
STATISTICS_TABLE_TASK=list npx ts-node src/dynamodb/StatisticsTable.ts

# Get all chunk statistics for specific run
STATISTICS_TABLE_TASK=chunks \
STATISTICS_TABLE_INTEGRATION_TIMESTAMP=2026-03-03T19:58:41.277Z \
npx ts-node src/dynamodb/StatisticsTable.ts
```

## PersonCurrentStateTable

**File**: `PersonCurrentStateTable.ts`

Tracks current hash state for each person. Used for delta computation.

### Table Schema
- **PK**: `personId` (e.g., "U12345678")
- **SK**: None (single record per person)
- **GSI**: `syncRunId-personId-index` (find persons last changed in a run; used by run pruning)

### Record Format
```json
{
  "personId": "U12345678",
  "hash": "a1b2c3d4...",
  "syncRunId": "2026-03-03T19:58:41.277Z",
  "deletedAt": "2026-03-04T02:10:00.000Z"
}
```

`deletedAt` is present, and the hash carries a `DELETED:` prefix, for a person soft-deleted because they left the
source (`markDeleted()`); a returning person's next write clears both.

### Usage
```typescript
const stateTable = new PersonCurrentStateTable(context);

// Processor: Get previous state for chunk
const personIds = ['U11111', 'U22222', 'U33333'];
const previousStates = await stateTable.batchGetPersonState(personIds);

// Processor: Write updated state
const updatedStates = [
  { personId: 'U11111', hash: 'hash1', syncRunId: '2026-03-03T19:58:41.277Z' },
  { personId: 'U22222', hash: 'hash2', syncRunId: '2026-03-03T19:58:41.277Z' }
];
await stateTable.batchWritePersonState(updatedStates);

// Find persons last changed in a sync run (run pruning, the harness's list task)
const personsInRun = await stateTable.getPersonsInSyncRun('2026-03-03T19:58:41.277Z');

// Post-processor (deletion detection): every person, by a full scan
const everyone = await stateTable.getAllPersons();
```

## PersonHistoryTable

**File**: `PersonHistoryTable.ts`

Append-only audit trail of person state changes.

### Table Schema
- **PK**: `personId` (e.g., "U12345678")
- **SK**: `syncRunId` (ISO timestamp)
- **GSI1**: `syncRunId-changeType-index` (query changes in sync run)
- **GSI2**: `changeType-syncRunId-index` (query changes by type across runs)

### Record Format
```json
{
  "personId": "U12345678",
  "syncRunId": "2026-03-03T19:58:41.277Z",
  "hash": "a1b2c3d4...",
  "changeType": "UPDATED",
  "previousHash": "old-hash"
}
```

### Change Types
- **NEW**: First time person appears in source
- **UPDATED**: Hash changed from previous sync
- **REACTIVATED**: Person returned after being marked deleted (their stored hash was `DELETED:`-marked); `previousHash`
  is their unmarked last hash. Before 2026-11-25 this was an `UPDATED` record whose `previousHash` carried the marker.
- **DELETED**: Person removed from source (detected by post-processor)
- **UNCHANGED**: NOT WRITTEN (skipped entirely to reduce costs)

### Usage
```typescript
const historyTable = new PersonHistoryTable(context);

// Processor: Write NEW person
await historyTable.writeHistory({
  personId: 'U11111',
  syncRunId: '2026-03-03T19:58:41.277Z',
  hash: 'hash1',
  changeType: 'NEW'
});

// Post-processor: Write DELETED person
await historyTable.writeHistory({
  personId: 'U22222',
  syncRunId: '2026-03-03T19:58:41.277Z',
  hash: 'oldHash',
  changeType: 'DELETED',
  previousHash: 'oldHash'
});

// Reporting: Get person history
const history = await historyTable.getPersonHistory('U11111');

// Reporting: Get all changes in sync run
const changes = await historyTable.getChangesInSyncRun('2026-03-03T19:58:41.277Z');

// Reporting: Get only NEW persons in sync run
const newPersons = await historyTable.getChangesInSyncRun('2026-03-03T19:58:41.277Z', 'NEW');

// Reporting: Get all DELETED persons across all runs
const deletedPersons = await historyTable.getChangesByType('DELETED');

// Reporting: Get DELETED persons since specific date
const recentDeleted = await historyTable.getChangesByType('DELETED', '2026-01-01T00:00:00.000Z');
```

### Harness Execution
The PersonHistoryTable includes a test harness for interactive operations:

```bash
# Truncate table
PERSON_HISTORY_TABLE_TASK=truncate npx ts-node src/dynamodb/PersonHistoryTable.ts

# Get person history
PERSON_HISTORY_TABLE_TASK=history \
PERSON_HISTORY_TABLE_PERSON_ID=U12345678 \
npx ts-node src/dynamodb/PersonHistoryTable.ts

# Get all changes in a sync run
PERSON_HISTORY_TABLE_TASK=changes \
PERSON_HISTORY_TABLE_SYNC_RUN_ID=2026-03-03T19:58:41.277Z \
npx ts-node src/dynamodb/PersonHistoryTable.ts

# Delete all history records for a sync run
PERSON_HISTORY_TABLE_TASK=delete \
PERSON_HISTORY_TABLE_SYNC_RUN_ID=2026-03-03T19:58:41.277Z \
npx ts-node src/dynamodb/PersonHistoryTable.ts
```

## PersonCurrentStateTable Harness

### Harness Execution
```bash
# Truncate table
PERSON_CURRENT_STATE_TABLE_TASK=truncate npx ts-node src/dynamodb/PersonCurrentStateTable.ts

# Get person state
PERSON_CURRENT_STATE_TABLE_TASK=get \
PERSON_CURRENT_STATE_TABLE_PERSON_ID=U12345678 \
npx ts-node src/dynamodb/PersonCurrentStateTable.ts

# List persons in a sync run
PERSON_CURRENT_STATE_TABLE_TASK=list \
PERSON_CURRENT_STATE_TABLE_SYNC_RUN_ID=2026-03-03T19:58:41.277Z \
npx ts-node src/dynamodb/PersonCurrentStateTable.ts

# Delete and restore from previous run
PERSON_CURRENT_STATE_TABLE_TASK=delete-restore \
PERSON_CURRENT_STATE_TABLE_SYNC_RUN_ID=2026-03-03T19:58:41.277Z \
npx ts-node src/dynamodb/PersonCurrentStateTable.ts
```

## Deleting Integration Runs

All three table classes provide deletion methods for removing records associated with a specific integration run.

### StatisticsTable.deleteByPartitionKey()

Deletes every item in the run's partition (chunk statistics, FLAGS, METADATA, TERMINAL_ERROR, CHUNK_STATUS, ERROR, the claims, the RUN marker and RUN_SUMMARY) for a specific integration run:

```typescript
const statsTable = new StatisticsTable(context);
const deletedCount = await statsTable.deleteByPartitionKey('2026-03-03T19:58:41.277Z');
console.log(`Deleted ${deletedCount} statistics records`);
```

**Harness:**
```bash
STATISTICS_TABLE_TASK=delete \
STATISTICS_TABLE_INTEGRATION_TIMESTAMP=2026-03-03T19:58:41.277Z \
npx ts-node src/dynamodb/StatisticsTable.ts
```

### PersonHistoryTable.deleteByPartitionKey()

Deletes all history records (NEW, UPDATED, DELETED) for a specific sync run:

```typescript
const historyTable = new PersonHistoryTable(context);
const deletedCount = await historyTable.deleteByPartitionKey('2026-03-03T19:58:41.277Z');
console.log(`Deleted ${deletedCount} history records`);
```

**Note**: This method uses GSI1 (`syncRunId-changeType-index`) to efficiently find all records for the sync run.

### PersonCurrentStateTable.deleteByPartitionKeyAndRestore()

Restores person state records to their pre-run state for a specific sync run:

```typescript
const stateTable = new PersonCurrentStateTable(context);
const historyTable = new PersonHistoryTable(context);

const result = await stateTable.deleteByPartitionKeyAndRestore(
  '2026-03-03T19:58:41.277Z',
  historyTable
);

console.log(`Deleted: ${result.deletedCount}, Restored: ${result.restoredCount}`);
```

**Restoration Process**:
1. Queries GSI to find all persons modified in the target sync run
2. For each person, queries PersonHistory to find their state before the target run
3. If previous state exists: **UPDATES** the person record with previous hash/syncRunId
4. If no previous state exists: **DELETES** the person record (they were created in target run)

**Key Behavior**: 
- PersonCurrentState maintains **one record per person** showing their current state
- When pruning a run, persons are **restored** to their previous state, not deleted wholesale
- Only persons who were **created** in the target run (no previous history) are deleted
- Example: If Person A was updated in Run 3, pruning Run 3 restores Person A to their Run 2 state

**Edge Cases**: 
- If a person's previous history entry is a DELETED record, their record is restored with the `DELETED:`-prefixed hash
  (still soft-deleted)
- If a person has no history before the target run, they are deleted (created in target run)

## IntegrationRunPruner (Orchestrator)

**File**: `IntegrationRun.ts`

Orchestrates the deletion of an integration run across all three tables, maintaining data integrity by restoring PersonCurrentState from the previous run.

### Usage

```typescript
import { IntegrationRunPruner } from './dynamodb/IntegrationRun';

const pruner = new IntegrationRunPruner('2026-03-03T19:58:41.277Z', context);
const result = await pruner.prune();

console.log(result);
// {
//   statisticsDeleted: 42,
//   historyDeleted: 150,
//   currentStateDeleted: 150,
//   currentStateRestored: 148
// }
```

### Execution Order

The pruner executes deletions in this specific order:

1. **StatisticsTable**: Remove the run's items: metadata, flags, errors, chunk statistics and statuses, claims, the RUN
   marker and the run summary
2. **PersonHistoryTable**: Remove the run's audit trail records
3. **PersonCurrentStateTable**: Remove current state and restore from previous run

Restoration uses each person's latest history entry *before* this run, so it doesn't need this run's own entries.
`prune()` refuses a run if any of its persons were changed by a later run: prune the later runs first.

### Harness Execution

```bash
INTEGRATION_RUN_PRUNER_TIMESTAMP=2026-03-03T19:58:41.277Z \
npx ts-node src/dynamodb/IntegrationRun.ts
```

### Use Cases

- **Failed Integration Cleanup**: Remove partial/failed integration runs
- **Test Data Cleanup**: Clean up test runs while preserving production data
- **Selective Pruning**: Remove specific runs while maintaining data integrity
- **Rollback**: Restore database to state before a problematic integration run

### Validation

The pruner validates the ISO timestamp format before executing:

```typescript
// Valid timestamps
new IntegrationRunPruner('2026-03-03T19:58:41.277Z', context);
new IntegrationRunPruner('2026-03-03T19:58:41+00:00', context);

// Invalid - throws error
new IntegrationRunPruner('2026-03-03', context); // Missing time
new IntegrationRunPruner('invalid', context);    // Not ISO format
```

### IAM Permissions for Pruning

The IntegrationRunPruner requires additional DynamoDB permissions beyond normal pipeline operations:

**Required Permissions**:
- `dynamodb:Query` - To find records matching partition keys
- `dynamodb:BatchGetItem` - The precondition's look-up of the run's persons
- `dynamodb:BatchWriteItem` - To delete records and restore PersonCurrentState records, in batches
- `dynamodb:GetItem`

**Note**: Run pruning is a manual, administrative operation; no pipeline task runs it. (The processor and
post-processor task roles do have `BatchWriteItem` on these tables, which can delete.)

**Deployment Options**:

1. **Local Execution** (Recommended for manual cleanup):
   ```bash
   # Run locally with AWS credentials
   INTEGRATION_RUN_PRUNER_TIMESTAMP=2026-03-03T19:58:41.277Z \
   npx ts-node src/dynamodb/IntegrationRun.ts
   ```
   Requires AWS credentials with DynamoDB delete permissions.

2. **Lambda Function** (For automated cleanup):
   Create a Lambda with a custom IAM role:
   ```typescript
   const prunerLambda = new NodejsFunction(this, 'PrunerLambda', {
     // ... config
   });
   
   // Grant the pruner's permissions, on the tables' real names (each table class exports DYNAMODB_TABLE_NAME(context),
   // e.g. `${STACK_ID}-person-current-state-${landscape}`, `${STACK_ID}-person-history-${landscape}`)
   [statisticsTableName, personCurrentStateTableName, personHistoryTableName].forEach(tableName => {
     prunerLambda.addToRolePolicy(new PolicyStatement({
       effect: Effect.ALLOW,
       actions: [
         'dynamodb:Query',
         'dynamodb:BatchGetItem',
         'dynamodb:BatchWriteItem',
         'dynamodb:GetItem'
       ],
       resources: [
         `arn:aws:dynamodb:${region}:${account}:table/${tableName}`,
         `arn:aws:dynamodb:${region}:${account}:table/${tableName}/index/*`
       ]
     }));
   });
   ```

3. **ECS Task** (For scheduled cleanup):
   Add a dedicated pruner task definition with elevated permissions separate from normal pipeline tasks.

**Security Consideration**: Deletion operations should be restricted to administrative roles and require explicit approval workflows in production environments.

## CDK Infrastructure

**File**: `../../lib/DynamoDB.ts`

The statistics, atomic counter and addon log tables are always created; the PersonCurrentState and PersonHistory
tables only in DynamoDB storage mode (`context.PREVIOUS_STORAGE_TYPE`); the mock target table only in a mock
landscape.

```typescript
import { DynamoDbTables, TableResourceIds } from '../lib/DynamoDB';

const tables = new DynamoDbTables({ scope: this, id: 'DynamoDb', props: { context, tags } });

// Grant permissions (the task definitions grant theirs with explicit policies instead)
tables.grantReadWriteData(processorTask, TableResourceIds.STATISTICS_TABLE);
tables.grantReadWriteData(processorTask, TableResourceIds.PERSON_CURRENT_STATE_TABLE);
tables.grantReadWriteData(processorTask, TableResourceIds.PERSON_HISTORY_TABLE);
```

**Default Behavior**: If `context.PREVIOUS_STORAGE_TYPE` is undefined, defaults to `'dynamodb'` mode (PersonCurrentState and PersonHistory tables are created).

## Design Principles

### 1. Composition Over Inheritance
Each domain-specific table class wraps a `DynamoDBTable` instance rather than inheriting. This promotes:
- Single responsibility (DynamoDBTable handles AWS SDK, wrappers handle domain logic)
- Reusability (DynamoDBTable can be used by any new table)
- Testability (easy to mock the base table)

### 2. Batch Operations
Use batch operations whenever possible to reduce costs and improve performance:
- `batchGetPersonState()` instead of individual `getItem()` calls
- `batchWriteHistory()` instead of individual `writeHistory()` calls
- DynamoDB limits: 25 items per BatchWriteItem, 100 keys per BatchGetItem

### 3. Pagination
All query methods automatically handle pagination. You don't need to manually manage `LastEvaluatedKey`.

### 4. Cost Optimization
- UNCHANGED persons are NOT written to PersonHistory (significant write savings)
- Use GSIs strategically to enable efficient queries
- Query by key; scan only for once-per-run work (deletion detection, truncation, backfilling run markers)

### 5. Type Safety
All domain-specific methods use TypeScript interfaces for records:
- `PersonCurrentStateRecord`
- `PersonHistoryRecord`
- `StatisticsItem`

## Migration Path from S3 to DynamoDB

### S3 State Files (Old Approach)
- `delta-storage/previous-input.ndjson`: Full person dataset from previous run (the baseline)
- `deltas/<population>/<syncRunId>/`: each chunk's delta (incremental updates)
- `deltas/<population>/<syncRunId>/chunk-NNNN_processing_complete.json`: Processor completion markers
- `chunks/<population>/<syncRunId>/_flags.json`: Sync run configuration
- `chunks/<population>/<syncRunId>/_metadata.json`: Sync run metadata (and `_terminal_error.json` beside it)

### DynamoDB Tables (New Approach)
- **PersonCurrentState**: Replaces `previous-input.ndjson` (keyed by personId)
- **PersonHistory**: Replaces the chunk deltas (append-only audit trail)
- **StatisticsTable FLAGS records**: Replaces `_flags.json`
- **StatisticsTable METADATA records**: Replaces `_metadata.json`
- **StatisticsTable CHUNK_STATUS records**: Replaces the completion markers

### Parallel Implementation
Both approaches exist in parallel. The code path is selected based on:
- `DeltaStrategyFactory.createStrategy()` chooses strategy based on `storage.type`
- `storage.type === 'dynamodb'` → DynamoDB path
- `storage.type === 's3'` → S3 path (existing, unchanged)

This allows gradual migration and rollback if needed.

## Testing

The table classes (StatisticsTable, PersonCurrentStateTable, PersonHistoryTable, MockTargetPersonTable) include
`truncate()` methods for test cleanup. To empty a landscape's tables use `BulkPurger`, which refuses a non-mock
landscape unless `force: true`. Use with caution:

```typescript
// WARNING: Destructive operation!
await statsTable.truncate();
await stateTable.truncate();
await historyTable.truncate();
```

## See Also
- [Integration Core DeltaStrategy Documentation](../../../integration-core/README.md)
- [Three-Phase Pipeline Skill](../../../.claude/skills/three-phase-pipeline/SKILL.md)
- [The statistics table](../../docs/STATISTICS_TABLE.md), [storage modes](../../docs/STORAGE_MODES.md) and
  [concurrency control](../../docs/CONCURRENCY_CONTROL.md)
