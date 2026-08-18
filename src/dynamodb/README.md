# DynamoDB Table Utilities

This directory contains utility classes for interacting with DynamoDB tables used throughout the integration pipeline. All classes follow a composition pattern where domain-specific wrappers compose a generic `DynamoDBTable` base class.

## Architecture Pattern

**Composition Over Inheritance**: Domain-specific table classes (StatisticsTable, PersonCurrentStateTable, PersonHistoryTable) wrap a generic `DynamoDBTable` instance rather than inheriting from it. This provides:
- Clear separation between generic DynamoDB operations and domain logic
- Easy reusability of the generic table class
- Type-safe domain-specific methods
- Single source of truth for common operations (batch operations, pagination, GSI queries)

## DynamoDBTable (Generic Base Class)

**File**: `DynamoDBTable.ts`

Generic wrapper for DynamoDB operations. Provides common functionality used by all table-specific wrappers.

### Features
- **Batch operations**: `batchWrite()`, `truncateTable()`
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
const item = await table.getItem('partition-value', 'sort-value');

// Query by partition key
const items = await table.queryByPartitionKey('partition-value', 'sort-prefix');

// Query GSI
const results = await table.queryGSI(
  'gsi-name', 
  'gsi-partition-key', 
  'value',
  'gsi-sort-key',    // optional
  'value',           // optional
  '>='               // optional operator
);

// Batch write
await table.batchWrite([item1, item2, item3], 'put');
```

## StatisticsTable

**File**: `StatisticsTable.ts`

Tracks integration run statistics, error events, and sync run metadata.

### Table Schema
- **PK**: `integrationTimestamp` (ISO timestamp)
- **SK**: `eventType` (STATISTICS | STATISTICS-chunk-XXXX | ERROR | FLAGS | METADATA | CHUNK_STATUS_nnnn)
- **GSI**: `errorType-timestamp-index` (for querying errors)

### Record Types

**1. STATISTICS** (Aggregated)
- SK: "STATISTICS"
- Contains: Total counts, success/failure metrics, execution time
- Written by: Merger phase

**2. STATISTICS-chunk-XXXX** (Per-Chunk)
- SK: "STATISTICS-chunk-0009"
- Contains: Chunk-specific metrics
- Written by: Processor tasks

**3. ERROR**
- SK: "ERROR"
- Contains: Error details, stack traces
- Written by: Error handlers

**4. FLAGS**
- SK: "FLAGS"
- Contains: bulkReset, trustPreviousStorage, ignoreRemovals
- Written by: Chunker phase
- Read by: Processor and Merger phases

**5. METADATA**
- SK: "METADATA"
- Contains: startTime, endTime, totalChunks, clientId
- Written by: Chunker and Merger phases
- Read by: Dashboard, reporting tools

**6. CHUNK_STATUS_nnnn**
- SK: "CHUNK_STATUS_0009"
- Contains: status (PENDING|PROCESSING|COMPLETED|FAILED), startTime, endTime, error
- Written by: Processor tasks
- Read by: Merger for resumption logic

### Usage
```typescript
const statsTable = new StatisticsTable(context);

// Get aggregated statistics
const stats = await statsTable.getStatistics('2026-03-03T19:58:41.277Z');

// Get chunk-specific statistics
const chunkStats = await statsTable.getChunkStatistics('2026-03-03T19:58:41.277Z', 'chunk-0009');

// Write flags for sync run
await statsTable.writeFlags('2026-03-03T19:58:41.277Z', {
  bulkReset: true,
  trustPreviousStorage: false,
  ignoreRemovals: false
});

// Read flags
const flags = await statsTable.readFlags('2026-03-03T19:58:41.277Z');

// Write metadata
await statsTable.writeMetadata('2026-03-03T19:58:41.277Z', {
  startTime: '2026-03-03T19:58:41.277Z',
  totalChunks: 42,
  clientId: 'delta-storage'
});

// Write chunk status
await statsTable.writeChunkStatus('2026-03-03T19:58:41.277Z', 'chunk-0009', {
  status: 'COMPLETED',
  startTime: '2026-03-03T19:59:00.000Z',
  endTime: '2026-03-03T20:05:30.000Z'
});

// Get completed chunk count (for resumption)
const completedCount = await statsTable.getCompletedChunkCount('2026-03-03T19:58:41.277Z');
```

### Harness Execution
The StatisticsTable includes a test harness for interactive operations:

```bash
# Truncate table
STATISTICS_TABLE_TASK=truncate npx ts-node src/dynamodb/StatisticsTable.ts

# Get statistics for specific run
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
- **GSI**: `syncRunId-personId-index` (for deletion detection)

### Record Format
```json
{
  "personId": "U12345678",
  "hash": "a1b2c3d4...",
  "syncRunId": "2026-03-03T19:58:41.277Z"
}
```

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

// Merger: Find all persons in sync run (for deletion detection)
const personsInRun = await stateTable.getPersonsInSyncRun('2026-03-03T19:58:41.277Z');
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
- **DELETED**: Person removed from source (detected by merger)
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

// Merger: Write DELETED person
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

Deletes all records (STATISTICS, FLAGS, METADATA, CHUNK_STATUS, ERROR records) for a specific integration run:

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
- If a person's previous history entry is a DELETED record, they are deleted from PersonCurrentState
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

1. **StatisticsTable**: Remove run metadata, flags, errors (no dependencies)
2. **PersonHistoryTable**: Remove audit trail records (needed for restoration)
3. **PersonCurrentStateTable**: Remove current state and restore from previous run

This order ensures that:
- Statistics/metadata are cleaned first
- History data is available when restoring PersonCurrentState
- Current state is restored to maintain data integrity

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
- `dynamodb:BatchWriteItem` - To delete records in batches
- `dynamodb:DeleteItem` - To delete individual items
- `dynamodb:PutItem` - To restore PersonCurrentState records

**Note**: Normal ECS tasks (Processor, Merger, Chunker) do NOT have these delete permissions by design, as deletion is not part of standard pipeline operations.

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
   
   // Grant delete permissions
   ['Statistics', 'PersonCurrentState', 'PersonHistory'].forEach(tableName => {
     prunerLambda.addToRolePolicy(new PolicyStatement({
       effect: Effect.ALLOW,
       actions: [
         'dynamodb:Query',
         'dynamodb:BatchWriteItem',
         'dynamodb:DeleteItem',
         'dynamodb:PutItem',
         'dynamodb:GetItem'
       ],
       resources: [
         `arn:aws:dynamodb:${region}:${account}:table/${stackId}-${tableName.toLowerCase()}-${landscape}`,
         `arn:aws:dynamodb:${region}:${account}:table/${stackId}-${tableName.toLowerCase()}-${landscape}/index/*`
       ]
     }));
   });
   ```

3. **ECS Task** (For scheduled cleanup):
   Add a dedicated pruner task definition with elevated permissions separate from normal pipeline tasks.

**Security Consideration**: Deletion operations should be restricted to administrative roles and require explicit approval workflows in production environments.

## CDK Infrastructure

**File**: `../../lib/DynamoDB.ts`

All three tables are created conditionally based on `context.PREVIOUS_STORAGE_TYPE` configuration:

```typescript
import { DynamoDBTables } from '../lib/DynamoDB';

const tables = new DynamoDBTables(this, 'DynamoDBTables', { context, tags });

// Grant permissions
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
- Avoid scanning tables; always query by key

### 5. Type Safety
All domain-specific methods use TypeScript interfaces for records:
- `PersonCurrentStateRecord`
- `PersonHistoryRecord`
- `StatisticsItem`

## Migration Path from S3 to DynamoDB

### S3 State Files (Old Approach)
- `previous-input.ndjson`: Full person dataset from previous run
- `mini-delta.ndjson`: Incremental updates
- `chunk-markers/`: Processor completion flags
- `run-state/flags.json`: Sync run configuration
- `run-state/metadata.json`: Sync run metadata

### DynamoDB Tables (New Approach)
- **PersonCurrentState**: Replaces `previous-input.ndjson` (keyed by personId)
- **PersonHistory**: Replaces `mini-delta.ndjson` (append-only audit trail)
- **StatisticsTable FLAGS records**: Replaces `run-state/flags.json`
- **StatisticsTable METADATA records**: Replaces `run-state/metadata.json`
- **StatisticsTable CHUNK_STATUS records**: Replaces `chunk-markers/`

### Parallel Implementation
Both approaches exist in parallel. The code path is selected based on:
- `DeltaStrategyFactory.createStrategy()` chooses strategy based on `storage.type`
- `storage.type === 'dynamodb'` → DynamoDB path
- `storage.type === 's3'` → S3 path (existing, unchanged)

This allows gradual migration and rollback if needed.

## Testing

All utility classes include `truncate()` methods for test cleanup. Use with caution in production:

```typescript
// WARNING: Destructive operation!
await statsTable.truncate();
await stateTable.truncate();
await historyTable.truncate();
```

## See Also
- [Integration Core DeltaStrategy Documentation](../../integration-core/README.md)
- [Three-Phase Pipeline Skill](../../integration-workspace-skills/skills/three-phase-pipeline/SKILL.md)
- [DynamoDB Resumption Requirements](../docs/dynamodb-resumption.md)
