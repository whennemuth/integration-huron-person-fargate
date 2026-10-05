# Concurrency Control in DynamoDB

## Overview
This document covers two utility classes for coordinating concurrent operations across multiple processors, Lambda functions, or ECS tasks:
- **AtomicCounter**: Atomic increment operations with guaranteed uniqueness
- **OCCFlag**: "First writer wins" claims using optimistic concurrency control (OCC)

These solve different problems, and the naming is deliberate:
- **Atomicity** (AtomicCounter's `ADD`) is about whether a single operation completes wholly or not
  at all - no partial writes.
- **OCC** (OCCFlag's conditional write) is about mutual exclusion - among multiple concurrent
  contenders attempting the same operation, ensuring exactly one succeeds and all others are told
  they lost, so they can decide what to do next (retry, abort, defer). There's no upfront locking;
  all contenders attempt the write freely, and DynamoDB detects and rejects the losers at commit
  time via a `ConditionExpression`.

Unlike AtomicCounter, OCCFlag is not tied to a dedicated table or schema - it operates against any
existing table's any attribute, since OCC is a property of *how* a write is performed, not of any
particular table design.

---

## AtomicCounter

### Purpose
Manages atomic counters stored in DynamoDB using the `ADD` operation. Ensures that concurrent increments never overwrite each other - all increments are applied, and no two incrementors ever receive the same value.

### Key Feature: ADD Operation
```typescript
UpdateExpression: 'ADD #counterValue :incrementBy SET LastUpdated = :timestamp'
```

DynamoDB's `ADD` operation is **inherently atomic** - it guarantees:
- ✅ No partial updates (all-or-nothing)
- ✅ No lost increments (all concurrent operations succeed)
- ✅ Unique values per incrementor (no two clients get same count)

### Methods

#### `increment(incrementBy?: number)`
- Atomically increments counter by specified amount (default: 1)
- Returns the **new counter value** after increment
- Multiple concurrent calls all succeed with unique values

#### `reset()`
- Sets counter back to 0
- Returns the counter instance for method chaining

#### `getValue()`
- Returns current counter value (0 if counter doesn't exist yet)

#### `tableExists(tableName?)`
- Checks if DynamoDB table exists

### Use Cases

1. **Chunk ID Generation**
   - Generate unique chunk identifiers across concurrent chunkers
   - Each chunker gets a unique, sequential chunk number

2. **Request Counting**
   - Track total API requests across multiple processors
   - No risk of lost counts due to concurrency

3. **Statistics Aggregation**
   - Sum up metrics from parallel workers
   - Each worker's contribution is guaranteed to be counted

4. **Sequence Number Generation**
   - Generate unique sequence numbers for events
   - Useful for ordering in distributed logs

### DynamoDB Schema

**Table Name**: `{STACK_ID}-atomic-counter-{landscape}`

**Item Structure**:
```json
{
  "counter_name": "chunk-id-generator",
  "counter_value": 42,
  "LastUpdated": "2026-08-28T12:05:30.123Z"
}
```

**Partition Key**: `counter_name` (string)

### Example Usage

```typescript
import { AbstractAtomicCounter } from './dynamodb/AtomicCounter';

const chunkCounter = new class extends AbstractAtomicCounter {
  getCounterName() { 
    return 'chunk-id-generator'; 
  }
}({ stackId: 'my-stack', region: 'us-east-2', landscape: 'preview' });

// Multiple concurrent tasks can safely increment
const myChunkId = await chunkCounter.increment();
console.log(`My chunk ID: ${myChunkId}`);
// Task 1 gets 1, Task 2 gets 2, Task 3 gets 3, etc. (guaranteed unique)
```

### Runner Tasks

```bash
# Increment counter
TASK=increment STACK_ID=my-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicCounter.ts

# Reset counter to 0
TASK=reset STACK_ID=my-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicCounter.ts

# Get current value
TASK=get-value STACK_ID=my-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicCounter.ts

# Check if table exists
TASK=exists STACK_ID=my-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicCounter.ts
```

---

## OCCFlag

### Purpose
Claims a "first writer wins" attribute on an existing DynamoDB item using a conditional write.
Ensures only **one caller** in a distributed system can successfully set the guarded attribute,
preventing duplicate execution of critical operations. Unlike the old dedicated-table design, an
`OCCFlag` instance targets any table/key/attribute supplied by the caller.

### Key Feature: Conditional Write
```typescript
ConditionExpression: 'attribute_not_exists(#attr)'
```

DynamoDB's conditional write ensures:
- ✅ Only first caller succeeds
- ✅ All other callers receive `ConditionalCheckFailedException`
- ✅ `onClaimSuccess`/`onClaimFailure` callbacks make the two outcomes explicit at the call site

### Methods

#### `update(value, onClaimSuccess, onClaimFailure)`
- Sets the guarded attribute atomically with condition
- **First caller**: Successfully sets the attribute, `onClaimSuccess()` is called
- **Other callers**: Condition fails, `onClaimFailure()` is called instead
- Stores any JSON-serializable value

#### `unset()`
- Removes just the guarded attribute (not the whole item) via `REMOVE`, guarded by
  `attribute_exists` so it's a no-op if not currently set
- Allows the attribute to be claimed again (useful for testing/reset and staleness recovery)

#### `getValue()`
- Returns the guarded attribute's current value or `undefined` if not set

### Use Cases

1. **Merger Trigger Guard** (current production use: `StatisticsTable.claimMergerTrigger()`,
   guarding the `MERGER_TRIGGER_CLAIM` record's `claimedAt` attribute)
   - Multiple processor tasks may each independently observe the completion condition
   - Only the first to claim triggers the merger; others see it already claimed and skip
   - Prevents duplicate merger execution

2. **Leader Election**
   - Multiple processes compete for leadership
   - First to claim becomes leader; others become followers via `onClaimFailure`

3. **One-Time Initialization / Idempotent Operations**
   - Ensure expensive setup runs exactly once
   - Guard against retry storms

### Usage: Any Existing Table, No Dedicated Schema

OCCFlag targets a caller-supplied partition key (and optional sort key) plus attribute name on
whatever table is passed in - e.g. a run's dedicated `MERGER_TRIGGER_CLAIM` record in the
statistics table, guarding its `claimedAt` attribute:

```json
{
  "integrationTimestamp": "2026-08-28T12:00:00.000Z",
  "eventType": "MERGER_TRIGGER_CLAIM",
  "claimedAt": "2026-08-28T12:34:56.789Z"
}
```

### Example Usage

```typescript
import { OCCFlag } from './dynamodb/OCCFlag';

const claim = new OCCFlag({
  tableName: 'my-stack-statistics-preview',
  region: 'us-east-2',
  partitionKeyName: 'integrationTimestamp',
  partitionKeyValue: syncRunId,
  sortKeyName: 'eventType',
  sortKeyValue: 'MERGER_TRIGGER_CLAIM',
  attributeName: 'claimedAt'
});

await claim.update(new Date().toISOString(),
  async () => {
    console.log('Won the claim - proceed with triggering the merger');
  },
  async () => {
    console.log('Lost the claim - another processor already triggered the merger');
  }
);
```

### Runner Tasks

```bash
# Attempt to claim (set) the guarded attribute
TASK=update TABLE_NAME=my-stack-statistics-preview REGION=us-east-2 \
  PARTITION_KEY_NAME=integrationTimestamp PARTITION_KEY_VALUE=2026-08-28T12:00:00.000Z \
  SORT_KEY_NAME=eventType SORT_KEY_VALUE=MERGER_TRIGGER_CLAIM ATTRIBUTE_NAME=claimedAt VALUE=2026-08-28T12:34:56.789Z \
  npx ts-node src/dynamodb/OCCFlag.ts

# Remove the guarded attribute (allows it to be claimed again)
TASK=unset TABLE_NAME=my-stack-statistics-preview REGION=us-east-2 \
  PARTITION_KEY_NAME=integrationTimestamp PARTITION_KEY_VALUE=2026-08-28T12:00:00.000Z \
  SORT_KEY_NAME=eventType SORT_KEY_VALUE=MERGER_TRIGGER_CLAIM ATTRIBUTE_NAME=claimedAt \
  npx ts-node src/dynamodb/OCCFlag.ts

# Get current value
TASK=get-value TABLE_NAME=my-stack-statistics-preview REGION=us-east-2 \
  PARTITION_KEY_NAME=integrationTimestamp PARTITION_KEY_VALUE=2026-08-28T12:00:00.000Z \
  SORT_KEY_NAME=eventType SORT_KEY_VALUE=MERGER_TRIGGER_CLAIM ATTRIBUTE_NAME=claimedAt \
  npx ts-node src/dynamodb/OCCFlag.ts

# Test race condition with 5 concurrent clients
TASK=test-race TABLE_NAME=my-stack-statistics-preview REGION=us-east-2 \
  PARTITION_KEY_NAME=integrationTimestamp PARTITION_KEY_VALUE=2026-08-28T12:00:00.000Z \
  SORT_KEY_NAME=eventType SORT_KEY_VALUE=MERGER_TRIGGER_CLAIM ATTRIBUTE_NAME=claimedAt \
  npx ts-node src/dynamodb/OCCFlag.ts
```

---

## Comparison: AtomicCounter vs OCCFlag

| Feature | AtomicCounter | OCCFlag |
|---------|--------------|------------|
| **Purpose** | Increment counter | Claim an attribute, first writer wins |
| **DynamoDB Operation** | `ADD` (accumulate) | `UPDATE` with `ConditionExpression` |
| **Concurrency Behavior** | All clients succeed | Only first client succeeds |
| **Return Value** | New counter value | void (calls `onClaimSuccess`/`onClaimFailure`) |
| **Key Method** | `increment()` | `update()` |
| **Reset Method** | `reset()` to 0 | `unset()` (removes just the attribute) |
| **Value Type** | number | any (JSON-serializable) |
| **Table Scope** | Dedicated table | Any existing table/attribute |
| **Use Case** | Counting, ID generation | Leader election, true-end claims, triggers |
| **Race Condition** | All count, unique values | First wins, others get `onClaimFailure` |

---

## Design Note: OCCFlag Intentionally Doesn't Share AtomicCounter's Pattern

AtomicCounter uses an abstract base class bound to its own dedicated table
(`{STACK_ID}-atomic-counter-{landscape}`), since a counter is inherently a standalone concept with
its own schema. OCCFlag is deliberately different: it's a concrete class, constructed with an
explicit `tableName`/key/`attributeName`, because OCC is a property of *how* a write happens, not
of any dedicated table - any existing table's any attribute can be subject to OCC. This means
OCCFlag requires **no new DynamoDB table or CDK changes** to use against an already-provisioned
table (e.g. an existing statistics table).

```typescript
// AtomicCounter: abstract, bound to its own dedicated table
export abstract class AbstractAtomicCounter { ... }

// OCCFlag: concrete, targets any table/key/attribute supplied by the caller
export class OCCFlag {
  constructor(params: {
    tableName: string, region?: string,
    partitionKeyName: string, partitionKeyValue: string,
    sortKeyName?: string, sortKeyValue?: string,
    attributeName: string
  }) { ... }
}
```

---

## When to Use Each Utility

### Use AtomicCounter When:
- ✅ You need to **count events** across concurrent workers
- ✅ You need **unique sequential IDs** (chunk IDs, sequence numbers)
- ✅ You need **sum aggregation** from parallel operations
- ✅ **All clients must succeed** with different values
- ✅ Order matters (first client gets 1, second gets 2, etc.)

### Use OCCFlag When:
- ✅ You need **exactly one caller** to perform an action
- ✅ You need **leader election** in a distributed system
- ✅ You need **one-time initialization** guarantees
- ✅ You need **idempotency** for critical operations
- ✅ You need to **prevent duplicate triggers** (e.g. merger trigger, true-end-of-data claims)
- ✅ The guarded attribute lives on an **existing** table you don't want to duplicate schema for

---

## Race Condition Prevention

### AtomicCounter: No Lost Increments
```typescript
// 5 concurrent workers - all succeed with unique values
const promises = [1,2,3,4,5].map(() => counter.increment());
const results = await Promise.all(promises);
// results: [1, 2, 3, 4, 5] (in some order, but all unique)
```

**Guarantee**: No increment is lost, every client gets a unique value.

### OCCFlag: First Client Wins
```typescript
// 5 concurrent workers - only one succeeds
const promises = [1,2,3,4,5].map(i => 
  flag.update(`worker-${i}`,
    async () => console.log(`Worker ${i} won`),
    async () => console.log(`Worker ${i} lost`)
  )
);
await Promise.all(promises);
// Output: "Worker 2 lost", "Worker 3 lost", "Worker 4 lost", "Worker 5 lost"
// (Worker 1 won, attribute value is "worker-1")
```

**Guarantee**: Exactly one client succeeds, all others get `onClaimFailure`.

---

## Implementation Notes

### AtomicCounter Implementation
- Uses DynamoDB `ADD` operation on `counter_value` attribute
- `ADD` is atomic at the DynamoDB service level (no client-side locking needed)
- Returns `UPDATED_NEW` to get the new value after increment
- Stores `LastUpdated` timestamp for observability

### OCCFlag Implementation
- Uses DynamoDB `UPDATE` with `attribute_not_exists(#attr)` condition (attribute name aliased to
  avoid reserved-word collisions)
- Conditional write prevents overwriting an existing claim
- Catches `ConditionalCheckFailedException` to detect "already claimed" and route to `onClaimFailure`
- `unset()` removes just the guarded attribute (not the whole item, since the item may carry other
  unrelated attributes) via `REMOVE` guarded by `attribute_exists`, silently no-op if not set
- No dedicated table, no timestamp bookkeeping baked in - callers add their own if needed

### Design Decision: Callbacks as Required Parameters
The `update()` method requires both `onClaimSuccess` and `onClaimFailure` rather than making either
optional:

```typescript
public update = async (
  value: any,
  onClaimSuccess: () => Promise<void>,
  onClaimFailure: () => Promise<void>
): Promise<void>
```

**Rationale**: Forces callers to explicitly handle both the "won" and "lost the race" scenarios,
making race condition handling visible in the code.

---

## Testing

### Test AtomicCounter
```bash
cd integration-huron-person-fargate
TASK=increment STACK_ID=test-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicCounter.ts
```

### Test OCCFlag Race Condition
```bash
cd integration-huron-person-fargate
TASK=test-race TABLE_NAME=test-table REGION=us-east-2 \
  PARTITION_KEY_NAME=pk PARTITION_KEY_VALUE=test-item ATTRIBUTE_NAME=claimed \
  npx ts-node src/dynamodb/OCCFlag.ts
```

This will simulate 5 concurrent clients trying to claim the same attribute - you'll see only one succeed and four trigger `onClaimFailure`.

---

## Infrastructure Requirements

### AtomicCounter Table
Requires a dedicated DynamoDB table, created via CDK/CloudFormation:
```typescript
new dynamodb.Table(this, 'AtomicCounterTable', {
  tableName: `${stackId}-atomic-counter-${landscape}`,
  partitionKey: { name: 'counter_name', type: dynamodb.AttributeType.STRING },
  billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
  removalPolicy: RemovalPolicy.DESTROY
});
```

### OCCFlag
No new table or CDK changes are required - `OCCFlag` targets an already-provisioned table (e.g. an
existing statistics table), supplied entirely via constructor parameters. The only infrastructure
concern is ensuring the caller's IAM role already has `dynamodb:UpdateItem`/`GetItem` on that table
(typically already true, since the same table is used for other reads/writes).

---

## Verification

✅ Both utilities build successfully (0 TypeScript errors)  
✅ OCCFlag requires no new infrastructure to adopt against an existing table  
✅ Comprehensive JSDoc comments for API documentation  
✅ Runner code with multiple test scenarios  
✅ Ready for use in production distributed systems
