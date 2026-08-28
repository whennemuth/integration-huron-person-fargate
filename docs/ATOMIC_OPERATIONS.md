# Atomic Operations in DynamoDB

## Overview
This document covers two utility classes for managing atomic operations in DynamoDB to prevent race conditions in distributed systems:
- **AtomicCounter**: Atomic increment operations with guaranteed uniqueness
- **AtomicFlag**: "Can only be set once" flags using conditional writes

Both utilities follow the same design pattern and are essential tools for coordinating concurrent operations across multiple processors, Lambda functions, or ECS tasks.

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

## AtomicFlag

### Purpose
Manages "can only be set once" flags using conditional writes. Ensures only **one client** in a distributed system can successfully set a flag, preventing duplicate execution of critical operations.

### Key Feature: Conditional Write
```typescript
ConditionExpression: 'attribute_not_exists(flag_value)'
```

DynamoDB's conditional write ensures:
- ✅ Only first client succeeds
- ✅ All other clients receive `ConditionalCheckFailedException`
- ✅ Fallback function called for clients that lost the race

### Methods

#### `setFlag(value, fallback)`
- Sets flag value atomically with condition
- **First client**: Successfully sets flag, operation completes
- **Other clients**: Condition fails, fallback function is called
- Stores any JSON-serializable value

#### `unsetFlag()`
- Deletes the flag from DynamoDB using `DeleteCommand`
- Allows flag to be set again (useful for testing/reset)

#### `getFlagValue()`
- Returns current flag value or `undefined` if not set

#### `tableExists(tableName?)`
- Checks if DynamoDB table exists

### Use Cases

1. **Merger Trigger Guard** (primary use case)
   - Last processor sets flag to trigger merger
   - Other processors see flag already set and skip trigger
   - Prevents duplicate merger execution

2. **Leader Election**
   - Multiple processes compete for leadership
   - First to set flag becomes leader
   - Others become followers via fallback

3. **One-Time Initialization**
   - Ensure expensive setup runs exactly once
   - First client does initialization
   - Others skip via fallback

4. **Idempotent Operations**
   - Prevent duplicate execution of critical operations
   - Guard against retry storms

### DynamoDB Schema

**Table Name**: `{STACK_ID}-atomic-flag-{landscape}`

**Item Structure**:
```json
{
  "flag_name": "merger-triggered-2026-08-28T12:00:00.000Z",
  "flag_value": "chunk-0009",
  "SetAt": "2026-08-28T12:05:30.123Z"
}
```

**Partition Key**: `flag_name` (string)

### Example Usage

```typescript
import { AbstractAtomicFlag } from './dynamodb/AtomicFlag';

const mergerTriggerFlag = new class extends AbstractAtomicFlag {
  getFlagName() { 
    return `merger-triggered-${syncRunId}`; 
  }
}({ stackId: 'my-stack', region: 'us-east-2', landscape: 'preview' });

// Last processor tries to set flag
await mergerTriggerFlag.setFlag(chunkId, async () => {
  console.log('Merger already triggered by another processor');
  // Fallback: do nothing, merger is already in progress
});
```

### Runner Tasks

```bash
# Set flag with optional value
TASK=set STACK_ID=my-stack REGION=us-east-2 FLAG_VALUE=processor-0009 npx ts-node src/dynamodb/AtomicFlag.ts

# Remove flag
TASK=unset STACK_ID=my-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicFlag.ts

# Get current value
TASK=get-value STACK_ID=my-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicFlag.ts

# Check if table exists
TASK=exists STACK_ID=my-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicFlag.ts

# Test race condition with 5 concurrent clients
TASK=test-race STACK_ID=my-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicFlag.ts
```

---

## Comparison: AtomicCounter vs AtomicFlag

| Feature | AtomicCounter | AtomicFlag |
|---------|--------------|------------|
| **Purpose** | Increment counter | Set once flag |
| **DynamoDB Operation** | `ADD` (accumulate) | `UPDATE` with conditional |
| **Concurrency Behavior** | All clients succeed | Only first client succeeds |
| **Return Value** | New counter value | void (success/fallback) |
| **Key Method** | `increment()` | `setFlag()` |
| **Reset Method** | `reset()` to 0 | `unsetFlag()` (delete) |
| **Value Type** | number | any (JSON-serializable) |
| **Use Case** | Counting, ID generation | Leader election, triggers |
| **Race Condition** | All count, unique values | First wins, others fallback |

---

## Shared Design Pattern

Both utilities follow the same architectural pattern:

### 1. Abstract Base Class
```typescript
export abstract class AbstractAtomicCounter { ... }
export abstract class AbstractAtomicFlag { ... }
```

### 2. Abstract Method
```typescript
public abstract getCounterName(): string;
public abstract getFlagName(): string;
```

### 3. Constructor Signature
```typescript
constructor({ stackId, region, landscape }: { 
  stackId: string; 
  region: string; 
  landscape: string 
})
```

### 4. DynamoDB Client Initialization
```typescript
private client: DynamoDBDocumentClient;
this.client = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
```

### 5. Table Name Convention
```typescript
export const DYNAMODB_TABLE_NAME = (context: IContext) => 
  `${context.STACK_ID}-atomic-{counter|flag}-${context.TAGS.Landscape.toLowerCase()}`;
```

### 6. Runner with TestEnvironment
```typescript
if (require.main === module) {
  const testEnvironment = TestEnvironment('ATOMIC_{COUNTER|FLAG}');
  // ... task enum and switch statement
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

### Use AtomicFlag When:
- ✅ You need **exactly one client** to perform an action
- ✅ You need **leader election** in distributed system
- ✅ You need **one-time initialization** guarantees
- ✅ You need **idempotency** for critical operations
- ✅ You need to **prevent duplicate triggers** (e.g., merger trigger)

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

### AtomicFlag: First Client Wins
```typescript
// 5 concurrent workers - only one succeeds
const promises = [1,2,3,4,5].map(i => 
  flag.setFlag(`worker-${i}`, () => console.log(`Worker ${i} lost`))
);
await Promise.all(promises);
// Output: "Worker 2 lost", "Worker 3 lost", "Worker 4 lost", "Worker 5 lost"
// (Worker 1 won, flag value is "worker-1")
```

**Guarantee**: Exactly one client succeeds, all others execute fallback.

---

## Implementation Notes

### AtomicCounter Implementation
- Uses DynamoDB `ADD` operation on `counter_value` attribute
- `ADD` is atomic at the DynamoDB service level (no client-side locking needed)
- Returns `UPDATED_NEW` to get the new value after increment
- Stores `LastUpdated` timestamp for observability

### AtomicFlag Implementation
- Uses DynamoDB `UPDATE` with `attribute_not_exists(flag_value)` condition
- Conditional write prevents overwriting existing flag
- Catches `ConditionalCheckFailedException` to detect "already set"
- Uses `DeleteCommand` for unset (cleaner than setting to null)
- Stores `SetAt` timestamp for audit trail

### Design Decision: Fallback as Required Parameter
The `setFlag()` method requires a fallback function rather than having it optional:

```typescript
public setFlag = async (value: any, fallback: () => Promise<void>): Promise<void>
```

**Rationale**: Forces callers to explicitly handle the "lost the race" scenario, making race condition handling visible in the code.

---

## Testing

### Test AtomicCounter
```bash
cd integration-huron-person-fargate
TASK=increment STACK_ID=test-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicCounter.ts
```

### Test AtomicFlag Race Condition
```bash
cd integration-huron-person-fargate
TASK=test-race STACK_ID=test-stack REGION=us-east-2 npx ts-node src/dynamodb/AtomicFlag.ts
```

This will simulate 5 concurrent clients trying to set the same flag - you'll see only one succeed and four trigger the fallback.

---

## Infrastructure Requirements

Both utilities require DynamoDB tables to be created via CDK/CloudFormation:

### AtomicCounter Table
```typescript
new dynamodb.Table(this, 'AtomicCounterTable', {
  tableName: `${stackId}-atomic-counter-${landscape}`,
  partitionKey: { name: 'counter_name', type: dynamodb.AttributeType.STRING },
  billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
  removalPolicy: RemovalPolicy.DESTROY
});
```

### AtomicFlag Table
```typescript
new dynamodb.Table(this, 'AtomicFlagTable', {
  tableName: `${stackId}-atomic-flag-${landscape}`,
  partitionKey: { name: 'flag_name', type: dynamodb.AttributeType.STRING },
  billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
  removalPolicy: RemovalPolicy.DESTROY
});
```

---

## Verification

✅ Both modules build successfully (0 TypeScript errors)  
✅ Pattern consistency maintained across both utilities  
✅ Comprehensive JSDoc comments for API documentation  
✅ Runner code with multiple test scenarios  
✅ Ready for use in production distributed systems
