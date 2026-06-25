# src/: Utilities & Test Harnesses

Test harnesses and utility modules for validating infrastructure, orchestrating manual operations, and debugging deployed services.

## Key Modules

### Runner.ts - Manual Chunking Orchestration
Test harness for manually triggering the chunking service outside of EventBridge schedule.

**Usage**:
```bash
# Simple trigger
CHUNKER_QUEUE_URL="..." REGION="us-east-2" STACK_ID="..." LANDSCAPE="dev" npx ts-node src/Runner.ts

# Queue seeding for parallel testing
MESSAGES_TO_PREPOPULATE="50" DESIRED_COUNT="10" npx ts-node src/Runner.ts

# Single person test
SINGLE_PERSON_BUID="U12345678" npx ts-node src/Runner.ts
```

### DesiredCount.ts - ECS Service Scaling
Utility for scaling ECS services up or down.

### PersonCache.ts - Person Data Caching
Cache person data for testing and debugging.

### Queue.ts - SQS Queue Utilities
Helper functions for working with SQS queues.

### TaskProtection.ts - ECS Task Protection
Manage ECS task protection to prevent premature termination.

### AtomicCounter.ts - DynamoDB Atomic Counter
DynamoDB-backed atomic counter for offset generation.

## Subdirectories

- `statistics/` - Statistics table utilities
- `storage/` - S3 storage helpers
- `chunking/` - Chunking phase utilities
- `processing/` - Processing phase utilities
- `merging/` - Merging phase utilities

## Execution

### VS Code (Recommended)
1. Open file (e.g., `src/Runner.ts`)
2. Press F5 → "Debug current file"
3. Breakpoints and debugging enabled

### Command Line
```bash
npx ts-node src/Runner.ts
npx ts-node src/DesiredCount.ts
```

## Environment Variables

Each utility uses `TestEnvironment` pattern for prefix-based variable loading:

```typescript
const testEnvironment = TestEnvironment('RUNNER');
['VAR1', 'VAR2'].forEach(testEnvironment.getVar);
```

See `.env` file and `example-env.md` for variable documentation.

## See Also

- [src/CLAUDE.md](./CLAUDE.md) - Detailed documentation
- [lib/CLAUDE.md](../lib/CLAUDE.md) - CDK infrastructure documentation
- [docker/CLAUDE.md](../docker/CLAUDE.md) - Container entry points
