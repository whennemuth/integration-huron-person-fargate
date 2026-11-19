# src/: Pipeline Code, Utilities and Test Harnesses

The pipeline's three phases, the Lambdas' handlers, the DynamoDB tables, and the operational tools and test
harnesses for validating infrastructure, starting runs by hand and debugging deployed services. A map of every
folder and file is in [CLAUDE.md](./CLAUDE.md).

## Folders

- `chunking/` - Phase 1: the chunker subscriber, chunking from the source API and from dropped files, run metadata,
  starting a run
- `processing/` - Phase 2: the processors, the processor subscriber, the target simulator, addons
- `post-processing/` - Phase 3: the post-processors, deferred deletes, the post-processor subscriber
- `runner/` - the manual Runner ([runner/README.md](./runner/README.md))
- `dynamodb/` - the table classes: statistics, atomic counters, person state and history, ... ([dynamodb/README.md](./dynamodb/README.md))
- `person-cache/` - the cache of people in the target ([person-cache/README.md](./person-cache/README.md))
- `reporting/` - `RunReporting`, the read side of runs, used by the dashboard
- `storage/` - S3 and file-system storage adapters ([docs/STORAGE_ADAPTER_USAGE.md](../docs/STORAGE_ADAPTER_USAGE.md))

## Top-level modules

- `Utils.ts` - stack and cluster names, `isMockLandscape()`, config loading
- `DesiredCount.ts` - get or set an ECS service's desired count (harness `DESIRED_COUNT`)
- `Alarm.ts` - a service's scale-in alarm, its state and period (harness `ALARM`)
- `TaskProtection.ts` - ECS task scale-in protection, so scaling in can't kill a task mid-work
- `Queue.ts` - SQS message types, sender and reader
- `ApiErrorTracking.ts` - target API errors and throttling, recorded in the statistics table (harness `API_ERROR_TRACKING`);
  `ApiErrorRetryStrategy.ts` - retries with exponential backoff and jitter

## Running a harness

Every module with a `require.main === module` block is a harness. Its variables are the group in `.env` named by the
prefix it passes to `TestEnvironment(...)` (e.g. `RUNNER_`); `example-env.md` documents them.

### VS Code (Recommended)
1. Open the file (e.g., `src/runner/Runner.ts`)
2. Press F5 → "Debug current file"
3. Breakpoints and debugging enabled

### Command Line
```bash
npx ts-node src/runner/Runner.ts
npx ts-node src/DesiredCount.ts
```

```typescript
if (require.main === module) {
  const testEnvironment = TestEnvironment('RUNNER');
  ['VAR1', 'VAR2'].forEach(testEnvironment.getVar);
  // ...
}
```

## See Also

- [src/CLAUDE.md](./CLAUDE.md) - the folder map, harness prefixes and pitfalls
- [src/runner/CLAUDE.md](./runner/CLAUDE.md) - the Runner's modes and settings
- [lib/CLAUDE.md](../lib/CLAUDE.md) - CDK infrastructure
- [docker/CLAUDE.md](../docker/CLAUDE.md) - the task image and its entry points
