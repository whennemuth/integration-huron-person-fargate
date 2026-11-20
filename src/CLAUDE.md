# src/: Pipeline Code, Utilities and Harnesses

Everything the task image compiles besides `docker/` and `context/*.ts`: the three phases' logic, the Lambdas'
handlers, the DynamoDB tables, and the operational tools. The pipeline as a whole is in the project
[CLAUDE.md](../CLAUDE.md); harness conventions in [docs/HARNESSES.md](../docs/HARNESSES.md).

**`src/` must not import from `lib/` or `dashboard/`** (the image compiles `src/` alone). Names that CDK and the
code both need live here (`src/Utils.ts`, the tables' `DYNAMODB_TABLE_NAME` exports) and `lib/` imports them.

## Map

| Folder | What | More |
|---|---|---|
| `chunking/` | Phase 1: the chunker subscriber, chunking from the API and from dropped files, run metadata, run start | [chunking/CLAUDE.md](chunking/CLAUDE.md) |
| `processing/` | Phase 2: the processors, the processor subscriber, the target simulator, addons | [processing/CLAUDE.md](processing/CLAUDE.md) |
| `post-processing/` | Phase 3: the post-processors, deferred deletes, the post-processor subscriber | [post-processing/CLAUDE.md](post-processing/CLAUDE.md) |
| `runner/` | The manual Runner | [runner/CLAUDE.md](runner/CLAUDE.md) |
| `dynamodb/` | Table classes: `StatisticsTable`, `AtomicCounter`, `OCCFlag`, `PersonCurrentStateTable`, `PersonHistoryTable`, `MockTargetPersonTable`, `PersonRecordProcessorLogTable`, the generic `DynamoDBTable` (`truncateTable()`), `BulkPurger`, `IntegrationRun` (prune one run) | [dynamodb/README.md](dynamodb/README.md), [docs/CONCURRENCY_CONTROL.md](../docs/CONCURRENCY_CONTROL.md), [docs/STATISTICS_TABLE.md](../docs/STATISTICS_TABLE.md) |
| `person-cache/` | The cache of people in the target, built through `PersonTargetReal`; `PersonCacheFactory` picks the S3 or DynamoDB variant | [person-cache/README.md](person-cache/README.md) |
| `reporting/` | `RunReporting`, the read side of runs, used by the dashboard | [docs/STATISTICS_TABLE.md](../docs/STATISTICS_TABLE.md#reading-runs-runreporting) |
| `schedule/` | The bulk sync schedule: `ScheduledRun` (the landscape's schedule and its rules), `EventBridgeSchedule` (the SDK), and the AWS cron model (`CronExpression`, `CronSchedule`, `CronExpressionBuilder`, `TimeZone`) | [schedule/CLAUDE.md](schedule/CLAUDE.md) |
| `storage/` | `IStorageAdapter` with S3 and file-system adapters | [docs/STORAGE_ADAPTER_USAGE.md](../docs/STORAGE_ADAPTER_USAGE.md) |

Top-level files:
- `Utils.ts`: `isMockLandscape()`, `ecsClusterName()`, `PROCESSOR_SERVICE_NAME`, `getStackName()`, config helpers.
- `DesiredCount.ts` (harness `DESIRED_COUNT`): get or set an ECS service's desired count, and its maximum.
- `Alarm.ts` (harness `ALARM`): `ServiceScaleInAlarm`, a service's scale-in alarm, its state and period.
- `TaskProtection.ts`: ECS task scale-in protection, so scaling in can't kill a task mid-work (exit 137).
- `Queue.ts`: queue message types, the SQS sender and reader, and an environment-based queue simulator for local runs.
- `ApiErrorTracking.ts` (harness `API_ERROR_TRACKING`): target API error events and throttling, persisted to the
  statistics table. `ApiErrorRetryStrategy.ts`: retries with exponential backoff and jitter.

## Harnesses here

Every module with a `require.main === module` block is a harness, run with `npx ts-node <file>` and its `.env` group.
Prefixes as the code declares them: `DESIRED_COUNT`, `ALARM`, `API_ERROR_TRACKING`; in `dynamodb/`:
`STATISTICS_TABLE`, `ATOMIC_COUNTER`, `OCC_FLAG`, `PERSON_CURRENT_STATE_TABLE`, `PERSON_HISTORY_TABLE`,
`MOCK_TARGET_PERSON_TABLE`, `BULK_PURGER`, `INTEGRATION_RUN_PRUNER`; in `person-cache/`: `PERSON_CACHE`
(`PersonCacheForS3.ts`); in `schedule/`: `CRON_SCHEDULE` (`CronSchedule.ts`), `SCHEDULED_RUN` (`ScheduledRun.ts`); the phases' and the Runner's in their
folders' files. Several table harnesses offer a
`truncate` task. To add one: [docs/HARNESSES.md](../docs/HARNESSES.md), and list it here.

## Pitfalls

- **Turning a subscriber off**: a subscriber Lambda with `DRY_RUN=true` logs what it would do and sends nothing (the
  chunker subscriber: no chunker messages; the processor subscriber: chunk files aren't processed). It stays that way
  until set back; the Runner's decorators and `ServiceToggler` set it (`src/runner/CLAUDE.md`), or:
  `aws lambda update-function-configuration --function-name chunker-subscriber-<landscape> --environment "Variables={DRY_RUN=true}"`
  (this replaces the whole environment: prefer `ServiceToggler`).
- **Table truncation is for mock landscapes**: `BulkPurger` refuses a non-mock landscape without `force: true`, and
  anything that clears the atomic counter table must leave its `not-a-counter:` items alone.
