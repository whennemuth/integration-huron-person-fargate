# Starting a Run

How a sync run of the pipeline starts: what the EventBridge schedule sends, how the run records who started it, what
is reset before the first chunker message goes out, how a run can start with several chunker tasks at once, how a run
can set its own processor maximum, and how a mock landscape's run can start from scratch. Read this before changing
the chunker subscriber Lambda (`src/chunking/ChunkerSubscriber.ts`, `lib/services/chunker/ChunkerSubscribingLambda.ts`),
`ScheduledRun`, the manual Runner's run start, or the dashboard's Bulk sync.

The order of a run start in the chunker subscriber, for a run's first API event: refusals (parallel start, mock reset,
processor maximum override), then a scheduled start skipped while a run is underway (below), then the processor
maximum, then a mock landscape's reset, then the run counters, then the parallel start (or the single queued message).

## The scheduled run

**The scheduled run is a full sync with stored hashes trusted** (`ScheduledRun`, `src/schedule/ScheduledRun.ts`, since
2026-11-11): the EventBridge schedule's input (`ScheduledRun.scheduleInput()`; the schedule is managed at run time
through `ScheduledRun`, never deployed) is `populationType: person-full`, `previousStateSource: storage`, with the
people endpoint, `iterationLimit` = the context's
`ECS.chunkerService.chunksPerTask` and `offset: 0`.

- No source endpoint returns only the people who changed yet, so a scheduled run fetches everyone. Trusting the
  stored hashes keeps people whose hash matches from reaching the target, which is the effect a delta run would
  have; the full sync then deactivates people missing from the source. Before, the schedule sent `person-delta`
  (no deactivations) with `trustPreviousStorage` from the context (`false` everywhere), so every scheduled run
  looked every person up in the target and deactivated no one.
- A source glitch that looks like a normal end (an empty batch mid-population) would deactivate the people after
  it, until the next good run reactivates them (decided 2026-11-10: no deactivation ceiling). A change made in
  the target outside the integration isn't put right for a person whose source record hasn't changed, until a
  bulk reset (the dashboard's Bulk sync) runs.
- Where a run's previous state comes from is one setting, `previousStateSource` (`storage`, or `target`: a bulk
  reset; `src/chunking/PreviousStateSource.ts`), set from the bulk reset choice by
  `ScheduledRun.previousStateSource(bulkReset)` for the schedule and the dashboard alike, and by
  `RUNNER_PREVIOUS_STATE_SOURCE` for the Runner. It has no default: the chunker subscriber refuses a start without one.
  It replaced the `bulkReset` / `trustPreviousStorage` pair, which only ever acted together (the subscriber still
  translates the pair from an old sender), and the context's dead `BULK_RESET` / `TRUST_PREVIOUS_STORAGE` are gone.
- A landscape gets this at its next pipeline deploy (a `lib/` change).

### A scheduled start while a run is underway

The chunker subscriber skips a start the bulk sync schedule makes (`startedBy.by === 'schedule'`, a run's first
event) while a run is underway in its landscape, or while a run started from the dashboard moments ago is still
starting (its launch flag, `/hp-dashboard/launch-flags/<landscape>`), since two runs at once would share the chunker's
counters and the run list (`ScheduledStartGuard`, `src/schedule/ScheduledStartGuard.ts`, through
`scheduledStartRefusal()` in `src/chunking/ChunkerSubscriber.ts`).

- **Underway** is the dashboard's rule (`RunActivity`, `src/reporting/RunActivity.ts`, which Bulk sync's guards use
  too), over the newest run in the statistics table's run list. A stalled run doesn't hold the schedule back, or a run
  that died would stop every scheduled run after it; a finished or failed run doesn't either.
- **When the run state can't be read, the start is skipped**: a missed day is safer than two runs at once.
- **The skip is returned** as a 409 result, never thrown, like the other refusals, and **recorded** in
  `/hp-dashboard/schedule-refusals/<landscape>` (an SSM parameter, the last one only), which the dashboard's Bulk sync
  schedule page shows: EventBridge Scheduler doesn't record what its target answered, so otherwise a skipped day would
  leave nothing but a missing run. A recording that fails only warns.
- **Only the schedule's starts** are checked: a person starting a run has Bulk sync's or the Runner's own guards.
- **Grants and environment**: the subscriber reads the statistics table (`GetItem`, `Query`, with its indexes; named in
  `DYNAMODB_STATISTICS_TABLE_NAME`, with `PREVIOUS_STORAGE_TYPE`) and the launch flag, and writes the refusal record.
  A subscriber deployed without `DYNAMODB_STATISTICS_TABLE_NAME` doesn't check, with a warning.
- In S3 storage mode a failed run's terminal error is in the chunks bucket, which this doesn't read: such a run reads as
  underway until it stalls, and scheduled starts are skipped until then.

## Who started a run (`startedBy`)

**Who started a run is recorded with it** (`startedBy`, `src/chunking/StartedBy.ts`, since 2026-11-11), for the
dashboard's Run history: `{ by: 'dashboard', principal? }` (Bulk sync, with the signed-in user's `buPrincipal`),
`{ by: 'schedule' }` (`ScheduledRun.scheduleInput()`) or `{ by: 'runner' }` (`RunnerForSingleMessage`,
`RunnerForSinglePerson`, `QueueSeeder`). Structured, so a principal can't be mistaken for a keyword; `RunStarter.from()`
reads one back, keeping only the known fields.

- **Its path**, like `termsEndpoint`'s: `ApiChunkerEvent.startedBy` -> the chunker subscriber's queue message
  (`handleApiEvent()`) -> `TaskParameters` (`ChunkFromAPI`, `getStartedBy()`) -> each chained chunker message
  (`ChunkerQueue.sendNextChunkingMessage()`) -> `Flags.startedBy` (`docker/chunker.ts`; the FLAGS item, or
  `_flags.json` in S3 mode; written once per run, by its first chunker task) -> `RunSummary.startedBy`
  (`AbstractPostProcessor.writeRunSummary()` reads the flags in either storage mode, since a summarized run's
  report is its summary alone).
- Runs from before it was recorded, and the S3 file drop, have none. The principal is stored with the run in the
  statistics table, so anyone who can read Run history sees it.
- Tests: `test/StartedBy.test.ts` (and the path's own tests: `ChunkerQueueChainStop`, `MetadataForDynamoDb`,
  `StatisticsTable`, `ScheduledRun`).

## The run counters

**A run starts with the chunker's counters at zero**: the source offset its chained messages take
(`chunker-offset-counter`) and chunk ordinals (`chunker-chunk-ordinal-counter`), both in the landscape's
atomic counter table. `resetRunCounters()` (`src/chunking/RunCounters.ts`) resets them, called from:

- the chunker subscriber Lambda's `handler()`, before it queues an API event that starts a run (offset 0, no
  `chunkDirectory`: `resetCountersForRunStart()` in `src/chunking/ChunkerSubscriber.ts`). That covers the
  EventBridge schedule's runs (which until 2026-11-10 never reset them) and the dashboard's Bulk sync. The Lambda
  has `STACK_ID`/`LANDSCAPE` in its environment and `UpdateItem` on that one table. Skipped in `DRY_RUN`; a
  failed reset fails the invocation.
- the manual Runner (`AbstractRunner.resetAtomicCounters()`), which calls `handleApiEvent()` in-process rather than
  through the Lambda.

Never in `handleApiEvent()` itself: the Runner's queue seeding calls it for every seeded message, taking their
offsets from the counter it has just reset (`QueueSeeder` resets its own).

In a mock landscape, a run start also resets the source simulator's depletion counter (`simulator-offset-counter`,
`runCounterNames()` in `src/chunking/RunCounters.ts`), with the chunker's two, so a run with the simulator reads its
population from the first person. A run from the real source doesn't notice.

## One chunker message, or N seeded at once (`parallelStart`)

**A run starts with one chunker message, or N seeded at once** (`parallelStart`, since 2026-11-12):

- **One message** (no `parallelStart`, or 0): the subscriber queues the event, and the chunker service's auto-scaling
  adds tasks as the work builds up. The EventBridge schedule always starts this way.
- **Seeded** (`ApiChunkerEvent.parallelStart` N, 1-20; Bulk sync's Parallel start): after the counter reset, the
  subscriber seeds the queue with N messages (`QueueSeeder`, offsets a task's chunks apart from the counter), waits
  for the chunker's scale-in alarm to leave `ALARM` (`MetricsCatchupDelay`, the 2026-07-02 incident fix), then sets
  the chunker service's desired count to N (`DesiredCount`). The procedure is `src/chunking/ParallelStart.ts`, which
  the Runner's queue seeding (`RunnerForQueueSeeding`, `MESSAGES_TO_PREPOPULATE` / `DESIRED_COUNT`) shares.
- **Refused** with a 400 result, returned rather than thrown (a throw would be retried, if retries were on, and refused
  again): before the counter reset, N outside 1-20, an event that doesn't start a run, `iterationLimit` 0 (seeded
  offsets step by a task's chunks), a single-person fetch (`buid`), or missing environment (`ParallelStart.refusal()`);
  after the reset but before anything is sent, N above the service's `MaxCapacity`. In `DRY_RUN` it only logs what it
  would do. Chained messages never carry `parallelStart`.

**The subscriber Lambda** (`lib/services/chunker/ChunkerSubscribingLambda.ts`) has, for this:

- a 600-second timeout (context `LAMBDA.chunkerSubscriber.timeoutSeconds`, every landscape): the alarm wait alone
  can take six minutes (a 60-second period plus a 300-second buffer). The wait is also bounded by the invocation's
  remaining time, less a minute, so a slow alarm can't time the Lambda out before the desired count is set.
- no automatic retry (`retryAttempts: 0`, events older than 15 minutes dropped): Lambda retries a failed
  asynchronous invocation twice by default, which for a run start would reset the counters and seed again into
  a run already underway. A failed start is started again by a person.
- `CHUNKER_CLUSTER_NAME` / `CHUNKER_SERVICE_NAME` in its environment, and the grants: `ecs:UpdateService` and
  `DescribeServices` on that one service, `application-autoscaling:DescribeScalableTargets` /
  `DescribeScalingPolicies` (resource `*`; they can't be scoped), `cloudwatch:DescribeAlarms` on the region's
  alarms.

First checked in staging on 2026-11-12 with 15 tasks: the alarm settled after 150 seconds, the 15 tasks were
running 45 seconds after the desired count was set, and no offset or person was chunked twice.

### History: the 2026-07-02 scale-in incident

On 2026-07-02 the chunker service scaled from a desired count of 4 to 0 before the Runner's queue seeding took
effect: its scale-in alarm could already be in `ALARM` from a previously empty queue, and CloudWatch evaluates alarms
periodically, so there was a gap between seeding and the alarm catching up. (The analysis was written up as
`INCIDENT_ANALYSIS_2026_07_02.md` at the repository root, no longer in the repository.)

Since then, queue seeding calls `MetricsCatchupDelay` (`src/runner/MetricsCatchupDelay.ts`) before setting the desired
count: it waits for the alarm's period plus a buffer, logging a countdown, and ends early once the alarm leaves
`ALARM`; alarm discovery is `ServiceScaleInAlarm` in `src/Alarm.ts`. The intent is to stop a stale scale-in alarm from
immediately reversing the desired count. The seeded parallel start above shares the same procedure.

At the time, the synthesized alarms were: the chunker's scale-in (lower) alarm on the composite metric
`visible + notVisible`, its scale-out (upper) alarm on visible messages only; the processor's and post-processor's
alarms both on visible messages only. So only the chunker's scale-in is protected against in-flight-only visibility
artifacts.

## A run's own processor maximum (`processorScalingCapacityOverride`)

**A run may have its own processor maximum** (`processorScalingCapacityOverride`, since 2026-11-17,
`processor-max-tasks-plan.md`): the processor service's "Max tasks" (its scalable target's `MaxCapacity`, which
`ProcessorServiceBooster` raises the desired count to while chunking) decides how many processors a run uses, and so
how hard they press Huron, which throttles them above a point.

- **The context's `ECS.processorService.maxScalingCapacity`** stays the deployed `MaxCapacity` and never changes at run
  time. It is what every run gets without an override, where Bulk sync's Processor tasks starts, and the point above
  which its review warns of throttling (and lets the run start: that is how to find out whether Huron takes more now).
- **Every run start sets the live maximum** (`src/chunking/ProcessorScalingCapacity.ts`, `apply()`): to the run's
  override (1-20, `ProcessorScalingCapacity.MIN`/`MAX`), or else back to `maxScalingCapacity`. Nothing sets it back
  after a run: the next run start does, and the schedule never carries an override. Between runs the service keeps
  the last run's maximum, but with nothing queued it runs no tasks.
  - The chunker subscriber does it for a run's first API event, right after its refusals and before a mock
    landscape's reset, the counters and the parallel start (`setProcessorCapacityForRunStart()`), and for a dropped
    file before its message is sent (`handleS3Event()`'s `beforeStart`). Its Lambda has `PROCESSOR_SERVICE_NAME` and
    `PROCESSOR_MAX_SCALING_CAPACITY`, `application-autoscaling:RegisterScalableTarget` on `*` (a scalable target's
    ARN carries an ID AWS assigns) limited by the conditions `service-namespace` = `ecs` and `scalable-dimension` =
    `ecs:service:DesiredCount`, and `ecs:DescribeServices`/`UpdateService` on the processor service.
  - The manual Runner does it in `ChunkingServiceRunner.start()`, before its counter reset
    (`PROCESSOR_SCALING_CAPACITY_OVERRIDE`, else the local context's `maxScalingCapacity`).
  - **Failures**: an override that can't be set fails the start before anything is reset or sent; restoring
    `maxScalingCapacity` only warns, so a glitch can't stop the schedule. `DRY_RUN` logs it.
- **The run records it** (`Flags.processorScalingCapacity`, `{ maxTasks, overridden }`,
  `src/chunking/ProcessorCapacityRecord.ts`), by `startedBy`'s path (the first event, once the maximum is set ->
  queue messages, seeded and chained -> Flags -> summary), and the run's page shows it ("Up to 8 processor tasks
  (override)"). None for a run whose maximum wasn't set, or a file drop.
- **Known limits**: a run start changes the maximum of a run already underway (as it already resets that run's
  counters). CloudFormation reports the scalable target as drifted after an override; a deploy doesn't put it back
  unless `maxScalingCapacity` changes in the template, the next run start does.
- The cluster's and processor service's names are in `src/Utils.ts` (`ecsClusterName()`, `PROCESSOR_SERVICE_NAME`),
  which `lib/` takes, since the task image compiles `src/` without `lib/`.
- Tests: `test/ProcessorScalingCapacity.test.ts`, `test/ProcessorScalingCapacityRunStart.test.ts`,
  `test/RunnerProcessorScalingCapacity.test.ts`, and the template's in `test/RunCounters.test.ts`.

## A mock landscape's run from scratch (`resetState`)

**A mock landscape's run can start from scratch** (`ApiChunkerEvent.resetState`, since 2026-11-14,
`mock-landscape-options-plan.md`): the subscriber resets the landscape's run state before the counters and before
anything is queued or seeded, through `src/chunking/MockLandscapeReset.ts`, which the Runner's
`MOCK_TARGET_RESET_STATE` shares (see [MOCK_LANDSCAPES.md](MOCK_LANDSCAPES.md) for what it empties). Emptying the
statistics table empties the landscape's Run history too.

- **Checked first, with the parallel start** (`runStartRefusal()`): a 400 result, nothing reset or sent, for a reset
  on an event that doesn't start a run, outside a mock landscape (`MockLandscapeReset.refusal()`), or without
  `CHUNKS_BUCKET_NAME`. Skipped in `DRY_RUN`; a failed reset fails the invocation (not retried), so the run doesn't
  start. Chained and seeded messages never carry `resetState`.
- **Only a mock landscape's subscriber can do it**: `ChunkerSubscribingLambda` gives it `CHUNKS_BUCKET_NAME` and the
  grants (`dynamodb:Scan`, `BatchWriteItem`, `DescribeTable` on the mock target, statistics and both person tables;
  `s3:ListBucket` limited to the `delta-storage/` prefix and `s3:DeleteObject` under it) only when
  `isMockLandscape()`; another landscape's subscriber has neither.
- **The time it takes** comes out of the invocation's 600 seconds, which a parallel start's alarm wait shares (that
  wait is bounded by the time left). The tables are truncated at the same time (`BulkPurger`), each with a parallel
  scan of `DynamoDBTable.TRUNCATE_SEGMENTS` segments, each keeping `TRUNCATE_BATCHES_IN_FLIGHT` delete batches of 25
  in flight (`DynamoDBTable.truncateTable()`, since 2026-11-15; throttled deletes retried with backoff), because one
  batch at a time couldn't empty the about 438,000 items a hybrid full sync left in mock within the time. Every
  truncation uses it: the Runner's reset and the tables' harness `truncate` tasks too. The mock target table's
  truncation goes through it as well (it used to read every whole record into memory and delete them one
  `DeleteItem` at a time, which the subscriber isn't granted).
- **Why mock's subscriber has 1,024 MB**: the first reset in mock (2026-11-16, about 438,000 items) took 348 s of the
  600 with no DynamoDB throttling: the limit was the Lambda's own share of a CPU, and it used 239 of its 256 MB with
  about 100 delete requests in flight. So mock's chunker subscriber has 1,024 MB
  (`LAMBDA.chunkerSubscriber.memorySizeMb` in `context.mock.json`): four times the memory, and the CPU share that
  comes with it.
- **A reset on its own** (`MockLandscapeResetEvent` `{ mockLandscapeReset: true, startedBy?, requestedAt? }`; the
  dashboard's Overview, Reset landscape): the subscriber runs the same reset and nothing else, with the same refusals
  (`resetEventRefusal()`), skipped in `DRY_RUN` (`handleResetEvent()`). It shows what Start from scratch will take.
- **Every reset is recorded** (`MockLandscapeResetRecord`, `MockLandscapeReset.recorder()`) in the landscape's atomic
  counter table, which no reset empties: the item `counter_name` `not-a-counter:mock-landscape-reset` (borrowing the
  table, not a counter: `NOT_A_COUNTER_PREFIX` in `src/dynamodb/AtomicCounter.ts`, and a `purpose` attribute saying
  so; anything that clears the counters must leave such items alone), attribute `reset`, written
  with `UpdateItem` (granted for the run counters) when it starts and when it ends: how it was started (`overview` or
  `bulk-sync`), by whom, when, each table's count and time, the S3 baseline objects, or the error. One still
  `running` 15 minutes after it started has stopped (`statusOf()`: its Lambda ran out of time). Recording never fails
  the reset; a failed reset is recorded, then fails the invocation.
