# src/runner: Manual Run Orchestration

The manual, off-schedule way to start a run: chooses a runner mode, loads configuration, prepares the run (processor
maximum, counters), optionally toggles the subscribers' `DRY_RUN` for test scenarios, and sends the chunker its
event(s). Not the normal path (that is the EventBridge schedule and the dashboard's Bulk sync, through the chunker
subscriber Lambda), but the two share the run-start procedures in `src/chunking/` ([docs/RUN_START.md](../../docs/RUN_START.md)).

Run it with `npx ts-node src/runner/Runner.ts` (or VS Code's "Debug current file"); its variables are the `RUNNER_`
group of `.env` (`RunnerTypes.setTestEnvironment()`). The other harnesses here: `npx ts-node src/runner/MetricsCatchupDelay.ts`
(`METRICS_CATCHUP_DELAY_`) and `npx ts-node src/runner/ServiceToggler.ts` (`SERVICE_TOGGLER_`).

## Files

- **`Runner.ts`**: entry point. Chooses the runner: single person (`SINGLE_PERSON_BUID`), queue seeding
  (`MESSAGES_TO_PREPOPULATE > 0`), or single message (default). Applies the decorators: `MESSAGING_ONLY`,
  `CHUNKING_ONLY`, `SOURCE_SIMULATOR`, otherwise restore full operation; and `MockLandscapeRunnerDecorator` whenever
  `LANDSCAPE` is a mock landscape. Aborts if `SOURCE_SIMULATOR=true` targets a non-mock landscape.
- **`AbstractRunner.ts`** (`ChunkingServiceRunner`), the template method `start()`:
  1. validate prerequisites (after refusing a `PROCESSOR_SCALING_CAPACITY_OVERRIDE` outside 1-20);
  2. load configuration (ConfigManager chain);
  3. resolve the data source;
  4. validate the endpoint;
  5. at a run's start (unless `SKIP_ATOMIC_COUNTER_RESET`): set the processor's maximum (`setProcessorScalingCapacity()`:
     the override, else the local context's `ECS.processorService.maxScalingCapacity`, as the chunker subscriber does;
     [docs/RUN_START.md](../../docs/RUN_START.md#a-runs-own-processor-maximum-processorscalingcapacityoverride)), then
     reset the counters;
  6. execute, with the record of the maximum on the source `Endpoint` (`processorScalingCapacity`), which each runner
     puts on its events (the queue seeder's too), for the run's Flags.
  Also shared helpers for endpoint validation, population normalization and source-simulator prediction logging.
- **`RunnerForSinglePerson.ts`**: needs `SINGLE_PERSON_BUID` and the queue URL; uses the person endpoint with
  `?buid=...`; sends one API chunker event.
- **`RunnerForSingleMessage.ts`**: the default, gradual ramp-up mode; sends one API chunker event, after resetting the
  run counters when stack, region and landscape are known (`resetRunCounters()` in `src/chunking/RunCounters.ts`, which
  the chunker subscriber also calls; [docs/RUN_START.md](../../docs/RUN_START.md#the-run-counters)).
- **`RunnerForQueueSeeding.ts`**: the high-parallel start. Checks the seeding and scaling variables and the
  desired-count guardrails, and that the atomic counter table exists, then runs
  `ParallelStart.start({ messages: MESSAGES_TO_PREPOPULATE, tasks: DESIRED_COUNT })` (`src/chunking/ParallelStart.ts`, the
  chunker subscriber's own procedure): the service's maximum checked before anything is sent, then seeding
  (`QueueSeeder`, counters reset first), then, if `DESIRED_COUNT > 0`, `MetricsCatchupDelay` and the desired count
  (`DesiredCount`). `DESIRED_COUNT` 0 seeds only.
- **`MetricsCatchupDelay.ts`** (harness `METRICS_CATCHUP_DELAY_`): waits out CloudWatch alarm lag after seeding, so a
  stale scale-in alarm can't immediately undo the desired count. Inputs: the alarm's context (`ECS_CLUSTER_NAME`,
  `ECS_SERVICE_NAME`, `REGION`) and timing (`ALARM_PERIOD_SECONDS`, `ADDITIONAL_DELAY_SECONDS`, `COUNTDOWN_STEP_SECONDS`).
  The period is the discovered alarm's, else the variable, else 60 s. It waits `period + additional delay`, or no
  longer than `maxWaitSeconds` when given (the chunker subscriber passes its Lambda's remaining time, less a margin),
  polling the alarm and ending early once it leaves `ALARM`; it warns if the alarm is still in `ALARM` at the end.
  Alarm discovery is `ServiceScaleInAlarm` in `src/Alarm.ts` (the scale-in alarm found from the service's autoscaling
  policies, by its negative step adjustment; `getAlarmName()`, `getAlarmState()`, `getAlarmPeriodSeconds()`).
- **`ServiceToggler.ts`** (harness `SERVICE_TOGGLER_`): turns the chunker or processor subscriber off or on by setting
  its Lambda's `DRY_RUN` (`ChunkerToggler`, `ProcessorToggler`).
- **`LambdaFunctionEnvironmentVariable.ts`**: reads and updates a Lambda's environment, batched
  (`setEnvironmentVariables(entries)`; `setEnvironmentVariable` goes through it) to avoid `ResourceConflictException`
  from rapid `UpdateFunctionConfiguration` calls. Used only by `ServiceToggler`.
- **`RunnerTypes.ts`**: `RunnerEnv`, `Endpoint`, the normalized population type; `extractEnvironment()` parses and coerces
  every variable; `optionalNumber()`; `setTestEnvironment()`.

### Decorators (`decorators/`)

- **`MessagingOnlyRunnerDecorator`**: chunker subscriber off (`DRY_RUN=true`), to test messages without chunk files.
- **`ChunkingOnlyRunnerDecorator`**: chunker subscriber on, processor subscriber off, to make chunk files without
  processing them.
- **`RestoreToFullOperationRunnerDecorator`**: both subscribers on, when no restrictive mode is chosen.
- **`SourceSimulatorRunnerDecorator`** ([docs/SOURCE_SIMULATOR.md](../../docs/SOURCE_SIMULATOR.md)):
  - checks the simulator's URL exists;
  - sends the run's simulator settings (`SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`, `_SIMULATED_DELAY_SECONDS`,
    `_ERROR_RATE`, `_NON_CURRENT_TERM_RATE`; `simulatorSettings()` in `AbstractRunner`) as query parameters of the
    source's `fetchPath` (`simulatorFetchPath()`), each when set, 0 and fractions included (`optionalNumber()`); an unset
    one is the simulator's own. **It never reads or changes the simulator Lambda's configuration.**
  - predicts the run's chunks (`getSourceSimulatorPredictions()`) from `/health` asked with the run's settings, else
    `SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`, else the context's `LAMBDA.sourceSimulator.mockTotalPopulation`;
  - uses the simulator's `/` (people), or `/person?buid=<SINGLE_PERSON_BUID>` in single-person mode, which returns the
    same payload `/` produces for that person. Single-person mode skips the predictions and the counter reset (the
    endpoint is stateless), fails prerequisites for a BUID the simulator doesn't generate (`U` + digits), and warns for
    one beyond its population;
  - returns the simulator's `/terms` as the source endpoint's `termsEndpoint`, which reaches the processors through the
    run's Flags. **It does not change `config.dataSource.terms`**: this runner's config never reaches the tasks (see
    "two lifecycles of `Config`" in the project CLAUDE.md). Hybrid runs don't use this decorator, so they keep the real
    terms endpoint;
  - resets the simulator's counter, and warns about seeding past the simulated population.
- **`MockLandscapeRunnerDecorator`** ([docs/MOCK_LANDSCAPES.md](../../docs/MOCK_LANDSCAPES.md)): applied whenever
  `LANDSCAPE` is a mock landscape (`isMockLandscape()`); there is no `USE_MOCK_TARGET` switch.
  - Verifies the mock stack is deployed (its mock target table exists).
  - `MOCK_TARGET_RESET_STATE=true` resets the landscape's run state through `src/chunking/MockLandscapeReset.ts`
    (`truncateTables()`, `detectStorageMode()`, `deleteS3Baseline()` delegate to it): the mock target and statistics
    tables, then the baseline of the stack's storage mode (the person tables in DynamoDB mode, or everything under
    `delta-storage/` in `${S3.chunksBucket}-${landscape}` in S3 mode; the mode is detected from whether the person
    current-state table exists). A custom shared delta storage dir (an S3 `keyPrefix` in the integration config) is not
    covered.
  - Warns about hybrid runs, especially `person-delta` ones: the real source depletes as it is read.
  - **Must NOT override `start()`** (see the note in the class): the inherited template method has to run against the
    decorator, or its `validatePrerequisites()` is silently bypassed.

## Environment (`RUNNER_` prefix in `.env`)

`CHUNKER_QUEUE_URL`, `REGION`, `STACK_ID`, `LANDSCAPE`; `SECRET_ARN` or `HURON_PERSON_CONFIG_PATH`; `POPULATION_TYPE`
(`person-full` | `person-delta`), `BULK_RESET`, `TRUST_PREVIOUS_STORAGE`, `DATASOURCE_ENDPOINTCONFIG_ITERATION_LIMIT`;
`SINGLE_PERSON_BUID`; `MESSAGES_TO_PREPOPULATE`, `DESIRED_COUNT`, `ECS_CLUSTER_NAME`, `ECS_SERVICE_NAME`;
`PROCESSOR_SCALING_CAPACITY_OVERRIDE`; `SKIP_ATOMIC_COUNTER_RESET`; `MESSAGING_ONLY`, `CHUNKING_ONLY`; `SOURCE_SIMULATOR`
and its `SOURCE_SIMULATOR_MOCK_*` settings; `MOCK_TARGET_RESET_STATE`; `PERSON_RECORD_PROCESSOR_ADDONS`. The authority
is `extractEnvironment()` in `RunnerTypes.ts`.

## Invariants

1. **Modes**: `MESSAGING_ONLY` and `CHUNKING_ONLY` can't both be true; `MESSAGING_ONLY` is incompatible with
   `SOURCE_SIMULATOR`; with `SOURCE_SIMULATOR`, `Runner.ts` doesn't apply `RestoreToFullOperationRunnerDecorator`, so
   the subscribers keep whatever `DRY_RUN` they had.
2. **Seeding order**: seed first, then the alarm catch-up delay, then the desired count.
3. **Counters**: the chunker counter is reset before seeding, and seeding needs the atomic counter table.
4. **Config precedence**: environment -> local file -> JSON env string -> Secrets Manager.
5. **`DRY_RUN` stays switched**: a decorator's change to a subscriber's `DRY_RUN` persists after the run until restored.

## Known limitations

1. `MetricsCatchupDelay` only observes the alarm's state; it can't guarantee what the alarm does after the delay.
2. If the alarm can't be found, the delay falls back to static/default timing, which may be too long or too short for
   the alarm's real configuration.
3. The safeguards are pre-flight checks and logging; there is no transactional lock between seeding and autoscaling
   policy evaluations.
4. Only the chunker's scale-in alarm counts in-flight messages (`visible + notVisible`); the processor's and
   post-processor's alarms count visible messages only, which matters when reading a run's scaling.

## Debugging order

1. Mode selection in `Runner.ts`.
2. Decorator side effects on the subscribers' `DRY_RUN`.
3. Seeding preconditions and the atomic counter in `RunnerForQueueSeeding.ts`.
4. `MetricsCatchupDelay`'s period and alarm-state logs.
5. `ServiceScaleInAlarm`'s discovery and period in `src/Alarm.ts`.
6. Lambda environment batching in `LambdaFunctionEnvironmentVariable.ts`.

Tests: `test/RunnerForQueueSeeding.test.ts`, `RunnerProcessorScalingCapacity`, `MetricsCatchupDelay`,
`SourceSimulatorRunnerDecorator`, `MockLandscape`, `MockLandscapeReset`, `ParallelStart`.

Why the seeding waits for the alarm (the 2026-07-02 incident):
[docs/RUN_START.md](../../docs/RUN_START.md#history-the-2026-07-02-scale-in-incident).
