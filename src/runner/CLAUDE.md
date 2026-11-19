# runner/: Manual Chunking Orchestration Memory

## Purpose
The `src/runner` directory contains the manual/off-schedule orchestration layer for starting chunking work, choosing execution mode, preparing environment/configuration, and optionally toggling downstream subscriber behavior for safe test scenarios.

This is not the normal scheduled execution path. It is an operator-facing/harness-facing control layer used for manual runs and controlled experiments.

## Session Knowledge Snapshot (2026-07)

### Why this file exists
This directory has many moving parts (factory + decorator + AWS control utilities + harnesses). Reconstructing intent from scratch in each session is expensive. This file preserves current understanding, including incident-driven decisions made around queue seeding and autoscaling timing.

### Incident context incorporated
Source: `INCIDENT_ANALYSIS_2026_07_02.md` (repository root).

Confirmed issue (2026-07-02): chunker scaled from desired count 4 to 0 before runner seeding took effect, because the scale-in alarm could already be in `ALARM` from a previously empty queue. CloudWatch alarm/metric evaluation lag (periodic evaluation) created a timing gap between seeding and autoscaling state catch-up.

Resulting runner behavior change:
- Queue seeding flow now invokes `MetricsCatchupDelay` before explicit desired-count scale-up.
- `MetricsCatchupDelay` waits for alarm period + buffer, logs countdown, and can end early when alarm exits `ALARM`.
- Alarm discovery/inspection is delegated to `ServiceScaleInAlarm` in `src/Alarm.ts`.

Design intent: avoid immediate desired-count reversal due to stale scale-in alarm state.

## Directory Map and Responsibilities

### Entry and control
- `Runner.ts`
  - Manual entrypoint for chunking service start.
  - Chooses runner mode via environment:
    - Single person (`SINGLE_PERSON_BUID`)
    - Queue seeding (`MESSAGES_TO_PREPOPULATE > 0`)
    - Single message (default)
  - Applies decorators for operational mode:
    - `MESSAGING_ONLY`
    - `CHUNKING_ONLY`
    - `SOURCE_SIMULATOR`
    - otherwise restore full operation

- `AbstractRunner.ts` (`ChunkingServiceRunner`)
  - Template Method base class:
    1. validate prerequisites (after refusing a `PROCESSOR_SCALING_CAPACITY_OVERRIDE` outside 1-20)
    2. load configuration (ConfigManager chain)
    3. resolve data source
    4. validate endpoint
    5. at a run's start (unless `SKIP_ATOMIC_COUNTER_RESET`): set the processor's maximum
       (`setProcessorScalingCapacity()`: the override, else the local context's `ECS.processorService.maxScalingCapacity`,
       as the chunker subscriber does at every run start; see "A run's own processor maximum" in
       [docs/RUN_START.md](../../docs/RUN_START.md#a-runs-own-processor-maximum-processorscalingcapacityoverride)), then reset the counters
    6. execute, with the record of the maximum on the source `Endpoint` (`processorScalingCapacity`), which each runner
       puts on its events (the queue seeder's too), for the run's Flags
  - Shared helpers for endpoint validation, population normalization, source-simulator prediction logging.

### Runner implementations
- `RunnerForSinglePerson.ts`
  - Requires `SINGLE_PERSON_BUID` and queue URL.
  - Uses person endpoint and appends `?buid=...`.
  - Sends one API chunker event.

- `RunnerForSingleMessage.ts`
  - Default gradual ramp-up mode.
  - Sends one API chunker event to queue.
  - Resets chunker atomic counter before send when stack/region/landscape are available (through the shared
    `resetRunCounters()` in `src/chunking/RunCounters.ts`, which the chunker subscriber Lambda also calls for a
    run's first event; see "The run counters" in [docs/RUN_START.md](../../docs/RUN_START.md#the-run-counters)).

- `RunnerForQueueSeeding.ts`
  - High-parallel-start mode.
  - Seeds queue with N messages using `QueueSeeder`.
  - Validates required env for seeding/scaling and checks desired-count guardrails.
  - Requires atomic counter table existence.
  - Seeds and scales through `ParallelStart.start({ messages: MESSAGES_TO_PREPOPULATE, tasks: DESIRED_COUNT })`
    (`src/chunking/ParallelStart.ts`), the same procedure as the chunker subscriber's parallel start for Bulk sync:
    the service's maximum checked before anything is sent, then seeding (`QueueSeeder`, counters reset first), then,
    if `DESIRED_COUNT > 0`, `MetricsCatchupDelay` and the desired count (`DesiredCount`). `DESIRED_COUNT` 0 seeds only.

### Timing and alarm protection
- `MetricsCatchupDelay.ts`
  - Purpose: absorb CloudWatch metric/alarm timing lag after queue seeding.
  - Inputs:
    - alarm lookup context (`ECS_CLUSTER_NAME`, `ECS_SERVICE_NAME`, `REGION`)
    - temporal tuning (`ALARM_PERIOD_SECONDS`, `ADDITIONAL_DELAY_SECONDS`, `COUNTDOWN_STEP_SECONDS`)
  - Behavior:
    - Resolve scale-in alarm and period when possible.
    - Fallback order for period: discovered alarm period -> env override -> default 60s.
    - Wait for `period + additional delay`, or no longer than `maxWaitSeconds` when given (the chunker subscriber
      passes its Lambda's remaining time, less a margin, so its timeout can't stop it mid-wait).
    - During wait, poll alarm state; exit early if state is no longer `ALARM`.
    - Warn if delay expires and state is still `ALARM`.

- External dependency: `src/Alarm.ts` (`ServiceScaleInAlarm`)
  - Discovers scale-in alarm by inspecting ECS autoscaling policies for negative step adjustment.
  - Supports `getAlarmName()`, `getAlarmState()`, and `getAlarmPeriodSeconds()`.

### Service toggling and lambda env control
- `ServiceToggler.ts`
  - Enables/disables chunker or processor subscriber behavior by setting Lambda env var `DRY_RUN`.
  - Wraps service-specific togglers (`ChunkerToggler`, `ProcessorToggler`).
  - Includes test harness (`SERVICE_TOGGLER_*` env).

- `LambdaFunctionEnvironmentVariable.ts`
  - Reads and updates Lambda environment variables.
  - Supports batch updates with `setEnvironmentVariables(entries)` to avoid back-to-back update conflicts (`ResourceConflictException`) from multiple rapid `UpdateFunctionConfiguration` calls.
  - Used only by `ServiceToggler` (`DRY_RUN`). The source simulator's settings are never written to its Lambda: they travel in the run's `fetchPath`.
  - `setEnvironmentVariable` now routes through the batched API.

### Decorators
- `decorators/MessagingOnlyRunnerDecorator.ts`
  - Disables chunker subscriber (`DRY_RUN=true`) so messages can be tested without chunk file creation.

- `decorators/ChunkingOnlyRunnerDecorator.ts`
  - Ensures chunker subscriber enabled.
  - Disables processor subscriber (`DRY_RUN=true`) so chunk files can be created without downstream processing.

- `decorators/RestoreToFullOperationRunnerDecorator.ts`
  - Ensures chunker and processor subscribers are enabled when no restrictive mode is selected.

- `decorators/SourceSimulatorRunnerDecorator.ts`
  - Validates source simulator URL exists.
  - Sends the run's simulator settings (`SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`, `_SIMULATED_DELAY_SECONDS`, `_ERROR_RATE`, `_NON_CURRENT_TERM_RATE`; `simulatorSettings()` in `AbstractRunner`) as query parameters of the source's `fetchPath`, built by `simulatorFetchPath()` in `src/chunking/fetch/SourceSimulator.ts`, as the dashboard's Bulk sync builds it. Each is sent when set, 0 included (`optionalNumber()` in `RunnerTypes.ts`; fractions too); an unset one is the simulator's own. It never reads or changes the simulator Lambda's configuration (until 2026-11-16 it rewrote its `MOCK_*` environment, which stayed for later runs).
  - Predicts the run's chunks (`getSourceSimulatorPredictions()`) from the simulator's `/health` asked with the run's settings, which reports the population as the run gets it; else the run's `SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`, else the context's `LAMBDA.sourceSimulator.mockTotalPopulation`.
  - Uses simulator endpoint instead of configured upstream endpoint: `/` (people), or `/person?buid=<SINGLE_PERSON_BUID>` in single person mode, which returns the same payload `/` produces for that person (so no population-of-1 workaround is needed). Single person mode skips the predictions and the counter reset (the endpoint is stateless), fails prerequisites for a BUID the simulator does not generate (`U` + digits), and warns for one beyond its population.
  - Returns the simulator's `/terms` as the source endpoint's `termsEndpoint`, which travels via the chunker message and the run's Flags to the processor tasks (`Flags.termsEndpoint`, applied by `applyTermsEndpoint()`), so simulated runs get simulated terms. It does NOT change `config.dataSource.terms`: this runner's config never reaches the tasks (see "two lifecycles of `Config`" in the project CLAUDE.md, and the `preLoadedMaps` story in [docs/MOCK_LANDSCAPES.md](../../docs/MOCK_LANDSCAPES.md#history-the-configpreloadedmaps-dead-code)). Hybrid runs don't use this decorator, so they keep the real terms endpoint.
  - Resets simulator atomic counter.
  - Warns about potential overseeding relative to simulated population.

- `decorators/MockLandscapeRunnerDecorator.ts`
  - Applied automatically whenever `LANDSCAPE` is a mock landscape (`^mock\d*$`, see `isMockLandscape` in `src/Utils.ts`). There is no `USE_MOCK_TARGET` switch: a mock landscape's integration config always points at its target simulator, a real landscape's never does.
  - Verifies the mock stack is deployed (its mock target table - the target simulator's storage - exists).
  - `MOCK_TARGET_RESET_STATE=true`: resets that landscape's run state so the delta baseline and mock target start in agreement, through `src/chunking/MockLandscapeReset.ts`, which the chunker subscriber's run start shares for an event with `resetState` (the dashboard's Bulk sync); the decorator's `truncateTables()`, `detectStorageMode()` and `deleteS3Baseline()` delegate to it. It truncates the mock target and statistics tables (via `BulkPurger`), then clears the baseline of the stack's storage mode: the person current-state and history tables in DynamoDB mode, or every object under `delta-storage/` in the landscape's chunks bucket (`${S3.chunksBucket}-${landscape}`, base name from the local context.json) in S3 mode. The mode is detected from the deployed stack: the person current-state table exists only in DynamoDB mode. A custom shared delta storage dir (an S3 `keyPrefix` in the integration config) is not covered.
  - Warns about hybrid runs (real source -> mock target), especially `person-delta` ones, since the real source API depletes as it is read.
  - Must NOT override `start()` (see the note in the class): the inherited template method has to run against the decorator, or its `validatePrerequisites()` is silently bypassed.
  - `Runner.ts` aborts outright if `SOURCE_SIMULATOR=true` targets a non-mock landscape.

### Environment extraction and harness support
- `RunnerTypes.ts`
  - Defines `RunnerEnv`, `Endpoint`, and normalized population type.
  - `extractEnvironment()` parses all runner env vars and boolean/number coercions.
  - `setTestEnvironment()` loads `RUNNER_*` prefixed harness variables through `TestEnvironment`.

## Operational Expectations and Invariants

1. Runner mode selection is mutually constrained:
- `MESSAGING_ONLY` and `CHUNKING_ONLY` cannot both be true.
- `MESSAGING_ONLY` is incompatible with `SOURCE_SIMULATOR`.
- Source simulator implies chunking-only behavior in entrypoint logic.

2. Queue seeding + explicit scale-up is intentionally guarded:
- seeding first
- then alarm/metrics catch-up delay
- then desired-count set

3. Atomic counters are part of seeding correctness:
- chunker counter reset before seeded operation
- table existence must be confirmed for seeding mode

4. Config loading precedence (important for ops):
- environment -> local file -> JSON env string -> secrets manager

## Known Limitations / Risks (Current)

1. `MetricsCatchupDelay` can only observe alarm state; it does not guarantee future alarm transitions after delay completion.
2. If alarm lookup fails, delay falls back to static/default timing, which may be conservative or insufficient depending on alarm config.
3. Runner safeguards are mostly pre-flight and logging based; no hard transactional lock exists between seeding and autoscaling policy evaluations.

## Related Infrastructure Context (for interpretation)

From current synthesized behavior in this session:
- Chunker:
  - lower (scale-in) alarm uses composite metric `visible + notVisible`
  - upper (scale-out) alarm uses visible-only metric
- Processor and Post-processor:
  - lower and upper alarms remain visible-only by default

This split matters when reading runner outcomes, because only chunker scale-in is currently protected against in-flight-only visibility artifacts.

## How to run these harnesses (quick)

- Main runner:
  - `npx ts-node src/runner/Runner.ts`
- Metrics catch-up delay harness:
  - `npx ts-node src/runner/MetricsCatchupDelay.ts`
- Service toggler harness:
  - `npx ts-node src/runner/ServiceToggler.ts`

Use `.env` / `TestEnvironment` prefixes per module (`RUNNER_`, `METRICS_CATCHUP_DELAY_`, `SERVICE_TOGGLER_`).

## Future Session Starting Points

If debugging runner behavior, check in this order:
1. Mode selection in `Runner.ts`
2. Decorator side effects on subscriber `DRY_RUN`
3. Queue seeding preconditions + atomic counter in `RunnerForQueueSeeding.ts`
4. `MetricsCatchupDelay` period and alarm-state logs
5. `ServiceScaleInAlarm` discovery and period resolution in `src/Alarm.ts`
6. Lambda env batching behavior in `LambdaFunctionEnvironmentVariable.ts`
