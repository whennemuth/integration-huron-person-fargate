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
    1. validate prerequisites
    2. load configuration (ConfigManager chain)
    3. resolve data source
    4. validate endpoint
    5. execute
  - Shared helpers for endpoint validation, population normalization, source-simulator prediction logging.

### Runner implementations
- `RunnerForSinglePerson.ts`
  - Requires `SINGLE_PERSON_BUID` and queue URL.
  - Uses person endpoint and appends `?buid=...`.
  - Sends one API chunker event.

- `RunnerForSingleMessage.ts`
  - Default gradual ramp-up mode.
  - Sends one API chunker event to queue.
  - Resets chunker atomic counter before send when stack/region/landscape are available.

- `RunnerForQueueSeeding.ts`
  - High-parallel-start mode.
  - Seeds queue with N messages using `QueueSeeder`.
  - Validates required env for seeding/scaling and checks desired-count guardrails.
  - Requires atomic counter table existence.
  - If `DESIRED_COUNT > 0`, performs explicit ECS desired count update via `DesiredCount` after running `MetricsCatchupDelay`.

### Timing and alarm protection
- `MetricsCatchupDelay.ts`
  - Purpose: absorb CloudWatch metric/alarm timing lag after queue seeding.
  - Inputs:
    - alarm lookup context (`ECS_CLUSTER_NAME`, `ECS_SERVICE_NAME`, `REGION`)
    - temporal tuning (`ALARM_PERIOD_SECONDS`, `ADDITIONAL_DELAY_SECONDS`, `COUNTDOWN_STEP_SECONDS`)
  - Behavior:
    - Resolve scale-in alarm and period when possible.
    - Fallback order for period: discovered alarm period -> env override -> default 60s.
    - Wait for `period + additional delay`.
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
  - Important session change: supports batch updates with `setEnvironmentVariables(entries)` to avoid back-to-back update conflicts (`ResourceConflictException`) from multiple rapid `UpdateFunctionConfiguration` calls.
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
  - Applies simulator Lambda env overrides in one batched update.
  - Uses simulator endpoint instead of configured upstream endpoint.
  - Resets simulator atomic counter.
  - Warns about potential overseeding relative to simulated population.

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
- Processor and Merger:
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
