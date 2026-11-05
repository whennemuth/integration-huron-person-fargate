# TODO: Make S3 storage mode database-free, and retire the atomic counter

A three-phase plan:

1. **Phase 1 - divorce S3 mode from the DynamoDB statistics table**: no S3-mode decision consults
   it, and S3-mode statistics are written to S3, next to each run's `_metadata.json`, instead.
2. **Phase 2 - toward a pure file-based S3 mode (no database)**: remove the remaining DynamoDB
   dependency (the atomic counter table), and build all coordination on storage primitives that
   both S3 and a mounted filesystem provide, so the pipeline could later run outside Fargate (e.g.
   a Docker Swarm with a shared volume standing in for S3).
3. **Phase 3 - one chunk coordination scheme for both modes**: apply phase 2's coordination-free
   names and set-based completion to DynamoDB mode too, and remove the atomic counter from the
   codebase altogether, so there is one scheme instead of two.

Phase 1 is the bulk of this document; phases 2 and 3 follow it. A related, independent fix (the
offset sent to the source) is recorded at the end.

## Background

### What S3 mode is supposed to be

Before DynamoDB mode existed, S3 mode kept everything that governs a running sync in S3 files under
the run's chunk directory (`chunks/<population>/<syncRunId>/`): `_flags.json`, `_metadata.json`,
`_terminal_error.json`, and the delta baseline (`delta-storage/previous-input.ndjson`). The
DynamoDB statistics table was added to collect **statistics only** - error events, per-chunk
statistics, run totals, run markers, run summaries: things recorded about a run, never consulted to
decide how a run proceeds. By that definition the statistics table could be removed without
affecting S3-mode operation.

### How S3 mode drifted

| Date | Commit | Change | Effect on S3 mode |
|---|---|---|---|
| 2026-09-18 | `9070d60` | Processor boost claim added (S3 mode: an S3 marker file, checked non-atomically) | Decision in S3 |
| 2026-09-23 | `3833dd6` | Boost claim moved to the statistics table (OCCFlag) in both modes, to make it race-free | **Drift 1**: a decision now depends on the statistics table |
| 2026-09-24 | `d40ac76` | Run totals and the end-of-records flag accumulated in the statistics table (`addToMetadataTotals()`), replacing the S3 rescan | **Drift 2**: writes moved to the table, but S3-mode readers still read `_metadata.json` |
| 2026-09-24 | `52fdf28` | Chunk chaining gated on the end-of-records flag | Inherits drift 2 |

**Drift 2 is fixed** (2026-10-24): `MetadataForS3.accumulateMetadataTotals()` keeps the run totals
and the end-of-records flag in `_metadata.json`, using S3 conditional writes (`IfMatch` on the ETag
read, `IfNoneMatch: '*'` to create) with retry, and only copies them to the statistics table as
best-effort statistics when a table is configured. Before that fix, S3-mode runs with
`iterationLimit > 0` could never see the end of records: chunker tasks chained forever (seen in the
mock landscape's single-person run), and the total-records safety cutoff and the post-processor's
expected chunk count read nothing.

**Drift 1 remains**, along with S3 mode's write-only use of the statistics table.

### Where S3 mode still touches the statistics table

| Use | Code | Kind |
|---|---|---|
| Processor boost claim / release | `MetadataForS3.claimProcessorBoost()` / `releaseProcessorBoostClaim()`, via `ProcessorServiceBooster` | **Decision** (drift 1) |
| Run totals copy + `RUN` marker | `MetadataForS3.copyTotalsToStatistics()` -> `StatisticsTable.addToMetadataTotals()` / `registerRun()` | Statistics (best effort) |
| Target API error events | `TrackingTargetApiErrorProcessor.writeErrorEvent()` (`src/ApiErrorTracking.ts`), built by `ProcessorCommon.buildErrorTracker()` (`ProcessorForS3`) and by the deferred delete handlers | Statistics |
| Per-chunk statistics | `TrackingTargetApiErrorProcessor.writeStatistics()` via `ProcessorCommon.writeTrackerStatistics()` (`ProcessorForS3`) | Statistics |
| Run summary | `AbstractPostProcessor.writeRunSummary()` -> `StatisticsTable.writeRunSummary()` | Statistics |
| Run reporting (read side) | `src/reporting/RunReporting.ts` (`listRuns`, `getRunReport`, chunk statistics, errors) | Reads statistics for reports, not decisions |
| Infrastructure | `DYNAMODB_STATISTICS_TABLE_NAME` + IAM grants in the chunker, processor and post-processor task definitions and `PostProcessorSubscribingLambda`; table created unconditionally in `lib/DynamoDB.ts` | |

Note: the chunker task definition passes `DYNAMODB_STATISTICS_TABLE_NAME` in both modes (fixed
2026-10-24) only because the boost claim still needs it; that goes away with drift 1.

### What phase 1 does NOT remove

Fixing drift 1 and moving statistics to S3 frees S3 mode from the **statistics table**, not from
DynamoDB entirely. These remain after phase 1:

- **Atomic counter table** - chunk ordinal allocation and next-offset allocation for parallel
  chunker tasks (`ChunkerQueue`, `QueueSeeder`, `docker/chunker.ts`, runners). A decision table,
  kept in phase 1 and removed in phase 2.
- **Mock target person table** - backs the target simulator; mock landscapes only.
- **Person-record-processor log table** - written by processor addons, only when one is
  enabled (`PERSON_RECORD_PROCESSOR_LOG_TABLE_NAME`).

## Requirements

1. **No S3-mode decision reads the statistics table.** Every decision consults S3 files under the
   run's chunk directory (or the atomic counter table).
2. **S3 mode runs correctly with no statistics table**: `DYNAMODB_STATISTICS_TABLE_NAME` unset, no
   IAM access, table not deployed.
3. **S3-mode statistics are written to S3** under the run's chunk directory, consolidated into
   `statistics.ndjson` next to `_metadata.json`.
4. **Concurrency-safe**: parallel chunker and processor tasks never lose each other's writes, and
   there is no single object that many tasks write repeatedly.
5. **Statistics never affect the sync**: writing them is best effort and never fails a task.
6. **Reporting keeps working** for S3-mode runs, reading the S3 files.
7. **DynamoDB mode is unchanged.**

## Proposed changes

### 1. Processor boost claim as an S3 claim object (drift 1)

Files: `src/chunking/metadata/MetadataForS3.ts`, `src/chunking/metadata/MetadataUtils.ts` (claim key
helper - `getProcessorBoostClaimKey()` existed before `3833dd6`).

- **Claim**: create `<chunkDirectory>/_processor_boost_claim.json` (`{ claimedAt, claimedByChunk }`)
  with `PutObject` + `IfNoneMatch: '*'`. Success = won; `412 PreconditionFailed` = someone else
  holds it. Atomic, unlike the pre-`3833dd6` existence check that motivated the move to DynamoDB.
- **Stale claim takeover** (same 10-minute threshold as `StatisticsTable.claimProcessorBoost()`):
  read the object and its ETag; if `claimedAt` is stale, overwrite it with `IfMatch: <etag>`. Only
  one task can win the takeover; the losers get `412`.
- **Release**: `DeleteObject` (a deleted-then-recreated claim is acceptable; same semantics as the
  DynamoDB release).
- `ProcessorServiceBooster` is unchanged (it goes through `MetadataBroker`).
- Then drop `statisticsTableName` from `MetadataForS3`'s decision paths entirely.

### 2. S3 statistics files

Appending to a single S3 object isn't atomic, and many processor tasks report errors and chunk
statistics concurrently, so `statistics.ndjson` is **consolidated once**, not appended to by every
task:

- **Per-writer part files** under `<chunkDirectory>/statistics/`, each written once by one task (no
  contention, no conditional writes needed):
  - `chunk-NNNN.json` - a processor task's chunk statistics (the `STATISTICS-chunk-*` record).
  - `errors-chunk-NNNN.ndjson` - that task's target API error events, buffered in memory and
    written with the chunk statistics (instead of one write per event, as `writeErrorEvent()` does
    against DynamoDB today).
  - `errors-post-processor.ndjson` - deferred delete errors.
- **Consolidation**: the post-processor, a single writer, merges the part files plus its run
  summary into `<chunkDirectory>/statistics.ndjson` (one record per line, with the same record
  types as the DynamoDB `eventType`s: `STATISTICS-chunk-*`, `ERROR:*`, `RUN_SUMMARY`), in
  `AbstractPostProcessor.main()`'s `finally`, where `RUN_SUMMARY` is written today. It may delete
  the part files afterwards.
- **No totals or `RUN` marker copies in S3**: `_metadata.json` already holds the totals, and a run
  is identified by its chunk directory. `MetadataForS3.copyTotalsToStatistics()` goes away.
- A run that never reaches the post-processor keeps its part files; readers fall back to them.

### 3. A statistics sink chosen by storage mode

Introduce a small interface (e.g. `StatisticsSink`: `writeErrorEvent`, `writeChunkStatistics`,
`writeRunSummary`) with:
- a DynamoDB implementation wrapping `StatisticsTable` (DynamoDB mode, unchanged behavior), and
- an S3 implementation writing the part files above.

Use it in `TrackingTargetApiErrorProcessor` (`src/ApiErrorTracking.ts`),
`ProcessorCommon.buildErrorTracker()` / `writeTrackerStatistics()`,
`AbstractDeferredDeleteHandler` / `DeferredDeleteHandler`, and
`AbstractPostProcessor.writeRunSummary()`. Every S3-sink write is wrapped so a failure is logged and
ignored (requirement 5). `computeExitCode()` must keep working from the tracker's in-memory error
counts.

### 4. Reporting for S3 runs

`src/reporting/RunReporting.ts` in S3 mode:
- `listRuns`: list run chunk directories (`chunks/<population>/` prefixes) instead of the `RUN`
  markers / `distinct-run-index`.
- `getRunReport`, chunk statistics, errors: read `statistics.ndjson`, or the part files if it
  doesn't exist, plus `_metadata.json`, `_flags.json` and `_terminal_error.json` (which it already
  reads from S3 in S3 mode).
- DynamoDB mode unchanged.

### 5. Infrastructure

- Task definitions (`lib/services/chunker/ChunkerTaskDefinition.ts`, `processor/`,
  `post-processor/`) and `PostProcessorSubscribingLambda`: in S3 mode, stop passing
  `DYNAMODB_STATISTICS_TABLE_NAME` and drop the statistics-table IAM grants (this also reverts the
  2026-10-24 chunker change for S3 mode).
- `lib/DynamoDB.ts`: create the statistics table only in DynamoDB mode - or keep creating it during
  a transition period, decided when this lands (note: removal policy is DESTROY).
- `MockLandscapeRunnerDecorator` / `BulkPurger` reset in S3 mode: statistics now live in run chunk
  directories, so the statistics-table truncation is unnecessary there.

### 6. Related cleanup

- The terminal-error path (`ChunkFromAPI` -> `MetadataForS3.write({ replace: true })`) overwrites
  `_metadata.json` unconditionally, dropping the run totals accumulated so far. Make it merge into
  the existing file with the same conditional write as `accumulateMetadataTotals()`.
- Update this project's `CLAUDE.md` ("Storage Modes", StatisticsTable notes) and
  `docs/MOCK_INTEGRATION_TESTING.md` (Part 2) to match.

## Concurrency summary

| Object | Writers | Mechanism |
|---|---|---|
| `_metadata.json` | every chunker task, once each | conditional read-modify-write (`IfMatch` / `IfNoneMatch`) + retry - done |
| `_processor_boost_claim.json` | chunker tasks competing for the claim | `IfNoneMatch: '*'` create; `IfMatch` stale takeover |
| `statistics/chunk-NNNN.json`, `errors-chunk-NNNN.ndjson` | one processor task each | plain write, no contention |
| `statistics.ndjson` | the post-processor only | plain write, no contention |

## Verification (phase 1)

- Unit tests: claim / lost claim / stale takeover / release (mocked S3 conditional writes); sink
  part-file writes and consolidation; `RunReporting` over S3 files; statistics failures never fail
  a task.
- Mock landscape, S3 mode, with `DYNAMODB_STATISTICS_TABLE_NAME` unset and the IAM grant removed:
  run `docs/MOCK_INTEGRATION_TESTING.md` Part 2 plus a batched full-population run
  (`iterationLimit > 0`, several chunker tasks). Expect chaining to stop, the processor boost to
  happen once, the post-processor to run, and `statistics.ndjson` to appear next to
  `_metadata.json` - with no statistics-table access attempted (an `AccessDenied` would surface it).
- DynamoDB mode: run Part 1 of the runbook unchanged.

## Suggested order (phase 1)

1. Boost claim to S3 (proposal 1) - removes the last S3-mode decision on the statistics table.
2. Statistics sink + S3 part files + consolidation (proposals 2, 3).
3. Reporting over S3 files (proposal 4).
4. Infrastructure and docs (proposals 5, 6).

---

# Phase 2: Toward a pure file-based S3 mode (no database)

## Goal

S3 mode keeps all of its state in files: no database of any kind. And it stays close to a general
file-based mode, so that moving the chunker, processor and post-processor out of Fargate - for
example into a Docker Swarm with a shared volume mount standing in for the S3 bucket - is a small
step rather than a redesign.

## Why not an S3-based atomic counter

The atomic counter could be rebuilt on S3 conditional writes (read the counter object and its
ETag, write the increment with `IfMatch`, retry on `412`), the same technique phase 1's
`_metadata.json` totals use. But that file is updated once per chunker task, while the counter is
incremented for **every chunk written** and **every chained message**, by up to
`maxScalingCapacity` parallel tasks. Each increment would cost a read plus a conditional write,
plus a backoff-and-retry per lost race - constant contention and latency during chunking. DynamoDB's
`UpdateItem` `ADD` has none of that. So rather than replace the counter, remove the need for it.

## What the counter is for today

1. **Next offsets for chained messages** (`ChunkerQueue.getNextOffset()`, `QueueSeeder`). Since
   `e5c5d54` ("Align API chunking with depletion semantics"), the source is treated as a depleting
   queue: each request claims the next available slice, and offset "remains API-compatible input
   but should not determine returned records". Unique offsets don't decide which data is fetched;
   they are effectively labels.
2. **Dense chunk ordinals** (`ChunkerQueue.createChunkOrdinalAllocator()`, one increment per chunk)
   giving `chunk-NNNN.ndjson` names numbered 0..N-1 across all chunker tasks. S3-mode completion
   detection depends on them: `PostProcessorSubscriber` requires processed-chunk markers to form a
   contiguous sequence from 0 whose count equals `_metadata.json`'s `chunkCount`. Contiguity also
   detects in-flight work: an ordinal handed out but not yet processed shows up as a gap.

## Proposed design: coordination-free names and set-based completion

1. **Chunk names unique by construction**: `chunk-<taskKey>-<seq>.ndjson`, where `taskKey` is
   unique per chunker task (its SQS message ID, its container/task ID, or a UUID) and `seq` counts
   up locally within the task. No two tasks can produce the same name, so no counter is needed.
2. **Offsets**: depend on the source's paging model - see "Source paging model" below. Either way
   no counter is needed: a depleting source needs no positions, and a limit/offset source gets
   positions from **lane striping** (each seeded message gets a lane index `i` and a stride `N`,
   the number of lanes; lane `i`'s chained messages cover page blocks `i`, `i+N`, `i+2N`, ...),
   unique without coordination.
3. **Task registration**, in files each written once by one task (no contention):
   - `_tasks/<taskKey>.started` - created before the task writes any chunk.
   - `_tasks/<taskKey>.done` - created at the end, listing the chunks the task wrote.
4. **Set-based completion** replaces the contiguity check. A run is complete when:
   - the end-of-records flag is set in `_metadata.json`,
   - every `_tasks/*.started` has a matching `.done`, and
   - every chunk listed in a `.done` file has a processed marker.

   An unfinished chunker task shows up as an unmatched `.started` file, replacing the gap check.
   `_metadata.json`'s `chunkCount` becomes informational rather than the completion criterion.
   Under limit/offset paging, completion also needs a coverage check - see below.

Affected: `ChunkerQueue`, `QueueSeeder`, `BigJsonFetch` / `ChunkFromAPI` (chunk naming,
registration), `docker/chunker.ts`, `PostProcessorSubscriber` (completion), the processor's chunk
key parsing and marker naming, `ChunkFromAPI.findMissingChunkOrdinals()`, the runners' counter
resets (`AbstractRunner.resetAtomicCounters()`), and the atomic counter table and its IAM grants in
`lib/` (S3 mode only). DynamoDB mode keeps its current ordinals until phase 3.

## Source paging model

The source currently behaves as a **depleting queue**: each request claims the next available slice
of records, and the offset sent is ignored. It may change to standard **limit/offset paging**,
where page `p` is requested as `?limit=<limit>&offset=<p × limit>` (with `limit` fixed for the whole
run). The two models need different rules in four places, so make the model an explicit setting -
e.g. `dataSource.people.pagination: 'depleting' | 'limitOffset'` (default `'depleting'`):

| Concern | Depleting (today) | Limit/offset |
|---|---|---|
| Offset sent | a label, or nothing | `page × limit` (see "Related fix" below) |
| Work assignment | any task fetches "the next slice" | lane striping over page blocks of `iterationLimit` pages: lane `i` owns blocks `i`, `i+N`, `i+2N`, ... |
| End of records | global flag: once any task sees a partial/empty batch, nothing is left anywhere (`partialOrEmptyChunkEncountered`) | a **position**: the lowest page that came back partial/empty (`endPage`, kept in `_metadata.json` as a minimum across tasks, via the same conditional write as the totals) |
| Chain stop / late-task cancel | stop/cancel everything once the flag is set | stop/cancel only blocks that start **after** `endPage` - lanes finish out of order, so a block *before* the end may still be unfetched when another lane reaches the end |
| Chunk names | unique per task (`chunk-<taskKey>-<seq>`): a re-fetch returns different records | unique per page block (`chunk-b<block>-<seq>`): a re-fetch of the same block (e.g. an SQS redelivery) overwrites its own chunks instead of duplicating them |
| Completion | started/done pairing + every listed chunk processed | the same, **plus coverage**: every block `0..endBlock` appears in some `.done` file - catching a lane whose task died before claiming its next block |

Notes:
- The global-flag rule is correct only for a depleting source. Applied to limit/offset paging it
  silently loses pages. The position-aware rule is essentially the `finalOffsetProcessed` boundary
  retired in `52fdf28`, whose reason - no single authoritative end position - holds only for a
  depleting source.
- This applies to the counter-based scheme too, not just to lane striping: if the source moves to
  limit/offset paging before this plan lands, the current chain-stop/cancel rule must become
  position-aware.

## Two storage primitives, for S3 and a filesystem

Build every coordination point on two operations that both S3 and a POSIX filesystem provide,
added to the existing `IStorageAdapter` (`src/storage/`, which already has `S3StorageAdapter` and
`FileSystemStorageAdapter`):

| Primitive | S3 | Mounted filesystem | Used for |
|---|---|---|---|
| **Create if absent** | `PutObject` with `IfNoneMatch: '*'` | `open(O_CREAT \| O_EXCL)` | Claims (processor boost), task registration, part files, markers |
| **Replace if unchanged** | `PutObject` with `IfMatch: <etag>` | lock file created with `O_EXCL` (with a stale-lock timeout) around write-to-temp then `rename` | `_metadata.json` totals only (low traffic) |

Plus plain read, write, list and delete. Avoid `flock` and other primitives that are unreliable on
network filesystems: `O_EXCL` create (NFSv3+) and `rename` within a directory are the dependable
operations. `MetadataForS3`'s conditional write (phase 1 / drift 2) and the boost claim (phase 1,
proposal 1) should move behind these primitives too, so the metadata code itself becomes
storage-agnostic.

## Remaining dependencies after phase 2

- **Mock-only**: the mock target person table and the source simulator's own depletion counter
  (`simulator-offset-counter`) - simulator infrastructure, present only in mock landscapes.
- **Person-record-processor log table**: could become per-chunk files under the run directory, like
  the phase 1 statistics part files.
- **Not storage, but in the way of leaving Fargate**: SQS (chunker and processor queues), ECS
  service scaling and the processor booster (`ProcessorServiceBooster`), Secrets Manager (config),
  the S3-event Lambda subscribers that start processing and post-processing, and the CDK stack
  itself. A local cluster would need equivalents (e.g. a message broker or file-based queue, a
  scaler, file or environment configuration, filesystem watchers). Phase 2 makes storage portable;
  these need their own plan.

## Verification (phase 2)

- Under each paging model (`depleting`, `limitOffset`): chunking with several lanes, including a
  lane that reaches the end before another lane has fetched an earlier block (no page may be lost
  under `limitOffset`), and an SQS redelivery of a block (no duplicate chunks under `limitOffset`).
- Unit tests for task-key chunk naming, registration files and set-based completion - including a
  task that has started but not finished (must block completion) and duplicate or late tasks.
- Both `IStorageAdapter` implementations exercised by the same primitive tests (create-if-absent
  contention, replace-if-unchanged retry, stale-lock recovery on the filesystem).
- Mock landscape, S3 mode, with no atomic counter table: a batched full-population run with several
  parallel chunker tasks, plus `docs/MOCK_INTEGRATION_TESTING.md` Part 2 - expect complete,
  non-duplicated chunks and a single post-processor run.

---

# Phase 3: One chunk coordination scheme for both modes

## Goal

Phase 2 removes the atomic counter from S3 mode only, leaving DynamoDB mode on counter-assigned
offsets and dense chunk ordinals: two schemes for the same job. Nothing in the phase 2 design is
specific to S3 mode - chunk files and their processed markers live in S3 in **both** modes - so
apply it to DynamoDB mode too and remove the atomic counter from the codebase.

## Every current use of the atomic counter table

| Counter | Used by | Purpose | After phase 3 |
|---|---|---|---|
| `chunker-offset-counter` | `ChunkerQueue.getNextOffset()`, `QueueSeeder` | next offset for each chained / seeded message | none needed: depleting source -> label; limit/offset -> lane striping (the seeder assigns lanes `0..N-1`) |
| `chunker-chunk-ordinal-counter` | `ChunkerQueue.createChunkOrdinalAllocator()`, `QueueSeeder` | dense chunk ordinals `0..N-1` | none needed: task- or block-unique chunk names |
| `simulator-offset-counter` | `SourceSimulator` (`getSimulatorCounter()`) | the source simulator's own depletion state - shared across concurrent simulator invocations | still needed, but mock-only - see below |

Plus the code that manages the counters: `AbstractRunner.resetAtomicCounters()` and
`SKIP_ATOMIC_COUNTER_RESET`, `RunnerForQueueSeeding.atomicCounterExists()`,
`ChunkerQueue.hasAtomicCounterSupport()` and the "Chunk ordinal mode" branch in
`docker/chunker.ts`, `src/dynamodb/AtomicCounter.ts`, and the table plus its IAM grants (chunker
task, source simulator Lambda) and `DYNAMODB_ATOMIC_COUNTER_TABLE_NAME` in `lib/`.

## DynamoDB mode today, and what changes

- **Completion** is decided in `ProcessorForDynamoDb` after each chunk: the run's `METADATA` has
  `partialOrEmptyChunkEncountered` set, and the count of `COMPLETED` `CHUNK_STATUS_*` items is at
  least `chunkCount`; the processor that wins `POST_PROCESSOR_TRIGGER_CLAIM` then triggers the
  post-processor.
- **A gap to verify**: `chunkCount` only includes chunker tasks that have *finished*. If one task
  has seen the end and finished while another is still writing chunks, the completed count can
  reach `chunkCount` before the second task's chunks exist - triggering the post-processor early,
  where deletion detection could treat that task's persons as removed. The comment in
  `ProcessorForDynamoDb` acknowledges `chunkCount` can still be growing, and the end-of-records
  flag narrows the window, but nothing appears to account for a task that is still running. The
  started/done registration closes it: an unmatched `.started` file blocks completion.
- **Changes**:
  - Chunker: the same chunk naming, registration and paging-model rules as S3 mode (shared code).
    Registration files live in S3 next to the chunks, in both modes - they describe chunks, not
    statistics - so there is one implementation.
  - `ProcessorForDynamoDb`: replace "completed count >= `chunkCount`" with the set-based check
    (every `.started` has a `.done`; every chunk listed in a `.done` has a `COMPLETED`
    `CHUNK_STATUS`; plus coverage under limit/offset paging). Keep the end-of-records condition, the
    terminal-error gate and `POST_PROCESSOR_TRIGGER_CLAIM` unchanged.
  - `CHUNK_STATUS_*` keys and `STATISTICS-chunk-*` records: keyed by the new chunk names instead of
    ordinals (and `RunReporting`'s chunk listing with them).
  - `PersonCurrentState` / `PersonHistory`: unaffected - they're keyed by person and sync run, not
    by chunk.

## The simulator's counter

The source simulator needs genuinely shared, frequently updated state (every simulated batch
request claims the next slice), so it is the one legitimate counter left - and it exists only in
mock landscapes. Options:
- **Recommended**: keep a counter table that CDK creates **only in mock landscapes** (or store the
  counter as an item in the existing mock target person table), so non-mock stacks have no counter
  table at all.
- Alternatively, an S3 object with conditional writes - simpler infrastructure, but contended under
  parallel chunking (see "Why not an S3-based atomic counter").

## Order

1. Land phase 2 in S3 mode (it proves the scheme).
2. Switch DynamoDB mode's chunker and completion check to the shared scheme; first verify (or rule
   out) the completion gap above with a test.
3. Move the simulator's counter to mock-only storage.
4. Remove the atomic counter: `AtomicCounter.ts`, the counter names and their resets, the
   `hasAtomicCounterSupport()` / ordinal-mode branches, the table, its grants and environment
   variable.

## Verification (phase 3)

- DynamoDB mode: a test that holds one chunker task "started" while others finish and their chunks
  complete - the post-processor must not be triggered until it is done.
- Mock landscape, both modes, with no non-mock counter table: `docs/MOCK_INTEGRATION_TESTING.md`
  Parts 1 and 2 plus a batched full-population run with several parallel chunker tasks.

---

# Related fix: the offset sent to the source

Recorded here; not done yet.

**Problem**: `BuCdmPeopleDataSourceBatch` (integration-huron-person,
`src/data-source/PeopleDataSourceBatch.ts`) sends `recordCount=<batchSize>` with
`offset=<page number>` - `0, 1, 2, ...`, incremented by one per batch - as if the source multiplied
the page number by the page size itself. The limit/offset convention passes the record offset:
`page × limit` - `0, 200, 400, ...` for a limit of 200. It is harmless today only because both the
real source and the source simulator ignore the offset (the depleting model); under limit/offset
paging, page 1 would ask for records 1-200 instead of 200-399, re-fetching almost everything.

**Fix**: keep counting pages internally (the chunker's offsets, `iterationLimit` and the counter /
lane arithmetic all work in pages already) and send `offset = page × batchSize`. Only the query
parameter changes; log messages that print "Batch <n>" can keep the page number. Best done together
with the paging-model setting ("Source paging model" in phase 2), so the value sent under the
`depleting` model can stay a plain label if preferred.
