# The Statistics Table

The statistics table (`StatisticsTable`, `src/dynamodb/`; one per landscape) holds each run's metadata, flags, errors,
per-chunk statistics, claim records and summary, in both storage modes' sense of a "run" (in S3 storage mode it is
still used for the claims, the RUN marker and the summary; metadata and flags are S3 files there). Read this before
changing what the pipeline writes per run, the table's indexes, or `src/reporting/RunReporting.ts`, which the
dashboard's Run history reads through.

## Keys and event types

- PK: `integrationTimestamp` (syncRunId), SK: `eventType`.
- Event types: `METADATA`, `FLAGS`, `TERMINAL_ERROR`, `STATISTICS-chunk-*`, `ERROR:*`, `CHUNK_STATUS_*`,
  `PROCESSOR_BOOST_CLAIM`, `POST_PROCESSOR_TRIGGER_CLAIM`, `SINGLE_REQUEST_FETCH_CLAIM`, `RUN`, `RUN_SUMMARY` (no
  aggregate `STATISTICS` item is ever written: processors always pass a chunkId).
- `PROCESSOR_BOOST_CLAIM` / `POST_PROCESSOR_TRIGGER_CLAIM` / `SINGLE_REQUEST_FETCH_CLAIM` are `OCCFlag`-guarded claim
  records ensuring exactly one chunker task runs the processor scaling check, exactly one processor task triggers the
  post-processor, and exactly one chunker task fetches a single-person run's person
  ([CHUNKING_END_AND_FAILURE.md](CHUNKING_END_AND_FAILURE.md#a-single-person-run-is-a-full-sync-of-one); see
  [CONCURRENCY_CONTROL.md](CONCURRENCY_CONTROL.md)); the boost and single-request fetch claims are written in S3
  storage mode too.
- In DynamoDB mode, METADATA and FLAGS replace S3 mode's `_metadata.json` and `_flags.json` files.

## The run list (`run-list-index`)

The `RUN` marker item (one per run, attributes `runList` = `RUNS` and `distinctRunId` = PK) is the only item in the
sparse, KEYS_ONLY `run-list-index` (partition key `runList`, sort key `distinctRunId`), which holds every run in one
partition in time order: `getRunIdsPage()` reads the newest `limit` runs (or those older than `before`) with one Query
(`ScanIndexForward: false`, `Limit: limit + 1`), and `getAllRunIds()` lists them all. It replaced the scan-only
`distinct-run-index` (partition key `distinctRunId`), removed 2026-11-10.

- **When a table can't serve `run-list-index`** (`isMissingIndexError()`: it doesn't exist, or it is still
  backfilling, which it was in preview for some minutes after CloudFormation reported the stack update complete),
  `RunReporting` fails with `ReportingError` `RUN_LIST_UNAVAILABLE` (a 503 from the dashboard) saying what to do.
- **The marker is written idempotently** by `writeFlags()`, `addToMetadataTotals()` (every chunker task, both storage
  modes) and `writeTerminalError()`.
- **`backfill-runs`**: runs whose markers lack `runList` (written before it existed, or before the markers did) need
  the harness task `backfill-runs` (one scan of the table), run after the stack update that adds the index and before
  the dashboard that reads it is deployed. A table created with the index needs no `backfill-runs`.

## Changing the table's GSIs

CloudFormation refuses an update that adds or removes more than one GSI, so change the statistics table's GSIs one
per deploy.

Where the landscapes stand: as of 2026-11-12, preview and mock have `run-list-index`, and so does staging, whose
pipeline stack was recreated that day. Migration's has it too, since its pipeline stack was recreated the same way on
2026-11-19.

A July stack couldn't be updated in place: it had a Merger service, and its ECS log groups have since moved to new
logical IDs under the same names, so CloudFormation would try to create them before deleting the old ones and fail
with "already exists". Recreating one: `teardown-fargate`, then `deploy-fargate` (which creates the empty ECR
repository, with the services at 0 tasks), then `docker-push`, then `deploy-dashboard`.

## Chunk statistics (`STATISTICS-chunk-*`)

`STATISTICS-chunk-*` items carry the chunk's `IntegrationResult` counts (success/failure/skipped,
added/updated/removed) as well as its error counts.

- Added/updated/removed are the successful pushes counted by operation (`pushedChangeCounts()` in
  `src/processing/ProcessorCommon.ts`), so they sum to the successes; `IntegrationResult`'s own are the delta's sizes
  before the push, which also count the failed pushes. Items written before 2026-11-09 hold the delta's counts, so a
  run with failures shows too many added/updated.
- Their `totalRecords` (and `chunkSize`) is `IntegrationResult.totalProcessed`, the people in the chunk's delta
  (those whose record changed), not the people in the chunk: people skipped as unchanged are never counted. So it
  can't measure a run's progress; the dashboard counts chunks instead (`chunksReported` ÷ METADATA `chunkCount`; see
  "Percent done" in [the dashboard's pages doc](../dashboard/docs/PAGES.md)).

## The run summary (`RUN_SUMMARY`)

`RUN_SUMMARY` (one per run) is written by `AbstractPostProcessor.main()`'s `finally` block, in both storage modes:
METADATA plus the summed chunk statistics plus the deletion outcome (`DeletionOutcome`, returned by
`runDeferredDeletes()`: COMPLETED with counts, SKIPPED with a reason, or FAILED). Status: `SUCCEEDED`, `FAILED`
(deletes skipped because the run carries a terminal error) or `POST_PROCESSING_FAILED`. Writing it never fails
post-processing.

Runs that never reach the post-processor have no summary; `src/reporting/RunReporting.ts` reports them as
FAILED/INCOMPLETE from METADATA, FLAGS and the terminal error (S3 files in S3 mode, located via METADATA's
`chunkDirectory`), with their `progress`: the STATISTICS-chunk items written so far, summed by `sumChunkStatistics()`,
the same function `buildRunSummary()` sums them with (so the figures shown while a run is underway and its summary
can't disagree), plus the latest chunk's end time.

## Reading runs (`RunReporting`)

`src/reporting/RunReporting.ts` is the read side (no transport): `listRuns`, `listRunPage`, `getRunReport`,
`getRunChunkStatistics`, paged `getRunErrors` and `getRunChanges` (person-history changes, DynamoDB mode only; S3 mode
keeps no per-person change record per run). Page cursors are base64url `LastEvaluatedKey`s, rejected unless their key
attributes match the requested run (and change type).
