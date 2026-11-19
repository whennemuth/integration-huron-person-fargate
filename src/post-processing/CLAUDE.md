# src/post-processing/: Phase 3, Consolidation and Deletions

Runs once per run, after every chunk is processed: consolidates the run's results into the baseline (S3 mode),
deactivates the people who have left the source (a full sync's deletions), and writes the run's summary.
`docker/post-processor.ts` routes to the storage mode's post-processor. **Both storage modes need the post-processor**,
for the deletions. The phases as a whole: the project [CLAUDE.md](../../CLAUDE.md).

## Files

- **`AbstractPostProcessor.ts`**: the template method `main()`: read the task parameters (from the queue message or the
  environment), the subclass's `merge()`, then the deferred deletes (`runDeferredDeletes()`, returning a
  `DeletionOutcome`), timers, task protection and the exit code; its `finally` writes the run's `RUN_SUMMARY`
  (`writeRunSummary()`, reading the run's Flags in either mode).
- **`PostProcessorForS3.ts`**: consolidates the delta chunk files under `deltas/{population}/{timestamp}/`, merges them
  with the baseline (`delta-storage/previous-input.ndjson`) and writes the result back there (`MergeEngine.ts`).
- **`PostProcessorForDynamoDB.ts`**: nothing to merge (the processors wrote the person tables directly); deletions only.
- **`AbstractDeferredDeleteHandler.ts`**, **`DeferredDeleteHandler.ts`** (harness `DEFERRED_DELETE`),
  **`DeferredDeleteHandlerForS3.ts`** (harness `DEFERRED_DELETE`), **`DeferredDeleteHandlerForDynamoDB.ts`**: find the
  people in the baseline but not in this run's source population, and soft-delete them in the target (`PATCH` with
  `active: false`). Deletions wait for the post-processor because, during chunk processing, a person missing from a
  chunk is usually just in another chunk.
- **`ChunkPopulationReader.ts`** (harness `CHUNK_POPULATION_READER`): DynamoDB mode's "who was in the source this run",
  read from the run's raw chunk files, because the person current-state table only records people whose hash changed.
- **`DeletedHashMarker.ts`**: the `DELETED:` hash prefix written for soft-deleted people (`toDeletedHash()`,
  `isDeletedHash()`), so they aren't selected for deletion again and are reactivated if they return
  ([docs/STORAGE_MODES.md](../../docs/STORAGE_MODES.md#soft-deleted-persons-in-the-baseline)).
- **`PostProcessorSubscriber.ts`**: the post-processor subscriber Lambda (S3 mode): on each delta chunk file, checks
  whether every expected delta chunk exists and, if so, queues the post-processor; blocked for a run with a terminal
  error; `DRY_RUN` respected.

## Rules and pitfalls

- **Failed runs never reach deletions**: a run with a terminal error skips deletion handling in both modes (and in S3
  mode never triggers the post-processor) ([docs/CHUNKING_END_AND_FAILURE.md](../../docs/CHUNKING_END_AND_FAILURE.md)).
- **Writing the summary never fails post-processing**; a run that never reaches the post-processor has no summary, and
  `RunReporting` reports it from its metadata and chunk statistics
  ([docs/STATISTICS_TABLE.md](../../docs/STATISTICS_TABLE.md#the-run-summary-run_summary)).
- **Deletions happen only for a full sync** (`person-full`); the "no deactivations" run type is `person-delta`.
- **A source glitch that looks like a normal end** would deactivate everyone after it until the next good run
  reactivates them; there is no deactivation ceiling ([docs/RUN_START.md](../../docs/RUN_START.md#the-scheduled-run)).
