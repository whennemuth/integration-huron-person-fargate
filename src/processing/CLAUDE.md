# src/processing/: Phase 2, Processing Chunks

Each processor task takes one chunk file from its queue and runs integration-huron-person's `HuronPersonIntegration`
over it as a "mini full sync": the chunk's people are mapped, compared with their stored hashes, and the changes pushed
to the target (Huron, or a mock landscape's target simulator). `docker/processor.ts` routes to the storage mode's
processor. The phases as a whole: the project [CLAUDE.md](../../CLAUDE.md).

## Files

- **`ProcessorForS3.ts`** (harness `DOCKER_PROCESSOR`): S3 mode; writes a mini-delta per chunk to S3, coordinated by
  marker files, which the post-processor consolidates.
- **`ProcessorForDynamoDb.ts`** (harness `DOCKER_PROCESSOR_DYNAMODB`): DynamoDB mode; writes straight to the person
  current-state and history tables, so no mini-deltas; the last processor of a run triggers the post-processor (once,
  by the `POST_PROCESSOR_TRIGGER_CLAIM` `OCCFlag`; never for a run with a terminal error).
- **`ProcessorCommon.ts`**: shared by both: `resolveNextChunk()`, `resolveCommonFlags()` (the run's Flags: previousStateSource,
  addons, ...), `applyTermsEndpoint()` (a simulated run's terms), `buildErrorTracker()`
  (`src/ApiErrorTracking.ts`), `resolveAddonPersonProcessor()`, `pushedChangeCounts()` (added / updated / removed
  counted from the successful pushes), `logIntegrationResult()`, `writeTrackerStatistics()` (the chunk's
  `STATISTICS-chunk-*` item), `computeExitCode()`, `isEcsTask()`.
- **`ProcessorSubscriber.ts`**: the processor subscriber Lambda; forwards chunk files' S3 event notifications to the
  processor queue, the message body unchanged from direct S3-to-SQS delivery. With `DRY_RUN=true` it forwards nothing.
- **`TargetSimulator.ts`**: a mock landscape's stand-in for the Huron person API
  ([docs/TARGET_SIMULATOR.md](../../docs/TARGET_SIMULATOR.md)).
- **`addon/`**: per-person addons run during processing
  ([addon/README.md](addon/README.md)).

## Rules and pitfalls

- **Per-run settings come from the run's Flags**, read from the chunk's run (`readFlagsFromChunkKey()`), never from the
  task definition: previousStateSource, addons, the terms endpoint. (Flags without previousStateSource fall back to
  `PREVIOUS_STATE_SOURCE` in a local run's environment, and are otherwise refused, as are Flags carrying the
  `bulkReset` / `trustPreviousStorage` pair it replaced: no task definition sets it.) Values in the task definition
  (`STATIC_MAP_USAGE`, from the config's `preLoadedMaps`, table names, `PREVIOUS_STORAGE_TYPE`) are fixed at deploy.
- **The processor has no mock awareness**: in a mock landscape the config secret points it at the target simulator.
- **Chunk statistics count only changed people**: a chunk's `totalRecords` is its delta's size, and added / updated /
  removed count successful pushes ([docs/STATISTICS_TABLE.md](../../docs/STATISTICS_TABLE.md)).
- **Soft-deleted people** in the baseline carry a `DELETED:` hash; a processor's write for a returning person clears
  it ([docs/STORAGE_MODES.md](../../docs/STORAGE_MODES.md#soft-deleted-persons-in-the-baseline)).
- How many processors run is the processor service's maximum, which every run start sets
  ([docs/RUN_START.md](../../docs/RUN_START.md)); Huron throttles them above a point (429s, tracked by
  `ApiErrorTracking`).
