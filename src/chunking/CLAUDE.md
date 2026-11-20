# src/chunking/: Phase 1, Chunking and Starting a Run

Turns the source's people into NDJSON chunk files (`chunks/<population>/<syncRunId>/chunk-NNNN.ndjson` in the
landscape's chunks bucket), which the processors pick up, and starts runs. How a run starts and ends is in the project
[CLAUDE.md](../../CLAUDE.md), with the detail in [docs/RUN_START.md](../../docs/RUN_START.md) and
[docs/CHUNKING_END_AND_FAILURE.md](../../docs/CHUNKING_END_AND_FAILURE.md); how chained chunker tasks avoid races in
[docs/CHUNKING_ARCHITECTURE.md](../../docs/CHUNKING_ARCHITECTURE.md).

## Files

**The chunker subscriber Lambda** (`ChunkerSubscriber.ts`, the dispatcher): takes an S3 event (a file dropped in the
input bucket: `filedrop/ChunkerS3Subscriber.ts`), an API event (the EventBridge schedule, the dashboard's Bulk sync, or
the Runner in-process: `fetch/ChunkerApiSubscriber.ts`, `handleApiEvent()`), or a mock landscape's reset-only event,
and queues the chunker's work. For a run's first event it applies the refusals (`runStartRefusal()`), skips a
scheduled start while a run is underway (`scheduledStartRefusal()`, `src/schedule/ScheduledStartGuard.ts`), then
the processor maximum, a mock reset, the counter reset and a parallel start, in that order.

**Run start** (shared by the subscriber and the Runner):
- `ScheduledRun` (in `src/schedule/`, [schedule/CLAUDE.md](../schedule/CLAUDE.md)): the schedule's input, and
  `previousStateFlags(bulkReset)`.
- `StartedBy.ts`: `StartedBy` / `RunStarter`, who started a run.
- `RunCounters.ts`: `resetRunCounters()`, `runCounterNames()`, `isRunStartEvent()`.
- `ParallelStart.ts`: seed N messages, wait for the scale-in alarm, set the desired count.
- `ProcessorScalingCapacity.ts`, `ProcessorCapacityRecord.ts`: a run's processor maximum, and its record in Flags.
- `MockLandscapeReset.ts`: a mock landscape's reset and its record.

**Chunking**:
- `ChunkerQueue.ts` (harness `CHUNKER_QUEUE`): the chunker task's single queue message, and the chained message to the
  next task (`sendNextChunkingMessage()`, which re-forwards the run's fields); the offset and chunk-ordinal counter
  names.
- `fetch/ChunkFromAPI.ts` (harness `CHUNK_FROM_API`): a chunker task's work against the source API (`TaskParameters`
  from the queue message, `runChunking()`), and `findMissingChunkOrdinals()`.
- `fetch/BigJsonFetch.ts` (harness `BIG_JSON_FETCH`): pages the source and streams the response into chunks; ordinals
  from `ChunkOrdinalAllocator`; end-of-records detection and the `maxTotalRecords` cutoff.
- `fetch/ChunkConfigOverride.ts`: a queue message's `baseUrl` / `fetchPath` override the configured endpoint.
- `fetch/ProcessorServiceBooster.ts` (harness `PROCESSOR_SERVICE_BOOSTER`): while chunking, raises the processor
  service's desired count straight to its maximum rather than waiting for alarm-driven scaling (once per run, by an
  `OCCFlag` claim).
- `fetch/QueueSeeder.ts` (harness `QUEUE_SEEDER`): seeds the chunker queue with messages a task's chunks apart.
- `fetch/SourceSimulator.ts` (harness `SOURCE_SIMULATOR`): the source simulator Lambda, `simulatorFetchPath()`,
  `simulationSettings()` ([docs/SOURCE_SIMULATOR.md](../../docs/SOURCE_SIMULATOR.md)).
- `filedrop/ChunkFromS3.ts` (harness `CHUNK_FROM_S3`), `filedrop/BigJsonFile.ts` (harness `BIG_JSON_FILE`): the same for
  a JSON file dropped in the input bucket; `filedrop/ChunkPathUtils.ts`: the chunk directory derived from an input key
  (shared with the post-processor).
- `PersonArrayWrapper.ts` (harness `PERSON_ARRAY_WRAPPER`): finds where the array of people sits in a JSON document
  (`detectPersonArrayPath()`: `0.response`, or top level).

**Run metadata** (`metadata/`, [metadata/README.md](metadata/README.md)): `IMetadataStorage.ts` (`Flags`,
`ChunkMetadata`), `MetadataForS3` / `MetadataForDynamoDb`, `MetadataFactory` (by `config.storage.type`) and
`MetadataFactoryForBootstrap` (by environment, before config is loaded), `MetadataBroker` (one chunk directory's reads,
writes and status checks, including the run's running `totalRecords`), `ChunkFileManager` (chunk files, always in S3).

## Rules and pitfalls

- **A per-run value travels the messages**: `ApiChunkerEvent` -> queue message -> `TaskParameters` ->
  `sendNextChunkingMessage()` -> Flags (`docker/chunker.ts`). A new per-run field must be added at every step, or
  chained tasks lose it. The Runner's `Config` never reaches the tasks.
- **Never reset the run counters in `handleApiEvent()`**: queue seeding calls it once per seeded message.
- **Subscriber refusals are returned** (a 400 result), not thrown, and the subscriber Lambda has no automatic retry.
- **`partialOrEmptyChunkEncountered`** is persisted metadata: never rename it, though it means "end of records".
- **Counts come from what was read and written**, never from `recordCount`.
- **A terminal error stops the run**: chained tasks check `isTerminalErrorEncountered()`; read the terminal error only
  through `StatisticsTable.readTerminalError()` in DynamoDB mode.
