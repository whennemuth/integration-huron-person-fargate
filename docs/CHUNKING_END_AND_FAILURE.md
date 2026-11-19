# How Chunking Ends, and Failed Runs

How the chunker decides it has reached the end of the source's records, the safety cutoff that stops it when the
source never says so, and how a failed run is kept from reaching deactivations. Read this before changing
`BigJsonFetch`, `ChunkFromAPI.runChunking()`, the chunker's chaining (`ChunkerQueue`), or anything that reads a run's
terminal error.

## End-of-records detection (`stopAtFirstPartial`)

The source API can return a "partial" batch (0 < length < `recordCount`) mid-population (a source-side bug), so by
default only an **empty** batch marks the end. `IContext` `ECS.chunkerTaskDefinition.stopAtFirstPartial` (optional,
default `false`) -> `ChunkerTaskDefinition` env `STOP_AT_FIRST_PARTIAL` -> `ChunkFromAPI.runChunking()` ->
`BigJsonFetchConfig.stopAtFirstPartial` -> `BuCdmPeopleDataSourceBatch` (integration-huron-person). `true` restores the
legacy stop-at-first-partial behavior. The fargate `README.md` ("Source End-of-Records Detection") describes the
setting for operators.

Consequences:

- `ChunkResult.endOfRecordsDetected` (renamed from `partialChunkEncountered`) / metadata
  `partialOrEmptyChunkEncountered` (persisted, so NOT renamed) mean "end of source records detected" (empty batch,
  or partial only if `stopAtFirstPartial`). All consumers (chain-stop, `isAlreadyFinished()`,
  post-processor-trigger gate, `ProcessorServiceBooster` final check) need exactly that signal, so they are unchanged.
- Counts are never inferred from `recordCount`: `totalRecords` = sum of actual response lengths, `chunkCount` = chunk
  files actually written.
- A task whose `iterationLimit` is met on a partial batch does NOT signal the end; the next chained task then gets an
  empty first batch, which `BigJsonFetch` treats as a normal end (warn, not error).
- With `iterationLimit=0` (e.g. `maxScalingCapacity=1`), the default mode relies on the source eventually returning
  an empty batch (true of the depleting real API and SourceSimulator). The safety cutoff below bounds the damage if
  it never does.

## Glitched-source safety cutoff (`maxTotalRecords`)

If the source never returns an empty batch, chunking would never end - an endless fetch loop when `iterationLimit=0`,
or endless chunker message/task chaining when `iterationLimit>0`.

`IContext` `ECS.chunkerTaskDefinition.maxTotalRecords` (optional, default `DEFAULT_MAX_TOTAL_RECORDS` = 500,000 in
`docker/chunkTypes.ts`; the real population is ~145,000; `<= 0` disables) -> env `MAX_TOTAL_RECORDS` ->
`ChunkFromAPI.runChunking()` (also reads the run's accumulated `totalRecords` via
`MetadataBroker.getRunningTotalRecords()`) -> `BigJsonFetchConfig.maxTotalRecords` / `runTotalRecordsAtStart` ->
`BigJsonFetch.enforceMaxTotalRecords()`, checked before the first fetch (fail fast) and on every batch before it is
written.

Exceeding it throws, surfacing through the existing terminal-error path (`runFailed` metadata + `markRunFailed`), so
later chained tasks bail on `isTerminalErrorEncountered()` and the S3-mode post-processor is blocked.

Precision caveat: `totalRecords` only includes tasks that have finished, so the cutoff can be overshot by up to the
in-flight records of concurrently running tasks.

## Failed runs never reach deactivations

A run is "failed" when its TERMINAL_ERROR record exists (DynamoDB: PK=syncRunId, SK=`TERMINAL_ERROR` in the
statistics table; S3: `_terminal_error.json`). Gates:

- `PostProcessorSubscriber` (S3 mode, blocks the post-processor),
- `ProcessorForDynamoDb`'s last-processor check (does not trigger the post-processor),
- `PostProcessorForS3`/`PostProcessorForDynamoDB.runDeferredDeletes()` (skip deletion handling, both modes).

The terminal error is read and written with the dedicated `StatisticsTable.writeTerminalError()` /
`readTerminalError()`.

### History: the terminal error that was never seen in DynamoDB mode

Until this was fixed, `MetadataForDynamoDb.readTerminalError()` read the METADATA record (and
`StatisticsTable.readMetadata()` strips `eventType`), so `terminalErrorExists()` was always `false` in DynamoDB mode -
chunker fast-fail on a failed run silently never happened there. Unit tests mocked `readMetadata` to return
`eventType: 'TERMINAL_ERROR'`, hiding it. It now uses the dedicated `StatisticsTable.writeTerminalError()` /
`readTerminalError()`.
