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

## Chaining, and the end-of-run checks

A chunker task with a non-zero `iterationLimit` sends the next chunking message (the next offset) **as soon as it
starts**, before the person cache and before fetching, so the next task can start while this one works
(`ChunkFromAPI.sendNextChunkingMessage()` -> `ChunkerQueue.sendNextChunkingMessage()`). No next message is sent when
`iterationLimit` is `0` (one task fetches everything), or when some task has already reached the end of the records
(`partialOrEmptyChunkEncountered`). All of this assumes the real people source **depletes**: it hands out each record
once per run, whatever the offset, so two tasks never fetch the same records.

`docker/chunker.ts` checks whether the run has already reached its end (`MetadataBroker.isAlreadyFinished()`,
through `cancelIfAlreadyFinished()`) twice: at start-up, and again just before `runChunking()`, after the person cache
and the Flags write (`⊘ Cancelling before fetching ...`). The second check narrows the window a slow start-up leaves
(a cache build takes minutes) but can't close it: a task can pass it a moment before another marks the end.

### A single-person run is a full sync of one

A single-person run (a dashboard Test, the Runner's `SINGLE_PERSON_BUID`) exists to exercise the whole pipeline as a
full sync does, only quicker, so it must not take paths of its own. Its source doesn't deplete, though: the person
endpoint (person-mode config, or a people fetch URL with a `buid` query parameter: `isSingleRequestSource()`,
`src/chunking/fetch/SingleRequestSource.ts`) ignores paging and returns the same person on every request. So
`BigJsonFetch` fetches it through `DepletingSingleRequestDataSource`, which makes it behave like a depleting population
of one:

- the run's first fetch, by whichever chunker task gets there first, wins the run's `SINGLE_REQUEST_FETCH_CLAIM`
  (`MetadataBroker.claimSingleRequestFetch()`, a first-writer-wins `OCCFlag` record in the statistics table, never
  released) and returns the person;
- every later fetch in the run, by any task, returns an empty batch (`Single-request source already fetched in this
  run: returning an empty batch`), which ends that task's fetch loop and marks the end of the records;
- no `recordCount`/`offset` parameters are sent to the person endpoint.

Everything else (chaining, the fetch loop, end detection, the end checks) runs as in a full sync. Until 2026-11-23
such a source ran with `iterationLimit=-1` ("Batching disabled ... only one request will be made"), one request
and stop, and nothing stopped a chained task from fetching the same person again.

Why, from preview's bulk-reset Test of one person (run `2026-11-12T17:04:00.376Z`): the Test sent `iterationLimit: 10`;
task A chained task B, then spent 3 minutes building the person cache; B found the cache lock held, skipped the cache,
fetched the same person and wrote chunk-0000, marking the end; A, whose only end check had been at start-up, fetched
the person again and wrote chunk-0001. Both chunks' processors updated the person. Now B's fetch wins the claim and
chunks the person, its next fetch is empty, and A cancels at its second end check (or, had it passed it, would find
the source depleted). The dashboard's Test has sent `iterationLimit: 0` since 2026-11-12.

### The person cache and the processors

With bulk reset (or previous storage not trusted) the first chunker task to take the cache lock builds the run's
person cache (`_personCache.txt`, `PersonCacheForS3.ensureCache()`); a task that finds the lock held skips the build
and goes on to write chunks, so a chunk can exist before its cache. Processors read the cache through
`PersonCacheLookup`, which waits while the lock holds (up to the lock's 10-minute TTL) and, when it can't have the
cache (no cache and no lock, the lock outlasting the wait, an S3 error), proceeds with an empty cache: every person
in the chunk is then looked up in Huron directly, slower but correct. It never throws: the cache is loaded once per
chunk, so a throw would make every person in the chunk wait again.

Until 2026-11-23 it gave up after about 3 minutes by throwing, and `UpsertDeltaStrategy` (integration-huron-person)
treated a failed lookup as a person absent from Huron and sent a CREATE: presumably rejected (409) for a person already
there, so their update was lost for the run. integration-huron-person's Upsert now falls back to a direct lookup when
the cache lookup fails, and classifies a person whose direct lookup also fails as UPDATE (the update re-checks Huron
and fails, keeping the person out of hash storage, if they aren't there). Details:
[src/person-cache/README.md](../src/person-cache/README.md#error-handling).

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
