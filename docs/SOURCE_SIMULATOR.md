# The Source Simulator

The source simulator (`src/chunking/fetch/SourceSimulator.ts`) is a Lambda Function URL that stands in for the BU CDM
source APIs, so the full three-phase pipeline can be tested without the real API's 30-minute cooldown. It exists only
in a mock landscape (see [MOCK_LANDSCAPES.md](MOCK_LANDSCAPES.md)). Read this before changing the simulator, the
Runner's `SourceSimulatorRunnerDecorator`, or the dashboard's Bulk sync with the simulator as its source.

## Endpoints

Path-based routing:

- **`/`** (default): mock person data, using a stateful depletion model (see repo memory
  `source-simulator-semantics.md` for allocator semantics). Its depletion counter is `simulator-offset-counter` in the
  landscape's atomic counter table, reset at every run start in a mock landscape
  ([RUN_START.md](RUN_START.md#the-run-counters)).
- **`/person?buid=U0000042`**: one person, mirroring the real person API. Stateless (the depletion counter is
  untouched) and idempotent: `U0000042` is index 41, the same payload `/` produces for that person. A BUID beyond
  `MOCK_TOTAL_POPULATION` returns `{ response: [] }`; a missing or malformed one (not `U` + digits) returns 400. It
  must be selected by a `buid` query parameter, because that is what makes the chunker do a single, non-paged request
  (`BigJsonFetch.isNotBatchable()`). With `RUNNER_SINGLE_PERSON_BUID` set, `SourceSimulatorRunnerDecorator` points the
  source here.
- **`/terms`**: mock current-terms data (6 static terms: 2 current, 2 past, 2 future) - required because
  `DataMapperOrg.isCurrentSemester()` needs terms data to filter student semesters, and running with
  `RUNNER_CHUNKING_ONLY=false` exercises this path.
- **`/health`**: unauthenticated; reports the simulator's settings (`totalPopulation`, `simulatedDelaySeconds`,
  `errorRate`). Asked with a run's settings, it reports them as that run gets them (clamped).

## API keys

Every endpoint accepts any of the three source keys in the config secret (people, person, terms), and rejects anything
else. Callers pick keys by URL, and the simulator's URLs can't satisfy all of them while hybrid runs need the secret's
real source URLs: the chunker never matches the simulator's `/person` to the configured person URL, so it sends the
people key there, whereas a client given the simulator's URLs directly (integration-huron-person's `SyncPerson.ts`)
sends the person key, and the processor always sends the terms key. See "API keys" in the `SourceSimulator.ts` header.

## How simulated terms reach the processor

Only for simulated-source runs. Terms are fetched by the processor tasks (integration-huron-person's
`getDataMapper()`/`DataMapperOrg`), from the config they load from the secret - so the Runner cannot redirect them by
changing its own config object (it used to, with no effect: mock runs fetched terms from the real terms API).

Instead `SourceSimulatorRunnerDecorator` returns a `termsEndpoint` with the source endpoint, which travels like
`personRecordProcessorAddons`: chunker SQS message (`ApiChunkerEvent.termsEndpoint`) -> `TaskParameters` ->
re-forwarded by `ChunkerQueue.sendNextChunkingMessage()` -> `docker/chunker.ts` writes it to the run's Flags
(`Flags.termsEndpoint`) -> `ProcessorForS3`/`ProcessorForDynamoDb` apply it via `applyTermsEndpoint()`
(`ProcessorCommon.ts`: replaces `dataSource.terms` baseUrl/fetchPath only; the apiKey stays).

Hybrid runs (real source -> mock target) and scheduled runs never set it, so they keep the configured (real) terms
endpoint. The field itself is generic, not mock-specific.

## Simulation settings

**`nonCurrentTermRate`** (optional query parameter, 0.0-1.0, default 0.0, on `/` and `/person`): the proportion of
simulated students assigned a non-current term, for testing semester-filtering logic. Which students get one is
decided by a hash of the person's index (not `Math.random()`), so a person's payload is identical on every request and
from either endpoint.

**Settings per request** (`simulationSettings()`, since 2026-11-14): `totalPopulation`, `simulatedDelaySeconds` (at
most 30) and `errorRate` query parameters, on any endpoint, override the Lambda's `MOCK_TOTAL_POPULATION`,
`MOCK_SIMULATED_DELAY_SECONDS` and `MOCK_ERROR_RATE`, which remain the defaults (set from the context at deploy). A
value that isn't a number is ignored; one out of range is clamped.

A run carries them in its `fetchPath` (e.g. `/?totalPopulation=1950`), so its settings travel with it and the Lambda's
configuration isn't changed for a run: the chunker keeps `fetchPath`'s own query on every page request
(`DataSource.getFetchUrl()` in integration-huron-person appends `recordCount` and `offset` to it; only `buid` stops it
paging) and passes `fetchPath` on in each chained message.

Both ways of starting a simulated run build the fetchPath with `simulatorFetchPath()` and change nothing in the Lambda:

- the dashboard's Bulk sync, which reads its form's defaults from the simulator's `/health` (`readSimulatorHealth()`;
  it reports the first three) (see [the dashboard's Bulk sync doc](../dashboard/docs/BULK_SYNC.md), "A mock
  landscape's own options");
- the manual Runner (`SourceSimulatorRunnerDecorator`, since 2026-11-16), whose `SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`,
  `_SIMULATED_DELAY_SECONDS`, `_ERROR_RATE` and `_NON_CURRENT_TERM_RATE` are each sent when set (0 included) and
  otherwise left to the simulator. `/health` asked with a run's settings is how the Runner predicts its chunks.

**History**: until 2026-11-16 the Runner wrote these settings into the Lambda's environment, where they stayed for
every later run (Bulk sync's defaults included), a 0 couldn't clear an earlier value, and a run could start before the
update applied.

## Safety: source simulation entails target simulation

The source simulator may only exist in a mock landscape, whose target is always mocked. This is enforced twice:
`lib/AppConstruct.ts` refuses to synthesize a stack with `LAMBDA.sourceSimulator.enabled` in a non-mock landscape (so
real stacks have no simulator Lambda at all), and `Runner.ts` aborts when `RUNNER_SOURCE_SIMULATOR=true` targets a
non-mock landscape. There is no supported way to run simulated source data against the real Huron target API.
