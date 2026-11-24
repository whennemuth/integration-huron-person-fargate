# Bulk Sync and the Launch Flag

The Bulk sync page starts a sync run in the dashboard's landscape the way its schedule does, after a review of what
will run. This doc covers its options (including a mock landscape's own), how the form, review and start fit
together, who is recorded as starting the run, and the launch flag that bridges the minutes before a started run is
listed. The routes are in [API.md](API.md); how the pipeline starts the run is in the fargate
[docs/RUN_START.md](../../docs/RUN_START.md). Read this before changing `src/services/BulkSync.ts`,
`BulkSyncRequest`, `public/js/bulk-sync.js`, `templates/partials/bulk-tab.mustache` or `src/services/LaunchFlag.ts`.

## The page

The `bulk` page (`templates/partials/bulk-tab.mustache`, `public/js/bulk-sync.js`) starts a sync run in this
landscape the way its schedule does (`GET`/`POST /api/bulk-sync`, `POST /api/bulk-sync/plan`, `BulkSync`).

### The options

`BulkSyncRequest`:

- **A run type**: Full sync (with deactivations), `person-full`, the default; Full sync (no deactivations),
  `person-delta`; a disabled Delta sync, since a source of only the people who changed doesn't exist yet; Test, one
  person by BUID through the person endpoint, `person-delta`.
- **Bulk reset**.
- **Custom settings**: chunker tasks ("Chunks per task", 1-50, default 10, `BulkSyncRequest.CHUNKS_PER_TASK_DEFAULT`;
  or "Single task", sent as `iterationLimit: 0`), Parallel start and Processor tasks (below).
  - **A test is always a single task**, whatever the form sends (`BulkSyncRequest.parse()`), since a chained chunker
    task would fetch the same person again: with bulk reset it got ahead of the first while that built the person
    cache, and the person was chunked twice (preview 2026-11-12). On the page, choosing Test checks Single task and
    locks both chunk modes, and choosing another run type unlocks them and restores the mode chosen before, kept on
    the fieldset as `data-chosen-mode` (`BulkSyncPage.lockChunkMode()`).
- The defaults are the scheduled run's (`ScheduledRun`, fargate `src/schedule/ScheduledRun.ts`). The Custom settings
  hint lists the three defaults (`BulkSyncView.defaultsText()`).

### Parallel start

`parallelStart`: how many chunker tasks start the run at once, standing for the Runner's `MESSAGES_TO_PREPOPULATE` and
`DESIRED_COUNT` together. 0 sends the chunker subscriber one message, and auto-scaling adds tasks. N above 0 goes on
the event, and the subscriber seeds the queue with N messages (offsets a task's chunks apart) and sets the chunker
service's desired count to N once its scale-in alarm has settled (fargate `src/chunking/ParallelStart.ts`, shared with
the Runner's queue seeding), so the start takes up to a few minutes, which the review and the status panel say.

- **Its default** is 15 (`BulkSyncRequest.PARALLEL_START_DEFAULT`, since 2026-11-16; it was 0), lowered to its most:
  `describe()`'s `defaults.parallelStart`, which the form starts from, so a run starts at full speed rather than
  ramping up from one task. A request without `parallelStart` (the API's, not the form's) still gets 0, so the form
  carries the default (`data-default-parallel`) and `readRequest()` sends it for a full sync even with Custom settings
  closed (until 2026-11-17 it didn't, and a run started that way began with one task).
- **Its most** is the chunker service's most tasks (`DesiredCount.getMax()`, its scalable target's `MaxCapacity`),
  capped at 20 (`parallelStartMax()`): `describe()` offers 0 to it, and `plan()`/`start()` refuse more
  (`INVALID_REQUEST`). The Lambda has `application-autoscaling:DescribeScalableTargets` and the chunker cluster and
  service names (`CHUNKER_CLUSTER_NAME`, `CHUNKER_SERVICE_NAME`, `lib/LandscapeResources.ts`) for this; it changes
  nothing on the service itself. If the maximum can't be read, 20 is offered and the subscriber refuses what the
  service can't run.
- **Refused** for a test (one person, one task) and with a single task (`iterationLimit` 0: seeded offsets are a
  task's chunks apart). On the page it's held at 0 and locked while Test or Single task is chosen, and the value chosen
  before is restored afterward (kept on its wrapper as `data-chosen-parallel`: `BulkSyncPage.lockParallelStart()`);
  `readRequest()` sends 0 then whatever the select says.

### Processor tasks

`processorScalingCapacityOverride` (since 2026-11-17, `processor-max-tasks-plan.md`; fargate
[docs/RUN_START.md](../../docs/RUN_START.md#a-runs-own-processor-maximum-processorscalingcapacityoverride)): the
processor service's maximum for this run only, 1 to 20 (`BulkSyncRequest.PROCESSOR_TASKS_MIN`/`MAX`, the chunker
subscriber's), after Parallel start under Custom settings.

- **It starts at the landscape's** (`defaults.processorScalingCapacity`, from the Lambda's
  `PROCESSOR_MAX_SCALING_CAPACITY`, the context's `ECS.processorService.maxScalingCapacity`;
  `LandscapeResources.processorMaxScalingCapacity`). A dashboard that doesn't know it offers "The landscape's own".
- **Sent only when it differs** from the landscape's (`data-default-processors`, `readRequest()`), never for a test
  (refused by `parse()`; on the page locked at the landscape's while Test is chosen, the choice restored after:
  `BulkSyncPage.lockProcessorTasks()`). The event carries it, and the chunker subscriber sets the service's live
  maximum to it at the run's start.
- **The review** says "It uses up to 8 processor tasks at once, for this run only", and above the landscape's warns
  "Processors may be throttled" (`BulkSyncView.throttlingWarning()`, from the plan's `processorMaxScalingCapacity`
  setting) above Start run and Back: starting is allowed, to find out whether Huron takes more now. The review's
  settings list both.
- Above the landscape's maximum it needs the `bulk:processors` action ([AUTHORIZATION.md](AUTHORIZATION.md)).

### Bulk reset

Bulk reset is one choice for both previous-state flags (`ScheduledRun.previousStateFlags()`): checked sends
`bulkReset: true, trustPreviousStorage: false`, unchecked `false, true`. The pipeline only acts on their combination: the
chunker subscriber translates them into the run's one `previousStateSource` (`target` for a reset), and refuses an
event that has neither it nor the pair.

### Not offered

Command line only, or not yet: addons, `CHUNKING_ONLY` / `MESSAGING_ONLY` (they switch the subscribers' `DRY_RUN`,
which stays switched), skipping the counter reset (the chunker subscriber always resets), and `ITEMS_PER_CHUNK` (the
chunker task's environment, set at deploy). The plan route's settings list them with their fixed values; the page
doesn't show that list.

### Schedules starting soon

Every landscape reads the same source API, which takes one client at a time. So `describe()` and `plan()` report
`upcomingSchedules`: the bulk sync schedules, this landscape's or another's, that are on, read the real source, and
start within the hour (`ScheduledRun.upcomingStarts()`, fargate `src/schedule/`; the Lambda may read every landscape's
schedule). The page warns of them in its form and its review (`BulkSyncView.upcomingSchedulesHtml()`), and starting is
still allowed. Absent when there are none or they can't be read. The schedule
itself, and its page's routes: [API.md](API.md#the-bulk-sync-schedule).

## A mock landscape's own options

`BulkSyncState.mock`, a "Mock landscape" section after Custom settings, only where `isMockLandscape()`
(`mock-landscape-options-plan.md`, since 2026-11-15):

- **Source**: the real source API (the default, a hybrid run) or the source simulator. With the simulator, the event's
  source is its Function URL (looked up by `AwsSourceSimulatorService`), its people endpoint or `/person?buid=` for a
  test, with the four simulator settings as query parameters of `fetchPath` (`simulatorFetchPath()`, which the manual
  Runner shares, and `simulationSettings()`, both in `src/chunking/fetch/SourceSimulator.ts`), and its `/terms` as the
  `termsEndpoint`. Nothing about the simulator Lambda is changed. Its settings start at its deployed ones, read from
  its unauthenticated `/health`; refused out of range (`BulkSyncRequest.SIMULATOR_LIMITS`: at most 500,000 people, the
  chunker's safety cutoff, and 30 seconds a response). When the simulator isn't deployed, its choice is disabled with
  the reason. More on the simulator: fargate [docs/SOURCE_SIMULATOR.md](../../docs/SOURCE_SIMULATOR.md).
- **A test with the simulator** reads one person, so the page doesn't send People and the simulator keeps its own
  population, which only says which BUIDs it has. While Test and the simulator are both chosen, the form carries
  `BulkSyncView.SIMULATOR_TEST_CLASS` (`BulkSyncPage.markSimulatorTest()`; `dashboard.css` does the rest): People is
  hidden, the other settings' hints speak of one person (`SIMULATOR_FIELDS`' `testHint`), and the BUID box says the
  simulator's range (`simulatorBuidHint()`: U0000001 to its population, from `/health`) with `U0000042` as its
  placeholder. A BUID only has to be `U` and digits from 1: one past the simulator's people is allowed (since
  2026-11-16, `mock-run-feedback-plan.md`), since the simulator then answers with no one and the test sees someone who
  isn't in the source; the review says so (`simulatorTestLine()`, given the page's population).
- **Start from scratch** (`resetState`): the event carries it, and the chunker subscriber empties the target
  simulator, the statistics (Run history) and the delta baseline before the run starts
  (`src/chunking/MockLandscapeReset.ts`). The page says Run history starts again from this run. It needs the
  `mock:reset` action.
- **Refused elsewhere** (`BulkSyncRequest.parse(body, { mockLandscape })`), and only a mock landscape's dashboard has
  the simulator's name and lookup grant. The review's `sourceSimulator`, `sourceSimulatorMock*` and
  `mockTargetResetState` settings come from the form in a mock landscape, and are fixed at `false` elsewhere.
- **The review** names the target simulator as the target, says which source, and notes a Parallel start seeded past
  the simulator's people (from the review's `itemsPerChunk`).
- **Offline**: `npm run dashboard:dev:offline:mock` plays the mock landscape, with a stand-in simulator
  (`OfflineBulkSyncService.STAND_IN_SIMULATOR`, mock's deployed settings) at an example URL. It is always found, so a
  simulator that isn't deployed is seen only in the tests.

## How the page works

- **The page's words are for people who don't know the pipeline**: bulk reset is explained as a fingerprint of each
  person's record rather than as hashes, and the page doesn't mention the schedule (a later page will describe what
  the schedule does).
- **`BulkSyncView`** is the words and markup. `formHtml(state, values)` starts at the defaults and keeps what was
  entered; choosing Test shows the BUID box and what a test is, checking Custom settings shows those fields, and
  "Chunks per task" shows its dropdown. A start the server would refuse now (`state.blocked`, not configured included)
  is said above Review run, which is disabled then; a stalled newest run instead offers the "Start despite the stalled
  run" checkbox (`overrideStalled`). Warnings (a mock landscape's real source) show first. `requestError()` catches an
  invalid request before it is sent, `summaryLines()` spells out what will run, and `confirmationHtml(plan)` shows the
  review: the summary, then a table of the event the chunker subscriber will receive.
- **`BulkSyncPage`** loads the state each time the page is shown and on Refresh. Review run locks the fields and asks
  the server for the review (`POST /api/bulk-sync/plan`), which a refusal answers under the fields (unlocked); Back
  unlocks them. So what is started is what was reviewed. Start posts the same request (`postSectionData()` in
  `dashboard.js`, which also carries the server's error `code`); a refusal is shown under the review.
- **The status panel is the server's state at each load** (`BulkSyncView.statusHtml()`): while a run launched moments
  ago isn't listed yet (`state.launch`), the launch note; else the newest run, as it is now, which Refresh keeps
  current. After a start the page simply reloads: the server raised the launch flag before answering, so the note
  shows at once, and Review run is disabled with the `RUN_LAUNCHING` reason. (It used to keep the started run as first
  found, so it said "Processing" after the run had finished, until the page was reloaded; fixed 2026-11-16,
  `mock-run-feedback-plan.md` issue 3.)
- The Lambda's environment carries what the review shows: `PIPELINE_STACK_ID`, `ITEMS_PER_CHUNK`, and the config
  secret's name (`lib/FunctionUrl.ts`).
- **Who starts it**: the handler passes the request's signed-in user (`Request.user`, the sign-in token's claims) to
  `plan()` and `start()`, and `BulkSync.startedBy(user)` makes `{ by: 'dashboard', principal: <buPrincipal> }` of it (no
  principal in a public dashboard). It goes on the event, so the review shows it and the run records it (Run history's
  Started by). Never taken from the request body.
- **Tests**: `test/DashboardBulkSyncPage.test.ts` (the script in a vm, as written and minified, and once over the
  offline routes); the service: `test/DashboardBulkSync.test.ts`.

## The launch flag

A run started from Bulk sync isn't listed until its first chunker task writes its flags and metadata (the RUN marker in
the statistics table), which can be minutes after Start: the chunker subscriber, invoked asynchronously, may first
reset a mock landscape (up to 10 minutes) or seed a parallel start (its alarm wait up to about 6), and the chunker task
chooses the run's syncRunId when it starts. A start from scratch also empties Run history. So until then the dashboard
keeps a flag of its own, `LaunchFlag` (`src/services/LaunchFlag.ts`, since 2026-11-16, `mock-run-feedback-plan.md`
issue 1); the pipeline knows nothing of it.

- **One SSM String parameter per landscape**, `/hp-dashboard/launch-flags/<landscape>` (apart from the landscape
  registry's path), holding a `Launch` as JSON: `launchedAt` (the start's `processedAt`, not the run's syncRunId),
  `expiresAt`, `startedBy` and the choices. Its lifetime is 5 minutes, plus 7 for a parallel start, plus 10 for a start
  from scratch (`LaunchFlag.lifetimeMs()`).
- **Raised** by `BulkSync.start()` once the start's checks pass, just before it invokes the chunker subscriber, with
  `PutParameter` `Overwrite: false`: of two starts that pass the checks together, only one put succeeds, and the other
  is refused `RUN_LAUNCHING` without invoking anything. Lowered again if the invoke fails. A review never raises it.
- **Assessed** (`LaunchFlag.assess(newestSyncRunId)`) by `BulkSync`'s state, start, review and reset checks, and by the
  runs route's first page (`RunList`): lowered once the newest listed run is no earlier than `launchedAt` less
  `CLOCK_SLACK_MS` (30 seconds: the chunker task's clock against the Lambda's), or once it has expired; otherwise
  active.
- **While active**, a start or a mock landscape's reset is refused (`RUN_LAUNCHING`), and the pages show the launch
  note (`RunFormat.launchNoteHtml()`, in `run-history.js`): what, by whom, how long ago, what it waits for (the reset,
  the parallel start's tasks), and the time after which it didn't start. Overview shows it in place of "No runs yet" or
  above the latest run, Run history above the list (`RunHistoryPage.showLaunch()`, `data-run-launch`), Bulk sync as its
  status panel.
- **Also refused while a mock landscape's reset runs** (`RESET_UNDERWAY`, from its record): during a start from
  scratch's reset the statistics table is empty, so the newest-run check alone would let a second start through.
- **Known trade-offs** (accepted 2026-11-16): a start that dies after the invoke (a failed reset, a dropped event, a
  chunker task that never starts) only shows in CloudWatch; its flag simply expires. A page load that reads an expired
  flag could, in the milliseconds before it deletes it, delete a new start's flag instead, leaving that start
  unguarded.
- **Offline** the flag is kept in memory (`MemoryLaunchFlagStore`), one per landscape played, shared by the offline
  Bulk sync and the runs route (`ServiceProvider.getLaunchFlag()`). No offline run ever appears, so a launch shows
  until it expires; restarting the server clears it. Tests clear it with `ServiceProvider.resetOfflineState()`.
- A dashboard teardown leaves a flag that happens to be up; there is at most one, and it lives for minutes.
- The `DASHBOARD_LAUNCH_FLAG` harness (`src/services/LaunchFlag.ts`) reads, assesses, raises and lowers it.
- Tests: `test/DashboardLaunchFlag.test.ts`, the launch cases in `test/DashboardBulkSync.test.ts`, the pages' in
  `DashboardBulkSyncPage`, `DashboardOverview` and `DashboardRunHistory`, and the grant in `DashboardStack`.
