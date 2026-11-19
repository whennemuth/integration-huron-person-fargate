# Dashboard Pages

How the dashboard page is framed (sidebar, fragments, top bar), the parts every page is built from, and what the Run
history and Overview pages show and why. Bulk sync has its own doc, [BULK_SYNC.md](BULK_SYNC.md); the Administration
pages are in [AUTHORIZATION.md](AUTHORIZATION.md). Read this before changing a page's template
(`templates/partials/*-tab.mustache`), its script (`public/js/`), or `public/css/dashboard.css`.

## Page frame: sidebar and top bar

The dashboard page (`templates/dashboard.mustache`) is a left sidebar of grouped page links, and a main column with a
top bar above the pages. The login page has the same top bar and no sidebar. Colors, type and shape come from the theme
tokens at the top of `public/css/dashboard.css` (`--hp-*`), which Bootstrap's own `--bs-*` variables point at. Change
the look there, not per component.

- **Sidebar** (`PageNavigation`, `src/handlers/PageNavigation.ts`): the page groups and their items (id, label, Font
  Awesome icon). A group without a label is listed with no heading: the Overview, at the top, is the default page, so
  a bare URL opens it. `renderDashboard()` passes its `templateData(?tab)`: the `navigation` groups plus a
  `<page>Active` flag per page, which the page partials use. An unknown `?tab` falls back to the default page rather
  than showing none. Each item is a Bootstrap pill (`data-bs-toggle="pill"`) for the tab pane of the same id. At `lg`
  and up the sidebar is a static column. Below, it's an off-canvas menu (`offcanvas-lg`) opened from the top bar,
  which `dashboard.js` closes once a page is chosen. Tests: `test/DashboardPageNavigation.test.ts` and the sidebar
  render tests in `test/DashboardNavigation.test.ts`.
- **Page fragments** (`initializePageNavigation()` in `public/js/dashboard.js`): choosing a page sets the URL fragment
  `#<page>` (a history entry, so back and forward move between pages), and a fragment, which the server never sees,
  takes precedence over the server-rendered page on load. Only the fragment's first segment names the page
  (`#<page>/<detail>`), leaving room for detail views. A page shown by following the URL (load, back, forward) adds no
  history entry. Pages that fetch data when shown are loaded from `onPageShown()`. A page with details
  (`PAGES_WITH_DETAILS`, e.g. `#runs/<syncRunId>`) is called again when the fragment changes while it is shown, and
  must ignore a repeat call (a fragment change fires both `popstate` and `hashchange`). Choosing such a page in the
  sidebar while it shows a detail returns to the page itself. The panes have `scroll-margin-top`, so the browser's
  jump to the pane a fragment names stays clear of the sticky top bar.
- **Top bar**: built by `buildNavigationBar()` (`src/handlers/NavigationBar.ts`). Tests:
  `test/DashboardNavigation.test.ts`.
- **Landscape navigator**: a select over the landscape registry, rendered server-side from the `LandscapeRegistry`'s
  per-instance cache (no SSM read per page, and no `/api/landscapes` call from the page), with this landscape
  selected. Choosing one sends the browser to its `url` (`public/js/dashboard.js`). An entry without a `url` is listed
  but disabled. When the registry doesn't list this landscape (e.g. run locally), it's added, selected. If the
  registry can't be read or lists no landscape, a badge naming this landscape (`LANDSCAPE`, else `local`) takes the
  navigator's place.
- **Landscape tone** (`LandscapeTone`, `src/handlers/LandscapeTone.ts`): with no colored banner, the navigator (or
  badge) is tinted so it's obvious which landscape you're in: red for `production`, amber for `test`, gray for `mock`.
  The tone comes from the optional context setting `LANDSCAPE_TONE`, which `lib/FunctionUrl.ts` validates at synth
  (any other value fails it) and passes to the Lambda as the `LANDSCAPE_TONE` environment variable. When it's unset, a
  mock landscape (`isMockLandscape()`) is marked `mock` and any other landscape gets no tint. `buildNavigationBar()`
  resolves it at render time, ignoring an invalid value with a warning. A local run reads `LANDSCAPE_TONE` from the
  environment (or `.env`). The tone is also given as text for screen readers ("Production landscape"). Impersonation
  has a tone of its own ([AUTHORIZATION.md](AUTHORIZATION.md#impersonation)). Tests:
  `test/DashboardLandscapeTone.test.ts`, the navigation bar tests in `test/DashboardNavigation.test.ts`, and the synth
  test in `test/DashboardStack.test.ts`.
- **Sign-out**: a link to `SIGN_OUT_PATH` (`/logout`), shown unless the dashboard is public; the edge functions answer
  it ([DEPLOYMENT.md](DEPLOYMENT.md#shared-sign-in)). The top bar also shows the JWT's `eduPersonPrincipalName` (else
  `buPrincipal`), as a button opening the who-am-I dialog.

## Page patterns

Every page (`templates/partials/*-tab.mustache`) is built from the same parts, styled in the "Page patterns" section of
`public/css/dashboard.css`. Reuse them for new pages rather than adding page-specific styles:

- **Page header**: `.page` > `header.page-header` with an `h1.page-title`, a one-line `.page-description`, and
  `.page-actions` (e.g. Refresh) on the right.
- **Sections**: `section.page-section`, with an optional `.section-heading` (`h2.section-title`) over a white
  `.surface`. Use `.surface-flush` when a table runs to its edges, and `.page-columns` for a main section beside a
  narrower one (stacked below `lg`). Cards are only for a bounded object shown as one unit (a person record, a mapping
  result), not for every section.
- **Stat tiles**: `.stat-grid` of `.stat-tile` (`.stat-label` over `.stat-value`).
- **Data tables**: `table.table.data-table` inside `.table-responsive`. Cells stay on one line and the table scrolls
  sideways on a narrow screen; mark prose cells `.wrap` and figures `.num`. Outcomes use `.status-badge` (`-success`,
  `-warning`, `-danger`), from `Html.statusBadge()` (or `Html.badge(label, tone)`). No fixed-height inner scroll boxes:
  pages scroll as a whole.
- **States** (`PageState`, `public/js/page-state.js`): the one pattern for a section with nothing to show: `loading`,
  `empty`, `error` and `redacted` (the viewer's role doesn't allow the content). `PageState.loading(target, title)` and
  friends replace a container's content; `PageState.element(kind, { title, message, compact })` builds one to place
  yourself. Text is set as text, so a message may carry server output. `dashboard.js`, `person-card.js` and
  `mapping-experiment-card.js` all render their states through it, so it loads before them. A section's data load goes
  through `fetchSectionData()` (`dashboard.js`), which throws an `Error` carrying the server's message and `status`.
  Tests: `test/DashboardPageState.test.ts` (in a vm with a stand-in DOM, since jest runs without one).
- **Markup** (`Html`, `public/js/page-state.js`): HTML built from template literals escapes every value with
  `Html.escape()`, and draws badges and tags with `Html`. `dashboard.js`'s `escapeHtml()` and `statusBadge()` delegate
  to it.
- **Refresh stamp** (`RefreshStamp`, `public/js/page-state.js`): a `.refresh-stamp` in `.page-actions`, beside
  Refresh, set to "Refreshed HH:MM" once a page's data loads. Pages refresh only when asked: there is no polling (an
  incomplete run may never finish).
- **Paged lists** (`PagedTable` and `CursorPager`, `public/js/run-history.js`): a data table filled a page at a time
  from a cursor-paged route, with a `.table-footer` saying how many rows are shown and offering the next page, or
  explaining a failed page and offering to try again. `.filter-chip` buttons (`aria-pressed`, with a count) over a list
  choose what it lists.

Scripts load in dependency order: `page-state.js`, `run-history.js`, `system-status.js`, `overview.js`,
`bulk-sync.js`, `administration.js`, then `dashboard.js`, which creates the pages.

## Run history page

The `runs` page (`templates/partials/runs-tab.mustache`, `public/js/run-history.js`) shows the landscape's runs from
the run reporting routes ([API.md](API.md#run-reporting)). Its classes each do one thing:

- **`RunsApi`** calls the routes (`/api/runs?include=report`, `/api/runs/{id}/summary|chunks|errors|changes`,
  `/api/capabilities`) through `fetchSectionData()`, and **`RunView`** reduces a run's report to what the page shows:
  the summary's figures when the post-processor wrote one, else what the metadata, flags and terminal error say, with
  the report's `progress` (the chunk statistics summed so far) as the figures (`soFar`). An unfinished run's duration
  runs from its start to its last chunk (`lastChunkAt`), not to now. Deactivations are the processors' `removed` plus
  the deletion step's `deletedCount`.
- **An unfinished run's status is its phase** (`RunView.phase(now)`, drawn by `RunView.statusBadgeHtml(now)` in the
  list's Status, the run page's header, Overview and Bulk sync's status panel), in place of "Incomplete":
  **Fetching** (the chunker hasn't found the end of the source; no percent), **Processing**, **Post-processing** (every
  chunk processed; deactivations and the summary come next; no percent) or **Stalled** (`RunView.stalledFor(now)`: the
  stall rule below, applied to a run waiting on post-processing too, so one whose post-processor never ran doesn't
  read "Post-processing" forever; the rule `RunActivity` applies for Bulk sync's guard; with its percent unless it
  stalled while fetching). All amber; green is kept for a finished run. A run without chunk figures keeps
  "Incomplete", a failed one "Failed".
- **An unfinished run's figures** (`soFar`) are marked wherever they show. In the list its Added, Updated,
  Deactivated, Errors and Duration are muted (`td.so-far`), with its percent done beside its phase (none while fetching
  or post-processing, or "so far" without chunk figures; Records come from the metadata and aren't marked). Its tiles
  (`.stat-tile-so-far`, on the run's page and Overview) are labeled "so far", the Processed tile also shows the percent
  done ("of 735 chunks") and a bar (`RunDetailView.completionHtml()`), and they are followed by the progress line
  (`RunDetailView.progressHtml()` over `RunView.progressParts()`: "412 of 735 chunks processed · last chunk finished 1
  min ago · processing should finish around 7:45 PM (in about 5 h 06 min)"; while fetching, "412 chunks processed",
  with no "of"); an incomplete full sync's Deactivated tile notes that post-processing adds deletions.
- **Percent done** (`RunView.completion`): chunks processed ÷ chunks written (`chunksReported` ÷ METADATA
  `chunkCount`), rounded down to one decimal and capped at 100, so it reads 100.0 only when every chunk is processed.
  Chunks hold the same number of people bar the last, so it is the share of people processed too, at chunk
  granularity. It measures processing, not chunking. Its state (`.completion-<state>`): `done` (100.0, and chunking has
  finished, METADATA `partialOrEmptyChunkEncountered`), `underway`, `fetching` (no percent: the total isn't known, and
  `chunkCount` is added as each chunker task finishes, so it lags the chunks processed and could read 100% mid-fetch),
  all amber, since the run isn't finished until its summary; `failed` (gray).
  - **Why chunks**: the processors' own figure (`results.processed`, each chunk's `totalRecords`) counts only the
    people sent to Huron, those whose record changed: by it, a run where few people changed (a nightly full sync
    trusting stored fingerprints, or a Test of an unchanged person) never looked done, and never reached
    Post-processing (found in preview 2026-11-12; phase 5d step 5).
  - **Why amber when done**: `done` was green until 2026-11-11, which read as contradicting "Incomplete" beside it.
- **Estimated finish** (`RunView.estimate(now)`, underway runs only): the chunk rate so far (chunks processed ÷ start to
  last chunk) applied to the chunks left, from the last chunk's end; it leaves out post-processing. A run is
  **stalled** (in place of the last-chunk time, amber) when no chunk has finished for more than `RunView.STALL_FACTOR`
  (10) times its average gap between chunks and at least `RunView.MIN_STALL_MS` (15 minutes); a still-fetching run can
  stall but gets no estimate. A run waiting on post-processing stalls by the same rule ("Stalled: post-processing
  hasn't finished 3 days after the last chunk"). All from the browser's clock at each load.
- **The processor maximum** (`RunView.processorTasksLabel()`): among a run's facts, "Up to 8 processor tasks
  (override)" (the summary's `processorScalingCapacity`, else the flags'), and "Processor maximum" on the Flags card;
  nothing for a run from before it was recorded. The offline catalog's incomplete run has an override of 8.
- **Started by** (`RunView.startedByLabel()`): a column after Started, and the first of a run's facts ("Started by
  jdoe"): the signed-in user's `buPrincipal` for a start from Bulk sync ("Dashboard" without one), "Schedule" or
  "Runner", from the summary's `startedBy` or else the flags' (fargate `src/chunking/StartedBy.ts`); a dash for a run
  from before it was recorded. The offline catalog's newest 30 runs have one
  (`OfflineRunCatalog.STARTED_BY_RECORDED_FOR`): the schedule's nightly runs, two single-person runs and the incomplete
  run from Bulk sync, the bulk reset from the Runner.
- A single-person run is named by its BUID only when the report has metadata (the source URL's `buid`): a finished
  run's summary doesn't record it.
- **`RunHistoryPage`** shows the list (`#runs`, 20 runs a page with "Load older runs") or one run
  (`#runs/<syncRunId>`). The list keeps what it has loaded while a run is open; Refresh reloads it. While a run
  launched moments ago isn't listed yet, its launch note shows above the list (`RunHistoryPage.showLaunch()`,
  `data-run-launch`; [BULK_SYNC.md](BULK_SYNC.md#the-launch-flag)).
- **`RunDetailView`** is one run: the header (status, population, storage mode, duration, dry run, bulk reset), stat
  tiles, a note when the run failed, failed post-processing or has no summary yet (for a run with `progress`, saying
  its figures are summed from the chunks processed so far), and four tabs, each loaded when first opened:
  - **Overview**: the summary; the deletion outcome, whose counts are Removed from source, the deletion step's
    `totalProcessed`, then Deactivated and Failed, which add up to it; and the flags, each shown as its value by
    `RunDetailView.flagValue()`, never as JSON: Started by as `startedByLabel()`, the terms endpoint as one URL, any
    other object as `name: value` pairs, and a value longer than `WIDE_FLAG_LENGTH` under its name across the card.
  - **Chunks**: per-chunk statistics with totals.
  - **Errors**: filtered by status code, with counts from the summary or else the chunk statistics.
  - **Changes**: by type; in S3 mode, where `changesSupported` is false, it says why there are none.
- Tests: `test/DashboardRunHistory.test.ts` runs the script in a vm (as written and minified) and drives `RunsApi`
  through the handler with the offline run history.

## Overview page

The `overview` page (`templates/partials/overview-tab.mustache`, `public/js/overview.js`) is the landing page.
`OverviewPage` loads, each time the page is shown and on Refresh, the latest run (`/api/runs?include=report&limit=1`:
its status, facts, how long ago it finished or started, the run page's note and stat tiles, figures so far for a run
without a summary, and a link to its page) and the service checks (`/api/status`, in the `ServiceStatusTable` of
`public/js/system-status.js`, which the Status page uses too). The two load independently, so one failing (the real
`SystemStatus` service isn't implemented yet) leaves the other shown. While a run launched moments ago isn't listed yet
(the runs route's `launch`), its launch note comes first, in place of "No runs yet" or above the latest run. Tests:
`test/DashboardOverview.test.ts`.

**A mock landscape's own section** ("Mock landscape", hidden in any other landscape; since 2026-11-15,
`mock-landscape-options-plan.md` step 3a): loaded with the rest from `GET /api/mock-landscape/reset`.

- It explains in plain words what a reset empties, and says what the last reset did (`BulkSync.resetState()`, from the
  record the chunker subscriber keeps in the atomic counter table): running, for how long and since when, up to 10
  minutes; finished, with each table's count and time to the second (`OverviewPage.resetDuration()`); failed, with
  why; or never finished, most likely out of time.
- It offers Reset landscape. It asks first (`OverviewPage.RESET_CONFIRMATION`), then posts (`BulkSync.requestReset()`),
  and says the reset runs in the background for up to 10 minutes: Refresh shows whether it has finished (the page
  never polls).
- Disabled when one can't be started now, with the reason as a note, except while a reset is running, which its own
  line already says (the reason is then only the button's title).
- It stays hidden when its read is refused ([AUTHORIZATION.md](AUTHORIZATION.md)).
- **Offline**, the stand-in service keeps the last reset in memory (one per landscape played,
  `ServiceProvider.getBulkSyncService()`): a past one that emptied an old hybrid run's tables, replaced by a reset
  requested there, which finds the tables empty. The sample Run history isn't emptied.
