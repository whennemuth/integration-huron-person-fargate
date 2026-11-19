# Dashboard API

Every route the dashboard Lambda answers (`src/handlers/DashboardHandler.ts`), with its request and response shapes
and refusal codes. Every `/api` route but `/api/me` needs an action of the signed-in user's roles and answers `403`
without it ([AUTHORIZATION.md](AUTHORIZATION.md)). Read this when adding or changing a route, or when a page gets an
answer it doesn't expect.

## Dashboard and access

- `GET /dashboard` - the dashboard page, with inline assets.
- `GET /api/me` - who the request is taken to be and what they may do here; needs only sign-in
  ([AUTHORIZATION.md](AUTHORIZATION.md#get-apime)).
- `GET /api/audit` - the landscape's audit trail, a page at a time (`audit:read`;
  [AUTHORIZATION.md](AUTHORIZATION.md#the-audit-trail)).
- `GET /api/access/roles`, `GET|POST /api/access/assignments`, `GET|PUT|DELETE /api/access/assignments/{person}` -
  who holds which roles, and giving roles to several people at once (`access:admin`;
  [AUTHORIZATION.md](AUTHORIZATION.md#the-access-routes)).
- `POST|DELETE /api/impersonation` - start or stop seeing the dashboard through another person's roles (starting needs
  `access:impersonate`; [AUTHORIZATION.md](AUTHORIZATION.md#impersonation)).

## Person and bulk operations

- `POST /api/person-lookup` - look up an individual person by BUID or HRN.
- `POST /api/person-sync` - sync an individual person record.
- `GET /api/bulk-sync` - what starting a run would do and whether it's allowed now (`BulkSyncState`: the form's
  defaults, the latest run, `launch` while a run launched moments ago isn't listed yet (see
  [BULK_SYNC.md](BULK_SYNC.md#the-launch-flag)), and `blocked` with the refusal a start would get, `NOT_CONFIGURED`
  included, so the page shows even where starting isn't configured).
- `POST /api/bulk-sync/plan` - review a run: the same checks as a start, then what it would send (`BulkSyncPlan`: the
  choices, the `ApiChunkerEvent`, and every setting of the run, named as the manual Runner's `RunnerEnv`, each with its
  source: `form`, `landscape`, `fixed` or `unused`; the page shows the choices and the event, not the settings); `200`,
  nothing is started.
- `POST /api/bulk-sync` - start a sync run, as the EventBridge schedule does (`BulkSync`, `src/services/BulkSync.ts`):
  `{ runType: 'full' | 'full-no-deactivations' | 'test', buid? (a test's, and only a test's), bulkReset?,
  iterationLimit? (0-50, 0 a single task; default 10; always 0 for a test), parallelStart? (0 to the chunker service's
  most tasks, at most 20; refused for a test or a single task), processorScalingCapacityOverride? (1-20, the processor
  service's maximum for this run; refused for a test), overrideStalled?, and in a mock landscape only: source? ('real'
  | 'simulator'), simulator? ({ totalPopulation?, simulatedDelaySeconds?, errorRate?, nonCurrentTermRate? }, with the
  simulator), resetState? }`; `202` with what was started.
- **Refusals** of a review or start are a `BulkSyncError` as `{ error, code }`: 400 invalid, 403 production, 409 a run
  underway or stalled, a run launched moments ago and not yet listed (`RUN_LAUNCHING`), a mock landscape's reset
  running (`RESET_UNDERWAY`) or a subscriber in `DRY_RUN`, 503 not configured.
- `GET /api/mock-landscape/reset` - a mock landscape's last reset of its run state and whether one can be started now
  (`MockLandscapeResetState`; `{ mockLandscape: false }` in any other landscape). `POST` starts one: the chunker
  subscriber, invoked asynchronously with a reset-only event, empties the target simulator, Run history and the
  fingerprints; `202` once it has accepted it. Refusals as Bulk sync's (`BulkSyncError`): 400 not a mock landscape,
  409 a run underway (`RUN_UNDERWAY`) or launching (`RUN_LAUNCHING`), a reset still running (`RESET_UNDERWAY`) or the
  subscriber in `DRY_RUN`, 503 not configured.
- `GET /api/status` - system health status and metrics.

## Run reporting

This landscape's sync runs, read in-process from its tables and chunks bucket through fargate's
`src/reporting/RunReporting.ts`; GET only, no `landscape` parameter.

- `GET /api/runs` - `{ runs }`, the syncRunIds, oldest first.
- `GET /api/runs?include=report&limit&before` - a page of runs with their reports, newest first:
  `{ items, before?, launch? }` (`launch` on the first page only, while a run launched moments ago isn't listed yet).
  Up to `limit` runs (1-50, default 20) older than `before` (a syncRunId); the response's `before` continues the list
  and is present only while older runs remain. A run with nothing recorded to report on is listed as
  `{ syncRunId, status: 'UNKNOWN' }`. Built by `RunList` (`src/handlers/RunList.ts`): `listRunPage({ limit, before })`
  (one Query of the statistics table's `run-list-index`), then that page's reports in parallel. So Overview
  (`limit=1`) reads one run's key. A bad `limit` is `400` `INVALID_LIMIT`, a bad `before` or `include` is `400`.
- A table that can't serve `run-list-index` yet (its pipeline stack not deployed since the index was added, or the
  index still being built) is `503` `RUN_LIST_UNAVAILABLE`, on this route and `GET /api/runs`, with a message saying
  what to do, which the run list's error state shows.
- `GET /api/runs/{syncRunId}/summary` - the run's report (`404` if nothing is recorded).
- `GET /api/runs/{syncRunId}/chunks` - `{ items }`, per-chunk statistics.
- `GET /api/runs/{syncRunId}/errors?cursor&limit&statusCode` - a page `{ items, cursor? }`.
- `GET /api/runs/{syncRunId}/changes?changeType&cursor&limit` - a page of person changes (`NEW` | `UPDATED` |
  `DELETED`; DynamoDB storage mode only).
- `GET /api/capabilities` - landscape, storage mode, whether change lists are supported.

`syncRunId` must be an ISO timestamp (URL-encoded or not); `limit` 1-500 (default 100). A `ReportingError` maps to
`400 { error, code }` (`INVALID_CURSOR`, `INVALID_LIMIT`, `UNSUPPORTED_IN_STORAGE_MODE`), except
`RUN_LIST_UNAVAILABLE` (the landscape's state, not the request's fault), which maps to `503 { error, code }`; anything
else to `500 { error, message }`.

## Landscape picker data

- `GET /api/landscapes` - `{ current, landscapes: [{ landscape, label, url?, isDefault, current }] }` from the landscape
  registry ([DEPLOYMENT.md](DEPLOYMENT.md#landscape-registry)). The dashboard page's own navigator doesn't call it
  ([PAGES.md](PAGES.md#page-frame-sidebar-and-top-bar)).
