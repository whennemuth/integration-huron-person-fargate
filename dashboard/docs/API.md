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
- `POST /api/person-sync` - sync an individual person record (Lookup and sync's Sync now): `{ personId, operation, hrn? }`.
  The person is always pushed, with no in-sync check. In DynamoDB mode a successful push then records their hash in
  the person current state table, as a manual SyncPerson with `SYNC_UPDATE_HASH=true` does (a new or changed hash
  only, with a person history record stamped with the time of the sync, which belongs to no run). A person in the
  source who can't be mapped is reported and left as they are; one the data mapper finds ineligible is deactivated.
- `GET /api/bulk-sync` - what starting a run would do and whether it's allowed now (`BulkSyncState`: the form's
  defaults, the latest run, `launch` while a run launched moments ago isn't listed yet (see
  [BULK_SYNC.md](BULK_SYNC.md#the-launch-flag)), and `blocked` with the refusal a start would get, `NOT_CONFIGURED`
  included, so the page shows even where starting isn't configured).
- `POST /api/bulk-sync/plan` - review a run: the same checks as a start, then what it would send (`BulkSyncPlan`: the
  choices, the `ApiChunkerEvent`, and every setting of the run, named as the manual Runner's `RunnerEnv`, each with its
  source: `form`, `landscape`, `fixed` or `unused`; the page shows the choices and the event, not the settings); `200`,
  nothing is started.
- `POST /api/bulk-sync` - start a sync run, as the EventBridge schedule does (`BulkSync`, `src/services/BulkSync.ts`):
  `{ runType: 'full' | 'full-no-deactivations' | 'test', buid? (a test's, and only a test's), previousStateSource? ('storage-then-target', the default, 'storage', or
  'target' for a bulk reset; bulkReset is refused),
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
- `upcomingSchedules` (in `BulkSyncState` and `BulkSyncPlan`): bulk sync schedules, this landscape's or another's,
  that read the real source and start within the hour (`UpcomingStart[]`: landscape, `thisLandscape`, `at`,
  `minutesAway`, description), since a run started now shares the source with them; absent when there are none or
  they can't be read. Starting is still allowed.
- `GET /api/status` - system health status and metrics.

## The bulk sync schedule

The landscape's schedule, EventBridge Scheduler's `chunker-api-schedule-<landscape>`, read and changed through
`BulkSchedule` (`src/services/BulkSchedule.ts`) over fargate's `src/schedule/ScheduledRun.ts`. When it runs travels
as `when`, a `ScheduleChoices`: `{ mode: 'simple', time: 'HH:MM', days: 'every-day' | 'weekdays' | 'weekends' |
['MON', ...] }` or `{ mode: 'cron', expression: 'cron(...)' }`; the backend builds and checks the cron. The run travels
as `run`, Bulk sync's request body (no `test` run type, no `resetState`).

- `GET /api/bulk-schedule` (`runs:read`) - `BulkScheduleState`: the schedule, or none (with its words, next runs,
  `choices` for the form, `run` read back as form values, who changed it last, and `staleEndpoint` when the config
  now names another endpoint); `when` (the form's starting choices from the context's `SCHEDULE`, the time zone, the
  spacing); `form` (Bulk sync's `BulkSyncState`); the other landscapes' schedules; `lastRefusal`, the last scheduled
  start the chunker subscriber skipped because a run was underway (`{ refusedAt, reason, syncRunId? }`, fargate
  `src/schedule/ScheduledStartGuard.ts`); `blocked` when the dashboard can't change the schedule (`NOT_CONFIGURED`).
- `POST /api/bulk-schedule/when` (`schedule:edit`, changes nothing) - `{ when, run? }` gives a `SchedulePreview`:
  the expression, its words, the next three runs, `errors` by field with `errorCode`, the `conflict` with another
  landscape's schedule (unless `run.source` is the simulator), and daylight-saving `notes`. Invalid choices are a
  `200` with their errors.
- `POST /api/bulk-schedule/plan` (`schedule:edit`, changes nothing) - `{ when, run, enabled? }` gives a
  `BulkSchedulePlan`: the preview, the run's choices, event and settings, `enabled` (default true) and whether saving
  `replaces` a schedule; refused as a save would be.
- `PUT /api/bulk-schedule` (`schedule:edit`) - create the schedule (`201`) or replace it (`200`), as reviewed;
  `BulkScheduleSaved`. Who changed it is the signed-in user.
- `POST /api/bulk-schedule/state` (`schedule:edit`) - `{ enabled }` turns it on or off; the schedule.
- `DELETE /api/bulk-schedule` (`schedule:edit`) - `{ deleted }`.
- **Refusals**: a `ScheduleError` as `{ error, code, errors, conflict? }`: 400 `INVALID_CHOICES`, `INVALID_CRON`,
  `TOO_FREQUENT` (more than once a day); 404 `NOT_FOUND`; 409 `TOO_CLOSE` (within an hour of another landscape's
  enabled schedule that reads the real source, when the schedule is to be on) or `EXISTS`. The run's settings are
  refused as Bulk sync's (`BulkSyncError`), and a choice the user's roles don't grant is a `403`.

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
  `REACTIVATED` | `DELETED`; DynamoDB storage mode only). A run's report (`/summary`, and each run's with
  `include=report`) carries `reactivated`, the count of its `REACTIVATED` records, in DynamoDB mode: the summary
  counts a reactivation among the updated.
- `GET /api/capabilities` - landscape, storage mode, whether change lists are supported.

`syncRunId` must be an ISO timestamp (URL-encoded or not); `limit` 1-500 (default 100). A `ReportingError` maps to
`400 { error, code }` (`INVALID_CURSOR`, `INVALID_LIMIT`, `UNSUPPORTED_IN_STORAGE_MODE`), except
`RUN_LIST_UNAVAILABLE` (the landscape's state, not the request's fault), which maps to `503 { error, code }`; anything
else to `500 { error, message }`.

## Landscape picker data

- `GET /api/landscapes` - `{ current, landscapes: [{ landscape, label, url?, isDefault, current }] }` from the landscape
  registry ([DEPLOYMENT.md](DEPLOYMENT.md#landscape-registry)). The dashboard page's own navigator doesn't call it
  ([PAGES.md](PAGES.md#page-frame-sidebar-and-top-bar)).
