# The Bulk Sync Schedule Page

The `schedule` page (Runs, "Bulk sync schedule") keeps this landscape's recurring bulk sync: EventBridge Scheduler's
`chunker-api-schedule-<landscape>`, which starts a run the way Bulk sync starts one by hand. This doc covers what the
page shows, how When is chosen and checked, and how saving, turning on or off and deleting work. The routes, with their
shapes and refusals: [API.md](API.md#the-bulk-sync-schedule). The schedule itself and its rules (one per landscape, at
most once a day, an hour from other landscapes', never deployed): fargate
[src/schedule/CLAUDE.md](../../src/schedule/CLAUDE.md). Read this before changing `public/js/bulk-schedule.js`,
`templates/partials/schedule-tab.mustache`, `src/services/BulkSchedule.ts`, or the schedule mode of
`public/js/bulk-sync.js`.

## The schedule is the record

Nothing about the schedule is kept in the dashboard's tables or anywhere else: every load reads it from EventBridge
Scheduler by name (`GET /api/bulk-schedule`, `ScheduledRun.get()`). Who changed it last and when travel in the
schedule's own description. A deploy never touches it, and `bin/teardown.sh` deletes it.

## What the page shows

- **With a schedule**, its card (`BulkScheduleView.cardHtml()`):
  - On or Off, its words and its cron, and its next three runs (or, while off, the runs it would make);
  - the run it starts, in the same sentences as Bulk sync's review (`BulkSyncView.summaryLines()` with the schedule's
    words leading);
  - who changed it last;
  - a warning when it calls an endpoint the integration config no longer names (`staleEndpoint`: the schedule's event
    is a snapshot);
  - the last scheduled run the chunker subscriber skipped because a run was underway (`lastRefusal`);
  - **Activate** or **Deactivate**, **Edit schedule**, and **Delete schedule** (which asks first).
- **Without one**, or while editing, Bulk sync's form in schedule mode with **When** first and **Turn the schedule on**
  last, and **Review run**.
- **Other landscapes**: every other landscape's schedule in the account, on or off, its words and its source, since
  only those that are on and read the real source keep this one an hour away.

## When

The When fieldset (`BulkScheduleView.whenHtml()`) gathers choices, not cron:

- **A time and days**: a time of day, and every day, weekdays, weekends, or chosen days;
- **Cron expression**: AWS cron typed in.

On every change the page sends the choices to `POST /api/bulk-schedule/when` and shows the answer under them
(`BulkScheduleView.previewHtml()`): the expression the server built, its words, the next three runs, any problem by
field ("hours: '25' isn't a value it allows"), any other landscape's schedule it would start within the hour of, and
daylight-saving notes (a 2:30 AM run has none on the night the clocks go forward). A change of source asks again: a
schedule on the source simulator is never too close to another. The page never builds or parses cron
(`BulkScheduleView.readWhen()` only reads the fields). Times are America/New_York, every landscape's.

## The run

The run's settings are Bulk sync's form (`BulkSyncView.formHtml()` with `layout.schedule`), its ids renamed `schedule-`
so they don't clash with Bulk sync's own form on the same page (`BulkScheduleView.runFormHtml()`). In schedule mode it
leaves out what a recurring run mustn't do or isn't held back by: Test, Start from scratch, the stalled run's override,
and the refusal about runs now (the chunker subscriber skips a scheduled start that finds a run underway). The server
checks the run as Bulk sync's (`BulkSync.scheduleEvent()`), including the choices a role must grant.

## Review, save, on/off, delete

- **Review run** reads the run (`BulkSyncPage.readRequest()`), When and on/off, locks the fields, and asks the server
  for the review (`POST /api/bulk-schedule/plan`, which makes a save's checks: a schedule too close to another
  landscape's is refused there when it is to be on). The review (`BulkSyncView.confirmationHtml()`, schedule mode)
  asks **Schedule this run?** with **Schedule run**, or **Save this schedule?** with **Save schedule** for an existing
  schedule, in recurring words ("At 04:00 AM, Monday through Friday (America/New_York): a full sync of everyone...").
  A refusal is shown under the form, which unlocks. **Back** returns to the form with what was entered.
- **Schedule run / Save schedule** sends what was reviewed (`PUT /api/bulk-schedule`), then loads the card.
- **Edit schedule** fills the form from the schedule's choices and run (`toChoices()` server side), with Custom settings
  open so what it has is what is saved again. **Cancel editing** drops the changes.
- **Activate / Deactivate** (`POST /api/bulk-schedule/state`): turning one on is checked against the other landscapes'
  schedules, and refused (shown on the card) when too close.
- **Delete schedule** asks, then `DELETE /api/bulk-schedule`; the form for a new schedule shows.

Editing needs `schedule:edit` (PipelineOperator, Operator, Admin); reading the page needs `runs:read`.

## Bulk sync's warning

Bulk sync itself warns, in its form and its review, of any schedule (this landscape's or another's) that is on, reads
the real source, and starts within the hour (`BulkSyncView.upcomingSchedulesHtml()`, from `upcomingSchedules`):
every landscape reads the same source, which takes one client at a time. Starting is still allowed.

## Offline

`npm run dashboard:dev:offline:mock` runs the real classes over an in-memory scheduler (`src/handlers/OfflineScheduler.ts`)
with staging on weekdays at 3:00 AM, preview every day at 5:00 AM, and mock1 at 4:30 AM on the source simulator. 4:00 AM
is clear of them; 3:30 AM shows a conflict. A schedule made offline lasts until the server stops.

Tests: `test/DashboardBulkSchedulePage.test.ts` (the script), `test/DashboardBulkSchedule.test.ts` (the service and the
routes).
