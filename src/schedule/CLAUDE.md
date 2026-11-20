# src/schedule/: The Bulk Sync Schedule

Everything about a landscape's bulk sync schedule lives here, as classes: the schedule itself (EventBridge Scheduler's
`chunker-api-schedule-<landscape>`, which invokes the chunker subscriber), the rules a bulk sync schedule follows,
and the AWS cron model under them (parsing, validating, building from plain choices, run times in a time zone,
wording, overlap between schedules). The design and its reasoning, including why it has its own cron engine rather
than a library: the plan doc `bulk-sync-schedule-plan.md` at the repo root while the work is underway.

## Files

- `ScheduledRun.ts` (harness `SCHEDULED_RUN`): the landscape's bulk sync schedule. Static: what a scheduled run is
  (`scheduleInput()`, `previousStateFlags()`, `scheduleName()`, `roleName()`, `usesRealSource()`, `TIMEZONE`,
  `MIN_SPACING_MINUTES`). Instance: `get()`, `preview(choices)`, `create()`, `replace()` (when, run and on/off in one
  update), `save()` (create or replace), `setEnabled()`, `setCron()`, `replaceInput()`, `delete()`,
  `otherSchedules()`, `upcomingStarts()` (real-source schedules starting within the hour, for Bulk sync's warning),
  with the bulk sync rules below. The dashboard's `BulkSchedule` (`dashboard/src/services/`) is a thin layer over it.
- `ScheduledStartGuard.ts`: the chunker subscriber skips a start the schedule makes while a run is underway in the
  landscape or a dashboard launch is up, and records why in `/hp-dashboard/schedule-refusals/<landscape>` for the
  dashboard (`SsmScheduledStartRefusalRecord`). Owns the launch flag's parameter path, which the dashboard's
  `LaunchFlag` takes from it.
- `EventBridgeSchedule.ts`: one named EventBridge Scheduler schedule through the SDK, knowing nothing about bulk
  syncs (`get()` gives a `ScheduleSnapshot`; `create()`, `update()`, `setEnabled()`, `delete()`, `list()`).
- `CronField.ts`: one field of an AWS cron (`MinuteField`, `HourField`, `DayOfMonthField`, `MonthField`,
  `DayOfWeekField`, `YearField`), with AWS's syntax summarized in its doc comment.
- `CronExpression.ts`: six fields, always valid (`parse()` refuses anything else, listing every problem by field);
  `matchesDay()`, `minutesOfDay`, `isAtMostOnceADay()`, `toAwsString()`.
- `CalendarDay.ts`: a date with no time or zone, what the day fields match against.
- `TimeZone.ts`: instants to local times and back with `Intl` alone, following EventBridge Scheduler at daylight-saving
  changes.
- `ScheduleChoices.ts`: what the dashboard page sends (a time and days, or a cron expression), and its parser.
- `CronExpressionBuilder.ts`: choices to a `CronExpression` (`fromChoices()`, or fluent `.at(4, 0).onWeekdays()`), and
  back (`toChoices()`, which fills the page's form from a schedule).
- `CronSchedule.ts` (harness `CRON_SCHEDULE`: `parse`, `build`, `overlap`, no AWS): an expression in a time zone; next
  and previous runs, `executions()`, `describe()` (cronstrue), `overlapsWith()`, `daylightSavingNotes()`.
- `ScheduleError.ts`: the package's one error type, by code (`INVALID_CHOICES`, `INVALID_CRON`, `TOO_FREQUENT`,
  `TOO_CLOSE`, `EXISTS`, `NOT_FOUND`), with per-field problems.

Tests: `test/schedule/`, and `test/ScheduledRun.test.ts` for the scheduled run's definition.

## Rules and pitfalls

- **The schedule is the record**: nothing about it is stored anywhere else; it is found by its name. A landscape has
  at most one (the name is fixed; a second create is refused `EXISTS`).
- **A bulk sync schedule runs at most once a day** (one minute and one hour; `TOO_FREQUENT`), in `America/New_York`.
- **Landscapes share the source API**: schedules of different landscapes that read the real source start at least
  `MIN_SPACING_MINUTES` (60) apart, checked against the other landscapes' enabled schedules when a schedule is
  created on, turned on, or changed while on (`TOO_CLOSE`). A schedule reading the source simulator (its input has a
  `termsEndpoint`) is never checked. Landscapes in another account or region can't be seen.
- **UpdateSchedule replaces the whole schedule**: anything it isn't sent goes back to its default. Change a schedule
  only through `EventBridgeSchedule.update()`, which reads it and sends every field back with the change.
- **A scheduled start never runs on top of another run in its landscape**: `ScheduledStartGuard` skips it (a stalled
  run doesn't count; a run state that can't be read does). Details: [docs/RUN_START.md](../../docs/RUN_START.md).
- **New schedules invoke once, on time**: no retries and no flexible window, so a missed day is a missing run, never
  a run hours late.
- **AWS cron is its own dialect**: six fields with a year and no seconds, day-of-week **1 is Sunday** (`2` is Monday),
  `W`, and exactly one of day-of-month and day-of-week must be `?`. Never hand an AWS expression to a standard cron
  library unconverted.
- **The page never builds or parses cron**: it sends `ScheduleChoices`, and `ScheduledRun.preview()` answers with the
  expression, its wording, the next runs and any problem.
- **Daylight saving**: a run time the clocks skip has no run that day; one that happens twice runs once, at the second.
  `TimeZone.resolve()` does the same, so run times shown match AWS's.
- **cronstrue takes seven fields here** (`CronSchedule.describe()` puts seconds first), so it can't read six as
  seconds-to-day-of-week, and `dayOfWeekStartIndexZero: false` for AWS's numbering.
- **Names**: the schedule `chunker-api-schedule-<landscape>`, its role `chunker-schedule-<landscape>`, its target the
  chunker subscriber (`CHUNKER_SUBSCRIBER_BASE_NAME` in `src/Utils.ts`, so this package needn't load the subscriber).
