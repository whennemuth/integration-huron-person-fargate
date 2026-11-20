# src/schedule/: Schedules and Their Cron Expressions

Everything about the landscape's bulk sync schedule lives here, as classes: the AWS cron model (parsing, validating,
building from plain choices, run times in a time zone, wording, overlap between schedules). The design and its
reasoning, including why it has its own cron engine rather than a library: the plan doc `bulk-sync-schedule-plan.md`
at the repo root while the work is underway.

## Files

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

Tests: `test/schedule/`.

## Rules and pitfalls

- **AWS cron is its own dialect**: six fields with a year and no seconds, day-of-week **1 is Sunday** (`2` is Monday),
  `W`, and exactly one of day-of-month and day-of-week must be `?`. Never hand an AWS expression to a standard cron
  library unconverted.
- **The page never builds or parses cron**: it sends `ScheduleChoices`, and the backend answers with the expression,
  its wording, the next runs and any problem.
- **Daylight saving**: a run time the clocks skip has no run that day; one that happens twice runs once, at the second.
  `TimeZone.resolve()` does the same, so run times shown match AWS's.
- **cronstrue takes seven fields here** (`CronSchedule.describe()` puts seconds first), so it can't read six as
  seconds-to-day-of-week, and `dayOfWeekStartIndexZero: false` for AWS's numbering.
- `CronSchedule` takes any valid expression; at most once a day is a bulk sync rule, applied by the schedule's owner.
