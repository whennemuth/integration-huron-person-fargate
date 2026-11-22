import cronstrue from 'cronstrue';
import { TestEnvironment } from 'integration-core';
import { CalendarDay } from './CalendarDay';
import { CronExpression } from './CronExpression';
import { CronExpressionBuilder } from './CronExpressionBuilder';
import { ScheduleChoicesParser } from './ScheduleChoices';
import { ScheduleError } from './ScheduleError';
import { TimeZone } from './TimeZone';

/**
 * How close two schedules come: the closest pair of runs found, one from each.
 * - overlaps: whether they are closer than the buffer asked for
 * - overlapsBy: by how much (ms) they fall short of the buffer, when they overlap
 * - gap: how far apart (ms) the closest pair is
 * - at / otherAt: that pair's runs, this schedule's and the other's
 * A schedule pair with no runs in the period compared has only `overlaps: false`.
 */
export type IOverlapDetails = {
  overlaps: boolean;
  overlapsBy?: number;
  gap?: number;
  at?: Date;
  otherAt?: Date;
};

export type ICronSchedule = {
  /** The six fields: `0 4 ? * MON-FRI *` */
  getExpression(): string;
  /** As AWS takes it: `cron(0 4 ? * MON-FRI *)` */
  getAwsExpression(): string;
  /** The first run after `from` (default now), or undefined when it never runs again */
  getNextExecutionDate(from?: Date): Date | undefined;
  /** The last run before `from` (default now), or undefined when it never ran */
  getPreviousExecutionDate(from?: Date): Date | undefined;
  /** The runs from `from` (inclusive) to `to` (exclusive), in order */
  executions(from: Date, to: Date): Iterable<Date>;
  /**
   * @param other The other cron schedule to check for overlap with.
   * @param startTimeBuffer The buffer time in milliseconds that one schedule must wait before the other can have begun.
   */
  overlapsWith(other: ICronSchedule, startTimeBuffer: number): IOverlapDetails;
};

/**
 * An AWS cron expression in a time zone: when it runs, how it reads in words, and how close it comes to another.
 *
 * Its own engine rather than a cron library (bulk-sync-schedule-plan.md, "Library or our own code"): AWS cron is its
 * own dialect (a year field, day-of-week counted from Sunday as 1, W, a required '?'), which cron-parser, for one,
 * reads differently, and run times follow EventBridge Scheduler's daylight-saving behavior (TimeZone). Runs are found
 * day by day: on each day the expression matches, at each of its times of day. cronstrue words the expression.
 *
 * Takes any valid expression. Whether it suits a bulk sync (at most once a day) is ScheduledRun's rule, which
 * isAtMostOnceADay() serves.
 */
export class CronSchedule implements ICronSchedule {
  /** How far getNextExecutionDate() and getPreviousExecutionDate() look */
  static readonly SEARCH_YEARS = 30;
  /** How far ahead overlapsWith() compares, by default: a year covers every day-of-week and month pattern */
  static readonly OVERLAP_HORIZON_DAYS = 366;
  /** The most runs overlapsWith() lists for one schedule, so a schedule that runs every minute can't stall it */
  static readonly MAX_COMPARED_EXECUTIONS = 10_000;

  readonly expression: CronExpression;
  readonly timeZone: TimeZone;

  /**
   * @param expression A CronExpression, or text CronExpression.parse() takes
   * @param timeZone The zone the expression is read in (default UTC, as AWS reads a schedule without one)
   * @throws ScheduleError INVALID_CRON for text that doesn't parse
   */
  constructor(expression: CronExpression | string, timeZone: TimeZone | string = TimeZone.UTC) {
    this.expression = typeof expression === 'string' ? CronExpression.parse(expression) : expression;
    this.timeZone = typeof timeZone === 'string' ? new TimeZone(timeZone) : timeZone;
  }

  getExpression(): string {
    return this.expression.toString();
  }

  getAwsExpression(): string {
    return this.expression.toAwsString();
  }

  /** At most one run on any day: a single minute and a single hour */
  isAtMostOnceADay(): boolean {
    return this.expression.isAtMostOnceADay();
  }

  getNextExecutionDate(from: Date = new Date()): Date | undefined {
    const last = this.timeZone.dayOf(from).plusDays(CronSchedule.SEARCH_YEARS * 366);
    for (const instant of this.instants(this.timeZone.dayOf(from), last, 1)) {
      if (instant.getTime() > from.getTime()) {
        return instant;
      }
    }
    return undefined;
  }

  getPreviousExecutionDate(from: Date = new Date()): Date | undefined {
    const last = this.timeZone.dayOf(from).plusDays(-CronSchedule.SEARCH_YEARS * 366);
    for (const instant of this.instants(this.timeZone.dayOf(from), last, -1)) {
      if (instant.getTime() < from.getTime()) {
        return instant;
      }
    }
    return undefined;
  }

  /** The next `count` runs after `from` (default now), fewer when it stops running */
  nextExecutions(count: number, from: Date = new Date()): Date[] {
    const runs: Date[] = [];
    let after = from;
    while (runs.length < count) {
      const next = this.getNextExecutionDate(after);
      if (!next) {
        break;
      }
      runs.push(next);
      after = next;
    }
    return runs;
  }

  *executions(from: Date, to: Date): Generator<Date> {
    const first = this.timeZone.dayOf(from).plusDays(-1);
    const last = this.timeZone.dayOf(to).plusDays(1);
    for (const instant of this.instants(first, last, 1)) {
      if (instant.getTime() >= to.getTime()) {
        return;
      }
      if (instant.getTime() >= from.getTime()) {
        yield instant;
      }
    }
  }

  /**
   * How close this schedule's runs come to the other's over the coming period: the closest pair, one run from each,
   * and whether it is closer than the buffer. Exact for the days each runs on: a weekday schedule and a weekend
   * schedule at the same time don't overlap, and 23:30 and 00:15 the next day do.
   *
   * @param startTimeBuffer How far apart (ms) runs of the two must start
   * @param from When the comparison starts (default now)
   * @param horizonDays How many days it covers (default a year)
   * @throws ScheduleError TOO_FREQUENT when either lists more than MAX_COMPARED_EXECUTIONS runs in the period
   */
  overlapsWith(
    other: ICronSchedule,
    startTimeBuffer: number,
    from: Date = new Date(),
    horizonDays: number = CronSchedule.OVERLAP_HORIZON_DAYS
  ): IOverlapDetails {
    const to = new Date(from.getTime() + horizonDays * 86_400_000);
    const mine = CronSchedule.listed(this, from, to);
    const theirs = CronSchedule.listed(other, from, to);

    let closest: { gap: number; at: Date; otherAt: Date } | undefined;
    let i = 0;
    let j = 0;
    while (i < mine.length && j < theirs.length) {
      const gap = Math.abs(mine[i].getTime() - theirs[j].getTime());
      if (!closest || gap < closest.gap) {
        closest = { gap, at: mine[i], otherAt: theirs[j] };
      }
      if (mine[i].getTime() < theirs[j].getTime()) {
        i++;
      } else {
        j++;
      }
    }
    if (!closest) {
      return { overlaps: false };
    }
    const overlaps = closest.gap < startTimeBuffer;
    return { overlaps, ...(overlaps ? { overlapsBy: startTimeBuffer - closest.gap } : {}), ...closest };
  }

  /** In words, with the time zone: "At 04:00 AM, Monday through Friday (America/New_York)" */
  describe(): string {
    let words: string;
    try {
      // Seven fields (seconds first), so cronstrue can't read the six as seconds-to-day-of-week
      words = cronstrue.toString(`0 ${this.getExpression()}`, { dayOfWeekStartIndexZero: false });
    } catch {
      words = this.getAwsExpression();
    }
    return `${words} (${this.timeZone.name})`;
  }

  /**
   * What daylight saving does to it over the coming period: the first day a run time is skipped (no run that day)
   * and the first a run time happens twice (one run, at the second), as notes for a user. Empty in a zone without
   * daylight saving, or for times clear of the change.
   */
  daylightSavingNotes(from: Date = new Date(), horizonDays: number = CronSchedule.OVERLAP_HORIZON_DAYS): string[] {
    const notes: string[] = [];
    let skipped = false;
    let repeated = false;
    const first = this.timeZone.dayOf(from);
    const last = first.plusDays(horizonDays);
    for (let day = first; day.compareTo(last) <= 0 && !(skipped && repeated); day = day.plusDays(1)) {
      if (!this.expression.matchesDay(day)) {
        continue;
      }
      for (const minuteOfDay of this.expression.minutesOfDay) {
        const { kind } = this.timeZone.resolve(day, minuteOfDay);
        const time = TimeZone.formatTimeOfDay(minuteOfDay);
        if (kind === 'skipped' && !skipped) {
          skipped = true;
          notes.push(`On ${day.format()} the clocks go forward past ${time} (${this.timeZone.name}), so there is no run that day.`);
        } else if (kind === 'repeated' && !repeated) {
          repeated = true;
          notes.push(`On ${day.format()} ${time} (${this.timeZone.name}) happens twice as the clocks go back; it runs once, at the second.`);
        }
      }
    }
    return notes;
  }

  /**
   * Every run on the days from `first` to `last` (both included), day by day in the direction given, and within a
   * day in the same order. Years the expression excludes are skipped whole.
   */
  private *instants(first: CalendarDay, last: CalendarDay, direction: 1 | -1): Generator<Date> {
    const { year } = this.expression;
    const times = direction === 1 ? this.expression.minutesOfDay : [...this.expression.minutesOfDay].reverse();
    const beyond = (day: CalendarDay) => direction * day.compareTo(last) > 0;

    for (let day = first; !beyond(day); day = day.plusDays(direction)) {
      if (!year.matches(day.year)) {
        const nextYear = direction === 1 ? year.nextYearFrom(day.year + 1) : year.previousYearFrom(day.year - 1);
        if (nextYear === undefined) {
          return;
        }
        // Continue from the first (or last) day of that year: the loop's step lands on it
        day = direction === 1 ? CalendarDay.of(nextYear, 1, 1).plusDays(-1) : CalendarDay.of(nextYear, 12, 31).plusDays(1);
        continue;
      }
      if (!this.expression.matchesDay(day)) {
        continue;
      }
      for (const minuteOfDay of times) {
        const { instant } = this.timeZone.resolve(day, minuteOfDay);
        if (instant) {
          yield instant;
        }
      }
    }
  }

  private static listed(schedule: ICronSchedule, from: Date, to: Date): Date[] {
    const runs: Date[] = [];
    for (const run of schedule.executions(from, to)) {
      if (runs.length === CronSchedule.MAX_COMPARED_EXECUTIONS) {
        throw new ScheduleError('TOO_FREQUENT',
          `${schedule.getAwsExpression()} runs too often to compare (more than ${CronSchedule.MAX_COMPARED_EXECUTIONS} runs)`);
      }
      runs.push(run);
    }
    return runs;
  }
}

/**
 * Harness CRON_SCHEDULE: try the schedule package's cron model from the command line, with no AWS access.
 *
 *   TASK=parse    EXPRESSION, TIME_ZONE         valid or not (each field's problem), words, next five runs, notes
 *   TASK=build    TIME and DAYS, or EXPRESSION  the expression the page's choices make, and the choices read back
 *   TASK=overlap  EXPRESSION, OTHER_EXPRESSION, BUFFER_MINUTES (default 60), TIME_ZONE
 *
 * TIME_ZONE defaults to America/New_York, bulk sync schedules' zone. DAYS is every-day, weekdays, weekends, or days
 * such as MON,WED,FRI.
 */
async function main(): Promise<void> {
  const {
    TASK: task, EXPRESSION: expression, OTHER_EXPRESSION: otherExpression, TIME: time, DAYS: days,
    BUFFER_MINUTES: bufferMinutes = '60', TIME_ZONE: timeZone = 'America/New_York'
  } = process.env;
  const zone = new TimeZone(timeZone);
  const required = (name: string, value?: string): string => {
    if (!value) {
      throw new Error(`${name} environment variable is required for TASK=${task}`);
    }
    return value;
  };
  const show = (schedule: CronSchedule) => {
    console.log(`Expression:     ${schedule.getAwsExpression()}`);
    console.log(`In words:       ${schedule.describe()}`);
    console.log(`Once a day:     ${schedule.isAtMostOnceADay() ? 'at most' : 'no, more often'}`);
    const runs = schedule.nextExecutions(5);
    console.log(`Next runs:      ${runs.length ? runs.map(run => zone.format(run)).join('\n                ') : 'none: it never runs again'}`);
    for (const note of schedule.daylightSavingNotes()) {
      console.log(`Note:           ${note}`);
    }
  };

  switch (task) {
    case 'parse': {
      show(new CronSchedule(required('EXPRESSION', expression), zone));
      break;
    }
    case 'build': {
      const choices = ScheduleChoicesParser.parse(expression
        ? { mode: 'cron', expression }
        : { mode: 'simple', time: required('TIME', time), days: days?.includes(',') ? days.split(',') : days ?? 'every-day' });
      const built = CronExpressionBuilder.fromChoices(choices);
      console.log(`Choices:        ${JSON.stringify(choices)}`);
      show(new CronSchedule(built, zone));
      console.log(`Read back:      ${JSON.stringify(CronExpressionBuilder.toChoices(built))}`);
      break;
    }
    case 'overlap': {
      const buffer = Number(bufferMinutes) * 60_000;
      const mine = new CronSchedule(required('EXPRESSION', expression), zone);
      const theirs = new CronSchedule(required('OTHER_EXPRESSION', otherExpression), zone);
      const details = mine.overlapsWith(theirs, buffer);
      console.log(`${mine.describe()}\n  vs ${theirs.describe()}, ${bufferMinutes} minutes apart at least:`);
      if (!details.at) {
        console.log('  No runs of both in the coming year');
        break;
      }
      console.log(`  ${details.overlaps ? 'OVERLAP' : 'Clear'}: closest ${Math.round(details.gap! / 60_000)} minutes apart, ` +
        `${zone.format(details.at)} and ${zone.format(details.otherAt!)}` +
        (details.overlaps ? `, ${Math.round(details.overlapsBy! / 60_000)} minutes short` : ''));
      break;
    }
    default:
      throw new Error(`Unknown TASK value: ${task}. Valid values: parse, build, overlap`);
  }
}

if (require.main === module) {
  const testEnvironment = TestEnvironment('CRON_SCHEDULE');

  [
    'TASK',
    'EXPRESSION',
    'OTHER_EXPRESSION',
    'TIME',
    'DAYS',
    'BUFFER_MINUTES',
    'TIME_ZONE'
  ].forEach(testEnvironment.getVar);

  main().catch((error: unknown) => {
    if (ScheduleError.isScheduleError(error)) {
      console.error(`CronSchedule harness: ${error.code}`);
      error.errors.forEach(({ field, message }) => console.error(`  ${field ? `${field}: ` : ''}${message}`));
    } else {
      console.error(`CronSchedule harness failed: ${error instanceof Error ? error.message : String(error)}`);
    }
    process.exitCode = 1;
  });
}
