import { DayOfWeekField } from './CronField';
import { ScheduleError, ScheduleFieldError } from './ScheduleError';

/** A day of the week, as AWS cron names it */
export type DayName = typeof DayOfWeekField.NAMES[number];

/** Which days a simple schedule runs on: a named set, or chosen days of the week */
export type DaysChoice = 'every-day' | 'weekdays' | 'weekends' | DayName[];

/** A time of day and the days: what the page's simple form gathers */
export type SimpleChoices = { mode: 'simple'; time: string; days: DaysChoice };

/** A cron expression typed in */
export type CronChoices = { mode: 'cron'; expression: string };

/**
 * When a schedule runs, as the dashboard page gathers it: in plain terms, or as a cron expression. The backend turns
 * either into a CronExpression (CronExpressionBuilder.fromChoices()), so the page never builds or parses cron itself.
 */
export type ScheduleChoices = SimpleChoices | CronChoices;

/** Reads ScheduleChoices from a request body, refusing anything else with every problem named */
export class ScheduleChoicesParser {
  static readonly DAYS_NAMED = ['every-day', 'weekdays', 'weekends'] as const;

  /**
   * @param body e.g. `{ mode: 'simple', time: '04:00', days: 'weekdays' }`, `{ mode: 'simple', time: '04:00',
   *   days: ['MON', 'WED'] }` or `{ mode: 'cron', expression: 'cron(0 4 ? * MON-FRI *)' }`
   * @returns The choices, normalized: a 24-hour HH:MM time, chosen days upper-cased in week order (Sunday first)
   * @throws ScheduleError INVALID_CHOICES
   */
  static parse(body: unknown): ScheduleChoices {
    if (!body || typeof body !== 'object') {
      throw new ScheduleError('INVALID_CHOICES', 'Say when the schedule runs: a time and days, or a cron expression');
    }
    const { mode, time, days, expression } = body as Record<string, unknown>;

    if (mode === 'cron') {
      if (typeof expression !== 'string' || !expression.trim()) {
        throw ScheduleError.of('INVALID_CHOICES', [{ field: 'expression', message: 'type a cron expression' }], 'No cron expression');
      }
      return { mode: 'cron', expression: expression.trim() };
    }
    if (mode !== 'simple') {
      throw ScheduleError.of('INVALID_CHOICES', [{ field: 'mode', message: "must be 'simple' or 'cron'" }], 'Unknown way of choosing when');
    }

    const errors: ScheduleFieldError[] = [];
    const parsedTime = ScheduleChoicesParser.time(time);
    if (parsedTime === undefined) {
      errors.push({ field: 'time', message: 'must be a time of day, HH:MM (00:00 to 23:59)' });
    }
    const parsedDays = ScheduleChoicesParser.days(days);
    if (parsedDays === undefined) {
      errors.push({ field: 'days', message: "must be 'every-day', 'weekdays', 'weekends' or at least one day (SUN-SAT)" });
    }
    if (errors.length) {
      throw ScheduleError.of('INVALID_CHOICES', errors, 'The schedule choices are incomplete');
    }
    return { mode: 'simple', time: parsedTime!, days: parsedDays! };
  }

  /** A 24-hour HH:MM (H:MM accepted), normalized to HH:MM; undefined when it isn't one */
  static time(value: unknown): string | undefined {
    const match = typeof value === 'string' ? /^(\d{1,2}):(\d{2})$/.exec(value.trim()) : null;
    if (!match || Number(match[1]) > 23 || Number(match[2]) > 59) {
      return undefined;
    }
    return `${match[1].padStart(2, '0')}:${match[2]}`;
  }

  private static days(value: unknown): DaysChoice | undefined {
    if (typeof value === 'string') {
      return (ScheduleChoicesParser.DAYS_NAMED as readonly string[]).includes(value) ? value as DaysChoice : undefined;
    }
    if (!Array.isArray(value) || value.length === 0) {
      return undefined;
    }
    const names = DayOfWeekField.NAMES as readonly string[];
    const chosen = new Set<string>();
    for (const day of value) {
      const name = typeof day === 'string' ? day.trim().toUpperCase() : '';
      if (!names.includes(name)) {
        return undefined;
      }
      chosen.add(name);
    }
    return names.filter(name => chosen.has(name)) as DayName[];
  }
}
