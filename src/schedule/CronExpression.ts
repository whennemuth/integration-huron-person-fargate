import { CalendarDay } from './CalendarDay';
import { CronField, DayOfMonthField, DayOfWeekField, HourField, MinuteField, MonthField, YearField } from './CronField';
import { ScheduleError, ScheduleFieldError } from './ScheduleError';

/** The six fields of a cron expression, as written */
export type CronFieldSources = {
  minutes: string;
  hours: string;
  dayOfMonth: string;
  month: string;
  dayOfWeek: string;
  year: string;
};

/**
 * An AWS cron expression, `cron(minutes hours day-of-month month day-of-week year)`, as six CronFields (the syntax
 * is described there). Immutable, and always valid: parse() refuses anything else, with every problem found, each
 * naming its field.
 *
 * Besides the fields' own rules, exactly one of day-of-month and day-of-week must be '?'. The Scheduler guide only
 * says '*' in one needs '?' in the other; the EventBridge rules guide, and every example in both, put '?' in one of
 * the two, so this does too.
 */
export class CronExpression {
  private constructor(
    readonly minutes: MinuteField,
    readonly hours: HourField,
    readonly dayOfMonth: DayOfMonthField,
    readonly month: MonthField,
    readonly dayOfWeek: DayOfWeekField,
    readonly year: YearField
  ) {}

  /**
   * @param text `cron(0 4 ? * MON-FRI *)`, or the six fields alone (`0 4 ? * MON-FRI *`)
   * @throws ScheduleError INVALID_CRON, listing every problem
   */
  static parse(text: string): CronExpression {
    const trimmed = (text ?? '').trim();
    const wrapped = /^cron\((.*)\)$/i.exec(trimmed);
    if (!wrapped && /^(rate|at)\(/i.test(trimmed)) {
      throw new ScheduleError('INVALID_CRON', `Only cron expressions are supported, not '${trimmed}'`);
    }
    const inner = (wrapped ? wrapped[1] : trimmed).trim();
    const parts = inner ? inner.split(/\s+/) : [];
    if (parts.length !== 6) {
      throw new ScheduleError(
        'INVALID_CRON',
        `A cron expression has six fields (minutes hours day-of-month month day-of-week year), but '${trimmed}' has ${parts.length}`
      );
    }

    const errors: ScheduleFieldError[] = [];
    const build = <T extends CronField>(make: () => T): T | undefined => {
      try {
        return make();
      } catch (error) {
        if (!ScheduleError.isScheduleError(error)) {
          throw error;
        }
        errors.push(...error.errors);
        return undefined;
      }
    };
    const [minutes, hours, dayOfMonth, month, dayOfWeek, year] = parts;
    const fields = {
      minutes: build(() => new MinuteField(minutes)),
      hours: build(() => new HourField(hours)),
      dayOfMonth: build(() => new DayOfMonthField(dayOfMonth)),
      month: build(() => new MonthField(month)),
      dayOfWeek: build(() => new DayOfWeekField(dayOfWeek)),
      year: build(() => new YearField(year)),
    };

    if (fields.dayOfMonth && fields.dayOfWeek
      && fields.dayOfMonth.isNoSpecificValue === fields.dayOfWeek.isNoSpecificValue) {
      errors.push({
        message: fields.dayOfMonth.isNoSpecificValue
          ? "day-of-month and day-of-week can't both be '?': give one of them a value"
          : "one of day-of-month and day-of-week must be '?' (e.g. '* * ?' for every day, '? * MON-FRI' for weekdays)",
      });
    }

    if (errors.length) {
      throw ScheduleError.of('INVALID_CRON', errors, `'${trimmed}' isn't a valid cron expression`);
    }
    return new CronExpression(
      fields.minutes!, fields.hours!, fields.dayOfMonth!, fields.month!, fields.dayOfWeek!, fields.year!
    );
  }

  /** @throws ScheduleError INVALID_CRON */
  static of(fields: CronFieldSources): CronExpression {
    const { minutes, hours, dayOfMonth, month, dayOfWeek, year } = fields;
    return CronExpression.parse([minutes, hours, dayOfMonth, month, dayOfWeek, year].join(' '));
  }

  /** The six fields, in order */
  get fields(): CronField[] {
    return [this.minutes, this.hours, this.dayOfMonth, this.month, this.dayOfWeek, this.year];
  }

  /** Whether it runs on the day (any time of it) */
  matchesDay(day: CalendarDay): boolean {
    return this.year.matches(day.year)
      && this.month.matches(day.month)
      && this.dayOfMonth.matchesDay(day)
      && this.dayOfWeek.matchesDay(day);
  }

  /** The times of day it runs on a day it runs, in minutes after midnight, ascending */
  get minutesOfDay(): number[] {
    const times: number[] = [];
    for (const hour of [...this.hours.valueSet].sort((a, b) => a - b)) {
      for (const minute of [...this.minutes.valueSet].sort((a, b) => a - b)) {
        times.push(hour * 60 + minute);
      }
    }
    return times;
  }

  /** At most once on any day: a single minute and a single hour */
  isAtMostOnceADay(): boolean {
    return this.minutes.isSingleValue() && this.hours.isSingleValue();
  }

  /** The six fields: `0 4 ? * MON-FRI *` */
  toString(): string {
    return this.fields.map(field => field.toString()).join(' ');
  }

  /** As AWS takes it: `cron(0 4 ? * MON-FRI *)` */
  toAwsString(): string {
    return `cron(${this.toString()})`;
  }
}
