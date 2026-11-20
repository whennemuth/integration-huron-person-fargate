import { CronExpression } from './CronExpression';
import { DayOfWeekField } from './CronField';
import { DayName, DaysChoice, ScheduleChoices, ScheduleChoicesParser } from './ScheduleChoices';
import { ScheduleError } from './ScheduleError';

/**
 * Builds a CronExpression from plain terms (a time of day, and which days), and reads plain terms back from one.
 *
 *   new CronExpressionBuilder().at(4, 0).onWeekdays().build()      // cron(0 4 ? * MON-FRI *)
 *   CronExpressionBuilder.fromChoices({ mode: 'simple', time: '04:00', days: ['MON', 'WED'] })
 *   CronExpressionBuilder.toChoices('cron(0 4 ? * MON-FRI *)')     // { mode: 'simple', time: '04:00', days: 'weekdays' }
 *
 * What it writes: every day as `M H * * ? *` (AWS's own form), chosen days as `M H ? * <days> *` with day names, never
 * numbers (AWS counts from Sunday, standard cron from Monday, so a number invites mistakes).
 */
export class CronExpressionBuilder {
  static readonly WEEKDAYS: readonly DayName[] = ['MON', 'TUE', 'WED', 'THU', 'FRI'];
  static readonly WEEKENDS: readonly DayName[] = ['SUN', 'SAT'];

  private hour?: number;
  private minute?: number;
  private days: DaysChoice = 'every-day';

  /** @throws ScheduleError INVALID_CHOICES for a time that isn't one */
  at(hour: number, minute = 0): this {
    if (!Number.isInteger(hour) || hour < 0 || hour > 23 || !Number.isInteger(minute) || minute < 0 || minute > 59) {
      throw ScheduleError.of('INVALID_CHOICES', [{ field: 'time', message: `${hour}:${minute} isn't a time of day` }], 'Bad time');
    }
    this.hour = hour;
    this.minute = minute;
    return this;
  }

  /** @param time 24-hour HH:MM */
  atTime(time: string): this {
    const parsed = ScheduleChoicesParser.time(time);
    if (parsed === undefined) {
      throw ScheduleError.of('INVALID_CHOICES', [{ field: 'time', message: 'must be a time of day, HH:MM (00:00 to 23:59)' }], 'Bad time');
    }
    const [hour, minute] = parsed.split(':').map(Number);
    return this.at(hour, minute);
  }

  everyDay(): this {
    this.days = 'every-day';
    return this;
  }

  onWeekdays(): this {
    this.days = 'weekdays';
    return this;
  }

  onWeekends(): this {
    this.days = 'weekends';
    return this;
  }

  /** @throws ScheduleError INVALID_CHOICES for no days */
  on(days: readonly DayName[]): this {
    if (days.length === 0) {
      throw ScheduleError.of('INVALID_CHOICES', [{ field: 'days', message: 'choose at least one day' }], 'No days');
    }
    this.days = [...days];
    return this;
  }

  /** @throws ScheduleError INVALID_CHOICES when no time was given */
  build(): CronExpression {
    if (this.hour === undefined || this.minute === undefined) {
      throw ScheduleError.of('INVALID_CHOICES', [{ field: 'time', message: 'choose a time of day' }], 'No time');
    }
    const dayOfWeek = CronExpressionBuilder.dayOfWeekField(this.days);
    return CronExpression.of({
      minutes: String(this.minute),
      hours: String(this.hour),
      dayOfMonth: dayOfWeek ? '?' : '*',
      month: '*',
      dayOfWeek: dayOfWeek ?? '?',
      year: '*',
    });
  }

  /**
   * The expression for the page's choices: built from a time and days, or parsed when typed in.
   * @throws ScheduleError INVALID_CHOICES or INVALID_CRON
   */
  static fromChoices(choices: ScheduleChoices): CronExpression {
    if (choices.mode === 'cron') {
      return CronExpression.parse(choices.expression);
    }
    const builder = new CronExpressionBuilder().atTime(choices.time);
    const { days } = choices;
    if (days === 'every-day') {
      return builder.everyDay().build();
    }
    if (days === 'weekdays') {
      return builder.onWeekdays().build();
    }
    if (days === 'weekends') {
      return builder.onWeekends().build();
    }
    return builder.on(days).build();
  }

  /**
   * The choices that show an expression on the page: simple when it is a time of day on days of the week (whatever
   * way it was written), else the cron expression as written. Edit schedule fills its form from this.
   * @throws ScheduleError INVALID_CRON for a string that doesn't parse
   */
  static toChoices(expression: CronExpression | string): ScheduleChoices {
    const cron = typeof expression === 'string' ? CronExpression.parse(expression) : expression;
    const asCron: ScheduleChoices = { mode: 'cron', expression: cron.toAwsString() };
    const { minutes, hours, dayOfMonth, month, dayOfWeek, year } = cron;
    if (!minutes.isSingleValue() || !hours.isSingleValue() || !month.isEvery() || !year.isEvery()) {
      return asCron;
    }
    const time = `${String(hours.singleValue).padStart(2, '0')}:${String(minutes.singleValue).padStart(2, '0')}`;

    let weekdays: number[];
    if (dayOfWeek.isNoSpecificValue && dayOfMonth.isEvery()) {
      weekdays = [1, 2, 3, 4, 5, 6, 7];
    } else if (dayOfMonth.isNoSpecificValue && !dayOfWeek.special) {
      weekdays = [...dayOfWeek.valueSet].sort((a, b) => a - b);
    } else {
      return asCron;
    }
    return { mode: 'simple', time, days: CronExpressionBuilder.daysChoice(weekdays) };
  }

  /** The day-of-week field for a days choice, or undefined for every day (which uses day-of-month instead) */
  private static dayOfWeekField(days: DaysChoice): string | undefined {
    if (days === 'every-day') {
      return undefined;
    }
    if (days === 'weekdays') {
      return 'MON-FRI';
    }
    if (days === 'weekends') {
      return CronExpressionBuilder.WEEKENDS.join(',');
    }
    const names = DayOfWeekField.NAMES as readonly string[];
    const chosen = names.filter(name => days.includes(name as DayName));
    return chosen.length === names.length ? undefined : chosen.join(',');
  }

  /** A set of weekdays (1 Sunday to 7 Saturday) as the simplest choice that names it */
  private static daysChoice(weekdays: number[]): DaysChoice {
    const names = weekdays.map(weekday => DayOfWeekField.NAMES[weekday - 1]);
    const same = (set: readonly DayName[]) => set.length === names.length && set.every(name => names.includes(name));
    if (names.length === 7) {
      return 'every-day';
    }
    if (same(CronExpressionBuilder.WEEKDAYS)) {
      return 'weekdays';
    }
    if (same(CronExpressionBuilder.WEEKENDS)) {
      return 'weekends';
    }
    return names;
  }
}
