/**
 * A date on the calendar, with no time and no time zone: the unit a cron expression's day fields are matched against
 * (CronExpression.matchesDay()). Immutable. Weekdays are numbered as AWS cron numbers them: 1 is Sunday, 7 Saturday.
 */
export class CalendarDay {
  private constructor(readonly year: number, readonly month: number, readonly day: number) {}

  /** @param month 1-12 */
  static of(year: number, month: number, day: number): CalendarDay {
    const date = new Date(Date.UTC(year, month - 1, day));
    if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) {
      throw new RangeError(`Not a calendar day: ${year}-${month}-${day}`);
    }
    return new CalendarDay(year, month, day);
  }

  /** The day a UTC timestamp (ms) falls on in UTC */
  private static fromUtcMillis(millis: number): CalendarDay {
    const date = new Date(millis);
    return new CalendarDay(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
  }

  private get utcMillis(): number {
    return Date.UTC(this.year, this.month - 1, this.day);
  }

  /** 1 (Sunday) to 7 (Saturday) */
  get weekday(): number {
    return new Date(this.utcMillis).getUTCDay() + 1;
  }

  get daysInMonth(): number {
    return new Date(Date.UTC(this.year, this.month, 0)).getUTCDate();
  }

  /** The day `days` later (earlier when negative) */
  plusDays(days: number): CalendarDay {
    return CalendarDay.fromUtcMillis(this.utcMillis + days * 86_400_000);
  }

  /** Negative, zero or positive as this day is before, the same as, or after the other */
  compareTo(other: CalendarDay): number {
    return this.utcMillis - other.utcMillis;
  }

  /** "Sun, Mar 14, 2027" */
  format(): string {
    return CalendarDay.displayFormat.format(new Date(this.utcMillis));
  }

  private static readonly displayFormat = new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC', weekday: 'short', month: 'short', day: 'numeric', year: 'numeric',
  });

  /** YYYY-MM-DD */
  toString(): string {
    const pad = (value: number) => String(value).padStart(2, '0');
    return `${this.year}-${pad(this.month)}-${pad(this.day)}`;
  }
}
