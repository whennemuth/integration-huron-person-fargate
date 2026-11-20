import { CalendarDay } from './CalendarDay';
import { ScheduleError } from './ScheduleError';

/**
 * One field of an AWS cron expression: `cron(minutes hours day-of-month month day-of-week year)`.
 *
 * What AWS accepts, from the EventBridge Scheduler user guide ("Cron-based schedules",
 * https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html), checked 2026-11-19:
 *
 * | Field        | Values              | Wildcards        |
 * |--------------|---------------------|------------------|
 * | minutes      | 0-59                | , - * /          |
 * | hours        | 0-23                | , - * /          |
 * | day-of-month | 1-31                | , - * ? / L W    |
 * | month        | 1-12 or JAN-DEC     | , - * /          |
 * | day-of-week  | 1-7 or SUN-SAT      | , - * ? L #      |
 * | year         | 1970-2199           | , - * /          |
 *
 * - Day-of-week counts from Sunday: 1 is Sunday, 2 Monday ... 7 Saturday (the guide's "3#2" is the second Tuesday,
 *   and "6L" the last Friday). Standard cron's 0-6 numbering doesn't apply.
 * - `L` in day-of-month is the month's last day. In day-of-week, `nL` is the month's last weekday n, and `L` alone is
 *   the week's last day (Saturday).
 * - `nW` in day-of-month is the weekday (Monday-Friday) nearest day n, never leaving the month.
 * - `n#k` in day-of-week is the k-th weekday n of the month, and must be the field's only expression.
 * - `?` (no specific value) is for day-of-month or day-of-week; CronExpression requires exactly one of the two.
 *
 * Kept stricter than AWS where the guide is silent, so nothing accepted here can surprise: `L`, `nW`, `nL` and `n#k`
 * stand alone (never in a list), `LW` and `L-n` are refused, a range runs low to high (no wrapping, e.g. FRI-MON),
 * and names are accepted in either case. AWS's own validation stays the authority when a schedule is saved.
 */
export type CronFieldName = 'minutes' | 'hours' | 'day-of-month' | 'month' | 'day-of-week' | 'year';

/** A day field's special form, which can't be expressed as a set of values */
export type CronSpecial =
  /** `L` in day-of-month */
  | { kind: 'last-day-of-month' }
  /** `nW` in day-of-month */
  | { kind: 'nearest-weekday'; day: number }
  /** `nL` in day-of-week */
  | { kind: 'last-weekday-of-month'; weekday: number }
  /** `n#k` in day-of-week */
  | { kind: 'nth-weekday-of-month'; weekday: number; nth: number };

type FieldSpec = {
  name: CronFieldName;
  min: number;
  max: number;
  /** Names for the values from min up, e.g. JAN-DEC */
  names?: readonly string[];
  /** Whether '?' is allowed (the day fields) */
  allowsNoSpecificValue?: boolean;
};

export abstract class CronField {
  readonly name: CronFieldName;
  readonly min: number;
  readonly max: number;
  /** The field as written */
  readonly source: string;
  private readonly names: readonly string[];
  private readonly noSpecificValue: boolean;
  private readonly specialForm?: CronSpecial;
  private readonly values: ReadonlySet<number>;

  /** @throws ScheduleError INVALID_CRON naming this field */
  protected constructor(source: string, spec: FieldSpec) {
    this.name = spec.name;
    this.min = spec.min;
    this.max = spec.max;
    this.names = spec.names ?? [];
    this.source = (source ?? '').trim();

    if (!this.source) {
      throw this.error('is missing');
    }
    if (this.source === '?') {
      if (!spec.allowsNoSpecificValue) {
        throw this.error("can't be '?', which is only for day-of-month or day-of-week");
      }
      this.noSpecificValue = true;
      this.values = new Set();
      return;
    }
    this.noSpecificValue = false;
    this.specialForm = this.parseSpecial(this.source.toUpperCase());
    this.values = this.specialForm ? new Set() : this.parseList(this.source);
  }

  /**
   * A special form of the whole field (`L`, `nW`, `nL`, `n#k`), or undefined when it is an ordinary list. Only the
   * day fields have any.
   * @param source The field, upper-cased
   */
  protected parseSpecial(_source: string): CronSpecial | undefined {
    return undefined;
  }

  /** A single value token (a number, or a name), or undefined when it is neither */
  protected parseValue(token: string): number | undefined {
    if (/^\d+$/.test(token)) {
      const value = Number(token);
      return value >= this.min && value <= this.max ? value : undefined;
    }
    const index = this.names.indexOf(token.toUpperCase());
    return index >= 0 ? this.min + index : undefined;
  }

  /** The field's allowed values, said for an error message: "0-59", "1-12 or JAN-DEC" */
  protected get allowedText(): string {
    const names = this.names.length ? ` or ${this.names[0]}-${this.names[this.names.length - 1]}` : '';
    return `${this.min}-${this.max}${names}`;
  }

  /** A refusal naming this field */
  protected error(message: string): ScheduleError {
    return new ScheduleError('INVALID_CRON', `${this.name} ${message}`, { errors: [{ field: this.name, message }] });
  }

  private parseList(source: string): Set<number> {
    const values = new Set<number>();
    for (const term of source.split(',')) {
      for (const value of this.parseTerm(term.trim(), source.includes(','))) {
        values.add(value);
      }
    }
    return values;
  }

  /** One list term: `*`, `n`, `a-b`, any of those with `/step` */
  private parseTerm(term: string, inList: boolean): number[] {
    if (!term) {
      throw this.error('has an empty entry in its list');
    }
    if (term === '?') {
      throw this.error("can't have '?' in a list: it must be the whole field");
    }
    const [range, step, extra] = term.split('/');
    if (extra !== undefined) {
      throw this.error(`'${term}' has more than one '/'`);
    }
    let stepBy = 1;
    if (step !== undefined) {
      if (!/^\d+$/.test(step) || Number(step) < 1) {
        throw this.error(`'${term}' needs a whole number of at least 1 after '/'`);
      }
      stepBy = Number(step);
    }

    let low: number;
    let high: number;
    if (range === '*') {
      [low, high] = [this.min, this.max];
    } else if (range.includes('-')) {
      const [from, to, more] = range.split('-');
      if (more !== undefined) {
        throw this.error(`'${term}' isn't a range (low-high)`);
      }
      low = this.valueOf(from, term, inList);
      high = this.valueOf(to, term, inList);
      if (low > high) {
        throw this.error(`'${term}' must run from low to high`);
      }
    } else {
      low = this.valueOf(range, term, inList);
      high = step !== undefined ? this.max : low;
    }

    const values: number[] = [];
    for (let value = low; value <= high; value += stepBy) {
      values.push(value);
    }
    return values;
  }

  private valueOf(token: string, term: string, inList: boolean): number {
    const value = this.parseValue(token);
    if (value !== undefined) {
      return value;
    }
    if (inList && this.parseSpecial(term.toUpperCase())) {
      throw this.error(`'${term}' must be the whole field: it can't be part of a list`);
    }
    throw this.error(`'${token}' isn't a value it allows (${this.allowedText})`);
  }

  /** Whether the field is '?' */
  get isNoSpecificValue(): boolean {
    return this.noSpecificValue;
  }

  /** The field's special form (`L`, `nW`, `nL`, `n#k`), if it has one */
  get special(): CronSpecial | undefined {
    return this.specialForm;
  }

  /** The values an ordinary field allows (empty for '?' or a special form) */
  get valueSet(): ReadonlySet<number> {
    return this.values;
  }

  /** Exactly one value: e.g. `2`, not `1,2`, `1-3`, `*` or a step such as `0/6` */
  isSingleValue(): boolean {
    return !this.noSpecificValue && !this.specialForm && this.values.size === 1;
  }

  /** The value of a single-value field, else undefined */
  get singleValue(): number | undefined {
    return this.isSingleValue() ? [...this.values][0] : undefined;
  }

  /** Every value of its range: `*`, or a list or range that amounts to it */
  isEvery(): boolean {
    return !this.noSpecificValue && !this.specialForm && this.values.size === this.max - this.min + 1;
  }

  /** Whether an ordinary field allows the value */
  matches(value: number): boolean {
    return this.values.has(value);
  }

  toString(): string {
    return this.source;
  }
}

export class MinuteField extends CronField {
  constructor(source: string) {
    super(source, { name: 'minutes', min: 0, max: 59 });
  }
}

export class HourField extends CronField {
  constructor(source: string) {
    super(source, { name: 'hours', min: 0, max: 23 });
  }
}

export class MonthField extends CronField {
  static readonly NAMES = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'] as const;

  constructor(source: string) {
    super(source, { name: 'month', min: 1, max: 12, names: MonthField.NAMES });
  }
}

export class YearField extends CronField {
  constructor(source: string) {
    super(source, { name: 'year', min: 1970, max: 2199 });
  }

  /** The first year from `year` on that the field allows, or undefined when there is none */
  nextYearFrom(year: number): number | undefined {
    for (let candidate = Math.max(year, this.min); candidate <= this.max; candidate++) {
      if (this.matches(candidate)) {
        return candidate;
      }
    }
    return undefined;
  }

  /** The last year up to `year` that the field allows, or undefined when there is none */
  previousYearFrom(year: number): number | undefined {
    for (let candidate = Math.min(year, this.max); candidate >= this.min; candidate--) {
      if (this.matches(candidate)) {
        return candidate;
      }
    }
    return undefined;
  }
}

export class DayOfMonthField extends CronField {
  constructor(source: string) {
    super(source, { name: 'day-of-month', min: 1, max: 31, allowsNoSpecificValue: true });
  }

  protected parseSpecial(source: string): CronSpecial | undefined {
    if (source === 'L') {
      return { kind: 'last-day-of-month' };
    }
    const weekday = /^(\d{1,2})W$/.exec(source);
    if (weekday) {
      const day = Number(weekday[1]);
      if (day < 1 || day > 31) {
        throw this.error(`'${source}' needs a day of 1-31 before W`);
      }
      return { kind: 'nearest-weekday', day };
    }
    if (source === 'LW' || /^L-\d+$/.test(source)) {
      throw this.error(`'${source}' isn't a form AWS documents: use L (the last day) or nW (the weekday nearest day n)`);
    }
    return undefined;
  }

  /** Whether the field allows the day ('?' allows any: day-of-week decides) */
  matchesDay(day: CalendarDay): boolean {
    const special = this.special;
    if (this.isNoSpecificValue) {
      return true;
    }
    if (special?.kind === 'last-day-of-month') {
      return day.day === day.daysInMonth;
    }
    if (special?.kind === 'nearest-weekday') {
      return day.day === DayOfMonthField.nearestWeekday(day, special.day);
    }
    return this.matches(day.day);
  }

  /**
   * The weekday nearest day n of the day's month, never leaving the month: a Saturday moves to the Friday before (or
   * the Monday after, when n is the 1st), a Sunday to the Monday after (or the Friday before, when n is the last day).
   * A day past the month's end counts as its last day.
   */
  static nearestWeekday(inMonth: CalendarDay, n: number): number {
    const last = inMonth.daysInMonth;
    const target = Math.min(n, last);
    const weekday = CalendarDay.of(inMonth.year, inMonth.month, target).weekday;
    if (weekday === 7) {
      return target > 1 ? target - 1 : target + 2;
    }
    if (weekday === 1) {
      return target < last ? target + 1 : target - 2;
    }
    return target;
  }
}

export class DayOfWeekField extends CronField {
  static readonly NAMES = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'] as const;

  constructor(source: string) {
    super(source, { name: 'day-of-week', min: 1, max: 7, names: DayOfWeekField.NAMES, allowsNoSpecificValue: true });
  }

  protected parseSpecial(source: string): CronSpecial | undefined {
    const last = /^(\w+)L$/.exec(source);
    if (last && source !== 'L') {
      const weekday = this.parseValue(last[1]);
      if (weekday === undefined) {
        throw this.error(`'${source}' needs a day of the week (${this.allowedText}) before L`);
      }
      return { kind: 'last-weekday-of-month', weekday };
    }
    if (source.includes('#')) {
      const nth = /^(\w+)#(\d)$/.exec(source);
      const weekday = nth ? this.parseValue(nth[1]) : undefined;
      if (!nth || source.includes(',')) {
        throw this.error(`'${source}' isn't valid: n#k (the k-th weekday n of the month) must be the field's only expression`);
      }
      if (weekday === undefined || Number(nth[2]) < 1 || Number(nth[2]) > 5) {
        throw this.error(`'${source}' needs a day of the week (${this.allowedText}) before # and 1-5 after it`);
      }
      return { kind: 'nth-weekday-of-month', weekday, nth: Number(nth[2]) };
    }
    return undefined;
  }

  /** `L` alone is the week's last day, Saturday */
  protected parseValue(token: string): number | undefined {
    return token.toUpperCase() === 'L' && this.source.toUpperCase() === 'L' ? 7 : super.parseValue(token);
  }

  /** Whether the field allows the day ('?' allows any: day-of-month decides) */
  matchesDay(day: CalendarDay): boolean {
    const special = this.special;
    if (this.isNoSpecificValue) {
      return true;
    }
    if (special?.kind === 'last-weekday-of-month') {
      return day.weekday === special.weekday && day.day + 7 > day.daysInMonth;
    }
    if (special?.kind === 'nth-weekday-of-month') {
      return day.weekday === special.weekday && Math.ceil(day.day / 7) === special.nth;
    }
    return this.matches(day.weekday);
  }
}
