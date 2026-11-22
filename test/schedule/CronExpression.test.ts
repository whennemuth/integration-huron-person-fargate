/**
 * Tests the AWS cron model's syntax (src/schedule/CronField.ts, CronExpression.ts): each field's values, names,
 * lists, ranges, steps and special forms as the EventBridge Scheduler guide gives them, day-of-week counted from
 * Sunday, the '?' rule, and refusals that name every field at fault.
 */
import { CalendarDay } from '../../src/schedule/CalendarDay';
import { CronExpression } from '../../src/schedule/CronExpression';
import { DayOfMonthField } from '../../src/schedule/CronField';
import { ScheduleError } from '../../src/schedule/ScheduleError';

const day = (text: string): CalendarDay => {
  const [year, month, date] = text.split('-').map(Number);
  return CalendarDay.of(year, month, date);
};

/** The ScheduleError parse() throws for an expression */
const refusal = (text: string): ScheduleError => {
  try {
    CronExpression.parse(text);
  } catch (error) {
    if (error instanceof ScheduleError) {
      return error;
    }
    throw error;
  }
  throw new Error(`'${text}' parsed`);
};

describe('CronExpression', () => {
  describe('parse', () => {
    it('takes cron(...) or the six fields alone, and gives both forms back', () => {
      for (const text of ['cron(0 4 ? * MON-FRI *)', '0 4 ? * MON-FRI *', '  cron( 0  4 ? * MON-FRI * )  ']) {
        const expression = CronExpression.parse(text);
        expect(expression.toString()).toBe('0 4 ? * MON-FRI *');
        expect(expression.toAwsString()).toBe('cron(0 4 ? * MON-FRI *)');
      }
    });

    it('refuses anything but six fields', () => {
      const error = refusal('cron(0 4 * * ?)');
      expect(error.code).toBe('INVALID_CRON');
      expect(error.message).toMatch(/six fields .* has 5/);
      expect(refusal('').code).toBe('INVALID_CRON');
    });

    it("refuses rate() and at() expressions, which aren't cron", () => {
      expect(refusal('rate(1 day)').message).toMatch(/Only cron expressions/);
      expect(refusal('at(2027-01-01T04:00:00)').message).toMatch(/Only cron expressions/);
    });

    it('lists every problem, each naming its field', () => {
      const error = refusal('cron(61 25 * * MON 1960)');
      expect(error.code).toBe('INVALID_CRON');
      expect(error.errors).toEqual([
        { field: 'minutes', message: "'61' isn't a value it allows (0-59)" },
        { field: 'hours', message: "'25' isn't a value it allows (0-23)" },
        { field: 'year', message: "'1960' isn't a value it allows (1970-2199)" },
        { message: expect.stringMatching(/one of day-of-month and day-of-week must be '\?'/) },
      ]);
    });
  });

  describe("the '?' rule", () => {
    it('needs exactly one of day-of-month and day-of-week to be ?', () => {
      expect(() => CronExpression.parse('0 4 * * ? *')).not.toThrow();
      expect(() => CronExpression.parse('0 4 ? * MON *')).not.toThrow();
      expect(refusal('0 4 ? * ? *').errors[0].message).toMatch(/can't both be '\?'/);
      expect(refusal('0 4 1 * MON *').errors[0].message).toMatch(/must be '\?'/);
      expect(refusal('0 4 * * * *').errors[0].message).toMatch(/must be '\?'/);
    });

    it('allows ? only in the day fields, and only as the whole field', () => {
      expect(refusal('? 4 * * ? *').errors).toEqual([{ field: 'minutes', message: expect.stringMatching(/only for day-of-month or day-of-week/) }]);
      expect(refusal('0 4 1,? * ? *').errors[0]).toEqual({ field: 'day-of-month', message: expect.stringMatching(/can't have '\?' in a list/) });
    });
  });

  describe('lists, ranges and steps', () => {
    it('expands them into values', () => {
      const expression = CronExpression.parse('0/15 1-3,22 ? JAN,JUL-SEP */2 2026-2030/2');
      expect([...expression.minutes.valueSet]).toEqual([0, 15, 30, 45]);
      expect([...expression.hours.valueSet]).toEqual([1, 2, 3, 22]);
      expect([...expression.month.valueSet]).toEqual([1, 7, 8, 9]);
      expect([...expression.dayOfWeek.valueSet]).toEqual([1, 3, 5, 7]);
      expect([...expression.year.valueSet]).toEqual([2026, 2028, 2030]);
    });

    it('reads names in either case, including names that contain L or W', () => {
      const expression = CronExpression.parse('0 4 ? jul wed *');
      expect([...expression.month.valueSet]).toEqual([7]);
      expect([...expression.dayOfWeek.valueSet]).toEqual([4]);
    });

    it('refuses a range that runs high to low, a bad step and an empty list entry', () => {
      expect(refusal('0 4 ? * FRI-MON *').errors[0]).toEqual({ field: 'day-of-week', message: "'FRI-MON' must run from low to high" });
      expect(refusal('0/0 4 * * ? *').errors[0].message).toMatch(/at least 1 after '\/'/);
      expect(refusal('0 4,,5 * * ? *').errors[0].message).toMatch(/empty entry/);
    });
  });

  describe('days', () => {
    it('counts day-of-week from Sunday as 1: 2 is Monday', () => {
      const expression = CronExpression.parse('0 4 ? * 2 *');
      expect(expression.matchesDay(day('2026-11-23'))).toBe(true); // Monday
      expect(expression.matchesDay(day('2026-11-24'))).toBe(false); // Tuesday
    });

    it('reads L in day-of-month as the last day', () => {
      const expression = CronExpression.parse('0 4 L * ? *');
      expect(expression.matchesDay(day('2027-02-28'))).toBe(true);
      expect(expression.matchesDay(day('2027-02-27'))).toBe(false);
      expect(expression.matchesDay(day('2028-02-28'))).toBe(false);
      expect(expression.matchesDay(day('2028-02-29'))).toBe(true);
    });

    it('reads nW as the weekday nearest day n, never leaving the month', () => {
      // Nov 15 2026 is a Sunday: the Monday after
      expect(CronExpression.parse('0 4 15W * ? *').matchesDay(day('2026-11-16'))).toBe(true);
      expect(CronExpression.parse('0 4 15W * ? *').matchesDay(day('2026-11-15'))).toBe(false);
      // Aug 1 2026 is a Saturday: not the Friday before (July), the Monday after
      expect(DayOfMonthField.nearestWeekday(day('2026-08-01'), 1)).toBe(3);
      // May 31 2026 is a Sunday: not the Monday after (June), the Friday before
      expect(DayOfMonthField.nearestWeekday(day('2026-05-01'), 31)).toBe(29);
      // A weekday stays put
      expect(DayOfMonthField.nearestWeekday(day('2026-11-01'), 18)).toBe(18);
    });

    it('reads nL in day-of-week as the last weekday n of the month, and L alone as Saturday', () => {
      const lastFriday = CronExpression.parse('0 4 ? * 6L *');
      expect(lastFriday.matchesDay(day('2026-11-27'))).toBe(true);
      expect(lastFriday.matchesDay(day('2026-11-20'))).toBe(false);
      expect(CronExpression.parse('0 4 ? * FRIL *').matchesDay(day('2026-11-27'))).toBe(true);
      const saturday = CronExpression.parse('0 4 ? * L *');
      expect(saturday.matchesDay(day('2026-11-21'))).toBe(true);
      expect(saturday.matchesDay(day('2026-11-14'))).toBe(true);
      expect(saturday.matchesDay(day('2026-11-20'))).toBe(false);
    });

    it('reads n#k as the k-th weekday n of the month', () => {
      const secondTuesday = CronExpression.parse('0 4 ? * 3#2 *');
      expect(secondTuesday.matchesDay(day('2026-11-10'))).toBe(true);
      expect(secondTuesday.matchesDay(day('2026-11-03'))).toBe(false);
      expect(secondTuesday.matchesDay(day('2026-11-17'))).toBe(false);
    });

    it('keeps L, W and # whole: never in a list, and only the documented forms', () => {
      expect(refusal('0 4 ? * 3#1,6#3 *').errors[0].message).toMatch(/must be the field's only expression/);
      expect(refusal('0 4 L,15 * ? *').errors[0].message).toMatch(/can't be part of a list/);
      expect(refusal('0 4 LW * ? *').errors[0].message).toMatch(/isn't a form AWS documents/);
      expect(refusal('0 4 L-2 * ? *').errors[0].message).toMatch(/isn't a form AWS documents/);
      expect(refusal('0 4 32W * ? *').errors[0].message).toMatch(/1-31 before W/);
      expect(refusal('0 4 ? * 3#6 *').errors[0].message).toMatch(/1-5 after it/);
    });

    it('matches on month and year too', () => {
      const expression = CronExpression.parse('0 4 1 JAN ? 2030');
      expect(expression.matchesDay(day('2030-01-01'))).toBe(true);
      expect(expression.matchesDay(day('2031-01-01'))).toBe(false);
      expect(expression.matchesDay(day('2030-02-01'))).toBe(false);
    });
  });

  describe('times of day', () => {
    it('lists them in order, and knows a schedule that runs at most once a day', () => {
      expect(CronExpression.parse('30 4 * * ? *').minutesOfDay).toEqual([270]);
      expect(CronExpression.parse('30 4 * * ? *').isAtMostOnceADay()).toBe(true);
      expect(CronExpression.parse('0,30 4,1 * * ? *').minutesOfDay).toEqual([60, 90, 240, 270]);
      expect(CronExpression.parse('0,30 4 * * ? *').isAtMostOnceADay()).toBe(false);
      expect(CronExpression.parse('0 */6 * * ? *').isAtMostOnceADay()).toBe(false);
    });
  });

  it('builds from named fields', () => {
    const expression = CronExpression.of({ minutes: '0', hours: '4', dayOfMonth: '?', month: '*', dayOfWeek: 'MON-FRI', year: '*' });
    expect(expression.toAwsString()).toBe('cron(0 4 ? * MON-FRI *)');
  });
});
