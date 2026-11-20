/**
 * Tests local times and instants in a time zone (src/schedule/TimeZone.ts, CalendarDay.ts), including
 * America/New_York's 2027 daylight-saving nights, resolved as EventBridge Scheduler does: a skipped time has no
 * instant, and a repeated time is the second occurrence.
 */
import { CalendarDay } from '../../src/schedule/CalendarDay';
import { TimeZone } from '../../src/schedule/TimeZone';

const newYork = new TimeZone('America/New_York');

describe('TimeZone', () => {
  it('resolves an ordinary local time, in winter and in summer', () => {
    expect(newYork.resolve(CalendarDay.of(2026, 11, 20), 240)).toEqual({ kind: 'normal', instant: new Date('2026-11-20T09:00:00Z') });
    expect(newYork.resolve(CalendarDay.of(2027, 7, 1), 240)).toEqual({ kind: 'normal', instant: new Date('2027-07-01T08:00:00Z') });
    expect(TimeZone.UTC.resolve(CalendarDay.of(2027, 3, 14), 150)).toEqual({ kind: 'normal', instant: new Date('2027-03-14T02:30:00Z') });
  });

  it('skips a time the clocks jump over in spring (Mar 14 2027, 2:00 to 3:00)', () => {
    expect(newYork.resolve(CalendarDay.of(2027, 3, 14), 150)).toEqual({ kind: 'skipped' });
    expect(newYork.resolve(CalendarDay.of(2027, 3, 14), 180)).toEqual({ kind: 'normal', instant: new Date('2027-03-14T07:00:00Z') });
    expect(newYork.resolve(CalendarDay.of(2027, 3, 14), 90)).toEqual({ kind: 'normal', instant: new Date('2027-03-14T06:30:00Z') });
  });

  it('takes the second of a time that happens twice in autumn (Nov 7 2027, 2:00 back to 1:00)', () => {
    // 1:30 EDT is 05:30Z, then 1:30 EST is 06:30Z
    expect(newYork.resolve(CalendarDay.of(2027, 11, 7), 90)).toEqual({ kind: 'repeated', instant: new Date('2027-11-07T06:30:00Z') });
  });

  it('gives the local day and time of an instant, and the offset', () => {
    const { day, minuteOfDay } = newYork.wallTime(new Date('2026-11-20T03:15:40Z'));
    expect(day.toString()).toBe('2026-11-19');
    expect(minuteOfDay).toBe(22 * 60 + 15);
    expect(newYork.offsetMinutes(new Date('2026-11-20T12:00:00Z'))).toBe(-300);
    expect(newYork.offsetMinutes(new Date('2027-07-01T12:00:00Z'))).toBe(-240);
  });

  it('formats instants and times of day', () => {
    expect(newYork.format(new Date('2026-11-20T09:00:00Z'))).toBe('Fri, Nov 20, 2026, 4:00 AM');
    expect(TimeZone.formatTimeOfDay(0)).toBe('12:00 AM');
    expect(TimeZone.formatTimeOfDay(240)).toBe('4:00 AM');
    expect(TimeZone.formatTimeOfDay(750)).toBe('12:30 PM');
    expect(TimeZone.formatTimeOfDay(23 * 60 + 5)).toBe('11:05 PM');
  });

  it('refuses a zone the platform doesn\'t know', () => {
    expect(() => new TimeZone('America/Atlantis')).toThrow(RangeError);
  });
});

describe('CalendarDay', () => {
  it('knows its weekday (1 Sunday to 7 Saturday), its month\'s length, and its neighbors', () => {
    expect(CalendarDay.of(2026, 11, 22).weekday).toBe(1);
    expect(CalendarDay.of(2026, 11, 28).weekday).toBe(7);
    expect(CalendarDay.of(2028, 2, 1).daysInMonth).toBe(29);
    expect(CalendarDay.of(2026, 12, 31).plusDays(1).toString()).toBe('2027-01-01');
    expect(CalendarDay.of(2027, 3, 1).plusDays(-1).toString()).toBe('2027-02-28');
    expect(CalendarDay.of(2027, 3, 14).format()).toBe('Sun, Mar 14, 2027');
  });

  it('refuses a day that isn\'t on the calendar', () => {
    expect(() => CalendarDay.of(2027, 2, 29)).toThrow(RangeError);
  });
});
