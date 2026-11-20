/**
 * Tests a cron expression in a time zone (src/schedule/CronSchedule.ts): next and previous runs (weekends, years,
 * expressions that never run), runs across daylight-saving nights as EventBridge Scheduler makes them, the notes
 * about those nights, the wording, and how close two schedules come (overlapsWith()).
 */
import { CronSchedule } from '../../src/schedule/CronSchedule';
import { ScheduleError } from '../../src/schedule/ScheduleError';

const NY = 'America/New_York';
/** Thursday Nov 19 2026, 7:00 AM in New York */
const thursdayMorning = new Date('2026-11-19T12:00:00Z');
const MINUTE = 60_000;

describe('CronSchedule', () => {
  describe('next and previous runs', () => {
    const weekdays = new CronSchedule('cron(0 4 ? * MON-FRI *)', NY);

    it('finds the next and previous run in the time zone', () => {
      expect(weekdays.getNextExecutionDate(thursdayMorning)).toEqual(new Date('2026-11-20T09:00:00Z'));
      expect(weekdays.getPreviousExecutionDate(thursdayMorning)).toEqual(new Date('2026-11-19T09:00:00Z'));
    });

    it('skips the weekend, and counts strictly after or before', () => {
      expect(weekdays.getNextExecutionDate(new Date('2026-11-20T09:00:00Z'))).toEqual(new Date('2026-11-23T09:00:00Z'));
      expect(weekdays.getPreviousExecutionDate(new Date('2026-11-23T09:00:00Z'))).toEqual(new Date('2026-11-20T09:00:00Z'));
      expect(weekdays.nextExecutions(3, thursdayMorning)).toEqual([
        new Date('2026-11-20T09:00:00Z'), new Date('2026-11-23T09:00:00Z'), new Date('2026-11-24T09:00:00Z'),
      ]);
    });

    it('jumps to the years the expression allows, and finds none outside them', () => {
      const in2030 = new CronSchedule('cron(0 4 1 1 ? 2030)', NY);
      expect(in2030.getNextExecutionDate(thursdayMorning)).toEqual(new Date('2030-01-01T09:00:00Z'));
      expect(in2030.getPreviousExecutionDate(thursdayMorning)).toBeUndefined();
      expect(in2030.getPreviousExecutionDate(new Date('2031-06-01T00:00:00Z'))).toEqual(new Date('2030-01-01T09:00:00Z'));
      expect(new CronSchedule('cron(0 4 1 1 ? 2020)', NY).getNextExecutionDate(thursdayMorning)).toBeUndefined();
    });

    it('finds no run for a day that never comes', () => {
      expect(new CronSchedule('cron(0 4 30 2 ? *)', NY).getNextExecutionDate(thursdayMorning)).toBeUndefined();
    });

    it('lists the runs between two instants', () => {
      const week = [...weekdays.executions(new Date('2026-11-22T00:00:00Z'), new Date('2026-11-29T00:00:00Z'))];
      expect(week.map(run => run.toISOString())).toEqual([
        '2026-11-23T09:00:00.000Z', '2026-11-24T09:00:00.000Z', '2026-11-25T09:00:00.000Z',
        '2026-11-26T09:00:00.000Z', '2026-11-27T09:00:00.000Z',
      ]);
    });

    it('reads a schedule without a zone in UTC, as AWS does', () => {
      expect(new CronSchedule('cron(0 4 * * ? *)').getNextExecutionDate(thursdayMorning)).toEqual(new Date('2026-11-20T04:00:00Z'));
    });
  });

  describe('daylight saving', () => {
    it('has no run on the day the clocks skip its time, and resumes the next day', () => {
      const runs = [...new CronSchedule('cron(30 2 * * ? *)', NY).executions(new Date('2027-03-13T00:00:00Z'), new Date('2027-03-16T00:00:00Z'))];
      expect(runs.map(run => run.toISOString())).toEqual(['2027-03-13T07:30:00.000Z', '2027-03-15T06:30:00.000Z']);
    });

    it('runs once, at the second, on the day its time happens twice', () => {
      const runs = [...new CronSchedule('cron(30 1 * * ? *)', NY).executions(new Date('2027-11-07T00:00:00Z'), new Date('2027-11-08T00:00:00Z'))];
      expect(runs.map(run => run.toISOString())).toEqual(['2027-11-07T06:30:00.000Z']);
    });

    it('says so in notes, and has none for times clear of the changes', () => {
      const from = new Date('2027-01-01T12:00:00Z');
      expect(new CronSchedule('cron(30 2 * * ? *)', NY).daylightSavingNotes(from)).toEqual([
        'On Sun, Mar 14, 2027 the clocks go forward past 2:30 AM (America/New_York), so there is no run that day.',
      ]);
      expect(new CronSchedule('cron(30 1 * * ? *)', NY).daylightSavingNotes(from)).toEqual([
        'On Sun, Nov 7, 2027 1:30 AM (America/New_York) happens twice as the clocks go back; it runs once, at the second.',
      ]);
      expect(new CronSchedule('cron(30 2 ? * MON-FRI *)', NY).daylightSavingNotes(from)).toEqual([]);
      expect(new CronSchedule('cron(0 4 * * ? *)', NY).daylightSavingNotes(from)).toEqual([]);
      expect(new CronSchedule('cron(30 2 * * ? *)').daylightSavingNotes(from)).toEqual([]);
    });
  });

  it('describes itself in words, with its time zone', () => {
    expect(new CronSchedule('cron(0 4 ? * MON-FRI *)', NY).describe()).toBe('At 04:00 AM, Monday through Friday (America/New_York)');
    expect(new CronSchedule('cron(0 4 ? * 2 *)', NY).describe()).toBe('At 04:00 AM, only on Monday (America/New_York)');
    expect(new CronSchedule('cron(15 10 ? * 6L 2027)', NY).describe()).toBe('At 10:15 AM, on the last Friday of the month, only in 2027 (America/New_York)');
  });

  it('gives its expression in both forms, and knows whether it runs at most once a day', () => {
    const schedule = new CronSchedule('0 4 ? * MON-FRI *', NY);
    expect(schedule.getExpression()).toBe('0 4 ? * MON-FRI *');
    expect(schedule.getAwsExpression()).toBe('cron(0 4 ? * MON-FRI *)');
    expect(schedule.isAtMostOnceADay()).toBe(true);
    expect(new CronSchedule('cron(0 4,16 * * ? *)', NY).isAtMostOnceADay()).toBe(false);
  });

  it('refuses an expression or a time zone that isn\'t one', () => {
    expect(() => new CronSchedule('cron(0 25 * * ? *)', NY)).toThrow(ScheduleError);
    expect(() => new CronSchedule('cron(0 4 * * ? *)', 'Mars/Olympus_Mons')).toThrow(RangeError);
  });

  describe('overlapsWith', () => {
    const at = (expression: string) => new CronSchedule(expression, NY);
    const hour = 60 * MINUTE;

    it('overlaps a schedule starting less than the buffer after it, by how much', () => {
      const details = at('cron(0 4 ? * MON-FRI *)').overlapsWith(at('cron(59 4 * * ? *)'), hour, thursdayMorning);
      expect(details).toMatchObject({ overlaps: true, gap: 59 * MINUTE, overlapsBy: MINUTE });
      expect(details.at).toEqual(new Date('2026-11-20T09:00:00Z'));
      expect(details.otherAt).toEqual(new Date('2026-11-20T09:59:00Z'));
    });

    it("doesn't overlap one exactly the buffer away", () => {
      const details = at('cron(0 4 ? * MON-FRI *)').overlapsWith(at('cron(0 5 * * ? *)'), hour, thursdayMorning);
      expect(details).toMatchObject({ overlaps: false, gap: hour });
      expect(details.overlapsBy).toBeUndefined();
    });

    it('compares across midnight', () => {
      const details = at('cron(30 23 ? * MON-FRI *)').overlapsWith(at('cron(15 0 * * ? *)'), hour, thursdayMorning);
      expect(details).toMatchObject({ overlaps: true, gap: 45 * MINUTE, overlapsBy: 15 * MINUTE });
    });

    it('takes the days into account: weekdays and weekends at the same time are a day apart', () => {
      const details = at('cron(0 4 ? * MON-FRI *)').overlapsWith(at('cron(0 4 ? * SUN,SAT *)'), hour, thursdayMorning);
      expect(details).toMatchObject({ overlaps: false, gap: 24 * hour });
    });

    it('finds the one day a month two schedules come close', () => {
      const details = at('cron(0 4 ? * 6L *)').overlapsWith(at('cron(30 4 ? * FRI *)'), hour, thursdayMorning);
      expect(details).toMatchObject({ overlaps: true, gap: 30 * MINUTE });
      expect(details.at).toEqual(new Date('2026-11-27T09:00:00Z'));
    });

    it('is symmetric', () => {
      const one = at('cron(0 4 ? * MON-FRI *)');
      const other = at('cron(30 3 * * ? *)');
      expect(one.overlapsWith(other, hour, thursdayMorning).gap).toBe(other.overlapsWith(one, hour, thursdayMorning).gap);
    });

    it('has nothing to compare when one never runs in the period', () => {
      expect(at('cron(0 4 * * ? *)').overlapsWith(at('cron(0 4 1 1 ? 2035)'), hour, thursdayMorning)).toEqual({ overlaps: false });
    });

    it('refuses to compare a schedule that runs too often', () => {
      expect(() => at('cron(* * * * ? *)').overlapsWith(at('cron(0 4 * * ? *)'), hour, thursdayMorning))
        .toThrow(expect.objectContaining({ code: 'TOO_FREQUENT' }));
    });
  });
});
