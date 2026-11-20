/**
 * Tests turning the dashboard page's plain choices into an AWS cron expression and back
 * (src/schedule/ScheduleChoices.ts, CronExpressionBuilder.ts): the parser's normalizing and refusals, what the
 * builder writes, and toChoices() reading any way of writing a time on days of the week as simple choices.
 */
import { CronExpressionBuilder } from '../../src/schedule/CronExpressionBuilder';
import { ScheduleChoices, ScheduleChoicesParser } from '../../src/schedule/ScheduleChoices';
import { ScheduleError } from '../../src/schedule/ScheduleError';

const thrown = (action: () => unknown): ScheduleError => {
  try {
    action();
  } catch (error) {
    if (error instanceof ScheduleError) {
      return error;
    }
    throw error;
  }
  throw new Error('nothing was thrown');
};

describe('ScheduleChoicesParser', () => {
  it('reads simple choices, normalizing the time and the chosen days', () => {
    expect(ScheduleChoicesParser.parse({ mode: 'simple', time: '4:00', days: 'weekdays' }))
      .toEqual({ mode: 'simple', time: '04:00', days: 'weekdays' });
    expect(ScheduleChoicesParser.parse({ mode: 'simple', time: '23:30', days: ['fri', 'MON', 'mon'] }))
      .toEqual({ mode: 'simple', time: '23:30', days: ['MON', 'FRI'] });
  });

  it('reads a cron expression, trimmed', () => {
    expect(ScheduleChoicesParser.parse({ mode: 'cron', expression: ' cron(0 4 * * ? *) ' }))
      .toEqual({ mode: 'cron', expression: 'cron(0 4 * * ? *)' });
  });

  it('names every problem with simple choices', () => {
    const error = thrown(() => ScheduleChoicesParser.parse({ mode: 'simple', time: '24:00', days: ['MON', 'FUNDAY'] }));
    expect(error.code).toBe('INVALID_CHOICES');
    expect(error.errors.map(({ field }) => field)).toEqual(['time', 'days']);
    expect(thrown(() => ScheduleChoicesParser.parse({ mode: 'simple', time: '04:00', days: [] })).errors[0].field).toBe('days');
    expect(thrown(() => ScheduleChoicesParser.parse({ mode: 'simple', days: 'every-day' })).errors[0].field).toBe('time');
  });

  it('refuses an unknown mode, an empty cron expression and a missing body', () => {
    expect(thrown(() => ScheduleChoicesParser.parse({ mode: 'weekly' })).errors[0].field).toBe('mode');
    expect(thrown(() => ScheduleChoicesParser.parse({ mode: 'cron', expression: '  ' })).errors[0].field).toBe('expression');
    expect(thrown(() => ScheduleChoicesParser.parse(undefined)).code).toBe('INVALID_CHOICES');
  });
});

describe('CronExpressionBuilder', () => {
  it('writes every day in AWS\'s own form, and days of the week by name', () => {
    expect(new CronExpressionBuilder().at(4, 0).everyDay().build().toAwsString()).toBe('cron(0 4 * * ? *)');
    expect(new CronExpressionBuilder().at(4, 30).onWeekdays().build().toAwsString()).toBe('cron(30 4 ? * MON-FRI *)');
    expect(new CronExpressionBuilder().at(0).onWeekends().build().toAwsString()).toBe('cron(0 0 ? * SUN,SAT *)');
    expect(new CronExpressionBuilder().atTime('23:05').on(['WED', 'MON']).build().toAwsString()).toBe('cron(5 23 ? * MON,WED *)');
  });

  it('writes all seven chosen days as every day', () => {
    const all = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'] as const;
    expect(new CronExpressionBuilder().at(4).on(all).build().toAwsString()).toBe('cron(0 4 * * ? *)');
  });

  it('refuses a time that isn\'t one, no time, and no days', () => {
    expect(thrown(() => new CronExpressionBuilder().at(24, 0)).code).toBe('INVALID_CHOICES');
    expect(thrown(() => new CronExpressionBuilder().atTime('noon')).code).toBe('INVALID_CHOICES');
    expect(thrown(() => new CronExpressionBuilder().everyDay().build()).errors[0].field).toBe('time');
    expect(thrown(() => new CronExpressionBuilder().on([])).errors[0].field).toBe('days');
  });

  describe('fromChoices', () => {
    it('builds simple choices and parses a typed expression', () => {
      expect(CronExpressionBuilder.fromChoices({ mode: 'simple', time: '04:00', days: 'weekdays' }).toAwsString())
        .toBe('cron(0 4 ? * MON-FRI *)');
      expect(CronExpressionBuilder.fromChoices({ mode: 'cron', expression: '0 4 L * ? *' }).toAwsString())
        .toBe('cron(0 4 L * ? *)');
    });

    it('passes on a typed expression\'s field problems', () => {
      const error = thrown(() => CronExpressionBuilder.fromChoices({ mode: 'cron', expression: 'cron(0 25 * * ? *)' }));
      expect(error.code).toBe('INVALID_CRON');
      expect(error.errors[0].field).toBe('hours');
    });
  });

  describe('toChoices', () => {
    it('reads a time on days of the week as simple choices, however the days are written', () => {
      const cases: [string, ScheduleChoices][] = [
        ['cron(0 4 * * ? *)', { mode: 'simple', time: '04:00', days: 'every-day' }],
        ['cron(0 4 ? * * *)', { mode: 'simple', time: '04:00', days: 'every-day' }],
        ['cron(30 23 ? * 2-6 *)', { mode: 'simple', time: '23:30', days: 'weekdays' }],
        ['cron(0 4 ? * 1,7 *)', { mode: 'simple', time: '04:00', days: 'weekends' }],
        ['cron(5 0 ? * WED,MON *)', { mode: 'simple', time: '00:05', days: ['MON', 'WED'] }],
      ];
      for (const [expression, choices] of cases) {
        expect(CronExpressionBuilder.toChoices(expression)).toEqual(choices);
      }
    });

    it('leaves anything else as the cron expression', () => {
      for (const expression of [
        'cron(0 4 L * ? *)', 'cron(0 0/6 * * ? *)', 'cron(0 4 ? * 6L *)', 'cron(0 4 * JAN ? *)',
        'cron(0 4 * * ? 2027)', 'cron(0 4 1 * ? *)', 'cron(0,30 4 * * ? *)',
      ]) {
        expect(CronExpressionBuilder.toChoices(expression)).toEqual({ mode: 'cron', expression });
      }
    });

    it('gives back the choices an expression was built from', () => {
      const choices: ScheduleChoices[] = [
        { mode: 'simple', time: '04:00', days: 'every-day' },
        { mode: 'simple', time: '04:00', days: 'weekdays' },
        { mode: 'simple', time: '04:00', days: 'weekends' },
        { mode: 'simple', time: '13:45', days: ['TUE', 'THU', 'SAT'] },
      ];
      for (const choice of choices) {
        expect(CronExpressionBuilder.toChoices(CronExpressionBuilder.fromChoices(choice))).toEqual(choice);
      }
    });
  });
});
