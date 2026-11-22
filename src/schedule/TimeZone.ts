import { CalendarDay } from './CalendarDay';

/**
 * How a local time on a day resolves to an instant:
 * - normal: it happens once;
 * - skipped: the clocks jump over it (spring), so a schedule doesn't run that day;
 * - repeated: it happens twice (autumn), and a schedule runs once, at the second.
 */
export type WallTimeResolution =
  | { kind: 'normal' | 'repeated'; instant: Date }
  | { kind: 'skipped'; instant?: undefined };

/**
 * A named IANA time zone (e.g. America/New_York), converting between instants and local dates and times with the
 * platform's Intl alone (no date library).
 *
 * Daylight saving follows EventBridge Scheduler ("Daylight savings time on EventBridge Scheduler",
 * https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html, checked 2026-11-19): when the clocks
 * go forward, a time that doesn't exist is skipped and the schedule runs again the next day; when they go back, it
 * runs only once, at the second occurrence. So resolve() gives no instant for the first, and the later instant for
 * the second, and next run times and overlap checks match what AWS will do.
 */
export class TimeZone {
  static readonly UTC = new TimeZone('UTC');

  private readonly partsFormat: Intl.DateTimeFormat;
  private readonly dateTimeFormat: Intl.DateTimeFormat;

  /** @throws RangeError for a name the platform doesn't know */
  constructor(readonly name: string) {
    try {
      this.partsFormat = new Intl.DateTimeFormat('en-US', {
        timeZone: name, hourCycle: 'h23',
        year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric',
      });
    } catch {
      throw new RangeError(`Unknown time zone: ${name}`);
    }
    this.dateTimeFormat = new Intl.DateTimeFormat('en-US', {
      timeZone: name, weekday: 'short', month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit',
    });
  }

  /** The local day and time (minutes after midnight, seconds dropped) of an instant */
  wallTime(instant: Date): { day: CalendarDay; minuteOfDay: number } {
    const parts: Record<string, number> = {};
    for (const { type, value } of this.partsFormat.formatToParts(instant)) {
      if (type !== 'literal') {
        parts[type] = Number(value);
      }
    }
    return { day: CalendarDay.of(parts.year, parts.month, parts.day), minuteOfDay: parts.hour * 60 + parts.minute };
  }

  /** The local day an instant falls on */
  dayOf(instant: Date): CalendarDay {
    return this.wallTime(instant).day;
  }

  /** The zone's offset from UTC at an instant, in minutes (America/New_York in winter: -300) */
  offsetMinutes(instant: Date): number {
    const { day, minuteOfDay } = this.wallTime(instant);
    const asUtc = Date.UTC(day.year, day.month - 1, day.day) + minuteOfDay * 60_000;
    const flooredInstant = Math.floor(instant.getTime() / 60_000) * 60_000;
    return Math.round((asUtc - flooredInstant) / 60_000);
  }

  /** When a local time on a day happens, if it does (see WallTimeResolution) */
  resolve(day: CalendarDay, minuteOfDay: number): WallTimeResolution {
    const asUtc = Date.UTC(day.year, day.month - 1, day.day) + minuteOfDay * 60_000;
    // A zone's offset changes at most once around any one local time: the offsets half a day either side are the
    // only candidates
    const offsets = new Set([-43_200_000, 43_200_000].map(shift => this.offsetMinutes(new Date(asUtc + shift))));
    const instants = [...offsets]
      .map(offset => new Date(asUtc - offset * 60_000))
      .filter(instant => {
        const wall = this.wallTime(instant);
        return wall.day.compareTo(day) === 0 && wall.minuteOfDay === minuteOfDay;
      })
      .sort((a, b) => a.getTime() - b.getTime());

    if (instants.length === 0) {
      return { kind: 'skipped' };
    }
    return { kind: instants.length > 1 ? 'repeated' : 'normal', instant: instants[instants.length - 1] };
  }

  /** "Thu, Nov 20, 2026, 4:00 AM" */
  format(instant: Date): string {
    return this.dateTimeFormat.format(instant);
  }

  /** A time of day, "4:00 AM", from minutes after midnight */
  static formatTimeOfDay(minuteOfDay: number): string {
    const hour = Math.floor(minuteOfDay / 60);
    const minute = String(minuteOfDay % 60).padStart(2, '0');
    return `${hour % 12 === 0 ? 12 : hour % 12}:${minute} ${hour < 12 ? 'AM' : 'PM'}`;
  }
}
