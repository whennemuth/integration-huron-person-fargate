/**
 * Why a schedule, or the choices it was asked for with, can't be accepted. One error type for the whole schedule
 * package (src/schedule/), so a caller such as the dashboard maps a code to a response once: the invalid input codes
 * to 400, the conflicts to 409.
 */
export type ScheduleErrorCode =
  /** The plain choices (time of day, days) are missing or malformed */
  | 'INVALID_CHOICES'
  /** A cron expression doesn't parse, or AWS refused it */
  | 'INVALID_CRON'
  /** A valid cron that runs more often than a bulk sync schedule may (more than once a day) */
  | 'TOO_FREQUENT'
  /** Too close to another landscape's schedule */
  | 'TOO_CLOSE'
  /** The landscape already has its schedule */
  | 'EXISTS'
  /** The landscape has no schedule */
  | 'NOT_FOUND';

/** One problem, naming the field it is about when there is one ('time', 'days', 'hours', 'day-of-week', ...) */
export type ScheduleFieldError = { field?: string; message: string };

export class ScheduleError extends Error {
  readonly errors: ScheduleFieldError[];

  /**
   * @param code What kind of refusal this is
   * @param message The whole refusal, in words fit to show a user
   * @param details The individual problems behind it (the message summarizes them), and anything a caller may show
   *   besides (e.g. the schedule a TOO_CLOSE conflicts with)
   */
  constructor(
    readonly code: ScheduleErrorCode,
    message: string,
    readonly details: { errors?: ScheduleFieldError[]; [name: string]: unknown } = {}
  ) {
    super(message);
    this.name = 'ScheduleError';
    this.errors = details.errors ?? [{ message }];
  }

  /** A refusal that gathers several problems, the message listing them */
  static of(code: ScheduleErrorCode, errors: ScheduleFieldError[], lead: string): ScheduleError {
    const listed = errors.map(({ field, message }) => (field ? `${field}: ${message}` : message)).join('; ');
    return new ScheduleError(code, `${lead}: ${listed}`, { errors });
  }

  static isScheduleError(error: unknown): error is ScheduleError {
    return error instanceof ScheduleError;
  }
}
