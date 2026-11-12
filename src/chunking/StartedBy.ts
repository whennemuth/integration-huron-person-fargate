/**
 * Who or what started a sync run, carried from the event that starts it to the run's Flags and summary, for Run
 * history's "Started by" (dashboard-overhaul-plan.md, phase 5d):
 * - the dashboard's Bulk sync, with the signed-in user's buPrincipal (none in a public dashboard);
 * - the EventBridge schedule (ScheduledRun.scheduleInput());
 * - the manual Runner (src/runner/).
 * Structured, so a principal can never be mistaken for one of the other kinds. Runs started before this, and the S3
 * file drop, have none.
 *
 * Its path: ApiChunkerEvent.startedBy -> the chunker subscriber's queue message (handleApiEvent()) ->
 * TaskParameters (ChunkFromAPI) -> each chained chunker message (ChunkerQueue.sendNextChunkingMessage()) ->
 * Flags.startedBy (docker/chunker.ts) -> RunSummary.startedBy (AbstractPostProcessor.writeRunSummary()).
 */
export type StartedBy =
  | { by: 'dashboard'; principal?: string }
  | { by: 'schedule' }
  | { by: 'runner' };

export class RunStarter {
  static readonly SCHEDULE: StartedBy = { by: 'schedule' };
  static readonly RUNNER: StartedBy = { by: 'runner' };

  /** A start from the dashboard, by the signed-in user if there is one */
  static dashboard(principal?: string): StartedBy {
    const name = typeof principal === 'string' ? principal.trim() : '';
    return name ? { by: 'dashboard', principal: name } : { by: 'dashboard' };
  }

  /**
   * A StartedBy read back from a message, flags or a summary, or undefined when the value isn't one (absent, or
   * written by something else). Only the known fields are kept.
   */
  static from(value: unknown): StartedBy | undefined {
    if (!value || typeof value !== 'object') {
      return undefined;
    }
    const { by, principal } = value as { by?: unknown; principal?: unknown };
    switch (by) {
      case 'schedule': return RunStarter.SCHEDULE;
      case 'runner': return RunStarter.RUNNER;
      case 'dashboard': return RunStarter.dashboard(typeof principal === 'string' ? principal : undefined);
      default: return undefined;
    }
  }
}
