import type { RunReport } from './RunReporting';

/** The newest run's state, as far as starting another is concerned */
export type RunActivityState = {
  /** none (no runs yet), finished (summarized), failed, underway, stalled (no chunk finished for too long) */
  kind: 'none' | 'finished' | 'failed' | 'underway' | 'stalled';
  syncRunId?: string;
  /** For underway or stalled: when its last chunk finished, if any has */
  lastChunkAt?: string;
};

/**
 * The newest run's state, as far as starting another is concerned: underway, or stalled, or neither. The stall
 * rule is the dashboard's (RunView.estimate() in public/js/run-history.js, phase 5a): no chunk has finished for
 * more than STALL_FACTOR times the run's average gap between chunks, and at least MIN_STALL_MS. A run with no
 * chunk finished yet is underway for FIRST_CHUNK_GRACE_MS after it started (the chunker's first fetches), then
 * stalled. Keep the constants in step with run-history.js.
 */
export class RunActivity {
  static readonly STALL_FACTOR = 10;
  static readonly MIN_STALL_MS = 15 * 60_000;
  static readonly FIRST_CHUNK_GRACE_MS = 60 * 60_000;

  static of(report: RunReport | { syncRunId: string; status: 'UNKNOWN' } | undefined, now: number): RunActivityState {
    if (!report) {
      return { kind: 'none' };
    }
    const { syncRunId } = report;
    const full = report as RunReport;
    if (full.summary) {
      return { kind: 'finished', syncRunId };
    }
    if (report.status === 'FAILED') {
      return { kind: 'failed', syncRunId };
    }
    if (report.status === 'UNKNOWN') {
      // Listed, but nothing recorded to report on: nothing is running
      return { kind: 'finished', syncRunId };
    }
    const started = Date.parse(full.metadata?.createdAt ?? syncRunId);
    const lastChunkAt = full.progress?.lastChunkAt;
    const last = lastChunkAt ? Date.parse(lastChunkAt) : NaN;
    const chunks = full.progress?.chunksReported ?? 0;
    if (!Number.isFinite(last) || chunks === 0) {
      const stalled = Number.isFinite(started) && now - started > RunActivity.FIRST_CHUNK_GRACE_MS;
      return { kind: stalled ? 'stalled' : 'underway', syncRunId };
    }
    const averageGap = Number.isFinite(started) && last > started ? (last - started) / chunks : 0;
    const stalled = now - last > Math.max(RunActivity.MIN_STALL_MS, RunActivity.STALL_FACTOR * averageGap);
    return { kind: stalled ? 'stalled' : 'underway', syncRunId, lastChunkAt };
  }
}
