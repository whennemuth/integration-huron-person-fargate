import { RunReport } from '../../../src/reporting/RunReporting';
import { RunReportingService } from '../services/ServiceTypes';

export const RUN_LIST_DEFAULT_LIMIT = 20;
export const RUN_LIST_MAX_LIMIT = 50;

/**
 * A run in the list: its report, or, for a run that is listed but has nothing recorded to report on (no
 * summary, metadata, flags or terminal error), just its id with status UNKNOWN.
 */
export type RunListItem = RunReport | { syncRunId: string; status: 'UNKNOWN' };

/** One page of the run list, newest first. `before` continues it, and is present only when older runs remain */
export type RunListPage = { items: RunListItem[]; before?: string };

/**
 * Pages through a landscape's runs newest first, each with its report, so a page of the run history costs
 * one request rather than one per run (GET /api/runs?include=report). RunReportingService reads the page's
 * syncRunIds by key (the `limit` newest older than `before`: one Query of the run-list-index), and the
 * page's reports are read in parallel.
 */
export class RunList {
  constructor(private readonly reporting: RunReportingService) {}

  async page(params: { limit?: number; before?: string } = {}): Promise<RunListPage> {
    const { limit = RUN_LIST_DEFAULT_LIMIT, before } = params;
    if (!Number.isInteger(limit) || limit < 1 || limit > RUN_LIST_MAX_LIMIT) {
      throw new RangeError(`limit must be an integer from 1 to ${RUN_LIST_MAX_LIMIT}`);
    }
    const { syncRunIds, hasOlder } = await this.reporting.listRunPage({ limit, before });
    const items = await Promise.all(syncRunIds.map(async (syncRunId): Promise<RunListItem> =>
      (await this.reporting.getRunReport(syncRunId)) ?? { syncRunId, status: 'UNKNOWN' }
    ));
    return {
      items,
      ...(hasOlder && syncRunIds.length > 0 ? { before: syncRunIds[syncRunIds.length - 1] } : {})
    };
  }
}
