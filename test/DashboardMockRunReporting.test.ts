/**
 * Tests the dashboard's mock run reporting (dashboard/src/handlers/MockRunReporting.ts): that its runs cover
 * every state the run reporting screens show, that what it records for a run is self-consistent, and that it
 * pages and fails the way the real RunReporting does.
 */
import { MOCK_RUN_COUNT, MockRunReportingService, NEWEST_MOCK_RUN, OffsetPager } from '../dashboard/src/handlers/MockRunReporting';
import { ChangeType, ReportingError, RunReport } from '../src/reporting/RunReporting';

const service = new MockRunReportingService('dynamodb');

const allReports = async (reporting = service): Promise<RunReport[]> => {
  const ids = await reporting.listRuns();
  return Promise.all(ids.map(async id => (await reporting.getRunReport(id))!));
};

/** Every item of a paged listing, following its cursors */
const drain = async <T>(fetch: (cursor?: string) => Promise<{ items: T[]; cursor?: string }>): Promise<{ items: T[]; pages: number }> => {
  const items: T[] = [];
  let cursor: string | undefined;
  let pages = 0;
  do {
    const page = await fetch(cursor);
    items.push(...page.items);
    cursor = page.cursor;
    pages++;
  } while (cursor);
  return { items, pages };
};

const expectReportingError = async (promise: Promise<unknown>, code: string) => {
  await expect(promise).rejects.toBeInstanceOf(ReportingError);
  await expect(promise).rejects.toMatchObject({ code });
};

describe('MockRunReportingService', () => {
  it('lists its runs oldest first, ending with the newest', async () => {
    const ids = await service.listRuns();
    expect(ids).toHaveLength(MOCK_RUN_COUNT);
    expect([...ids].sort()).toEqual(ids);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids[ids.length - 1]).toBe(NEWEST_MOCK_RUN);
    expect(await service.getRunReport('2020-01-01T00:00:00.000Z')).toBeUndefined();
  });

  it('is deterministic', async () => {
    expect(await allReports(new MockRunReportingService('dynamodb'))).toEqual(await allReports());
  });

  it('covers every run status and kind of run the screens show', async () => {
    const reports = await allReports();
    expect(new Set(reports.map(report => report.status))).toEqual(new Set(['SUCCEEDED', 'FAILED', 'INCOMPLETE', 'POST_PROCESSING_FAILED']));
    // Failed both with a summary (post-processed, deletes skipped) and without (a terminal error in chunking)
    expect(reports.some(report => report.status === 'FAILED' && report.summary)).toBe(true);
    expect(reports.some(report => report.status === 'FAILED' && !report.summary && report.terminalError)).toBe(true);
    const summaries = reports.flatMap(report => report.summary ? [report.summary] : []);
    expect(summaries.some(summary => summary.dryRun)).toBe(true);
    expect(summaries.some(summary => summary.bulkReset)).toBe(true);
    expect(summaries.some(summary => summary.totalRecords === 1)).toBe(true);
    expect(new Set(summaries.map(summary => summary.syncPopulation))).toEqual(new Set(['person-full', 'person-delta']));
    expect(new Set(summaries.map(summary => summary.deletion?.status))).toEqual(new Set(['COMPLETED', 'SKIPPED', 'FAILED']));
  });

  it('records chunk statistics, errors and changes that agree with each run\'s summary', async () => {
    for (const { syncRunId, summary } of await allReports()) {
      if (!summary) continue;
      const chunks = await service.getRunChunkStatistics(syncRunId);
      expect(chunks).toHaveLength(summary.chunksReported);
      const sum = (field: string) => chunks.reduce((total, chunk: any) => total + (chunk[field] ?? 0), 0);
      expect(summary.results).toMatchObject({
        processed: sum('totalRecords'), succeeded: sum('successCount'), failed: sum('failureCount'),
        added: sum('addedCount'), updated: sum('updatedCount')
      });
      expect(summary.totalErrors).toBe(sum('totalErrors'));

      const { items: errors } = await drain(cursor => service.getRunErrors(syncRunId, { cursor, limit: 500 }));
      expect(errors).toHaveLength(summary.totalErrors);
      for (const [status, count] of Object.entries(summary.errorsByStatus)) {
        expect(errors.filter(error => error.statusCode === Number(status))).toHaveLength(count);
      }

      const changes = async (changeType: ChangeType) =>
        (await drain(cursor => service.getRunChanges(syncRunId, changeType, { cursor, limit: 500 }))).items.length;
      const expected = summary.dryRun ? [0, 0, 0] : [summary.results.added, summary.results.updated,
        summary.deletion?.status === 'COMPLETED' ? summary.deletion.deletedCount : 0];
      expect([await changes('NEW'), await changes('UPDATED'), await changes('DELETED')]).toEqual(expected);
    }
  });

  it('gives each run without a summary its progress from the chunks reported so far, and summarized runs none', async () => {
    const reports = await allReports();
    for (const { syncRunId, summary, progress, metadata } of reports) {
      if (summary) {
        expect(progress).toBeUndefined();
        continue;
      }
      const chunks = await service.getRunChunkStatistics(syncRunId);
      const sum = (field: string) => chunks.reduce((total, chunk: any) => total + (chunk[field] ?? 0), 0);
      expect(progress).toMatchObject({
        chunksReported: chunks.length,
        results: { processed: sum('totalRecords'), added: sum('addedCount'), updated: sum('updatedCount') },
        totalErrors: sum('totalErrors')
      });
      if (chunks.length > 0) {
        expect(progress!.lastChunkAt).toBe(chunks.map(chunk => chunk.endTimestamp).sort().at(-1));
        // An unfinished run has reported only some of its chunks
        expect(progress!.chunksReported).toBeLessThan(metadata!.chunkCount);
      }
    }
    // An incomplete run with figures so far, and a run that failed in chunking with none
    expect(reports.some(report => report.status === 'INCOMPLETE' && report.progress!.chunksReported > 0)).toBe(true);
    expect(reports.some(report => report.terminalError && report.progress!.chunksReported === 0)).toBe(true);
  });

  it('pages the newest run\'s errors, newest pages following the cursor, filtered by status code', async () => {
    const all = await drain(cursor => service.getRunErrors(NEWEST_MOCK_RUN, { cursor }));
    expect(all.items).toHaveLength(260);
    expect(all.pages).toBe(3);
    // In sort key order: by status code, then time
    const keys = all.items.map(error => error.eventType);
    expect([...keys].sort()).toEqual(keys);

    const throttles = await drain(cursor => service.getRunErrors(NEWEST_MOCK_RUN, { statusCode: 429, limit: 50, cursor }));
    expect(throttles.items).toHaveLength(120);
    expect(throttles.pages).toBe(3);
    expect(throttles.items.every(error => error.statusCode === 429 && error.isThrottled)).toBe(true);
  });

  it('pages the newest run\'s changes', async () => {
    const updated = await drain(cursor => service.getRunChanges(NEWEST_MOCK_RUN, 'UPDATED', { cursor }));
    expect(updated.items).toHaveLength(240);
    expect(updated.pages).toBe(3);
    expect(updated.items.every(change => change.changeType === 'UPDATED' && change.previousHash)).toBe(true);
  });

  it('refuses a cursor from another listing, a malformed cursor, and an out-of-range limit', async () => {
    const { cursor } = await service.getRunErrors(NEWEST_MOCK_RUN, { statusCode: 429, limit: 10 });
    await expectReportingError(service.getRunErrors(NEWEST_MOCK_RUN, { statusCode: 500, cursor }), 'INVALID_CURSOR');
    await expectReportingError(service.getRunChanges(NEWEST_MOCK_RUN, 'NEW', { cursor }), 'INVALID_CURSOR');
    await expectReportingError(service.getRunErrors(NEWEST_MOCK_RUN, { cursor: 'not-a-cursor' }), 'INVALID_CURSOR');
    await expectReportingError(service.getRunErrors(NEWEST_MOCK_RUN, { limit: 0 }), 'INVALID_LIMIT');
    await expectReportingError(service.getRunErrors(NEWEST_MOCK_RUN, { limit: 501 }), 'INVALID_LIMIT');
  });

  describe('in S3 storage mode', () => {
    const s3 = new MockRunReportingService('s3');

    it('reports S3 mode, without change lists', async () => {
      expect(s3.getCapabilities()).toEqual({ landscape: 'local', storageMode: 's3', changesSupported: false });
      expect((await s3.getRunReport(NEWEST_MOCK_RUN))?.summary?.storageMode).toBe('s3');
      await expectReportingError(s3.getRunChanges(NEWEST_MOCK_RUN, 'NEW'), 'UNSUPPORTED_IN_STORAGE_MODE');
    });

    it('is selected by MOCK_STORAGE_MODE, which must name a storage mode', () => {
      const saved = process.env.MOCK_STORAGE_MODE;
      try {
        process.env.MOCK_STORAGE_MODE = 'S3';
        expect(new MockRunReportingService().getCapabilities().storageMode).toBe('s3');
        delete process.env.MOCK_STORAGE_MODE;
        expect(new MockRunReportingService().getCapabilities()).toMatchObject({ storageMode: 'dynamodb', changesSupported: true });
        expect(() => new MockRunReportingService('file')).toThrow("MOCK_STORAGE_MODE must be one of dynamodb, s3 (got 'file')");
      } finally {
        if (saved === undefined) delete process.env.MOCK_STORAGE_MODE; else process.env.MOCK_STORAGE_MODE = saved;
      }
    });
  });
});

describe('OffsetPager', () => {
  const items = Array.from({ length: 25 }, (_, n) => n);

  it('pages with a cursor until the last page, which has none', () => {
    const pager = new OffsetPager({ syncRunId: 'run' });
    const first = pager.page(items, { limit: 10 });
    expect(first.items).toEqual(items.slice(0, 10));
    const third = pager.page(items, { limit: 10, cursor: pager.page(items, { limit: 10, cursor: first.cursor }).cursor });
    expect(third).toEqual({ items: items.slice(20) });
    expect(pager.page([], {})).toEqual({ items: [] });
  });
});
