/**
 * Tests the Run history page's script (dashboard/public/js/run-history.js): how a run's report reads on the
 * page (RunFormat, RunView), the run reporting routes as the page calls them (RunsApi, CursorPager), and the
 * rows it builds. The script is browser code with no module system, so it is run in a vm context, as written
 * and as the Lambda build minifies it.
 *
 * RunsApi is driven through the dashboard handler with the mock services (LOCALHOST_WITH_MOCKS), so the page's
 * requests are checked against the routes and the mock run history they will meet under `dashboard:dev -- --mocks`.
 */
import { readFileSync } from 'fs';
import { join } from 'path';
import { Script, createContext } from 'vm';
import { minifyJS } from '../dashboard/scripts/BuildTemplates';
import { handler } from '../dashboard/src/handlers/DashboardHandler';
import { LambdaFunctionUrlEvent } from '../dashboard/src/handlers/DashboardTypes';
import { MOCK_RUN_COUNT, NEWEST_MOCK_RUN } from '../dashboard/src/handlers/MockRunReporting';

const JS_DIR = join(__dirname, '../dashboard/public/js');
const CLASSES = ['Html', 'RunFormat', 'RunView', 'RunsApi', 'CursorPager', 'PagedTable', 'RunListTable', 'RunDetailView', 'RunHistoryPage'];

/** Load run-history.js as the browser would, after page-state.js (Html, PageState), returning its classes */
const loadRunHistory = (minified = false): Record<string, any> => {
  const context = createContext({ URL, URLSearchParams, console });
  const source = ['page-state.js', 'run-history.js'].map(file => readFileSync(join(JS_DIR, file), 'utf-8')).join('\n');
  const exports = CLASSES.map(name => `this.${name} = ${name};`).join('\n');
  new Script(`${minified ? minifyJS(source).minified : source}\n${exports}`).runInContext(context);
  return context as any;
};

/**
 * dashboard.js's fetchSectionData, over the handler instead of fetch: the route's JSON, or an Error carrying
 * the server's message and the response status
 */
const getJson = async (path: string): Promise<any> => {
  const url = new URL(path, 'https://dashboard.example.com');
  const event = {
    rawPath: url.pathname,
    queryStringParameters: Object.fromEntries(url.searchParams),
    headers: {},
    requestContext: { http: { method: 'GET', path: url.pathname } },
    isBase64Encoded: false
  } as unknown as LambdaFunctionUrlEvent;
  const result = await handler(event);
  const data = result.body ? JSON.parse(`${result.body}`) : {};
  if (result.statusCode !== 200) {
    throw Object.assign(new Error(data.message || data.error), { status: result.statusCode });
  }
  return data;
};

const NOW = Date.parse('2026-11-06T02:30:00.000Z');

/** A full sync underway: 6 of its 15 chunks processed, the last 9 minutes before NOW */
const incomplete = {
  syncRunId: '2026-11-06T02:00:00.000Z',
  status: 'INCOMPLETE',
  metadata: { createdAt: '2026-11-06T02:00:00.000Z', syncPopulation: 'person-full', chunkCount: 15, totalRecords: 14500 },
  progress: {
    chunksReported: 6,
    results: { processed: 6000, succeeded: 5990, failed: 10, skipped: 0, added: 40, updated: 5900, removed: 3 },
    totalErrors: 10, throttleCount: 4, errorsByStatus: { 429: 4, 500: 6 },
    lastChunkAt: '2026-11-06T02:21:00.000Z'
  }
};

describe.each([false, true])('run-history.js (minified: %s)', minified => {
  const page = loadRunHistory(minified);
  const { RunFormat, RunView, RunsApi, CursorPager, RunListTable, RunDetailView, Html } = page;

  describe('RunFormat', () => {
    it('reads durations in the largest unit that reads well', () => {
      expect(RunFormat.duration(45_000)).toBe('45 s');
      expect(RunFormat.duration(47 * 60_000)).toBe('47 min');
      expect(RunFormat.duration(125 * 60_000)).toBe('2 h 05 min');
      expect(RunFormat.duration(undefined)).toBe(RunFormat.NONE);
      expect(RunFormat.duration(-1)).toBe(RunFormat.NONE);
    });

    it('labels statuses, populations, storage modes and HTTP codes', () => {
      expect(RunFormat.status('POST_PROCESSING_FAILED')).toEqual({ label: 'Post-processing failed', tone: 'danger' });
      expect(RunFormat.status('INCOMPLETE').tone).toBe('warning');
      expect(RunFormat.status('SOMETHING_NEW')).toEqual({ label: 'SOMETHING_NEW', tone: null });
      expect(RunFormat.population('person-full')).toBe('Full sync');
      expect(RunFormat.storageMode('dynamodb')).toBe('DynamoDB');
      expect(RunFormat.httpStatus(429)).toBe('429 Too Many Requests');
      expect(RunFormat.httpStatus(418)).toBe('418');
    });

    it('says how long ago a time was', () => {
      const now = Date.parse('2026-11-06T12:00:00.000Z');
      expect(RunFormat.ago('2026-11-06T11:59:30.000Z', now)).toBe('just now');
      expect(RunFormat.ago('2026-11-06T11:15:00.000Z', now)).toBe('45 min ago');
      expect(RunFormat.ago('2026-11-06T02:00:00.000Z', now)).toBe('10 h ago');
      expect(RunFormat.ago('2026-11-05T11:00:00.000Z', now)).toBe('1 day ago');
      expect(RunFormat.ago('2026-11-03T12:00:00.000Z', now)).toBe('3 days ago');
      expect(RunFormat.ago('not a time', now)).toBe(RunFormat.NONE);
    });

    it('shows a dash for a missing figure, never NaN or 0', () => {
      expect(RunFormat.number(undefined)).toBe(RunFormat.NONE);
      expect(RunFormat.number('abc')).toBe(RunFormat.NONE);
      expect(RunFormat.number(0)).toBe('0');
      expect(RunFormat.number(14500)).toBe((14500).toLocaleString());
    });
  });

  describe('RunView', () => {
    it('reads a summarized run: figures, duration, and deactivations from the deletion step', () => {
      const run = new RunView({
        syncRunId: '2026-11-04T02:00:00.000Z',
        status: 'SUCCEEDED',
        summary: {
          syncRunId: '2026-11-04T02:00:00.000Z', status: 'SUCCEEDED', storageMode: 'dynamodb', dryRun: false,
          createdAt: '2026-11-04T02:00:00.000Z', completedAt: '2026-11-04T02:47:00.000Z',
          syncPopulation: 'person-full', bulkReset: true, chunkCount: 15, totalRecords: 14500, chunksReported: 15,
          results: { processed: 14500, succeeded: 14360, failed: 140, skipped: 0, added: 130, updated: 240, removed: 0 },
          totalErrors: 260, throttleCount: 120, errorsByStatus: { 429: 120 },
          deletion: { status: 'COMPLETED', deletedCount: 12, failedCount: 0, totalProcessed: 12, message: '' }
        }
      });
      expect(run.durationMs).toBe(47 * 60_000);
      expect(run.populationLabel).toBe('Full sync');
      expect(run.tags).toEqual(['Bulk reset']);
      expect(run.facts).toEqual(['Full sync', 'DynamoDB storage', '47 min', 'Bulk reset']);
      expect(run.deactivated).toBe(12);
      expect([run.changeCount('NEW'), run.changeCount('UPDATED'), run.changeCount('DELETED')]).toEqual([130, 240, 12]);
    });

    it('reads a run that failed before post-processing from its metadata, flags and terminal error', () => {
      const run = new RunView({
        syncRunId: '2026-10-30T02:00:00.000Z',
        status: 'FAILED',
        metadata: { createdAt: '2026-10-30T02:00:00.000Z', syncPopulation: 'person-delta', totalRecords: 3000 },
        flags: { syncPopulation: 'person-delta', runFailed: true, runFailureMessage: 'from the flags' },
        terminalError: { stage: 'chunking', errorMessage: 'Source API returned 503', errorTimestamp: '2026-10-30T02:04:00.000Z' }
      });
      expect(run.hasSummary).toBe(false);
      expect(run.records).toBe(3000);
      expect(run.durationMs).toBeUndefined();
      expect(run.added).toBeUndefined();
      expect(run.deactivated).toBeUndefined();
      expect(run.failureMessage).toBe('Source API returned 503');
      expect(run.failedAt).toBe('2026-10-30T02:04:00.000Z');
    });

    it("reads an unfinished run's figures so far from its progress, its duration to the last chunk", () => {
      const run = new RunView(incomplete);
      expect(run.hasSummary).toBe(false);
      expect(run.soFar).toBe(true);
      expect([run.processed, run.succeeded, run.added, run.updated, run.deactivated, run.errors, run.throttled])
        .toEqual([6000, 5990, 40, 5900, 3, 10, 4]);
      expect(run.errorsByStatus).toEqual({ 429: 4, 500: 6 });
      expect(run.chunksReported).toBe(6);
      expect(run.durationMs).toBe(21 * 60_000);
      expect(run.facts).toEqual(['Full sync', '21 min so far']);
      expect(run.deactivationNote).toBe('Post-processing adds deletions');
      // No end-of-chunking flag in its metadata, so it is still fetching: no percent or estimate yet
      expect(run.progressText(NOW)).toBe('6 chunks processed · last chunk finished 9 min ago · still fetching: no percent or estimate until every record is fetched');
    });

    it('says no chunk has finished, and gives no duration, for an unfinished run with none processed', () => {
      const run = new RunView({ ...incomplete, metadata: { ...incomplete.metadata, chunkCount: undefined },
        progress: { chunksReported: 0, results: {}, totalErrors: 0, throttleCount: 0, errorsByStatus: {} } });
      expect(run.durationMs).toBeUndefined();
      expect(run.facts).toEqual(['Full sync']);
      expect(run.progressText(NOW)).toBe('0 chunks processed · no chunk has finished yet · still fetching: no percent or estimate until every record is fetched');
    });

    it('notes deletions to come only for an incomplete full sync, and ignores progress beside a summary', () => {
      const delta = { ...incomplete, metadata: { ...incomplete.metadata, syncPopulation: 'person-delta' } };
      expect(new RunView(delta).deactivationNote).toBeUndefined();
      expect(new RunView({ ...incomplete, status: 'FAILED' }).deactivationNote).toBeUndefined();
      const summarized = new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: { processed: 1 } } });
      expect(summarized.soFar).toBe(false);
      expect(summarized.processed).toBe(1);
      expect(summarized.progressText(NOW)).toBeUndefined();
    });

    it("names a single-person run's person when its source URL carries the BUID", () => {
      const run = new RunView({
        syncRunId: '2026-11-01T15:17:00.000Z',
        status: 'INCOMPLETE',
        metadata: { syncPopulation: 'person-delta', source: 'https://cdm.example.bu.edu/api/person?buid=U0000042' }
      });
      expect(run.populationLabel).toBe('Person U0000042');
      expect(new RunView({ syncRunId: 'x', metadata: { source: 'not a url' } }).buid).toBeUndefined();
    });

    it('marks a listed run with nothing recorded as unknown', () => {
      const run = new RunView({ syncRunId: '2026-11-01T02:00:00.000Z', status: 'UNKNOWN' });
      expect(RunFormat.status(run.status).label).toBe('Unknown');
      expect(run.startedAt).toBe('2026-11-01T02:00:00.000Z');
    });
  });

  describe('RunsApi', () => {
    it('asks for the routes the handler serves', async () => {
      const paths: string[] = [];
      const api = new RunsApi(async (path: string) => { paths.push(path); return { items: [] }; });
      await api.listRuns({ limit: 20 });
      await api.listRuns({ limit: 20, cursor: '2026-10-01T02:00:00.000Z' });
      await api.errors('2026-10-01T02:00:00.000Z', { statusCode: 429, cursor: 'abc', limit: 50 });
      await api.changes('2026-10-01T02:00:00.000Z', { changeType: 'NEW' });
      expect(paths).toEqual([
        '/api/runs?include=report&limit=20',
        '/api/runs?include=report&limit=20&before=2026-10-01T02%3A00%3A00.000Z',
        '/api/runs/2026-10-01T02%3A00%3A00.000Z/errors?statusCode=429&cursor=abc&limit=50',
        '/api/runs/2026-10-01T02%3A00%3A00.000Z/changes?changeType=NEW'
      ]);
    });

    it('asks for the capabilities once, and again only after a failure', async () => {
      const getJson = jest.fn()
        .mockRejectedValueOnce(new Error('down'))
        .mockResolvedValue({ storageMode: 's3', changesSupported: false });
      const api = new RunsApi(getJson);
      await expect(api.capabilities()).rejects.toThrow('down');
      expect(await api.capabilities()).toEqual({ storageMode: 's3', changesSupported: false });
      await api.capabilities();
      expect(getJson).toHaveBeenCalledTimes(2);
    });
  });

  describe('CursorPager', () => {
    it('pages until a page comes back without a cursor', async () => {
      const pages: Record<string, any> = { start: { items: [1, 2], cursor: 'b' }, b: { items: [3] } };
      const pager = new CursorPager(async (cursor?: string) => pages[cursor || 'start']);
      expect(pager.hasMore).toBe(true);
      expect(await pager.next()).toEqual([1, 2]);
      expect(await pager.next()).toEqual([3]);
      expect(pager.hasMore).toBe(false);
      expect(await pager.next()).toEqual([]);
      expect(pager.items).toEqual([1, 2, 3]);
    });

    it('discards a page that arrives after a reset', async () => {
      let resolve: (page: any) => void = () => {};
      const pager = new CursorPager(() => new Promise(done => { resolve = done; }));
      const pending = pager.next();
      pager.reset();
      resolve({ items: ['stale'] });
      expect(await pending).toBeNull();
      expect(pager.items).toEqual([]);
      expect(pager.hasMore).toBe(true);
    });

    it('keeps its place when a page fails, so the same page can be asked for again', async () => {
      const loadPage = jest.fn()
        .mockResolvedValueOnce({ items: [1], cursor: 'b' })
        .mockRejectedValueOnce(new Error('timeout'))
        .mockResolvedValueOnce({ items: [2] });
      const pager = new CursorPager(loadPage);
      await pager.next();
      await expect(pager.next()).rejects.toThrow('timeout');
      expect(await pager.next()).toEqual([2]);
      expect(loadPage.mock.calls.map(([cursor]) => cursor)).toEqual([undefined, 'b', 'b']);
    });
  });

  describe('percent done and estimated finish', () => {
    const MIN = 60_000;
    /**
     * An unfinished full sync that started at 02:00 (NOW is 02:30): chunks of its chunkCount processed, the last at
     * lastChunkAt; processed is the people its processors sent to Huron (those that changed), which the percent ignores
     */
    const unfinished = (params: {
      processed?: number; records?: number; chunks?: number; chunkCount?: number; lastChunkAt?: string; chunkingFinished?: boolean; status?: string
    } = {}) => {
      const {
        processed = 200, records = 14500, chunks = 6, chunkCount = 15, lastChunkAt = '2026-11-06T02:21:00.000Z', chunkingFinished = true, status = 'INCOMPLETE'
      } = params;
      return new RunView({
        syncRunId: '2026-11-06T02:00:00.000Z',
        status,
        metadata: {
          createdAt: '2026-11-06T02:00:00.000Z', syncPopulation: 'person-full', chunkCount, totalRecords: records,
          ...(chunkingFinished ? { partialOrEmptyChunkEncountered: true } : {})
        },
        progress: { chunksReported: chunks, results: { processed }, totalErrors: 0, throttleCount: 0, errorsByStatus: {}, lastChunkAt }
      });
    };
    const percent = (params: Parameters<typeof unfinished>[0]) => unfinished(params).completion;
    /** Fetching is over, but the chunks written were never recorded */
    const withoutChunkCount = () => new RunView({ ...incomplete, metadata: { ...incomplete.metadata, chunkCount: undefined, partialOrEmptyChunkEncountered: true } });

    it('counts chunks processed of chunks written, rounded down to one decimal, so it reads 100.0 only when every chunk is processed', () => {
      expect(percent({ chunks: 734, chunkCount: 735 })).toEqual({ percent: 99.8, text: '99.8', reported: 734, chunks: 735, state: 'underway' });
      expect(percent({ chunks: 15 })).toMatchObject({ text: '100.0', state: 'done' });
      expect(percent({ chunks: 16 })).toMatchObject({ text: '100.0', reported: 15 });
      expect(percent({ chunks: 1, chunkCount: 3 })).toMatchObject({ text: '33.3' });
      expect(percent({ chunks: 0 })).toMatchObject({ text: '0.0' });
    });

    it('counts a chunk once it is processed, however few of its people changed', () => {
      // Preview's Test of one unchanged person: its one chunk processed, no one sent to Huron
      expect(percent({ processed: 0, records: 1, chunks: 1, chunkCount: 1 })).toMatchObject({ text: '100.0', state: 'done' });
      expect(percent({ processed: 0, chunks: 6 })).toMatchObject({ text: '40.0', state: 'underway' });
    });

    it("gives no percent while fetching, is failed for a failed run, and isn't given without chunk figures or for a summarized run", () => {
      // The chunks written lag the chunks processed until fetching is over
      expect(percent({ chunks: 16, chunkingFinished: false })).toEqual({ state: 'fetching', reported: 16 });
      expect(percent({ status: 'FAILED' })).toMatchObject({ text: '40.0', state: 'failed' });
      expect(percent({ status: 'FAILED', chunkingFinished: false })).toMatchObject({ text: '40.0', state: 'failed' });
      expect(percent({ chunkCount: 0 })).toBeUndefined();
      expect(withoutChunkCount().completion).toBeUndefined();
      expect(new RunView({ ...incomplete, progress: { ...incomplete.progress, chunksReported: undefined } }).completion).toBeUndefined();
      expect(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: { processed: 14500 }, chunkCount: 15, chunksReported: 15 } }).completion).toBeUndefined();
    });

    it("estimates the finish from the run's chunk rate so far, from its last chunk", () => {
      // 6 chunks in the 21 minutes to the last chunk; 9 to go at that rate is 31.5 minutes after it
      const run = unfinished();
      const at = Date.parse('2026-11-06T02:52:30.000Z');
      expect(run.estimate(NOW)).toEqual({ kind: 'estimate', at, inMs: 22.5 * MIN });
      expect(run.progressText(NOW)).toBe(
        `6 of 15 chunks processed · last chunk finished 9 min ago · processing should finish around ${RunFormat.clockTime(at, NOW)} (in about ${RunFormat.duration(22.5 * MIN)})`
      );
      // However few people changed
      expect(unfinished({ processed: 0 }).estimate(NOW)).toEqual({ kind: 'estimate', at, inMs: 22.5 * MIN });
    });

    it('calls a run stalled when no chunk has finished for 10 times its average gap, and at least 15 minutes', () => {
      const last = Date.parse('2026-11-06T02:21:00.000Z');
      // 6 chunks in 21 minutes: 3.5 minutes apart, so stalled after 35 minutes
      expect(unfinished().estimate(last + 34 * MIN)?.kind).toBe('estimate');
      expect(unfinished().estimate(last + 36 * MIN)).toEqual({ kind: 'stalled', sinceMs: 36 * MIN });
      // 21 chunks in 21 minutes: 1 minute apart, but never stalled before 15 minutes
      expect(unfinished({ chunks: 21, chunkCount: 40 }).estimate(last + 14 * MIN)?.kind).toBe('estimate');
      expect(unfinished({ chunks: 21, chunkCount: 40 }).estimate(last + 16 * MIN)?.kind).toBe('stalled');
      // Preview's Nov 6 run: 734 of 735 chunks, the last 3 days before
      const nov6 = unfinished({ processed: 146473, records: 146673, chunks: 734, chunkCount: 735 });
      expect(nov6.progressText(last + 3 * 24 * 60 * MIN)).toBe('734 of 735 chunks processed · Stalled: no chunk has finished for 3 days');
      expect(nov6.phase(last + 3 * 24 * 60 * MIN)).toEqual({ label: 'Stalled', tone: 'warning', percent: '99.8' });
    });

    it('gives no percent or estimate while still fetching, though such a run can stall', () => {
      const fetching = unfinished({ chunks: 16, chunkingFinished: false });
      expect(fetching.estimate(NOW)).toBeUndefined();
      expect(fetching.progressText(NOW)).toBe('16 chunks processed · last chunk finished 9 min ago · still fetching: no percent or estimate until every record is fetched');
      const stalled = fetching.progressText(Date.parse('2026-11-07T02:21:00.000Z'));
      expect(stalled).toContain('Stalled: no chunk has finished for 1 day');
      expect(stalled).not.toContain('still fetching');
    });

    it('gives none once every chunk is processed, for a failed run, or before a chunk has finished', () => {
      const done = unfinished({ chunks: 15 });
      expect(done.estimate(NOW)).toBeUndefined();
      expect(done.progressText(NOW)).toBe('15 of 15 chunks processed · last chunk finished 9 min ago · post-processing comes next');
      const failed = unfinished({ status: 'FAILED' });
      expect(failed.estimate(Date.parse('2026-11-09T02:21:00.000Z'))).toBeUndefined();
      expect(failed.progressText(NOW)).toBe('6 of 15 chunks processed · last chunk finished 9 min ago');
      expect(unfinished({ chunks: 0, lastChunkAt: undefined as any }).estimate(NOW)).toBeUndefined();
    });

    it('shows the percent in the Processed tile in the color of its state, and marks the stalled notice and the estimate', () => {
      const underway = RunDetailView.completionHtml(unfinished({ chunks: 734, chunkCount: 735 }));
      expect(underway).toContain('stat-completion completion-underway');
      expect(underway).toContain('<span class="stat-completion-percent">99.8%</span>');
      expect(underway).toContain('of 735 chunks</span>');
      expect(underway).toContain('aria-label="Percent of chunks processed"');
      expect(underway).toContain('aria-valuenow="99.8"');
      expect(underway).toContain('style="width: 99.8%"');
      expect(RunDetailView.completionHtml(unfinished({ chunks: 0, chunkCount: 1 }))).toContain('of 1 chunk</span>');
      expect(RunDetailView.completionHtml(unfinished({ chunks: 15 }))).toContain('completion-done');
      expect(RunDetailView.completionHtml(unfinished({ status: 'FAILED' }))).toContain('completion-failed');
      expect(RunDetailView.completionHtml(unfinished({ chunkingFinished: false }))).toBe('');
      expect(RunDetailView.completionHtml(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: {} } }))).toBe('');

      const last = Date.parse('2026-11-06T02:21:00.000Z');
      expect(RunDetailView.progressHtml(unfinished(), last + 3 * 24 * 60 * MIN)).toContain(
        '<span class="progress-stalled"><i class="fas fa-triangle-exclamation me-1" aria-hidden="true"></i>Stalled: no chunk has finished for 3 days</span>'
      );
      expect(RunDetailView.progressHtml(unfinished(), NOW)).toContain('<span class="progress-estimate"><i class="fas fa-clock me-1"');
      expect(RunDetailView.progressHtml(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: {} } }), NOW)).toBe('');
    });

    it("shows a row's percent beside its phase, none while fetching or post-processing, and none for a summarized run", () => {
      // The row is drawn at the real time, long after these runs' last chunk, so they read as stalled
      const underway = RunListTable.rowHtml(unfinished({ chunks: 734, chunkCount: 735 }));
      expect(underway).toContain('<span class="completion-note completion-underway"');
      expect(underway).toContain('>99.8%</span>');
      expect(underway).toContain('>Stalled</span>');
      expect(underway).not.toContain('Incomplete');
      const done = RunListTable.rowHtml(unfinished({ chunks: 15 }));
      expect(done).toContain('>Stalled</span>');
      expect(done).toContain('>100.0%</span>');
      const fetching = RunListTable.rowHtml(unfinished({ chunkingFinished: false }));
      expect(fetching).toContain('>Stalled</span>');
      expect(fetching).not.toContain('completion-note');
      expect(fetching).not.toContain('so-far-note');
      expect(RunListTable.rowHtml(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: {} } }))).not.toContain('completion-note');
    });

    describe('phase', () => {
      const last = Date.parse('2026-11-06T02:21:00.000Z');

      it('says what an unfinished run is doing, with its percent while processing', () => {
        expect(unfinished({ chunkingFinished: false }).phase(NOW)).toEqual({ label: 'Fetching', tone: 'warning' });
        expect(unfinished().phase(NOW)).toEqual({ label: 'Processing', tone: 'warning', percent: '40.0' });
        expect(unfinished({ chunks: 15 }).phase(NOW)).toEqual({ label: 'Post-processing', tone: 'warning' });
        // 6 chunks in 21 minutes: stalled after 35 minutes without one; no percent if it stalled while fetching
        expect(unfinished().phase(last + 36 * MIN)).toEqual({ label: 'Stalled', tone: 'warning', percent: '40.0' });
        expect(unfinished({ chunkingFinished: false }).phase(last + 36 * MIN)).toEqual({ label: 'Stalled', tone: 'warning' });
      });

      it('reads Post-processing once the last chunk is processed, though no one changed', () => {
        expect(unfinished({ processed: 0, records: 1, chunks: 1, chunkCount: 1 }).phase(NOW)).toEqual({ label: 'Post-processing', tone: 'warning' });
        expect(unfinished({ processed: 0, chunks: 15 }).phase(NOW)).toEqual({ label: 'Post-processing', tone: 'warning' });
      });

      it('calls a run waiting on post-processing stalled by the same rule, so a run never post-processed stops reading Post-processing', () => {
        // 6 chunks in 21 minutes, all processed: stalled after 35 minutes
        const done = unfinished({ chunks: 6, chunkCount: 6 });
        expect(done.phase(last + 34 * MIN)?.label).toBe('Post-processing');
        expect(done.phase(last + 36 * MIN)).toEqual({ label: 'Stalled', tone: 'warning', percent: '100.0' });
        expect(done.progressText(last + 3 * 24 * 60 * MIN))
          .toBe("6 of 6 chunks processed · Stalled: post-processing hasn't finished 3 days after the last chunk");
        // No estimate for it either way
        expect(done.estimate(last + 36 * MIN)).toBeUndefined();
      });

      it('leaves a finished or failed run, or one without chunk figures, to its status', () => {
        expect(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: {} } }).phase(NOW)).toBeUndefined();
        expect(unfinished({ status: 'FAILED' }).phase(NOW)).toBeUndefined();
        expect(withoutChunkCount().phase(NOW)).toBeUndefined();
      });

      it('draws the phase as the status badge, never green while unfinished', () => {
        expect(unfinished({ chunks: 15 }).statusBadgeHtml(NOW)).toContain('status-badge-warning');
        expect(unfinished({ chunks: 15 }).statusBadgeHtml(NOW)).toContain('>Post-processing<');
        expect(unfinished({ status: 'FAILED' }).statusBadgeHtml(NOW)).toContain('>Failed<');
        expect(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: {} } }).statusBadgeHtml(NOW)).toContain('status-badge-success');
        expect(withoutChunkCount().statusBadgeHtml(NOW)).toContain('>Incomplete<');
      });
    });

    it('says a length of time and a time of day as a person would', () => {
      expect([30_000, 5 * MIN, 3 * 60 * MIN, 24 * 60 * MIN, 3 * 24 * 60 * MIN].map(ms => RunFormat.span(ms)))
        .toEqual(['under a minute', '5 min', '3 h', '1 day', '3 days']);
      const later = Date.parse('2026-11-06T02:50:45.000Z');
      expect(RunFormat.clockTime(later, later)).toBe(new Date(later).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' }));
      expect(RunFormat.clockTime(later, later + 3 * 24 * 60 * MIN)).toBe(RunFormat.dateTime(new Date(later).toISOString()));
      expect(RunFormat.clockTime(NaN)).toBe(RunFormat.NONE);
    });
  });

  describe('who started a run', () => {
    it('names the signed-in user, the schedule or the Runner, from the summary or else the flags', () => {
      expect(RunView.startedByLabel({ by: 'dashboard', principal: 'jdoe' })).toBe('jdoe');
      expect(RunView.startedByLabel({ by: 'dashboard' })).toBe('Dashboard');
      expect(RunView.startedByLabel({ by: 'schedule' })).toBe('Schedule');
      expect(RunView.startedByLabel({ by: 'runner' })).toBe('Runner');
      expect(RunView.startedByLabel(undefined)).toBeUndefined();
      expect(RunView.startedByLabel({ by: 'someone' })).toBeUndefined();

      const summarized = new RunView({ syncRunId: 'a', status: 'SUCCEEDED', summary: { results: {}, startedBy: { by: 'schedule' } } });
      expect(summarized.startedBy).toBe('Schedule');
      expect(summarized.facts[0]).toBe('Started by Schedule');
      const unfinished = new RunView({ ...incomplete, flags: { ...(incomplete as any).flags, startedBy: { by: 'dashboard', principal: 'jdoe' } } });
      expect(unfinished.startedBy).toBe('jdoe');
    });

    it("shows it in its own column, and a dash for a run from before it was recorded", () => {
      expect(RunListTable.HEAD_HTML).toMatch(/<th scope="col">Started<\/th>\s*<th scope="col">Started by<\/th>\s*<th scope="col">Status<\/th>/);
      const row = (summary: any) => RunListTable.rowHtml(new RunView({ syncRunId: '2026-11-04T02:00:00.000Z', status: 'SUCCEEDED', summary }));
      expect(row({ results: {}, startedBy: { by: 'dashboard', principal: 'j<d>oe' } })).toContain('<td>j&lt;d&gt;oe</td>');
      expect(row({ results: {} })).toContain(`<td>${RunFormat.NONE}</td>`);
    });
  });

  describe('rows', () => {
    it("links a run's row to its page, and escapes what it shows", () => {
      const row = RunListTable.rowHtml(new RunView({
        syncRunId: '2026-11-04T02:00:00.000Z',
        status: 'FAILED',
        metadata: { syncPopulation: '<b>odd</b>' }
      }));
      expect(row).toContain('data-href="#runs/2026-11-04T02:00:00.000Z"');
      expect(row).toContain('href="#runs/2026-11-04T02:00:00.000Z"');
      expect(row).toContain('status-badge-danger');
      expect(row).toContain('&lt;b&gt;odd&lt;/b&gt;');
      expect(row).not.toContain('<b>');
    });

    it("mutes an unfinished run's figures in its row and says they are so far, leaving its records alone", () => {
      const row = RunListTable.rowHtml(new RunView(incomplete));
      // Still fetching, so no percent beside its phase
      expect(row).not.toContain('completion-note');
      // Once fetching is over: its percent done (6 of 15 chunks), the progress line as its hover text
      const processing = RunListTable.rowHtml(new RunView({ ...incomplete, metadata: { ...incomplete.metadata, partialOrEmptyChunkEncountered: true } }));
      expect(processing).toContain('class="completion-note completion-underway"');
      expect(processing).toContain('>40.0%</span>');
      expect(processing).toContain('Summed from the chunks processed so far: 6 of 15 chunks processed');
      // Without chunk figures there is no phase or percent, so it says "so far"
      const withoutChunks = RunListTable.rowHtml(new RunView({ ...incomplete, progress: { ...incomplete.progress, chunksReported: undefined } }));
      expect(withoutChunks).toContain('class="so-far-note"');
      expect(withoutChunks).not.toContain('completion-note');
      expect(row.match(/class="num so-far"/g)).toHaveLength(5);
      expect(row).toContain(`<td class="num">${(14500).toLocaleString()}</td>`);
      expect(row).toContain('<td class="num so-far">21 min</td>');

      const finished = RunListTable.rowHtml(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: {} } }));
      expect(finished).not.toContain('so-far');
    });

    it("labels an unfinished run's tiles so far, notes deletions to come, and says how far it has got", () => {
      const tiles = RunDetailView.tilesHtml(new RunView(incomplete), NOW);
      expect(tiles.match(/stat-tile stat-tile-so-far/g)).toHaveLength(5);
      expect(tiles).toContain('Processed so far');
      expect(tiles).toContain('Deactivated so far');
      expect(tiles).toContain('<div class="stat-note">Post-processing adds deletions</div>');
      expect(tiles).toContain('<p class="stat-progress">6 chunks processed · last chunk finished 9 min ago · still fetching');
      // No percent while fetching; once fetching is over, of how many chunks
      expect(tiles).not.toContain('stat-completion');
      const processing = RunDetailView.tilesHtml(new RunView({ ...incomplete, metadata: { ...incomplete.metadata, partialOrEmptyChunkEncountered: true } }), NOW);
      expect(processing).toContain('of 15 chunks</span>');
      expect(processing.match(/stat-completion /g)).toHaveLength(1);
      expect(RunDetailView.noteHtml(new RunView(incomplete))).toContain('summed from the chunks processed so far');

      const finished = RunDetailView.tilesHtml(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: {} } }), NOW);
      expect(finished).not.toContain('so far');
      expect(finished).not.toContain('stat-progress');
    });

    it("lists an unfinished run's known facts: chunks processed, last chunk, duration so far", () => {
      const view = Object.assign(Object.create(RunDetailView.prototype), { run: new RunView(incomplete), capabilities: {} });
      const facts = Object.fromEntries(view.summaryFacts());
      // Still fetching: the chunks written lag those processed, so only those processed so far
      expect(facts).toMatchObject({
        Chunks: '6 processed so far', Duration: '21 min so far', Succeeded: (5990).toLocaleString(), 'Throttled requests': '4'
      });
      const processing = Object.assign(Object.create(RunDetailView.prototype), {
        run: new RunView({ ...incomplete, metadata: { ...incomplete.metadata, partialOrEmptyChunkEncountered: true } }), capabilities: {}
      });
      expect(Object.fromEntries(processing.summaryFacts()).Chunks).toBe('6 of 15 processed');
      expect(facts['Last chunk finished']).toBe(RunFormat.dateTime('2026-11-06T02:21:00.000Z', { seconds: true }));
      expect(facts).not.toHaveProperty('Dry run');
    });

    it("shows an error's person from its details, and escapes its message", () => {
      const row = RunDetailView.errorRowHtml({
        timestamp: '2026-11-04T02:01:30.893Z', statusCode: 429, isThrottled: true,
        message: 'Rate <limited>', detailsObj: JSON.stringify({ sourceIdentifier: 'U12345678' })
      });
      expect(row).toContain('429 Too Many Requests');
      expect(row).toContain('Throttled');
      expect(row).toContain('U12345678');
      expect(row).toContain('Rate &lt;limited&gt;');
      expect(Html.escape(`"it's" & more`)).toBe('&quot;it&#39;s&quot; &amp; more');
    });
  });
});

describe('run-history.js against the dashboard routes and mock run history', () => {
  const { RunsApi, CursorPager } = loadRunHistory();

  beforeAll(() => { process.env.LOCALHOST_WITH_MOCKS = 'true'; });
  afterAll(() => { delete process.env.LOCALHOST_WITH_MOCKS; });
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  it('pages through every run, newest first, 20 at a time', async () => {
    const api = new RunsApi(getJson);
    const pager = new CursorPager((cursor?: string) => api.listRuns({ cursor, limit: 20 }));
    const pages: number[] = [];
    while (pager.hasMore) {
      pages.push((await pager.next()).length);
    }
    expect(pages).toEqual([20, 20]);
    expect(pager.items).toHaveLength(MOCK_RUN_COUNT);
    expect(pager.items[0].syncRunId).toBe(NEWEST_MOCK_RUN);
    const startedAt = pager.items.map((run: any) => run.startedAt);
    expect([...startedAt].sort().reverse()).toEqual(startedAt);
    expect(new Set(pager.items.map((run: any) => run.status))).toEqual(
      new Set(['SUCCEEDED', 'FAILED', 'INCOMPLETE', 'POST_PROCESSING_FAILED'])
    );
  });

  it('shows every unsummarized mock run its figures so far, in the list as on its page', async () => {
    const api = new RunsApi(getJson);
    const runs = (await api.listRuns({ limit: 50 })).items;
    const unfinished = runs.filter((run: any) => !run.hasSummary && run.status !== 'UNKNOWN');
    expect(unfinished.length).toBeGreaterThan(0);
    expect(unfinished.every((run: any) => run.soFar)).toBe(true);
    expect(runs.filter((run: any) => run.hasSummary).some((run: any) => run.soFar)).toBe(false);

    const underway = unfinished.find((run: any) => run.status === 'INCOMPLETE' && run.chunksReported > 0);
    const chunks = await api.chunks(underway.syncRunId);
    const sum = (field: string) => chunks.reduce((total: number, chunk: any) => total + (chunk[field] ?? 0), 0);
    expect([underway.processed, underway.added, underway.updated, underway.errors])
      .toEqual([sum('totalRecords'), sum('addedCount'), sum('updatedCount'), sum('totalErrors')]);
    // As in a real nightly run, few of its people changed (all a processor counts), but its percent counts chunks
    expect(underway.processed).toBeLessThan(underway.records / 10);
    expect(underway.completion).toMatchObject({ state: 'underway', reported: chunks.length, chunks: underway.chunkCount });
    expect(underway.completion.percent).toBeGreaterThan(50);
    const page = await api.run(underway.syncRunId);
    expect([page.processed, page.added, page.updated, page.deactivated, page.errors, page.durationMs])
      .toEqual([underway.processed, underway.added, underway.updated, underway.deactivated, underway.errors, underway.durationMs]);
  });

  it("pages through one status code's errors, and counts them as the summary does", async () => {
    const api = new RunsApi(getJson);
    const run = await api.run(NEWEST_MOCK_RUN);
    const pager = new CursorPager((cursor?: string) => api.errors(NEWEST_MOCK_RUN, { statusCode: 429, cursor, limit: 50 }));
    while (pager.hasMore) {
      await pager.next();
    }
    expect(pager.items).toHaveLength(run.errorsByStatus[429]);
    expect(pager.items.every((item: any) => item.statusCode === 429)).toBe(true);
  });

  it("pages through a run's changes of one type, as many as the summary counts", async () => {
    const api = new RunsApi(getJson);
    const run = await api.run(NEWEST_MOCK_RUN);
    for (const changeType of ['NEW', 'UPDATED', 'DELETED']) {
      const pager = new CursorPager((cursor?: string) => api.changes(NEWEST_MOCK_RUN, { changeType, cursor, limit: 50 }));
      while (pager.hasMore) {
        await pager.next();
      }
      expect(pager.items).toHaveLength(run.changeCount(changeType));
    }
  });

  it('rejects with the status and message the page shows: an invalid cursor, a run that does not exist', async () => {
    const api = new RunsApi(getJson);
    await expect(api.errors(NEWEST_MOCK_RUN, { cursor: 'not-a-cursor' })).rejects.toMatchObject({ status: 400 });
    await expect(api.run('2001-01-01T00:00:00.000Z')).rejects.toMatchObject({ status: 404 });
  });

  it('is told, in S3 mode, that changes are not recorded', async () => {
    process.env.MOCK_STORAGE_MODE = 's3';
    try {
      const api = new RunsApi(getJson);
      expect(await api.capabilities()).toMatchObject({ storageMode: 's3', changesSupported: false });
      await expect(api.changes(NEWEST_MOCK_RUN, { changeType: 'NEW' })).rejects.toMatchObject({ status: 400 });
    } finally {
      delete process.env.MOCK_STORAGE_MODE;
    }
  });
});
