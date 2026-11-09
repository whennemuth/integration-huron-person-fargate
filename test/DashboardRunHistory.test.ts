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
      expect(run.progressText(NOW)).toBe('6 of 15 chunks processed · last chunk finished 9 min ago');
    });

    it('says no chunk has finished, and gives no duration, for an unfinished run with none processed', () => {
      const run = new RunView({ ...incomplete, metadata: { ...incomplete.metadata, chunkCount: undefined },
        progress: { chunksReported: 0, results: {}, totalErrors: 0, throttleCount: 0, errorsByStatus: {} } });
      expect(run.durationMs).toBeUndefined();
      expect(run.facts).toEqual(['Full sync']);
      expect(run.progressText(NOW)).toBe('0 chunks processed · no chunk has finished yet');
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
      expect(row).toContain('class="so-far-note"');
      expect(row).toContain('Summed from the chunks processed so far: 6 of 15 chunks processed');
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
      expect(tiles).toContain('<p class="stat-progress">6 of 15 chunks processed · last chunk finished 9 min ago</p>');
      expect(RunDetailView.noteHtml(new RunView(incomplete))).toContain('summed from the chunks processed so far');

      const finished = RunDetailView.tilesHtml(new RunView({ ...incomplete, status: 'SUCCEEDED', summary: { results: {} } }), NOW);
      expect(finished).not.toContain('so far');
      expect(finished).not.toContain('stat-progress');
    });

    it("lists an unfinished run's known facts: chunks processed, last chunk, duration so far", () => {
      const view = Object.assign(Object.create(RunDetailView.prototype), { run: new RunView(incomplete), capabilities: {} });
      const facts = Object.fromEntries(view.summaryFacts());
      expect(facts).toMatchObject({
        Chunks: '6 of 15 processed', Duration: '21 min so far', Succeeded: (5990).toLocaleString(), 'Throttled requests': '4'
      });
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
