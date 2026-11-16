/**
 * Tests the Overview page's script (dashboard/public/js/overview.js) and the service status table it shares with
 * the Status page (dashboard/public/js/system-status.js): how the latest run and the service checks read, and
 * that each of the two loads shows on its own when the other fails. The scripts are browser code with no module
 * system, so they run in a vm context (as written and as the Lambda build minifies them), after the scripts
 * they build on, against a minimal stand-in for the DOM.
 */
import { readFileSync } from 'fs';
import { join } from 'path';
import { Script, createContext } from 'vm';
import { minifyJS } from '../dashboard/scripts/BuildTemplates';
import { handler } from '../dashboard/src/handlers/DashboardHandler';
import { LambdaFunctionUrlEvent } from '../dashboard/src/handlers/DashboardTypes';
import { NEWEST_OFFLINE_RUN } from '../dashboard/src/handlers/OfflineRunReporting';

const JS_DIR = join(__dirname, '../dashboard/public/js');
const SCRIPTS = ['page-state.js', 'run-history.js', 'system-status.js', 'overview.js'];
const CLASSES = ['RunsApi', 'RunView', 'ServiceStatusTable', 'OverviewPage'];

/** Just enough of an element: classes, attributes, text, markup, children and listeners */
class FakeElement {
  className = '';
  textContent = '';
  dataset: Record<string, string> = {};
  attributes: Record<string, string> = {};
  children: FakeElement[] = [];
  listeners: Record<string, () => void> = {};
  private markup = '';
  constructor(public tagName = 'div') {}
  get innerHTML() { return this.markup; }
  set innerHTML(markup: string) { this.markup = markup; this.children = []; }
  setAttribute(name: string, value: string) { this.attributes[name] = value; }
  appendChild(child: FakeElement) { this.children.push(child); return child; }
  replaceChildren(...children: FakeElement[]) { this.children = children; this.markup = ''; }
  addEventListener(type: string, listener: () => void) { this.listeners[type] = listener; }
  /** The text of the state (PageState) this element shows, if any */
  get stateText(): string {
    const text = (element: FakeElement): string => [element.textContent, ...element.children.map(text)].join(' ');
    return text(this).replace(/\s+/g, ' ').trim();
  }
}

/** The Overview partial's parts, as OverviewPage finds them under its root */
const fakeOverview = () => {
  const parts: Record<string, FakeElement> = {
    '[data-latest-run]': new FakeElement(),
    '[data-services]': new FakeElement(),
    '[data-refresh-stamp="overview"]': new FakeElement('span'),
    '[data-action="refresh-overview"]': new FakeElement('button')
  };
  return { root: { querySelector: (selector: string) => parts[selector] || null }, parts };
};

const loadScripts = (minified = false): Record<string, any> => {
  const document = { createElement: (tag: string) => new FakeElement(tag) };
  const context = createContext({ URL, URLSearchParams, console, document });
  const source = SCRIPTS.map(file => readFileSync(join(JS_DIR, file), 'utf-8')).join('\n');
  const exports = CLASSES.map(name => `this.${name} = ${name};`).join('\n');
  new Script(`${minified ? minifyJS(source).minified : source}\n${exports}`).runInContext(context);
  return context as any;
};

const NOW = Date.parse('2026-11-06T12:00:00.000Z');

const succeeded = {
  syncRunId: '2026-11-04T02:00:00.000Z',
  status: 'SUCCEEDED',
  summary: {
    syncRunId: '2026-11-04T02:00:00.000Z', status: 'SUCCEEDED', storageMode: 'dynamodb', dryRun: false,
    createdAt: '2026-11-04T02:00:00.000Z', completedAt: '2026-11-04T02:47:00.000Z', syncPopulation: 'person-full',
    chunksReported: 15, totalErrors: 260, throttleCount: 120, errorsByStatus: {},
    results: { processed: 14500, succeeded: 14360, failed: 140, skipped: 0, added: 130, updated: 240, removed: 0 }
  }
};

const failedEarly = {
  syncRunId: '2026-11-06T02:00:00.000Z',
  status: 'FAILED',
  metadata: { createdAt: '2026-11-06T02:00:00.000Z', syncPopulation: 'person-delta' },
  terminalError: { stage: 'chunking', errorMessage: 'Source API returned 503', errorTimestamp: '2026-11-06T02:04:00.000Z' }
};

/** A full sync underway: 6 of its 15 chunks processed, the last 10 minutes before NOW */
const underway = {
  syncRunId: '2026-11-06T10:00:00.000Z',
  status: 'INCOMPLETE',
  metadata: { createdAt: '2026-11-06T10:00:00.000Z', syncPopulation: 'person-full', chunkCount: 15, totalRecords: 14500, partialOrEmptyChunkEncountered: true },
  progress: {
    chunksReported: 6, totalErrors: 0, throttleCount: 0, errorsByStatus: {},
    results: { processed: 6000, succeeded: 6000, failed: 0, skipped: 0, added: 40, updated: 5960, removed: 0 },
    lastChunkAt: '2026-11-06T11:50:00.000Z'
  }
};

describe.each([false, true])('overview.js and system-status.js (minified: %s)', minified => {
  const { RunView, ServiceStatusTable, OverviewPage } = loadScripts(minified);

  describe('the latest run', () => {
    it('links to the run, says how long ago it finished, and shows its figures', () => {
      const html = OverviewPage.latestRunHtml(new RunView(succeeded), NOW);
      expect(html).toContain('href="#runs/2026-11-04T02:00:00.000Z"');
      expect(html).toContain('View run');
      expect(html).toContain('status-badge-success');
      expect(html).toMatch(/Full sync &middot; DynamoDB storage &middot; 47 min &middot; Finished 2 days ago/);
      expect(html).toContain((14500).toLocaleString());
      expect(html).not.toContain('run-note');
    });

    it("shows a run that is underway with its figures so far, and how far it has got", () => {
      const html = OverviewPage.latestRunHtml(new RunView(underway), NOW);
      expect(html).toContain('Started 2 h ago');
      expect(html).toMatch(/Full sync &middot; 1 h 50 min so far &middot; Started 2 h ago/);
      expect(html).toContain('run-note-warning');
      expect(html).toContain('Added so far');
      expect(html).toContain((6000).toLocaleString());
      expect(html).toContain('6 of 15 chunks processed · last chunk finished 10 min ago');
      // 6 of 15 chunks, and at its rate so far (6 chunks in the 110 minutes to its last chunk) the other 9 take
      // 165 minutes from it: about 2 h 35 min to go
      expect(html).toContain('<span class="stat-completion-percent">40.0%</span>');
      expect(html).toContain('of 15 chunks</span>');
      expect(html).toContain('(in about 2 h 35 min)');
    });

    it('says when a run that has not finished started, and why it failed', () => {
      const html = OverviewPage.latestRunHtml(new RunView(failedEarly), NOW);
      expect(html).toContain('Started 10 h ago');
      expect(html).toContain('run-note-danger');
      expect(html).toContain('Source API returned 503');
    });
  });

  describe('ServiceStatusTable', () => {
    it('names each service, and tints its status by what the status means', () => {
      const html = ServiceStatusTable.html({
        targetApi: { status: 'healthy', responseTime: 85 },
        database: { status: 'unhealthy' },
        '<odd>': { status: 'degraded' }
      });
      expect(html).toContain('Target API (Huron)');
      expect(html).toContain('85 ms');
      expect(html).toContain('status-badge-success');
      expect(html).toContain('status-badge-danger');
      expect(html).toContain('status-badge-warning');
      expect(html).toContain('&lt;odd&gt;');
      expect(html).not.toContain('<odd>');
    });

    it('shows the empty state when no service is reported', () => {
      const container = new FakeElement();
      ServiceStatusTable.show(container, {});
      expect(container.stateText).toContain('No services reported');
    });
  });

  describe("the Mock landscape section (a mock landscape's reset)", () => {
    const table = (name: string, itemsDeleted: number, durationMs: number) => ({ table: name, tableName: `t-${name}`, itemsDeleted, durationMs });
    const succeededReset = {
      status: 'succeeded', via: 'overview', startedAt: '2026-11-06T11:50:00.000Z', finishedAt: '2026-11-06T11:51:42.000Z', durationMs: 102_000,
      startedBy: { by: 'dashboard', principal: 'wrh' }, storageMode: 'dynamodb',
      tables: [table('mock_target_person', 146554, 101_000), table('statistics', 1489, 4_000)]
    };
    const state = (last?: any, extra: any = {}) => ({ mockLandscape: true, landscape: 'mock', ...(last ? { last } : {}), ...extra });

    it('says what the last reset emptied and how long each table took, to the second', () => {
      const html = OverviewPage.resetHtml(state(succeededReset), NOW);
      expect(html).toContain('The last reset, started 10 min ago from Overview by wrh, finished in 1 min 42 s.');
      expect(html).toMatch(/Target simulator&#39;s people<\/td>\s*<td class="num">146,554<\/td>\s*<td class="num">1 min 41 s<\/td>/);
      expect(html).toMatch(/Run history \(statistics\)<\/td>\s*<td class="num">1,489<\/td>\s*<td class="num">4.0 s<\/td>/);
      expect(html).toContain('data-action="reset-landscape"');
      expect(html).not.toMatch(/data-action="reset-landscape" disabled/);
    });

    it('says a reset is underway, never finished, failed, or has not happened', () => {
      expect(OverviewPage.lastResetHtml({ ...succeededReset, status: 'running', durationMs: undefined }, NOW))
        .toContain('A reset started 10 min ago from Overview by wrh is underway.');
      expect(OverviewPage.lastResetHtml({ ...succeededReset, status: 'stopped', via: 'bulk-sync' }, NOW))
        .toContain("by Bulk sync&#39;s Start from scratch by wrh, never finished: it most likely ran out of the chunker subscriber's 10 minutes");
      expect(OverviewPage.lastResetHtml({ ...succeededReset, status: 'failed', durationMs: 3_000, error: 'AccessDenied <x>' }, NOW))
        .toContain('failed after 3.0 s: AccessDenied &lt;x&gt;');
      expect(OverviewPage.lastResetHtml(undefined, NOW)).toContain('No reset recorded yet.');
      expect(OverviewPage.resetHtml(state(undefined, { lastError: 'denied' }), NOW)).toContain('The last reset could not be read: denied');
    });

    it("disables Reset landscape, saying why, when one can't be started now", () => {
      const html = OverviewPage.resetHtml(state(succeededReset, { blocked: { code: 'RUN_UNDERWAY', message: 'The run of x is still underway' } }), NOW);
      expect(html).toContain("A reset can't be started now: The run of x is still underway");
      expect(html).toMatch(/data-action="reset-landscape" disabled/);
    });

    /** The Overview partial's parts, with the Mock landscape section and what its content holds */
    const resetPage = (reset: () => Promise<any>, options: { confirm?: boolean; post?: () => Promise<any> } = {}) => {
      const { root, parts } = fakeOverview();
      const section = Object.assign(new FakeElement('section'), { hidden: true });
      const message = Object.assign(new FakeElement('p'), { hidden: true });
      const button = Object.assign(new FakeElement('button'), { disabled: false });
      const container = Object.assign(new FakeElement(), {
        querySelector: (selector: string) => ({ '[data-mock-reset-message]': message, '[data-action="reset-landscape"]': button } as any)[selector]
      });
      parts['[data-mock-reset-section]'] = section;
      parts['[data-mock-reset]'] = container as any;
      const posted: string[] = [];
      const getJson = async (path: string) => path === '/api/mock-landscape/reset' ? reset() : path === '/api/status' ? { services: {} } : {};
      const postJson = async (path: string) => { posted.push(path); return options.post ? options.post() : { status: 'requested' }; };
      const page = new OverviewPage({
        api: { listRuns: async () => ({ items: [] }) }, getJson, postJson, root, now: () => NOW, confirm: () => options.confirm ?? true
      });
      return { page, section, container, message, button, posted };
    };

    it('shows the section only in a mock landscape, and says when its state can\'t be read', async () => {
      const elsewhere = resetPage(async () => ({ mockLandscape: false }));
      await elsewhere.page.load();
      expect(elsewhere.section.hidden).toBe(true);
      const mock = resetPage(async () => state(succeededReset));
      await mock.page.load();
      expect(mock.section.hidden).toBe(false);
      expect(mock.container.innerHTML).toContain('finished in 1 min 42 s');
      const failing = resetPage(async () => { throw new Error('denied'); });
      await failing.page.load();
      expect(failing.section.hidden).toBe(false);
      expect(failing.container.stateText).toBe("The mock landscape's last reset could not be read denied");
    });

    it('asks before a reset, then hands it to the chunker subscriber and says it has started', async () => {
      const declined = resetPage(async () => state(succeededReset), { confirm: false });
      await declined.page.load();
      expect(await declined.page.reset()).toBe(false);
      expect(declined.posted).toEqual([]);

      const confirmed = resetPage(async () => state(succeededReset));
      await confirmed.page.load();
      expect(await confirmed.page.reset()).toBe(true);
      expect(confirmed.posted).toEqual(['/api/mock-landscape/reset']);
      expect(confirmed.button.disabled).toBe(true);
      expect(confirmed.message).toMatchObject({ hidden: false, textContent: expect.stringContaining('The reset has started') });
    });

    it("says why a reset wasn't started, and lets it be tried again", async () => {
      const refused = resetPage(async () => state(succeededReset), { post: async () => { throw new Error('A reset is still running'); } });
      await refused.page.load();
      expect(await refused.page.reset()).toBe(false);
      expect(refused.button.disabled).toBe(false);
      expect(refused.message.textContent).toBe("The reset wasn't started: A reset is still running");
    });
  });

  describe('OverviewPage', () => {
    const createPage = (listRuns: () => Promise<any>, getJson: (path: string) => Promise<any>) => {
      const { root, parts } = fakeOverview();
      const page = new OverviewPage({ api: { listRuns }, getJson, root, now: () => NOW });
      return { page, parts };
    };

    it('shows the latest run even when the service checks fail, and stamps the refresh', async () => {
      const { page, parts } = createPage(
        async () => ({ items: [new RunView(succeeded)] }),
        async () => { throw new Error('SystemStatus not yet implemented'); }
      );
      expect(await page.load()).toBe(true);
      expect(parts['[data-latest-run]'].innerHTML).toContain('#runs/2026-11-04T02:00:00.000Z');
      expect(parts['[data-services]'].stateText).toBe('Service status could not be loaded SystemStatus not yet implemented');
      expect(parts['[data-refresh-stamp="overview"]'].textContent).toMatch(/^Refreshed /);
    });

    it('shows the service checks even when the runs fail, and says when there are no runs', async () => {
      const failing = createPage(
        async () => { throw new Error('table not found'); },
        async () => ({ services: { sourceApi: { status: 'healthy' } } })
      );
      await failing.page.load();
      expect(failing.parts['[data-latest-run]'].stateText).toBe('The latest run could not be loaded table not found');
      expect(failing.parts['[data-services]'].innerHTML).toContain('Source API (Boston University)');

      const empty = createPage(async () => ({ items: [] }), async () => ({ services: {} }));
      await empty.page.load();
      expect(empty.parts['[data-latest-run]'].stateText).toMatch(/^No runs yet/);
    });

    it('leaves the refresh time alone when nothing loaded', async () => {
      const { page, parts } = createPage(
        async () => { throw new Error('down'); },
        async () => { throw new Error('down'); }
      );
      await page.load();
      expect(parts['[data-refresh-stamp="overview"]'].textContent).toBe('');
    });

    it('reloads on Refresh, and discards a load that a later one overtook', async () => {
      let resolveFirst: (value: any) => void = () => {};
      const listRuns = jest.fn()
        .mockImplementationOnce(() => new Promise(resolve => { resolveFirst = resolve; }))
        .mockResolvedValue({ items: [new RunView(succeeded)] });
      const { page, parts } = createPage(listRuns, async () => ({ services: {} }));
      const first = page.load();
      parts['[data-action="refresh-overview"]'].listeners.click();
      resolveFirst({ items: [new RunView(failedEarly)] });
      expect(await first).toBe(false);
      await new Promise(resolve => setImmediate(resolve));
      expect(listRuns).toHaveBeenCalledTimes(2);
      expect(parts['[data-latest-run]'].innerHTML).toContain('2026-11-04T02:00:00.000Z');
    });
  });
});

describe('overview.js against the dashboard routes and offline services', () => {
  const { RunsApi, OverviewPage } = loadScripts();

  /** dashboard.js's fetchSectionData, over the handler instead of fetch */
  const getJson = async (path: string): Promise<any> => {
    const url = new URL(path, 'https://dashboard.example.com');
    const result = await handler({
      rawPath: url.pathname,
      queryStringParameters: Object.fromEntries(url.searchParams),
      headers: {},
      requestContext: { http: { method: 'GET', path: url.pathname } },
      isBase64Encoded: false
    } as unknown as LambdaFunctionUrlEvent);
    const data = result.body ? JSON.parse(`${result.body}`) : {};
    if (result.statusCode !== 200) {
      throw Object.assign(new Error(data.message || data.error), { status: result.statusCode });
    }
    return data;
  };

  beforeAll(() => { process.env.LOCAL_OFFLINE = 'true'; });
  afterAll(() => { delete process.env.LOCAL_OFFLINE; });
  beforeEach(() => jest.spyOn(console, 'log').mockImplementation(() => {}));
  afterEach(() => jest.restoreAllMocks());

  it('shows the newest offline run and the offline service checks', async () => {
    const { root, parts } = fakeOverview();
    await new OverviewPage({ api: new RunsApi(getJson), getJson, root }).load();
    expect(parts['[data-latest-run]'].innerHTML).toContain(`#runs/${NEWEST_OFFLINE_RUN}`);
    expect(parts['[data-services]'].innerHTML).toContain('Target API (Huron)');
  });

  it("serves a mock landscape's reset: its last one, then a reset requested, then that one (offline, playing mock)", async () => {
    const call = async (method: string): Promise<{ statusCode: number; data: any }> => {
      const result = await handler({
        rawPath: '/api/mock-landscape/reset', queryStringParameters: {}, headers: {}, body: method === 'POST' ? '{}' : undefined,
        requestContext: { http: { method, path: '/api/mock-landscape/reset' } }, isBase64Encoded: false
      } as unknown as LambdaFunctionUrlEvent);
      return { statusCode: result.statusCode, data: JSON.parse(`${result.body}`) };
    };
    expect((await call('GET')).data).toEqual({ mockLandscape: false });
    process.env.LANDSCAPE = 'mock';
    try {
      const before = await call('GET');
      expect(before).toMatchObject({ statusCode: 200, data: { mockLandscape: true, landscape: 'mock', last: { status: 'succeeded', via: 'overview' } } });
      expect(before.data.last.tables).toHaveLength(4);
      const requested = await call('POST');
      expect(requested).toMatchObject({ statusCode: 202, data: { status: 'requested', landscape: 'mock' } });
      const after = await call('GET');
      expect(after.data.last).toMatchObject({ status: 'succeeded', startedAt: requested.data.requestedAt });
      expect(after.data.last.tables.every((table: any) => table.itemsDeleted === 0)).toBe(true);
      expect((await call('PUT')).statusCode).toBe(405);
    } finally {
      delete process.env.LANDSCAPE;
    }
  });
});
