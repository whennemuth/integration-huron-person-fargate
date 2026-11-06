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
import { NEWEST_MOCK_RUN } from '../dashboard/src/handlers/MockRunReporting';

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

describe('overview.js against the dashboard routes and mock services', () => {
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

  beforeAll(() => { process.env.LOCALHOST_WITH_MOCKS = 'true'; });
  afterAll(() => { delete process.env.LOCALHOST_WITH_MOCKS; });
  beforeEach(() => jest.spyOn(console, 'log').mockImplementation(() => {}));
  afterEach(() => jest.restoreAllMocks());

  it('shows the newest mock run and the mock service checks', async () => {
    const { root, parts } = fakeOverview();
    await new OverviewPage({ api: new RunsApi(getJson), getJson, root }).load();
    expect(parts['[data-latest-run]'].innerHTML).toContain(`#runs/${NEWEST_MOCK_RUN}`);
    expect(parts['[data-services]'].innerHTML).toContain('Target API (Huron)');
  });
});
