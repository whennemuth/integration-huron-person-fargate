/**
 * Tests the Bulk sync page's script (dashboard/public/js/bulk-sync.js): the form (run type, test, bulk reset, custom
 * settings) and what it says about the newest run and the guards, the review the server builds (every setting of
 * the run), the checks made before sending, and the page's load, review, start and finding of the started run. The
 * script is browser code with no module system, so it runs in a vm context (as written and as the Lambda build
 * minifies it), after the scripts it builds on, against a minimal stand-in for the DOM; and once against the
 * dashboard routes with the mock services.
 */
import { readFileSync } from 'fs';
import { join } from 'path';
import { Script, createContext } from 'vm';
import { minifyJS } from '../dashboard/scripts/BuildTemplates';
import { handler } from '../dashboard/src/handlers/DashboardHandler';
import { LambdaFunctionUrlEvent } from '../dashboard/src/handlers/DashboardTypes';

const JS_DIR = join(__dirname, '../dashboard/public/js');
const SCRIPTS = ['page-state.js', 'run-history.js', 'bulk-sync.js'];
const CLASSES = ['RunsApi', 'RunView', 'RunFormat', 'BulkSyncView', 'BulkSyncPage'];

/** Just enough of an element: markup, text, hidden, disabled, children, listeners, and fixed parts to find */
class FakeElement {
  textContent = '';
  hidden = false;
  disabled = false;
  dataset: Record<string, string> = {};
  children: FakeElement[] = [];
  listeners: Record<string, (event: any) => void> = {};
  parts: Record<string, FakeElement> = {};
  buttons: FakeElement[] = [];
  private markup = '';
  constructor(public tagName = 'div') {}
  get innerHTML() { return this.markup; }
  set innerHTML(markup: string) { this.markup = markup; this.children = []; }
  set className(_value: string) {}
  setAttribute() {}
  appendChild(child: FakeElement) { this.children.push(child); return child; }
  replaceChildren(...children: FakeElement[]) { this.children = children; this.markup = ''; }
  addEventListener(type: string, listener: (event: any) => void) { this.listeners[type] = listener; }
  querySelector(selector: string) { return this.parts[selector] ?? null; }
  querySelectorAll() { return this.buttons; }
  get stateText(): string {
    const text = (element: FakeElement): string => [element.textContent, ...element.children.map(text)].join(' ');
    return text(this).replace(/\s+/g, ' ').trim();
  }
}

/** The Bulk sync partial's parts, as BulkSyncPage finds them under its root */
const fakeBulkPage = () => {
  const form = new FakeElement();
  form.parts = {
    '[data-bulk-form-error]': new FakeElement('p'), '[data-bulk-confirm]': new FakeElement(), '[data-bulk-start-error]': new FakeElement('p'),
    '[data-bulk-fields]': new FakeElement('fieldset'), '[data-bulk-confirm] button[type="submit"]': new FakeElement('button')
  };
  form.buttons = [new FakeElement('button'), new FakeElement('button')];
  const parts: Record<string, FakeElement> = {
    '[data-bulk-form]': form,
    '[data-bulk-status]': new FakeElement(),
    '[data-refresh-stamp="bulk"]': new FakeElement('span'),
    '[data-action="refresh-bulk"]': new FakeElement('button')
  };
  return { root: { querySelector: (selector: string) => parts[selector] || null }, parts, form };
};

const loadScripts = (minified = false): Record<string, any> => {
  const document = { createElement: (tag: string) => new FakeElement(tag) };
  const context = createContext({ URL, URLSearchParams, console, document });
  const source = SCRIPTS.map(file => readFileSync(join(JS_DIR, file), 'utf-8')).join('\n');
  const exports = CLASSES.map(name => `this.${name} = ${name};`).join('\n');
  new Script(`${minified ? minifyJS(source).minified : source}\n${exports}`).runInContext(context);
  return context as any;
};

const NOW = Date.parse('2026-11-10T12:00:00.000Z');
const finished = { kind: 'finished', syncRunId: '2026-11-09T02:00:00.000Z' };
const stalled = { kind: 'stalled', syncRunId: '2026-11-06T17:12:48.824Z', lastChunkAt: '2026-11-07T04:59:44.820Z' };
const state = (overrides: Record<string, any> = {}) => ({
  landscape: 'preview',
  defaults: { runType: 'full', bulkReset: false, iterationLimit: 10, parallelStart: 0 },
  iterationLimit: { max: 50 },
  parallelStart: { max: 20, enabled: false },
  latestRun: finished,
  warnings: [],
  ...overrides
});

/** What POST /api/bulk-sync/plan answers: the choices, the event and a few settings */
const plan = (choices: Record<string, any>) => ({
  landscape: 'preview',
  choices: { bulkReset: false, trustPreviousStorage: true, iterationLimit: 10, parallelStart: 0, overrideStalled: false, ...choices },
  event: {
    baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: 'person-full', bulkReset: false,
    trustPreviousStorage: true, iterationLimit: 0, offset: 0, processingMetadata: { processedAt: 'set when the run is started', processorVersion: '1.0.0' }
  },
  settings: [
    { name: 'populationType', value: 'person-full', source: 'form', note: 'Full sync' },
    { name: 'messagesToPrepopulate', value: '0', source: 'form' },
    { name: 'clusterName', value: null, source: 'unused', note: 'Only for a parallel start' }
  ]
});

describe.each([false, true])('bulk-sync.js (minified: %s)', minified => {
  const { BulkSyncView, BulkSyncPage, RunFormat } = loadScripts(minified);

  describe('BulkSyncView', () => {
    it('names the newest run in the status panel, linked to its page', () => {
      expect(BulkSyncView.latestRunHtml({ kind: 'none' })).toContain('No runs yet in this landscape.');
      const html = BulkSyncView.latestRunHtml(finished);
      expect(html).toContain('href="#runs/2026-11-09T02:00:00.000Z"');
      expect(html).toContain('finished.');
      expect(BulkSyncView.latestRunHtml(stalled)).toContain('has stalled without finishing.');
    });

    it("starts the form at the scheduled run's choices, with the test and custom fields hidden", () => {
      const html = BulkSyncView.formHtml(state());
      expect(html).toContain('Full sync (with deactivations)');
      expect(html).toContain('Full sync (no deactivations)');
      expect(html).toMatch(/value="full" checked/);
      expect(html).not.toMatch(/value="test" checked/);
      // Delta sync is shown, but can't be chosen until a source of only the people who changed exists
      expect(html).toMatch(/value="delta" disabled/);
      expect(html).toContain('a source of only the people who changed does not exist yet');
      expect(html).not.toContain('schedule');
      // 10 chunks per task, from 1 to 50
      expect(html).toMatch(/value="perTask" checked/);
      expect(html).not.toMatch(/data-bulk-chunks hidden/);
      expect(html).toMatch(/<select class="form-select bulk-number" name="chunksPerTask" id="bulk-chunksPerTask">\s*<option value="1">1<\/option>/);
      expect(html).toContain('<option value="10" selected>10</option>');
      expect(html).toContain('<option value="50">50</option>');
      expect(html).not.toContain('<option value="51">');
      expect(html).toContain('the defaults are used: 10 chunks per task, and a start with one message');
      // Custom settings is named once, by its checkbox: its fields' group has no heading of its own
      expect(html.match(/>Custom settings</g)).toHaveLength(1);
      expect(html).toContain('<legend class="form-label">Chunker tasks</legend>');
      expect(html).toContain('One task reads every chunk itself. With no other task to share the work, reading the source takes much longer.');
      expect(html).not.toMatch(/name="bulkReset" id="bulk-bulkReset" checked/);
      expect(html).not.toContain('trustPreviousStorage');
      expect(html).toMatch(/data-bulk-test hidden/);
      expect(html).toMatch(/data-bulk-custom aria-label="Custom settings" hidden/);
      expect(html).not.toContain('overrideStalled');
      expect(html).toMatch(/<button class="btn btn-primary" type="submit">/);
    });

    it('offers parallel start from 0 to 20, read-only until queue seeding exists', () => {
      const html = BulkSyncView.formHtml(state());
      expect(html).toMatch(/name="parallelStart" id="bulk-parallelStart" disabled/);
      expect(html).toMatch(/id="bulk-parallelStart" disabled>\s*<option value="0" selected>0<\/option>/);
      expect(html).toContain('<option value="20">20</option>');
      expect(html).toContain('Read-only at 0 until shared queue seeding is built.');
      expect(BulkSyncView.formHtml(state({ parallelStart: { max: 20, enabled: true } }))).not.toMatch(/id="bulk-parallelStart" disabled/);
    });

    it('keeps what was entered, showing the BUID box for a test and the custom fields when chosen', () => {
      const html = BulkSyncView.formHtml(state(), { runType: 'test', buid: 'U1<2', bulkReset: true, custom: true, iterationLimit: 7 });
      expect(html).toMatch(/value="test" checked/);
      expect(html).toContain('value="U1&lt;2"');
      expect(html).not.toMatch(/data-bulk-test hidden/);
      expect(html).toContain('A test puts a single person through the whole sync');
      expect(html).toMatch(/name="bulkReset" id="bulk-bulkReset" checked/);
      expect(html).not.toMatch(/data-bulk-custom aria-label="Custom settings" hidden/);
      expect(html).toMatch(/name="custom" id="bulk-custom" checked/);
      expect(html).toContain('<option value="7" selected>7</option>');

      // A single task: its choice checked, and the dropdown hidden at its default
      const single = BulkSyncView.formHtml(state(), { runType: 'full', custom: true, iterationLimit: 0 });
      expect(single).toMatch(/value="single" checked/);
      expect(single).toMatch(/data-bulk-chunks hidden/);
      expect(single).toContain('<option value="10" selected>10</option>');
    });

    it('explains bulk reset in plain words', () => {
      const html = BulkSyncView.formHtml(state());
      expect(html).toContain('keeps a fingerprint of their record');
      expect(html).not.toContain('hash');
    });

    it('says why a start is refused now and disables the button, but offers the override for a stalled run', () => {
      const notConfigured = BulkSyncView.formHtml(state({ blocked: { code: 'NOT_CONFIGURED', message: 'missing CHUNKER_FUNCTION_NAME' } }));
      expect(notConfigured).toContain('A run can&#39;t be started now');
      expect(notConfigured).toContain('missing CHUNKER_FUNCTION_NAME');
      expect(notConfigured).toMatch(/type="submit" disabled/);

      const html = BulkSyncView.formHtml(state({ latestRun: stalled, blocked: { code: 'RUN_STALLED', message: 'stalled' } }));
      expect(html).toContain('name="overrideStalled"');
      expect(html).toContain('Start despite the stalled run');
      expect(html).not.toMatch(/type="submit" disabled/);
      expect(html).not.toContain('A run can&#39;t be started now');
    });

    it('shows the warnings before the form', () => {
      expect(BulkSyncView.formHtml(state({ warnings: ['mock is a mock landscape'] }))).toContain('mock is a mock landscape');
    });

    it('spells out what a run will do', () => {
      const lines = (choices: any, latestRun: any = finished) => BulkSyncView.summaryLines(choices, 'preview', latestRun);
      expect(lines({ runType: 'full', bulkReset: true, iterationLimit: 10 })).toEqual([
        "Full sync of everyone in the source, into preview's Huron. People no longer in the source are deactivated afterward.",
        'Bulk reset: everyone is looked up in Huron and created or updated as needed.',
        'Each chunker task reads 10 chunks, then hands the rest on to a new task.'
      ]);
      expect(lines({ runType: 'full-no-deactivations', bulkReset: false, iterationLimit: 0 })).toEqual([
        "Full sync of everyone in the source, into preview's Huron. No one is deactivated.",
        'People whose record has not changed since their last sync are skipped, and Huron is not contacted for them.',
        'A single chunker task reads the whole source.'
      ]);
      expect(lines({ runType: 'test', buid: 'U12345678', bulkReset: false, iterationLimit: 10, overrideStalled: true }, stalled)).toEqual([
        "Test of U12345678 alone, into preview's Huron. No one is deactivated.",
        'People whose record has not changed since their last sync are skipped, and Huron is not contacted for them.',
        `The stalled run of ${RunFormat.dateTime(stalled.syncRunId)} is left as it is.`
      ]);
    });

    it('reviews what will run and the event the chunker subscriber will be sent, and nothing more', () => {
      const html = BulkSyncView.confirmationHtml(plan({ runType: 'full' }), finished);
      expect(html).toContain('The event sent to the chunker subscriber');
      expect(html.match(/<table /g)).toHaveLength(1);
      expect(html).toMatch(/<tr><th scope="col">Setting<\/th><th scope="col">Value<\/th><\/tr>/);
      expect(html).toContain('<code>fetchPath</code>');
      expect(html).toContain('<code>/api/people</code>');
      expect(html).toContain('{&quot;processedAt&quot;:&quot;set when the run is started&quot;');
      // The plan's settings aren't shown
      expect(html).not.toContain('messagesToPrepopulate');
      expect(html).not.toContain('clusterName');
      expect(html).toContain('data-action="confirm-start"');
    });

    it("refuses a request the server would refuse as invalid, before it's sent", () => {
      expect(BulkSyncView.requestError({ runType: 'test', buid: '' })).toBe('A test needs the BUID of the person to sync.');
      expect(BulkSyncView.requestError({ runType: 'test', buid: 'B123' })).toBe("A BUID is a U followed by digits (got 'B123').");
      expect(BulkSyncView.requestError({ runType: 'full', iterationLimit: NaN })).toBe('Choose how many chunks each chunker task reads, or a single task.');
      expect(BulkSyncView.requestError({ runType: 'full', iterationLimit: -2 })).toBe('Choose how many chunks each chunker task reads, or a single task.');
      expect(BulkSyncView.requestError({ runType: 'test', buid: 'U123', iterationLimit: 3 })).toBeUndefined();
      expect(BulkSyncView.requestError({ runType: 'full' })).toBeUndefined();
    });

    it('says a started run will appear, then links to it once it has', () => {
      const result = { startedAt: '2026-11-10T11:58:00.000Z', run: { population: 'person-delta', buid: 'U12345678' } };
      const waiting = BulkSyncView.startedHtml({ result }, NOW);
      expect(waiting).toContain('Started 2 min ago: Test of U12345678.');
      expect(waiting).toContain('Refresh to look for it.');
      const run = { syncRunId: '2026-11-10T11:58:20.000Z', startedAt: '2026-11-10T11:58:20.000Z', status: 'INCOMPLETE', populationLabel: 'Person U12345678' };
      const found = BulkSyncView.startedHtml({ result, run }, NOW);
      expect(found).toContain('href="#runs/2026-11-10T11:58:20.000Z"');
      expect(found).toContain('status-badge-warning');
      const full = { startedAt: '2026-11-10T11:58:00.000Z', run: { runType: 'full-no-deactivations', population: 'person-delta' } };
      expect(BulkSyncView.startedHtml({ result: full }, NOW)).toContain('Started 2 min ago: Full sync (no deactivations).');
    });
  });

  describe('BulkSyncPage', () => {
    const page = (options: Record<string, any> = {}) => {
      const { root, parts, form } = fakeBulkPage();
      const getJson = options.getJson ?? jest.fn(async () => state());
      const postJson = options.postJson ?? jest.fn(async (path: string, request: any) => path === '/api/bulk-sync/plan'
        ? plan(request)
        : { status: 'started', startedAt: '2026-11-10T11:58:00.000Z', landscape: 'preview', run: { ...request, population: 'person-delta' } });
      const runs = options.runs ?? { listRuns: jest.fn(async () => ({ items: [] })) };
      return { bulkPage: new BulkSyncPage({ getJson, postJson, runs, root, now: () => NOW }), parts, form, getJson, postJson, runs };
    };

    it('loads the state into the form and the status panel, and stamps the refresh', async () => {
      const { bulkPage, parts, form } = page();
      expect(await bulkPage.load()).toBe(true);
      expect(form.innerHTML).toContain('data-bulk-sync-form');
      expect(parts['[data-bulk-status]'].innerHTML).toContain('finished.');
      expect(parts['[data-refresh-stamp="bulk"]'].textContent).toMatch(/^Refreshed /);
    });

    it("says so when the state can't be read", async () => {
      const { bulkPage, form } = page({ getJson: jest.fn(async () => { throw new Error('The statistics table could not be read'); }) });
      expect(await bulkPage.load()).toBe(false);
      expect(form.stateText).toContain('Bulk sync could not be read');
      expect(form.stateText).toContain('The statistics table could not be read');
    });

    it("stops at an invalid request, else asks the server for the review and shows it, the fields locked", async () => {
      const { bulkPage, form, postJson } = page();
      await bulkPage.load();
      expect(await bulkPage.review({ runType: 'test', buid: '', custom: false })).toBe(false);
      expect(form.parts['[data-bulk-form-error]']).toMatchObject({ hidden: false, textContent: 'A test needs the BUID of the person to sync.' });
      expect(postJson).not.toHaveBeenCalled();

      expect(await bulkPage.review({ runType: 'test', buid: 'U1', bulkReset: true, custom: false })).toBe(true);
      // custom only redraws the form: it isn't sent
      expect(postJson).toHaveBeenCalledWith('/api/bulk-sync/plan', { runType: 'test', buid: 'U1', bulkReset: true });
      expect(form.parts['[data-bulk-form-error]'].hidden).toBe(true);
      expect(form.parts['[data-bulk-confirm]'].innerHTML).toContain('Test of U1 alone, into preview&#39;s Huron. No one is deactivated.');
      // What is started is what was reviewed: the fields are locked meanwhile
      expect(form.parts['[data-bulk-fields]'].disabled).toBe(true);
    });

    it("shows a review the server refuses under the fields, and unlocks them", async () => {
      const refusal = Object.assign(new Error('The run of x is still underway: wait for it to finish'), { status: 409, code: 'RUN_UNDERWAY' });
      const { bulkPage, form } = page({ postJson: jest.fn(async () => { throw refusal; }) });
      await bulkPage.load();
      expect(await bulkPage.review({ runType: 'full', bulkReset: false })).toBe(false);
      expect(form.parts['[data-bulk-form-error]']).toMatchObject({
        hidden: false, textContent: "The run can't be started: The run of x is still underway: wait for it to finish"
      });
      expect(form.parts['[data-bulk-fields]'].disabled).toBe(false);
      expect(bulkPage.request).toBeUndefined();
    });

    it('starts the reviewed run, then links to it once it is the newest run', async () => {
      const newest = { syncRunId: '2026-11-10T11:58:20.000Z', startedAt: '2026-11-10T11:58:20.000Z', status: 'INCOMPLETE', populationLabel: 'Person U1' };
      const listRuns = jest.fn().mockResolvedValueOnce({ items: [{ syncRunId: '2026-11-09T02:00:00.000Z' }] }).mockResolvedValue({ items: [newest] });
      const { bulkPage, parts, postJson } = page({ runs: { listRuns } });
      await bulkPage.load();
      await bulkPage.review({ runType: 'test', buid: 'U1', bulkReset: true, iterationLimit: 2, parallelStart: 0, custom: true });

      expect(await bulkPage.confirm()).toBe(true);
      expect(postJson).toHaveBeenLastCalledWith('/api/bulk-sync', { runType: 'test', buid: 'U1', bulkReset: true, iterationLimit: 2, parallelStart: 0 });
      // The newest run is still the older one: the run hasn't appeared yet
      expect(parts['[data-bulk-status]'].innerHTML).toContain('Refresh to look for it.');

      await bulkPage.load();
      expect(parts['[data-bulk-status]'].innerHTML).toContain('href="#runs/2026-11-10T11:58:20.000Z"');
    });

    it("shows a refused start under the review, and doesn't count it as started", async () => {
      const refusal = Object.assign(new Error('The run of x is still underway: wait for it to finish'), { status: 409, code: 'RUN_UNDERWAY' });
      const postJson = jest.fn(async (path: string, request: any) => {
        if (path === '/api/bulk-sync/plan') return plan(request);
        throw refusal;
      });
      const { bulkPage, form } = page({ postJson });
      await bulkPage.load();
      await bulkPage.review({ runType: 'full', bulkReset: false });
      expect(await bulkPage.confirm()).toBe(false);
      expect(form.parts['[data-bulk-start-error]']).toMatchObject({
        hidden: false, textContent: "The run wasn't started: The run of x is still underway: wait for it to finish"
      });
      expect(form.buttons.every(button => !button.disabled)).toBe(true);
      expect(bulkPage.started).toBeUndefined();
    });

    it("reads the request from the form's fields: the BUID only for a test, the custom settings only when chosen", () => {
      const radio = (value: string, checked: boolean) => ({ value, checked });
      const fields = (params: { runType: string; buid?: string; custom?: boolean; chunkMode?: string; chunksPerTask?: string; overrideStalled?: boolean }) => ({
        elements: {
          runType: ['full', 'full-no-deactivations', 'delta', 'test'].map(type => radio(type, type === params.runType)),
          buid: { value: params.buid ?? '' }, bulkReset: { checked: true }, custom: { checked: !! params.custom },
          chunkMode: ['perTask', 'single'].map(mode => radio(mode, mode === (params.chunkMode ?? 'perTask'))),
          chunksPerTask: { value: params.chunksPerTask ?? '10' }, parallelStart: { value: '0' },
          ...(params.overrideStalled === undefined ? {} : { overrideStalled: { checked: params.overrideStalled } })
        }
      });
      expect(BulkSyncPage.readRequest(fields({ runType: 'full', buid: 'U9' }))).toEqual({ runType: 'full', bulkReset: true, custom: false });
      expect(BulkSyncPage.readRequest(fields({ runType: 'test', buid: ' u123 ', custom: true, chunksPerTask: '3', overrideStalled: true }))).toEqual({
        runType: 'test', buid: 'U123', bulkReset: true, iterationLimit: 3, parallelStart: 0, overrideStalled: true, custom: true
      });
      // A single task is iterationLimit 0, whatever the dropdown says
      expect(BulkSyncPage.readRequest(fields({ runType: 'full-no-deactivations', custom: true, chunkMode: 'single', chunksPerTask: '25' })))
        .toEqual({ runType: 'full-no-deactivations', bulkReset: true, iterationLimit: 0, parallelStart: 0, custom: true });
    });

    it('shows the BUID box for a test, the custom fields while Custom settings is checked, and the dropdown for chunks per task', () => {
      const { bulkPage } = page();
      const test = new FakeElement();
      const custom = new FakeElement();
      const chunks = new FakeElement();
      const form = { querySelector: (selector: string) => ({ '[data-bulk-test]': test, '[data-bulk-custom]': custom, '[data-bulk-chunks]': chunks } as any)[selector] };
      bulkPage.onChange({ target: { form, name: 'chunkMode', value: 'single' } });
      expect(chunks.hidden).toBe(true);
      bulkPage.onChange({ target: { form, name: 'chunkMode', value: 'perTask' } });
      expect(chunks.hidden).toBe(false);
      bulkPage.onChange({ target: { form, name: 'runType', value: 'test' } });
      expect(test.hidden).toBe(false);
      bulkPage.onChange({ target: { form, name: 'runType', value: 'full' } });
      expect(test.hidden).toBe(true);
      bulkPage.onChange({ target: { form, name: 'custom', checked: false } });
      expect(custom.hidden).toBe(true);
      bulkPage.onChange({ target: { form, name: 'custom', checked: true } });
      expect(custom.hidden).toBe(false);
    });
  });
});

describe('bulk-sync.js against the dashboard routes and mock services', () => {
  const { RunsApi, BulkSyncPage } = loadScripts();

  /** dashboard.js's fetchSectionData and postSectionData, over the handler instead of fetch */
  const call = async (path: string, method = 'GET', body?: unknown): Promise<any> => {
    const result = await handler({
      rawPath: path, headers: {}, body: body === undefined ? undefined : JSON.stringify(body),
      queryStringParameters: Object.fromEntries(new URL(path, 'https://dashboard.example.com').searchParams),
      requestContext: { http: { method, path: new URL(path, 'https://dashboard.example.com').pathname } }, isBase64Encoded: false
    } as unknown as LambdaFunctionUrlEvent);
    const data = result.body ? JSON.parse(`${result.body}`) : {};
    if (result.statusCode >= 400) {
      throw Object.assign(new Error(data.message || data.error), { status: result.statusCode, code: data.code });
    }
    return data;
  };

  beforeAll(() => { process.env.LOCALHOST_WITH_MOCKS = 'true'; });
  afterAll(() => { delete process.env.LOCALHOST_WITH_MOCKS; });
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  it('loads the mock state, reviews a test with its event, and starts it', async () => {
    const { root, parts, form } = fakeBulkPage();
    const bulkPage = new BulkSyncPage({
      getJson: (path: string) => call(path), postJson: (path: string, body: unknown) => call(path, 'POST', body),
      runs: new RunsApi((path: string) => call(path)), root
    });
    expect(await bulkPage.load()).toBe(true);
    expect(parts['[data-bulk-status]'].innerHTML).toContain('finished.');
    expect(await bulkPage.review({ runType: 'test', buid: 'U0000042', bulkReset: true, custom: false })).toBe(true);
    const review = form.parts['[data-bulk-confirm]'].innerHTML;
    expect(review).toContain('<code>/api/person?buid=U0000042</code>');
    expect(review).toContain('<code>trustPreviousStorage</code>');
    expect(await bulkPage.confirm()).toBe(true);
    expect(bulkPage.started.result).toMatchObject({ status: 'started', run: { population: 'person-delta', buid: 'U0000042' } });
  });
});
