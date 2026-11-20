/**
 * Tests the Bulk sync schedule page's script (dashboard/public/js/bulk-schedule.js) and what it takes from Bulk sync's
 * (bulk-sync.js): the When fieldset and the server's answer under it, reading When back as choices, Bulk sync's form
 * in schedule mode (no test, no Start from scratch, its own ids), the review in recurring words, the schedule's card
 * and the other landscapes', Bulk sync's warning of a schedule starting soon, and the page's load, review, save,
 * on/off and delete against stubbed routes. The scripts are browser code with no module system, so they run in a vm
 * context, as written and as the Lambda build minifies them, against a minimal stand-in for the DOM.
 */
import { readFileSync } from 'fs';
import { join } from 'path';
import { Script, createContext } from 'vm';
import { minifyJS } from '../dashboard/scripts/BuildTemplates';

const JS_DIR = join(__dirname, '../dashboard/public/js');
const SCRIPTS = ['page-state.js', 'run-history.js', 'bulk-sync.js', 'bulk-schedule.js'];
const CLASSES = ['BulkSyncView', 'BulkSyncPage', 'BulkScheduleView', 'BulkSchedulePage'];

/** Just enough of an element: markup, hidden, disabled, listeners, and fixed parts to find */
class FakeElement {
  textContent = '';
  hidden = false;
  disabled = false;
  dataset: Record<string, string> = {};
  listeners: Record<string, (event: any) => void> = {};
  parts: Record<string, any> = {};
  all: Record<string, any[]> = {};
  private markup = '';
  constructor(public tagName = 'div') {}
  get innerHTML() { return this.markup; }
  set innerHTML(markup: string) { this.markup = markup; }
  set className(_value: string) {}
  setAttribute() {}
  appendChild(child: FakeElement) { return child; }
  replaceChildren() { this.markup = ''; }
  addEventListener(type: string, listener: (event: any) => void) { this.listeners[type] = listener; }
  querySelector(selector: string) { return this.parts[selector] ?? null; }
  querySelectorAll(selector: string) { return this.all[selector] ?? []; }
}

const loadScripts = (minified = false): Record<string, any> => {
  const document = { createElement: (tag: string) => new FakeElement(tag) };
  const context = createContext({ URL, URLSearchParams, console, document });
  const source = SCRIPTS.map(file => readFileSync(join(JS_DIR, file), 'utf-8')).join('\n');
  const exports = CLASSES.map(name => `this.${name} = ${name};`).join('\n');
  new Script(`${minified ? minifyJS(source).minified : source}\n${exports}`).runInContext(context);
  return context as any;
};

/** Bulk sync's form state (BulkScheduleState.form), a mock landscape's when mock is true */
const formState = (mock = false) => ({
  landscape: mock ? 'mock' : 'preview',
  defaults: { runType: 'full', bulkReset: false, iterationLimit: 10, parallelStart: 15, processorScalingCapacity: 3 },
  iterationLimit: { max: 50 },
  parallelStart: { max: 15, enabled: true },
  processorScalingCapacity: { max: 20 },
  latestRun: { kind: 'stalled', syncRunId: '2026-11-06T17:12:48.824Z' },
  blocked: { code: 'RUN_UNDERWAY', message: 'A run is underway' },
  warnings: [],
  itemsPerChunk: 200,
  ...(mock ? {
    mock: {
      simulator: { available: true, defaults: { totalPopulation: 1950, simulatedDelaySeconds: 3, errorRate: 0, nonCurrentTermRate: 0 } },
      limits: { totalPopulation: 500000, simulatedDelaySeconds: 30 }
    }
  } : {})
});

const WHEN = { timezone: 'America/New_York', minSpacingMinutes: 60, defaults: { mode: 'simple', time: '04:00', days: 'every-day' } };

const schedule = (overrides: Record<string, any> = {}) => ({
  name: 'chunker-api-schedule-preview', enabled: true, expression: 'cron(0 4 ? * MON-FRI *)', timezone: 'America/New_York',
  description: 'At 04:00 AM, Monday through Friday (America/New_York)',
  nextRuns: ['Fri, Nov 20, 2026, 4:00 AM', 'Mon, Nov 23, 2026, 4:00 AM', 'Tue, Nov 24, 2026, 4:00 AM'],
  choices: { mode: 'simple', time: '04:00', days: 'weekdays' }, realSource: true,
  run: { runType: 'full', bulkReset: false, iterationLimit: 10, parallelStart: 5 },
  changedBy: 'jdoe', changedAt: '2026-11-18T15:00:00.000Z',
  ...overrides
});

/** A radio group or checkbox set, as form.elements gives it */
const group = (values: string[], checked: string[]) => values.map(value => ({ value, checked: checked.includes(value) }));

describe.each([false, true])('bulk-schedule.js (minified: %s)', minified => {
  const { BulkSyncView, BulkSyncPage, BulkScheduleView, BulkSchedulePage } = loadScripts(minified);

  describe('When', () => {
    it('shows a time and days, the chosen days, and the time zone and spacing', () => {
      const html = BulkScheduleView.whenHtml({ mode: 'simple', time: '03:15', days: ['MON', 'THU'] }, WHEN);
      expect(html).toMatch(/name="whenTime" id="schedule-whenTime" value="03:15"/);
      expect(html).toMatch(/<option value="chosen" selected>Chosen days<\/option>/);
      expect(html).toMatch(/id="schedule-whenDay-MON" value="MON" checked/);
      expect(html).toMatch(/id="schedule-whenDay-THU" value="THU" checked/);
      expect(html).not.toMatch(/id="schedule-whenDay-TUE" value="TUE" checked/);
      expect(html).toMatch(/data-when-cron hidden/);
      expect(html).toMatch(/Times are America\/New_York time/);
      expect(html).toMatch(/at least 60 minutes apart/);
    });

    it('shows a cron expression typed in, with the simple fields hidden', () => {
      const html = BulkScheduleView.whenHtml({ mode: 'cron', expression: 'cron(0 4 L * ? *)' }, WHEN);
      expect(html).toMatch(/data-when-simple hidden/);
      expect(html).toMatch(/value="cron\(0 4 L \* \? \*\)"/);
      expect(html).toMatch(/id="schedule-whenMode-cron" value="cron" checked/);
    });

    it('reads the choices back, for the server to build and check', () => {
      const elements = (overrides: Record<string, any>) => ({
        whenMode: group(['simple', 'cron'], ['simple']), whenTime: { value: '04:30' }, whenDays: { value: 'weekdays' },
        whenDay: group(['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'], []), whenExpression: { value: '' }, ...overrides
      });
      expect(BulkScheduleView.readWhen({ elements: elements({}) })).toEqual({ mode: 'simple', time: '04:30', days: 'weekdays' });
      expect(BulkScheduleView.readWhen({ elements: elements({ whenDays: { value: 'chosen' }, whenDay: group(['SUN', 'MON', 'TUE'], ['SUN', 'TUE']) }) }))
        .toEqual({ mode: 'simple', time: '04:30', days: ['SUN', 'TUE'] });
      expect(BulkScheduleView.readWhen({ elements: elements({ whenMode: group(['simple', 'cron'], ['cron']), whenExpression: { value: ' cron(0 4 * * ? *) ' } }) }))
        .toEqual({ mode: 'cron', expression: 'cron(0 4 * * ? *)' });
    });

    it("shows the server's answer: the words, expression and next runs, a conflict, and notes", () => {
      const html = BulkScheduleView.previewHtml({
        expression: 'cron(30 2 * * ? *)', description: 'At 02:30 AM (America/New_York)', nextRuns: ['Fri, Nov 20, 2026, 2:30 AM'],
        conflict: { landscape: 'staging', description: 'At 03:00 AM', at: 'Fri, Nov 20, 2026, 2:30 AM', otherAt: 'Fri, Nov 20, 2026, 3:00 AM', minutesApart: 30 },
        notes: ['On Sun, Mar 14, 2027 the clocks go forward past 2:30 AM (America/New_York), so there is no run that day.']
      });
      expect(html).toMatch(/At 02:30 AM \(America\/New_York\)/);
      expect(html).toMatch(/Next: Fri, Nov 20, 2026, 2:30 AM\./);
      expect(html).toMatch(/Too close to staging&#39;s schedule|Too close to staging's schedule/);
      expect(html).toMatch(/30 minutes from this one/);
      expect(html).toMatch(/Daylight saving/);
    });

    it("shows the server's problems by field, and an expression that never runs", () => {
      expect(BulkScheduleView.previewHtml({ errors: [{ field: 'hours', message: "'25' isn't a value it allows (0-23)" }] }))
        .toMatch(/Not a schedule yet[\s\S]*hours: /);
      expect(BulkScheduleView.previewHtml({ expression: 'cron(0 4 30 2 ? *)', description: 'x', nextRuns: [] })).toMatch(/It never runs\./);
      expect(BulkScheduleView.previewHtml(undefined)).toBe('');
    });
  });

  describe("Bulk sync's form in schedule mode", () => {
    it('leaves out a test, Start from scratch, the stalled run and the guard on runs now, and uses its own ids', () => {
      const html = BulkScheduleView.runFormHtml(formState(true), undefined, { leadHtml: '<p data-lead></p>', tailHtml: BulkScheduleView.enabledHtml(true) });
      expect(html).not.toMatch(/value="test"/);
      expect(html).not.toMatch(/name="resetState"/);
      expect(html).not.toMatch(/name="buid"/);
      expect(html).not.toMatch(/overrideStalled/);
      expect(html).not.toMatch(/A run can&#39;t be started now|A run can't be started now/);
      expect(html).toMatch(/name="source" id="schedule-source-simulator"/);
      expect(html).not.toMatch(/(id|for)="bulk-/);
      expect(html.indexOf('data-lead')).toBeLessThan(html.indexOf('Run type'));
      expect(html).toMatch(/name="scheduleEnabled" id="schedule-scheduleEnabled" checked/);
    });

    it("keeps Bulk sync's own form as it was", () => {
      const html = BulkSyncView.formHtml(formState(true), undefined);
      expect(html).toMatch(/id="bulk-runType-test"/);
      expect(html).toMatch(/name="resetState"/);
      expect(html).toMatch(/overrideStalled/);
    });

    it('reads a schedule form without a BUID box or Start from scratch', () => {
      const fields: Record<string, any> = {
        runType: group(['full', 'full-no-deactivations'], ['full']), bulkReset: { checked: true }, custom: { checked: false },
        chunkMode: group(['perTask', 'single'], ['perTask']), source: group(['real', 'simulator'], ['real'])
      };
      expect(BulkSyncPage.readRequest({ elements: fields, dataset: { defaultParallel: '15' } }))
        .toEqual({ runType: 'full', bulkReset: true, parallelStart: 15, source: 'real', custom: false });
    });
  });

  describe('the review', () => {
    const plan = {
      landscape: 'preview',
      choices: { runType: 'full', bulkReset: false, trustPreviousStorage: true, iterationLimit: 10, parallelStart: 5, processorScalingCapacityOverride: 4 },
      event: { baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', startedBy: { by: 'schedule' } },
      settings: []
    };

    it("asks 'Schedule this run?' in recurring words, with Schedule run", () => {
      const html = BulkSyncView.confirmationHtml(plan, undefined, undefined, { schedule: true, when: 'At 04:00 AM, Monday through Friday (America/New_York)', whenHtml: '<p data-when></p>' });
      expect(html).toMatch(/Schedule this run\?/);
      expect(html).toMatch(/At 04:00 AM, Monday through Friday \(America\/New_York\): a full sync of everyone in the source/);
      expect(html).toMatch(/in each scheduled run/);
      expect(html).toMatch(/skipped that day/);
      expect(html).toMatch(/data-when/);
      expect(html).toMatch(/The event the schedule sends the chunker subscriber/);
      expect(html).toMatch(/fa-calendar-check[^<]*<\/i>Schedule run/);
      expect(html).toMatch(/data-action="confirm-start"/);
    });

    it('asks to save an existing schedule when told', () => {
      const html = BulkSyncView.confirmationHtml(plan, undefined, undefined, { schedule: true, when: 'x', title: 'Save this schedule?', button: 'Save schedule' });
      expect(html).toMatch(/Save this schedule\?/);
      expect(html).toMatch(/<\/i>Save schedule/);
    });

    it("keeps a start's review as it was", () => {
      const html = BulkSyncView.confirmationHtml(plan, undefined, undefined);
      expect(html).toMatch(/Start this run\?/);
      expect(html).toMatch(/Full sync of everyone in the source/);
      expect(html).toMatch(/for this run only/);
      expect(html).not.toMatch(/skipped that day/);
    });
  });

  describe("Bulk sync's warning of a schedule starting soon", () => {
    const upcoming = [
      { landscape: 'staging', thisLandscape: false, at: 'Fri, Nov 20, 2026, 3:00 AM', minutesAway: 25, description: 'At 03:00 AM' },
      { landscape: 'preview', thisLandscape: true, at: 'Fri, Nov 20, 2026, 3:40 AM', minutesAway: 1, description: 'At 03:40 AM' }
    ];

    it('names each, and says why it matters, in the form and the review', () => {
      const note = BulkSyncView.upcomingSchedulesHtml(upcoming);
      expect(note).toMatch(/A scheduled run starts soon/);
      expect(note).toMatch(/staging&#39;s scheduled run starts at Fri, Nov 20, 2026, 3:00 AM, in 25 minutes\.|staging's scheduled run starts at Fri, Nov 20, 2026, 3:00 AM, in 25 minutes\./);
      expect(note).toMatch(/This landscape&#39;s scheduled run|This landscape's scheduled run/);
      expect(note).toMatch(/in 1 minute\./);
      expect(BulkSyncView.formHtml({ ...formState(), upcomingSchedules: upcoming }, undefined)).toMatch(/A scheduled run starts soon/);
      expect(BulkSyncView.confirmationHtml({ landscape: 'preview', choices: { runType: 'full' }, event: {}, settings: [], upcomingSchedules: upcoming }))
        .toMatch(/A scheduled run starts soon/);
    });

    it('says nothing when there are none, or in a schedule\'s own form', () => {
      expect(BulkSyncView.upcomingSchedulesHtml(undefined)).toBe('');
      expect(BulkSyncView.upcomingSchedulesHtml([])).toBe('');
      expect(BulkScheduleView.runFormHtml({ ...formState(), upcomingSchedules: upcoming }, undefined, {})).not.toMatch(/A scheduled run starts soon/);
    });
  });

  describe('the card', () => {
    const state = (overrides: Record<string, any> = {}) => ({ landscape: 'preview', schedule: schedule(), form: formState(), when: WHEN, otherSchedules: [], ...overrides });

    it('shows it on, when, its next runs, the run it starts and who changed it, with Deactivate, Edit and Delete', () => {
      const html = BulkScheduleView.cardHtml(state());
      expect(html).toMatch(/status-badge-success">On</);
      expect(html).toMatch(/At 04:00 AM, Monday through Friday \(America\/New_York\)/);
      expect(html).toMatch(/cron\(0 4 \? \* MON-FRI \*\)/);
      expect(html).toMatch(/Next runs: Fri, Nov 20, 2026, 4:00 AM; Mon, Nov 23/);
      expect(html).toMatch(/It starts with 5 chunker tasks at once/);
      expect(html).toMatch(/Changed by jdoe on /);
      expect(html).toMatch(/<\/i>Deactivate/);
      expect(html).toMatch(/<\/i>Edit schedule/);
      expect(html).toMatch(/<\/i>Delete schedule/);
      expect(html).toMatch(/data-schedule-delete-confirm hidden/);
    });

    it('shows it off with Activate, a stale endpoint, and the last scheduled run skipped', () => {
      const html = BulkScheduleView.cardHtml(state({
        schedule: schedule({
          enabled: false,
          staleEndpoint: { scheduled: { baseUrl: 'https://old.example', fetchPath: '/people' }, configured: { baseUrl: 'https://new.example', fetchPath: '/people' } }
        }),
        lastRefusal: { refusedAt: '2026-11-19T09:00:00.000Z', reason: 'The scheduled run was skipped: run x is underway in this landscape' }
      }));
      expect(html).toMatch(/status-badge-warning">Off</);
      expect(html).toMatch(/It starts nothing while it is off/);
      expect(html).toMatch(/<\/i>Activate/);
      expect(html).toMatch(/endpoint the config no longer names/);
      expect(html).toMatch(/https:\/\/old\.example\/people/);
      expect(html).toMatch(/A scheduled run was skipped on /);
      expect(html).toMatch(/run x is underway/);
    });

    it("says a mock landscape's real-source run goes into its target simulator", () => {
      const html = BulkScheduleView.cardHtml(state({ landscape: 'mock', form: formState(true) }));
      expect(html).toMatch(/into mock&#39;s target simulator|into mock's target simulator/);
      expect(html).toMatch(/The source is the real source API/);
    });

    it("lists the other landscapes' schedules, or says there are none", () => {
      const html = BulkScheduleView.othersHtml([
        { landscape: 'staging', enabled: true, realSource: true, description: 'At 03:00 AM' },
        { landscape: 'mock1', enabled: false, realSource: false, description: 'At 04:30 AM' }
      ]);
      expect(html).toMatch(/<td>staging<\/td>/);
      expect(html).toMatch(/Real source/);
      expect(html).toMatch(/Source simulator/);
      expect(html).toMatch(/status-badge-warning">Off/);
      expect(BulkScheduleView.othersHtml([])).toMatch(/No other landscape/);
    });
  });

  describe('BulkSchedulePage', () => {
    /** The page's parts under its root, with the routes stubbed */
    const page = (answers: { state: any; plan?: any }) => {
      const main = new FakeElement();
      const confirm = new FakeElement();
      const preview = new FakeElement();
      const fields = new FakeElement('fieldset');
      const submit = new FakeElement('button');
      const formError = new FakeElement('p');
      const cardError = new FakeElement('p');
      main.parts = {
        '[data-bulk-confirm]': confirm, '[data-schedule-preview]': preview, '[data-bulk-fields]': fields,
        '[data-bulk-confirm] button[type="submit"]': submit, '[data-bulk-form-error]': formError, '[data-schedule-error]': cardError
      };
      main.all = { '[data-schedule-actions] button': [new FakeElement('button')], '[data-bulk-confirm] button': [new FakeElement('button')] };
      const parts: Record<string, any> = {
        '[data-schedule-main]': main, '[data-schedule-others]': new FakeElement(),
        '[data-refresh-stamp="schedule"]': new FakeElement('span'), '[data-action="refresh-schedule"]': new FakeElement('button')
      };
      const calls: any[] = [];
      const getJson = jest.fn(async (path: string) => { calls.push(['GET', path]); return answers.state; });
      const postJson = jest.fn(async (path: string, body: any) => {
        calls.push(['POST', path, body]);
        return path.endsWith('/plan') ? answers.plan : path.endsWith('/when') ? { expression: 'cron(0 4 * * ? *)', description: 'At 04:00 AM', nextRuns: [] } : {};
      });
      const sendJson = jest.fn(async (method: string, path: string, body?: any) => { calls.push([method, path, body]); return {}; });
      const instance = new BulkSchedulePage({ getJson, postJson, sendJson, root: { querySelector: (selector: string) => parts[selector] || null } });
      return { instance, main, confirm, calls, formError, fields };
    };
    const state = (withSchedule: boolean) => ({
      landscape: 'preview', form: formState(), when: WHEN, otherSchedules: [], ...(withSchedule ? { schedule: schedule() } : {})
    });

    it('loads the card when there is a schedule, and turns it off', async () => {
      const { instance, main, calls } = page({ state: state(true) });
      expect(await instance.load()).toBe(true);
      expect(main.innerHTML).toMatch(/data-schedule-card/);
      expect(await instance.toggle()).toBe(true);
      expect(calls).toContainEqual(['POST', '/api/bulk-schedule/state', { enabled: false }]);
    });

    it('loads the form when there is none, reviews the choices, and saves them', async () => {
      const plan = {
        landscape: 'preview', enabled: true, replaces: false,
        when: { expression: 'cron(0 4 ? * MON-FRI *)', description: 'At 04:00 AM, Monday through Friday (America/New_York)', nextRuns: ['Fri, Nov 20, 2026, 4:00 AM'] },
        run: { choices: { runType: 'full', bulkReset: false, iterationLimit: 10, parallelStart: 15 }, event: { startedBy: { by: 'schedule' } }, settings: [] }
      };
      const { instance, main, confirm, calls } = page({ state: state(false), plan });
      await instance.load();
      expect(main.innerHTML).toMatch(/data-schedule-when/);
      expect(main.innerHTML).toMatch(/Review run/);
      const form = {
        dataset: { defaultParallel: '15' },
        elements: {
          runType: group(['full', 'full-no-deactivations'], ['full']), bulkReset: { checked: false }, custom: { checked: false },
          chunkMode: group(['perTask', 'single'], ['perTask']), scheduleEnabled: { checked: true },
          whenMode: group(['simple', 'cron'], ['simple']), whenTime: { value: '04:00' }, whenDays: { value: 'weekdays' }, whenDay: [], whenExpression: { value: '' }
        }
      };
      expect(await instance.review(form)).toBe(true);
      const body = { when: { mode: 'simple', time: '04:00', days: 'weekdays' }, run: { runType: 'full', bulkReset: false, parallelStart: 15 }, enabled: true };
      expect(calls).toContainEqual(['POST', '/api/bulk-schedule/plan', body]);
      expect(confirm.innerHTML).toMatch(/Schedule this run\?/);
      expect(confirm.innerHTML).toMatch(/<\/i>Schedule run/);
      expect(await instance.confirm()).toBe(true);
      expect(calls).toContainEqual(['PUT', '/api/bulk-schedule', body]);
    });

    it('shows a refused review under the form, and unlocks it', async () => {
      const { instance, formError, fields } = page({ state: state(false) });
      await instance.load();
      instance.postJson = jest.fn(async (path: string) => {
        if (path.endsWith('/plan')) throw new Error("staging's schedule starts at 3:00 AM");
        return {};
      });
      const form = {
        dataset: {}, elements: {
          runType: group(['full'], ['full']), bulkReset: { checked: false }, custom: { checked: false }, chunkMode: group(['perTask'], ['perTask']),
          scheduleEnabled: { checked: true }, whenMode: group(['simple'], ['simple']), whenTime: { value: '03:30' }, whenDays: { value: 'every-day' }
        }
      };
      expect(await instance.review(form)).toBe(false);
      expect(formError.textContent).toBe("The schedule can't be saved: staging's schedule starts at 3:00 AM");
      expect(fields.disabled).toBe(false);
    });

    it('edits the schedule from its choices and run, with Custom settings open', async () => {
      const { instance, main } = page({ state: state(true) });
      await instance.load();
      instance.edit();
      expect(main.innerHTML).toMatch(/id="schedule-whenDays"[\s\S]*<option value="weekdays" selected>/);
      expect(main.innerHTML).toMatch(/name="custom" id="schedule-custom" checked/);
      expect(main.innerHTML).toMatch(/<option value="5" selected>5<\/option>/);
      expect(main.innerHTML).toMatch(/Cancel editing/);
    });

    it('deletes the schedule, and loads again', async () => {
      const { instance, calls } = page({ state: state(true) });
      await instance.load();
      expect(await instance.remove()).toBe(true);
      expect(calls).toContainEqual(['DELETE', '/api/bulk-schedule', undefined]);
      expect(calls.filter(([method]) => method === 'GET')).toHaveLength(2);
    });
  });
});
