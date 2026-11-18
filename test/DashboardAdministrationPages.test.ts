/**
 * The Administration scripts (dashboard/public/js/administration.js: WhoAmI, AccessPage, AuditTrailPage) and the
 * redacted state every section shows for a 403 (PageState.failed(), page-state.js). Browser code with no module
 * system, so it runs in a vm context (as written and as the Lambda build minifies it), after the scripts it builds on,
 * against a minimal stand-in for the DOM.
 */
import { readFileSync } from 'fs';
import { join } from 'path';
import { Script, createContext } from 'vm';
import { minifyJS } from '../dashboard/scripts/BuildTemplates';

const JS_DIR = join(__dirname, '../dashboard/public/js');
const SCRIPTS = ['page-state.js', 'run-history.js', 'administration.js'];
const CLASSES = ['PageState', 'WhoAmI', 'AccessPage', 'AuditTrailPage'];

/** Just enough of an element: classes, attributes, text, markup, children and listeners */
class FakeElement {
  className = '';
  textContent = '';
  hidden = false;
  dataset: Record<string, string> = {};
  attributes: Record<string, string> = {};
  children: FakeElement[] = [];
  listeners: Record<string, (event?: any) => void> = {};
  private markup = '';
  constructor(public tagName = 'div') {}
  get innerHTML() { return this.markup; }
  set innerHTML(markup: string) { this.markup = markup; this.children = []; }
  setAttribute(name: string, value: string) { this.attributes[name] = value; }
  appendChild(child: FakeElement) { this.children.push(child); return child; }
  replaceChildren(...children: FakeElement[]) { this.children = children; this.markup = ''; }
  addEventListener(type: string, listener: (event?: any) => void) { this.listeners[type] = listener; }
  querySelector(): FakeElement | null { return null; }
  querySelectorAll(): FakeElement[] { return []; }
  /** The text this element shows, its children's included */
  get text(): string {
    const text = (element: FakeElement): string => [element.textContent, ...element.children.map(text)].join(' ');
    return text(this).replace(/\s+/g, ' ').trim();
  }
}

const loadScripts = (minified = false): Record<string, any> => {
  const document = { createElement: (tag: string) => new FakeElement(tag) };
  const context = createContext({ URL, URLSearchParams, FormData: class {}, console, document, window: {} });
  const source = SCRIPTS.map(file => readFileSync(join(JS_DIR, file), 'utf-8')).join('\n');
  const exports = CLASSES.map(name => `this.${name} = ${name};`).join('\n');
  new Script(`${minified ? minifyJS(source).minified : source}\n${exports}`).runInContext(context);
  return context as any;
};

/** A 403 as fetchSectionData() throws it */
const forbidden = (action = 'runs:read') => Object.assign(new Error(`None of your roles (Viewer) has a policy that grants ${action}.`), { status: 403, action });

describe.each([false, true])('administration.js (minified: %s)', minified => {
  const scripts = loadScripts(minified);
  const { PageState, WhoAmI, AccessPage, AuditTrailPage } = scripts;

  describe('PageState.failed', () => {
    it('shows a 403 as redacted, naming the action the roles lack', () => {
      const container = new FakeElement();
      PageState.failed(container, 'Runs could not be loaded', forbidden('audit:read'));
      const [state] = container.children;
      expect(state.dataset.state).toBe('redacted');
      expect(state.text).toContain('Content redacted');
      expect(state.text).toContain('This content is redacted. It needs audit:read, which none of your roles grants.');
    });

    it('shows any other failure as the error state with the server\'s message', () => {
      const container = new FakeElement();
      PageState.failed(container, 'Runs could not be loaded', Object.assign(new Error('Table not found'), { status: 503 }));
      const [state] = container.children;
      expect(state.dataset.state).toBe('error');
      expect(state.text).toBe('Runs could not be loaded Table not found');
    });

    it('builds the same element to place yourself, compact', () => {
      const element = PageState.failedElement('More could not be loaded', forbidden(), { compact: true });
      expect(element.className).toContain('page-state-compact');
      expect(element.dataset.state).toBe('redacted');
    });
  });

  describe('WhoAmI', () => {
    const me = {
      signedIn: true, publicMode: false,
      user: { buPrincipal: 'jdoe', eppn: 'jdoe@bu.edu', displayName: 'Jane <Doe>' },
      roles: [{ name: 'Viewer', description: 'Sees the runs', policies: [{ name: 'RunReporting', description: 'Runs', actions: ['runs:read'] }] }],
      actions: ['runs:read'], unknownRoles: ['Ghost']
    };

    it("shows the user's identity, roles, policies and actions, escaped", () => {
      const html = WhoAmI.dialogHtml(me);
      expect(html).toContain('Jane &lt;Doe&gt;');
      expect(html).toContain('jdoe@bu.edu');
      expect(html).toContain('Viewer');
      expect(html).toContain('RunReporting');
      expect(html).toContain('runs:read');
      expect(html).toContain('unknown to this dashboard (ignored): Ghost');
    });

    it('says when the user has no role', () => {
      expect(WhoAmI.dialogHtml({ ...me, roles: [], unknownRoles: [] })).toContain('None. You can see every page, but not its data.');
    });

    it('shows the no-role notice only to a signed-in user with no role, never in public mode', () => {
      expect(WhoAmI.hasNoRole({ ...me, roles: [] })).toBe(true);
      expect(WhoAmI.hasNoRole(me)).toBe(false);
      expect(WhoAmI.hasNoRole({ ...me, roles: [], publicMode: true })).toBe(false);
      expect(WhoAmI.hasNoRole({ signedIn: false, publicMode: false, roles: [] })).toBe(false);
      expect(WhoAmI.hasNoRole(null)).toBe(false);
    });

    it('reads /api/me once, and again after a failure', async () => {
      const getJson = jest.fn().mockRejectedValueOnce(new Error('offline')).mockResolvedValue(me);
      const whoAmI = new WhoAmI({ getJson, root: {} });
      jest.spyOn(console, 'warn').mockImplementation(() => {});
      expect(await whoAmI.me()).toBeNull();
      expect(await whoAmI.me()).toBe(me);
      expect(await whoAmI.me()).toBe(me);
      expect(getJson).toHaveBeenCalledTimes(2);
      jest.restoreAllMocks();
    });
  });

  describe('AccessPage', () => {
    /** An Access page over stand-in parts, with the person jdoe shown holding these roles and these checked */
    const page = (options: { held: string[]; checked: string[]; me?: string; confirm?: boolean; send?: jest.Mock }) => {
      const parts: Record<string, any> = {
        '[data-access-list]': new FakeElement(),
        '[data-access-person]': new FakeElement(),
        '[data-access-lookup]': new FakeElement('form'),
        '#accessPrincipal': new FakeElement('input'),
        '[data-refresh-stamp="access"]': new FakeElement('span'),
        '[data-action="refresh-access"]': new FakeElement('button')
      };
      const result = new FakeElement('span');
      parts['[data-access-person]'].querySelector = () => result;
      const confirm = jest.fn().mockReturnValue(options.confirm ?? false);
      const sendJson = options.send ?? jest.fn().mockResolvedValue({ buPrincipal: 'jdoe', added: [], removed: ['Admin'], roles: options.checked, grants: [] });
      const getJson = jest.fn().mockResolvedValue({ roles: [], assignments: [] });
      const access = new AccessPage({
        getJson, sendJson, confirm,
        me: async () => ({ user: { buPrincipal: options.me ?? 'jdoe' } }),
        root: { querySelector: (selector: string) => parts[selector] || null }
      });
      access.shown = { buPrincipal: 'jdoe', roles: options.held };
      access.checkedRoles = () => options.checked;
      return { access, confirm, sendJson, result };
    };

    it('asks before an Admin removes their own Admin role, and saves nothing if they decline', async () => {
      const { access, confirm, sendJson } = page({ held: ['Admin'], checked: ['Viewer'] });
      await access.save();
      expect(confirm).toHaveBeenCalledWith(AccessPage.OWN_ADMIN_CONFIRMATION);
      expect(sendJson).not.toHaveBeenCalled();
    });

    it('saves once they confirm, saying what changed', async () => {
      const { access, sendJson, result } = page({ held: ['Admin', 'Viewer'], checked: ['Viewer'], confirm: true });
      await access.save();
      expect(sendJson).toHaveBeenCalledWith('PUT', '/api/access/assignments/jdoe', { roles: ['Viewer'] });
      expect(result.text).toBe('Saved: removed Admin');
    });

    it("doesn't ask when the change is to someone else's roles", async () => {
      const { access, confirm, sendJson } = page({ held: ['Admin'], checked: [], me: 'boss' });
      await access.save();
      expect(confirm).not.toHaveBeenCalled();
      expect(sendJson).toHaveBeenCalled();
    });

    it("shows a refused save with the server's message", async () => {
      const send = jest.fn().mockRejectedValue(Object.assign(new Error('jdoe is the landscape\'s last Admin: give someone else Admin first.'), { status: 409 }));
      const { access, result } = page({ held: ['Admin'], checked: [], me: 'boss', send });
      await access.save();
      expect(result.text).toContain("jdoe is the landscape's last Admin");
    });

    it('lists a person with their name, roles and latest grant, escaped', () => {
      const html = AccessPage.rowHtml({
        buPrincipal: 'jdoe', profile: { displayName: '<b>Jane</b>' },
        grants: [{ role: 'Viewer', grantedBy: 'boss', grantedAt: '2026-11-17T10:00:00.000Z' }, { role: 'Admin', grantedBy: 'seed', grantedAt: '2026-11-17T12:00:00.000Z' }]
      });
      expect(html).toContain('data-principal="jdoe"');
      expect(html).toContain('&lt;b&gt;Jane&lt;/b&gt;');
      expect(html).toContain('by seed');
      expect(html).toContain('Viewer');
      expect(html).toContain('Admin');
    });

    it('shows a list it may not read as redacted', async () => {
      const list = new FakeElement();
      const access = new AccessPage({
        getJson: jest.fn().mockRejectedValue(forbidden('access:admin')), sendJson: jest.fn(), me: async () => null,
        root: { querySelector: (selector: string) => (selector === '[data-access-list]' ? list : new FakeElement()) }
      });
      expect(await access.load()).toBe(false);
      expect(list.children[0].dataset.state).toBe('redacted');
      expect(list.children[0].text).toContain('access:admin');
    });
  });

  describe('AuditTrailPage', () => {
    it('asks for the filtered page after the cursor', () => {
      expect(AuditTrailPage.path({}, undefined)).toBe('/api/audit');
      expect(AuditTrailPage.path({ principal: 'jdoe', action: 'bulk:start' }, 'abc')).toBe('/api/audit?principal=jdoe&action=bulk%3Astart&cursor=abc');
    });

    it('shows a record: who, as whom, what, and how it went, escaped', () => {
      const html = AuditTrailPage.rowHtml({
        at: '2026-11-17T12:00:00.000Z', buPrincipal: 'boss', viewedAs: 'jdoe', action: 'bulk:start', target: 'U1',
        decision: 'DENIED', detail: '<script>'
      });
      expect(html).toContain('boss');
      expect(html).toContain('as jdoe');
      expect(html).toContain('Denied');
      expect(html).toContain('&lt;script&gt;');
      expect(AuditTrailPage.rowHtml({ at: 'x', buPrincipal: 'a', action: 'person:sync', decision: 'ALLOWED', outcome: 'FAILED' }))
        .toContain('Failed');
    });
  });
});
