/**
 * Tests the dashboard's state patterns (dashboard/public/js/page-state.js): the loading, empty, error and
 * redacted states every page and component renders through PageState. The script is browser code with no
 * module system, so it is run in a vm context against a minimal stand-in for the DOM.
 *
 * Also checks that every dashboard script and stylesheet survives the minifier the Lambda build applies
 * (dashboard/scripts/BuildTemplates.ts), which the local server never exercises: it serves the files as written.
 */
import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';
import { Script, createContext } from 'vm';
import { minifyJS } from '../dashboard/scripts/BuildTemplates';

const JS_DIR = join(__dirname, '../dashboard/public/js');

/** Just enough of an element for PageState: classes, attributes, dataset, text and children */
class FakeElement {
  className = '';
  textContent = '';
  dataset: Record<string, string> = {};
  attributes: Record<string, string> = {};
  children: FakeElement[] = [];
  constructor(public tagName: string, public id?: string) {}
  setAttribute(name: string, value: string) { this.attributes[name] = value; }
  appendChild(child: FakeElement) { this.children.push(child); return child; }
  replaceChildren(...children: FakeElement[]) { this.children = children; }
  find(className: string): FakeElement | undefined {
    for (const child of this.children) {
      if (child.className.split(' ').includes(className)) return child;
      const found = child.find(className);
      if (found) return found;
    }
    return undefined;
  }
}

/** Load page-state.js as the browser would, returning its PageState class and the fake document */
const loadPageState = (minified = false) => {
  const elements: Record<string, FakeElement> = {};
  const document = {
    createElement: (tag: string) => new FakeElement(tag),
    getElementById: (id: string) => elements[id] || null
  };
  const context = createContext({ document });
  const source = readFileSync(join(JS_DIR, 'page-state.js'), 'utf-8');
  new Script(`${minified ? minifyJS(source).minified : source}\nthis.PageState = PageState;`).runInContext(context);
  const addContainer = (id: string) => (elements[id] = new FakeElement('div', id));
  return { PageState: (context as any).PageState, addContainer };
};

describe('PageState', () => {
  it.each([false, true])('builds each state with its icon, role and wording (minified: %s)', minified => {
    const { PageState } = loadPageState(minified);

    const loading = PageState.element('loading', { title: 'Loading activity history' });
    expect(loading.className).toBe('page-state page-state-loading');
    expect(loading.dataset.state).toBe('loading');
    expect(loading.attributes.role).toBe('status');
    expect(loading.find('page-state-icon').className).toContain('spinner-border');
    expect(loading.find('page-state-title').textContent).toBe('Loading activity history');
    expect(loading.find('page-state-message')).toBeUndefined();

    const error = PageState.element('error', { title: 'Could not load', message: 'The server answered 500' });
    expect(error.attributes.role).toBe('alert');
    expect(error.find('page-state-icon').className).toContain('fa-circle-exclamation');
    expect(error.find('page-state-message').textContent).toBe('The server answered 500');

    const empty = PageState.element('empty');
    expect(empty.attributes.role).toBeUndefined();
    expect(empty.find('page-state-title').textContent).toBe('Nothing to show');
  });

  it('explains a redaction by default: the viewer needs a role, or a higher one', () => {
    const { PageState } = loadPageState();
    const redacted = PageState.element('redacted');
    expect(redacted.className).toBe('page-state page-state-redacted');
    expect(redacted.find('page-state-icon').className).toContain('fa-lock');
    expect(redacted.find('page-state-title').textContent).toBe('Content redacted');
    expect(redacted.find('page-state-message').textContent).toMatch(/administrator.*role/);
  });

  it('renders a compact, single-line variant', () => {
    const { PageState } = loadPageState();
    expect(PageState.element('loading', { compact: true }).className).toBe('page-state page-state-loading page-state-compact');
  });

  it('sets text as text, so a message carrying markup is shown, not interpreted', () => {
    const { PageState } = loadPageState();
    const state = PageState.element('error', { message: '<img src=x onerror=alert(1)>' });
    expect(state.find('page-state-message').textContent).toBe('<img src=x onerror=alert(1)>');
    expect(state.find('page-state-message').children).toEqual([]);
  });

  it('replaces a container\'s content, found by element or by id', () => {
    const { PageState, addContainer } = loadPageState();
    const container = addContainer('historyContent');
    container.appendChild(new FakeElement('table'));

    const shown = PageState.loading('historyContent', 'Loading');
    expect(container.children).toEqual([shown]);

    PageState.error(container, 'Failed', 'detail');
    expect(container.children).toHaveLength(1);
    expect(container.children[0].dataset.state).toBe('error');

    PageState.empty(container, 'No activity yet', 'Runs will be listed here');
    expect(container.children[0].find('page-state-message')?.textContent).toBe('Runs will be listed here');

    PageState.redacted(container);
    expect(container.children[0].dataset.state).toBe('redacted');
  });

  it('ignores a container that does not exist, and refuses an unknown state', () => {
    const { PageState } = loadPageState();
    expect(PageState.loading('missing', 'Loading')).toBeNull();
    expect(() => PageState.element('broken')).toThrow("PageState: unknown state 'broken'");
  });
});

describe('Dashboard scripts after minification', () => {
  it.each(readdirSync(JS_DIR).filter(file => file.endsWith('.js')))('%s still parses', file => {
    const { minified } = minifyJS(readFileSync(join(JS_DIR, file), 'utf-8'));
    expect(() => new Script(minified)).not.toThrow();
  });
});
