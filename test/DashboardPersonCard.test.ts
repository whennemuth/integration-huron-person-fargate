/**
 * The person card's errors (dashboard/public/js/person-card.js): a failed lookup says why, the server's reason
 * included (a 403 names the action the user's roles lack), rather than only that it failed. The script runs in a vm
 * context (as written and as the Lambda build minifies it), after page-state.js, against a minimal stand-in DOM.
 */
import { readFileSync } from 'fs';
import { join } from 'path';
import { Script, createContext } from 'vm';
import { minifyJS } from '../dashboard/scripts/BuildTemplates';

const JS_DIR = join(__dirname, '../dashboard/public/js');

class FakeElement {
  className = '';
  textContent = '';
  innerHTML = '';
  dataset: Record<string, string> = {};
  children: any[] = [];
  constructor(public tagName = 'div') {}
  setAttribute() { /* not needed */ }
  appendChild(child: any) { this.children.push(child); return child; }
  replaceChildren(...children: any[]) { this.children = children; }
  querySelector(): any { return null; }
  get text(): string {
    const text = (node: any): string => [node.textContent ?? node.data ?? '', ...(node.children ?? []).map(text)].join(' ');
    return text(this).replace(/\s+/g, ' ').trim();
  }
}

const load = (minified: boolean) => {
  const container = new FakeElement();
  const document = {
    createElement: (tag: string) => new FakeElement(tag),
    createTextNode: (data: string) => ({ data }),
    getElementById: () => container
  };
  const context = createContext({ console, document, window: { location: { origin: '' } } });
  const source = ['page-state.js', 'person-card.js'].map(file => readFileSync(join(JS_DIR, file), 'utf-8')).join('\n');
  new Script(`${minified ? minifyJS(source).minified : source}\nthis.PersonCard = PersonCard;`).runInContext(context);
  return { PersonCard: (context as any).PersonCard, container };
};

describe.each([false, true])('PersonCard errors (minified: %s)', minified => {
  const { PersonCard, container } = load(minified);
  const card = new PersonCard('personResults');

  it("shows a refused lookup with the server's reason", () => {
    card.renderError('Error looking up person', 'None of your roles (Viewer) has a policy that grants person:lookup.');
    const [state] = container.children;
    expect(state.dataset.state).toBe('error');
    expect(state.text).toBe('Error looking up person None of your roles (Viewer) has a policy that grants person:lookup.');
  });

  it('still shows a failure the server gave no reason for', () => {
    card.renderError('Error looking up person');
    expect(container.children[0].text).toBe('Error looking up person');
  });

  it("says why the source column has no data, as text", () => {
    const box = new FakeElement();
    card.container = { querySelector: () => ({ querySelector: () => ({ querySelector: () => box }) }) };
    card.showSourceMessage('Error looking up CDM data: None of your roles (Viewer) has a policy that grants person:lookup. <b>', 'danger');
    expect(box.children[0].className).toBe('text-danger');
    expect(box.children[0].children[1].data).toContain('grants person:lookup. <b>');
  });
});
