/**
 * Impersonation through the handler (authorization-plan.md, decision 14), offline: starting and stopping it, the gate
 * with a valid, a revoked and a forged cookie, viewing only against each changing route, /api/me, the audit trail's
 * viewedAs, and the top bar's controls by role.
 */
import { Impersonation } from '../dashboard/src/authorization/Impersonation';
import { COOKIE, event, json, loadHandler, signedInEnvironment } from './support/OfflineDashboard';
import { TestSignIn } from './support/TestSignIn';

const signIn = new TestSignIn();
const jdoe = signIn.cookie({ buPrincipal: ['jdoe'] }, COOKIE);
const as = (person: string) => `${jdoe}; ${Impersonation.COOKIE_NAME}=${encodeURIComponent(person)}`;

jest.setTimeout(60_000);

let loaded: ReturnType<typeof loadHandler>;
beforeAll(() => { loaded = loadHandler(signedInEnvironment(signIn)); });
afterAll(() => loaded.restore());
beforeEach(async () => {
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'info').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  await given('Admin');
});
afterEach(() => jest.restoreAllMocks());

/** jdoe (the local user) holds these roles; amy is a Viewer and olly an Operator */
const given = async (jdoeRoles: string) => {
  process.env.USER_ROLES = jdoeRoles;
  loaded.ServiceProvider.resetOfflineState();
  const store = loaded.ServiceProvider.getRoleAssignmentStore();
  await store.setRoles('amy', ['Viewer'], 'seed');
  await store.setRoles('olly', ['Operator'], 'seed');
};

const call = (method: string, path: string, cookie: string, body?: object) => loaded.handler(event(method, path, { body, cookie }));
const records = async () => (await loaded.ServiceProvider.getAuditStore().list()).items;
const setCookie = (result: any): string | undefined => result.headers?.['Set-Cookie'];

describe('starting and stopping', () => {
  it("lets an Admin start, setting the cookie and answering with the person's roles", async () => {
    const result = await call('POST', '/api/impersonation', jdoe, { principal: 'amy@bu.edu' });
    expect(result.statusCode).toBe(200);
    expect(json(result)).toEqual({ impersonating: { buPrincipal: 'amy', label: 'amy', roles: ['Viewer'] } });
    expect(setCookie(result)).toBe(Impersonation.setCookie('amy'));
    expect((await records())[0]).toMatchObject({ action: 'access:impersonate', decision: 'ALLOWED', target: 'amy', detail: 'started', buPrincipal: 'jdoe' });
  });

  it('refuses an Operator, yourself, and someone who is no one', async () => {
    await given('Operator');
    const operator = await call('POST', '/api/impersonation', jdoe, { principal: 'amy' });
    expect(operator.statusCode).toBe(403);
    expect(json(operator).action).toBe('access:impersonate');
    await given('Admin');
    expect(json(await call('POST', '/api/impersonation', jdoe, { principal: 'JDOE' }))).toMatchObject({ code: 'INVALID_PRINCIPAL', error: "You can't impersonate yourself." });
    expect((await call('POST', '/api/impersonation', jdoe, { principal: 'a b' })).statusCode).toBe(400);
  });

  it('stops, clearing the cookie and recording it', async () => {
    const result = await call('DELETE', '/api/impersonation', as('amy'));
    expect(result.statusCode).toBe(200);
    expect(setCookie(result)).toBe(Impersonation.clearCookieValue());
    expect((await records())[0]).toMatchObject({ action: 'access:impersonate', target: 'amy', detail: 'stopped', viewedAs: 'amy' });
  });
});

describe('while impersonating', () => {
  it("decides with the person's roles: a Viewer's reads, and a Viewer's refusals", async () => {
    expect((await call('GET', '/api/runs', as('amy'))).statusCode).toBe(200);
    const review = await call('POST', '/api/bulk-sync/plan', as('amy'), { runType: 'full' });
    expect(review.statusCode).toBe(403);
    expect(json(review).message).toBe('None of your roles (Viewer) has a policy that grants bulk:start.');
    expect((await call('GET', '/api/access/assignments', as('amy'))).statusCode).toBe(403);
    expect((await call('GET', '/api/audit', as('amy'))).statusCode).toBe(403);
  });

  it("allows an Operator's review, which changes nothing", async () => {
    expect((await call('POST', '/api/bulk-sync/plan', as('olly'), { runType: 'full' })).statusCode).toBe(200);
    expect((await call('POST', '/api/person-lookup', as('olly'), { personId: 'U1' })).statusCode).not.toBe(403);
  });

  it.each([
    ['POST', '/api/person-sync', { personId: 'U1' }],
    ['POST', '/api/bulk-sync', { runType: 'full' }],
    ['POST', '/api/mock-landscape/reset', undefined]
  ])("refuses %s %s even though the Operator's roles allow it", async (method, path, body) => {
    const result = await call(method, path, as('olly'), body as object | undefined);
    expect(result.statusCode).toBe(403);
    expect(json(result)).toMatchObject({ code: 'IMPERSONATING', message: Impersonation.STOP_TO_CHANGE });
    expect((await records())[0]).toMatchObject({ decision: 'DENIED', buPrincipal: 'jdoe', viewedAs: 'olly', detail: Impersonation.STOP_TO_CHANGE });
  });

  it("refuses a role change with the person's own refusal, which is what impersonation is for", async () => {
    const result = await call('PUT', '/api/access/assignments/amy', as('olly'), { roles: [] });
    expect(json(result)).toMatchObject({ error: 'FORBIDDEN', action: 'access:admin', roles: ['Operator'] });
  });

  it('records both people on every audited request', async () => {
    await call('POST', '/api/person-lookup', as('olly'), { personId: 'U1' });
    expect((await records())[0]).toMatchObject({ action: 'person:lookup', buPrincipal: 'jdoe', viewedAs: 'olly' });
  });

  it('says at /api/me who is signed in, whose roles are in force, and that they may impersonate', async () => {
    const me = json(await call('GET', '/api/me', as('amy')));
    expect(me).toMatchObject({ user: { buPrincipal: 'jdoe' }, viewedAs: { buPrincipal: 'amy', label: 'amy' }, mayImpersonate: true });
    expect(me.roles.map((r: any) => r.name)).toEqual(['Viewer']);
  });

  it('may switch to someone else, decided on their own roles', async () => {
    const result = await call('POST', '/api/impersonation', as('amy'), { principal: 'olly' });
    expect(result.statusCode).toBe(200);
    expect(setCookie(result)).toBe(Impersonation.setCookie('olly'));
  });
});

describe('a cookie that grants nothing', () => {
  it('is ignored and cleared once the Admin role is gone, and audited as a denial', async () => {
    await given('Operator');
    const result = await call('POST', '/api/bulk-sync/plan', as('amy'), { runType: 'full' });
    // Decided with jdoe's own roles: an Operator may review
    expect(result.statusCode).toBe(200);
    expect(setCookie(result)).toBe(Impersonation.clearCookieValue());
    const denial = (await records()).find((r: any) => r.action === 'access:impersonate');
    expect(denial).toMatchObject({ decision: 'DENIED', buPrincipal: 'jdoe', detail: expect.stringMatching(/may not impersonate/) });
    expect(denial.viewedAs).toBeUndefined();
  });

  it('is ignored and cleared when made by hand', async () => {
    const result = await call('GET', '/api/me', `${jdoe}; ${Impersonation.COOKIE_NAME}=%3Cscript%3E`);
    expect(json(result).viewedAs).toBeUndefined();
    expect(setCookie(result)).toBe(Impersonation.clearCookieValue());
    expect((await records())[0]).toMatchObject({ action: 'access:impersonate', decision: 'DENIED' });
  });

  it('grants nothing without a sign-in', async () => {
    const result = await loaded.handler(event('GET', '/api/runs', { cookie: `${Impersonation.COOKIE_NAME}=amy` }));
    expect(result.statusCode).toBe(403);
  });
});

describe('the top bar', () => {
  /** The page's markup, without its inline scripts (which name the same ids and actions) */
  const page = async (cookie: string) => {
    const body = `${(await call('GET', '/dashboard', cookie)).body}`;
    const scripts = body.indexOf('<script');
    return scripts < 0 ? body : body.slice(0, scripts);
  };

  it('shows the Impersonate button and dialog to an Admin', async () => {
    const body = await page(jdoe);
    expect(body).toContain('data-bs-target="#impersonateDialog"');
    expect(body).toContain('id="impersonateDialog"');
    expect(body).not.toContain('data-action="stop-impersonating"');
  });

  it('omits them for everyone else', async () => {
    for (const roles of ['Operator', 'Viewer', '']) {
      await given(roles);
      const body = await page(jdoe);
      expect(body).not.toContain('impersonateDialog');
    }
  });

  it('while impersonating, offers Stop impersonating <person> and the impersonating tone in place of Impersonate', async () => {
    await loaded.ServiceProvider.getRoleAssignmentStore().recordProfile({ isAnonymous: false, buPrincipal: 'amy', displayName: 'Amy Lee' } as any);
    const body = await page(as('amy'));
    expect(body).toContain('Stop impersonating Amy Lee');
    expect(body).toContain('landscape-tone-impersonating');
    expect(body).toContain('Impersonating Amy Lee: viewing only');
    expect(body).not.toContain('impersonateDialog');
  });
});
