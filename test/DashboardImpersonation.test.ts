/**
 * Impersonation (dashboard/src/authorization/Impersonation.ts, authorization-plan.md decision 14): the cookie, the
 * re-check of the signed-in user's own roles on every request, and viewing only.
 */
import { RoleAssignments } from '../dashboard/src/authorization/Authorizer';
import { Impersonation } from '../dashboard/src/authorization/Impersonation';
import { Principal } from '../dashboard/src/authorization/Principal';
import { StaticRoleCatalog } from '../dashboard/src/authorization/RoleCatalog';
import { RoutePermissions } from '../dashboard/src/authorization/RoutePermissions';

const catalog = new StaticRoleCatalog();
const roles: { [buPrincipal: string]: string[] } = { admin: ['Admin'], operator: ['Operator'], viewer: ['Viewer'] };
const assignments: RoleAssignments = { getRoles: async buPrincipal => roles[buPrincipal] ?? [] };
const cookie = (value: string) => `hp-dashboard-auth=abc.def.ghi; ${Impersonation.COOKIE_NAME}=${value}; other=1`;

const resolve = (buPrincipal: string | undefined, cookieHeader?: string) => Impersonation.resolve({
  user: buPrincipal ? Principal.of(buPrincipal) : Principal.anonymous(), cookieHeader, assignments, catalog
});

describe('Impersonation', () => {
  it('is the user themself without a cookie', async () => {
    const imp = await resolve('admin', 'hp-dashboard-auth=abc');
    expect(imp.impersonating).toBe(false);
    expect(imp.effective).toBe(imp.user);
    expect(imp.authorizer).toBe(imp.userAuthorizer);
    expect(imp.mayImpersonate).toBe(true);
    expect(imp.clearCookie).toBe(false);
    expect(imp.denial).toBeUndefined();
  });

  it('says only Admins may impersonate', async () => {
    expect((await resolve('operator')).mayImpersonate).toBe(false);
    expect((await resolve('viewer')).mayImpersonate).toBe(false);
    expect((await resolve('nobody')).mayImpersonate).toBe(false);
    expect((await resolve(undefined)).mayImpersonate).toBe(false);
  });

  it("authorizes an Admin's request with the person's roles", async () => {
    const imp = await resolve('admin', cookie('viewer'));
    expect(imp.impersonating).toBe(true);
    expect(imp.user.buPrincipal).toBe('admin');
    expect(imp.effective.buPrincipal).toBe('viewer');
    expect((await imp.authorizer.authorize('runs:read')).allowed).toBe(true);
    expect((await imp.authorizer.authorize('bulk:start')).allowed).toBe(false);
    expect((await imp.authorizer.authorize('access:admin')).allowed).toBe(false);
    // The user's own roles stay available, for the button and for stopping
    expect((await imp.userAuthorizer.authorize('access:impersonate')).allowed).toBe(true);
    expect(imp.clearCookie).toBe(false);
  });

  it('may impersonate someone with no role, to see what they see', async () => {
    const imp = await resolve('admin', cookie('newcomer'));
    expect(imp.effective.buPrincipal).toBe('newcomer');
    expect((await imp.authorizer.authorize('runs:read')).allowed).toBe(false);
  });

  it("ignores, clears and reports a cookie from someone whose roles don't grant impersonation", async () => {
    for (const user of ['operator', 'viewer', 'nobody']) {
      const imp = await resolve(user, cookie('admin'));
      expect(imp.impersonating).toBe(false);
      expect(imp.effective.buPrincipal).toBe(user);
      expect(imp.clearCookie).toBe(true);
      expect(imp.denial).toMatch(/may not impersonate/);
      // Their own roles decide: an Operator can't reach Admin actions through the cookie
      expect((await imp.authorizer.authorize('access:admin')).allowed).toBe(false);
    }
  });

  it('stops working the moment the Admin role is taken away', async () => {
    roles.formeradmin = ['Admin'];
    expect((await resolve('formeradmin', cookie('viewer'))).impersonating).toBe(true);
    roles.formeradmin = ['Operator'];
    const imp = await resolve('formeradmin', cookie('viewer'));
    expect(imp.impersonating).toBe(false);
    expect(imp.denial).toBeDefined();
    expect(imp.clearCookie).toBe(true);
  });

  it('clears a forged or malformed cookie as a denial', async () => {
    for (const value of ['', 'a%20b', 'jdoe%40bu.edu', '%3Cscript%3E']) {
      const imp = await resolve('admin', cookie(value));
      expect(imp.impersonating).toBe(false);
      expect(imp.clearCookie).toBe(true);
      expect(imp.denial).toMatch(/no valid buPrincipal/);
    }
  });

  it('clears a cookie that came with no one signed in', async () => {
    const imp = await resolve(undefined, cookie('viewer'));
    expect(imp.impersonating).toBe(false);
    expect(imp.clearCookie).toBe(true);
    expect(imp.denial).toBeDefined();
  });

  it('clears a cookie naming the user themself, without a denial', async () => {
    const imp = await resolve('admin', cookie('ADMIN'));
    expect(imp.impersonating).toBe(false);
    expect(imp.clearCookie).toBe(true);
    expect(imp.denial).toBeUndefined();
  });

  it('refuses every change while impersonating, even one the person may make', async () => {
    const imp = await resolve('admin', cookie('operator'));
    const refusal = (method: string, path: string) => imp.refusalFor(RoutePermissions.permissionFor(method, path));
    expect((await imp.authorizer.authorize('person:sync')).allowed).toBe(true);
    expect(refusal('POST', '/api/person-sync')).toBe(Impersonation.STOP_TO_CHANGE);
    expect(refusal('POST', '/api/bulk-sync')).toBe(Impersonation.STOP_TO_CHANGE);
    expect(refusal('POST', '/api/mock-landscape/reset')).toBe(Impersonation.STOP_TO_CHANGE);
    expect(refusal('PUT', '/api/access/assignments/jdoe')).toBe(Impersonation.STOP_TO_CHANGE);
    expect(refusal('POST', '/api/bulk-sync/plan')).toBeUndefined();
    expect(refusal('POST', '/api/person-lookup')).toBeUndefined();
    expect(refusal('GET', '/api/runs')).toBeUndefined();
    expect(refusal('DELETE', '/api/impersonation')).toBeUndefined();
  });

  it('refuses nothing when not impersonating', async () => {
    const imp = await resolve('admin');
    expect(imp.refusalFor(RoutePermissions.permissionFor('POST', '/api/bulk-sync'))).toBeUndefined();
  });

  it('refuses to start impersonating yourself, or with no one signed in', () => {
    expect(Impersonation.startRefusal(Principal.of('admin'), 'admin')).toMatch(/yourself/);
    expect(Impersonation.startRefusal(Principal.anonymous(), 'viewer')).toMatch(/Sign in/);
    expect(Impersonation.startRefusal(Principal.of('admin'), 'viewer')).toBeUndefined();
  });

  it('reads the cookie, the first of several', () => {
    expect(Impersonation.readCookie(undefined)).toBeUndefined();
    expect(Impersonation.readCookie('a=1; b=2')).toBeUndefined();
    expect(Impersonation.readCookie(`${Impersonation.COOKIE_NAME}=jdoe`)).toBe('jdoe');
    expect(Impersonation.readCookie(`x=1;${Impersonation.COOKIE_NAME}=a%2Eb; ${Impersonation.COOKIE_NAME}=c`)).toBe('a.b');
    expect(Impersonation.readCookie(`x${Impersonation.COOKIE_NAME}=jdoe`)).toBeUndefined();
  });

  it('sets a host-only, HttpOnly, Secure, SameSite=Strict session cookie, and clears it', () => {
    const set = Impersonation.setCookie('jdoe');
    expect(set).toBe(`${Impersonation.COOKIE_NAME}=jdoe; Path=/; HttpOnly; Secure; SameSite=Strict`);
    expect(set).not.toMatch(/Domain|Max-Age|Expires/);
    const clear = Impersonation.clearCookieValue();
    expect(clear).toMatch(new RegExp(`^${Impersonation.COOKIE_NAME}=;`));
    expect(clear).toMatch(/Max-Age=0/);
    expect(clear).toMatch(/HttpOnly; Secure; SameSite=Strict/);
  });
});
