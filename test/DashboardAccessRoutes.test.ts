/**
 * The Access page's routes (dashboard/src/handlers/AccessRoutes.ts, authorization-plan.md decisions 3 and 11) through
 * the handler offline: the catalog's roles, everyone's assignments, one person's, replacing and removing roles, the
 * last-Admin safeguard, refusals, and what the audit trail records of them.
 */
import { COOKIE, event, json, loadHandler, signedInEnvironment } from './support/OfflineDashboard';
import { TestSignIn } from './support/TestSignIn';

const signIn = new TestSignIn();
const jdoe = signIn.cookie({ buPrincipal: ['jdoe'], givenName: ['Jane'], sn: ['Doe'], eduPersonPrincipalName: ['jdoe@bu.edu'] }, COOKIE);

jest.setTimeout(60_000);

let loaded: ReturnType<typeof loadHandler>;
beforeAll(() => { loaded = loadHandler(signedInEnvironment(signIn)); });
afterAll(() => loaded.restore());
beforeEach(() => {
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'info').mockImplementation(() => {});
  // jdoe is the landscape's one Admin (the offline store is seeded with USER_ROLES)
  process.env.USER_ROLES = 'Admin';
  loaded.ServiceProvider.resetOfflineState();
});
afterEach(() => jest.restoreAllMocks());

const call = (method: string, path: string, body?: object) => loaded.handler(event(method, path, { body, cookie: jdoe }));
const store = () => loaded.ServiceProvider.getRoleAssignmentStore();

describe('GET /api/access/roles', () => {
  it("lists the catalog's roles with their policies and actions", async () => {
    const { roles } = json(await call('GET', '/api/access/roles'));
    expect(roles.map((r: any) => r.name)).toEqual(
      ['RunViewer', 'PersonViewer', 'Viewer', 'PersonOperator', 'PipelineOperator', 'Operator', 'Admin']);
    expect(roles[2]).toEqual({
      name: 'Viewer', description: expect.any(String),
      policies: [
        { name: 'RunReporting', description: expect.any(String), actions: ['runs:read'] },
        { name: 'PersonDataRead', description: expect.any(String), actions: ['person:lookup', 'person:preview'] }
      ]
    });
  });
});

describe('the assignments', () => {
  it('lists everyone with a role, with the profiles of those who signed in', async () => {
    await call('GET', '/api/me');
    const { assignments } = json(await call('GET', '/api/access/assignments'));
    expect(assignments).toEqual([expect.objectContaining({
      buPrincipal: 'jdoe', grants: [expect.objectContaining({ role: 'Admin', grantedBy: 'seed' })],
      profile: expect.objectContaining({ displayName: 'Jane Doe', eppn: 'jdoe@bu.edu' })
    })]);
  });

  it('looks a person up by buPrincipal or eduPersonPrincipalName', async () => {
    const byEppn = json(await call('GET', '/api/access/assignments/JDoe%40bu.edu'));
    expect(byEppn).toMatchObject({ buPrincipal: 'jdoe', from: 'eppn', roles: ['Admin'] });
    const someone = json(await call('GET', '/api/access/assignments/amy'));
    expect(someone).toEqual({ buPrincipal: 'amy', from: 'buPrincipal', grants: [], roles: [] });
  });

  it("replaces a person's roles, granted by the Admin who saved them", async () => {
    const result = await call('PUT', '/api/access/assignments/amy', { roles: ['Viewer', 'Operator'] });
    expect(result.statusCode).toBe(200);
    expect(json(result)).toMatchObject({ buPrincipal: 'amy', added: ['Operator'], removed: [], roles: ['Operator'] });
    expect((await store().getGrants('amy'))[0].grantedBy).toBe('jdoe');
    const swapped = json(await call('PUT', '/api/access/assignments/amy', { roles: ['Viewer'] }));
    expect(swapped).toMatchObject({ added: ['Viewer'], removed: ['Operator'], pruned: [] });
  });

  it('saves only the roles not covered by another, saying what it dropped', async () => {
    const result = json(await call('PUT', '/api/access/assignments/amy', { roles: ['Viewer', 'Admin', 'Operator'] }));
    expect(result).toMatchObject({
      roles: ['Admin'], added: ['Admin'],
      pruned: [{ role: 'Viewer', coveredBy: 'Admin' }, { role: 'Operator', coveredBy: 'Admin' }]
    });
    expect(await store().getRoles('amy')).toEqual(['Admin']);
  });

  it("drops a role that's already held once a covering one is saved", async () => {
    await store().setRoles('amy', ['Viewer', 'Operator'], 'seed');
    expect(json(await call('PUT', '/api/access/assignments/amy', { roles: ['Viewer', 'Operator'] })))
      .toMatchObject({ roles: ['Operator'], removed: ['Viewer'], pruned: [{ role: 'Viewer', coveredBy: 'Operator' }] });
  });

  it('removes them all', async () => {
    await call('PUT', '/api/access/assignments/amy', { roles: ['Viewer'] });
    expect(json(await call('DELETE', '/api/access/assignments/amy'))).toMatchObject({ removed: ['Viewer'], roles: [] });
    expect(await store().getRoles('amy')).toEqual([]);
  });

  it('refuses an unknown role, a bad body and a bad principal', async () => {
    const unknown = await call('PUT', '/api/access/assignments/amy', { roles: ['Viewer', 'Superuser'] });
    expect(unknown.statusCode).toBe(400);
    expect(json(unknown)).toMatchObject({ code: 'INVALID_ROLE' });
    expect(json(await call('PUT', '/api/access/assignments/amy', { roles: 'Viewer' })).code).toBe('INVALID_REQUEST');
    expect(json(await call('PUT', '/api/access/assignments/amy@gmail.com', { roles: [] })).code).toBe('INVALID_PRINCIPAL');
    expect(await store().getRoles('amy')).toEqual([]);
  });
});

describe('the last Admin', () => {
  it("can't remove their own Admin role, or be removed, while no one else holds it", async () => {
    for (const result of [
      await call('PUT', '/api/access/assignments/jdoe', { roles: ['Viewer'] }),
      await call('DELETE', '/api/access/assignments/jdoe')
    ]) {
      expect(result.statusCode).toBe(409);
      expect(json(result)).toMatchObject({ code: 'LAST_ADMIN' });
    }
    expect(await store().getRoles('jdoe')).toEqual(['Admin']);
  });

  it('can, once someone else is an Admin', async () => {
    await call('PUT', '/api/access/assignments/amy', { roles: ['Admin'] });
    expect((await call('PUT', '/api/access/assignments/jdoe', { roles: ['Viewer'] })).statusCode).toBe(200);
    expect(await store().holdersOf('Admin')).toEqual(['amy']);
  });

  it('may keep Admin while choosing roles it covers, which are dropped', async () => {
    const result = await call('PUT', '/api/access/assignments/jdoe', { roles: ['Admin', 'Viewer'] });
    expect(result.statusCode).toBe(200);
    expect(json(result)).toMatchObject({ roles: ['Admin'], pruned: [{ role: 'Viewer', coveredBy: 'Admin' }] });
  });

  it('judges the roles that would be saved: Viewer and Operator without Admin still leave no Admin', async () => {
    expect((await call('PUT', '/api/access/assignments/jdoe', { roles: ['Viewer', 'Operator'] })).statusCode).toBe(409);
  });
});

describe('who may', () => {
  it('refuses an Operator, reading or changing', async () => {
    process.env.USER_ROLES = 'Operator';
    for (const [method, path] of [['GET', '/api/access/assignments'], ['GET', '/api/access/roles'], ['PUT', '/api/access/assignments/amy']]) {
      const result = await call(method, path, method === 'PUT' ? { roles: ['Admin'] } : undefined);
      expect(result.statusCode).toBe(403);
      expect(json(result).action).toBe('access:admin');
    }
  });
});

describe('the audit trail', () => {
  it('records each change with the person and the roles saved, and what was dropped', async () => {
    await call('PUT', '/api/access/assignments/amy', { roles: ['Operator'] });
    await call('PUT', '/api/access/assignments/olly', { roles: ['Viewer', 'Operator'] });
    await call('PUT', '/api/access/assignments/jdoe', { roles: [] });
    const { items } = await loaded.ServiceProvider.getAuditStore().list();
    expect(items[0]).toMatchObject({ action: 'access:admin', decision: 'ALLOWED', outcome: 'FAILED', target: 'jdoe', detail: 'roles=none (HTTP 409)' });
    expect(items[1]).toMatchObject({ target: 'olly', outcome: 'SUCCEEDED', detail: 'roles=Operator; dropped Viewer (covered by Operator)' });
    expect(items[2]).toMatchObject({ action: 'access:admin', outcome: 'SUCCEEDED', target: 'amy', detail: 'roles=Operator', buPrincipal: 'jdoe' });
  });

  it("doesn't record reading roles: the Access page's lists and lookups", async () => {
    await call('GET', '/api/access/roles');
    await call('GET', '/api/access/assignments');
    await call('GET', '/api/access/assignments/amy');
    expect((await loaded.ServiceProvider.getAuditStore().list()).items).toEqual([]);
  });
});
