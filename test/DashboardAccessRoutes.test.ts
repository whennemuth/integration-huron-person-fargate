/**
 * The Access page's routes (dashboard/src/handlers/AccessRoutes.ts, authorization-plan.md decisions 3 and 11) through
 * the handler offline: the catalog's roles, everyone's assignments, one person's, replacing and removing roles, giving
 * roles to several people at once, the last-Admin safeguard, refusals, and what the audit trail records of them.
 */
import { MAX_BULK_PRINCIPALS } from '../dashboard/src/handlers/AccessRoutes';
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

describe('giving roles to several people', () => {
  const grant = (principals: unknown, roles: unknown) => call('POST', '/api/access/assignments', { principals, roles });

  it('gives the roles to everyone who holds none, each once, by buPrincipal or eduPersonPrincipalName', async () => {
    const result = await grant(['amy', ' Bob@bu.edu ', 'amy', 'carl', ''], ['Viewer']);
    expect(result.statusCode).toBe(200);
    expect(json(result)).toEqual({ roles: ['Viewer'], pruned: [], granted: ['amy', 'bob', 'carl'], unchanged: [] });
    for (const who of ['amy', 'bob', 'carl']) {
      expect(await store().getGrants(who)).toEqual([expect.objectContaining({ role: 'Viewer', grantedBy: 'jdoe' })]);
    }
  });

  it('leaves anyone who already holds a role exactly as they are, and says so', async () => {
    await store().setRoles('amy', ['Operator'], 'seed');
    await store().setRoles('bob', ['RunViewer'], 'seed');
    const result = json(await grant(['amy', 'bob', 'jdoe', 'carl'], ['Viewer']));
    expect(result).toMatchObject({
      granted: ['carl'],
      unchanged: [{ buPrincipal: 'amy', roles: ['Operator'] }, { buPrincipal: 'bob', roles: ['RunViewer'] }, { buPrincipal: 'jdoe', roles: ['Admin'] }]
    });
    expect(await store().getGrants('amy')).toEqual([expect.objectContaining({ role: 'Operator', grantedBy: 'seed' })]);
    expect(await store().getRoles('bob')).toEqual(['RunViewer']);
    expect(await store().getRoles('jdoe')).toEqual(['Admin']);
  });

  it('gives only the roles not covered by another, saying what it dropped', async () => {
    expect(json(await grant(['amy'], ['Viewer', 'Operator'])))
      .toMatchObject({ roles: ['Operator'], pruned: [{ role: 'Viewer', coveredBy: 'Operator' }], granted: ['amy'] });
    expect(await store().getRoles('amy')).toEqual(['Operator']);
  });

  it('saves nothing when any person is not valid, naming them', async () => {
    const result = await grant(['amy', 'bob@gmail.com', 'not a login'], ['Viewer']);
    expect(result.statusCode).toBe(400);
    expect(json(result)).toMatchObject({
      code: 'INVALID_PRINCIPAL', error: expect.stringContaining('bob@gmail.com, not a login'),
      invalid: [expect.objectContaining({ principal: 'bob@gmail.com' }), expect.objectContaining({ principal: 'not a login' })]
    });
    expect(await store().getRoles('amy')).toEqual([]);
  });

  it('refuses no people, no roles, an unknown role, too many people and a bad body', async () => {
    const tooMany = Array.from({ length: MAX_BULK_PRINCIPALS + 1 }, (_, i) => `person${i}`);
    for (const [principals, roles, code] of [
      [[' ', ''], ['Viewer'], 'INVALID_REQUEST'],
      [['amy'], [], 'INVALID_REQUEST'],
      [['amy'], ['Superuser'], 'INVALID_ROLE'],
      [tooMany, ['Viewer'], 'TOO_MANY_PRINCIPALS'],
      ['amy', ['Viewer'], 'INVALID_REQUEST'],
      [[7], ['Viewer'], 'INVALID_REQUEST']
    ] as [unknown, unknown, string][]) {
      const result = await grant(principals, roles);
      expect(result.statusCode).toBe(400);
      expect(json(result).code).toBe(code);
    }
    expect(await store().holdersOf('Viewer')).toEqual([]);
  });

  it('takes as many as the maximum', async () => {
    const most = Array.from({ length: MAX_BULK_PRINCIPALS }, (_, i) => `person${i}`);
    expect(json(await grant(most, ['RunViewer'])).granted).toHaveLength(MAX_BULK_PRINCIPALS);
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
    for (const [method, path] of [['GET', '/api/access/assignments'], ['GET', '/api/access/roles'], ['PUT', '/api/access/assignments/amy'], ['POST', '/api/access/assignments']]) {
      const result = await call(method, path, method === 'GET' ? undefined : { principals: ['amy'], roles: ['Admin'] });
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

  it('records each person a bulk grant gave roles to, and who it left as they were', async () => {
    await store().setRoles('amy', ['Operator'], 'seed');
    await call('POST', '/api/access/assignments', { principals: ['amy', 'bob', 'carl'], roles: ['Viewer', 'RunViewer'] });
    const { items } = await loaded.ServiceProvider.getAuditStore().list();
    expect(items).toHaveLength(3);
    expect(items[0]).toMatchObject({
      action: 'access:admin', decision: 'ALLOWED', outcome: 'SUCCEEDED', buPrincipal: 'jdoe', target: '3 people',
      detail: 'bulk roles=Viewer; dropped RunViewer (covered by Viewer); granted 2; left as they were 1: amy (Operator)'
    });
    expect(items.slice(1).map((item: any) => [item.target, item.detail])).toEqual([['carl', 'roles=Viewer (bulk)'], ['bob', 'roles=Viewer (bulk)']]);
  });

  it("doesn't record reading roles: the Access page's lists and lookups", async () => {
    await call('GET', '/api/access/roles');
    await call('GET', '/api/access/assignments');
    await call('GET', '/api/access/assignments/amy');
    expect((await loaded.ServiceProvider.getAuditStore().list()).items).toEqual([]);
  });
});
