/**
 * The gate (dashboard/src/authorization/RequestGate.ts, authorization-plan.md decision 7) through the dashboard handler,
 * offline, with a signed sign-in token and the local user's roles (USER_ROLES): deny by default, each role against
 * each route, a user with no role, an anonymous request, public mode, /api/me, and Bulk sync's choices.
 */
import { Action } from '../dashboard/src/authorization/Actions';
import { StaticRoleCatalog } from '../dashboard/src/authorization/RoleCatalog';
import { COOKIE, event, json, loadHandler, signedInEnvironment } from './support/OfflineDashboard';
import { TestSignIn } from './support/TestSignIn';

const signIn = new TestSignIn();
const catalog = new StaticRoleCatalog();
const SIGNED_IN_ENV = signedInEnvironment(signIn);

const jdoe = signIn.cookie({ buPrincipal: ['jdoe'], givenName: ['Jane'], sn: ['Doe'] }, COOKIE);


// The handler's modules load (and compile) on the first request
jest.setTimeout(60_000);

beforeEach(() => {
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'info').mockImplementation(() => {});
});
afterEach(() => jest.restoreAllMocks());

describe('the gate, signed in', () => {
  let loaded: ReturnType<typeof loadHandler>;
  beforeAll(() => { loaded = loadHandler(SIGNED_IN_ENV); });
  afterAll(() => loaded.restore());

  /** A request as jdoe, holding these roles (comma-delimited; '' for none) */
  const as = async (roles: string, method: string, path: string, body?: object) => {
    process.env.USER_ROLES = roles;
    loaded.ServiceProvider.resetOfflineState();
    return loaded.handler(event(method, path, { body, cookie: jdoe }));
  };

  // Every row has four values: with fewer, it.each takes the callback's last parameter for done()
  const ROUTES: [string, string, Action, object | undefined][] = [
    ['GET', '/api/runs', 'runs:read', undefined],
    ['GET', '/api/runs/2026-11-04T05:00:00.000Z/summary', 'runs:read', undefined],
    ['GET', '/api/capabilities', 'runs:read', undefined],
    ['GET', '/api/landscapes', 'runs:read', undefined],
    ['GET', '/api/status', 'runs:read', undefined],
    ['GET', '/api/bulk-sync', 'runs:read', undefined],
    ['GET', '/api/mock-landscape/reset', 'runs:read', undefined],
    ['POST', '/api/person-lookup', 'person:lookup', { searchType: 'buid', searchValue: 'U12345678' }],
    ['POST', '/api/person-sync/preview', 'person:preview', { personId: 'U12345678' }],
    ['POST', '/api/person-sync/preview-experiment', 'person:preview', { personId: 'U12345678' }],
    ['POST', '/api/person-sync', 'person:sync', { personId: 'U12345678' }],
    ['POST', '/api/bulk-sync/plan', 'bulk:start', { runType: 'full' }],
    ['POST', '/api/bulk-sync', 'bulk:start', { runType: 'full' }],
    ['POST', '/api/mock-landscape/reset', 'mock:reset', undefined],
    ['GET', '/api/access/assignments', 'access:admin', undefined],
    ['POST', '/api/access/assignments', 'access:admin', { principals: ['amy'], roles: ['Viewer'] }],
    ['GET', '/api/audit', 'audit:read', undefined]
  ];

  describe.each(['', ...catalog.listRoles().map(role => role.name)])('holding %j', roles => {
    it.each(ROUTES)('%s %s needs %s', async (method, path, action, body) => {
      const result = await as(roles, method, path, body);
      const granted = !!roles && !!catalog.getRole(roles)!.allows(action);
      if (granted) {
        expect(result.statusCode).not.toBe(403);
      } else {
        expect(result.statusCode).toBe(403);
        expect(json(result)).toEqual({
          error: 'FORBIDDEN', action, roles: roles ? [roles] : [],
          message: roles
            ? `None of your roles (${roles}) has a policy that grants ${action}.`
            : `You have no role in this landscape, so you can't ${action}. Ask an Admin for a role.`
        });
      }
    });
  });

  it('serves the page to a user with no role: only data is refused', async () => {
    const result = await as('', 'GET', '/dashboard');
    expect(result.statusCode).toBe(200);
  });

  it('refuses an /api route with no mapping, even to an Admin', async () => {
    const result = await as('Admin', 'GET', '/api/secret');
    expect(result.statusCode).toBe(403);
    expect(json(result).error).toBe('FORBIDDEN');
  });

  it("answers a known route's wrong method 405, before asking for a role", async () => {
    const result = await as('', 'GET', '/api/bulk-sync/plan');
    expect(result.statusCode).toBe(405);
    expect(result.headers?.Allow).toBe('POST');
  });

  it('refuses the API to a request without a sign-in token that verifies', async () => {
    process.env.USER_ROLES = 'Admin';
    loaded.ServiceProvider.resetOfflineState();
    const forged = new TestSignIn().cookie({ buPrincipal: ['jdoe'] }, COOKIE);
    for (const cookie of [undefined, forged]) {
      const result = await loaded.handler(event('GET', '/api/runs', { cookie }));
      expect(result.statusCode).toBe(403);
      expect(json(result).message).toBe('Sign in to runs:read.');
    }
  });

  describe('/api/me', () => {
    it('says who the user is and that they have no role, and records their profile', async () => {
      const result = await as('', 'GET', '/api/me');
      expect(result.statusCode).toBe(200);
      expect(json(result)).toMatchObject({
        signedIn: true, user: { buPrincipal: 'jdoe', displayName: 'Jane Doe' }, roles: [], actions: [], publicMode: false
      });
      const profile = await loaded.ServiceProvider.getRoleAssignmentStore().getProfile('jdoe');
      expect(profile).toMatchObject({ displayName: 'Jane Doe' });
    });

    it("lists a user's roles, their policies and actions", async () => {
      const me = json(await as('Operator', 'GET', '/api/me'));
      expect(me.roles.map((r: any) => r.name)).toEqual(['Operator']);
      expect(me.roles[0].policies.map((p: any) => p.name)).toEqual(['RunReporting', 'PersonDataRead', 'PersonDataWrite', 'PipelineControl']);
      expect(me.actions).toEqual(catalog.getRole('Operator')!.actions());
    });

    it('needs sign-in', async () => {
      const result = await loaded.handler(event('GET', '/api/me'));
      expect(result.statusCode).toBe(403);
    });
  });

  describe("Bulk sync's choices", () => {
    const run = (extra: object) => ({ runType: 'full', ...extra });

    it('lets an Operator start from scratch: PipelineControl grants mock:reset', async () => {
      expect((await as('Operator', 'POST', '/api/bulk-sync/plan', run({ resetState: true }))).statusCode).toBe(200);
    });

    it('refuses Start from scratch, reviewing or starting, to an authorizer without mock:reset', async () => {
      // No role of the catalog grants bulk:start without mock:reset, so the service is asked directly
      process.env.USER_ROLES = '';
      loaded.ServiceProvider.resetOfflineState();
      const service = loaded.ServiceProvider.getBulkSyncService({});
      const asked: string[] = [];
      const authorizer = {
        authorize: async (action: Action) => {
          asked.push(action);
          return { allowed: action !== 'mock:reset', action, roles: ['Custom'], reason: `Custom doesn't grant ${action}.` };
        },
        effectivePermissions: async () => ({}) as any
      };
      await expect(service.plan(run({ resetState: true }), undefined, authorizer)).rejects.toMatchObject({ name: 'ForbiddenError' });
      await expect(service.start(run({ resetState: true }), undefined, authorizer)).rejects.toMatchObject({ name: 'ForbiddenError' });
      expect(asked).toEqual(['mock:reset', 'mock:reset']);
      expect((await service.describe()).launch).toBeUndefined();
      // Nothing else needs an action of its own
      asked.length = 0;
      await service.plan(run({}), undefined, authorizer);
      expect(asked).toEqual([]);
    });

    it('refuses Processor tasks above the landscape\'s maximum to an Operator, and allows them at it', async () => {
      // The offline landscape's maximum is 3 (OfflineBulkSyncService)
      const above = await as('Operator', 'POST', '/api/bulk-sync/plan', run({ processorScalingCapacityOverride: 5 }));
      expect(above.statusCode).toBe(403);
      expect(json(above)).toMatchObject({ error: 'FORBIDDEN', action: 'bulk:processors', roles: ['Operator'] });
      const at = await as('Operator', 'POST', '/api/bulk-sync/plan', run({ processorScalingCapacityOverride: 3 }));
      expect(at.statusCode).toBe(200);
    });

    it('lets an Admin start above the maximum', async () => {
      expect((await as('Admin', 'POST', '/api/bulk-sync/plan', run({ processorScalingCapacityOverride: 5 }))).statusCode).toBe(200);
    });

    it('starts nothing when a choice is refused', async () => {
      const refused = await as('Operator', 'POST', '/api/bulk-sync', run({ processorScalingCapacityOverride: 5 }));
      expect(refused.statusCode).toBe(403);
      process.env.USER_ROLES = 'Operator';
      const state = json(await loaded.handler(event('GET', '/api/bulk-sync', { cookie: jdoe })));
      expect(state.launch).toBeUndefined();
    });
  });
});

describe('the gate, public mode', () => {
  let loaded: ReturnType<typeof loadHandler>;
  beforeAll(() => {
    const { APP_LOGIN_HEADER, APP_LOGOUT_HEADER, ...env } = SIGNED_IN_ENV;
    // No app login/logout headers: public mode
    loaded = loadHandler({ ...env, APP_AUTHORIZATION: 'false' }, ['APP_LOGIN_HEADER', 'APP_LOGOUT_HEADER']);
  });
  afterAll(() => loaded.restore());

  it('allows every route to anyone, as before authorization', async () => {
    for (const [method, path, body] of [['GET', '/api/runs'], ['POST', '/api/bulk-sync/plan', { runType: 'full', processorScalingCapacityOverride: 5 }]] as const) {
      loaded.ServiceProvider.resetOfflineState();
      const result = await loaded.handler(event(method, path, { body: body as object | undefined }));
      expect(result.statusCode).toBe(200);
    }
  });

  it('says it is public at /api/me', async () => {
    const me = json(await loaded.handler(event('GET', '/api/me')));
    expect(me).toMatchObject({ signedIn: false, publicMode: true });
    expect(me.actions).not.toContain('access:impersonate');
  });

  it('still refuses an /api route with no mapping', async () => {
    expect((await loaded.handler(event('GET', '/api/secret'))).statusCode).toBe(403);
  });
});
