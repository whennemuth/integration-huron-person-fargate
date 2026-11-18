/**
 * The audit trail (dashboard/src/audit/AuditTrail.ts, authorization-plan.md decision 9): what is recorded of which
 * requests, and GET /api/audit. The rules on their own, then through the handler offline.
 */
import { Principal } from '../dashboard/src/authorization/Principal';
import { RoutePermissions } from '../dashboard/src/authorization/RoutePermissions';
import { AuditStore, InMemoryAuditStore } from '../dashboard/src/audit/AuditStore';
import { AuditTrail, auditsAllowed, describeRequest, MAX_DETAIL_LENGTH } from '../dashboard/src/audit/AuditTrail';
import { COOKIE, event, json, loadHandler, signedInEnvironment } from './support/OfflineDashboard';
import { TestSignIn } from './support/TestSignIn';

describe('which allowed requests are audited', () => {
  it.each([
    ['POST', '/api/person-lookup', true],
    ['POST', '/api/person-sync/preview', true],
    ['POST', '/api/person-sync/preview-experiment', true],
    ['POST', '/api/person-sync', true],
    ['POST', '/api/bulk-sync', true],
    ['POST', '/api/mock-landscape/reset', true],
    ['GET', '/api/access/assignments', true],
    ['PUT', '/api/access/assignments/jdoe', true],
    ['POST', '/api/bulk-sync/plan', false],
    ['GET', '/api/runs', false],
    ['GET', '/api/bulk-sync', false],
    ['GET', '/api/audit', false],
    ['GET', '/api/me', false],
    ['GET', '/dashboard', false]
  ])('%s %s: %s', (method, path, audited) => {
    expect(auditsAllowed(RoutePermissions.permissionFor(method, path))).toBe(audited);
  });
});

describe('describeRequest', () => {
  it('names the person a person route is about', () => {
    expect(describeRequest('/api/person-lookup', JSON.stringify({ personId: 'U123', system: 'source', searchType: 'buid' })))
      .toEqual({ target: 'U123', detail: 'source by buid' });
    expect(describeRequest('/api/person-lookup', JSON.stringify({ firstName: 'Jane', lastName: 'Doe', system: 'target', searchType: 'name' })))
      .toEqual({ target: 'Jane Doe', detail: 'target by name' });
    expect(describeRequest('/api/person-sync', JSON.stringify({ personId: 'U123', operation: 'update' }))).toEqual({ target: 'U123', detail: 'update' });
    expect(describeRequest('/api/person-sync/preview-experiment', JSON.stringify({ personId: 'U123' }))).toEqual({ target: 'U123', detail: 'experiment' });
  });

  it("gives a start's choices, and a test's BUID", () => {
    expect(describeRequest('/api/bulk-sync', JSON.stringify({ runType: 'full', resetState: true, processorScalingCapacityOverride: 8, overrideStalled: false })))
      .toEqual({ detail: 'runType=full, resetState=true, processorScalingCapacityOverride=8' });
    expect(describeRequest('/api/bulk-sync', JSON.stringify({ runType: 'test', buid: 'U0000042' }))).toEqual({ target: 'U0000042', detail: 'runType=test' });
  });

  it("names the person of an access route, and copes with a body it can't read", () => {
    expect(describeRequest('/api/access/assignments/jdoe%40bu.edu', null)).toEqual({ target: 'jdoe@bu.edu' });
    expect(describeRequest('/api/person-sync', '{not json')).toEqual({});
    expect(describeRequest('/api/mock-landscape/reset', null)).toEqual({});
  });
});

describe('AuditTrail', () => {
  const trail = (store: AuditStore, principal = Principal.of('jdoe')) => new AuditTrail({ store, landscape: 'mock', principal });

  it('records an allowed request with its outcome, and a failure with its status', async () => {
    const store = new InMemoryAuditStore();
    await trail(store).allowed('person:sync', 200, { target: 'U123', detail: 'update' });
    await trail(store).allowed('person:sync', 502, { target: 'U123' });
    const [failed, succeeded] = (await store.list()).items;
    expect(succeeded).toMatchObject({ landscape: 'mock', buPrincipal: 'jdoe', action: 'person:sync', decision: 'ALLOWED', outcome: 'SUCCEEDED', target: 'U123', detail: 'update' });
    expect(failed).toMatchObject({ outcome: 'FAILED', detail: 'HTTP 502' });
  });

  it('records a denial with its reason, and remembers it', async () => {
    const store = new InMemoryAuditStore();
    const audit = trail(store);
    await audit.denied({ allowed: false, action: 'bulk:start', roles: ['Viewer'], reason: 'None of your roles (Viewer) has a policy that grants bulk:start.' });
    expect(audit.denialRecorded).toBe(true);
    const [record] = (await store.list()).items;
    expect(record).toMatchObject({ decision: 'DENIED', action: 'bulk:start', detail: 'None of your roles (Viewer) has a policy that grants bulk:start.' });
    expect(record).not.toHaveProperty('outcome');
  });

  it('names an anonymous request anonymous', async () => {
    const store = new InMemoryAuditStore();
    await trail(store, Principal.anonymous()).allowed('person:sync', 200, {});
    expect((await store.list()).items[0].buPrincipal).toBe('anonymous');
  });

  it('shortens a long detail', async () => {
    const store = new InMemoryAuditStore();
    await trail(store).allowed('bulk:start', 202, { detail: 'x'.repeat(MAX_DETAIL_LENGTH * 2) });
    expect((await store.list()).items[0].detail!.length).toBe(MAX_DETAIL_LENGTH);
  });

  it('never fails the request when the store does', async () => {
    const failing: AuditStore = { append: () => Promise.reject(new Error('throttled')), list: async () => ({ items: [] }) };
    const error = jest.spyOn(console, 'error').mockImplementation(() => {});
    try {
      await expect(trail(failing).allowed('person:sync', 200, {})).resolves.toBeUndefined();
      expect(error.mock.calls[0][0]).toMatch(/Couldn't record ALLOWED person:sync for jdoe.*throttled/);
    } finally {
      jest.restoreAllMocks();
    }
  });
});

describe('the audit trail through the handler', () => {
  const signIn = new TestSignIn();
  const jdoe = signIn.cookie({ buPrincipal: ['jdoe'] }, COOKIE);
  let loaded: ReturnType<typeof loadHandler>;

  // The handler's modules load (and compile) on the first request
  jest.setTimeout(60_000);

  beforeAll(() => { loaded = loadHandler(signedInEnvironment(signIn)); });
  afterAll(() => loaded.restore());
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'info').mockImplementation(() => {});
    loaded.ServiceProvider.resetOfflineState();
  });
  afterEach(() => jest.restoreAllMocks());

  /** A request as jdoe, holding these roles */
  const as = (roles: string, method: string, path: string, options: { body?: object; query?: Record<string, string> } = {}) => {
    process.env.USER_ROLES = roles;
    return loaded.handler(event(method, path, { ...options, cookie: jdoe }));
  };
  const records = async () => (await loaded.ServiceProvider.getAuditStore().list()).items;

  it('records a denial at the gate', async () => {
    expect((await as('Viewer', 'POST', '/api/bulk-sync', { body: { runType: 'test', buid: 'U0000042' } })).statusCode).toBe(403);
    expect(await records()).toEqual([expect.objectContaining({
      landscape: 'mock', buPrincipal: 'jdoe', action: 'bulk:start', decision: 'DENIED', target: 'U0000042',
      detail: 'None of your roles (Viewer) has a policy that grants bulk:start.'
    })]);
  });

  it('records reads of personal data and changes, not reads of runs or reviews', async () => {
    await as('Operator', 'GET', '/api/runs');
    await as('Operator', 'POST', '/api/bulk-sync/plan', { body: { runType: 'full' } });
    await as('Operator', 'POST', '/api/person-lookup', { body: { personId: 'U12345678', system: 'source', searchType: 'buid' } });
    await as('Operator', 'POST', '/api/bulk-sync', { body: { runType: 'full' } });
    const [start, lookup, ...rest] = await records();
    expect(rest).toEqual([]);
    expect(lookup).toMatchObject({ action: 'person:lookup', decision: 'ALLOWED', target: 'U12345678' });
    expect(start).toMatchObject({ action: 'bulk:start', decision: 'ALLOWED', outcome: 'SUCCEEDED', detail: 'runType=full' });
  });

  it('records a refused choice as a denial of its action, and nothing more', async () => {
    const result = await as('Operator', 'POST', '/api/bulk-sync', { body: { runType: 'full', processorScalingCapacityOverride: 5 } });
    expect(result.statusCode).toBe(403);
    expect(await records()).toEqual([expect.objectContaining({ action: 'bulk:processors', decision: 'DENIED', buPrincipal: 'jdoe' })]);
  });

  it('records a refused start as FAILED', async () => {
    // A test without a BUID is invalid (400)
    await as('Operator', 'POST', '/api/bulk-sync', { body: { runType: 'test' } });
    expect((await records())[0]).toMatchObject({ action: 'bulk:start', decision: 'ALLOWED', outcome: 'FAILED', detail: 'runType=test (HTTP 400)' });
  });

  describe('GET /api/audit', () => {
    it('lists the trail to an Admin, newest first, and filters it', async () => {
      await as('Viewer', 'POST', '/api/person-sync', { body: { personId: 'U1' } });
      await as('Operator', 'POST', '/api/person-lookup', { body: { personId: 'U2' } });
      const all = json(await as('Admin', 'GET', '/api/audit'));
      expect(all.items.map((r: any) => r.action)).toEqual(['person:lookup', 'person:sync']);
      const denials = json(await as('Admin', 'GET', '/api/audit', { query: { action: 'person:sync' } }));
      expect(denials.items).toEqual([expect.objectContaining({ decision: 'DENIED', target: 'U1' })]);
      const page = json(await as('Admin', 'GET', '/api/audit', { query: { limit: '1' } }));
      expect(page.items.length).toBe(1);
      const next = json(await as('Admin', 'GET', '/api/audit', { query: { limit: '1', cursor: page.cursor } }));
      expect(next.items[0].action).toBe('person:sync');
      expect(next.cursor).toBeUndefined();
    });

    it('is not itself audited', async () => {
      await as('Admin', 'GET', '/api/audit');
      expect(await records()).toEqual([]);
    });

    it('refuses an Operator, recording the denial', async () => {
      expect((await as('Operator', 'GET', '/api/audit')).statusCode).toBe(403);
      expect(await records()).toEqual([expect.objectContaining({ action: 'audit:read', decision: 'DENIED' })]);
    });

    it('answers a bad limit, filter or cursor 400', async () => {
      const queries: Record<string, string>[] = [{ limit: '0' }, { limit: 'many' }, { action: 'bulk:stop' }, { principal: 'x@gmail.com' }, { cursor: 'nope' }];
      for (const query of queries) {
        const result = await as('Admin', 'GET', '/api/audit', { query });
        expect(result.statusCode).toBe(400);
        expect(json(result).code).toMatch(/^INVALID_/);
      }
    });
  });
});

describe('the audit trail in public mode', () => {
  const signIn = new TestSignIn();
  let loaded: ReturnType<typeof loadHandler>;
  jest.setTimeout(60_000);

  beforeAll(() => {
    loaded = loadHandler({ ...signedInEnvironment(signIn), APP_AUTHORIZATION: 'false' }, ['APP_LOGIN_HEADER', 'APP_LOGOUT_HEADER']);
  });
  afterAll(() => loaded.restore());

  it('names the principal anonymous', async () => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    try {
      loaded.ServiceProvider.resetOfflineState();
      await loaded.handler(event('POST', '/api/person-lookup', { body: { personId: 'U2' } }));
      const [record] = (await loaded.ServiceProvider.getAuditStore().list()).items;
      expect(record).toMatchObject({ buPrincipal: 'anonymous', action: 'person:lookup', decision: 'ALLOWED' });
    } finally {
      jest.restoreAllMocks();
    }
  });
});
