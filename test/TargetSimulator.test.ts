/**
 * Contract tests for the target simulator (src/target-simulator/TargetSimulator.ts).
 *
 * The simulator only earns its keep if the pipeline's REAL Huron client code - unchanged - works
 * against it. So rather than asserting on hand-built requests, these tests run the simulator behind
 * a local HTTP server (via the same payload v2.0 event adapter the Lambda uses) and drive it with
 * integration-huron-person's own HuronPersonDataTarget, ReadPerson and ListPeople.
 *
 * LIMITATION: Node's HTTP server accepts anything the client sends, so these tests cannot catch an
 * AWS front door that rejects a request before invoking the Lambda - as a Lambda Function URL does
 * for the raw [ ] in the Huron client's query strings. That can only be verified by probing the
 * deployed endpoint (see the "Mock Landscapes" section of CLAUDE.md).
 */
import http from 'http';
import { AddressInfo } from 'net';
import { CrudOperation, FieldSet, Status } from 'integration-core';
import { HuronPersonDataTarget, ListPeople, ReadPerson, TargetPersonDeleteType } from 'integration-huron-person';
import { MockTargetPersonRecord, MockTargetPersonStore } from '../src/dynamodb/MockTargetPersonTable';
import { hrnFor, signJwt, TargetSimulator, toSimulatorRequest } from '../src/target-simulator/TargetSimulator';

const EXTERNAL_TOKEN = 'test-external-token';
const API_USER = 'bu-sso_api-user@bu.edu';

class InMemoryStore implements MockTargetPersonStore {
  public records = new Map<string, MockTargetPersonRecord>();
  async get(personId: string) { return this.records.get(personId); }
  async put(record: MockTargetPersonRecord) { this.records.set(record.personId, JSON.parse(JSON.stringify(record))); }
  async listAll() { return [...this.records.values()]; }
}

const person = (buid: string, extra: Record<string, any>[] = []): FieldSet => ({
  fieldValues: [
    { id: buid },
    { sourceIdentifier: buid },
    { employeeId: buid },
    { firstName: `First${buid}` },
    { lastName: `Last${buid}` },
    { userId: `bu-sso_${buid}@bu.edu` },
    { employer: { hrn: 'lookup:sourceIdentifier:10003827' } },
    { organization: { hrn: 'lookup:sourceIdentifier:10003827' } },
    { roles: [{ hrn: 'hrn:hrs:lists:roles/site-user' }] },
    { __arrayFieldOperations: { append: ['roles'] } },
    ...extra
  ]
} as FieldSet);

describe('TargetSimulator (contract with the real Huron client code)', () => {
  let store: InMemoryStore;
  let simulator: TargetSimulator;
  let server: http.Server;
  let config: any;
  const requests: string[] = [];

  beforeAll(async () => {
    server = http.createServer((req, res) => {
      let body = '';
      req.on('data', chunk => (body += chunk));
      req.on('end', async () => {
        const [rawPath, rawQueryString = ''] = (req.url || '/').split('?');
        requests.push(`${req.method} ${req.url}`);
        // Same adapter the Lambda handler uses for API Gateway HTTP API (payload v2.0) events
        const request = toSimulatorRequest({
          rawPath, rawQueryString, headers: req.headers as any, body, isBase64Encoded: false,
          requestContext: { http: { method: req.method } }
        } as any);
        const response = await simulator.handle(request);
        res.writeHead(response.statusCode, response.headers);
        res.end(response.body);
      });
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as AddressInfo;

    config = {
      dataTarget: {
        endpointConfig: {
          baseUrl: `http://127.0.0.1:${port}`,
          authMethod: 'externalToken',
          externalToken: EXTERNAL_TOKEN,
          userId: API_USER,
          loginSvcPath: '/loginsvc/api/v1/token/',
          timeout: 5000
        },
        personsPath: '/api/v2/persons',
        organizationsPath: '/api/v2/organizations',
        personDeleteType: TargetPersonDeleteType.SOFT
      },
      cache: { enabled: false }
    };
  });

  afterAll(async () => {
    await new Promise<void>(resolve => server.close(() => resolve()));
  });

  beforeEach(() => {
    store = new InMemoryStore();
    simulator = new TargetSimulator({ store, getExpectedToken: async () => EXTERNAL_TOKEN });
    requests.length = 0;
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => jest.restoreAllMocks());

  const target = () => new HuronPersonDataTarget({ config });

  it('CREATE: HuronPersonDataTarget creates the person, which the simulator assigns an hrn', async () => {
    const result = await target().pushOne({ data: person('U0000001'), crud: CrudOperation.CREATE });

    expect(result.status).toBe(Status.SUCCESS);
    const record = store.records.get('U0000001')!;
    expect(record.hrn).toBe(hrnFor('U0000001'));
    expect(record.data).toMatchObject({
      hrn: hrnFor('U0000001'), id: 'U0000001', sourceIdentifier: 'U0000001', firstName: 'FirstU0000001', active: true
    });
    expect(record.data).not.toHaveProperty('__arrayFieldOperations');
    // The client authenticated first, via the token endpoint
    expect(requests[0]).toBe(`GET /loginsvc/api/v1/token/${API_USER}`);
  });

  it('UPDATE: the client finds the hrn via its "hail mary" lookup, then PATCHes - roles are appended, not replaced', async () => {
    await target().pushOne({ data: person('U0000001'), crud: CrudOperation.CREATE });

    const updated = person('U0000001', [{ title: 'Professor' }]);
    (updated.fieldValues.find((fv: any) => 'roles' in fv) as any).roles = [
      { hrn: 'hrn:hrs:lists:roles/site-user' }, { hrn: 'hrn:hrs:lists:roles/irb-general-user' }
    ];
    const result = await target().pushOne({ data: updated, crud: CrudOperation.UPDATE });

    expect(result.status).toBe(Status.SUCCESS);
    expect(requests.some(r => r.startsWith(`PATCH /api/v2/persons/${hrnFor('U0000001')}`))).toBe(true);
    const { data } = store.records.get('U0000001')!;
    expect(data.title).toBe('Professor');
    expect(data.roles).toEqual([
      { hrn: 'hrn:hrs:lists:roles/site-user' }, { hrn: 'hrn:hrs:lists:roles/irb-general-user' }
    ]);
    expect(data.userId).toBe('bu-sso_U0000001@bu.edu'); // userId is never sent on UPDATE
  });

  it('UPDATE of an unknown person fails without a PATCH (lookup returns 200 with no data)', async () => {
    const result = await target().pushOne({ data: person('U0000404'), crud: CrudOperation.UPDATE });

    expect(result.status).toBe(Status.FAILURE);
    expect(requests.some(r => r.startsWith('PATCH'))).toBe(false);
  });

  it('DELETE is a soft delete (after the self-deactivation guard lookup), and __active reactivates', async () => {
    await target().pushOne({ data: person('U0000001'), crud: CrudOperation.CREATE });

    const deleted = await target().pushOne({ data: { fieldValues: [{ sourceIdentifier: 'U0000001' }] } as FieldSet, crud: CrudOperation.DELETE });

    expect(deleted.status).toBe(Status.SUCCESS);
    expect(requests.some(r => r.includes('filter[0!userId!and]'))).toBe(true); // self-deactivation guard
    let record = store.records.get('U0000001')!;
    expect(record.data.active).toBe(false);
    expect(record.deactivated).toBe(true);
    expect(record.deactivatedAt).toBeDefined();

    const reactivated = await target().pushOne({ data: person('U0000001', [{ __active: true }]), crud: CrudOperation.UPDATE });

    expect(reactivated.status).toBe(Status.SUCCESS);
    record = store.records.get('U0000001')!;
    expect(record.data.active).toBe(true);
    expect(record.deactivated).toBe(false);
    expect(record.deactivatedAt).toBeUndefined();
  });

  it('ReadPerson.readPersonBySourceIdentifier: finds persons (inactive included), honors include, and returns [] - not an error - when absent', async () => {
    await target().pushOne({ data: person('U0000001'), crud: CrudOperation.CREATE });
    await target().pushOne({ data: { fieldValues: [{ sourceIdentifier: 'U0000001' }] } as FieldSet, crud: CrudOperation.DELETE });
    const reader = new ReadPerson({ config });

    await expect(reader.readPersonBySourceIdentifier('U0000001', ['hrn', 'id', 'sourceIdentifier']))
      .resolves.toEqual([{ hrn: hrnFor('U0000001'), id: 'U0000001', sourceIdentifier: 'U0000001' }]);
    await expect(reader.readPersonBySourceIdentifier('U0000404', ['hrn'])).resolves.toEqual([]);
  });

  it.each([1001, 1000])('ListPeople pages through all %d persons (page-index offsets, terminating on a short page)', async (count) => {
    for (let i = 1; i <= count; i++) {
      const buid = `U${String(i).padStart(7, '0')}`;
      await store.put({
        personId: buid, hrn: hrnFor(buid), createdAt: 't', lastModified: 't',
        data: { hrn: hrnFor(buid), id: buid, sourceIdentifier: buid, active: i % 10 !== 0 }
      });
    }

    const people = await new ListPeople(config, 500).listSourceIdentifiers();

    expect(people).toHaveLength(count); // inactive persons included
    expect(new Set(people.map(p => p.sourceIdentifier)).size).toBe(count);
    expect(Object.keys(people[0])).toEqual(['sourceIdentifier']);
    // 1001 -> pages 0,1,2 (2 is short); 1000 -> pages 0,1 full, so page 2 comes back empty
    expect(requests.filter(r => r.startsWith('GET /api/v2/persons?'))).toHaveLength(3);
  });

  describe('toSimulatorRequest (HTTP API payload v2.0 adapter)', () => {
    const event = (rawQueryString: string) => ({
      rawPath: '/api/v2/persons', rawQueryString, headers: {}, isBase64Encoded: false,
      requestContext: { http: { method: 'GET' } }
    } as any);

    it('decodes raw and percent-encoded bracketed query keys identically', () => {
      const raw = toSimulatorRequest(event(
        'pagination[offset]=0&pagination[pageSize]=500&filter[0!includeInactive!and]=eq:true&filter[0!userId!and]=eq:bu-sso_a%40bu.edu&include=sourceIdentifier'
      ));
      const encoded = toSimulatorRequest(event(
        'pagination%5Boffset%5D=0&pagination%5BpageSize%5D=500&filter%5B0%21includeInactive%21and%5D=eq%3Atrue&filter%5B0%21userId%21and%5D=eq%3Abu-sso_a%40bu.edu&include=sourceIdentifier'
      ));

      expect(raw.query).toEqual([
        ['pagination[offset]', '0'],
        ['pagination[pageSize]', '500'],
        ['filter[0!includeInactive!and]', 'eq:true'],
        ['filter[0!userId!and]', 'eq:bu-sso_a@bu.edu'],
        ['include', 'sourceIdentifier'],
      ]);
      expect(encoded.query).toEqual(raw.query);
    });
  });

  describe('auth', () => {
    const call = (authorization?: string, path = '/api/v2/persons') => simulator.handle({
      method: 'GET', path, query: [], headers: authorization ? { authorization } : {}
    });

    it('issues tokens only for the expected external token', async () => {
      expect((await call(`Bearer wrong`, `/loginsvc/api/v1/token/${API_USER}`)).statusCode).toBe(401);
      const ok = await call(`Bearer ${EXTERNAL_TOKEN}`, `/loginsvc/api/v1/token/${API_USER}`);
      expect(ok.statusCode).toBe(200);
      expect(ok.body.split('.')).toHaveLength(3);
    });

    it('rejects API calls without a valid, unexpired simulator-issued JWT', async () => {
      const now = Math.floor(Date.now() / 1000);
      expect((await call()).statusCode).toBe(401);
      expect((await call(`Bearer ${EXTERNAL_TOKEN}`)).statusCode).toBe(401);
      expect((await call(`Bearer ${signJwt({ exp: now + 60 }, 'some-other-key')}`)).statusCode).toBe(401);
      expect((await call(`Bearer ${signJwt({ exp: now - 1 }, EXTERNAL_TOKEN)}`)).statusCode).toBe(401);
      expect((await call(`Bearer ${signJwt({ exp: now + 60 }, EXTERNAL_TOKEN)}`)).statusCode).toBe(200);
    });
  });
});
