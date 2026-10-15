/**
 * Target Simulator - an HTTP stand-in for the Huron person API (the "target system").
 *
 * Deployed as a Lambda behind an API Gateway HTTP API (payload v2.0 events) in mock landscapes only
 * (see isMockLandscape in src/Utils.ts and lib/services/processor/TargetSimulator.ts). A mock
 * landscape's integration config points dataTarget.endpointConfig.baseUrl at that API,
 * so the pipeline's ECS tasks call it through the very same client code (HuronPersonDataTarget,
 * ReadPerson, ReadPeople, ListPeople) they use against the real Huron API - they cannot tell the
 * difference. Persons are stored in the landscape's mock target table (MockTargetPersonTable).
 *
 * Implements the subset of the Huron API the pipeline uses:
 * - GET   /loginsvc/api/v1/token/{userId}   Bearer <external token>  -> raw JWT (text)
 * - POST  /api/v2/persons                   create a person (hrn assigned from sourceIdentifier)
 * - PATCH /api/v2/persons/{hrn}             update; {active:false} deactivates (soft delete)
 * - GET   /api/v2/persons/{hrn}             -> { data: person }
 * - GET   /api/v2/persons?...               filtered, paged list -> { pagination, data: [...] }
 *     pagination[offset]=<page index>, pagination[pageSize]=N,
 *     filter[{priority}!{field}!{and|or}]=eq:{value}, include=a,b
 *
 * Contract details the client code depends on (see test/TargetSimulator.test.ts):
 * - "Not found" on a list query is 200 with data: [] (the client treats any GET error as "create").
 * - pagination[offset] is a PAGE INDEX, and paging stops on a short page - so a page past the end
 *   must be 200 with data: [].
 * - filter[...includeInactive...] is a flag (include deactivated persons), not a field match.
 * - All calls but the token request require a Bearer JWT issued by this simulator; anything else
 *   gets 401, to which the client responds by re-authenticating once and retrying.
 */
import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import type { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from 'aws-lambda';
import { createHmac, timingSafeEqual } from 'crypto';
import {
  MockTargetPersonRecord,
  MockTargetPersonStore,
  MockTargetPersonTable
} from '../dynamodb/MockTargetPersonTable';

export const FUNCTION_BASE_NAME = 'target-simulator';

export enum ENVIRONMENT_VARIABLES_NAMES {
  TABLE_NAME = 'DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME',
  TOKEN_SECRET_ARN = 'TOKEN_SECRET_ARN',
  REGION = 'REGION',
  LIST_CACHE_TTL_SECONDS = 'LIST_CACHE_TTL_SECONDS'
}

export const PERSONS_PATH = '/api/v2/persons';
export const TOKEN_PATH_PREFIX = '/loginsvc/api/v1/token/';
const HRN_PREFIX = 'hrn:hrs:persons:';
const DEFAULT_PAGE_SIZE = 500;
const TOKEN_LIFETIME_SECONDS = 60 * 60;

export const hrnFor = (sourceIdentifier: string): string => `${HRN_PREFIX}${sourceIdentifier}`;
export const personIdFromHrn = (hrn: string): string | undefined =>
  hrn.startsWith(HRN_PREFIX) ? hrn.substring(HRN_PREFIX.length) : undefined;

export type SimulatorRequest = {
  method: string;
  path: string;
  /** Decoded query parameters, in order (keys may repeat) */
  query: Array<[string, string]>;
  /** Header names lower-cased */
  headers: Record<string, string | undefined>;
  body?: string;
};

export type SimulatorResponse = {
  statusCode: number;
  headers: Record<string, string>;
  body: string;
};

// ===================== JWT (HS256, keyed by the external token) =====================

const base64url = (input: Buffer | string): string => Buffer.from(input).toString('base64url');

export const signJwt = (payload: Record<string, any>, key: string): string => {
  const header = base64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const body = base64url(JSON.stringify(payload));
  const signature = createHmac('sha256', key).update(`${header}.${body}`).digest('base64url');
  return `${header}.${body}.${signature}`;
};

export const verifyJwt = (token: string, key: string, nowSeconds: number): boolean => {
  const parts = token.split('.');
  if (parts.length !== 3) return false;
  const [header, body, signature] = parts;
  const expected = createHmac('sha256', key).update(`${header}.${body}`).digest('base64url');
  const a = Buffer.from(signature);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !timingSafeEqual(a, b)) return false;
  try {
    const { exp } = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
    return typeof exp === 'number' && exp > nowSeconds;
  } catch {
    return false;
  }
};

// ===================== Request handling =====================

type Filter = { field: string; logic: 'and' | 'or'; comparator: string; value: string };

const json = (statusCode: number, body: any): SimulatorResponse => ({
  statusCode,
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
});

/** Error body in the shape the pipeline's ApiErrorTracking parses */
const error = (status: number, message: string): SimulatorResponse => json(status, {
  errors: [{ status, internalErrorMessage: message, incidentId: `target-simulator-${Date.now()}`, detail: [] }]
});

export class TargetSimulator {
  private listCache?: { expires: number; records: MockTargetPersonRecord[] };

  constructor(private readonly params: {
    store: MockTargetPersonStore;
    /** The external token clients authenticate with - also the key the simulator signs JWTs with */
    getExpectedToken: () => Promise<string>;
    /** How long a full listing may be served from memory (sequential list pages, guard lookups) */
    listCacheTtlMs?: number;
    now?: () => Date;
  }) {}

  private now = (): Date => this.params.now?.() ?? new Date();

  public async handle(req: SimulatorRequest): Promise<SimulatorResponse> {
    const method = req.method.toUpperCase();
    const path = req.path.replace(/\/+$/, '') || '/';

    if (method === 'GET' && path.startsWith(TOKEN_PATH_PREFIX.replace(/\/$/, ''))) {
      return this.issueToken(req, path);
    }

    if (!await this.isAuthorized(req)) {
      return error(401, 'Missing, invalid or expired bearer token');
    }

    if (path === PERSONS_PATH) {
      if (method === 'GET') return this.listPersons(req);
      if (method === 'POST') return this.createPerson(req);
    }
    else if (path.startsWith(`${PERSONS_PATH}/`)) {
      const hrn = decodeURIComponent(path.substring(PERSONS_PATH.length + 1));
      if (method === 'GET') return this.getPerson(hrn);
      if (method === 'PATCH') return this.updatePerson(hrn, req);
    }

    return error(404, `No route for ${method} ${path}`);
  }

  // ---- auth ----

  private bearer(req: SimulatorRequest): string | undefined {
    const authorization = req.headers['authorization'] ?? '';
    const match = /^Bearer\s+(.+)$/i.exec(authorization.trim());
    return match?.[1].trim();
  }

  private async issueToken(req: SimulatorRequest, path: string): Promise<SimulatorResponse> {
    const expected = await this.params.getExpectedToken();
    const presented = this.bearer(req);
    if (!presented || presented !== expected) {
      return error(401, 'Invalid external token');
    }
    const userId = decodeURIComponent(path.substring(TOKEN_PATH_PREFIX.length - 1).replace(/^\//, ''));
    const iat = Math.floor(this.now().getTime() / 1000);
    const token = signJwt({ sub: userId, iss: FUNCTION_BASE_NAME, iat, exp: iat + TOKEN_LIFETIME_SECONDS }, expected);
    // Raw JWT text, as the real login service returns it
    return { statusCode: 200, headers: { 'Content-Type': 'text/plain' }, body: token };
  }

  private async isAuthorized(req: SimulatorRequest): Promise<boolean> {
    const token = this.bearer(req);
    if (!token) return false;
    const key = await this.params.getExpectedToken();
    return verifyJwt(token, key, Math.floor(this.now().getTime() / 1000));
  }

  // ---- persons ----

  private parseBody(req: SimulatorRequest): Record<string, any> | undefined {
    try {
      const parsed = JSON.parse(req.body || '{}');
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : undefined;
    } catch {
      return undefined;
    }
  }

  private async createPerson(req: SimulatorRequest): Promise<SimulatorResponse> {
    const body = this.parseBody(req);
    if (!body) return error(400, 'Request body must be a JSON object');

    const sourceIdentifier = body.sourceIdentifier;
    if (!sourceIdentifier || typeof sourceIdentifier !== 'string') {
      return error(400, 'sourceIdentifier is required');
    }
    if (await this.params.store.get(sourceIdentifier)) {
      return error(409, `A person with sourceIdentifier ${sourceIdentifier} already exists`);
    }

    const timestamp = this.now().toISOString();
    const hrn = hrnFor(sourceIdentifier);
    const { __arrayFieldOperations, hrn: _ignored, ...fields } = body;
    const data = { ...fields, hrn, active: body.active ?? true };
    const record: MockTargetPersonRecord = {
      personId: sourceIdentifier, hrn, data, createdAt: timestamp, lastModified: timestamp,
      ...(data.active === false && { deactivated: true, deactivatedAt: timestamp })
    };
    await this.params.store.put(record);
    this.listCache = undefined;
    return json(201, { data });
  }

  private async findByHrn(hrn: string): Promise<MockTargetPersonRecord | undefined> {
    const personId = personIdFromHrn(hrn);
    return personId ? this.params.store.get(personId) : undefined;
  }

  private async getPerson(hrn: string): Promise<SimulatorResponse> {
    const record = await this.findByHrn(hrn);
    return record ? json(200, { data: record.data }) : error(404, `Person ${hrn} not found`);
  }

  private async updatePerson(hrn: string, req: SimulatorRequest): Promise<SimulatorResponse> {
    const body = this.parseBody(req);
    if (!body) return error(400, 'Request body must be a JSON object');

    const record = await this.findByHrn(hrn);
    if (!record) return error(404, `Person ${hrn} not found`);

    const { __arrayFieldOperations, hrn: _ignored, ...fields } = body;
    const appendFields: string[] = __arrayFieldOperations?.append ?? [];
    const data = { ...record.data };
    for (const [field, value] of Object.entries(fields)) {
      if (appendFields.includes(field) && Array.isArray(value) && Array.isArray(data[field])) {
        // Append without duplicating entries already present
        const existing = new Set(data[field].map((v: any) => JSON.stringify(v)));
        data[field] = [...data[field], ...value.filter(v => !existing.has(JSON.stringify(v)))];
      } else {
        data[field] = value;
      }
    }

    const timestamp = this.now().toISOString();
    const updated: MockTargetPersonRecord = { ...record, data, lastModified: timestamp };
    if (fields.active === false && !record.deactivated) {
      updated.deactivated = true;
      updated.deactivatedAt = timestamp;
    } else if (fields.active === true) {
      updated.deactivated = false;
      delete updated.deactivatedAt;
    }

    await this.params.store.put(updated);
    this.listCache = undefined;
    return json(200, { data });
  }

  private parseListQuery(query: Array<[string, string]>): {
    pageIndex: number; pageSize: number; filters: Filter[]; includeInactive: boolean; include?: string[]
  } | string {
    let pageIndex = 0;
    let pageSize = DEFAULT_PAGE_SIZE;
    let includeInactive = false;
    let include: string[] | undefined;
    const filters: Filter[] = [];

    for (const [key, value] of query) {
      if (key === 'pagination[offset]') {
        pageIndex = Number(value);
      } else if (key === 'pagination[pageSize]') {
        pageSize = Number(value);
      } else if (key === 'include') {
        include = value.split(',').map(s => s.trim()).filter(Boolean);
      } else {
        const match = /^filter\[(\d+)!([^!\]]+)!(and|or)\]$/.exec(key);
        if (!match) continue; // e.g. sort - ignored
        const [, , field, logic] = match;
        const separator = value.indexOf(':');
        const comparator = separator < 0 ? 'eq' : value.substring(0, separator);
        const operand = separator < 0 ? value : value.substring(separator + 1);
        if (field === 'includeInactive') {
          includeInactive = operand === 'true';
          continue;
        }
        if (comparator !== 'eq') {
          return `Unsupported filter comparator "${comparator}" (only "eq" is simulated)`;
        }
        filters.push({ field, logic: logic as 'and' | 'or', comparator, value: operand });
      }
    }

    if (!Number.isInteger(pageIndex) || pageIndex < 0 || !Number.isInteger(pageSize) || pageSize <= 0) {
      return 'Invalid pagination';
    }
    return { pageIndex, pageSize, filters, includeInactive, include };
  }

  private matches(data: Record<string, any>, filters: Filter[]): boolean {
    const test = (f: Filter) => data[f.field] !== undefined && data[f.field] !== null && `${data[f.field]}` === f.value;
    const ands = filters.filter(f => f.logic === 'and');
    const ors = filters.filter(f => f.logic === 'or');
    return ands.every(test) && (ors.length === 0 || ors.some(test));
  }

  /**
   * Candidate records for a filter set: a direct key lookup when every match must be a specific
   * sourceIdentifier (the per-person lookups made while syncing), otherwise a full listing.
   */
  private async candidates(filters: Filter[]): Promise<MockTargetPersonRecord[]> {
    const keyFields = new Set(['sourceIdentifier', 'id']);
    const ands = filters.filter(f => f.logic === 'and');
    const ors = filters.filter(f => f.logic === 'or');
    const andKey = ands.find(f => f.field === 'sourceIdentifier');
    const allOrsAreKeys = ors.length > 0 && ors.every(f => keyFields.has(f.field));

    const keys = andKey ? [andKey.value] : allOrsAreKeys ? [...new Set(ors.map(f => f.value))] : undefined;
    if (keys) {
      // id and sourceIdentifier are both the BUID in this integration
      const found = await Promise.all(keys.map(k => this.params.store.get(k)));
      return found.filter((r): r is MockTargetPersonRecord => !!r);
    }

    const ttl = this.params.listCacheTtlMs ?? 0;
    const nowMs = this.now().getTime();
    if (ttl > 0 && this.listCache && this.listCache.expires > nowMs) {
      return this.listCache.records;
    }
    const records = (await this.params.store.listAll())
      .sort((a, b) => a.personId.localeCompare(b.personId));
    if (ttl > 0) {
      this.listCache = { expires: nowMs + ttl, records };
    }
    return records;
  }

  private async listPersons(req: SimulatorRequest): Promise<SimulatorResponse> {
    const parsed = this.parseListQuery(req.query);
    if (typeof parsed === 'string') return error(400, parsed);
    const { pageIndex, pageSize, filters, includeInactive, include } = parsed;

    const matching = (await this.candidates(filters))
      .filter(r => includeInactive || r.data.active !== false)
      .filter(r => this.matches(r.data, filters));

    const page = matching.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize).map(r => {
      if (!include) return r.data;
      return Object.fromEntries(include.filter(f => f in r.data).map(f => [f, r.data[f]]));
    });

    return json(200, { pagination: { offset: pageIndex, pageSize, total: matching.length }, data: page });
  }
}

// ===================== Lambda adapter =====================

/**
 * Convert an API Gateway HTTP API (payload v2.0) event to a SimulatorRequest. Query keys arrive raw
 * (e.g. pagination[offset]) or percent-encoded (pagination%5Boffset%5D) - both decode the same.
 * NOTE: Not a Lambda Function URL - those reject the Huron client's raw [ ] with a 400 before
 * invoking the function.
 */
export const toSimulatorRequest = (event: APIGatewayProxyEventV2): SimulatorRequest => {
  const decode = (s: string) => {
    try { return decodeURIComponent(s.replace(/\+/g, ' ')); } catch { return s; }
  };
  const query: Array<[string, string]> = (event.rawQueryString || '')
    .split('&')
    .filter(Boolean)
    .map(pair => {
      const i = pair.indexOf('=');
      return i < 0 ? [decode(pair), ''] : [decode(pair.substring(0, i)), decode(pair.substring(i + 1))];
    });
  const headers = Object.fromEntries(
    Object.entries(event.headers || {}).map(([k, v]) => [k.toLowerCase(), v])
  );
  const body = event.body && event.isBase64Encoded
    ? Buffer.from(event.body, 'base64').toString('utf8')
    : event.body;
  return { method: event.requestContext.http.method, path: event.rawPath || '/', query, headers, body };
};


let simulator: TargetSimulator | undefined;

/**
 * Caching (per warm Lambda container): the simulator is built on the first invocation and kept in
 * the module-level `simulator` variable, so every later invocation in the same container reuses it.
 * The external token lives in that instance's `expectedToken` closure variable and is fetched from
 * Secrets Manager only on the first token check (`??=`) - after that, warm invocations reuse it with
 * no Secrets Manager call. So it is one fetch per container, not one per request.
 * - The PROMISE is cached (not just the string), so concurrent checks that start before the first
 *   fetch completes share that single in-flight request instead of each making their own.
 * - A failed fetch is NOT cached (`expectedToken` is reset in the catch), so a transient error
 *   (e.g. throttling) is retried on the next request instead of failing until the container recycles.
 * - A changed token is only picked up when containers recycle - fine here, since CDK generates the
 *   token once and nothing rotates it.
 *
 * @returns The singleton instance of the TargetSimulator.
 */
const getSimulator = (): TargetSimulator => {
  if (simulator) return simulator;
  const {
    [ENVIRONMENT_VARIABLES_NAMES.TABLE_NAME]: tableName,
    [ENVIRONMENT_VARIABLES_NAMES.TOKEN_SECRET_ARN]: tokenSecretArn,
    [ENVIRONMENT_VARIABLES_NAMES.REGION]: region,
    [ENVIRONMENT_VARIABLES_NAMES.LIST_CACHE_TTL_SECONDS]: listCacheTtlSeconds = '30'
  } = process.env;

  let expectedToken: Promise<string> | undefined;
  const getExpectedToken = () => {
    expectedToken ??= new SecretsManagerClient({ region })
      .send(new GetSecretValueCommand({ SecretId: tokenSecretArn }))
      .then(({ SecretString }) => {
        if (!SecretString) throw new Error('Target simulator token secret is empty');
        return SecretString;
      })
      .catch(e => { expectedToken = undefined; throw e; });
    return expectedToken;
  };

  simulator = new TargetSimulator({
    store: new MockTargetPersonTable({ tableName, region }),
    getExpectedToken,
    listCacheTtlMs: Number(listCacheTtlSeconds) * 1000
  });
  return simulator;
};

export async function handler(event: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> {
  const request = toSimulatorRequest(event);
  try {
    const response = await getSimulator().handle(request);
    console.log(`${request.method} ${request.path} -> ${response.statusCode}`);
    return response;
  } catch (e: any) {
    console.error(`${request.method} ${request.path} failed:`, e);
    return error(500, e?.message ?? 'Internal error');
  }
}
