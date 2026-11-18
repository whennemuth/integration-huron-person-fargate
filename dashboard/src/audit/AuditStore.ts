import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { randomUUID } from 'crypto';
import { TestEnvironment } from 'integration-core';
import { Action, isAction } from '../authorization/Actions';
import { PrincipalResolver } from '../authorization/Principal';

/** Who made an audited request in a public dashboard, which has no sign-in (authorization-plan.md, decision 12) */
export const ANONYMOUS = 'anonymous';

/** What is recorded of one audited request (decision 9) */
export type AuditEntry = {
  landscape: string;
  /** Who made the request: the signed-in user, never the person they impersonate; `anonymous` in public mode */
  buPrincipal: string;
  /** While impersonating: the person whose roles decided the request (decision 14) */
  viewedAs?: string;
  action: Action;
  /** What it was done to: a BUID or HRN, a run, or a principal */
  target?: string;
  decision: 'ALLOWED' | 'DENIED';
  /** Absent for a denial, which never ran */
  outcome?: 'SUCCEEDED' | 'FAILED';
  /** A short detail: a denial's reason, a start's choices, a role change */
  detail?: string;
};

export type AuditRecord = AuditEntry & { id: string; at: string };

export type AuditPage = { items: AuditRecord[]; cursor?: string };

export type AuditQuery = { cursor?: string; limit?: number; buPrincipal?: string; action?: string };

export class AuditError extends Error {
  constructor(message: string, readonly code: 'INVALID_CURSOR' | 'INVALID_LIMIT' | 'INVALID_FILTER') {
    super(message);
    this.name = 'AuditError';
  }
}

/**
 * The landscape's audit trail (decision 9): appended to, and listed newest first, optionally only one person's
 * requests or one action's.
 */
export interface AuditStore {
  append(entry: AuditEntry, at?: Date): Promise<AuditRecord>;
  list(query?: AuditQuery): Promise<AuditPage>;
}

/** The audit table's name: one per landscape, owned by its dashboard stack (dashboard/lib/AccessTables.ts) */
export const auditTableName = (stackId: string, landscape: string): string => `${stackId}-audit-${landscape.toLowerCase()}`;

/** Every record shares one partition: the volume is human-scale, and one partition lists them all in time order */
export const AUDIT_PARTITION = 'AUDIT';
/** The index listing one person's records in time order */
export const PRINCIPAL_INDEX = 'principal-index';

export const AUDIT_LIMITS = { DEFAULT: 50, MAX: 100 };

const sortKeyOf = (record: { at: string; id: string }) => `${record.at}#${record.id}`;

/** The query's limit and filters, checked; refused as an AuditError */
const checkQuery = (query: AuditQuery): { limit: number; buPrincipal?: string; action?: Action } => {
  const limit = query.limit ?? AUDIT_LIMITS.DEFAULT;
  if (!Number.isInteger(limit) || limit < 1 || limit > AUDIT_LIMITS.MAX) {
    throw new AuditError(`limit must be a whole number from 1 to ${AUDIT_LIMITS.MAX}`, 'INVALID_LIMIT');
  }
  let buPrincipal: string | undefined;
  if (query.buPrincipal) {
    if (query.buPrincipal === ANONYMOUS) {
      buPrincipal = ANONYMOUS;
    } else {
      const resolution = PrincipalResolver.resolve(query.buPrincipal);
      if (!resolution.ok) {
        throw new AuditError(resolution.reason, 'INVALID_FILTER');
      }
      buPrincipal = resolution.buPrincipal;
    }
  }
  if (query.action && !isAction(query.action)) {
    throw new AuditError(`Not an action: ${query.action}`, 'INVALID_FILTER');
  }
  return { limit, buPrincipal, action: query.action as Action | undefined };
};

const encodeCursor = (key: Record<string, any>): string => Buffer.from(JSON.stringify(key)).toString('base64url');

const decodeCursor = (cursor: string): Record<string, any> => {
  try {
    const key = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    if (key && typeof key === 'object' && typeof key.sk === 'string') {
      return key;
    }
  } catch {
    // Refused below
  }
  throw new AuditError('The cursor is not one this list gave', 'INVALID_CURSOR');
};

/**
 * A record's id: a sequence number of this process, then a random UUID. Records are sorted by time then id, so two
 * written in the same millisecond by one Lambda instance keep the order they were written in (a random id alone left it
 * to chance); records of two instances in the same millisecond were concurrent anyway.
 */
let sequence = 0;
const SEQUENCE_DIGITS = 8;
const newId = (): string => {
  sequence = (sequence + 1) % 36 ** SEQUENCE_DIGITS;
  return `${sequence.toString(36).padStart(SEQUENCE_DIGITS, '0')}-${randomUUID()}`;
};

const newRecord = (entry: AuditEntry, at: Date): AuditRecord => {
  const record: AuditRecord = { ...entry, id: newId(), at: at.toISOString() };
  // Leave out what isn't there, rather than keeping undefined attributes
  (Object.keys(record) as (keyof AuditRecord)[]).forEach(key => record[key] === undefined && delete record[key]);
  return record;
};

/** Offline runs and tests: kept in memory for the life of the process */
export class InMemoryAuditStore implements AuditStore {
  private readonly records: AuditRecord[] = [];

  constructor(private readonly now: () => Date = () => new Date()) {}

  async append(entry: AuditEntry, at: Date = this.now()): Promise<AuditRecord> {
    const record = newRecord(entry, at);
    this.records.push(record);
    return record;
  }

  async list(query: AuditQuery = {}): Promise<AuditPage> {
    const { limit, buPrincipal, action } = checkQuery(query);
    const after = query.cursor ? decodeCursor(query.cursor).sk : undefined;
    const matching = [...this.records]
      .sort((a, b) => sortKeyOf(b).localeCompare(sortKeyOf(a)))
      .filter(r => (!after || sortKeyOf(r) < after) && (!buPrincipal || r.buPrincipal === buPrincipal) && (!action || r.action === action));
    const items = matching.slice(0, limit);
    return {
      items,
      ...(matching.length > limit && { cursor: encodeCursor({ sk: sortKeyOf(items[items.length - 1]) }) })
    };
  }
}

/**
 * The landscape's audit table: partition key `pk` (always `AUDIT`), sort key `sk` (`<ISO time>#<id>`, so a Query
 * lists newest first), and the `principal-index` (partition key `buPrincipal`, sort key `sk`) for one person's.
 */
export class DynamoDbAuditStore implements AuditStore {
  private readonly client: DynamoDBDocumentClient;

  constructor(readonly tableName: string, client: DynamoDBClient | DynamoDBDocumentClient, private readonly now: () => Date = () => new Date()) {
    this.client = client instanceof DynamoDBDocumentClient ? client : DynamoDBDocumentClient.from(client);
  }

  static forLandscape(params: { stackId: string; landscape: string; region: string }): DynamoDbAuditStore {
    const { stackId, landscape, region } = params;
    return new DynamoDbAuditStore(auditTableName(stackId, landscape), new DynamoDBClient({ region }));
  }

  async append(entry: AuditEntry, at: Date = this.now()): Promise<AuditRecord> {
    const record = newRecord(entry, at);
    await this.client.send(new PutCommand({
      TableName: this.tableName,
      Item: { pk: AUDIT_PARTITION, sk: sortKeyOf(record), ...record },
      // A new id never collides; refuse rather than overwrite if it somehow did
      ConditionExpression: 'attribute_not_exists(sk)'
    }));
    return record;
  }

  async list(query: AuditQuery = {}): Promise<AuditPage> {
    const { limit, buPrincipal, action } = checkQuery(query);
    let ExclusiveStartKey: Record<string, any> | undefined;
    if (query.cursor) {
      const key = decodeCursor(query.cursor);
      // A cursor continues the list it came from: the same partition, and the same person's when filtered
      const fits = buPrincipal ? key.buPrincipal === buPrincipal && key.pk === AUDIT_PARTITION : key.pk === AUDIT_PARTITION && key.buPrincipal === undefined;
      if (!fits) {
        throw new AuditError('The cursor is not one this list gave', 'INVALID_CURSOR');
      }
      ExclusiveStartKey = key;
    }

    const items: AuditRecord[] = [];
    // Each Query evaluates at most the records still wanted, so the filter can only return fewer, never too many,
    // and LastEvaluatedKey is exactly where the next page starts
    do {
      const result = await this.client.send(new QueryCommand({
        TableName: this.tableName,
        ...(buPrincipal
          ? { IndexName: PRINCIPAL_INDEX, KeyConditionExpression: 'buPrincipal = :key', ExpressionAttributeValues: { ':key': buPrincipal, ...(action && { ':action': action }) } }
          : { KeyConditionExpression: 'pk = :key', ExpressionAttributeValues: { ':key': AUDIT_PARTITION, ...(action && { ':action': action }) } }),
        ...(action && { FilterExpression: '#action = :action', ExpressionAttributeNames: { '#action': 'action' } }),
        ScanIndexForward: false,
        Limit: limit - items.length,
        ExclusiveStartKey
      }));
      items.push(...(result.Items ?? []).map(DynamoDbAuditStore.recordOf));
      ExclusiveStartKey = result.LastEvaluatedKey;
    } while (ExclusiveStartKey && items.length < limit);

    return { items, ...(ExclusiveStartKey && { cursor: encodeCursor(ExclusiveStartKey) }) };
  }

  private static recordOf(item: Record<string, any>): AuditRecord {
    const { pk, sk, ...record } = item;
    return record as AuditRecord;
  }
}

/**
 * Harness: a landscape's audit trail, in its dashboard's audit table. Read only.
 *
 * TASK:
 *   list  newest first: LIMIT records (default 50, at most 100) after CURSOR, optionally only PRINCIPAL's
 *         (a buPrincipal, <id>@bu.edu or anonymous) or ACTION's (e.g. bulk:start)
 */
if (require.main === module) {
  const testEnvironment = TestEnvironment('DASHBOARD_AUDIT_TRAIL');
  [
    'TASK',
    'LANDSCAPE',
    'REGION',
    'STACK_ID',
    'PRINCIPAL',
    'ACTION',
    'LIMIT',
    'CURSOR'
  ].forEach(testEnvironment.getVar);

  const task = testEnvironment.getVar('TASK');
  const landscape = testEnvironment.getVar('LANDSCAPE');
  const region = testEnvironment.getVar('REGION');
  const stackId = testEnvironment.getVar('STACK_ID') || 'hp-dashboard';
  const limit = testEnvironment.getVar('LIMIT');

  (async () => {
    if (!landscape || !region) {
      throw new Error('DASHBOARD_AUDIT_TRAIL_LANDSCAPE and DASHBOARD_AUDIT_TRAIL_REGION are required');
    }
    const store = DynamoDbAuditStore.forLandscape({ stackId, landscape, region });
    switch (task) {
      case 'list': {
        const page = await store.list({
          limit: limit ? Number(limit) : undefined,
          cursor: testEnvironment.getVar('CURSOR') || undefined,
          buPrincipal: testEnvironment.getVar('PRINCIPAL') || undefined,
          action: testEnvironment.getVar('ACTION') || undefined
        });
        console.log(`Audit trail (${store.tableName}):`, JSON.stringify(page, null, 2));
        break;
      }
      default:
        throw new Error(`Unknown DASHBOARD_AUDIT_TRAIL_TASK: ${task} (list)`);
    }
  })().catch(error => {
    console.error(error);
    process.exit(1);
  });
}
