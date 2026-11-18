/**
 * The audit store (dashboard/src/audit/AuditStore.ts, authorization-plan.md decision 9): appending records, and
 * listing them newest first, by person or action, a page at a time. The in-memory store, and the DynamoDB store
 * against a stand-in audit table.
 */
import { DynamoDBDocumentClient, PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import {
  AUDIT_PARTITION, AuditEntry, AuditError, AuditStore, auditTableName, DynamoDbAuditStore, InMemoryAuditStore,
  PRINCIPAL_INDEX
} from '../dashboard/src/audit/AuditStore';

const documentClient = () => DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-east-2' }));
const TABLE = 'hp-dashboard-audit-mock';
const T0 = Date.parse('2026-11-17T12:00:00.000Z');
const at = (minute: number) => new Date(T0 + minute * 60_000);

const entry = (buPrincipal: string, action: AuditEntry['action'], extra: Partial<AuditEntry> = {}): AuditEntry => ({
  landscape: 'mock', buPrincipal, action, decision: 'ALLOWED', outcome: 'SUCCEEDED', ...extra
});

/**
 * A stand-in audit table that answers Query as DynamoDB does: on the table or the principal index, newest first,
 * evaluating at most Limit items before the filter, with LastEvaluatedKey when it stopped early
 */
const standInTable = () => {
  const dynamo = mockClient(DynamoDBDocumentClient);
  const items: Record<string, any>[] = [];
  dynamo.on(PutCommand).callsFake(input => {
    expect(input.TableName).toBe(TABLE);
    if (items.some(item => item.sk === input.Item.sk)) {
      throw Object.assign(new Error('exists'), { name: 'ConditionalCheckFailedException' });
    }
    items.push(input.Item);
    return {};
  });
  dynamo.on(QueryCommand).callsFake(input => {
    const byPrincipal = input.IndexName === PRINCIPAL_INDEX;
    const key = input.ExpressionAttributeValues[':key'];
    const ordered = items
      .filter(item => byPrincipal ? item.buPrincipal === key : item.pk === key)
      .sort((a, b) => input.ScanIndexForward === false ? b.sk.localeCompare(a.sk) : a.sk.localeCompare(b.sk));
    const start = input.ExclusiveStartKey ? ordered.findIndex(item => item.sk === input.ExclusiveStartKey.sk) + 1 : 0;
    const evaluated = ordered.slice(start, start + input.Limit);
    const last = evaluated[evaluated.length - 1];
    const more = start + input.Limit < ordered.length;
    return {
      Items: evaluated.filter(item => !input.FilterExpression || item.action === input.ExpressionAttributeValues[':action']),
      ...(more && { LastEvaluatedKey: { pk: last.pk, sk: last.sk, ...(byPrincipal && { buPrincipal: last.buPrincipal }) } })
    };
  });
  return { dynamo, items };
};

const behavesAsAStore = (makeStore: () => AuditStore) => {
  const fill = async (store: AuditStore) => {
    await store.append(entry('admin', 'access:admin', { target: 'jdoe', detail: 'Roles: Operator' }), at(0));
    await store.append(entry('jdoe', 'person:lookup', { target: 'U12345678' }), at(1));
    await store.append(entry('jdoe', 'bulk:start', { decision: 'DENIED', outcome: undefined, detail: 'None of your roles (Viewer)' }), at(2));
    await store.append(entry('admin', 'bulk:start', { viewedAs: 'jdoe' }), at(3));
    await store.append(entry('jdoe', 'person:sync', { outcome: 'FAILED' }), at(4));
  };

  it('appends a record with an id and its time, leaving out what is absent', async () => {
    const store = makeStore();
    const record = await store.append(entry('jdoe', 'bulk:start', { decision: 'DENIED', outcome: undefined }), at(0));
    expect(record.id).toMatch(/^[0-9a-z]{8}-[0-9a-f-]{36}$/);
    expect(record.at).toBe('2026-11-17T12:00:00.000Z');
    expect(record).not.toHaveProperty('outcome');
    expect((await store.list()).items).toEqual([record]);
  });

  it('keeps the order records were written in within one millisecond', async () => {
    const store = makeStore();
    for (const action of ['person:lookup', 'person:preview', 'person:sync', 'bulk:start', 'mock:reset'] as const) {
      await store.append(entry('jdoe', action), at(0));
    }
    expect((await store.list()).items.map(r => r.action)).toEqual(['mock:reset', 'bulk:start', 'person:sync', 'person:preview', 'person:lookup']);
  });

  it('lists newest first', async () => {
    const store = makeStore();
    await fill(store);
    const { items, cursor } = await store.list();
    expect(items.map(r => r.action)).toEqual(['person:sync', 'bulk:start', 'bulk:start', 'person:lookup', 'access:admin']);
    expect(items[1].viewedAs).toBe('jdoe');
    expect(cursor).toBeUndefined();
  });

  it('pages, each page continuing where the last stopped', async () => {
    const store = makeStore();
    await fill(store);
    const first = await store.list({ limit: 2 });
    const second = await store.list({ limit: 2, cursor: first.cursor });
    const third = await store.list({ limit: 2, cursor: second.cursor });
    expect([...first.items, ...second.items, ...third.items].map(r => r.at)).toEqual(
      [4, 3, 2, 1, 0].map(m => at(m).toISOString()));
    expect(third.cursor).toBeUndefined();
  });

  it("lists one person's records, by buPrincipal or eppn", async () => {
    const store = makeStore();
    await fill(store);
    expect((await store.list({ buPrincipal: 'jdoe' })).items.map(r => r.action)).toEqual(['person:sync', 'bulk:start', 'person:lookup']);
    expect((await store.list({ buPrincipal: 'JDoe@bu.edu', limit: 1 })).items.map(r => r.action)).toEqual(['person:sync']);
  });

  it("lists one action's records, filling the page", async () => {
    const store = makeStore();
    await fill(store);
    const first = await store.list({ action: 'bulk:start', limit: 1 });
    expect(first.items.map(r => r.buPrincipal)).toEqual(['admin']);
    const second = await store.list({ action: 'bulk:start', limit: 1, cursor: first.cursor });
    expect(second.items.map(r => r.buPrincipal)).toEqual(['jdoe']);
    expect((await store.list({ action: 'bulk:start', buPrincipal: 'jdoe' })).items.length).toBe(1);
  });

  it('refuses a bad limit, filter or cursor', async () => {
    const store = makeStore();
    for (const limit of [0, 101, 1.5]) {
      await expect(store.list({ limit })).rejects.toMatchObject({ code: 'INVALID_LIMIT' });
    }
    await expect(store.list({ action: 'bulk:stop' })).rejects.toMatchObject({ code: 'INVALID_FILTER' });
    await expect(store.list({ buPrincipal: 'x@gmail.com' })).rejects.toMatchObject({ code: 'INVALID_FILTER' });
    await expect(store.list({ cursor: 'not-a-cursor' })).rejects.toBeInstanceOf(AuditError);
  });

  it('lists the anonymous records of a public dashboard', async () => {
    const store = makeStore();
    await store.append(entry('anonymous', 'person:sync'), at(0));
    expect((await store.list({ buPrincipal: 'anonymous' })).items.length).toBe(1);
  });
};

describe('InMemoryAuditStore', () => {
  behavesAsAStore(() => new InMemoryAuditStore());
});

describe('DynamoDbAuditStore', () => {
  let table: ReturnType<typeof standInTable>;
  beforeEach(() => { table = standInTable(); });
  afterEach(() => table.dynamo.restore());

  behavesAsAStore(() => new DynamoDbAuditStore(TABLE, documentClient()));

  it('keeps every record in one partition, sorted by time then id, never overwriting', async () => {
    const store = new DynamoDbAuditStore(TABLE, documentClient());
    const record = await store.append(entry('jdoe', 'person:sync'), at(0));
    expect(table.items[0]).toEqual({ pk: AUDIT_PARTITION, sk: `${record.at}#${record.id}`, ...record });
    expect(table.dynamo.commandCalls(PutCommand)[0].args[0].input.ConditionExpression).toBe('attribute_not_exists(sk)');
  });

  it("refuses a cursor from another list: the whole trail's for one person's, or another person's", async () => {
    const store = new DynamoDbAuditStore(TABLE, documentClient());
    for (let m = 0; m < 3; m++) {
      await store.append(entry(m % 2 ? 'amy' : 'jdoe', 'person:sync'), at(m));
    }
    const whole = await store.list({ limit: 1 });
    const jdoes = await store.list({ limit: 1, buPrincipal: 'jdoe' });
    await expect(store.list({ cursor: whole.cursor, buPrincipal: 'jdoe' })).rejects.toMatchObject({ code: 'INVALID_CURSOR' });
    await expect(store.list({ cursor: jdoes.cursor })).rejects.toMatchObject({ code: 'INVALID_CURSOR' });
    await expect(store.list({ cursor: jdoes.cursor, buPrincipal: 'amy' })).rejects.toMatchObject({ code: 'INVALID_CURSOR' });
    expect((await store.list({ cursor: jdoes.cursor, buPrincipal: 'jdoe' })).items.length).toBe(1);
  });

  it('is named per landscape', () => {
    expect(auditTableName('hp-dashboard', 'Staging')).toBe('hp-dashboard-audit-staging');
    expect(DynamoDbAuditStore.forLandscape({ stackId: 'hp-dashboard', landscape: 'mock', region: 'us-east-2' }).tableName).toBe(TABLE);
  });
});
