/**
 * The role assignment store (dashboard/src/authorization/RoleAssignmentStore.ts, authorization-plan.md decisions 3,
 * 4 and 6): the in-memory store, and the DynamoDB store against a stand-in access table.
 */
import { DynamoDBDocumentClient, PutCommand, QueryCommand, ScanCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { Authorizer } from '../dashboard/src/authorization/Authorizer';
import { Principal } from '../dashboard/src/authorization/Principal';
import {
  accessTableName, DynamoDbRoleAssignmentStore, InMemoryRoleAssignmentStore, PROFILE_KEY, RoleAssignmentStore
} from '../dashboard/src/authorization/RoleAssignmentStore';
import { StaticRoleCatalog } from '../dashboard/src/authorization/RoleCatalog';

const T0 = new Date('2026-11-17T12:00:00.000Z');
const documentClient = () => DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-east-2' }));
const TABLE = 'hp-dashboard-access-mock';

/** A stand-in access table: items keyed by buPrincipal and sk, answering the commands the store sends */
const standInTable = () => {
  const dynamo = mockClient(DynamoDBDocumentClient);
  const items = new Map<string, Record<string, any>>();
  const keyOf = (item: Record<string, any>) => `${item.buPrincipal}|${item.sk}`;
  const sorted = () => [...items.values()].sort((a, b) => keyOf(a).localeCompare(keyOf(b)));
  dynamo.on(QueryCommand).callsFake(input => {
    expect(input.TableName).toBe(TABLE);
    return { Items: sorted().filter(item => item.buPrincipal === input.ExpressionAttributeValues[':p']) };
  });
  // Two pages, to show the store follows LastEvaluatedKey
  dynamo.on(ScanCommand).callsFake(input => {
    const all = sorted();
    const start = input.ExclusiveStartKey ? all.findIndex(item => keyOf(item) === keyOf(input.ExclusiveStartKey)) + 1 : 0;
    const page = all.slice(start, start + 2);
    return { Items: page, ...(start + 2 < all.length && { LastEvaluatedKey: { buPrincipal: page[1].buPrincipal, sk: page[1].sk } }) };
  });
  dynamo.on(PutCommand).callsFake(input => { items.set(keyOf(input.Item), input.Item); return {}; });
  dynamo.on(TransactWriteCommand).callsFake(input => {
    for (const write of input.TransactItems) {
      write.Put ? items.set(keyOf(write.Put.Item), write.Put.Item) : items.delete(keyOf(write.Delete.Key));
    }
    return {};
  });
  return { dynamo, items };
};

const principal = (claims: object) => Principal.fromClaims(claims);
const jdoe = principal({ buPrincipal: ['jdoe'], eduPersonPrincipalName: ['jdoe@bu.edu'], mail: ['jdoe@bu.edu'], givenName: ['Jane'], sn: ['Doe'] });

/** What both stores must do alike */
const behavesAsAStore = (makeStore: () => { store: RoleAssignmentStore; now: { value: Date } }) => {
  it('has no roles for someone never assigned one', async () => {
    const { store } = makeStore();
    expect(await store.getRoles('jdoe')).toEqual([]);
    expect(await store.getGrants('jdoe')).toEqual([]);
    expect(await store.listAssignments()).toEqual([]);
  });

  it('replaces a person\'s roles, keeping the original grant of a role they keep', async () => {
    const { store, now } = makeStore();
    expect(await store.setRoles('jdoe', ['Viewer', 'Operator', 'Viewer'], 'admin1')).toEqual(
      { added: ['Operator', 'Viewer'], removed: [], roles: ['Operator', 'Viewer'] });
    now.value = new Date(T0.getTime() + 60_000);
    expect(await store.setRoles('jdoe', ['Admin', 'Operator'], 'admin2')).toEqual(
      { added: ['Admin'], removed: ['Viewer'], roles: ['Admin', 'Operator'] });
    expect(await store.getRoles('jdoe')).toEqual(['Admin', 'Operator']);
    expect(await store.getGrants('jdoe')).toEqual([
      { role: 'Admin', grantedBy: 'admin2', grantedAt: '2026-11-17T12:01:00.000Z' },
      { role: 'Operator', grantedBy: 'admin1', grantedAt: '2026-11-17T12:00:00.000Z' }
    ]);
  });

  it('changes nothing when the roles are the same', async () => {
    const { store } = makeStore();
    await store.setRoles('jdoe', ['Viewer'], 'admin1');
    expect(await store.setRoles('jdoe', ['Viewer'], 'admin2')).toEqual({ added: [], removed: [], roles: ['Viewer'] });
    expect((await store.getGrants('jdoe'))[0].grantedBy).toBe('admin1');
  });

  it('removes every role with an empty set, and drops the person from the list', async () => {
    const { store } = makeStore();
    await store.setRoles('jdoe', ['Viewer'], 'admin1');
    expect(await store.setRoles('jdoe', [], 'admin1')).toEqual({ added: [], removed: ['Viewer'], roles: [] });
    expect(await store.listAssignments()).toEqual([]);
  });

  it('lists everyone with a role, sorted, with the profiles of those who signed in', async () => {
    const { store } = makeStore();
    await store.setRoles('zed', ['Viewer'], 'admin1');
    await store.setRoles('jdoe', ['Admin'], 'admin1');
    await store.setRoles('amy', ['Operator'], 'admin1');
    await store.recordProfile(jdoe);
    await store.recordProfile(Principal.of('nobody'));
    const list = await store.listAssignments();
    expect(list.map(a => a.buPrincipal)).toEqual(['amy', 'jdoe', 'zed']);
    expect(list[1]).toEqual({
      buPrincipal: 'jdoe',
      grants: [{ role: 'Admin', grantedBy: 'admin1', grantedAt: T0.toISOString() }],
      profile: { eppn: 'jdoe@bu.edu', mail: 'jdoe@bu.edu', displayName: 'Jane Doe', lastSeen: T0.toISOString() }
    });
    expect(list[0].profile).toBeUndefined();
  });

  it('says who holds a role', async () => {
    const { store } = makeStore();
    await store.setRoles('b', ['Admin', 'Viewer'], 'x');
    await store.setRoles('a', ['Admin'], 'x');
    await store.setRoles('c', ['Viewer'], 'x');
    expect(await store.holdersOf('Admin')).toEqual(['a', 'b']);
    expect(await store.holdersOf('Operator')).toEqual([]);
  });

  it('keeps the last profile seen, apart from the roles', async () => {
    const { store, now } = makeStore();
    await store.recordProfile(jdoe);
    now.value = new Date(T0.getTime() + 3_600_000);
    await store.recordProfile(principal({ buPrincipal: ['jdoe'], givenName: ['Janet'] }));
    expect(await store.getProfile('jdoe')).toEqual({ displayName: 'Janet', lastSeen: '2026-11-17T13:00:00.000Z' });
    expect(await store.getRoles('jdoe')).toEqual([]);
    expect(await store.getProfile('someone')).toBeUndefined();
  });

  it('records nothing for an anonymous principal', async () => {
    const { store } = makeStore();
    await store.recordProfile(Principal.anonymous());
    expect(await store.listAssignments()).toEqual([]);
  });

  it('serves the Authorizer', async () => {
    const { store } = makeStore();
    await store.setRoles('jdoe', ['Operator'], 'admin1');
    const decision = await new Authorizer(Principal.of('jdoe'), store, new StaticRoleCatalog()).authorize('bulk:start');
    expect(decision.allowed).toBe(true);
  });
};

describe('InMemoryRoleAssignmentStore', () => {
  behavesAsAStore(() => {
    const now = { value: T0 };
    return { store: new InMemoryRoleAssignmentStore({}, () => now.value), now };
  });

  it('is seeded, as the local launch seeds its user', async () => {
    const store = new InMemoryRoleAssignmentStore({ wrh: ['Admin', 'Viewer'] }, () => T0);
    expect(await store.getRoles('wrh')).toEqual(['Admin', 'Viewer']);
    expect((await store.getGrants('wrh'))[0].grantedBy).toBe('seed');
  });
});

describe('DynamoDbRoleAssignmentStore', () => {
  let table: ReturnType<typeof standInTable>;
  beforeEach(() => { table = standInTable(); });
  afterEach(() => table.dynamo.restore());

  behavesAsAStore(() => {
    const now = { value: T0 };
    return { store: new DynamoDbRoleAssignmentStore(TABLE, documentClient(), () => now.value), now };
  });

  it('keeps one item per role and one profile item per person', async () => {
    const store = new DynamoDbRoleAssignmentStore(TABLE, documentClient(), () => T0);
    await store.setRoles('jdoe', ['Viewer', 'Admin'], 'admin1');
    await store.recordProfile(jdoe);
    expect([...table.items.values()]).toEqual(expect.arrayContaining([
      { buPrincipal: 'jdoe', sk: 'Viewer', grantedBy: 'admin1', grantedAt: T0.toISOString() },
      { buPrincipal: 'jdoe', sk: 'Admin', grantedBy: 'admin1', grantedAt: T0.toISOString() },
      { buPrincipal: 'jdoe', sk: PROFILE_KEY, eppn: 'jdoe@bu.edu', mail: 'jdoe@bu.edu', displayName: 'Jane Doe', lastSeen: T0.toISOString() }
    ]));
    expect(table.items.size).toBe(3);
  });

  it('makes a change in one transaction, and sends none when nothing changes', async () => {
    const store = new DynamoDbRoleAssignmentStore(TABLE, documentClient(), () => T0);
    await store.setRoles('jdoe', ['Viewer'], 'admin1');
    await store.setRoles('jdoe', ['Operator'], 'admin1');
    await store.setRoles('jdoe', ['Operator'], 'admin1');
    const transactions = table.dynamo.commandCalls(TransactWriteCommand);
    expect(transactions.length).toBe(2);
    expect(transactions[1].args[0].input.TransactItems).toEqual([
      { Delete: { TableName: TABLE, Key: { buPrincipal: 'jdoe', sk: 'Viewer' } } },
      { Put: { TableName: TABLE, Item: { buPrincipal: 'jdoe', sk: 'Operator', grantedBy: 'admin1', grantedAt: T0.toISOString() } } }
    ]);
  });

  it('refuses the profile key as a role', async () => {
    const store = new DynamoDbRoleAssignmentStore(TABLE, documentClient());
    await expect(store.setRoles('jdoe', [PROFILE_KEY], 'admin1')).rejects.toThrow(/not a role/);
  });

  it('reads consistently, so a change shows at once', async () => {
    const store = new DynamoDbRoleAssignmentStore(TABLE, documentClient());
    await store.getRoles('jdoe');
    expect(table.dynamo.commandCalls(QueryCommand)[0].args[0].input.ConsistentRead).toBe(true);
  });

  it('is named per landscape', () => {
    expect(accessTableName('hp-dashboard', 'Staging')).toBe('hp-dashboard-access-staging');
    expect(DynamoDbRoleAssignmentStore.forLandscape({ stackId: 'hp-dashboard', landscape: 'mock', region: 'us-east-2' }).tableName).toBe(TABLE);
  });
});
