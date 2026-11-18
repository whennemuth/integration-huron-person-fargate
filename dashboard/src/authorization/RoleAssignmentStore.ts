import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, QueryCommand, ScanCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { TestEnvironment } from 'integration-core';
import { RoleAssignments } from './Authorizer';
import { Principal, PrincipalResolver } from './Principal';
import { StaticRoleCatalog } from './RoleCatalog';

/** One role a person holds, who granted it and when */
export type RoleGrant = { role: string; grantedBy: string; grantedAt: string };

/** What the dashboard last saw of a person when they signed in, so the Access page can show names */
export type Profile = { eppn?: string; mail?: string; displayName?: string; lastSeen: string };

/** One person's assignments, for the Access page */
export type Assignment = { buPrincipal: string; grants: RoleGrant[]; profile?: Profile };

/** What a change of a person's roles did */
export type RoleChange = { added: string[]; removed: string[]; roles: string[] };

/**
 * Who holds which roles in this landscape (authorization-plan.md, decisions 3 and 6), and what was last seen of
 * them. Role names are kept as given: which of them the catalog knows is the Authorizer's concern.
 */
export interface RoleAssignmentStore extends RoleAssignments {
  /** The person's role names, sorted; none for someone never assigned a role */
  getRoles(buPrincipal: string): Promise<string[]>;
  /** The person's grants, with who granted each and when */
  getGrants(buPrincipal: string): Promise<RoleGrant[]>;
  /**
   * Replace the person's roles with these (assign, retract and swap in one action). A role they already hold keeps
   * its original grant; an empty set removes them all.
   */
  setRoles(buPrincipal: string, roles: string[], grantedBy: string): Promise<RoleChange>;
  /** Everyone with a role, sorted by buPrincipal, with their profiles. Human-scale, so one list, not pages. */
  listAssignments(): Promise<Assignment[]>;
  /** Who holds the role (the last-Admin safeguard, decision 11) */
  holdersOf(role: string): Promise<string[]>;
  /** The person's profile, if they have signed in since the table was created */
  getProfile(buPrincipal: string): Promise<Profile | undefined>;
  /** Record what the sign-in token says of the person, as last seen now */
  recordProfile(principal: Principal, at?: Date): Promise<void>;
}

/** The access table's name: one per landscape, owned by its dashboard stack (dashboard/lib/AccessTables.ts) */
export const accessTableName = (stackId: string, landscape: string): string =>
  `${stackId}-access-${landscape.toLowerCase()}`;

/** The sort key of a person's profile item; role items are sorted by their role name */
export const PROFILE_KEY = '#PROFILE';

const roleChange = (current: string[], wanted: string[]): RoleChange => {
  const roles = [...new Set(wanted)].sort();
  return {
    added: roles.filter(role => !current.includes(role)),
    removed: current.filter(role => !roles.includes(role)),
    roles
  };
};

const profileOf = (principal: Principal, at: Date): Profile => ({
  ...(principal.eppn && { eppn: principal.eppn }),
  ...(principal.mail && { mail: principal.mail }),
  ...(principal.displayName && { displayName: principal.displayName }),
  lastSeen: at.toISOString()
});

const requireKey = (buPrincipal: string): string => {
  if (!buPrincipal) {
    throw new Error('A buPrincipal is required');
  }
  return buPrincipal;
};

/** Offline runs and tests: kept in memory for the life of the process */
export class InMemoryRoleAssignmentStore implements RoleAssignmentStore {
  private readonly grants = new Map<string, Map<string, RoleGrant>>();
  private readonly profiles = new Map<string, Profile>();

  /** Seeded with some people's roles, granted by `seed` (e.g. the local launch's USER_ROLES) */
  constructor(seed: { [buPrincipal: string]: string[] } = {}, private readonly now: () => Date = () => new Date()) {
    for (const [buPrincipal, roles] of Object.entries(seed)) {
      const grantedAt = this.now().toISOString();
      this.grants.set(buPrincipal, new Map(roles.map(role => [role, { role, grantedBy: 'seed', grantedAt }])));
    }
  }

  async getRoles(buPrincipal: string): Promise<string[]> {
    return (await this.getGrants(buPrincipal)).map(grant => grant.role);
  }

  async getGrants(buPrincipal: string): Promise<RoleGrant[]> {
    return [...(this.grants.get(buPrincipal)?.values() ?? [])].sort((a, b) => a.role.localeCompare(b.role));
  }

  async setRoles(buPrincipal: string, roles: string[], grantedBy: string): Promise<RoleChange> {
    const current = this.grants.get(requireKey(buPrincipal)) ?? new Map<string, RoleGrant>();
    const change = roleChange([...current.keys()].sort(), roles);
    const grantedAt = this.now().toISOString();
    change.removed.forEach(role => current.delete(role));
    change.added.forEach(role => current.set(role, { role, grantedBy, grantedAt }));
    current.size ? this.grants.set(buPrincipal, current) : this.grants.delete(buPrincipal);
    return change;
  }

  async listAssignments(): Promise<Assignment[]> {
    const people = [...this.grants.keys()].sort();
    return Promise.all(people.map(async buPrincipal => ({
      buPrincipal, grants: await this.getGrants(buPrincipal),
      ...(this.profiles.has(buPrincipal) && { profile: this.profiles.get(buPrincipal) })
    })));
  }

  async holdersOf(role: string): Promise<string[]> {
    return [...this.grants.entries()].filter(([, grants]) => grants.has(role)).map(([buPrincipal]) => buPrincipal).sort();
  }

  async getProfile(buPrincipal: string): Promise<Profile | undefined> {
    return this.profiles.get(buPrincipal);
  }

  async recordProfile(principal: Principal, at: Date = this.now()): Promise<void> {
    if (!principal.isAnonymous) {
      this.profiles.set(principal.buPrincipal!, profileOf(principal, at));
    }
  }
}

/**
 * The landscape's access table: partition key `buPrincipal`, sort key `sk`, one item per role held (the role's name,
 * with `grantedBy` and `grantedAt`) and one `#PROFILE` item per person seen.
 */
export class DynamoDbRoleAssignmentStore implements RoleAssignmentStore {
  private readonly client: DynamoDBDocumentClient;

  constructor(readonly tableName: string, client: DynamoDBClient | DynamoDBDocumentClient, private readonly now: () => Date = () => new Date()) {
    this.client = client instanceof DynamoDBDocumentClient ? client : DynamoDBDocumentClient.from(client);
  }

  static forLandscape(params: { stackId: string; landscape: string; region: string }): DynamoDbRoleAssignmentStore {
    const { stackId, landscape, region } = params;
    return new DynamoDbRoleAssignmentStore(accessTableName(stackId, landscape), new DynamoDBClient({ region }));
  }

  /** Every item of the person: their grants and their profile */
  private async itemsOf(buPrincipal: string): Promise<Record<string, any>[]> {
    const items: Record<string, any>[] = [];
    let ExclusiveStartKey: Record<string, any> | undefined;
    do {
      const result = await this.client.send(new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: 'buPrincipal = :p',
        ExpressionAttributeValues: { ':p': requireKey(buPrincipal) },
        ConsistentRead: true,
        ExclusiveStartKey
      }));
      items.push(...(result.Items ?? []));
      ExclusiveStartKey = result.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return items;
  }

  private static grantOf(item: Record<string, any>): RoleGrant {
    return { role: item.sk, grantedBy: item.grantedBy, grantedAt: item.grantedAt };
  }

  private static profileFrom(item: Record<string, any>): Profile {
    const { eppn, mail, displayName, lastSeen } = item;
    return { ...(eppn && { eppn }), ...(mail && { mail }), ...(displayName && { displayName }), lastSeen };
  }

  async getRoles(buPrincipal: string): Promise<string[]> {
    return (await this.getGrants(buPrincipal)).map(grant => grant.role);
  }

  async getGrants(buPrincipal: string): Promise<RoleGrant[]> {
    return (await this.itemsOf(buPrincipal))
      .filter(item => item.sk !== PROFILE_KEY)
      .map(DynamoDbRoleAssignmentStore.grantOf)
      .sort((a, b) => a.role.localeCompare(b.role));
  }

  async setRoles(buPrincipal: string, roles: string[], grantedBy: string): Promise<RoleChange> {
    if (roles.includes(PROFILE_KEY)) {
      throw new Error(`${PROFILE_KEY} is not a role`);
    }
    const change = roleChange(await this.getRoles(buPrincipal), roles);
    const grantedAt = this.now().toISOString();
    const writes = [
      ...change.removed.map(role => ({ Delete: { TableName: this.tableName, Key: { buPrincipal, sk: role } } })),
      ...change.added.map(role => ({
        Put: { TableName: this.tableName, Item: { buPrincipal, sk: role, grantedBy, grantedAt } }
      }))
    ];
    // One transaction, so a change is never half made (a person holds a handful of roles at most)
    if (writes.length) {
      await this.client.send(new TransactWriteCommand({ TransactItems: writes }));
    }
    return change;
  }

  /** Every item in the table: human-scale (people with roles, and people who signed in) */
  private async scanAll(): Promise<Record<string, any>[]> {
    const items: Record<string, any>[] = [];
    let ExclusiveStartKey: Record<string, any> | undefined;
    do {
      const result = await this.client.send(new ScanCommand({ TableName: this.tableName, ExclusiveStartKey }));
      items.push(...(result.Items ?? []));
      ExclusiveStartKey = result.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return items;
  }

  async listAssignments(): Promise<Assignment[]> {
    const people = new Map<string, { grants: RoleGrant[]; profile?: Profile }>();
    const entry = (buPrincipal: string) => people.get(buPrincipal) ?? people.set(buPrincipal, { grants: [] }).get(buPrincipal)!;
    for (const item of await this.scanAll()) {
      if (item.sk === PROFILE_KEY) {
        entry(item.buPrincipal).profile = DynamoDbRoleAssignmentStore.profileFrom(item);
      } else {
        entry(item.buPrincipal).grants.push(DynamoDbRoleAssignmentStore.grantOf(item));
      }
    }
    return [...people.entries()]
      .filter(([, { grants }]) => grants.length)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([buPrincipal, { grants, profile }]) => ({
        buPrincipal, grants: grants.sort((a, b) => a.role.localeCompare(b.role)), ...(profile && { profile })
      }));
  }

  async holdersOf(role: string): Promise<string[]> {
    return (await this.listAssignments())
      .filter(({ grants }) => grants.some(grant => grant.role === role))
      .map(({ buPrincipal }) => buPrincipal);
  }

  async getProfile(buPrincipal: string): Promise<Profile | undefined> {
    const item = (await this.itemsOf(buPrincipal)).find(i => i.sk === PROFILE_KEY);
    return item ? DynamoDbRoleAssignmentStore.profileFrom(item) : undefined;
  }

  async recordProfile(principal: Principal, at: Date = this.now()): Promise<void> {
    if (principal.isAnonymous) {
      return;
    }
    await this.client.send(new PutCommand({
      TableName: this.tableName,
      Item: { buPrincipal: principal.buPrincipal, sk: PROFILE_KEY, ...profileOf(principal, at) }
    }));
  }
}

/**
 * Harness: a landscape's role assignments, in its dashboard's access table. Also how the first Admin is seeded after
 * a landscape's dashboard is deployed (authorization-plan.md, decision 4; the fargate README's "Dashboard access").
 *
 * TASK:
 *   list    everyone with a role, with their profiles
 *   get     PRINCIPAL's roles and profile
 *   set     replace PRINCIPAL's roles with ROLES (comma-delimited; empty removes them all)
 *   grant   add ROLES to PRINCIPAL's roles
 *   revoke  remove ROLES from PRINCIPAL's roles
 *
 * PRINCIPAL is a buPrincipal or <id>@bu.edu. ROLES must be roles the catalog knows (Viewer, Operator, Admin).
 * GRANTED_BY (default: "harness") is recorded with each role added.
 */
if (require.main === module) {
  const testEnvironment = TestEnvironment('DASHBOARD_ROLE_ASSIGNMENTS');
  [
    'TASK',
    'LANDSCAPE',
    'REGION',
    'STACK_ID',
    'PRINCIPAL',
    'ROLES',
    'GRANTED_BY'
  ].forEach(testEnvironment.getVar);

  const task = testEnvironment.getVar('TASK');
  const landscape = testEnvironment.getVar('LANDSCAPE');
  const region = testEnvironment.getVar('REGION');
  const stackId = testEnvironment.getVar('STACK_ID') || 'hp-dashboard';
  const grantedBy = testEnvironment.getVar('GRANTED_BY') || 'harness';

  const principal = (): string => {
    const resolution = PrincipalResolver.resolve(testEnvironment.getVar('PRINCIPAL'));
    if (!resolution.ok) {
      throw new Error(`DASHBOARD_ROLE_ASSIGNMENTS_PRINCIPAL: ${resolution.reason}`);
    }
    return resolution.buPrincipal;
  };
  const roles = (): string[] => {
    const catalog = new StaticRoleCatalog();
    const names = (testEnvironment.getVar('ROLES') || '').split(',').map(r => r.trim()).filter(Boolean);
    const unknown = names.filter(name => !catalog.getRole(name));
    if (unknown.length) {
      throw new Error(`Unknown role(s): ${unknown.join(', ')} (known: ${catalog.listRoles().map(r => r.name).join(', ')})`);
    }
    return names;
  };

  (async () => {
    if (!landscape || !region) {
      throw new Error('DASHBOARD_ROLE_ASSIGNMENTS_LANDSCAPE and DASHBOARD_ROLE_ASSIGNMENTS_REGION are required');
    }
    const store = DynamoDbRoleAssignmentStore.forLandscape({ stackId, landscape, region });
    const show = (label: string, result: any) => console.log(`${label} (${store.tableName}):`, JSON.stringify(result, null, 2));
    switch (task) {
      case 'list':
        show('Assignments', await store.listAssignments());
        break;
      case 'get': {
        const who = principal();
        show(who, { grants: await store.getGrants(who), profile: await store.getProfile(who) ?? 'never signed in' });
        break;
      }
      case 'set': {
        const who = principal();
        show(`Set ${who}`, await store.setRoles(who, roles(), grantedBy));
        break;
      }
      case 'grant': {
        const who = principal();
        show(`Granted ${who}`, await store.setRoles(who, [...await store.getRoles(who), ...roles()], grantedBy));
        break;
      }
      case 'revoke': {
        const who = principal();
        const revoked = roles();
        show(`Revoked ${who}`, await store.setRoles(who, (await store.getRoles(who)).filter(r => !revoked.includes(r)), grantedBy));
        break;
      }
      default:
        throw new Error(`Unknown DASHBOARD_ROLE_ASSIGNMENTS_TASK: ${task} (list | get | set | grant | revoke)`);
    }
  })().catch(error => {
    console.error(error);
    process.exit(1);
  });
}
