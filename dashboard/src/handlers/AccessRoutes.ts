import { ACTIONS } from '../authorization/Actions';
import { PrincipalResolver } from '../authorization/Principal';
import { RoleAssignmentStore } from '../authorization/RoleAssignmentStore';
import { pruneCoveredRoles, RoleCatalog } from '../authorization/RoleCatalog';

/** A route's answer, as JSON */
export type AccessResponse = { status: number; body: Record<string, unknown> };

/** The role that can't be left without a holder (authorization-plan.md, decision 11) */
export const ADMIN_ROLE = 'Admin';

/** The most people one bulk grant takes: each is a read and a write, within the Lambda's 30 seconds */
export const MAX_BULK_PRINCIPALS = 100;

/** A person a bulk grant left as they were, because they already held a role */
export type UnchangedPerson = { buPrincipal: string; roles: string[] };

const PERSON_PATH = /^\/api\/access\/assignments\/([^/]+)$/;

const refuse = (status: number, code: string, error: string, more: Record<string, unknown> = {}): AccessResponse =>
  ({ status, body: { error, code, ...more } });

/**
 * The Access page's routes (authorization-plan.md, decisions 3 and 11), behind the gate's access:admin:
 * - `GET /api/access/roles`: the catalog's roles, each with its policies and their actions;
 * - `GET /api/access/assignments`: everyone with a role, with their grants and profiles;
 * - `GET /api/access/assignments/{person}`: one person's grants and profile (a buPrincipal or <id>@bu.edu);
 * - `PUT /api/access/assignments/{person}` `{ roles }`: replace their roles (assign, retract, swap at once), less any
 *   role another of them covers (decision 15), answering what was dropped as `pruned`;
 * - `DELETE /api/access/assignments/{person}`: remove them all;
 * - `POST /api/access/assignments` `{ principals, roles }`: give the roles to each of several people (at most
 *   MAX_BULK_PRINCIPALS) who holds no role yet. Anyone who already holds a role is left as they are, and listed as
 *   `unchanged` with their roles, so a bulk grant never overwrites a role given one person at a time. Nothing is saved
 *   unless every principal is valid.
 * A change that would leave no Admin is refused 409 LAST_ADMIN. Refusals are `{ error, code }`.
 */
export class AccessRoutes {
  constructor(private readonly params: { store: RoleAssignmentStore; catalog: RoleCatalog }) {}

  async answer(request: { method: string; path: string; body: string | null; grantedBy: string }): Promise<AccessResponse> {
    const { method, path, body, grantedBy } = request;
    if (path === '/api/access/roles') {
      return { status: 200, body: { roles: this.roles() } };
    }
    if (path === '/api/access/assignments') {
      return method === 'POST'
        ? this.grantMany(body, grantedBy)
        : { status: 200, body: { assignments: await this.params.store.listAssignments() } };
    }
    const person = path.match(PERSON_PATH)?.[1];
    if (person === undefined) {
      return refuse(404, 'NOT_FOUND', `No such route: ${path}`);
    }
    let input: string;
    try {
      input = decodeURIComponent(person);
    } catch {
      return refuse(400, 'INVALID_PRINCIPAL', `Not a buPrincipal or eduPersonPrincipalName: ${person}`);
    }
    const resolution = PrincipalResolver.resolve(input);
    if (!resolution.ok) {
      return refuse(400, 'INVALID_PRINCIPAL', resolution.reason);
    }
    const { buPrincipal, from } = resolution;
    switch (method) {
      case 'GET':
        return { status: 200, body: await this.person(buPrincipal, from) };
      case 'PUT': {
        const parsed = this.parse(body, '{ "roles": [...] }');
        if ('status' in parsed) {
          return parsed;
        }
        const roles = this.rolesOf(parsed.json);
        return Array.isArray(roles) ? this.setRoles(buPrincipal, roles, grantedBy) : roles;
      }
      case 'DELETE':
        return this.setRoles(buPrincipal, [], grantedBy);
      default:
        return refuse(405, 'METHOD_NOT_ALLOWED', 'Method not allowed');
    }
  }

  /** The catalog's roles, policies and actions, for the Access page's checkboxes and the who-am-I dialog */
  roles(): { name: string; description: string; policies: { name: string; description: string; actions: string[] }[] }[] {
    return this.params.catalog.listRoles().map(role => ({
      name: role.name,
      description: role.description,
      policies: role.policies.map(policy => ({
        name: policy.name, description: policy.description, actions: ACTIONS.filter(action => policy.allows(action))
      }))
    }));
  }

  private async person(buPrincipal: string, from: 'buPrincipal' | 'eppn'): Promise<Record<string, unknown>> {
    const { store } = this.params;
    const [grants, profile] = await Promise.all([store.getGrants(buPrincipal), store.getProfile(buPrincipal)]);
    return { buPrincipal, from, grants, roles: grants.map(grant => grant.role), ...(profile && { profile }) };
  }

  /** The body as JSON, or the refusal */
  private parse(body: string | null, shape: string): { json: any } | AccessResponse {
    try {
      return { json: JSON.parse(body || '') };
    } catch {
      return refuse(400, 'INVALID_REQUEST', `The request body must be JSON: ${shape}`);
    }
  }

  /** The body's roles, checked against the catalog; or the refusal */
  private rolesOf(parsed: any): string[] | AccessResponse {
    const roles = parsed?.roles;
    if (!Array.isArray(roles) || roles.some(role => typeof role !== 'string')) {
      return refuse(400, 'INVALID_REQUEST', 'roles must be a list of role names (empty to remove them all)');
    }
    const unknown = roles.filter(role => !this.params.catalog.getRole(role));
    if (unknown.length) {
      const known = this.params.catalog.listRoles().map(role => role.name).join(', ');
      return refuse(400, 'INVALID_ROLE', `Unknown role(s): ${unknown.join(', ')} (known: ${known})`);
    }
    return roles;
  }

  /**
   * Give the roles (less the covered ones) to each person who holds none, in the order given, each once. Someone who
   * already holds a role keeps exactly what they hold: they are answered as `unchanged`, for the page to say so. It
   * only ever adds, so the last-Admin safeguard has nothing to guard.
   */
  private async grantMany(body: string | null, grantedBy: string): Promise<AccessResponse> {
    const parsed = this.parse(body, '{ "principals": [...], "roles": [...] }');
    if ('status' in parsed) {
      return parsed;
    }
    const chosen = this.rolesOf(parsed.json);
    if (!Array.isArray(chosen)) {
      return chosen;
    }
    if (chosen.length === 0) {
      return refuse(400, 'INVALID_REQUEST', 'Choose at least one role to give');
    }
    const inputs = parsed.json?.principals;
    if (!Array.isArray(inputs) || inputs.some(input => typeof input !== 'string')) {
      return refuse(400, 'INVALID_REQUEST', 'principals must be a list of buPrincipals or eduPersonPrincipalNames');
    }
    const principals: string[] = [];
    const invalid: { principal: string; reason: string }[] = [];
    inputs.map(input => input.trim()).filter(Boolean).forEach(input => {
      const resolution = PrincipalResolver.resolve(input);
      if (!resolution.ok) {
        invalid.push({ principal: input, reason: resolution.reason });
      } else if (!principals.includes(resolution.buPrincipal)) {
        principals.push(resolution.buPrincipal);
      }
    });
    if (invalid.length) {
      return refuse(400, 'INVALID_PRINCIPAL',
        `Nothing was saved. Not a buPrincipal or <id>@bu.edu: ${invalid.map(({ principal }) => principal).join(', ')}`, { invalid });
    }
    if (principals.length === 0) {
      return refuse(400, 'INVALID_REQUEST', 'Enter at least one person');
    }
    if (principals.length > MAX_BULK_PRINCIPALS) {
      return refuse(400, 'TOO_MANY_PRINCIPALS',
        `At most ${MAX_BULK_PRINCIPALS} people at a time (${principals.length} given): split the list and save each part.`);
    }
    const { store, catalog } = this.params;
    const { kept: roles, pruned } = pruneCoveredRoles(catalog, chosen);
    const granted: string[] = [];
    const unchanged: UnchangedPerson[] = [];
    for (const buPrincipal of principals) {
      const held = await store.getRoles(buPrincipal);
      if (held.length) {
        unchanged.push({ buPrincipal, roles: held });
      } else {
        await store.setRoles(buPrincipal, roles, grantedBy);
        granted.push(buPrincipal);
      }
    }
    return { status: 200, body: { roles, pruned, granted, unchanged } };
  }

  /**
   * Replace a person's roles, less the covered ones (decision 15), unless that leaves no Admin (decision 11, judged on
   * the roles that would be saved). Checked against the store as it is now: two Admins removing each other at the same
   * moment could both pass, which an Admin seeded with the harness puts right.
   */
  private async setRoles(buPrincipal: string, chosen: string[], grantedBy: string): Promise<AccessResponse> {
    const { store, catalog } = this.params;
    const { kept: roles, pruned } = pruneCoveredRoles(catalog, chosen);
    const current = await store.getRoles(buPrincipal);
    if (current.includes(ADMIN_ROLE) && !roles.includes(ADMIN_ROLE)) {
      const admins = await store.holdersOf(ADMIN_ROLE);
      if (admins.every(admin => admin === buPrincipal)) {
        return refuse(409, 'LAST_ADMIN', `${buPrincipal} is the landscape's last Admin: give someone else Admin first, so the landscape always has one.`);
      }
    }
    const change = await store.setRoles(buPrincipal, roles, grantedBy);
    return { status: 200, body: { buPrincipal, ...change, pruned, grants: await store.getGrants(buPrincipal) } };
  }
}
