import { ACTIONS } from '../authorization/Actions';
import { PrincipalResolver } from '../authorization/Principal';
import { RoleAssignmentStore } from '../authorization/RoleAssignmentStore';
import { pruneCoveredRoles, RoleCatalog } from '../authorization/RoleCatalog';

/** A route's answer, as JSON */
export type AccessResponse = { status: number; body: Record<string, unknown> };

/** The role that can't be left without a holder (authorization-plan.md, decision 11) */
export const ADMIN_ROLE = 'Admin';

const PERSON_PATH = /^\/api\/access\/assignments\/([^/]+)$/;

const refuse = (status: number, code: string, error: string): AccessResponse => ({ status, body: { error, code } });

/**
 * The Access page's routes (authorization-plan.md, decisions 3 and 11), behind the gate's access:admin:
 * - `GET /api/access/roles`: the catalog's roles, each with its policies and their actions;
 * - `GET /api/access/assignments`: everyone with a role, with their grants and profiles;
 * - `GET /api/access/assignments/{person}`: one person's grants and profile (a buPrincipal or <id>@bu.edu);
 * - `PUT /api/access/assignments/{person}` `{ roles }`: replace their roles (assign, retract, swap at once), less any
 *   role another of them covers (decision 15), answering what was dropped as `pruned`;
 * - `DELETE /api/access/assignments/{person}`: remove them all.
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
      return { status: 200, body: { assignments: await this.params.store.listAssignments() } };
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
        const roles = this.rolesOf(body);
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

  /** The body's roles, checked against the catalog; or the refusal */
  private rolesOf(body: string | null): string[] | AccessResponse {
    let parsed: any;
    try {
      parsed = JSON.parse(body || '');
    } catch {
      return refuse(400, 'INVALID_REQUEST', 'The request body must be JSON: { "roles": [...] }');
    }
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
