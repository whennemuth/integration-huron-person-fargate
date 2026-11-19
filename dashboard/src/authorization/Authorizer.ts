import { Action, ACTIONS } from './Actions';
import { Principal } from './Principal';
import { Role } from './Role';
import { RoleCatalog } from './RoleCatalog';

/**
 * Where a person's role names come from (authorization-plan.md, decision 6: per landscape). The role assignment
 * store (step 2) implements it; tests and offline runs can give a map.
 */
export interface RoleAssignments {
  getRoles(buPrincipal: string): Promise<string[]>;
}

/** The answer to one authorization question, enough for a 403, an audit record and the who-am-I dialog */
export type AuthorizationDecision = {
  allowed: boolean;
  action: Action;
  /** The roles that were considered: the principal's, as known to the catalog */
  roles: string[];
  /** The role and policy that granted it, when allowed (none in public mode) */
  role?: string;
  policy?: string;
  /** Why: names the granting role and policy, or what is missing */
  reason: string;
};

/** What a principal may do: their roles, and every action with the role and policy granting it (for `/api/me`) */
export type EffectivePermissions = {
  buPrincipal?: string;
  roles: { name: string; description: string; policies: { name: string; description: string; actions: Action[] }[] }[];
  actions: Action[];
  /** Assigned role names the catalog doesn't know (ignored for authorization) */
  unknownRoles: string[];
  publicMode: boolean;
};

/** Decides one request's actions for one principal; deny by default (decision 7) */
export interface RequestAuthorizer {
  authorize(action: Action): Promise<AuthorizationDecision>;
  effectivePermissions(): Promise<EffectivePermissions>;
}

/** The message a denial carries, e.g. "None of your roles (Viewer) has a policy that grants bulk:start." */
export const denialMessage = (action: Action, roles: string[]): string => roles.length
  ? `None of your roles (${roles.join(', ')}) has a policy that grants ${action}.`
  : `You have no role in this landscape, so you can't ${action}. Ask an Admin for a role.`;

/**
 * The authorizer of a signed-in (or anonymous) principal. Its roles are read once, at the first question, and kept
 * for the request: a request is decided against one consistent set of roles.
 */
export class Authorizer implements RequestAuthorizer {
  private resolved?: Promise<{ roles: Role[]; unknown: string[] }>;

  constructor(
    readonly principal: Principal,
    private readonly assignments: RoleAssignments,
    private readonly catalog: RoleCatalog
  ) {}

  private roles(): Promise<{ roles: Role[]; unknown: string[] }> {
    if (!this.resolved) {
      this.resolved = this.loadRoles();
    }
    return this.resolved;
  }

  private async loadRoles(): Promise<{ roles: Role[]; unknown: string[] }> {
    if (this.principal.isAnonymous) {
      return { roles: [], unknown: [] };
    }
    const names = [...new Set(await this.assignments.getRoles(this.principal.buPrincipal!))];
    const roles: Role[] = [];
    const unknown: string[] = [];
    for (const name of names) {
      const role = this.catalog.getRole(name);
      role ? roles.push(role) : unknown.push(name);
    }
    if (unknown.length) {
      console.warn(`Ignoring roles unknown to the catalog for ${this.principal.buPrincipal}: ${unknown.join(', ')}`);
    }
    // In the catalog's order, so messages and the who-am-I dialog list them the same way every time
    const order = this.catalog.listRoles().map(role => role.name);
    roles.sort((a, b) => order.indexOf(a.name) - order.indexOf(b.name));
    return { roles, unknown };
  }

  async authorize(action: Action): Promise<AuthorizationDecision> {
    const { roles } = await this.roles();
    const roleNames = roles.map(role => role.name);
    if (this.principal.isAnonymous) {
      return { allowed: false, action, roles: [], reason: `Sign in to ${action}.` };
    }
    for (const role of roles) {
      const policy = role.allows(action);
      if (policy) {
        return {
          allowed: true, action, roles: roleNames, role: role.name, policy: policy.name,
          reason: `${role.name} grants ${action} through ${policy.name}.`
        };
      }
    }
    return { allowed: false, action, roles: roleNames, reason: denialMessage(action, roleNames) };
  }

  async effectivePermissions(): Promise<EffectivePermissions> {
    const { roles, unknown } = await this.roles();
    const granted = new Set(roles.flatMap(role => role.actions()));
    return {
      buPrincipal: this.principal.buPrincipal,
      roles: roles.map(role => ({
        name: role.name,
        description: role.description,
        policies: role.policies.map(policy => ({
          name: policy.name, description: policy.description, actions: ACTIONS.filter(a => policy.allows(a))
        }))
      })),
      actions: ACTIONS.filter(action => granted.has(action)),
      unknownRoles: unknown,
      publicMode: false
    };
  }
}

/**
 * Public mode (no sign-in): the dashboard is open to whoever reaches it, as before authorization, so every action is
 * allowed but impersonation, which needs someone to impersonate from (decision 14). Only a landscape deployed without
 * authentication uses it.
 */
export class PermitAllAuthorizer implements RequestAuthorizer {
  static readonly NOT_IN_PUBLIC_MODE: readonly Action[] = ['access:impersonate'];

  async authorize(action: Action): Promise<AuthorizationDecision> {
    return PermitAllAuthorizer.NOT_IN_PUBLIC_MODE.includes(action)
      ? { allowed: false, action, roles: [], reason: `This dashboard is public: there is no one signed in to ${action}.` }
      : { allowed: true, action, roles: [], reason: 'This dashboard is public: authorization is off.' };
  }

  async effectivePermissions(): Promise<EffectivePermissions> {
    return {
      roles: [], actions: ACTIONS.filter(a => !PermitAllAuthorizer.NOT_IN_PUBLIC_MODE.includes(a)), unknownRoles: [],
      publicMode: true
    };
  }
}
