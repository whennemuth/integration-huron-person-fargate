import { AuthorizationDecision, Authorizer, PermitAllAuthorizer, RequestAuthorizer, RoleAssignments } from './Authorizer';
import { Principal } from './Principal';
import { RoleCatalog } from './RoleCatalog';
import { RoutePermission, RoutePermissions } from './RoutePermissions';

/** What the gate decided of a request, for the route that serves it */
export type RequestAccess = {
  /** Who signed in; anonymous in public mode, or without a sign-in token that verifies */
  principal: Principal;
  /** Decides this request's actions, and the choices within it (Bulk sync's) */
  authorizer: RequestAuthorizer;
  publicMode: boolean;
  permission: RoutePermission;
  /** The route's own decision, when it needs an action */
  decision?: AuthorizationDecision;
};

/** A request the gate answers itself, as JSON */
export type Refusal = { status: 403 | 405; body: Record<string, unknown>; headers?: Record<string, string> };

export type GateResult = { access: RequestAccess; refusal?: Refusal };

/** The body of a 403 (authorization-plan.md, decision 7): `{ error: "FORBIDDEN", action, roles, message }` */
export const forbiddenBody = (decision: AuthorizationDecision): Record<string, unknown> => ({
  error: 'FORBIDDEN', action: decision.action, roles: decision.roles, message: decision.reason
});

/** A choice within a request that the user's roles don't allow (Bulk sync's), refused with the same 403 */
export class ForbiddenError extends Error {
  constructor(readonly decision: AuthorizationDecision) {
    super(decision.reason);
    this.name = 'ForbiddenError';
  }

  get body(): Record<string, unknown> {
    return forbiddenBody(this.decision);
  }
}

/**
 * The one gate every request passes before it is routed (decision 7: deny by default). It finds what the route needs
 * (RoutePermissions), who is asking (the verified sign-in token's claims), and decides with their roles: pages need
 * only authentication, `/api/me` only sign-in, every other `/api` route its action, and an unmapped one is refused.
 * Public mode has no sign-in, so everything is allowed but impersonation (PermitAllAuthorizer).
 */
export class RequestGate {
  constructor(private readonly assignments: RoleAssignments, private readonly catalog: RoleCatalog) {}

  async check(params: { method: string; path: string; publicMode: boolean; claims: unknown }): Promise<GateResult> {
    const { method, path, publicMode, claims } = params;
    const permission = RoutePermissions.permissionFor(method, path);
    const principal = publicMode ? Principal.anonymous() : Principal.fromClaims(claims);
    const authorizer: RequestAuthorizer = publicMode
      ? new PermitAllAuthorizer()
      : new Authorizer(principal, this.assignments, this.catalog);
    const access: RequestAccess = { principal, authorizer, publicMode, permission };

    switch (permission.kind) {
      case 'page':
        return { access };
      case 'method-not-allowed':
        return {
          access,
          refusal: { status: 405, body: { error: 'Method not allowed' }, headers: { Allow: permission.allow.join(', ') } }
        };
      case 'unmapped':
        return {
          access,
          refusal: { status: 403, body: { error: 'FORBIDDEN', message: `${method} ${path} is not a route of this dashboard.` } }
        };
      case 'signed-in':
        if (!publicMode && principal.isAnonymous) {
          return { access, refusal: { status: 403, body: { error: 'FORBIDDEN', message: 'Sign in to see who you are.' } } };
        }
        return { access };
      case 'impersonation':
      case 'action': {
        // Impersonation is decided on the user's own roles (decision 14); its routes come with its gate (step 6)
        const action = permission.kind === 'action' ? permission.action : 'access:impersonate';
        const decision = await authorizer.authorize(action);
        return decision.allowed
          ? { access: { ...access, decision } }
          : { access: { ...access, decision }, refusal: { status: 403, body: forbiddenBody(decision) } };
      }
    }
  }
}
