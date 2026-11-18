import { AuthorizationDecision, PermitAllAuthorizer, RequestAuthorizer, RoleAssignments } from './Authorizer';
import { Impersonation } from './Impersonation';
import { Principal } from './Principal';
import { RoleCatalog } from './RoleCatalog';
import { RoutePermission, RoutePermissions } from './RoutePermissions';

/** What the gate decided of a request, for the route that serves it */
export type RequestAccess = {
  /** Who signed in; anonymous in public mode, or without a sign-in token that verifies. Audit and startedBy name them */
  principal: Principal;
  /** While impersonating (decision 14): the person whose roles decide the request */
  viewedAs?: Principal;
  /** Decides this request's actions, and the choices within it (Bulk sync's): the person's while impersonating */
  authorizer: RequestAuthorizer;
  /** The signed-in user's own roles, whatever they impersonate: starting and stopping impersonation, the top bar */
  userAuthorizer: RequestAuthorizer;
  /** Whether the user's own roles grant access:impersonate (the top bar's Impersonate button) */
  mayImpersonate: boolean;
  publicMode: boolean;
  permission: RoutePermission;
  /** The route's own decision, when it needs an action */
  decision?: AuthorizationDecision;
  /** Set-Cookie values the response must carry: clearing an impersonation cookie that was refused */
  setCookies: string[];
  /** Why an impersonation cookie was refused, when that is a denial to audit (a revoked role, a forged cookie) */
  impersonationDenial?: string;
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
 * (RoutePermissions), who is asking (the verified sign-in token's claims) and whose roles decide (the impersonated
 * person's, if the impersonation cookie names one and the user's own roles still allow it: decision 14), and decides:
 * pages need only authentication, `/api/me` and stopping impersonation only sign-in, starting impersonation the user's
 * own `access:impersonate`, every other `/api` route its action, and an unmapped one is refused. While impersonating,
 * a request that changes something is refused even when the person's roles would allow it. Public mode has no
 * sign-in, so everything is allowed but impersonation (PermitAllAuthorizer).
 */
export class RequestGate {
  constructor(private readonly assignments: RoleAssignments, private readonly catalog: RoleCatalog) {}

  async check(params: { method: string; path: string; publicMode: boolean; claims: unknown; cookieHeader?: string }): Promise<GateResult> {
    const { method, path, publicMode, claims, cookieHeader } = params;
    const permission = RoutePermissions.permissionFor(method, path);
    const access = await this.access({ publicMode, claims, cookieHeader, permission });
    const refuse = (refusal: Refusal): GateResult => ({ access, refusal });

    switch (permission.kind) {
      case 'page':
        return { access };
      case 'method-not-allowed':
        return refuse({ status: 405, body: { error: 'Method not allowed' }, headers: { Allow: permission.allow.join(', ') } });
      case 'unmapped':
        return refuse({ status: 403, body: { error: 'FORBIDDEN', message: `${method} ${path} is not a route of this dashboard.` } });
      case 'signed-in':
        if (!publicMode && access.principal.isAnonymous) {
          return refuse({ status: 403, body: { error: 'FORBIDDEN', message: 'Sign in to see who you are.' } });
        }
        return { access };
      case 'impersonation': {
        if (permission.stop) {
          // Anyone signed in may stop: it only clears their own cookie
          return access.principal.isAnonymous && !publicMode
            ? refuse({ status: 403, body: { error: 'FORBIDDEN', message: 'Sign in to stop impersonating.' } })
            : { access };
        }
        // Decided on the user's own roles, never the impersonated person's
        const decision = await access.userAuthorizer.authorize('access:impersonate');
        access.decision = decision;
        return decision.allowed ? { access } : refuse({ status: 403, body: forbiddenBody(decision) });
      }
      case 'action': {
        const decision = await access.authorizer.authorize(permission.action);
        access.decision = decision;
        if (!decision.allowed) {
          return refuse({ status: 403, body: forbiddenBody(decision) });
        }
        if (access.viewedAs && permission.changes) {
          return refuse({
            status: 403,
            body: { error: 'FORBIDDEN', code: 'IMPERSONATING', action: permission.action, message: Impersonation.STOP_TO_CHANGE }
          });
        }
        return { access };
      }
    }
  }

  private async access(params: {
    publicMode: boolean; claims: unknown; cookieHeader?: string; permission: RoutePermission;
  }): Promise<RequestAccess> {
    const { publicMode, claims, cookieHeader, permission } = params;
    if (publicMode) {
      const authorizer = new PermitAllAuthorizer();
      return {
        principal: Principal.anonymous(), authorizer, userAuthorizer: authorizer, mayImpersonate: false, publicMode,
        permission, setCookies: []
      };
    }
    const impersonation = await Impersonation.resolve({
      user: Principal.fromClaims(claims), cookieHeader, assignments: this.assignments, catalog: this.catalog
    });
    return {
      principal: impersonation.user,
      ...(impersonation.impersonating && { viewedAs: impersonation.effective }),
      authorizer: impersonation.authorizer,
      userAuthorizer: impersonation.userAuthorizer,
      mayImpersonate: impersonation.mayImpersonate,
      publicMode,
      permission,
      setCookies: impersonation.clearCookie ? [Impersonation.clearCookieValue()] : [],
      ...(impersonation.denial && { impersonationDenial: impersonation.denial })
    };
  }
}
