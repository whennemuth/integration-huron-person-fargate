import { Authorizer, RoleAssignments } from './Authorizer';
import { Principal, PrincipalResolver } from './Principal';
import { RoleCatalog } from './RoleCatalog';
import { RoutePermission, RoutePermissions } from './RoutePermissions';

/**
 * Seeing the dashboard through another person's roles (authorization-plan.md, decision 14).
 *
 * The person travels in a cookie, which grants nothing by itself: every request re-checks that the signed-in user's
 * **own** roles still grant `access:impersonate`. If they do, the request is authorized with the person's roles; if
 * not (the role was taken away, or the cookie was made by hand), the cookie is ignored and cleared, the request is
 * authorized with the user's own roles, and the attempt is a denial to audit. While impersonating, only requests that
 * change nothing go ahead (`refusalWhileImpersonating()`).
 */
export class Impersonation {
  /**
   * Host-only (no Domain), so it stays in this landscape, whose roles are its own; `HttpOnly`, `Secure`,
   * `SameSite=Strict`; a session cookie, so closing the browser stops it.
   */
  static readonly COOKIE_NAME = 'hp-dashboard-impersonate';
  static readonly STOP_TO_CHANGE = 'Stop impersonating to do this.';

  private constructor(
    /** Who is signed in: the person audit records and `startedBy` name */
    readonly user: Principal,
    /** Whose roles decide this request: the impersonated person, else the user */
    readonly effective: Principal,
    /** The authorizer of the user's own roles (the Impersonate button, starting and stopping) */
    readonly userAuthorizer: Authorizer,
    /** The authorizer of the effective principal's roles: every other check of the request */
    readonly authorizer: Authorizer,
    /** Whether the user's own roles grant `access:impersonate` (the top bar's button) */
    readonly mayImpersonate: boolean,
    /** The cookie must be cleared: it named no one usable, or the user may no longer impersonate */
    readonly clearCookie: boolean,
    /** Why a cookie was refused, when the refusal is a denial to audit (a revoked role, a forged cookie) */
    readonly denial?: string
  ) {}

  get impersonating(): boolean {
    return this.effective !== this.user;
  }

  /** Resolve a request's impersonation from its signed-in user and its `cookie` header */
  static async resolve(params: {
    user: Principal; cookieHeader?: string; assignments: RoleAssignments; catalog: RoleCatalog;
  }): Promise<Impersonation> {
    const { user, cookieHeader, assignments, catalog } = params;
    const userAuthorizer = new Authorizer(user, assignments, catalog);
    const mayImpersonate = !user.isAnonymous && (await userAuthorizer.authorize('access:impersonate')).allowed;
    const as = (effective: Principal, authorizer: Authorizer, clearCookie: boolean, denial?: string) =>
      new Impersonation(user, effective, userAuthorizer, authorizer, mayImpersonate, clearCookie, denial);
    const asSelf = (clearCookie: boolean, denial?: string) => as(user, userAuthorizer, clearCookie, denial);

    const raw = Impersonation.readCookie(cookieHeader);
    if (raw === undefined) {
      return asSelf(false);
    }
    if (user.isAnonymous) {
      return asSelf(true, 'An impersonation cookie came with no one signed in.');
    }
    const resolved = PrincipalResolver.resolve(raw);
    if (!resolved.ok || resolved.from !== 'buPrincipal') {
      return asSelf(true, `The impersonation cookie names no valid buPrincipal (${JSON.stringify(raw)}).`);
    }
    if (!mayImpersonate) {
      return asSelf(true, `${user.buPrincipal} may not impersonate (no role grants access:impersonate); ` +
        `ignored a cookie naming ${resolved.buPrincipal}.`);
    }
    if (resolved.buPrincipal === user.buPrincipal) {
      // Harmless (only the start route sets it, and it refuses yourself), so cleared without a denial
      return asSelf(true);
    }
    const person = Principal.of(resolved.buPrincipal);
    return as(person, new Authorizer(person, assignments, catalog), false);
  }

  /**
   * Why a request with this permission is refused while impersonating, or undefined: anything that changes
   * something is refused, even when the person's roles would allow it, so nothing is ever done in someone else's
   * name.
   */
  refusalFor(permission: RoutePermission): string | undefined {
    return this.impersonating && !RoutePermissions.allowedWhileImpersonating(permission)
      ? Impersonation.STOP_TO_CHANGE
      : undefined;
  }

  /** Why starting to impersonate this person is refused, or undefined */
  static startRefusal(user: Principal, buPrincipal: string): string | undefined {
    if (user.isAnonymous) {
      return 'Sign in to impersonate someone.';
    }
    if (buPrincipal === user.buPrincipal) {
      return "You can't impersonate yourself.";
    }
    return undefined;
  }

  /** The cookie's value, undefined when the header has none; the first of several, as the browser orders them */
  static readCookie(cookieHeader?: string): string | undefined {
    if (!cookieHeader) {
      return undefined;
    }
    for (const part of cookieHeader.split(/;\s*/)) {
      const at = part.indexOf('=');
      if (at > 0 && part.slice(0, at).trim() === Impersonation.COOKIE_NAME) {
        try {
          return decodeURIComponent(part.slice(at + 1).trim());
        } catch {
          return part.slice(at + 1).trim();
        }
      }
    }
    return undefined;
  }

  /** The `Set-Cookie` value that starts impersonating this person */
  static setCookie(buPrincipal: string): string {
    return `${Impersonation.COOKIE_NAME}=${encodeURIComponent(buPrincipal)}; Path=/; HttpOnly; Secure; SameSite=Strict`;
  }

  /** The `Set-Cookie` value that stops it (and clears a refused cookie) */
  static clearCookieValue(): string {
    return `${Impersonation.COOKIE_NAME}=; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=0; ` +
      'Expires=Thu, 01 Jan 1970 00:00:00 GMT';
  }
}
