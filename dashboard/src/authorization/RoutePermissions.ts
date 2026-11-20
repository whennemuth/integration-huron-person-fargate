import { Action } from './Actions';

/**
 * What a request needs before it is routed (authorization-plan.md, decision 7: one gate, deny by default):
 * - `page`: a page or the sign-in routes, which need only authentication, as before;
 * - `signed-in`: `/api/me`, which needs only sign-in (a user with no role may ask who they are);
 * - `action`: the action the route needs, and whether it changes anything (decision 14: while impersonating, only
 *   requests that change nothing are allowed);
 * - `impersonation`: starting (`POST`) or stopping (`DELETE`) impersonation, checked against the signed-in user's
 *   own roles, never the impersonated person's; stopping is allowed while impersonating;
 * - `method-not-allowed`: a known route asked with a method it doesn't serve, answered `405` as the handler always
 *   has, without asking for a role, since it reveals nothing;
 * - `unmapped`: an `/api/*` route with no mapping, denied (`403`), so a route added without a mapping is closed.
 */
export type RoutePermission =
  | { kind: 'page' }
  | { kind: 'signed-in' }
  | { kind: 'action'; action: Action; changes: boolean }
  | { kind: 'impersonation'; stop: boolean }
  | { kind: 'method-not-allowed'; allow: string[] }
  | { kind: 'unmapped' };

type MethodMap = { [method: string]: RoutePermission };

const read = (action: Action): RoutePermission => ({ kind: 'action', action, changes: false });
const change = (action: Action): RoutePermission => ({ kind: 'action', action, changes: true });

export class RoutePermissions {
  /** Pages and the sign-in routes: anything outside `/api/` the handler serves needs only authentication */
  static readonly PAGES = ['/', '/dashboard', '/login-page', '/login-redirect'];

  /** Exact `/api/*` paths, by method (decision 5's table) */
  static readonly ROUTES: { [path: string]: MethodMap } = {
    '/api/me': { GET: { kind: 'signed-in' } },

    // People: read personal data, or write it to the target. The page posts each of them.
    '/api/person-lookup': { POST: read('person:lookup') },
    '/api/person-sync/preview': { POST: read('person:preview') },
    '/api/person-sync/preview-experiment': { POST: read('person:preview') },
    '/api/person-sync': { POST: change('person:sync') },

    // Bulk sync: its state is a read (decided 2026-11-17); the review is the first half of a start, but changes
    // nothing, so it may be asked while impersonating (decision 14)
    '/api/bulk-sync': { GET: read('runs:read'), POST: change('bulk:start') },
    '/api/bulk-sync/plan': { POST: read('bulk:start') },

    // The bulk sync schedule: reading it is a read; working out when choices would run, and the review, change
    // nothing (so may be asked while impersonating); saving, turning it on or off, and deleting it change it
    '/api/bulk-schedule': { GET: read('runs:read'), PUT: change('schedule:edit'), DELETE: change('schedule:edit') },
    '/api/bulk-schedule/when': { POST: read('schedule:edit') },
    '/api/bulk-schedule/plan': { POST: read('schedule:edit') },
    '/api/bulk-schedule/state': { POST: change('schedule:edit') },

    // A mock landscape's last reset (a read), and starting one
    '/api/mock-landscape/reset': { GET: read('runs:read'), POST: change('mock:reset') },

    // Run reporting and what describes the landscape
    '/api/status': { GET: read('runs:read') },
    '/api/runs': { GET: read('runs:read') },
    '/api/capabilities': { GET: read('runs:read') },
    '/api/landscapes': { GET: read('runs:read') },

    // Administration (step 5), and impersonation (decision 14)
    // Everyone with a role, and giving roles to several people at once (only to those who hold none)
    '/api/access/assignments': { GET: read('access:admin'), POST: change('access:admin') },
    '/api/access/roles': { GET: read('access:admin') },
    '/api/audit': { GET: read('audit:read') },
    '/api/impersonation': { POST: { kind: 'impersonation', stop: false }, DELETE: { kind: 'impersonation', stop: true } }
  };

  /** `/api/*` paths under a prefix, by method */
  static readonly PREFIXES: { prefix: string; methods: MethodMap }[] = [
    { prefix: '/api/runs/', methods: { GET: read('runs:read') } },
    // One person's assignments: read, replace (with the last-Admin safeguard, step 5) or remove
    {
      prefix: '/api/access/assignments/',
      methods: { GET: read('access:admin'), PUT: change('access:admin'), DELETE: change('access:admin') }
    }
  ];

  static permissionFor(method: string, path: string): RoutePermission {
    if (!path.startsWith('/api/')) {
      // Pages, and anything else outside the API, which the handler answers (a 404 for an unknown page) after
      // authentication as before
      return { kind: 'page' };
    }
    const methods = RoutePermissions.ROUTES[path]
      ?? RoutePermissions.PREFIXES.find(({ prefix }) => path.startsWith(prefix) && path.length > prefix.length)?.methods;
    if (!methods) {
      return { kind: 'unmapped' };
    }
    const verb = (method || 'GET').toUpperCase();
    // A HEAD is a GET without the body: it needs what the GET needs
    const permission = methods[verb] ?? (verb === 'HEAD' ? methods.GET : undefined);
    return permission ?? { kind: 'method-not-allowed', allow: Object.keys(methods) };
  }

  /**
   * Whether a request with this permission may go ahead while impersonating (decision 14): pages, `/api/me`,
   * reads, and stopping impersonation (or starting it for someone else, which replaces it). Anything that changes
   * something is refused, "Stop impersonating to do this".
   */
  static allowedWhileImpersonating(permission: RoutePermission): boolean {
    switch (permission.kind) {
      case 'action': return !permission.changes;
      case 'unmapped': return false;
      default: return true;
    }
  }
}
