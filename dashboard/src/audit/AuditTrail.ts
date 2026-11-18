import { Action } from '../authorization/Actions';
import { AuthorizationDecision } from '../authorization/Authorizer';
import { Principal } from '../authorization/Principal';
import { RoutePermission } from '../authorization/RoutePermissions';
import { ANONYMOUS, AuditEntry, AuditStore } from './AuditStore';

/**
 * The actions whose allowed requests are audited (authorization-plan.md, decision 9): reads of personal data, changes,
 * and administration. runs:read and audit:read aren't (noise); bulk:processors goes in a start's detail.
 */
export const AUDITED_WHEN_ALLOWED: readonly Action[] = [
  'person:lookup', 'person:preview', 'person:sync', 'bulk:start', 'mock:reset', 'access:admin'
];

/** The longest detail kept: a short note, not a payload */
export const MAX_DETAIL_LENGTH = 500;

/**
 * Whether an allowed request with this permission is audited: its action is one of AUDITED_WHEN_ALLOWED, except a
 * run's review (bulk:start without a change), which starts nothing
 */
export const auditsAllowed = (permission: RoutePermission): permission is Extract<RoutePermission, { kind: 'action' }> =>
  permission.kind === 'action'
  && AUDITED_WHEN_ALLOWED.includes(permission.action)
  && !(permission.action === 'bulk:start' && !permission.changes);

/** What an audited request was done to, and a short note of how, from its route and body */
export const describeRequest = (path: string, body: string | null): { target?: string; detail?: string } => {
  let fields: Record<string, any> = {};
  try {
    const parsed = body ? JSON.parse(body) : {};
    fields = parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    // An unparsable body is answered 400 by its route; the record still says who tried what
  }
  const text = (value: unknown) => (typeof value === 'string' || typeof value === 'number') && `${value}`.trim() ? `${value}`.trim() : undefined;
  switch (path) {
    case '/api/person-lookup': {
      const name = [text(fields.firstName), text(fields.lastName)].filter(Boolean).join(' ');
      return { target: text(fields.personId) ?? (name || undefined), detail: [text(fields.system), text(fields.searchType) && `by ${fields.searchType}`].filter(Boolean).join(' ') || undefined };
    }
    case '/api/person-sync/preview':
    case '/api/person-sync/preview-experiment':
    case '/api/person-sync':
      return {
        target: text(fields.personId) ?? text(fields.hrn),
        detail: [path.endsWith('experiment') ? 'experiment' : undefined, text(fields.operation)].filter(Boolean).join(', ') || undefined
      };
    case '/api/bulk-sync': {
      // A start's choices, the ones that say what kind of run it was (bulk:processors among them)
      const choices = ['runType', 'bulkReset', 'resetState', 'source', 'processorScalingCapacityOverride', 'parallelStart', 'iterationLimit']
        .filter(name => fields[name] !== undefined)
        .map(name => `${name}=${typeof fields[name] === 'object' ? JSON.stringify(fields[name]) : fields[name]}`);
      return { target: text(fields.buid), detail: choices.join(', ') || undefined };
    }
    default: {
      // One person's assignments: the person, and the roles they were given
      const person = path.match(/^\/api\/access\/assignments\/([^/]+)$/)?.[1];
      if (!person) {
        return {};
      }
      let target = person;
      try { target = decodeURIComponent(person); } catch { /* kept as sent */ }
      const roles = Array.isArray(fields.roles) ? `roles=${fields.roles.join(',') || 'none'}` : undefined;
      return { target, ...(roles && { detail: roles }) };
    }
  }
};

/**
 * The audit trail of one request (decision 9): who made it, in which landscape, bound once; then what was decided and
 * how it went. Recording never fails the request: a write error is logged and swallowed.
 */
export class AuditTrail {
  private deniedRecorded = false;

  /** viewedAs: while impersonating, the person whose roles decided the request (decision 14) */
  constructor(private readonly params: { store: AuditStore; landscape: string; principal: Principal; viewedAs?: Principal }) {}

  /** Who made the request, as recorded: the signed-in user's buPrincipal, or anonymous */
  get buPrincipal(): string {
    return this.params.principal.buPrincipal ?? ANONYMOUS;
  }

  /** Whether this request's denial is already recorded (a choice refused within the route) */
  get denialRecorded(): boolean {
    return this.deniedRecorded;
  }

  async record(entry: Omit<AuditEntry, 'landscape' | 'buPrincipal'>): Promise<void> {
    const { store, landscape, viewedAs } = this.params;
    const detail = entry.detail && entry.detail.length > MAX_DETAIL_LENGTH ? `${entry.detail.slice(0, MAX_DETAIL_LENGTH - 1)}…` : entry.detail;
    try {
      await store.append({
        ...entry, ...(detail !== undefined && { detail }), landscape, buPrincipal: this.buPrincipal,
        ...(viewedAs?.buPrincipal && { viewedAs: viewedAs.buPrincipal })
      });
    } catch (error) {
      console.error(`Couldn't record ${entry.decision} ${entry.action} for ${this.buPrincipal} in the audit trail: ${error instanceof Error ? error.message : error}`);
    }
  }

  /** A denial, with the reason as its detail */
  async denied(decision: AuthorizationDecision, target?: string): Promise<void> {
    this.deniedRecorded = true;
    await this.record({ action: decision.action, decision: 'DENIED', ...(target && { target }), detail: decision.reason });
  }

  /** An allowed request, once its route has answered: SUCCEEDED for a 2xx, else FAILED */
  async allowed(action: Action, statusCode: number, described: { target?: string; detail?: string }): Promise<void> {
    await this.record({
      action, decision: 'ALLOWED', outcome: statusCode >= 200 && statusCode < 300 ? 'SUCCEEDED' : 'FAILED',
      ...(described.target && { target: described.target }),
      ...(described.detail && { detail: statusCode >= 300 ? `${described.detail} (HTTP ${statusCode})` : described.detail }),
      ...(!described.detail && statusCode >= 300 && { detail: `HTTP ${statusCode}` })
    });
  }
}
