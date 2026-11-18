/**
 * RoutePermissions (dashboard/src/authorization/RoutePermissions.ts): what each request needs before it is routed
 * (authorization-plan.md decisions 5, 7 and 14).
 */
import { RoutePermissions } from '../dashboard/src/authorization/RoutePermissions';

const permit = RoutePermissions.permissionFor;

describe('RoutePermissions', () => {
  it('lets pages and the sign-in routes through with authentication only', () => {
    for (const path of [...RoutePermissions.PAGES, '/favicon.ico', '/no-such-page']) {
      expect(permit('GET', path)).toEqual({ kind: 'page' });
    }
  });

  it('needs only sign-in for /api/me', () => {
    expect(permit('GET', '/api/me')).toEqual({ kind: 'signed-in' });
  });

  it.each([
    ['POST', '/api/person-lookup', 'person:lookup', false],
    ['POST', '/api/person-sync/preview', 'person:preview', false],
    ['POST', '/api/person-sync/preview-experiment', 'person:preview', false],
    ['POST', '/api/person-sync', 'person:sync', true],
    ['GET', '/api/bulk-sync', 'runs:read', false],
    ['POST', '/api/bulk-sync', 'bulk:start', true],
    ['POST', '/api/bulk-sync/plan', 'bulk:start', false],
    ['GET', '/api/mock-landscape/reset', 'runs:read', false],
    ['POST', '/api/mock-landscape/reset', 'mock:reset', true],
    ['GET', '/api/status', 'runs:read', false],
    ['GET', '/api/runs', 'runs:read', false],
    ['GET', '/api/runs/2026-11-04T05%3A00%3A00.000Z/summary', 'runs:read', false],
    ['GET', '/api/runs/2026-11-04T05:00:00.000Z/errors', 'runs:read', false],
    ['GET', '/api/capabilities', 'runs:read', false],
    ['GET', '/api/landscapes', 'runs:read', false],
    ['GET', '/api/access/assignments', 'access:admin', false],
    ['GET', '/api/access/roles', 'access:admin', false],
    ['GET', '/api/access/assignments/jdoe', 'access:admin', false],
    ['PUT', '/api/access/assignments/jdoe', 'access:admin', true],
    ['DELETE', '/api/access/assignments/jdoe', 'access:admin', true],
    ['GET', '/api/audit', 'audit:read', false],
    ['head', '/api/runs', 'runs:read', false]
  ])('%s %s needs %s (changes: %s)', (method, path, action, changes) => {
    expect(permit(method, path)).toEqual({ kind: 'action', action, changes });
  });

  it('maps starting and stopping impersonation', () => {
    expect(permit('POST', '/api/impersonation')).toEqual({ kind: 'impersonation', stop: false });
    expect(permit('DELETE', '/api/impersonation')).toEqual({ kind: 'impersonation', stop: true });
  });

  it.each([
    ['GET', '/api/bulk-sync/plan', ['POST']],
    ['DELETE', '/api/bulk-sync', ['GET', 'POST']],
    ['PUT', '/api/mock-landscape/reset', ['GET', 'POST']],
    ['POST', '/api/runs', ['GET']],
    ['POST', '/api/runs/2026-11-04T05:00:00.000Z/summary', ['GET']],
    ['GET', '/api/person-sync', ['POST']],
    ['GET', '/api/impersonation', ['POST', 'DELETE']]
  ])('answers %s %s 405, allowing %j', (method, path, allow) => {
    expect(permit(method, path)).toEqual({ kind: 'method-not-allowed', allow });
  });

  it('denies an /api route with no mapping', () => {
    for (const path of ['/api/', '/api/secret', '/api/runs/', '/api/access/assignments/', '/api/bulk-sync/plan/x', '/api/ME']) {
      expect(permit('GET', path)).toEqual({ kind: 'unmapped' });
    }
  });

  it('allows only what changes nothing while impersonating', () => {
    const allowed = (method: string, path: string) => RoutePermissions.allowedWhileImpersonating(permit(method, path));
    expect(allowed('GET', '/dashboard')).toBe(true);
    expect(allowed('GET', '/api/me')).toBe(true);
    expect(allowed('GET', '/api/runs')).toBe(true);
    expect(allowed('POST', '/api/person-lookup')).toBe(true);
    expect(allowed('POST', '/api/person-sync/preview')).toBe(true);
    expect(allowed('POST', '/api/bulk-sync/plan')).toBe(true);
    expect(allowed('GET', '/api/access/assignments/jdoe')).toBe(true);
    expect(allowed('DELETE', '/api/impersonation')).toBe(true);
    expect(allowed('POST', '/api/impersonation')).toBe(true);

    expect(allowed('POST', '/api/person-sync')).toBe(false);
    expect(allowed('POST', '/api/bulk-sync')).toBe(false);
    expect(allowed('POST', '/api/mock-landscape/reset')).toBe(false);
    expect(allowed('PUT', '/api/access/assignments/jdoe')).toBe(false);
    expect(allowed('DELETE', '/api/access/assignments/jdoe')).toBe(false);
    expect(allowed('GET', '/api/secret')).toBe(false);
  });
});
