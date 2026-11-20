/**
 * The authorization model (dashboard/src/authorization, authorization-plan.md decision 5): actions, policies, roles,
 * the static catalog, principals from the sign-in token's claims, and the Authorizer's decisions.
 */
import { ACTIONS, isAction } from '../dashboard/src/authorization/Actions';
import { Authorizer, denialMessage, PermitAllAuthorizer, RoleAssignments } from '../dashboard/src/authorization/Authorizer';
import { Principal, PrincipalResolver } from '../dashboard/src/authorization/Principal';
import { Policy } from '../dashboard/src/authorization/Policy';
import { Role } from '../dashboard/src/authorization/Role';
import { pruneCoveredRoles, RoleCatalog, StaticRoleCatalog } from '../dashboard/src/authorization/RoleCatalog';

const catalog = new StaticRoleCatalog();

const assignmentsOf = (map: { [buPrincipal: string]: string[] }): RoleAssignments & { calls: number } => {
  const assignments = {
    calls: 0,
    async getRoles(buPrincipal: string) {
      assignments.calls++;
      return map[buPrincipal] ?? [];
    }
  };
  return assignments;
};

const claimsOf = (buPrincipal: string, extra: object = {}) => ({
  buPrincipal: [buPrincipal], eduPersonPrincipalName: [`${buPrincipal}@bu.edu`], mail: [`${buPrincipal}@bu.edu`],
  givenName: ['Pat'], sn: ['Doe'], ...extra
});

describe('StaticRoleCatalog', () => {
  it('has the roles of decision 5, from least to most', () => {
    expect(catalog.listRoles().map(r => r.name)).toEqual(
      ['RunViewer', 'PersonViewer', 'Viewer', 'PersonOperator', 'PipelineOperator', 'Operator', 'Admin']);
  });

  it('builds each role from its policies (2026-11-18)', () => {
    const policies = (name: string) => catalog.getRole(name)!.policies.map(policy => policy.name);
    expect(policies('RunViewer')).toEqual(['RunReporting']);
    expect(policies('PersonViewer')).toEqual(['PersonDataRead']);
    expect(policies('Viewer')).toEqual(['RunReporting', 'PersonDataRead']);
    expect(policies('PersonOperator')).toEqual(['RunReporting', 'PersonDataRead', 'PersonDataWrite']);
    expect(policies('PipelineOperator')).toEqual(['RunReporting', 'PersonDataRead', 'PipelineControl']);
    expect(policies('Operator')).toEqual(['RunReporting', 'PersonDataRead', 'PersonDataWrite', 'PipelineControl']);
    expect(policies('Admin')).toEqual(Object.keys(StaticRoleCatalog.POLICIES));
  });

  it('grants each role exactly its actions', () => {
    expect(catalog.getRole('RunViewer')!.actions()).toEqual(['runs:read']);
    expect(catalog.getRole('PersonViewer')!.actions()).toEqual(['person:lookup', 'person:preview']);
    expect(catalog.getRole('Viewer')!.actions()).toEqual(['runs:read', 'person:lookup', 'person:preview']);
    expect(catalog.getRole('PersonOperator')!.actions()).toEqual(['runs:read', 'person:lookup', 'person:preview', 'person:sync']);
    expect(catalog.getRole('PipelineOperator')!.actions()).toEqual(['runs:read', 'person:lookup', 'person:preview', 'bulk:start', 'schedule:edit', 'mock:reset']);
    expect(catalog.getRole('Operator')!.actions()).toEqual(
      ['runs:read', 'person:lookup', 'person:preview', 'person:sync', 'bulk:start', 'schedule:edit', 'mock:reset']);
    expect(catalog.getRole('Admin')!.actions()).toEqual([...ACTIONS]);
  });

  it('has no two roles granting the same actions, so pruning never has to choose between equals', () => {
    const sets = catalog.listRoles().map(role => role.actions().join(','));
    expect(new Set(sets).size).toBe(sets.length);
  });

  it('prunes the finer roles into the one that covers them', () => {
    expect(pruneCoveredRoles(catalog, ['PersonOperator', 'PipelineOperator']).kept).toEqual(['PersonOperator', 'PipelineOperator']);
    expect(pruneCoveredRoles(catalog, ['PersonOperator', 'PipelineOperator', 'Operator']).kept).toEqual(['Operator']);
    expect(pruneCoveredRoles(catalog, ['RunViewer', 'PersonViewer', 'Viewer'])).toEqual({
      kept: ['Viewer'], pruned: [{ role: 'RunViewer', coveredBy: 'Viewer' }, { role: 'PersonViewer', coveredBy: 'Viewer' }]
    });
    // Neither covers the other: both are kept, though together they grant what Viewer does
    expect(pruneCoveredRoles(catalog, ['RunViewer', 'PersonViewer']).kept).toEqual(['RunViewer', 'PersonViewer']);
  });

  it('keeps bulk:processors and impersonation from Operators', () => {
    const operator = catalog.getRole('Operator')!;
    expect(operator.allows('bulk:processors')).toBeUndefined();
    expect(operator.allows('access:impersonate')).toBeUndefined();
    expect(catalog.getRole('Admin')!.allows('bulk:processors')!.name).toBe('PipelineTuning');
    expect(catalog.getRole('Admin')!.allows('access:impersonate')!.name).toBe('AccessAdministration');
  });

  it('puts every action in exactly one policy', () => {
    const policies = Object.values(StaticRoleCatalog.POLICIES);
    for (const action of ACTIONS) {
      expect(policies.filter(p => p.allows(action)).length).toBe(1);
    }
  });

  it('knows no other role', () => {
    expect(catalog.getRole('Superuser')).toBeUndefined();
    expect(catalog.getRole('admin')).toBeUndefined();
  });

  it('recognizes actions', () => {
    expect(isAction('bulk:start')).toBe(true);
    expect(isAction('bulk:stop')).toBe(false);
  });
});

describe('pruneCoveredRoles', () => {
  it('keeps only Admin of all three, saying what covers the others', () => {
    expect(pruneCoveredRoles(catalog, ['Viewer', 'Operator', 'Admin'])).toEqual({
      kept: ['Admin'], pruned: [{ role: 'Viewer', coveredBy: 'Admin' }, { role: 'Operator', coveredBy: 'Admin' }]
    });
    expect(pruneCoveredRoles(catalog, ['Operator', 'Viewer'])).toEqual({ kept: ['Operator'], pruned: [{ role: 'Viewer', coveredBy: 'Operator' }] });
  });

  it('keeps a single role, no role, and repeats once', () => {
    expect(pruneCoveredRoles(catalog, ['Viewer', 'Viewer'])).toEqual({ kept: ['Viewer'], pruned: [] });
    expect(pruneCoveredRoles(catalog, [])).toEqual({ kept: [], pruned: [] });
  });

  /** A catalog of policies made up for the rule's cases */
  const custom = (roles: [string, string[]][]): RoleCatalog => {
    const made = roles.map(([name, actions]) => new Role(name, name, [new Policy(`${name}Policy`, name, actions as any)]));
    return { getRole: name => made.find(role => role.name === name), listRoles: () => made };
  };

  it("keeps roles that don't cover each other, whatever their rank", () => {
    const roles = custom([['Auditor', ['audit:read']], ['Operator', ['runs:read', 'bulk:start']]]);
    expect(pruneCoveredRoles(roles, ['Operator', 'Auditor'])).toEqual({ kept: ['Auditor', 'Operator'], pruned: [] });
  });

  it('keeps the later in the catalog of two that grant the same actions', () => {
    const roles = custom([['Reader', ['runs:read']], ['Watcher', ['runs:read']]]);
    expect(pruneCoveredRoles(roles, ['Watcher', 'Reader'])).toEqual({ kept: ['Watcher'], pruned: [{ role: 'Reader', coveredBy: 'Watcher' }] });
  });

  it('leaves names the catalog does not know to the caller', () => {
    expect(pruneCoveredRoles(catalog, ['Ghost', 'Viewer', 'Admin'])).toEqual({ kept: ['Admin', 'Ghost'], pruned: [{ role: 'Viewer', coveredBy: 'Admin' }] });
  });
});

describe('Principal', () => {
  it('is built from array-valued claims', () => {
    const p = Principal.fromClaims(claimsOf('wrh', { eduPersonNickname: ['Warren'] }));
    expect(p.buPrincipal).toBe('wrh');
    expect(p.eppn).toBe('wrh@bu.edu');
    expect(p.mail).toBe('wrh@bu.edu');
    expect(p.displayName).toBe('Warren Doe');
    expect(p.label).toBe('Warren Doe');
    expect(p.isAnonymous).toBe(false);
  });

  it('accepts plain string claims and lower-cases the key', () => {
    const p = Principal.fromClaims({ buPrincipal: ' WRH ' });
    expect(p.buPrincipal).toBe('wrh');
    expect(p.label).toBe('wrh');
  });

  it('is anonymous without a usable buPrincipal', () => {
    for (const claims of [undefined, null, 'wrh', {}, { buPrincipal: [] }, { buPrincipal: [''] }, { buPrincipal: [42] }]) {
      expect(Principal.fromClaims(claims).isAnonymous).toBe(true);
    }
  });

  it('refuses to make a principal of nothing', () => {
    expect(() => Principal.of('  ')).toThrow();
    expect(Principal.of('JDoe').buPrincipal).toBe('jdoe');
  });
});

describe('PrincipalResolver', () => {
  it('takes a buPrincipal as it is', () => {
    expect(PrincipalResolver.resolve(' jdoe ')).toEqual({ ok: true, buPrincipal: 'jdoe', from: 'buPrincipal' });
  });

  it('takes the id of a bu.edu eduPersonPrincipalName', () => {
    expect(PrincipalResolver.resolve('JDoe@BU.edu')).toEqual({ ok: true, buPrincipal: 'jdoe', from: 'eppn' });
  });

  it('refuses another domain, an empty id, junk and nothing', () => {
    for (const input of ['jdoe@gmail.com', '@bu.edu', 'a@b@bu.edu', 'j doe', '<script>', '', '   ', undefined, 7]) {
      const resolution = PrincipalResolver.resolve(input);
      expect(resolution.ok).toBe(false);
    }
  });
});

describe('Authorizer', () => {
  const assignments = assignmentsOf({
    viewer: ['Viewer'], operator: ['Operator'], admin: ['Admin'], both: ['Operator', 'Viewer'], odd: ['Viewer', 'Ghost']
  });
  const authorizerFor = (buPrincipal: string) => new Authorizer(Principal.of(buPrincipal), assignments, catalog);

  it('allows what a role grants, naming the role and policy', async () => {
    const decision = await authorizerFor('operator').authorize('bulk:start');
    expect(decision).toMatchObject({ allowed: true, action: 'bulk:start', roles: ['Operator'], role: 'Operator', policy: 'PipelineControl' });
  });

  it('denies what no role grants, with the message of decision 7', async () => {
    const decision = await authorizerFor('viewer').authorize('bulk:start');
    expect(decision.allowed).toBe(false);
    expect(decision.roles).toEqual(['Viewer']);
    expect(decision.reason).toBe('None of your roles (Viewer) has a policy that grants bulk:start.');
  });

  it('denies everything to a user with no role', async () => {
    for (const action of ACTIONS) {
      const decision = await authorizerFor('nobody').authorize(action);
      expect(decision.allowed).toBe(false);
      expect(decision.reason).toBe(denialMessage(action, []));
    }
  });

  it('denies everything to an anonymous principal, without asking for roles', async () => {
    const counted = assignmentsOf({});
    const decision = await new Authorizer(Principal.anonymous(), counted, catalog).authorize('runs:read');
    expect(decision.allowed).toBe(false);
    expect(counted.calls).toBe(0);
  });

  it('keeps only Admins on bulk:processors, access and audit', async () => {
    for (const action of ['bulk:processors', 'access:admin', 'access:impersonate', 'audit:read'] as const) {
      expect((await authorizerFor('operator').authorize(action)).allowed).toBe(false);
      expect((await authorizerFor('admin').authorize(action)).allowed).toBe(true);
    }
  });

  it('lists roles in the catalog order and ignores unknown ones', async () => {
    expect((await authorizerFor('both').authorize('runs:read')).roles).toEqual(['Viewer', 'Operator']);
    const odd = authorizerFor('odd');
    expect((await odd.authorize('runs:read')).roles).toEqual(['Viewer']);
    expect((await odd.effectivePermissions()).unknownRoles).toEqual(['Ghost']);
  });

  it('reads the roles once per request', async () => {
    const counted = assignmentsOf({ admin: ['Admin'] });
    const authorizer = new Authorizer(Principal.of('admin'), counted, catalog);
    await Promise.all([authorizer.authorize('runs:read'), authorizer.authorize('bulk:start'), authorizer.effectivePermissions()]);
    expect(counted.calls).toBe(1);
  });

  it('describes its effective permissions for /api/me', async () => {
    const permissions = await authorizerFor('both').effectivePermissions();
    expect(permissions.buPrincipal).toBe('both');
    expect(permissions.publicMode).toBe(false);
    expect(permissions.roles.map(r => r.name)).toEqual(['Viewer', 'Operator']);
    expect(permissions.roles[1].policies.map(p => p.name)).toEqual(['RunReporting', 'PersonDataRead', 'PersonDataWrite', 'PipelineControl']);
    expect(permissions.actions).toEqual(catalog.getRole('Operator')!.actions());
  });
});

describe('PermitAllAuthorizer', () => {
  it('allows every action but impersonation in public mode', async () => {
    const authorizer = new PermitAllAuthorizer();
    for (const action of ACTIONS) {
      expect((await authorizer.authorize(action)).allowed).toBe(action !== 'access:impersonate');
    }
    const permissions = await authorizer.effectivePermissions();
    expect(permissions.publicMode).toBe(true);
    expect(permissions.actions).not.toContain('access:impersonate');
    expect(permissions.actions.length).toBe(ACTIONS.length - 1);
  });
});
