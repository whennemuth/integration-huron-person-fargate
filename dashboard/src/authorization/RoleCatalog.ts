import { Policy } from './Policy';
import { Role } from './Role';

/** The roles a landscape's dashboard knows (authorization-plan.md, decision 5) */
export interface RoleCatalog {
  /** The role of this name, if the catalog has one */
  getRole(name: string): Role | undefined;
  /** Every role, in the catalog's order */
  listRoles(): Role[];
}

/** What pruning a set of roles kept, and what it dropped with the kept role that covers each */
export type PrunedRoles = { kept: string[]; pruned: { role: string; coveredBy: string }[] };

/**
 * Drop the roles another chosen role covers (authorization-plan.md, decision 15): a role is covered when every action
 * it grants is also granted by another chosen role, so holding it adds nothing. Actions are compared, not a ranking of
 * roles. Of two roles granting the same actions, the later in the catalog is kept. Names the catalog doesn't know are
 * kept as they are (refusing them is the caller's business). Kept roles come back in the catalog's order.
 */
export const pruneCoveredRoles = (catalog: RoleCatalog, names: string[]): PrunedRoles => {
  const order = catalog.listRoles().map(role => role.name);
  const chosen = [...new Set(names)];
  const known = chosen.filter(name => order.includes(name)).sort((a, b) => order.indexOf(a) - order.indexOf(b));
  const actionsOf = new Map(known.map(name => [name, new Set(catalog.getRole(name)!.actions())]));
  const covers = (by: string, role: string): boolean => {
    const mine = actionsOf.get(role)!;
    const theirs = actionsOf.get(by)!;
    if (by === role || [...mine].some(action => !theirs.has(action))) {
      return false;
    }
    // A strictly larger set covers; an equal one only from later in the catalog, so one of the two is kept
    return theirs.size > mine.size || order.indexOf(by) > order.indexOf(role);
  };
  const kept = known.filter(role => !known.some(by => covers(by, role)));
  const pruned = known
    .filter(role => !kept.includes(role))
    // Covering is transitive, so some kept role covers each dropped one
    .map(role => ({ role, coveredBy: kept.find(by => covers(by, role))! }));
  return { kept: [...kept, ...chosen.filter(name => !order.includes(name))], pruned };
};

/**
 * The roles, policies and actions of decision 5, defined in code. A table-backed catalog could replace it behind
 * RoleCatalog without changing anything else.
 */
export class StaticRoleCatalog implements RoleCatalog {
  static readonly POLICIES = {
    RunReporting: new Policy('RunReporting', "Read the landscape's runs and status", ['runs:read']),
    PersonDataRead: new Policy('PersonDataRead', 'Look people up and preview their sync', ['person:lookup', 'person:preview']),
    PersonDataWrite: new Policy('PersonDataWrite', 'Sync a person into the target', ['person:sync']),
    PipelineControl: new Policy('PipelineControl', "Review and start sync runs, and empty a mock landscape's run state", ['bulk:start', 'mock:reset']),
    PipelineTuning: new Policy('PipelineTuning', "Start a run with more processor tasks than the landscape's maximum", ['bulk:processors']),
    AccessAdministration: new Policy('AccessAdministration', "Manage who holds which roles, and see the dashboard through another person's roles", ['access:admin', 'access:impersonate']),
    AuditReview: new Policy('AuditReview', 'Read the audit trail', ['audit:read'])
  };

  private readonly roles: Role[];

  constructor() {
    const p = StaticRoleCatalog.POLICIES;
    this.roles = [
      new Role('Viewer', 'Sees the runs and the status', [p.RunReporting]),
      new Role('Operator', 'Runs the integration: people and sync runs', [p.RunReporting, p.PersonDataRead, p.PersonDataWrite, p.PipelineControl]),
      new Role('Admin', 'Everything, including roles and the audit trail', Object.values(p))
    ];
  }

  getRole(name: string): Role | undefined {
    return this.roles.find(role => role.name === name);
  }

  listRoles(): Role[] {
    return [...this.roles];
  }
}
