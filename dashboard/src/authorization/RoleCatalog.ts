import { Policy } from './Policy';
import { Role } from './Role';

/** The roles a landscape's dashboard knows (authorization-plan.md, decision 5) */
export interface RoleCatalog {
  /** The role of this name, if the catalog has one */
  getRole(name: string): Role | undefined;
  /** Every role, in the catalog's order */
  listRoles(): Role[];
}

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
