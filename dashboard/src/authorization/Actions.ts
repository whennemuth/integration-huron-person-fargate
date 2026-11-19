/**
 * What a signed-in user may be allowed to do in a landscape's dashboard (authorization-plan.md, decision 5). A route,
 * or a choice within a request, needs exactly one action; a role grants it through one of its policies.
 */
export const ACTIONS = [
  /** Read the landscape's runs, status, landscapes, Bulk sync's state and a mock landscape's last reset */
  'runs:read',
  /** Look a person up in the source and the target (a read of personal data) */
  'person:lookup',
  /** Preview what a sync of one person would send */
  'person:preview',
  /** Sync one person into the target */
  'person:sync',
  /** Review and start a sync run (Bulk sync) */
  'bulk:start',
  /** Empty a mock landscape's run state: Overview's Reset landscape, or a start with Start from scratch */
  'mock:reset',
  /** Start a run with Processor tasks above the landscape's maximum */
  'bulk:processors',
  /** Read and change who holds which roles */
  'access:admin',
  /** See the dashboard through another person's roles (decision 14) */
  'access:impersonate',
  /** Read the audit trail */
  'audit:read'
] as const;

export type Action = typeof ACTIONS[number];

export const isAction = (value: unknown): value is Action => (ACTIONS as readonly string[]).includes(value as string);
