import { Action, ACTIONS } from './Actions';
import { Policy } from './Policy';

/** A named set of policies (authorization-plan.md, decision 5): it grants an action when any of its policies does */
export class Role {
  constructor(readonly name: string, readonly description: string, readonly policies: Policy[]) {}

  /** The policy that grants the action, if any (the first, in the role's order) */
  allows(action: Action): Policy | undefined {
    return this.policies.find(policy => policy.allows(action));
  }

  /** Every action the role grants, in the order ACTIONS lists them */
  actions(): Action[] {
    return ACTIONS.filter(action => !!this.allows(action));
  }
}
