import { Action } from './Actions';

/** A named set of actions (authorization-plan.md, decision 5). A role is a set of policies. */
export class Policy {
  readonly actions: ReadonlySet<Action>;

  constructor(readonly name: string, readonly description: string, actions: Action[]) {
    this.actions = new Set(actions);
  }

  allows(action: Action): boolean {
    return this.actions.has(action);
  }
}
