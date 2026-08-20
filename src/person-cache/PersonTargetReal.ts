import { Config, HuronPerson, ListPeople } from "integration-huron-person";

export type AbstractPersonTarget = {
  getFullPopulationFromTarget(config?: Config): Promise<HuronPerson[]>;
}

/**
 * Fetch full population from real Huron API.
 * Uses ListPeople to query all person records.
 */
export class PersonTargetReal implements AbstractPersonTarget {
  
  constructor() { }

  async getFullPopulationFromTarget(config?: Config): Promise<HuronPerson[]> {
    if (!config) {
      throw new Error('Config is required for PersonTargetReal');
    }
    const listPeople = new ListPeople(config, 500);
    return listPeople.listSourceIdentifiers();
  }
}