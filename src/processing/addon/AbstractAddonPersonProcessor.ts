import { PersonRecordProcessor } from 'integration-huron-person';
import { PersonRecordProcessorLogTable } from '../../dynamodb/PersonRecordProcessorLogTable';

export const PERSON_RECORD_PROCESSOR_LOG_TABLE_ENV_VARIABLE = 'PERSON_RECORD_PROCESSOR_LOG_TABLE_NAME';

export enum Addon {
  ORG_COMPARISON_LOGGING
  // 1) Add more enum values here as needed and for each new value:
  // 2) Add to reflect this in the switch statement in src\processing\addon\PersonRecordProcessorFactory.ts,
  // 3) Create a corresponding AbstractPersonRecordProcessor in the impl subdirectory.
}

/**
 * Base class for all personRecordProcessor addons. `processRecord` IS a
 * PersonRecordProcessor (not a function that takes one), so it can be handed directly to
 * HuronPersonIntegration's personRecordProcessor param. `addon` identifies which
 * addon(s) an instance represents - a single value for concrete addons, or an
 * array for a PersonRecordProcessorComposite wrapping several (see PersonRecordProcessorComposite.ts).
 *
 * NOTE: concrete addons (e.g. OrgComparisonLoggingPersonProcessor) live in their own
 * files that import this base class - the factory/registry that imports those concrete classes
 * lives in PersonRecordProcessorFactory.ts (not here), to avoid a circular import.
 */
export abstract class AbstractPersonRecordProcessor {
  abstract readonly addon: Addon | Addon[];
  abstract processRecord: PersonRecordProcessor;

  /**
   * addon is passed explicitly (rather than read off `this.addon`) so this
   * works unambiguously even though `this.addon` is a single value for concrete
   * subclasses but an array for PersonRecordProcessorComposite (which never calls this itself).
   */
  protected logEntry = async (addon: Addon, personid: string, data: Record<string, any>): Promise<void> => {
    const tableName = process.env[PERSON_RECORD_PROCESSOR_LOG_TABLE_ENV_VARIABLE];
    if (!tableName) {
      console.warn(`${PERSON_RECORD_PROCESSOR_LOG_TABLE_ENV_VARIABLE} not configured - skipping log entry for addon ${addon}`);
      return;
    }
    const table = PersonRecordProcessorLogTable.fromTableName(tableName, process.env.REGION);
    await table.putEntry(Addon[addon], personid, data);
  }
}

