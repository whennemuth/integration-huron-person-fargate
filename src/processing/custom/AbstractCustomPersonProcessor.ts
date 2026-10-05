import { PersonRecordProcessor } from 'integration-huron-person';
import { PersonRecordProcessorLogTable } from '../../dynamodb/PersonRecordProcessorLogTable';

export const PERSON_RECORD_PROCESSOR_LOG_TABLE_ENV_VARIABLE = 'PERSON_RECORD_PROCESSOR_LOG_TABLE_NAME';

export enum Customization {
  ORG_COMPARISON_LOGGING
  // 1) Add more enum values here as needed and for each new value:
  // 2) Add to reflect this in the switch statement in src\processing\custom\PersonRecordProcessorFactory.ts,
  // 3) Create a corresponding AbstractPersonRecordProcessor in the impl subdirectory.
}

/**
 * Base class for all personRecordProcessor customizations. `processRecord` IS a
 * PersonRecordProcessor (not a function that takes one), so it can be handed directly to
 * HuronPersonIntegration's personRecordProcessor param. `customization` identifies which
 * customization(s) an instance represents - a single value for concrete customizations, or an
 * array for a PersonRecordProcessorComposite wrapping several (see PersonRecordProcessorComposite.ts).
 *
 * NOTE: concrete customizations (e.g. OrgComparisonLoggingPersonProcessor) live in their own
 * files that import this base class - the factory/registry that imports those concrete classes
 * lives in PersonRecordProcessorFactory.ts (not here), to avoid a circular import.
 */
export abstract class AbstractPersonRecordProcessor {
  abstract readonly customization: Customization | Customization[];
  abstract processRecord: PersonRecordProcessor;

  /**
   * customization is passed explicitly (rather than read off `this.customization`) so this
   * works unambiguously even though `this.customization` is a single value for concrete
   * subclasses but an array for PersonRecordProcessorComposite (which never calls this itself).
   */
  protected logEntry = async (customization: Customization, personid: string, data: Record<string, any>): Promise<void> => {
    const tableName = process.env[PERSON_RECORD_PROCESSOR_LOG_TABLE_ENV_VARIABLE];
    if (!tableName) {
      console.warn(`${PERSON_RECORD_PROCESSOR_LOG_TABLE_ENV_VARIABLE} not configured - skipping log entry for customization ${customization}`);
      return;
    }
    const table = PersonRecordProcessorLogTable.fromTableName(tableName, process.env.REGION);
    await table.putEntry(Customization[customization], personid, data);
  }
}

