import { BasicPushAllOperation, FieldSet, TestEnvironment } from "integration-core";
import { BasicCache, Cache, Config, ConfigManager, DataTargetFactory, FieldDefinitions, ReadPerson, TargetPersonDeleteType } from "integration-huron-person";
import { TrackingTargetApiErrorProcessor } from "../ApiErrorTracking";
import { getLocalConfig } from "../Utils";

export type DeferredDeleteHandlerCoreParams = {
  region?: string;
  primaryKeyFieldNames: string[]; // Field names to use for identifying same record across datasets
  config?: Config;                // Configuration for PersonDataTarget initialization
  cache?: Cache<string, string>;  // JWT token cache
  errorTracker?: TrackingTargetApiErrorProcessor;
  useMockTarget?: boolean;        // If true, soft-deletes go to MockPersonDataTarget instead of the real Huron API
};

export type DeferredDeleteResult = {
  deletedCount: number;
  failedCount: number;
  totalProcessed: number;
  message: string;
};

export abstract class AbstractDeferredDeleteHandler {
  protected params: DeferredDeleteHandlerCoreParams;

  constructor(params: DeferredDeleteHandlerCoreParams) {
    this.params = params;
  }

  /**
   * Get the records that have been removed from the source.
   * @returns A promise that resolves to an array of removed records enriched with HRN.
   */
  public abstract getRemovedRecords(): Promise<FieldSet[]>;

  /**
   * Get the configured delete type from environment variable.
   * Defaults to SOFT if not specified.
   */
  public static getDeleteType(): TargetPersonDeleteType {
    const { PERSON_DELETE_TYPE } = process.env;
    if (!PERSON_DELETE_TYPE) {
      return TargetPersonDeleteType.SOFT; // Default
    }
    // Validate and return
    const deleteType = PERSON_DELETE_TYPE.toLowerCase();
    if (deleteType === TargetPersonDeleteType.SOFT) return TargetPersonDeleteType.SOFT;
    if (deleteType === TargetPersonDeleteType.HARD) return TargetPersonDeleteType.HARD;
    if (deleteType === TargetPersonDeleteType.LOG) return TargetPersonDeleteType.LOG;
    if (deleteType === TargetPersonDeleteType.NONE) return TargetPersonDeleteType.NONE;
    
    console.warn(`Invalid PERSON_DELETE_TYPE: ${PERSON_DELETE_TYPE}. Defaulting to SOFT.`);
    return TargetPersonDeleteType.SOFT;
  }

  /**
   * Check if deletion handling is enabled (not NONE).
   */
  public static isConfiguredForDeletes(): boolean {
    return this.getDeleteType() !== TargetPersonDeleteType.NONE;
  }

  /**
   * Main entry point for processing deletions.
   * Routes to appropriate handler based on configured delete type.
   */
  public async processDeletes(): Promise<DeferredDeleteResult> {
    const { SOFT, HARD, LOG, NONE } = TargetPersonDeleteType;
    const deleteType = AbstractDeferredDeleteHandler.getDeleteType();
    console.log(`\nProcessing deletions with type: ${deleteType}`);
    
    switch(deleteType) {
      case SOFT:
        return await this.handleSoftDeletes();
      case HARD:
        return await this.handleHardDeletes();
      case LOG: case NONE:
        if(deleteType === LOG) {
          const removedRecords = await this.getRemovedRecords();
          this.logRemovedRecords(removedRecords);
        }
        console.log('Deletion handling is disabled (TargetPersonDeleteType.NONE). No deletes will be processed.');
        return {
          deletedCount: 0,
          failedCount: 0,
          totalProcessed: 0,
          message: 'Deletion handling disabled'
        };
      default:
        console.warn(`Unknown TargetPersonDeleteType: ${deleteType}. No deletes will be processed.`);
        return {
          deletedCount: 0,
          failedCount: 0,
          totalProcessed: 0,
          message: `Unknown delete type: ${deleteType}`
        };
    }
  }

  /**
   * Log the removed records for debugging and verification purposes.
   * @param removedRecords An array of removed records to be logged.
   */
  private logRemovedRecords = (removedRecords: FieldSet[]): void => {
    console.log(`\nLogging ${removedRecords.length} removed record(s):`);
    for (const record of removedRecords) {
      const pkeyValues = this.params.primaryKeyFieldNames.map(fieldName => {
        const fieldValue = record.fieldValues.find((fv: any) => fieldName in fv);
        return fieldValue ? `${fieldName}=${fieldValue[fieldName]}` : `${fieldName}=undefined`;
      }).join(', ');
      console.log(`  - ${pkeyValues}`);
    }
  }

  /**
   * Perform soft deletes (PATCH with active=false) for records removed from source.
   */
  private handleSoftDeletes = async (): Promise<DeferredDeleteResult> => {
    const { getRemovedRecords } = this;
    try {
      // Find records in baseline but NOT in consolidated (true removals)
      const removedRecords = await getRemovedRecords();
      
      if (removedRecords.length === 0) {
        console.log('\nNo records to soft-delete (no removals detected)');
        return {
          deletedCount: 0,
          failedCount: 0,
          totalProcessed: 0,
          message: 'No removals detected'
        };
      }

      console.log(`\nIdentified ${removedRecords.length} record(s) for soft deletion`);

      // Select real vs mock target based on useMockTarget flag, so mocked runs never hit the real Huron API
      const config = await this.getConfig();
      const cache = await this.getCache();
      const errorEventProcessor = await this.getErrorTracker();
      const targetFactory = new DataTargetFactory({
        config,
        flags: { useMockTarget: this.params.useMockTarget },
        cache,
        errorEventProcessor
      });
      const dataTarget = targetFactory.create();

      console.log('Starting batch soft-delete operation...');
      const batchResult = dataTarget.pushAll
        ? await dataTarget.pushAll({ added: [], updated: [], removed: removedRecords })
        : await BasicPushAllOperation({
            all: { added: [], updated: [], removed: removedRecords },
            pusher: dataTarget
          }).push();

      const successCount = batchResult.successes?.length || 0;
      const failureCount = batchResult.failures?.length || 0;

      console.log(`✓ Soft-delete completed: ${successCount} successes, ${failureCount} failures`);

      const { primaryKeyFieldNames } = this.params;
      const successKeys = new Set(
        (batchResult.successes || [])
          .map(s => this.extractPrimaryKeyValue({ fieldValues: s.primaryKey } as FieldSet, primaryKeyFieldNames))
          .filter((k): k is string => !!k)
      );
      const successfulRecords = removedRecords.filter(r => {
        const key = this.extractPrimaryKeyValue(r, primaryKeyFieldNames);
        return key !== null && successKeys.has(key);
      });
      await this.onSoftDeleteSuccess(successfulRecords);

      return {
        deletedCount: successCount,
        failedCount: failureCount,
        totalProcessed: removedRecords.length,
        message: `Soft-deleted ${successCount} of ${removedRecords.length} records`
      };

    } catch (error: any) {
      console.error(`Failed to process soft deletes: ${error.message}`);
      console.error(error.stack);
      throw error;
    }
  }

  /**
   * Hook called with the subset of removed records that were successfully soft-deleted.
   * No-op by default; subclasses can override to record additional bookkeeping (e.g. an audit
   * trail entry) that only that storage mode needs.
   */
  protected async onSoftDeleteSuccess(successfulRecords: FieldSet[]): Promise<void> {
    // No-op by default
  }

  /**
   * Hard deletes are not yet implemented.
   * This is a placeholder for future functionality if needed.
   */
  private handleHardDeletes = async (): Promise<DeferredDeleteResult> => {
    throw new Error('Hard deletes are not implemented. Use soft deletes instead.');
  }

  /**
   * Enrich removed records with HRN by looking up via sourceIdentifier.
   * 
   * At merge time, records only contain sourceIdentifier, not HRN. But soft-delete requires HRN.
   * For each record missing HRN, look it up from the target API using sourceIdentifier.
   * 
   * @param removedRecords Records identified as removed (may lack HRN)
   * @returns Records enriched with HRN where found
   */
  protected enrichRemovedRecordsWithHrn = async (removedRecords: FieldSet[]): Promise<FieldSet[]> => {
    if (removedRecords.length === 0) {
      return removedRecords;
    }

    console.log(`\nEnriching ${removedRecords.length} removed record(s) with HRN...`);
    const config = await this.getConfig();
    const reader = new ReadPerson({ config });
    const enrichedRecords: FieldSet[] = [];
    let enrichedCount = 0;
    let failedCount = 0;

    for (const record of removedRecords) {
      // Check if HRN already exists
      const existingHrn = record.fieldValues.find((fv: any) => fv.hrn)?.hrn;
      
      if (existingHrn) {
        // Already has HRN, no lookup needed
        enrichedRecords.push(record);
        continue;
      }

      // No HRN - try to look up via sourceIdentifier
      const sourceIdentifier = record.fieldValues.find((fv: any) => fv.sourceIdentifier)?.sourceIdentifier;
      
      if (!sourceIdentifier || typeof sourceIdentifier !== 'string') {
        console.warn(`  ⚠ Record missing both HRN and sourceIdentifier - cannot enrich:`, 
          JSON.stringify(record.fieldValues));
        failedCount++;
        // Still include the record - PersonDataTarget will handle the error
        enrichedRecords.push(record);
        continue;
      }

      try {
        // Look up person by sourceIdentifier to get HRN
        const persons = await reader.readPersonBySourceIdentifier(sourceIdentifier, ['hrn']);
        
        if (persons.length === 0) {
          console.warn(`  ⚠ No person found for sourceIdentifier=${sourceIdentifier}`);
          failedCount++;
          enrichedRecords.push(record);
          continue;
        }

        if (persons.length > 1) {
          console.warn(`  ⚠ Multiple persons found for sourceIdentifier=${sourceIdentifier}, using first`);
        }

        const hrn = persons[0].hrn;
        if (!hrn) {
          console.warn(`  ⚠ Person found but HRN is missing for sourceIdentifier=${sourceIdentifier}`);
          failedCount++;
          enrichedRecords.push(record);
          continue;
        }

        // Add HRN to record's fieldValues
        const enrichedRecord = {
          ...record,
          fieldValues: [...record.fieldValues, { hrn }]
        };
        enrichedRecords.push(enrichedRecord);
        enrichedCount++;
        console.log(`  ✓ Enriched sourceIdentifier=${sourceIdentifier} with hrn=${hrn}`);

      } catch (error: any) {
        console.error(`  ✗ Failed to look up HRN for sourceIdentifier=${sourceIdentifier}: ${error.message}`);
        failedCount++;
        // Include the record anyway - PersonDataTarget will handle the error
        enrichedRecords.push(record);
      }
    }

    console.log(`  Enrichment complete: ${enrichedCount} enriched, ${failedCount} failed`);
    return enrichedRecords;
  }

  /**
   * Find records that exist in baseline but NOT in consolidated.
   * These are records that have been removed from the source system.
   * 
   * Comparison is done using primary key fields to identify the "same" record.
   */
  protected findRemovedRecords = (baseline: FieldSet[], consolidated: FieldSet[]): FieldSet[] => {
    const { params: { primaryKeyFieldNames }, extractPrimaryKeyValue } = this;

    // Build a set of primary key combinations from consolidated for efficient lookup
    const consolidatedKeys = new Set<string>();
    for (const fieldSet of consolidated) {
      const pkeyValue = extractPrimaryKeyValue(fieldSet, primaryKeyFieldNames);
      if (pkeyValue) {
        consolidatedKeys.add(pkeyValue);
      }
    }

    // Find baseline records whose primary key is NOT in consolidated
    const removedRecords: FieldSet[] = [];
    for (const fieldSet of baseline) {
      const pkeyValue = extractPrimaryKeyValue(fieldSet, primaryKeyFieldNames);
      if (pkeyValue && !consolidatedKeys.has(pkeyValue)) {
        removedRecords.push(fieldSet);
      }
    }

    return removedRecords;
  }

  /**
   * Extract primary key value(s) from a FieldSet as a composite string.
   * Returns null if any primary key field is missing or undefined.
   * 
   * Example: If primaryKeyFieldNames = ['sourceIdentifier'], returns 'U12345678'
   * Example: If primaryKeyFieldNames = ['firstName', 'lastName'], returns 'John|Doe'
   */
  protected extractPrimaryKeyValue = (fieldSet: FieldSet, primaryKeyFieldNames: string[]): string | null => {
    const values: string[] = [];
    
    for (const fieldName of primaryKeyFieldNames) {
      // Find the field value object containing this field name
      const fieldValue = fieldSet.fieldValues.find((fv: any) => fieldName in fv);
      
      if (!fieldValue || fieldValue[fieldName] === undefined || fieldValue[fieldName] === null) {
        return null; // Missing or null primary key - can't compare
      }
      
      values.push(String(fieldValue[fieldName]));
    }
    
    return values.join('|'); // Composite key
  }

  /**
   * Get the configuration object for the Huron Person integration.
   * @returns The configuration object for the Huron Person integration.
   */
  public getConfig = async (): Promise<Config> => {
    if(this.params.config) {
      return this.params.config;
    }
    const { HURON_PERSON_CONFIG_PATH, SECRET_ARN } = process.env;
    const configManager = ConfigManager.getInstance();
    const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
    const config = await configManager
      .reset()
      .fromJsonString('HURON_PERSON_CONFIG_JSON')
      .fromSecretManager(SECRET_ARN)
      .fromEnvironment()
      .fromFileSystem(localConfigPath)
      .getConfigAsync('people');
    this.params.config = config;
    return config;
  }

  /**
   * Get a cache instance for the Huron Person integration, or undefined if the cache is not 
   * available.
   * @returns The cache instance for the Huron Person integration.
   */
  public getCache = async (): Promise<Cache<string, string> | undefined> => {
    if(this.params.cache) {
      return this.params.cache;
    }
    const config = await this.getConfig();
    const cache = BasicCache.getInstance(config);
    // Cache is required for PersonDataTarget API calls
    if (!cache) {
      console.warn('  Cache not available - skipping deletion processing');
      console.warn('  Set CACHE_ENABLED=true and configure CACHE_PATH to enable deletions');
      return undefined;
    }
    this.params.cache = cache;
    return cache;
  }

  /**
   * Create error tracker for deletion operations. Redirected to the isolated mock statistics 
   * table when useMockTarget is true, so bulk STATISTICS/ERROR records never mix with 
   * production data.
   */
  public getErrorTracker = async (): Promise<TrackingTargetApiErrorProcessor | undefined> => {
    const { useMockTarget, region } = this.params;
    const statisticsTableName = useMockTarget
      ? (process.env.DYNAMODB_MOCK_STATISTICS_TABLE_NAME || '')
      : (process.env.DYNAMODB_STATISTICS_TABLE_NAME || '');
    const errorTracker = new TrackingTargetApiErrorProcessor({
      tableName: statisticsTableName,
      integrationTimestamp: new Date().toISOString(),
      region,
      logToConsole: true
    });
    return errorTracker;
  }
}
