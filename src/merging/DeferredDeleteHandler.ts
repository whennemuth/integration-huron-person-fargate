import { S3 } from '@aws-sdk/client-s3';
import { FieldSet, TestEnvironment } from "integration-core";
import { BasicCache, Cache, Config, ConfigManager, FieldDefinitions, HuronPersonDataTarget, ReadPerson, TargetPersonDeleteType } from "integration-huron-person";
import * as readline from 'readline';
import { Readable } from 'stream';
import { TrackingTargetApiErrorProcessor } from "../processing/ApiErrorTracking";
import { getLocalConfig } from "../Utils";

export type DeferredDeleteHandlerParams = {
  bucketName: string;
  region?: string;
  mergedNdjsonPath: string;      // Consolidated output from all chunks (current source state)
  baselineNdjsonPath: string;    // Shared delta-storage/previous-input.ndjson (previous run state)
  primaryKeyFieldNames: string[]; // Field names to use for identifying same record across datasets
  config: Config;                // Configuration for PersonDataTarget initialization
  cache: Cache<string, string>;  // JWT token cache
  errorTracker: TrackingTargetApiErrorProcessor;
};

export type DeferredDeleteResult = {
  deletedCount: number;
  failedCount: number;
  totalProcessed: number;
  message: string;
};

/**
 * DeferredDeleteHandler performs soft or hard deletes AFTER all chunks have been processed and merged.
 * 
 * Problem: ChunkedDeltaStrategy filters out removals during chunk processing because missing records
 * are just in other chunks, not actually removed from source. But this also prevents VALID deletions.
 * 
 * Solution: After merger consolidates all chunks, compare consolidated vs baseline:
 * - Consolidated: Represents current state of source system (all chunks merged)
 * - Baseline: Represents previous run state (delta-storage/previous-input.ndjson)
 * - Removals: Records in baseline but NOT in consolidated = truly removed from source
 * 
 * Workflow:
 * 1. Read consolidated NDJSON file (all processed chunks merged together)
 * 2. Read baseline NDJSON file (previous run's complete dataset)
 * 3. Compare using primary key fields to find records in baseline but not in consolidated
 * 4. Soft-delete those records via PersonDataTarget (PATCH with active=false)
 * 5. Track success/failure via errorTracker
 * 
 * Note: The baseline file RETAINS all records (as per HashMapMerger logic) even after soft-delete.
 * This is correct - baseline represents "all records we've ever seen", not "currently active records".
 * Soft-deleting is idempotent (PATCH active=false), so re-deleting on subsequent runs is safe.
 */
export class DeferredDeleteHandler {
  private s3: S3;

  constructor(private params: DeferredDeleteHandlerParams) {
    this.s3 = new S3({ region: params.region });
  }

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
    const deleteType = DeferredDeleteHandler.getDeleteType();
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
   * Get the records that have been removed from the source.
   * @returns A promise that resolves to an array of removed records enriched with HRN.
   */
  public getRemovedRecords = async (): Promise<FieldSet[]> => {
    const { 
      params: { bucketName, mergedNdjsonPath, baselineNdjsonPath }, 
      readNdjsonFile, findRemovedRecords, enrichRemovedRecordsWithHrn
    } = this;

    // Step 1: Read consolidated file (current source state)
    console.log(`Reading consolidated file: s3://${bucketName}/${mergedNdjsonPath}`);
    const consolidated = await readNdjsonFile(mergedNdjsonPath);
    console.log(`  Parsed ${consolidated.length} records from consolidated file`);

    // Step 2: Read baseline file (previous run state)
    console.log(`Reading baseline file: s3://${bucketName}/${baselineNdjsonPath}`);
    const baseline = await readNdjsonFile(baselineNdjsonPath);
    console.log(`  Parsed ${baseline.length} records from baseline file`);

    // Step 3: Find records in baseline but NOT in consolidated (true removals)
    const removedRecords = findRemovedRecords(baseline, consolidated);

    // Step 4: Enrich removed records with HRN if missing (lookup via sourceIdentifier)
    const enrichedRecords = await enrichRemovedRecordsWithHrn(removedRecords);

    return enrichedRecords;
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
    const { getRemovedRecords, params: { 
      config, cache, errorTracker: errorEventProcessor 
    }} = this;
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

      // Soft-delete via PersonDataTarget
      const dataTarget = new HuronPersonDataTarget({ config, cache, errorEventProcessor });

      console.log('Starting batch soft-delete operation...');
      const batchResult = await dataTarget.pushAll({
        added: [],
        updated: [],
        removed: removedRecords
      });

      const successCount = batchResult.successes?.length || 0;
      const failureCount = batchResult.failures?.length || 0;

      console.log(`✓ Soft-delete completed: ${successCount} successes, ${failureCount} failures`);

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
   * Hard deletes are not yet implemented.
   * This is a placeholder for future functionality if needed.
   */
  private handleHardDeletes = async (): Promise<DeferredDeleteResult> => {
    throw new Error('Hard deletes are not implemented. Use soft deletes instead.');
  }

  /**
   * Read and parse NDJSON file from S3.
   * Returns array of FieldSets.
   */
  private readNdjsonFile = async (key: string): Promise<FieldSet[]> => {
    const { params: { bucketName }, s3 } = this;
    try {
      const response = await s3.getObject({
        Bucket: bucketName,
        Key: key
      });

      const fieldSets: FieldSet[] = [];
      const stream = response.Body as Readable;
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (line.trim()) {
          try {
            const fieldSet = JSON.parse(line) as FieldSet;
            fieldSets.push(fieldSet);
          } catch (parseError: any) {
            throw new Error(`Failed to parse NDJSON line: ${parseError.message}`);
          }
        }
      }

      return fieldSets;

    } catch (error: any) {
      if (error.name === 'NoSuchKey') {
        console.warn(`  File not found: s3://${bucketName}/${key}`);
        return [];
      }
      throw new Error(`Failed to read NDJSON file ${key}: ${error.message}`);
    }
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
  private enrichRemovedRecordsWithHrn = async (removedRecords: FieldSet[]): Promise<FieldSet[]> => {
    if (removedRecords.length === 0) {
      return removedRecords;
    }

    console.log(`\nEnriching ${removedRecords.length} removed record(s) with HRN...`);
    const { config } = this.params;
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
  private findRemovedRecords = (baseline: FieldSet[], consolidated: FieldSet[]): FieldSet[] => {
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
  private extractPrimaryKeyValue = (fieldSet: FieldSet, primaryKeyFieldNames: string[]): string | null => {
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
   * Get an instance of DeferredDeleteHandler.
   * @param params {
   *   @param region: AWS region (e.g., 'us-east-2')
   *   @param bucketName: S3 bucket name
   *   @param sourceKey: S3 key for the source NDJSON file
   *   @param targetKey: S3 key for the target NDJSON file
   *   @param primaryKeyFieldNames: Array of primary key field names
   * }
   * @returns A promise that resolves to an instance of DeferredDeleteHandler or undefined if initialization fails.
   */
  public static getInstance = async (params: { 
    region: string | undefined, 
    bucketName: string, 
    sourceKey: string, 
    targetKey: string, 
    primaryKeyFieldNames: string[] 
  }): Promise<DeferredDeleteHandler | undefined> => {
    const { region, bucketName, sourceKey, targetKey, primaryKeyFieldNames } = params;
    // Load config for PersonDataTarget initialization
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

    // Create cache for JWT tokens
    const cache = BasicCache.getInstance(config);
    
    // Cache is required for PersonDataTarget API calls
    if (!cache) {
      console.warn('  Cache not available - skipping deletion processing');
      console.warn('  Set CACHE_ENABLED=true and configure CACHE_PATH to enable deletions');
      return undefined; // Exit deletion processing but continue with merge
    }

    // Create error tracker for deletion operations
    const errorTracker = new TrackingTargetApiErrorProcessor({
      tableName: process.env.DYNAMODB_STATISTICS_TABLE_NAME || '',
      integrationTimestamp: new Date().toISOString(),
      region,
      logToConsole: true
    });

    // Create and run DeferredDeleteHandler
    return new DeferredDeleteHandler({
      bucketName,
      region,
      mergedNdjsonPath: sourceKey,          // Consolidated chunks (current source state)
      baselineNdjsonPath: targetKey,        // Existing baseline (previous run state)
      primaryKeyFieldNames,                 // For identifying same records
      config,
      cache,
      errorTracker
    });
  }
}


if(require.main === module) {
  (async () => {
    const testEnvironment = TestEnvironment('DEFERRED_DELETE');
    [
      'CHUNKS_BUCKET',
      'REGION',
      'MERGED_NDJSON_KEY',
      'BASELINE_NDJSON_KEY',
      'PERSON_DELETE_TYPE',
      'HURON_PERSON_CONFIG_PATH',
      'SECRET_ARN',
      'HURON_PERSON_CONFIG_JSON',
      'DYNAMODB_STATISTICS_TABLE_NAME',
      'CACHE_ENABLED',
      'CACHE_PATH'
    ].forEach(testEnvironment.getVar);

    const { 
      CHUNKS_BUCKET:bucketName, REGION:region, MERGED_NDJSON_KEY: 
      sourceKey, BASELINE_NDJSON_KEY: targetKey  
    } = process.env;

    if(!bucketName) {
      console.error('Missing bucket name in configuration!');
      process.exit(1);
    }
    if(!sourceKey) {
      console.error('Missing source key in configuration!');
      process.exit(1);
    }
    if(!targetKey) {
      console.error('Missing target key in configuration!');
      process.exit(1);
    }

    const primaryKeyFieldNames = FieldDefinitions.filter(fd => fd.isPrimaryKey).map(fd => fd.name);
    const handler = await DeferredDeleteHandler.getInstance({
      region, bucketName, sourceKey, targetKey, primaryKeyFieldNames
    });

    if (!handler) {
      console.error('DeferredDeleteHandler instance could not be created. Skipping deletion processing.');
      process.exit(1);
    }

    const result = await handler.processDeletes();
    console.log(`\nDeletion processing result: ${result.message}`);
  })();
}