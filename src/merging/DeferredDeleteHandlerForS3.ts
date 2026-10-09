import { S3 } from '@aws-sdk/client-s3';
import { FieldSet, TestEnvironment } from "integration-core";
import readline from "readline";
import { Readable } from "stream";
import { AbstractDeferredDeleteHandler, DeferredDeleteHandlerCoreParams } from "./AbstractDeferredDeleteHandler";
import { isDeletedHash, toDeletedHash } from "./DeletedHashMarker";
import { BasicCache, Config, FieldDefinitions } from 'integration-huron-person';

export type DeferredDeleteHandlerForS3Params = DeferredDeleteHandlerCoreParams & {
  bucketName: string;
  mergedNdjsonPath: string;      // Consolidated output from all chunks (current source state)
  baselineNdjsonPath: string;    // Shared delta-storage/previous-input.ndjson (previous run state)
  primaryKeyFieldNames: string[];
  config: Config;
  cache: BasicCache;
};

/**
 * This class facilitates the template design pattern.
 * TODO: Put a more detailed description of the class and its responsibilities here.
 */
export class DeferredDeleteHandlerForS3 extends AbstractDeferredDeleteHandler {
  private s3: S3;

  constructor(params: DeferredDeleteHandlerForS3Params) {
    super(params);
    this.s3 = new S3({});
  }

  /**
   * Read and parse NDJSON file from S3.
   * Returns array of FieldSets.
   */
  private readNdjsonFile = async (key: string): Promise<FieldSet[]> => {
    const { s3 } = this;
    const { bucketName } = this.params as DeferredDeleteHandlerForS3Params;

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

  public async getRemovedRecords(): Promise<FieldSet[]> {
    const { 
      readNdjsonFile, findRemovedRecords, enrichRemovedRecordsWithHrn
    } = this;

    const { bucketName, mergedNdjsonPath, baselineNdjsonPath } = this.params as DeferredDeleteHandlerForS3Params;

    // Step 1: Read consolidated file (current source state)
    console.log(`Reading consolidated file: s3://${bucketName}/${mergedNdjsonPath}`);
    const consolidated = await readNdjsonFile(mergedNdjsonPath);
    console.log(`  Parsed ${consolidated.length} records from consolidated file`);

    // Step 2: Read baseline file (previous run state)
    console.log(`Reading baseline file: s3://${bucketName}/${baselineNdjsonPath}`);
    const fullBaseline = await readNdjsonFile(baselineNdjsonPath);
    console.log(`  Parsed ${fullBaseline.length} records from baseline file`);

    // Persons already soft-deleted by a previous run are not deletion candidates again.
    const baseline = fullBaseline.filter(fs => !isDeletedHash(fs.hash));
    const alreadyDeletedCount = fullBaseline.length - baseline.length;
    if (alreadyDeletedCount > 0) {
      console.log(`  Excluded ${alreadyDeletedCount} already soft-deleted record(s) from baseline`);
    }

    // Step 3: Find records in baseline but NOT in consolidated (true removals)
    const removedRecords = findRemovedRecords(baseline, consolidated);

    // Step 4: Enrich removed records with HRN if missing (lookup via sourceIdentifier).
    const enrichedRecords = await enrichRemovedRecordsWithHrn(removedRecords);

    return enrichedRecords;
  }

  /**
   * Mark the soft-deleted persons in the baseline file so later runs don't select them for
   * deletion again (see DeletedHashMarker.ts). The baseline is rewritten in place: the merge step
   * has already written this run's merged result to it, and HashMapMerger retains baseline-only
   * records (including these) indefinitely.
   */
  protected onSoftDeleteSuccess = async (successfulRecords: FieldSet[]): Promise<void> => {
    if (successfulRecords.length === 0) {
      return;
    }

    const { readNdjsonFile, extractPrimaryKeyValue, s3 } = this;
    const { bucketName, baselineNdjsonPath, primaryKeyFieldNames } = this.params as DeferredDeleteHandlerForS3Params;

    const deletedKeys = new Set(
      successfulRecords
        .map(r => extractPrimaryKeyValue(r, primaryKeyFieldNames))
        .filter((k): k is string => !!k)
    );

    const baseline = await readNdjsonFile(baselineNdjsonPath);
    let markedCount = 0;
    const marked = baseline.map(fs => {
      const key = extractPrimaryKeyValue(fs, primaryKeyFieldNames);
      if (!fs.hash || !key || !deletedKeys.has(key)) {
        return fs;
      }
      markedCount++;
      return { ...fs, hash: toDeletedHash(fs.hash) };
    });

    console.log(`Marking ${markedCount} record(s) as soft-deleted in baseline file: s3://${bucketName}/${baselineNdjsonPath}`);
    await s3.putObject({
      Bucket: bucketName,
      Key: baselineNdjsonPath,
      Body: marked.map(fs => JSON.stringify(fs)).join('\n') + '\n',
      ContentType: 'application/x-ndjson'
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

    const handler = new DeferredDeleteHandlerForS3({
      bucketName, 
      primaryKeyFieldNames, 
      baselineNdjsonPath: targetKey,
      mergedNdjsonPath: sourceKey,
      region,
    } as DeferredDeleteHandlerForS3Params);


    if (!handler) {
      console.error('DeferredDeleteHandler instance could not be created. Skipping deletion processing.');
      process.exit(1);
    }

    const result = await handler.processDeletes();
    console.log(`\nDeletion processing result: ${result.message}`);
  })();
}