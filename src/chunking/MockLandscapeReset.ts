import { DeleteObjectsCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { DescribeTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { IContext } from '../../context/IContext';
import { BulkPurger, BulkPurgerMode, BulkPurgerTables } from '../dynamodb/BulkPurger';
import { DYNAMODB_TABLE_NAME as personCurrentStateTableName } from '../dynamodb/PersonCurrentStateTable';
import { isMockLandscape, MOCK_LANDSCAPE_PATTERN } from '../Utils';

/** Where a mock landscape's run state is: its tables (named from the stack and landscape) and its chunks bucket */
export type MockLandscapeLocation = {
  stackId: string;
  region: string;
  landscape: string;
  /** The landscape's chunks bucket, whose delta-storage/ holds the S3 mode baseline */
  chunksBucketName: string;
};

/** The storage mode a stack is deployed in, which decides where its delta baseline is */
export type StorageMode = 'dynamodb' | 's3';

/** What a reset does to AWS, injectable for tests */
export type MockLandscapeResetDeps = {
  detectStorageMode: () => Promise<StorageMode>;
  truncateTables: (tables: BulkPurgerTables[]) => Promise<void>;
  /** @returns The number of objects deleted */
  deleteS3Baseline: () => Promise<number>;
};

export type MockLandscapeResetResult = {
  storageMode: StorageMode;
  tablesTruncated: BulkPurgerTables[];
  /** S3 mode only */
  baselineObjectsDeleted?: number;
};

/**
 * Resets a mock landscape's run state, so a run starts from scratch with the delta baseline and the mock target in
 * agreement. It truncates the mock target and statistics tables (BulkPurger, which refuses a non-mock landscape),
 * then clears the delta baseline of the stack's storage mode:
 * - DynamoDB mode: truncates the person current-state and person history tables.
 * - S3 mode: deletes every object under the shared delta storage dir (SHARED_DELTA_STORAGE_DIR) of the landscape's
 *   chunks bucket.
 * The mode is detected from the deployed stack rather than read from config: the person current-state table exists
 * only in DynamoDB mode (CDK creates it only then, and deletes it when the stack is switched to S3 mode).
 *
 * Shared by the manual Runner (MockLandscapeRunnerDecorator, MOCK_TARGET_RESET_STATE) and the chunker subscriber's
 * run start (ApiChunkerEvent.resetState, the dashboard's Bulk sync). Emptying the statistics table empties the
 * landscape's Run history too.
 */
export class MockLandscapeReset {
  /**
   * Shared delta storage dir of S3 mode. CDK leaves SHARED_DELTA_STORAGE_DIR unset (so the tasks use this default)
   * unless the integration config's storage.config has a keyPrefix, which a reset doesn't cover.
   */
  static readonly SHARED_DELTA_STORAGE_DIR = 'delta-storage';

  constructor(private readonly deps: MockLandscapeResetDeps) {}

  /** Why a landscape's run state can't be reset, or undefined when it can (only a mock landscape's) */
  static refusal(landscape: string | undefined): string | undefined {
    return isMockLandscape(landscape)
      ? undefined
      : `"${landscape}" is not a mock landscape (must match ${MOCK_LANDSCAPE_PATTERN}): only a mock landscape's run state can be reset`;
  }

  /** The tables a reset truncates in each storage mode */
  static tablesFor(storageMode: StorageMode): BulkPurgerTables[] {
    const { MOCK_TARGET_PERSON, PERSON_CURRENT_STATE, PERSON_HISTORY, STATISTICS } = BulkPurgerTables;
    return storageMode === 'dynamodb'
      ? [MOCK_TARGET_PERSON, PERSON_CURRENT_STATE, PERSON_HISTORY, STATISTICS]
      : [MOCK_TARGET_PERSON, STATISTICS];
  }

  /** The AWS operations for a landscape */
  static awsDeps(location: MockLandscapeLocation): MockLandscapeResetDeps {
    return {
      detectStorageMode: () => MockLandscapeReset.detectStorageMode(location),
      truncateTables: tables => MockLandscapeReset.truncateTables(location, tables),
      deleteS3Baseline: () => MockLandscapeReset.deleteS3Baseline(location.region, location.chunksBucketName),
    };
  }

  /** Resets the run state (see the class comment) */
  async run(): Promise<MockLandscapeResetResult> {
    const storageMode = await this.deps.detectStorageMode();
    console.log(`   🗑️  Resetting mock landscape run state (${storageMode} storage mode)...`);
    const tablesTruncated = MockLandscapeReset.tablesFor(storageMode);
    await this.deps.truncateTables(tablesTruncated);
    const baselineObjectsDeleted = storageMode === 's3' ? await this.deps.deleteS3Baseline() : undefined;
    console.log('   ✓ Mock landscape reset complete');
    return { storageMode, tablesTruncated, ...(baselineObjectsDeleted === undefined ? {} : { baselineObjectsDeleted }) };
  }

  /** The minimal context BulkPurger and the table names need */
  static contextFor({ stackId, region, landscape }: Pick<MockLandscapeLocation, 'stackId' | 'region' | 'landscape'>): IContext {
    return { STACK_ID: stackId, TAGS: { Landscape: landscape }, REGION: region } as IContext;
  }

  /** DynamoDB mode if the person current-state table exists, S3 mode otherwise */
  static async detectStorageMode(location: Pick<MockLandscapeLocation, 'stackId' | 'region' | 'landscape'>): Promise<StorageMode> {
    const client = new DynamoDBClient({ region: location.region });
    try {
      await client.send(new DescribeTableCommand({ TableName: personCurrentStateTableName(MockLandscapeReset.contextFor(location)) }));
      return 'dynamodb';
    } catch (error: any) {
      if (error.name === 'ResourceNotFoundException') {
        return 's3';
      }
      throw error;
    }
  }

  /** Truncates the given tables of the landscape (BulkPurger refuses a non-mock landscape) */
  static async truncateTables(location: Pick<MockLandscapeLocation, 'stackId' | 'region' | 'landscape'>, tables: BulkPurgerTables[]): Promise<void> {
    const context = MockLandscapeReset.contextFor(location);
    await new BulkPurger({ mode: BulkPurgerMode.TRUNCATE, tables, context, dryRun: false }).purge();
  }

  /**
   * Deletes the S3 mode delta baseline: every object under the shared delta storage dir of the chunks bucket.
   * @returns The number of objects deleted
   */
  static async deleteS3Baseline(region: string, bucketName: string): Promise<number> {
    const prefix = `${MockLandscapeReset.SHARED_DELTA_STORAGE_DIR}/`;
    const s3 = new S3Client({ region });

    let deleted = 0;
    let continuationToken: string | undefined;
    do {
      const listed = await s3.send(new ListObjectsV2Command({
        Bucket: bucketName, Prefix: prefix, ContinuationToken: continuationToken
      }));
      const keys = (listed.Contents ?? []).map(({ Key }) => Key!).filter(Boolean);
      if (keys.length > 0) {
        // A listing page holds at most 1000 keys, the DeleteObjects limit
        await s3.send(new DeleteObjectsCommand({
          Bucket: bucketName,
          Delete: { Objects: keys.map(Key => ({ Key })), Quiet: true }
        }));
        deleted += keys.length;
      }
      continuationToken = listed.IsTruncated ? listed.NextContinuationToken : undefined;
    } while (continuationToken);

    console.log(`   Deleted ${deleted} delta baseline object(s) from s3://${bucketName}/${prefix}`);
    return deleted;
  }
}
