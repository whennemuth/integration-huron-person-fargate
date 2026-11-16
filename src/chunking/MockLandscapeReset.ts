import { DeleteObjectsCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { DescribeTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { IContext } from '../../context/IContext';
import { BulkPurger, BulkPurgerMode, BulkPurgerTables, TableTruncation } from '../dynamodb/BulkPurger';
import {
  DYNAMODB_PARTITION_KEY as atomicCounterKey, DYNAMODB_TABLE_NAME as atomicCounterTableName, NOT_A_COUNTER_PREFIX
} from '../dynamodb/AtomicCounter';
import type { StartedBy } from './StartedBy';
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
  /** @returns What each table's truncation deleted, and how long it took */
  truncateTables: (tables: BulkPurgerTables[]) => Promise<TableTruncation[] | void>;
  /** @returns The number of objects deleted */
  deleteS3Baseline: () => Promise<number>;
};

export type MockLandscapeResetResult = {
  storageMode: StorageMode;
  tablesTruncated: BulkPurgerTables[];
  /** Each table's count and time */
  tables: TableTruncation[];
  /** S3 mode only */
  baselineObjectsDeleted?: number;
  durationMs: number;
};

/** How a reset was started: Overview's Reset landscape, or Bulk sync's Start from scratch (a run's resetState) */
export type MockLandscapeResetVia = 'overview' | 'bulk-sync';

/**
 * A reset's record (the item MockLandscapeReset.RECORD_NAME, not a counter, borrowing the landscape's atomic counter
 * table because no reset empties it; its `purpose` attribute says so):
 * written when it starts and again when it ends, so the dashboard can say what the last reset did and how long each
 * table took. One that starts and never ends (its Lambda timed out) stays `running`: statusOf() calls it stopped.
 */
export type MockLandscapeResetRecord = {
  status: 'running' | 'succeeded' | 'failed';
  via: MockLandscapeResetVia;
  startedAt: string;
  finishedAt?: string;
  durationMs?: number;
  startedBy?: StartedBy;
  storageMode?: StorageMode;
  tables?: TableTruncation[];
  baselineObjectsDeleted?: number;
  error?: string;
};

/** Writes a reset's record; recording never fails the reset */
export type MockLandscapeResetRecorder = (record: MockLandscapeResetRecord) => Promise<void>;

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
 * Shared by the manual Runner (MockLandscapeRunnerDecorator, MOCK_TARGET_RESET_STATE) and the chunker subscriber: a
 * run's start (ApiChunkerEvent.resetState, Bulk sync's Start from scratch), or a reset on its own (a
 * MockLandscapeResetEvent, Overview's Reset landscape). The subscriber records each reset (MockLandscapeResetRecord) in
 * the atomic counter table. The tables are truncated at the same time, each with a parallel scan
 * (DynamoDBTable.truncateTable()). Emptying the statistics table empties the landscape's Run history too.
 */
export class MockLandscapeReset {
  /**
   * Shared delta storage dir of S3 mode. CDK leaves SHARED_DELTA_STORAGE_DIR unset (so the tasks use this default)
   * unless the integration config's storage.config has a keyPrefix, which a reset doesn't cover.
   */
  static readonly SHARED_DELTA_STORAGE_DIR = 'delta-storage';

  /**
   * The atomic counter table item that holds the last reset's record (MockLandscapeResetRecord): not a counter, which
   * its key says (NOT_A_COUNTER_PREFIX) and its `purpose` attribute explains
   */
  static readonly RECORD_NAME = `${NOT_A_COUNTER_PREFIX}mock-landscape-reset`;
  /** Written with the record, for anyone who comes across the item in the atomic counter table */
  static readonly RECORD_PURPOSE = "Not a counter: the last mock landscape reset's record, kept in this table because no reset empties it (src/chunking/MockLandscapeReset.ts)";
  /**
   * A reset still `running` this long after it started has stopped: no Lambda runs longer (15 minutes, its maximum),
   * so it ran out of time or failed without recording it
   */
  static readonly STOPPED_AFTER_MS = 15 * 60_000;

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

  /**
   * Resets the run state (see the class comment). With a recorder, records the reset when it starts and when it
   * ends, succeeded or failed (a failure is then rethrown).
   */
  async run(options: { via?: MockLandscapeResetVia; startedBy?: StartedBy; recorder?: MockLandscapeResetRecorder; now?: () => number } = {}): Promise<MockLandscapeResetResult> {
    const { via = 'overview', startedBy, recorder, now = Date.now } = options;
    const started = now();
    const base = { via, startedAt: new Date(started).toISOString(), ...(startedBy ? { startedBy } : {}) };
    const record = async (entry: MockLandscapeResetRecord) => {
      try {
        await recorder?.(entry);
      } catch (error: any) {
        console.warn(`Couldn't record the mock landscape reset (${entry.status}): ${error?.message ?? error}`);
      }
    };
    await record({ ...base, status: 'running' });
    try {
      const storageMode = await this.deps.detectStorageMode();
      console.log(`   🗑️  Resetting mock landscape run state (${storageMode} storage mode)...`);
      const tablesTruncated = MockLandscapeReset.tablesFor(storageMode);
      const tables = (await this.deps.truncateTables(tablesTruncated)) || [];
      tables.forEach(({ tableName, itemsDeleted, durationMs }) => console.log(`   Deleted ${itemsDeleted} item(s) from ${tableName} in ${durationMs} ms`));
      const baselineObjectsDeleted = storageMode === 's3' ? await this.deps.deleteS3Baseline() : undefined;
      const finished = now();
      const result: MockLandscapeResetResult = {
        storageMode, tablesTruncated, tables, durationMs: finished - started,
        ...(baselineObjectsDeleted === undefined ? {} : { baselineObjectsDeleted })
      };
      console.log(`   ✓ Mock landscape reset complete in ${result.durationMs} ms`);
      await record({
        ...base, status: 'succeeded', finishedAt: new Date(finished).toISOString(), durationMs: result.durationMs, storageMode, tables,
        ...(baselineObjectsDeleted === undefined ? {} : { baselineObjectsDeleted })
      });
      return result;
    } catch (error: any) {
      const finished = now();
      await record({
        ...base, status: 'failed', finishedAt: new Date(finished).toISOString(), durationMs: finished - started,
        error: `${error?.message ?? error}`
      });
      throw error;
    }
  }

  /** A record's status as of now: `running` beyond STOPPED_AFTER_MS is `stopped` */
  static statusOf(record: MockLandscapeResetRecord, now: number = Date.now()): MockLandscapeResetRecord['status'] | 'stopped' {
    return record.status === 'running' && now - Date.parse(record.startedAt) > MockLandscapeReset.STOPPED_AFTER_MS
      ? 'stopped'
      : record.status;
  }

  /**
   * Writes a reset's record to the landscape's atomic counter table (UpdateItem, which the chunker subscriber is
   * granted there for the run counters), whole: the item's `reset` attribute is replaced.
   */
  static recorder(location: Pick<MockLandscapeLocation, 'stackId' | 'region' | 'landscape'>): MockLandscapeResetRecorder {
    const client = DynamoDBDocumentClient.from(new DynamoDBClient({ region: location.region }), { marshallOptions: { removeUndefinedValues: true } });
    return async record => {
      await client.send(new UpdateCommand({
        TableName: atomicCounterTableName(MockLandscapeReset.contextFor(location)),
        Key: { [atomicCounterKey]: MockLandscapeReset.RECORD_NAME },
        UpdateExpression: 'SET #reset = :reset, #purpose = :purpose, LastUpdated = :now',
        ExpressionAttributeNames: { '#reset': 'reset', '#purpose': 'purpose' },
        ExpressionAttributeValues: { ':reset': record, ':purpose': MockLandscapeReset.RECORD_PURPOSE, ':now': new Date().toISOString() }
      }));
    };
  }

  /** Reads the last reset's record (GetItem on the atomic counter table), or undefined if there has been none */
  static async readRecord(location: Pick<MockLandscapeLocation, 'stackId' | 'region' | 'landscape'>): Promise<MockLandscapeResetRecord | undefined> {
    const client = DynamoDBDocumentClient.from(new DynamoDBClient({ region: location.region }));
    const { Item } = await client.send(new GetCommand({
      TableName: atomicCounterTableName(MockLandscapeReset.contextFor(location)),
      Key: { [atomicCounterKey]: MockLandscapeReset.RECORD_NAME }
    }));
    return Item?.reset as MockLandscapeResetRecord | undefined;
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

  /** Truncates the given tables of the landscape, at the same time (BulkPurger refuses a non-mock landscape) */
  static async truncateTables(location: Pick<MockLandscapeLocation, 'stackId' | 'region' | 'landscape'>, tables: BulkPurgerTables[]): Promise<TableTruncation[]> {
    const context = MockLandscapeReset.contextFor(location);
    return new BulkPurger({ mode: BulkPurgerMode.TRUNCATE, tables, context, dryRun: false }).purge();
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
