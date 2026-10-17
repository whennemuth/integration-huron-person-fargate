import { DeleteObjectsCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { DescribeTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { ChunkingServiceRunner } from "../AbstractRunner";
import { MockTargetPersonTable } from "../../dynamodb/MockTargetPersonTable";
import { BulkPurger, BulkPurgerMode, BulkPurgerTables } from "../../dynamodb/BulkPurger";
import { DYNAMODB_TABLE_NAME as personCurrentStateTableName } from "../../dynamodb/PersonCurrentStateTable";
import { Config } from "integration-huron-person";
import { IContext } from "../../../context/IContext";
import { isMockLandscape, MOCK_LANDSCAPE_PATTERN } from "../../Utils";
import { Endpoint, NormalizedPopulationType, TargetConfig } from "../RunnerTypes";
import { SyncPopulation } from "../../../docker/chunkTypes";

/**
 * Decorator applied whenever the runner targets a mock landscape (one whose name matches
 * MOCK_LANDSCAPE_PATTERN, e.g. "mock" or "mock1" - see isMockLandscape in src/Utils.ts).
 *
 * A mock landscape is a stack dedicated to mocked runs: its integration config points the ECS tasks
 * at the target simulator (a stand-in for the Huron API, backed by the mock target table) instead of
 * the real Huron API, and its standard tables only ever hold mock-run data. So this decorator does
 * not need to (and cannot) switch anything on for the run - that is decided by the landscape at
 * deploy time. It only:
 *
 * - Verifies the landscape really is a mock landscape, and that its mock target table exists
 *   (i.e. the mock stack is actually deployed).
 * - Optionally resets the landscape's run state before the run (RUNNER_MOCK_TARGET_RESET_STATE),
 *   so the delta baseline never disagrees with what the mock target holds. It always truncates the
 *   mock target and statistics tables, then clears the delta baseline of the stack's storage mode:
 *   - DynamoDB mode: truncates the person current-state and person history tables.
 *   - S3 mode: deletes the baseline files (previous-input.ndjson, hashes.ndjson) under the shared
 *     delta storage dir (SHARED_DELTA_STORAGE_DIR, 'delta-storage') of the landscape's chunks bucket.
 *   The mode is detected from the deployed stack rather than read from config: the person
 *   current-state table exists only in DynamoDB mode (CDK creates it only then, and deletes it
 *   when the stack is switched to S3 mode).
 * - Warns about "hybrid" runs (real source API -> mock target), which consume from the real
 *   source API like any other landscape does.
 */
export class MockLandscapeRunnerDecorator extends ChunkingServiceRunner {
  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super(wrappedRunner.env);
  }

  public async validatePrerequisites(): Promise<boolean> {
    const { landscape, mockTargetResetState, sourceSimulator, populationType } = this.wrappedRunner.env;

    if (!isMockLandscape(landscape)) {
      console.error(`✗ Landscape "${landscape}" is not a mock landscape (must match ${MOCK_LANDSCAPE_PATTERN})`);
      return false;
    }

    console.log(`🎭 Mock landscape: ${landscape} (target is always mocked)`);
    console.log(`   Reset state: ${mockTargetResetState}`);

    if (!sourceSimulator) {
      console.warn('   ⚠️  HYBRID RUN: real source API -> mock target');
      if (this.normalizePopulationType(populationType) === SyncPopulation.PersonDelta) {
        console.warn('   ⚠️  The real source API depletes as it is read. A delta population fetched by this mock');
        console.warn('   ⚠️  landscape will NOT be available to the real landscape(s) that need it.');
      }
    }

    try {
      const context = this.buildContext();

      const mockTable = new MockTargetPersonTable({ context });
      if (!await mockTable.tableExists()) {
        console.error(`   ✗ Mock target table not found - is the "${landscape}" mock stack deployed?`);
        return false;
      }

      if (mockTargetResetState) {
        await this.resetRunState(context);
      }
    } catch (error: any) {
      console.error(`   ✗ Failed to prepare mock landscape: ${error.message}`);
      return false;
    }

    return this.wrappedRunner.validatePrerequisites();
  }

  public async resolveDataSource(config: Config): Promise<Endpoint> {
    return this.wrappedRunner.resolveDataSource(config);
  }

  public async resolveDataTarget(config: Config): Promise<TargetConfig> {
    return this.wrappedRunner.resolveDataTarget(config);
  }

  public async execute(
    sourceEndpoint: Endpoint,
    targetConfig: TargetConfig,
    config: Config,
    populationType: NormalizedPopulationType
  ): Promise<void> {
    return this.wrappedRunner.execute(sourceEndpoint, targetConfig, config, populationType);
  }

  // NOTE: Do not override start(). The inherited template method must run with `this` = this
  // decorator so that the overrides above (validatePrerequisites -> mock landscape checks and
  // reset, resolveDataTarget) are actually called. Delegating start() to wrappedRunner.start()
  // would run the template against the wrapped runner and silently bypass this decorator.

  /**
   * Resets the landscape's run state: the mock target, the statistics and the delta baseline of
   * whichever storage mode the stack is deployed in (see the class comment).
   */
  protected async resetRunState(context: IContext): Promise<void> {
    const storageMode = await this.detectStorageMode(context);
    console.log(`   🗑️  Resetting mock landscape run state (${storageMode} storage mode)...`);

    const tables = storageMode === 'dynamodb'
      ? [
          BulkPurgerTables.MOCK_TARGET_PERSON,
          BulkPurgerTables.PERSON_CURRENT_STATE,
          BulkPurgerTables.PERSON_HISTORY,
          BulkPurgerTables.STATISTICS
        ]
      : [
          BulkPurgerTables.MOCK_TARGET_PERSON,
          BulkPurgerTables.STATISTICS
        ];

    await this.truncateTables(context, tables);

    if (storageMode === 's3') {
      await this.deleteS3Baseline(context);
    }

    console.log('   ✓ Mock landscape reset complete');
  }

  /** Truncates the given tables of the landscape (BulkPurger refuses a non-mock landscape) */
  protected async truncateTables(context: IContext, tables: BulkPurgerTables[]): Promise<void> {
    await new BulkPurger({ mode: BulkPurgerMode.TRUNCATE, tables, context, dryRun: false }).purge();
  }

  /**
   * Detects the storage mode the stack is deployed in: DynamoDB mode if its person current-state
   * table exists, S3 mode otherwise.
   */
  protected async detectStorageMode(context: IContext): Promise<'dynamodb' | 's3'> {
    const client = new DynamoDBClient({ region: context.REGION });
    try {
      await client.send(new DescribeTableCommand({ TableName: personCurrentStateTableName(context) }));
      return 'dynamodb';
    } catch (error: any) {
      if (error.name === 'ResourceNotFoundException') {
        return 's3';
      }
      throw error;
    }
  }

  /**
   * Deletes the S3 mode delta baseline: every object under the shared delta storage dir of the
   * landscape's chunks bucket.
   *
   * @returns The number of objects deleted
   */
  protected async deleteS3Baseline(context: IContext): Promise<number> {
    const bucketName = await this.chunksBucketName(context);
    const prefix = `${MockLandscapeRunnerDecorator.SHARED_DELTA_STORAGE_DIR}/`;
    const s3 = new S3Client({ region: context.REGION });

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

  /**
   * The landscape's chunks bucket, named as CDK names it: `${S3.chunksBucket}-${landscape}`. The
   * base name comes from the local context.json (it is the same in every landscape's context).
   */
  private async chunksBucketName(context: IContext): Promise<string> {
    const { S3: { chunksBucket } = {} } = await this.loadContext();
    if (!chunksBucket) {
      throw new Error('S3.chunksBucket is missing from context/context.json - cannot locate the chunks bucket');
    }
    return `${chunksBucket}-${context.TAGS.Landscape.toLowerCase()}`;
  }

  /**
   * Shared delta storage dir of S3 mode. CDK leaves SHARED_DELTA_STORAGE_DIR unset (so the tasks
   * use this default) unless the integration config's storage.config has a keyPrefix.
   */
  public static readonly SHARED_DELTA_STORAGE_DIR = 'delta-storage';

  /**
   * Build IContext from environment variables.
   * Used for table name resolution and table instantiation.
   */
  private buildContext(): IContext {
    const { stackId, landscape, region } = this.wrappedRunner.env;

    if (!stackId || !landscape || !region) {
      throw new Error('Missing required environment: STACK_ID, LANDSCAPE, REGION');
    }

    return {
      STACK_ID: stackId,
      TAGS: { Landscape: landscape },
      REGION: region
    } as IContext;
  }

}
