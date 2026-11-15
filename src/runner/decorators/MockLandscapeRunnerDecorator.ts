import { ChunkingServiceRunner } from "../AbstractRunner";
import { MockTargetPersonTable } from "../../dynamodb/MockTargetPersonTable";
import { BulkPurgerTables } from "../../dynamodb/BulkPurger";
import { MockLandscapeReset, StorageMode } from "../../chunking/MockLandscapeReset";
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
 * - Optionally resets the landscape's run state before the run (RUNNER_MOCK_TARGET_RESET_STATE, through
 *   src/chunking/MockLandscapeReset.ts, which the chunker subscriber's run start shares),
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
   * whichever storage mode the stack is deployed in, through the reset the chunker subscriber's run
   * start shares (src/chunking/MockLandscapeReset.ts).
   */
  protected async resetRunState(context: IContext): Promise<void> {
    await new MockLandscapeReset({
      detectStorageMode: () => this.detectStorageMode(context),
      truncateTables: tables => this.truncateTables(context, tables),
      deleteS3Baseline: () => this.deleteS3Baseline(context),
    }).run();
  }

  /** Truncates the given tables of the landscape (BulkPurger refuses a non-mock landscape) */
  protected async truncateTables(context: IContext, tables: BulkPurgerTables[]): Promise<void> {
    await MockLandscapeReset.truncateTables(this.locationOf(context), tables);
  }

  /**
   * Detects the storage mode the stack is deployed in: DynamoDB mode if its person current-state
   * table exists, S3 mode otherwise.
   */
  protected async detectStorageMode(context: IContext): Promise<StorageMode> {
    return MockLandscapeReset.detectStorageMode(this.locationOf(context));
  }

  /**
   * Deletes the S3 mode delta baseline: every object under the shared delta storage dir of the
   * landscape's chunks bucket.
   *
   * @returns The number of objects deleted
   */
  protected async deleteS3Baseline(context: IContext): Promise<number> {
    return MockLandscapeReset.deleteS3Baseline(context.REGION, await this.chunksBucketName(context));
  }

  private locationOf(context: IContext): { stackId: string; region: string; landscape: string } {
    return { stackId: context.STACK_ID, region: context.REGION, landscape: context.TAGS.Landscape };
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
  public static readonly SHARED_DELTA_STORAGE_DIR = MockLandscapeReset.SHARED_DELTA_STORAGE_DIR;

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
