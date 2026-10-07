import { ChunkingServiceRunner } from "../AbstractRunner";
import { MockTargetPersonTable } from "../../dynamodb/MockTargetPersonTable";
import { BulkPurger, BulkPurgerMode, BulkPurgerTables } from "../../dynamodb/BulkPurger";
import { Config, ConfigManager } from "integration-huron-person";
import { IContext } from "../../../context/IContext";
import { isMockLandscape, MOCK_LANDSCAPE_PATTERN } from "../../Utils";
import { Endpoint, NormalizedPopulationType, TargetConfig } from "../RunnerTypes";
import { SyncPopulation } from "../../../docker/chunkTypes";

/**
 * Decorator applied whenever the runner targets a mock landscape (one whose name matches
 * MOCK_LANDSCAPE_PATTERN, e.g. "mock" or "mock1" - see isMockLandscape in src/Utils.ts).
 *
 * A mock landscape is a stack dedicated to mocked runs: its ECS tasks always use the mock target
 * (MockPersonDataTarget writing to the mock target table) instead of the real Huron API, and its
 * standard tables only ever hold mock-run data. So this decorator does not need to (and cannot)
 * switch anything on for the run - that is decided by the landscape at deploy time. It only:
 *
 * - Verifies the landscape really is a mock landscape, and that its mock target table exists
 *   (i.e. the mock stack is actually deployed).
 * - Optionally resets the landscape's run state before the run (RUNNER_MOCK_TARGET_RESET_STATE):
 *   truncates the mock target table AND the person current-state, person history and statistics
 *   tables, so the delta baseline never disagrees with what the mock target holds.
 * - Forwards mockTargetValidateOnly (RUNNER_MOCK_TARGET_VALIDATE_ONLY) for the run.
 * - Warns about "hybrid" runs (real source API -> mock target), which consume from the real
 *   source API like any other landscape does.
 */
export class MockLandscapeRunnerDecorator extends ChunkingServiceRunner {
  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super(wrappedRunner.env);
  }

  public async validatePrerequisites(): Promise<boolean> {
    const { landscape, mockTargetResetState, mockTargetValidateOnly, sourceSimulator, populationType } = this.wrappedRunner.env;

    if (!isMockLandscape(landscape)) {
      console.error(`✗ Landscape "${landscape}" is not a mock landscape (must match ${MOCK_LANDSCAPE_PATTERN})`);
      return false;
    }

    console.log(`🎭 Mock landscape: ${landscape} (target is always mocked)`);
    console.log(`   Reset state: ${mockTargetResetState}`);
    console.log(`   Validate only: ${mockTargetValidateOnly}`);

    if (!sourceSimulator) {
      console.warn('   ⚠️  HYBRID RUN: real source API -> mock target');
      if (this.normalizePopulationType(populationType) === SyncPopulation.PersonDelta) {
        console.warn('   ⚠️  The real source API depletes as it is read. A delta population fetched by this mock');
        console.warn('   ⚠️  landscape will NOT be available to the real landscape(s) that need it.');
      }
    }

    try {
      const context = this.buildContext();
      const config = await this.getConfig();

      const mockTable = new MockTargetPersonTable({ config, context });
      if (!await mockTable.tableExists()) {
        console.error(`   ✗ Mock target table not found - is the "${landscape}" mock stack deployed?`);
        return false;
      }

      if (mockTargetResetState) {
        console.log('   🗑️  Resetting mock landscape run state...');
        await new BulkPurger({
          mode: BulkPurgerMode.TRUNCATE,
          tables: [
            BulkPurgerTables.MOCK_TARGET_PERSON,
            BulkPurgerTables.PERSON_CURRENT_STATE,
            BulkPurgerTables.PERSON_HISTORY,
            BulkPurgerTables.STATISTICS
          ],
          context,
          integrationConfig: config,
          dryRun: false
        }).purge();
        console.log('   ✓ Mock landscape reset complete');
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
    const targetConfig = await this.wrappedRunner.resolveDataTarget(config);
    return {
      ...targetConfig,
      mockTargetValidateOnly: this.wrappedRunner.env.mockTargetValidateOnly || false
    };
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

  /**
   * Get config using ConfigManager.
   * Same pattern as used in SourceSimulatorRunnerDecorator.
   */
  private async getConfig() {
    const { configPath, secretArn } = this.wrappedRunner.env;
    const configManager = ConfigManager.getInstance();

    const config = await configManager
      .reset()
      .fromJsonString('HURON_PERSON_CONFIG_JSON')
      .fromSecretManager(secretArn)
      .fromEnvironment()
      .fromFileSystem(configPath)
      .getConfigAsync('people');

    return config;
  }
}
