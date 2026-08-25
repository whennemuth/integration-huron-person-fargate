import { ChunkingServiceRunner } from "../AbstractRunner";
import { MockTargetPersonTable } from "../../dynamodb/MockTargetPersonTable";
import { Config, ConfigManager } from "integration-huron-person";
import { IContext } from "../../../context/IContext";
import { Endpoint, NormalizedPopulationType, TargetConfig } from "../RunnerTypes";

/**
 * Decorator that enables mock target mode for testing with source simulator.
 * 
 * When enabled, processors use MockPersonDataTarget instead of real Huron API.
 * This prevents simulated data from affecting production systems.
 * 
 * SAFETY ENFORCEMENT: Mock target is ALWAYS enabled when source simulator is active,
 * regardless of environment variable settings. This prevents simulated data from
 * reaching the real target API.
 * 
 * Environment Variables:
 * - RUNNER_TARGET_MOCK: Enable mock target mode (boolean)
 * - RUNNER_TARGET_MOCK_RESET_STATE: Truncate table before run (boolean)
 * - RUNNER_TARGET_MOCK_VALIDATE_ONLY: Dry-run mode, log operations but don't execute (boolean)
 * 
 * Table Operations:
 * - If resetState is true, truncates mockTargetPersonTable during validatePrerequisites()
 * - Validates table exists before proceeding
 * - Sets flags for processors to use MockPersonDataTarget instead of HuronPersonDataTarget
 */
export class MockTargetRunnerDecorator extends ChunkingServiceRunner {
  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super(wrappedRunner.env);
  }

  /**
   * Validate prerequisites and optionally reset mock target state.
   * 
   * If mockTargetResetState is true:
   * 1. Resolves config using ConfigManager
   * 2. Creates IContext from environment variables
   * 3. Instantiates mockTargetPersonTable
   * 4. Truncates table to clear previous test data
   * 
   * Also validates that the mock target table exists.
   */
  public async validatePrerequisites(): Promise<boolean> {
    const { mockTarget, mockTargetResetState, sourceSimulator, region, stackId, landscape } = this.wrappedRunner.env;
    
    // Safety check: Enforce mock target when source simulator is active
    const effectiveMockTarget = mockTarget || sourceSimulator;
    
    if (effectiveMockTarget) {
      console.log('🎭 Mock Target Mode Enabled');
      
      if (sourceSimulator && !mockTarget) {
        console.log('   ⚠️  SAFETY ENFORCEMENT: Mock target automatically enabled because source simulator is active');
        console.log('   ⚠️  This prevents simulated data from reaching the real target API');
      }
      
      console.log(`   Reset state: ${mockTargetResetState}`);
      console.log(`   Validate only: ${this.wrappedRunner.env.mockTargetValidateOnly}`);
      
      // Validate table exists by attempting to get config and create table instance
      try {
        const context = this.buildContext();
        const tableName = this.getTableName(context);
        console.log(`   Table: ${tableName}`);
        
        // If reset is requested, truncate the table
        if (mockTargetResetState) {
          console.log('   🗑️  Resetting mock target state...');
          const config = await this.getConfig();
          const mockTable = new MockTargetPersonTable({ config, context });
          await mockTable.truncate();
          console.log('   ✓ Mock target reset complete');
        }
        
      } catch (error: any) {
        console.error(`   ✗ Failed to access mock target table: ${error.message}`);
        console.error('   Ensure the mockTargetPersonTable exists in DynamoDB');
        return false;
      }
    }
    
    return this.wrappedRunner.validatePrerequisites();
  }

  /**
   * Prepare chunk metadata with mock target configuration.
   * 
   * Sets flags.useMockTarget and flags.mockTargetConfig based on environment.
   * These flags are written to StatisticsTable and read by processors.
   * 
   * SAFETY: If source simulator is active, always enables mock target.
   */
  public async resolveDataSource(config: Config): Promise<Endpoint> {
    return this.wrappedRunner.resolveDataSource(config);
  }

  public async resolveDataTarget(config: Config): Promise<TargetConfig> {
    const { mockTarget, mockTargetValidateOnly, mockTargetResetState, sourceSimulator } = this.wrappedRunner.env;
    
    // Safety enforcement: Always use mock when source simulator is active
    const effectiveMockTarget = mockTarget || sourceSimulator;
    
    if (effectiveMockTarget) {
      return {
        useMockTarget: true,
        mockTargetValidateOnly: mockTargetValidateOnly || false,
        mockTargetResetState: mockTargetResetState || false
      };
    }
    
    // Delegate to wrapped runner for real target
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

  async start(): Promise<void> {
    return this.wrappedRunner.start();
  }

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
   * Get mock target table name from context.
   * Uses same naming convention as PersonCurrentStateTable.
   */
  private getTableName(context: IContext): string {
    const { DYNAMODB_TABLE_NAME } = require('../../dynamodb/MockTargetPersonTable');
    return DYNAMODB_TABLE_NAME(context);
  }

  /**
   * Get config using ConfigManager.
   * Same pattern as used in SourceSimulatorRunnerDecorator.
   * 
   * Override preLoadedMaps to prevent ReadOrganizations/ReadStates/ReadCountries calls
   * in mock target mode. This forces use of lookup expressions instead of HRN literals.
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

    // Override preLoadedMaps in mock target mode to prevent calls to real target system
    // for organization/state/country lookups. Use lookup expressions instead.
    config.preLoadedMaps = {
      orgMap: false,
      stateMap: false,
      countryMap: false
    };

    return config;
  }
}
