import { Config, ConfigManager } from "integration-huron-person";
import { SyncPopulation } from "../../docker/chunkTypes";
import { getLocalConfig } from "../Utils";
import { Endpoint, NormalizedPopulationType, RunnerEnv, TargetConfig, extractEnvironment } from './RunnerTypes';
import { IContext } from "../../context/IContext";
import { SimulationSettings, SourceSimulatorFunctionURL } from "../chunking/fetch/SourceSimulator";
import { resetRunCounters } from '../chunking/RunCounters';

/**
 * Abstract base class implementing the Template Method pattern for chunking service runners.
 * 
 * Defines the skeleton algorithm for starting the chunking service, with customization points
 * for different execution modes (single person, queue seeding, single message).
 * 
 * Template Method: start() orchestrates the flow:
 * 1. Extract environment variables
 * 2. Load configuration via ConfigManager
 * 3. Validate prerequisites (mode-specific)
 * 4. Resolve data source endpoint (mode-specific)
 * 5. Validate endpoint configuration
 * 6. Execute chunking operation (mode-specific)
 */
export abstract class ChunkingServiceRunner {
  public env: RunnerEnv;

  constructor(env?: RunnerEnv) {
    if(env) {
      this.env = env;
    } else if (!this.env) {
      this.env = this.extractEnvironment();
    }
  }

  public getEnv(): RunnerEnv {
    return this.env;
  }

  /**
   * Template method: Orchestrates the runner workflow.
   * Subclasses should not override this method.
   */
  async start(): Promise<void> {
    const { env } = this;
    
    if (!await this.validatePrerequisites()) {
      return;
    }

    const config = await this.loadConfiguration();

    const sourceEndpoint = await this.resolveDataSource(config);
    
    if (!this.validateEndpoint(sourceEndpoint)) {
      return;
    }

    const targetConfig = await this.resolveDataTarget(config);

    // Reset atomic counters before execution (unless explicitly skipped)
    if (!env.skipAtomicCounterReset) {
      await this.resetAtomicCounters();
    } else {
      console.log('⚠️  Atomic counter reset skipped (SKIP_ATOMIC_COUNTER_RESET=true)');
    }

    const populationType = this.normalizePopulationType(env.populationType);
    await this.execute(sourceEndpoint, targetConfig, config, populationType);
  }

  /**
   * Extract environment variables into RunnerEnv structure.
   * Common implementation for all runners.
   */
  protected extractEnvironment = (): RunnerEnv => extractEnvironment();

  /**
   * Load configuration using ConfigManager chain pattern.
   * Common implementation for all runners.
   */
  protected async loadConfiguration(): Promise<Config> {
    const { env } = this;
    const configManager = ConfigManager.getInstance();
    const localConfigPath = env.configPath || getLocalConfig();
    
    return await configManager
      .reset()
      .fromEnvironment()                            // ← Environment is first - takes precedence over all.
      .fromFileSystem(localConfigPath)              // ← Local dev only
      .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
      .fromSecretManager(env.secretArn)             // ← Fallback to Secrets Manager
      .getConfigAsync(env.buid ? 'person' : 'people');
  }

  protected async loadContext(): Promise<IContext> {
    const context = await require('../../context/context.json') as IContext;
    return context;
  }

  /**
   * The run's source simulator settings (SOURCE_SIMULATOR_MOCK_*), each only if set (0 included), sent as query
   * parameters in the run's fetchPath (SourceSimulatorRunnerDecorator). An unset one is the simulator's own.
   */
  protected simulatorSettings(): Partial<SimulationSettings> {
    const {
      sourceSimulatorMockTotalPopulation: totalPopulation,
      sourceSimulatorMockSimulatedDelaySeconds: simulatedDelaySeconds,
      sourceSimulatorMockErrorRate: errorRate,
      sourceSimulatorMockNonCurrentTermRate: nonCurrentTermRate
    } = this.env;
    const settings: Partial<SimulationSettings> = { totalPopulation, simulatedDelaySeconds, errorRate, nonCurrentTermRate };
    return Object.fromEntries(Object.entries(settings).filter(([, value]) => value !== undefined));
  }

  /**
   * Log the predicted chunking output from the simulator's population for this run (see
   * getSourceSimulatorPredictions()). Used when the source simulator is enabled.
   */
  protected async logSourceSimulatorPredictions(): Promise<void> {
    const predictions = await this.getSourceSimulatorPredictions();
    const { totalFullChunks, remainingItems, itemsPerChunk } = predictions;
    if(totalFullChunks > 0) {
      console.log(`Should generate ${totalFullChunks} full chunks of ${itemsPerChunk} items each, with ${remainingItems} remaining items in the last chunk.`);
    }
    else if(remainingItems > 0) {
      console.log(`Should generate 1 chunk with ${remainingItems} items.`);
    }
    else {
      console.warn("The source simulator's population is not known. Cannot predict chunking output.");
    }
  }

  /**
   * What the run should chunk, from the simulator's population for this run: its /health asked with the run's
   * settings (so a SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION is applied, clamped as a request's would be), else that
   * setting itself, else the context's LAMBDA.sourceSimulator.mockTotalPopulation (what the simulator is deployed
   * with). Never reads or changes the simulator Lambda's configuration.
   */
  protected async getSourceSimulatorPredictions(): Promise<{ 
    totalPopulation: number, totalFullChunks: number, remainingItems: number, itemsPerChunk: number 
  }> {
    const { landscape, region } = this.env;
    const predictions = { totalPopulation: 0, totalFullChunks: 0, remainingItems: 0, itemsPerChunk: 0 };

    // Destructure variables from the stack context.
    const { 
      ITEMS_PER_CHUNK, LAMBDA: { sourceSimulator: { mockTotalPopulation } = {}} = {} 
    } = (await this.loadContext());

    const settings = this.simulatorSettings();
    const health = await new SourceSimulatorFunctionURL({ landscape: landscape!, region: region! }).health(settings);
    const totalPopulation: number | undefined = health?.totalPopulation ?? settings.totalPopulation ?? mockTotalPopulation;

    // Proceed only if we have actual numeric values to work with.
    if(totalPopulation !== undefined && !isNaN(totalPopulation)) {
      if(!isNaN(ITEMS_PER_CHUNK) && ITEMS_PER_CHUNK > 0) {
        const totalFullChunks = Math.floor(totalPopulation / ITEMS_PER_CHUNK);
        const remainingItems = totalPopulation % ITEMS_PER_CHUNK;
        predictions.totalPopulation = totalPopulation;
        predictions.totalFullChunks = totalFullChunks;
        predictions.remainingItems = remainingItems;
        predictions.itemsPerChunk = ITEMS_PER_CHUNK;
      }
    }
    else {
      console.warn("The source simulator's population is not known (its /health couldn't be read, and neither SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION nor the context's LAMBDA.sourceSimulator.mockTotalPopulation is set). Cannot predict chunking output.");
    }

    return predictions;
  }

  /**
   * Validate endpoint has required baseUrl and fetchPath.
   * Common implementation for all runners.
   */
  protected validateEndpoint(endpoint: Endpoint): boolean {
    if (!endpoint.baseUrl) {
      console.error('Missing baseUrl in configuration!');
      return false;
    }
    if (!endpoint.fetchPath) {
      console.error('Missing fetchPath in configuration!');
      return false;
    }
    return true;
  }

  /**
   * Normalize population type to PersonDelta or PersonFull.
   * Common implementation for all runners.
   */
  protected normalizePopulationType(populationType?: string): NormalizedPopulationType {
    const { PersonDelta, PersonFull } = SyncPopulation;
    return populationType?.toLowerCase() === PersonDelta ? PersonDelta : PersonFull;
  }

  /**
   * Reset atomic counters to ensure clean offsets and chunk ordinals for new runs.
   * 
   * Centralized implementation called automatically from base class start() method.
   * Resets both chunker queue offset counter and chunk ordinal counter.
   * 
   * This method is BLOCKING - if reset fails, an error is thrown and execution stops.
   * This ensures chunk ordinals always start at 0 for new sync runs.
   * 
   * To skip automatic reset (e.g., for continuing previous run), set environment
   * variable SKIP_ATOMIC_COUNTER_RESET=true.
   * 
   * @throws Error if required environment variables (stackId, region, landscape) are missing
   * @throws Error if atomic counter reset operations fail
   */
  protected async resetAtomicCounters(): Promise<void> {
    const { stackId, region, landscape } = this.env;

    if (!stackId || !region || !landscape) {
      throw new Error(
        'Cannot reset atomic counters: missing required environment variables ' +
        `(stackId=${stackId}, region=${region}, landscape=${landscape})`
      );
    }

    console.log(`\n📝 Resetting the chunker's atomic counters (offsets and chunk ordinals)\n`);
    await resetRunCounters({ stackId, region, landscape });
  }

  /**
   * Validate mode-specific prerequisites before proceeding.
   * Subclasses must implement this to check their required environment variables.
   */
  public abstract validatePrerequisites(): Promise<boolean>;

  /**
   * Resolve the data source endpoint (baseUrl + fetchPath).
   * Subclasses must implement this to determine which endpoint to use.
   */
  public abstract resolveDataSource(config: Config): Promise<Endpoint>;

  /**
   * Resolve the data target configuration.
   * Returns metadata about which target to use (mock vs real).
   * Subclasses must implement this to determine target behavior.
   */
  public abstract resolveDataTarget(config: Config): Promise<TargetConfig>;

  /**
   * Execute the chunking operation with resolved source endpoint and target config.
   * Subclasses must implement this to perform their specific execution logic.
   */
  public abstract execute(
    sourceEndpoint: Endpoint,
    targetConfig: TargetConfig,
    config: Config,
    populationType: NormalizedPopulationType
  ): Promise<void>;

  // public abstract execute_proposed_change({ sourceEndpoint, targetEndpoint, config, populationType }: {
  //   sourceEndpoint: Endpoint,
  //   targetEndpoint: Endpoint,
  //   config: Config, 
  //   populationType: NormalizedPopulationType
  // }): Promise<void>;
}
