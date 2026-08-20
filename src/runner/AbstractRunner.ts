import { Config, ConfigManager } from "integration-huron-person";
import { SyncPopulation } from "../../docker/chunkTypes";
import { getLocalConfig } from "../Utils";
import { Endpoint, NormalizedPopulationType, RunnerEnv, TargetConfig, extractEnvironment } from './RunnerTypes';
import { IContext } from "../../context/IContext";
import { SourceSimulatorFunctionURL } from "../chunking/fetch/SourceSimulator";
import { AbstractAtomicCounter } from '../AtomicCounter';
import { CHUNKER_COUNTER_NAME, CHUNK_ORDINAL_COUNTER_NAME } from '../chunking/ChunkerQueue';

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

  constructor() {
    this.env = this.extractEnvironment();
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
   * Log the predicted chunking output based on MOCK_TOTAL_POPULATION environment variable or config 
   * value. Used when the source simulator is enabled.
   * @param configTotalPopulation 
   */
  protected async logSourceSimulatorPredictions(): Promise<void> {
    const predictions = await this.getSourceSimulatorPredictions();
    const { totalFullChunks, remainingItems, itemsPerChunk } = predictions;
    if(totalFullChunks > 0) {
      console.log(`Should generate ${totalFullChunks} full chunks of ${itemsPerChunk} items each, with ${remainingItems} remaining items in the last chunk.`);
    }
    else {
      console.warn('MOCK_TOTAL_POPULATION is not set or is not a valid number. Cannot predict chunking output.');
    }
  }

  protected async getSourceSimulatorPredictions(): Promise<{ 
    totalPopulation: number, totalFullChunks: number, remainingItems: number, itemsPerChunk: number 
  }> {
    const { landscape, region,  } = this.env;
    const predictions = { totalPopulation: 0, totalFullChunks: 0, remainingItems: 0, itemsPerChunk: 0 };

    // Destructure variables from the stack context.
    const { 
      ITEMS_PER_CHUNK, LAMBDA: { sourceSimulator: { mockTotalPopulation } = {}} = {} 
    } = (await this.loadContext());

    // Check the MOCK_TOTAL_POPULATION environment variable on the lambda function first, falling 
    // back to the config value.
    const sourceSimulatorFunctionUrl = new SourceSimulatorFunctionURL({ landscape: landscape!, region: region! });
    let envTotalPopulation = await sourceSimulatorFunctionUrl.getEnvironmentVariable('MOCK_TOTAL_POPULATION');
    const totalPopulation = /\d+/.test(`${envTotalPopulation}`) ? 
      parseInt(envTotalPopulation!) : 
      mockTotalPopulation;

    // Proceed only if we have actual numeric values to work with.
    if(totalPopulation && !isNaN(totalPopulation)) {
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
      console.warn('MOCK_TOTAL_POPULATION is not set or is not a valid number. Cannot predict chunking output.');
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

    console.log(`\n📝 Resetting atomic counter for chunker queue: ${CHUNKER_COUNTER_NAME}\n`);
    const offsetCounter = new class extends AbstractAtomicCounter {
      getCounterName(): string {
        return CHUNKER_COUNTER_NAME;
      }
    }({ stackId, region, landscape });
    await offsetCounter.reset();

    console.log(`\n📝 Resetting atomic counter for chunk ordinals: ${CHUNK_ORDINAL_COUNTER_NAME}\n`);
    const chunkOrdinalCounter = new class extends AbstractAtomicCounter {
      getCounterName(): string {
        return CHUNK_ORDINAL_COUNTER_NAME;
      }
    }({ stackId, region, landscape });
    await chunkOrdinalCounter.reset();
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
