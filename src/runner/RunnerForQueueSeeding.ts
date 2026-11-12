import { DataSourceConfig, Config } from 'integration-huron-person';
import { QueueSeeder } from '../chunking/fetch/QueueSeeder';
import { ParallelStart } from '../chunking/ParallelStart';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Endpoint, NormalizedPopulationType, TargetConfig } from './RunnerTypes';
import { AbstractAtomicCounter } from '../dynamodb/AtomicCounter';
import { CHUNKER_COUNTER_NAME } from '../chunking/ChunkerQueue';

/**
 * Runner for queue seeding mode with parallel processing.
 * 
 * Pre-populates the chunker queue with multiple messages to enable "hitting
 * the ground running" with parallel ECS task execution. Optionally scales
 * the ECS service to match the workload.
 */
export class QueueSeedingRunner extends ChunkingServiceRunner {
  public async validatePrerequisites(): Promise<boolean> {
    const { 
      queueUrl, messagesToPrepopulate, buid, region, stackId, landscape, 
      desiredCount, clusterName, serviceName, sourceSimulator
    } = this.env;

    if (!queueUrl) {
      console.error('Missing CHUNKER_QUEUE_URL environment variable!');
      return false;
    }

    // Validate desiredCount is a valid number if provided
    if (desiredCount && isNaN(Number(desiredCount))) {
      console.error(`Invalid DESIRED_COUNT environment variable: ${desiredCount}. Must be a number.`);
      return false;
    }
    const seedNumber = parseInt(messagesToPrepopulate);
    
    if (buid) {
      console.warn(`MESSAGES_TO_PREPOPULATE is set to ${seedNumber} > 0, but SINGLE_PERSON_BUID is also ` +
        `set (${buid}). Seeding the queue is not appropriate when processing just one person. Cancelling operation`);
      return false;
    }
    if (!region) {
      console.error('REGION environment variable is required for queue seeding.');
      return false;
    }
    if (!stackId) {
      console.error('STACK_ID environment variable is required for queue seeding.');
      return false;
    }
    if (!landscape) {
      console.error('LANDSCAPE environment variable is required for queue seeding.');
      return false;
    }
    if (desiredCount > 0) {
      if (!clusterName) {
        console.error('ECS_CLUSTER_NAME environment variable is required to set desired count.');
        return false;
      }
      if (!serviceName) {
        console.error('ECS_SERVICE_NAME environment variable is required to set desired count.');
        return false;
      }
      if (desiredCount > seedNumber) {
        console.error(`DESIRED_COUNT environment variable (${desiredCount}), if greater than 0, ` +
          `should not be greater than MESSAGES_TO_PREPOPULATE (${seedNumber}). Cancelling operation`);
        return false;
      }
    }

    if(sourceSimulator) {
      // Validate we are not overseeding as determined by comparing seedNumber to source simulator predicted .  
      const predictions = await this.getSourceSimulatorPredictions();
      const { totalPopulation } = predictions;
      if (seedNumber > totalPopulation) {
        console.error(`MESSAGES_TO_PREPOPULATE (${seedNumber}) exceeds the total population predicted by the source simulator (${totalPopulation}). Cancelling operation`);
        return false;
      }
    }

    return true;
  }

  /**
   * Lookup the atomic counter in DynamoDB to ensure it exists.
   * @returns {Promise<boolean>} True if the atomic counter exists, false otherwise.
   */
  private async atomicCounterExists(): Promise<boolean> {
    const { stackId, region, landscape } = this.env;
    if (!stackId || !region || !landscape) {
      console.error('STACK_ID, REGION, and LANDSCAPE environment variables are required for atomic counter.');
      return false;
    } 
    const atomicCounter = new class extends AbstractAtomicCounter {
      getCounterName(): string {
        return CHUNKER_COUNTER_NAME;
      }
    }({ stackId, region, landscape });
    if (!await atomicCounter.tableExists()) {
      console.error(`An atomic counter is needed for queue seeding, but atomic counter table ` +
        `does not exist for stack ${stackId} in region ${region}. Please ensure the chunker ` +
        `queue has been created.`);
      return false;
    }
    return true;
  }

  public async resolveDataSource(config: Config): Promise<Endpoint> {
    let { 
      endpointConfig: { baseUrl } = {}, 
      fetchPath 
    } = config.dataSource?.people as DataSourceConfig || {};
    return { baseUrl: baseUrl!, fetchPath: fetchPath! };
  }

  public async resolveDataTarget(config: Config): Promise<TargetConfig> {
    // The target itself (real vs mock) is determined by the landscape, not the runner
    const { dataTarget } = config;
    return {
      endpoint: {
        baseUrl: dataTarget?.endpointConfig?.baseUrl || '',
        fetchPath: dataTarget?.personsPath || ''
      }
    };
  }

  /**
   * Seed MESSAGES_TO_PREPOPULATE messages and, for a DESIRED_COUNT above 0, wait for the chunker's scale-in alarm and
   * set the service's desired count: the procedure the chunker subscriber's parallel start uses too
   * (src/chunking/ParallelStart.ts), so the service's maximum is checked before anything is sent.
   */
  public async execute(
    sourceEndpoint: Endpoint,
    targetConfig: TargetConfig,
    config: Config,
    populationType: NormalizedPopulationType
  ): Promise<void> {
    const { env } = this;
    const seedNumber = parseInt(env.messagesToPrepopulate);

    // Bail out if the atomic counter does not exist, as this is a prerequisite for queue seeding.
    if(!await this.atomicCounterExists()) {
      return;
    }

    const parallelStart = this.parallelStart(sourceEndpoint, populationType);
    const result = await parallelStart.start({ messages: seedNumber, tasks: env.desiredCount > 0 ? env.desiredCount : 0 });
    if (result.statusCode !== 200) {
      console.error(`Queue seeding didn't complete: ${result.body}`);
    }
  }

  /** The parallel start for this run: the Runner's seeding (counters reset first), with the chunker service's AWS side */
  protected parallelStart(endpoint: Endpoint, populationType: NormalizedPopulationType): ParallelStart {
    const { env } = this;
    const seed = async (messagesToSeed: number): Promise<number> => {
      const queueSeeder = new QueueSeeder({
        region: env.region!,
        stackId: env.stackId!,
        landscape: env.landscape!,
        baseUrl: endpoint.baseUrl,
        fetchPath: endpoint.fetchPath,
        populationType,
        bulkReset: env.bulkReset,
        trustPreviousStorage: env.trustPreviousStorage,
        iterationLimit: env.iterationLimit ? env.iterationLimit : 0,
        messagesToSeed,
        queueUrl: env.queueUrl!,
        dryRun: false,
        personRecordProcessorAddons: env.personRecordProcessorAddons,
        termsEndpoint: endpoint.termsEndpoint
      });
      // Reset the atomic counters to ensure a clean slate for seeded messages and chunk ordinals
      await queueSeeder.resetAtomicCounters();
      return queueSeeder.seedQueue();
    };
    return new ParallelStart(ParallelStart.awsDeps({ clusterName: env.clusterName, serviceName: env.serviceName, region: env.region }, seed));
  }
}
