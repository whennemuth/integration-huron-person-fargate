import { DataSourceConfig } from 'integration-huron-person';
import { DesiredCount } from '../DesiredCount';
import { QueueSeeder } from '../chunking/fetch/QueueSeeder';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Endpoint, NormalizedPopulationType, RunnerEnv } from './RunnerTypes';
import { AbstractAtomicCounter } from '../AtomicCounter';
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
      desiredCount, clusterName, serviceName 
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


    // Validate we are not overseeding as determined by comparing seedNumber to source simulator predicted .  
    const predictions = await this.getSourceSimulatorPredictions();
    const { totalPopulation } = predictions;

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

  public async resolveDataSource(config: any): Promise<Endpoint> {
    let { 
      endpointConfig: { baseUrl } = {}, 
      fetchPath 
    } = config.dataSource?.people as DataSourceConfig || {};
    return { baseUrl: baseUrl!, fetchPath: fetchPath! };
  }

  public async execute(
    endpoint: Endpoint, 
    config: any, 
    populationType: NormalizedPopulationType
  ): Promise<void> {
    const { env } = this;
    const seedNumber = parseInt(env.messagesToPrepopulate);

    // Bail out if the atomic counter does not exist, as this is a prerequisite for queue seeding.
    if(!await this.atomicCounterExists()) {
      return;
    }

    // Seed the queue
    await this.seedQueue(endpoint, env, populationType, seedNumber);

    // Scale ECS service if requested
    if (env.desiredCount > 0) {
      await this.scaleEcsService(env);
    }
  }

  private async scaleEcsService(env: RunnerEnv): Promise<void> {
    console.log(`\n🚀 Scaling up ECS service ${env.serviceName} in cluster ${env.clusterName} to desired count of ${env.desiredCount}...\n`);
    
    const desiredCountManager = new DesiredCount({ 
      clusterName: env.clusterName!, 
      serviceName: env.serviceName!, 
      region: env.region! 
    });
    
    const max = await desiredCountManager.getMax();
    if (max !== undefined && env.desiredCount > max) {
      console.warn(
        `Desired count of ${env.desiredCount} exceeds the maximum allowed by the auto-scaling ` +
        `configuration (${max}). Cancelling operation`
      );
      return;
    }
    
    await desiredCountManager.setTo(env.desiredCount);
    console.log(`✓ ECS service scaled to ${env.desiredCount}\n`);
  }

  private async seedQueue(
    endpoint: Endpoint, 
    env: RunnerEnv, 
    populationType: NormalizedPopulationType,
    seedNumber: number
  ): Promise<void> {
    console.log(`\n🌱 Seeding queue with ${seedNumber} messages...\n`);
    
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
      messagesToSeed: seedNumber,
      queueUrl: env.queueUrl!,
      dryRun: false
    });
    
    // Reset the atomic counters to ensure a clean slate for seeded messages and chunk ordinals
    await queueSeeder.resetAtomicCounters();

    // Seed the queue with the specified number of messages
    await queueSeeder.seedQueue();
    
    console.log(`\n✓ Queue seeding complete. Ready to scale up desiredCount to ${seedNumber}.\n`);
  }
}
