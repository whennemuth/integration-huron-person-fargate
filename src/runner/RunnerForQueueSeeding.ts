import { DataSourceConfig } from 'integration-huron-person';
import { getFunctionUrl } from '../../lib/services/chunker/SourceSimulator';
import { DesiredCount } from '../DesiredCount';
import { QueueSeeder } from '../chunking/fetch/QueueSeeder';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Endpoint, NormalizedPopulationType, RunnerEnv } from './RunnerTypes';
import { env } from 'process';

/**
 * Runner for queue seeding mode with parallel processing.
 * 
 * Pre-populates the chunker queue with multiple messages to enable "hitting
 * the ground running" with parallel ECS task execution. Optionally scales
 * the ECS service to match the workload.
 */
export class QueueSeedingRunner extends ChunkingServiceRunner {
  public async validatePrerequisites(): Promise<boolean> {
    const { env } = this;
    if (!env.queueUrl) {
      console.error('Missing CHUNKER_QUEUE_URL environment variable!');
      return false;
    }

    const seedNumber = parseInt(env.messagesToPrepopulate);
    
    if (env.buid) {
      console.warn(`MESSAGES_TO_PREPOPULATE is set to ${seedNumber} > 0, but SINGLE_PERSON_BUID is also ` +
        `set (${env.buid}). Seeding the queue is not appropriate when processing just one person. Cancelling operation`);
      return false;
    }

    if (!env.region) {
      console.error('REGION environment variable is required for queue seeding.');
      return false;
    }
    if (!env.stackId) {
      console.error('STACK_ID environment variable is required for queue seeding.');
      return false;
    }
    if (!env.landscape) {
      console.error('LANDSCAPE environment variable is required for queue seeding.');
      return false;
    }

    if (env.desiredCount > 0) {
      if (!env.clusterName) {
        console.error('ECS_CLUSTER_NAME environment variable is required to set desired count.');
        return false;
      }
      if (!env.serviceName) {
        console.error('ECS_SERVICE_NAME environment variable is required to set desired count.');
        return false;
      }
    }

    return true;
  }

  public async resolveDataSource(config: any): Promise<Endpoint> {
    const { env } = this;
    let { 
      endpointConfig: { baseUrl } = {}, 
      fetchPath 
    } = config.dataSource?.people as DataSourceConfig || {};

    // Use source simulator if enabled
    if (env.sourceSimulator) {
      if (!env.region || !env.landscape) {
        throw new Error('REGION and LANDSCAPE are required to use source simulator');
      }

      const functionUrl = await getFunctionUrl({ 
        landscape: env.landscape, 
        region: env.region 
      });

      const functionUrlObj = new URL(functionUrl);
      baseUrl = `${functionUrlObj.protocol}//${functionUrlObj.host}`;
      fetchPath = functionUrlObj.pathname;
      
      console.log(`Using source simulator: ${JSON.stringify({ baseUrl, fetchPath }, null, 2)}`);
    }

    return { baseUrl: baseUrl!, fetchPath: fetchPath! };
  }

  public async execute(
    endpoint: Endpoint, 
    config: any, 
    populationType: NormalizedPopulationType
  ): Promise<void> {
    const { env } = this;
    const seedNumber = parseInt(env.messagesToPrepopulate);

    // Scale ECS service if requested
    if (env.desiredCount > 0) {
      await this.scaleEcsService(env);
    }

    // Seed the queue
    await this.seedQueue(endpoint, env, populationType, seedNumber);
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
      limit: env.peopleLimit ? parseInt(env.peopleLimit) : 0,
      messagesToSeed: seedNumber,
      queueUrl: env.queueUrl!,
      dryRun: false
    });
    
    await queueSeeder.resetAtomicCounter();
    await queueSeeder.seedQueue();
    
    console.log(`\n✓ Queue seeding complete. Ready to scale up desiredCount to ${seedNumber}.\n`);
  }
}
