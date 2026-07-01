import { Config, DataSourceConfig } from 'integration-huron-person';
import { AbstractAtomicCounter } from '../AtomicCounter';
import { CHUNKER_COUNTER_NAME } from '../chunking/ChunkerQueue';
import { ApiChunkerEvent } from '../chunking/ChunkerSubscriber';
import { handleApiEvent } from '../chunking/fetch/ChunkerApiSubscriber';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Endpoint, NormalizedPopulationType } from './RunnerTypes';

/**
 * Runner for single message execution mode.
 * 
 * Sends a single initial message to trigger the chunking process using the
 * normal one-in/one-out pattern. This is the default execution mode for
 * standard chunking operations.
 */
export class SingleMessageRunner extends ChunkingServiceRunner {
  public async validatePrerequisites(): Promise<boolean> {
    const { env } = this;
    if (!env.queueUrl) {
      console.error('Missing CHUNKER_QUEUE_URL environment variable!');
      return false;
    }

    // Validate source simulator prerequisites if enabled
    if (env.sourceSimulator) {
      if (!env.region) {
        console.error('REGION environment variable is required to use the source simulator.');
        return false;
      }
      if (!env.landscape) {
        console.error('LANDSCAPE environment variable is required to use the source simulator.');
        return false;
      }
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

  public async execute(
    endpoint: Endpoint, 
    config: any, 
    populationType: NormalizedPopulationType
  ): Promise<void> {
    const { bulkReset, trustPreviousStorage, iterationLimit, stackId, region, landscape, queueUrl } = this.env;
    const apiChunkerEvent: ApiChunkerEvent = {
      baseUrl: endpoint.baseUrl,
      fetchPath: endpoint.fetchPath,
      populationType,
      bulkReset,
      trustPreviousStorage,
      iterationLimit: iterationLimit ? iterationLimit : 0,
      offset: 0,
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    };

    // Reset the atomic counter for the chunker queue to ensure a clean state before sending the single person request
    if( stackId && region && landscape) {
      console.log(`\n📝 Resetting atomic counter for chunker queue: ${CHUNKER_COUNTER_NAME}\n`);
      const atomicCounter = new class extends AbstractAtomicCounter {
        getCounterName(): string {
          return CHUNKER_COUNTER_NAME;
        }
      }({ stackId, region, landscape });
      await atomicCounter.reset();
    }

    console.log(`\n📨 Sending single initial message to trigger chunking process...\n`);
    await handleApiEvent(apiChunkerEvent, queueUrl!);
    console.log(`\n✓ Initial message sent successfully\n`);
  }
}
