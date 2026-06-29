import { ApiChunkerEvent } from '../chunking/ChunkerSubscriber';
import { handleApiEvent } from '../chunking/fetch/ChunkerApiSubscriber';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Endpoint, NormalizedPopulationType, RunnerEnv } from './RunnerTypes';

/**
 * Runner for single person testing mode.
 * 
 * Uses the person endpoint with a specific BUID to test the full end-to-end
 * flow confined to one person for quick validation and error isolation.
 */
export class SinglePersonRunner extends ChunkingServiceRunner {
  public async validatePrerequisites(): Promise<boolean> {
    const { env } = this;
    if (!env.buid) {
      console.error('SINGLE_PERSON_BUID environment variable is required for single person runner!');
      return false;
    }
    if (!env.queueUrl) {
      console.error('Missing CHUNKER_QUEUE_URL environment variable!');
      return false;
    }
    return true;
  }

  public async resolveDataSource(config: any): Promise<Endpoint> {
    const { env } = this;
    const { 
      endpointConfig: { baseUrl: personBaseUrl } = {}, 
      fetchPath: personFetchPath 
    } = config.dataSource.person || {};
    
    return {
      baseUrl: personBaseUrl,
      fetchPath: `${personFetchPath}?buid=${env.buid}`
    };
  }

  public async execute(
    endpoint: Endpoint, 
    config: any, 
    populationType: NormalizedPopulationType
  ): Promise<void> {
    const { env } = this;
    const apiChunkerEvent: ApiChunkerEvent = {
      baseUrl: endpoint.baseUrl,
      fetchPath: endpoint.fetchPath,
      populationType,
      bulkReset: env.bulkReset,
      trustPreviousStorage: env.trustPreviousStorage,
      limit: env.peopleLimit ? parseInt(env.peopleLimit) : 0,
      offset: 0,
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    };

    console.log(`\n📝 Sending single person request for BUID: ${env.buid}\n`);
    await handleApiEvent(apiChunkerEvent, env.queueUrl!);
    console.log(`\n✓ Single person request sent successfully\n`);
  }
}
