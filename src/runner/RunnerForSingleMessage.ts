import { DataSourceConfig } from 'integration-huron-person';
import { getFunctionUrl } from '../../lib/services/chunker/SourceSimulator';
import { ApiChunkerEvent } from '../chunking/ChunkerSubscriber';
import { handleApiEvent } from '../chunking/fetch/ChunkerApiSubscriber';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Endpoint, NormalizedPopulationType, RunnerEnv } from './RunnerTypes';

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

  public async resolveDataSource(config: any): Promise<Endpoint> {
    const { env } = this;
    let { 
      endpointConfig: { baseUrl } = {}, 
      fetchPath 
    } = config.dataSource?.people as DataSourceConfig || {};

    // Use source simulator if enabled
    if (env.sourceSimulator) {
      const functionUrl = await getFunctionUrl({ 
        landscape: env.landscape!, 
        region: env.region! 
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
    const apiChunkerEvent: ApiChunkerEvent = {
      baseUrl: endpoint.baseUrl,
      fetchPath: endpoint.fetchPath,
      populationType,
      bulkReset: env.bulkReset,
      trustPreviousStorage: env.trustPreviousStorage,
      limit: env.callLimit ? parseInt(env.callLimit) : 0,
      offset: 0,
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    };

    console.log(`\n📨 Sending single initial message to trigger chunking process...\n`);
    await handleApiEvent(apiChunkerEvent, env.queueUrl!);
    console.log(`\n✓ Initial message sent successfully\n`);
  }
}
