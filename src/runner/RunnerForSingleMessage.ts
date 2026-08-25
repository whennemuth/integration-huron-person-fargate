import { Config, DataSourceConfig } from 'integration-huron-person';
import { ApiChunkerEvent } from '../chunking/ChunkerSubscriber';
import { handleApiEvent } from '../chunking/fetch/ChunkerApiSubscriber';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Endpoint, NormalizedPopulationType, TargetConfig } from './RunnerTypes';

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

  public async resolveDataTarget(config: Config): Promise<TargetConfig> {
    // Standard runners use real Huron API target (unless overridden by decorator)
    const { dataTarget } = config;
    return {
      useMockTarget: this.env.mockTarget || false,
      mockTargetValidateOnly: this.env.mockTargetValidateOnly,
      endpoint: {
        baseUrl: dataTarget?.endpointConfig?.baseUrl || '',
        fetchPath: dataTarget?.personsPath || ''
      }
    };
  }

  public async execute(
    sourceEndpoint: Endpoint,
    targetConfig: TargetConfig,
    config: Config,
    populationType: NormalizedPopulationType
  ): Promise<void> {
    const { bulkReset, trustPreviousStorage, iterationLimit, queueUrl } = this.env;
    const apiChunkerEvent: ApiChunkerEvent = {
      baseUrl: sourceEndpoint.baseUrl,
      fetchPath: sourceEndpoint.fetchPath,
      populationType,
      bulkReset,
      trustPreviousStorage,
      iterationLimit: iterationLimit ? iterationLimit : 0,
      offset: 0,
      useMockTarget: targetConfig.useMockTarget,
      mockTargetValidateOnly: targetConfig.mockTargetValidateOnly,
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    };

    // Note: Atomic counter reset now handled automatically by base class start() method

    console.log(`\n📨 Sending single initial message to trigger chunking process...\n`);
    await handleApiEvent(apiChunkerEvent, queueUrl!);
    console.log(`\n✓ Initial message sent successfully\n`);
  }
}
