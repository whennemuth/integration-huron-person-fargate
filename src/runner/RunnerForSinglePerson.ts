import { ApiChunkerEvent } from '../chunking/ChunkerSubscriber';
import { handleApiEvent } from '../chunking/fetch/ChunkerApiSubscriber';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Config } from 'integration-huron-person';
import { Endpoint, NormalizedPopulationType, TargetConfig } from './RunnerTypes';

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

  public async resolveDataSource(config: Config): Promise<Endpoint> {
    const { env } = this;
    const { 
      endpointConfig: { baseUrl: personBaseUrl } = {}, 
      fetchPath: personFetchPath 
    } = config.dataSource.person || {};
    
    return {
      baseUrl: personBaseUrl || '',
      fetchPath: personFetchPath ? `${personFetchPath}?buid=${env.buid}` : ''
    };
  }

  public async resolveDataTarget(config: Config): Promise<TargetConfig> {
    // Standard runners use real Huron API target
    const { dataTarget } = config;
    return {
      useMockTarget: false,
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
    const { 
      bulkReset, trustPreviousStorage, iterationLimit, buid, queueUrl 
    } = this.env;
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

    console.log(`\n📝 Sending single person request for BUID: ${buid}\n`);
    await handleApiEvent(apiChunkerEvent, queueUrl!);
    console.log(`\n✓ Single person request sent successfully\n`);
  }
}
