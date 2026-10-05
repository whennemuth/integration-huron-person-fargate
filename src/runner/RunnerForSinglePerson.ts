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
    const { 
      bulkReset, trustPreviousStorage, iterationLimit, buid, queueUrl, sourceSimulator, personRecordProcessorCustomizations
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
      personRecordProcessorCustomizations,
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    };

    // Source simulator ignores the requested BUID and always generates its own synthetic person (U0000001)
    if (sourceSimulator) {
      console.log(`\n📝 Single person mode requested BUID: ${buid}, but source simulator is enabled - it will generate its own synthetic person (U0000001) instead\n`);
    } else {
      console.log(`\n📝 Sending single person request for BUID: ${buid}\n`);
    }
    await handleApiEvent(apiChunkerEvent, queueUrl!);
    console.log(`\n✓ Single person request sent successfully\n`);
  }
}
