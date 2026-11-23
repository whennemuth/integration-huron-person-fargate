import { ApiChunkerEvent } from '../chunking/ChunkerSubscriber';
import { handleApiEvent } from '../chunking/fetch/ChunkerApiSubscriber';
import { ChunkingServiceRunner } from './AbstractRunner';
import { Config } from 'integration-huron-person';
import { Endpoint, NormalizedPopulationType, TargetConfig } from './RunnerTypes';
import { RunStarter } from '../chunking/StartedBy';
import { SyncPopulation } from '../../docker/chunkTypes';

/**
 * Why a single-person run can't have the given POPULATION_TYPE, or undefined when it can.
 *
 * A single-person run is always person-delta. A person-full run deactivates everyone in the baseline missing from its
 * source, and a single-person source holds one person, so person-full would soft-delete everyone else in the target.
 * (On 2026-11-23 one did in preview: its post-processor began looking up 143,890 people's HRNs to deactivate them, and
 * was stopped before sending a delete.) The dashboard's Test forces person-delta likewise (BulkSyncRequest.parse()).
 *
 * Unset means person-delta. Any other value is refused rather than corrected, so a .env set for full syncs is noticed:
 * normalizePopulationType() would read anything but person-delta, a typo included, as person-full.
 */
export const singlePersonPopulationRefusal = (populationType?: string): string | undefined => {
  const value = populationType?.trim();
  if (!value || value.toLowerCase() === SyncPopulation.PersonDelta) {
    return undefined;
  }
  return `✗ A single-person run (SINGLE_PERSON_BUID) must be ${SyncPopulation.PersonDelta}, but POPULATION_TYPE=${value}. ` +
    `A ${SyncPopulation.PersonFull} run of one person would deactivate everyone else in the target. ` +
    `Set POPULATION_TYPE=${SyncPopulation.PersonDelta}, or leave it unset. Aborting.`;
};

/**
 * Runner for single person testing mode.
 *
 * Uses the person endpoint with a specific BUID to test the full end-to-end
 * flow confined to one person for quick validation and error isolation.
 * Always person-delta (singlePersonPopulationRefusal()); Runner.ts refuses any other POPULATION_TYPE before building
 * a runner, since some decorators change things (purges, service toggles) before validatePrerequisites() reaches here.
 */
export class SinglePersonRunner extends ChunkingServiceRunner {
  public async validatePrerequisites(): Promise<boolean> {
    const { env } = this;
    if (!env.buid) {
      console.error('SINGLE_PERSON_BUID environment variable is required for single person runner!');
      return false;
    }
    const populationRefusal = singlePersonPopulationRefusal(env.populationType);
    if (populationRefusal) {
      console.error(populationRefusal);
      return false;
    }
    if (!env.queueUrl) {
      console.error('Missing CHUNKER_QUEUE_URL environment variable!');
      return false;
    }
    return true;
  }

  /** Always person-delta: an unset POPULATION_TYPE would otherwise be read as person-full */
  protected normalizePopulationType(_populationType?: string): NormalizedPopulationType {
    return SyncPopulation.PersonDelta;
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
    // The target itself (real vs mock) is determined by the landscape, not the runner
    const { dataTarget } = config;
    return {
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
      bulkReset, trustPreviousStorage, iterationLimit, buid, queueUrl, sourceSimulator, personRecordProcessorAddons
    } = this.env;
    const apiChunkerEvent: ApiChunkerEvent = {
      baseUrl: sourceEndpoint.baseUrl,
      fetchPath: sourceEndpoint.fetchPath,
      populationType,
      bulkReset,
      trustPreviousStorage,
      iterationLimit: iterationLimit ? iterationLimit : 0,
      offset: 0,
      personRecordProcessorAddons,
      termsEndpoint: sourceEndpoint.termsEndpoint,
      startedBy: RunStarter.RUNNER,
      ...(sourceEndpoint.processorScalingCapacity ? { processorScalingCapacity: sourceEndpoint.processorScalingCapacity } : {}),
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    };

    const via = sourceSimulator ? ' (source simulator single person endpoint)' : '';
    console.log(`\n📝 Sending single person request for BUID: ${buid}${via}\n`);
    await handleApiEvent(apiChunkerEvent, queueUrl!);
    console.log(`\n✓ Single person request sent successfully\n`);
  }
}
