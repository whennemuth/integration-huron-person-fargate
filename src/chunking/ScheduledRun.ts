import { SyncPopulation } from '../../docker/chunkTypes';
import type { ApiChunkerEvent } from './ChunkerSubscriber';
import { RunStarter } from './StartedBy';

/**
 * What a landscape's scheduled run is, defined once for the EventBridge schedule's input
 * (lib/services/chunker/ChunkerService.ts, through scheduleInput()) and the dashboard's Bulk sync defaults
 * (dashboard/src/services/BulkSync.ts).
 *
 * A full sync with stored hashes trusted. The source has no endpoint that returns only changed people yet, so a run
 * fetches everyone; trusting the stored hashes keeps people whose hash matches from reaching the target, which is
 * the effect a delta run would have, and the full sync deactivates people missing from the source.
 *
 * The two previous-state flags only ever act together (bulkReset || !trustPreviousStorage: the chunker's person
 * cache, DeltaStrategyFactory.trustsPreviousStorage() in integration-huron-person), so they are set as one choice,
 * bulk reset or not (previousStateFlags()). Consolidating them into one setting is a TODO in integration-huron-person
 * (src/delta-strategy/TODO.md). A missing trustPreviousStorage reads as false in the chunker subscriber, which acts
 * as a reset, so both are always sent. The context's BULK_RESET and TRUST_PREVIOUS_STORAGE play no part here.
 */
export class ScheduledRun {
  static readonly POPULATION = SyncPopulation.PersonFull;
  static readonly BULK_RESET = false;
  /** What the schedule's static input says in place of the time a run is started */
  static readonly PROCESSED_AT_PLACEHOLDER = 'Placeholder. Override at runtime with ISO timestamp';

  /**
   * The flag pair for a run that does or doesn't bulk reset: a bulk reset looks each person up in the target; without
   * one, people whose stored hash matches are skipped.
   */
  static previousStateFlags(bulkReset: boolean): { bulkReset: boolean; trustPreviousStorage: boolean } {
    return { bulkReset, trustPreviousStorage: !bulkReset };
  }

  /**
   * The EventBridge schedule's input: the chunker event that starts a scheduled run.
   *
   * @param params.baseUrl, params.fetchPath - the integration config's people endpoint
   * @param params.iterationLimit - the context's ECS.chunkerService.chunksPerTask
   * @param params.personRecordProcessorAddons - addons for scheduled runs (none yet: see ChunkerService)
   */
  static scheduleInput(params: {
    baseUrl: string;
    fetchPath: string;
    iterationLimit: number;
    personRecordProcessorAddons?: string;
  }): ApiChunkerEvent {
    const { baseUrl, fetchPath, iterationLimit, personRecordProcessorAddons } = params;
    return {
      baseUrl,
      fetchPath,
      populationType: ScheduledRun.POPULATION,
      iterationLimit,
      offset: 0,
      ...ScheduledRun.previousStateFlags(ScheduledRun.BULK_RESET),
      ...(personRecordProcessorAddons !== undefined ? { personRecordProcessorAddons } : {}),
      startedBy: RunStarter.SCHEDULE,
      processingMetadata: {
        processedAt: ScheduledRun.PROCESSED_AT_PLACEHOLDER,
        processorVersion: '1.0.0'
      }
    };
  }
}
