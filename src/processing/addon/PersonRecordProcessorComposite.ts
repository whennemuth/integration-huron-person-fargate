import { PersonRecordProcessor } from 'integration-huron-person';
import { AbstractPersonRecordProcessor, Addon } from './AbstractAddonPersonProcessor';

/**
 * Combines multiple AbstractPersonRecordProcessor instances into a single PersonRecordProcessor,
 * invoking each wrapped instance's processRecord in turn for every record. Mirrors the wrapping
 * style used by src/runner/decorators (holds inner instance(s), delegates to them) - here
 * composing several rather than decorating exactly one, since multiple simultaneously-active
 * addons (see PersonRecordProcessorFactory.ts) need to be combined into the single
 * callback HuronPersonIntegration accepts.
 */
export class PersonRecordProcessorComposite extends AbstractPersonRecordProcessor {
  readonly addon: Addon[];

  constructor(private readonly processors: AbstractPersonRecordProcessor[]) {
    super();
    this.addon = processors.flatMap(p => Array.isArray(p.addon) ? p.addon : [p.addon]);
  }

  processRecord: PersonRecordProcessor = async (record): Promise<void> => {
    for (const processor of this.processors) {
      try {
        await processor.processRecord(record);
      } catch (error) {
        // Don't let one addon's failure prevent the others from running
        console.error(`personRecordProcessor addon ${processor.addon} failed:`, error);
      }
    }
  }
}
