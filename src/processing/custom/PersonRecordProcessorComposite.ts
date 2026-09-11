import { PersonRecordProcessor } from 'integration-huron-person';
import { AbstractPersonRecordProcessor, Customization } from './AbstractCustomPersonProcessor';

/**
 * Combines multiple AbstractPersonRecordProcessor instances into a single PersonRecordProcessor,
 * invoking each wrapped instance's processRecord in turn for every record. Mirrors the wrapping
 * style used by src/runner/decorators (holds inner instance(s), delegates to them) - here
 * composing several rather than decorating exactly one, since multiple simultaneously-active
 * customizations (see PersonRecordProcessorFactory.ts) need to be combined into the single
 * callback HuronPersonIntegration accepts.
 */
export class PersonRecordProcessorComposite extends AbstractPersonRecordProcessor {
  readonly customization: Customization[];

  constructor(private readonly processors: AbstractPersonRecordProcessor[]) {
    super();
    this.customization = processors.flatMap(p => Array.isArray(p.customization) ? p.customization : [p.customization]);
  }

  processRecord: PersonRecordProcessor = async (record): Promise<void> => {
    for (const processor of this.processors) {
      try {
        await processor.processRecord(record);
      } catch (error) {
        // Don't let one customization's failure prevent the others from running
        console.error(`personRecordProcessor customization ${processor.customization} failed:`, error);
      }
    }
  }
}
