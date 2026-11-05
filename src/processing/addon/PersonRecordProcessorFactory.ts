import { AbstractPersonRecordProcessor, Addon } from './AbstractAddonPersonProcessor';
import { OrgComparisonLoggingPersonProcessor } from './impl/OrgComparisonLogging';
import { PersonRecordProcessorComposite } from './PersonRecordProcessorComposite';
import { parseAddons } from './AddonParser';

/**
 * Registry mapping each Addon to its concrete AbstractPersonRecordProcessor
 * implementation. Kept separate from AbstractAddonPersonProcessor.ts (which concrete
 * addons import for their base class) to avoid a circular import.
 */
const construct = (addon: Addon): AbstractPersonRecordProcessor | undefined => {
  const { ORG_COMPARISON_LOGGING } = Addon;
  switch (addon) {
    case ORG_COMPARISON_LOGGING:
      return new OrgComparisonLoggingPersonProcessor();
    default:
      console.warn(`Unsupported addon: ${addon}`);
      return undefined;
  }
};

/**
 * Resolve the active personRecordProcessor addon(s), sourced from the run's Flags
 * (see src/chunking/metadata/IMetadataStorage.ts's personRecordProcessorAddons field) -
 * not from task-definition environment variables, so it can change per-run without a redeploy.
 *
 * A comma-delimited list resolves to a PersonRecordProcessorComposite wrapping one instance per
 * valid addon, so multiple addons can run simultaneously for the same sync.
 */
export const personRecordProcessorFactory = async (addonsCsv?: string): Promise<AbstractPersonRecordProcessor | undefined> => {
  if (!addonsCsv) {
    return undefined;
  }

  const addons = parseAddons(addonsCsv);
  if (addons.length === 0) {
    return undefined;
  }

  const processors = addons
    .map(construct)
    .filter((processor): processor is AbstractPersonRecordProcessor => !!processor);

  if (processors.length === 0) {
    return undefined;
  }

  if (processors.length === 1) {
    return processors[0];
  }

  return new PersonRecordProcessorComposite(processors);
};


