import { AbstractPersonRecordProcessor, Customization } from './AbstractCustomPersonProcessor';
import { OrgComparisonLoggingPersonProcessor } from './impl/OrgComparisonLogging';
import { PersonRecordProcessorComposite } from './PersonRecordProcessorComposite';
import { parseCustomizations } from './CustomizationParser';

/**
 * Registry mapping each Customization to its concrete AbstractPersonRecordProcessor
 * implementation. Kept separate from AbstractCustomPersonProcessor.ts (which concrete
 * customizations import for their base class) to avoid a circular import.
 */
const construct = (customization: Customization): AbstractPersonRecordProcessor | undefined => {
  const { ORG_COMPARISON_LOGGING } = Customization;
  switch (customization) {
    case ORG_COMPARISON_LOGGING:
      return new OrgComparisonLoggingPersonProcessor();
    default:
      console.warn(`Unsupported customization: ${customization}`);
      return undefined;
  }
};

/**
 * Resolve the active personRecordProcessor customization(s), sourced from the run's Flags
 * (see src/chunking/metadata/IMetadataStorage.ts's personRecordProcessorCustomizations field) -
 * not from task-definition environment variables, so it can change per-run without a redeploy.
 *
 * A comma-delimited list resolves to a PersonRecordProcessorComposite wrapping one instance per
 * valid customization, so multiple customizations can run simultaneously for the same sync.
 */
export const personRecordProcessorFactory = async (customizationsCsv?: string): Promise<AbstractPersonRecordProcessor | undefined> => {
  if (!customizationsCsv) {
    return undefined;
  }

  const customizations = parseCustomizations(customizationsCsv);
  if (customizations.length === 0) {
    return undefined;
  }

  const processors = customizations
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


