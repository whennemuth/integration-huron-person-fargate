import { Customization } from '../src/processing/custom/AbstractCustomPersonProcessor';
import { personRecordProcessorFactory } from '../src/processing/custom/PersonRecordProcessorFactory';
import { OrgComparisonLoggingPersonProcessor } from '../src/processing/custom/impl/OrgComparisonLogging';
import { PersonRecordProcessorComposite } from '../src/processing/custom/PersonRecordProcessorComposite';

describe('personRecordProcessorFactory', () => {
  it('returns undefined when no customizations string is provided', async () => {
    const result = await personRecordProcessorFactory(undefined);
    expect(result).toBeUndefined();
  });

  it('returns undefined for an empty customizations string', async () => {
    const result = await personRecordProcessorFactory('');
    expect(result).toBeUndefined();
  });

  it('returns an OrgComparisonLoggingPersonProcessor for a single recognized customization key', async () => {
    const result = await personRecordProcessorFactory('ORG_COMPARISON_LOGGING');
    expect(result).toBeInstanceOf(OrgComparisonLoggingPersonProcessor);
    expect(result?.customization).toBe(Customization.ORG_COMPARISON_LOGGING);
  });

  it('returns undefined for an unrecognized customization key', async () => {
    const result = await personRecordProcessorFactory('NOT_A_REAL_CUSTOMIZATION');
    expect(result).toBeUndefined();
  });

  // parseCustomizations (see CustomizationParser.test.ts) de-duplicates identical customizations,
  // so requesting the same one twice still resolves to a single instance, not a composite.
  it('returns a single instance (not a composite) when the same customization is given twice', async () => {
    const result = await personRecordProcessorFactory('ORG_COMPARISON_LOGGING,ORG_COMPARISON_LOGGING');
    expect(result).toBeInstanceOf(OrgComparisonLoggingPersonProcessor);
    expect(result).not.toBeInstanceOf(PersonRecordProcessorComposite);
  });

  it('ignores unrecognized keys mixed in with a valid one', async () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    const result = await personRecordProcessorFactory('NOT_A_REAL_CUSTOMIZATION,ORG_COMPARISON_LOGGING');
    expect(result).toBeInstanceOf(OrgComparisonLoggingPersonProcessor);
    warnSpy.mockRestore();
  });
});

