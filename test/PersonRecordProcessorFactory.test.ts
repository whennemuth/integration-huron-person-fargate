import { Addon } from '../src/processing/addon/AbstractAddonPersonProcessor';
import { personRecordProcessorFactory } from '../src/processing/addon/PersonRecordProcessorFactory';
import { OrgComparisonLoggingPersonProcessor } from '../src/processing/addon/impl/OrgComparisonLogging';
import { PersonRecordProcessorComposite } from '../src/processing/addon/PersonRecordProcessorComposite';

describe('personRecordProcessorFactory', () => {
  it('returns undefined when no addons string is provided', async () => {
    const result = await personRecordProcessorFactory(undefined);
    expect(result).toBeUndefined();
  });

  it('returns undefined for an empty addons string', async () => {
    const result = await personRecordProcessorFactory('');
    expect(result).toBeUndefined();
  });

  it('returns an OrgComparisonLoggingPersonProcessor for a single recognized addon key', async () => {
    const result = await personRecordProcessorFactory('ORG_COMPARISON_LOGGING');
    expect(result).toBeInstanceOf(OrgComparisonLoggingPersonProcessor);
    expect(result?.addon).toBe(Addon.ORG_COMPARISON_LOGGING);
  });

  it('returns undefined for an unrecognized addon key', async () => {
    const result = await personRecordProcessorFactory('NOT_A_REAL_ADDON');
    expect(result).toBeUndefined();
  });

  // parseAddons (see AddonParser.test.ts) de-duplicates identical addons,
  // so requesting the same one twice still resolves to a single instance, not a composite.
  it('returns a single instance (not a composite) when the same addon is given twice', async () => {
    const result = await personRecordProcessorFactory('ORG_COMPARISON_LOGGING,ORG_COMPARISON_LOGGING');
    expect(result).toBeInstanceOf(OrgComparisonLoggingPersonProcessor);
    expect(result).not.toBeInstanceOf(PersonRecordProcessorComposite);
  });

  it('ignores unrecognized keys mixed in with a valid one', async () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    const result = await personRecordProcessorFactory('NOT_A_REAL_ADDON,ORG_COMPARISON_LOGGING');
    expect(result).toBeInstanceOf(OrgComparisonLoggingPersonProcessor);
    warnSpy.mockRestore();
  });
});

