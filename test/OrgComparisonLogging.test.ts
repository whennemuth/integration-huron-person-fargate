import { OrgComparisonLoggingPersonProcessor } from '../src/processing/custom/impl/OrgComparisonLogging';
import { Customization } from '../src/processing/custom/AbstractCustomPersonProcessor';

describe('OrgComparisonLoggingPersonProcessor', () => {
  let processor: OrgComparisonLoggingPersonProcessor;
  let logEntrySpy: jest.SpyInstance;

  const mappedWithOrgs = (title: string | undefined, organizationHrn: string, secondaryUnitHrn?: string) => ({
    fieldValues: [
      ...(title !== undefined ? [{ title }] : []),
      { organization: { hrn: organizationHrn } },
      ...(secondaryUnitHrn ? [{ secondaryUnit: { hrn: secondaryUnitHrn } }] : [])
    ]
  }) as any;

  beforeEach(() => {
    processor = new OrgComparisonLoggingPersonProcessor();
    logEntrySpy = jest.spyOn(processor as any, 'logEntry').mockResolvedValue(undefined);
  });

  it('exposes ORG_COMPARISON_LOGGING as its customization', () => {
    expect(processor.customization).toBe(Customization.ORG_COMPARISON_LOGGING);
  });

  it('does nothing when the record has an error', async () => {
    await processor.processRecord({ raw: { personid: '1' }, error: new Error('boom') });
    expect(logEntrySpy).not.toHaveBeenCalled();
  });

  it('does nothing when the record has no mapped data', async () => {
    await processor.processRecord({ raw: { personid: '1' } });
    expect(logEntrySpy).not.toHaveBeenCalled();
  });

  it('does nothing for a non-student (title is not "Student")', async () => {
    await processor.processRecord({
      raw: { personid: '1' },
      mapped: mappedWithOrgs('Some Employee Title', 'org-a', 'org-b')
    });
    expect(logEntrySpy).not.toHaveBeenCalled();
  });

  it('does nothing when there is no secondaryUnit', async () => {
    await processor.processRecord({
      raw: { personid: '1' },
      mapped: mappedWithOrgs('Student', 'org-a')
    });
    expect(logEntrySpy).not.toHaveBeenCalled();
  });

  it('does nothing when organization and secondaryUnit match', async () => {
    await processor.processRecord({
      raw: { personid: '1' },
      mapped: mappedWithOrgs('Student', 'org-a', 'org-a')
    });
    expect(logEntrySpy).not.toHaveBeenCalled();
  });

  it('logs an entry when the student has differing primary and secondary orgs', async () => {
    await processor.processRecord({
      raw: { personid: 'U123' },
      mapped: mappedWithOrgs('Student', 'org-a', 'org-b')
    });
    expect(logEntrySpy).toHaveBeenCalledWith(Customization.ORG_COMPARISON_LOGGING, 'U123', { personid: 'U123', primaryOrg: 'org-a', secondaryOrg: 'org-b' });
  });
});
