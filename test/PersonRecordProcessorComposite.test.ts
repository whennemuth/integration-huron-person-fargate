import { AbstractPersonRecordProcessor, Customization } from '../src/processing/custom/AbstractCustomPersonProcessor';
import { PersonRecordProcessorComposite } from '../src/processing/custom/PersonRecordProcessorComposite';

class FakePersonRecordProcessor extends AbstractPersonRecordProcessor {
  readonly customization: Customization;
  public calls: any[] = [];
  private readonly shouldThrow: boolean;

  constructor(customization: Customization, shouldThrow = false) {
    super();
    this.customization = customization;
    this.shouldThrow = shouldThrow;
  }

  processRecord = async (record: any): Promise<void> => {
    this.calls.push(record);
    if (this.shouldThrow) {
      throw new Error(`boom from ${this.customization}`);
    }
  };
}

describe('PersonRecordProcessorComposite', () => {
  it('exposes the flattened customizations of all wrapped processors', () => {
    const a = new FakePersonRecordProcessor(Customization.ORG_COMPARISON_LOGGING);
    const b = new FakePersonRecordProcessor(Customization.ORG_COMPARISON_LOGGING);
    const composite = new PersonRecordProcessorComposite([a, b]);
    expect(composite.customization).toEqual([Customization.ORG_COMPARISON_LOGGING, Customization.ORG_COMPARISON_LOGGING]);
  });

  it('invokes processRecord on every wrapped processor for a single record', async () => {
    const a = new FakePersonRecordProcessor(Customization.ORG_COMPARISON_LOGGING);
    const b = new FakePersonRecordProcessor(Customization.ORG_COMPARISON_LOGGING);
    const composite = new PersonRecordProcessorComposite([a, b]);

    const record = { raw: { personid: 'U1' } };
    await composite.processRecord(record);

    expect(a.calls).toEqual([record]);
    expect(b.calls).toEqual([record]);
  });

  it('continues invoking remaining processors if one throws', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation();
    const a = new FakePersonRecordProcessor(Customization.ORG_COMPARISON_LOGGING, true);
    const b = new FakePersonRecordProcessor(Customization.ORG_COMPARISON_LOGGING);
    const composite = new PersonRecordProcessorComposite([a, b]);

    const record = { raw: { personid: 'U1' } };
    await composite.processRecord(record);

    expect(a.calls).toEqual([record]);
    expect(b.calls).toEqual([record]);
    expect(errorSpy).toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});
