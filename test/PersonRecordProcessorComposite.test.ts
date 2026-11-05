import { AbstractPersonRecordProcessor, Addon } from '../src/processing/addon/AbstractAddonPersonProcessor';
import { PersonRecordProcessorComposite } from '../src/processing/addon/PersonRecordProcessorComposite';

class FakePersonRecordProcessor extends AbstractPersonRecordProcessor {
  readonly addon: Addon;
  public calls: any[] = [];
  private readonly shouldThrow: boolean;

  constructor(addon: Addon, shouldThrow = false) {
    super();
    this.addon = addon;
    this.shouldThrow = shouldThrow;
  }

  processRecord = async (record: any): Promise<void> => {
    this.calls.push(record);
    if (this.shouldThrow) {
      throw new Error(`boom from ${this.addon}`);
    }
  };
}

describe('PersonRecordProcessorComposite', () => {
  it('exposes the flattened addons of all wrapped processors', () => {
    const a = new FakePersonRecordProcessor(Addon.ORG_COMPARISON_LOGGING);
    const b = new FakePersonRecordProcessor(Addon.ORG_COMPARISON_LOGGING);
    const composite = new PersonRecordProcessorComposite([a, b]);
    expect(composite.addon).toEqual([Addon.ORG_COMPARISON_LOGGING, Addon.ORG_COMPARISON_LOGGING]);
  });

  it('invokes processRecord on every wrapped processor for a single record', async () => {
    const a = new FakePersonRecordProcessor(Addon.ORG_COMPARISON_LOGGING);
    const b = new FakePersonRecordProcessor(Addon.ORG_COMPARISON_LOGGING);
    const composite = new PersonRecordProcessorComposite([a, b]);

    const record = { raw: { personid: 'U1' } };
    await composite.processRecord(record);

    expect(a.calls).toEqual([record]);
    expect(b.calls).toEqual([record]);
  });

  it('continues invoking remaining processors if one throws', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation();
    const a = new FakePersonRecordProcessor(Addon.ORG_COMPARISON_LOGGING, true);
    const b = new FakePersonRecordProcessor(Addon.ORG_COMPARISON_LOGGING);
    const composite = new PersonRecordProcessorComposite([a, b]);

    const record = { raw: { personid: 'U1' } };
    await composite.processRecord(record);

    expect(a.calls).toEqual([record]);
    expect(b.calls).toEqual([record]);
    expect(errorSpy).toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});
