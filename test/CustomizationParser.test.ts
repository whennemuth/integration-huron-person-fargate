import { Customization } from '../src/processing/custom/AbstractCustomPersonProcessor';
import { parseCustomizations } from '../src/processing/custom/CustomizationParser';

describe('parseCustomizations', () => {
  it('resolves a single valid key name', () => {
    expect(parseCustomizations('ORG_COMPARISON_LOGGING')).toEqual([Customization.ORG_COMPARISON_LOGGING]);
  });

  it('resolves a single valid numeric value (as a string)', () => {
    expect(parseCustomizations(String(Customization.ORG_COMPARISON_LOGGING))).toEqual([Customization.ORG_COMPARISON_LOGGING]);
  });

  it('tolerates surrounding whitespace around a key name', () => {
    expect(parseCustomizations('  ORG_COMPARISON_LOGGING  ')).toEqual([Customization.ORG_COMPARISON_LOGGING]);
  });

  it('tolerates surrounding whitespace around a numeric value', () => {
    expect(parseCustomizations(`  ${Customization.ORG_COMPARISON_LOGGING}  `)).toEqual([Customization.ORG_COMPARISON_LOGGING]);
  });

  it('returns an empty array for an empty string', () => {
    expect(parseCustomizations('')).toEqual([]);
  });

  it('returns an empty array when only commas/whitespace are given', () => {
    expect(parseCustomizations(' , , ')).toEqual([]);
  });

  it('discards an unrecognized key name and warns', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    expect(parseCustomizations('NOT_A_REAL_CUSTOMIZATION')).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('NOT_A_REAL_CUSTOMIZATION'));
    warnSpy.mockRestore();
  });

  it('discards a numeric value with no corresponding enum member and warns', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    // 9999 is not a valid Customization member
    expect(parseCustomizations('9999')).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('9999'));
    warnSpy.mockRestore();
  });

  it('discards a negative numeric value that has no corresponding enum member', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    expect(parseCustomizations('-1')).toEqual([]);
    warnSpy.mockRestore();
  });

  it('treats a non-integer numeric-looking token (e.g. "1.5") as a key lookup, not a number, and discards it', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    expect(parseCustomizations('1.5')).toEqual([]);
    warnSpy.mockRestore();
  });

  it('de-duplicates when the same customization is given as both its key name and its numeric value', () => {
    const csv = `ORG_COMPARISON_LOGGING,${Customization.ORG_COMPARISON_LOGGING}`;
    expect(parseCustomizations(csv)).toEqual([Customization.ORG_COMPARISON_LOGGING]);
  });

  it('de-duplicates repeated identical key names', () => {
    expect(parseCustomizations('ORG_COMPARISON_LOGGING,ORG_COMPARISON_LOGGING')).toEqual([Customization.ORG_COMPARISON_LOGGING]);
  });

  it('de-duplicates repeated identical numeric values', () => {
    const csv = `${Customization.ORG_COMPARISON_LOGGING},${Customization.ORG_COMPARISON_LOGGING}`;
    expect(parseCustomizations(csv)).toEqual([Customization.ORG_COMPARISON_LOGGING]);
  });

  it('keeps valid tokens while discarding unrecognized ones mixed into the same list', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    const csv = `NOT_A_REAL_CUSTOMIZATION,ORG_COMPARISON_LOGGING,9999`;
    expect(parseCustomizations(csv)).toEqual([Customization.ORG_COMPARISON_LOGGING]);
    warnSpy.mockRestore();
  });
});
