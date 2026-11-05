import { Addon } from '../src/processing/addon/AbstractAddonPersonProcessor';
import { parseAddons } from '../src/processing/addon/AddonParser';

describe('parseAddons', () => {
  it('resolves a single valid key name', () => {
    expect(parseAddons('ORG_COMPARISON_LOGGING')).toEqual([Addon.ORG_COMPARISON_LOGGING]);
  });

  it('resolves a single valid numeric value (as a string)', () => {
    expect(parseAddons(String(Addon.ORG_COMPARISON_LOGGING))).toEqual([Addon.ORG_COMPARISON_LOGGING]);
  });

  it('tolerates surrounding whitespace around a key name', () => {
    expect(parseAddons('  ORG_COMPARISON_LOGGING  ')).toEqual([Addon.ORG_COMPARISON_LOGGING]);
  });

  it('tolerates surrounding whitespace around a numeric value', () => {
    expect(parseAddons(`  ${Addon.ORG_COMPARISON_LOGGING}  `)).toEqual([Addon.ORG_COMPARISON_LOGGING]);
  });

  it('returns an empty array for an empty string', () => {
    expect(parseAddons('')).toEqual([]);
  });

  it('returns an empty array when only commas/whitespace are given', () => {
    expect(parseAddons(' , , ')).toEqual([]);
  });

  it('discards an unrecognized key name and warns', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    expect(parseAddons('NOT_A_REAL_ADDON')).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('NOT_A_REAL_ADDON'));
    warnSpy.mockRestore();
  });

  it('discards a numeric value with no corresponding enum member and warns', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    // 9999 is not a valid Addon member
    expect(parseAddons('9999')).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('9999'));
    warnSpy.mockRestore();
  });

  it('discards a negative numeric value that has no corresponding enum member', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    expect(parseAddons('-1')).toEqual([]);
    warnSpy.mockRestore();
  });

  it('treats a non-integer numeric-looking token (e.g. "1.5") as a key lookup, not a number, and discards it', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    expect(parseAddons('1.5')).toEqual([]);
    warnSpy.mockRestore();
  });

  it('de-duplicates when the same addon is given as both its key name and its numeric value', () => {
    const csv = `ORG_COMPARISON_LOGGING,${Addon.ORG_COMPARISON_LOGGING}`;
    expect(parseAddons(csv)).toEqual([Addon.ORG_COMPARISON_LOGGING]);
  });

  it('de-duplicates repeated identical key names', () => {
    expect(parseAddons('ORG_COMPARISON_LOGGING,ORG_COMPARISON_LOGGING')).toEqual([Addon.ORG_COMPARISON_LOGGING]);
  });

  it('de-duplicates repeated identical numeric values', () => {
    const csv = `${Addon.ORG_COMPARISON_LOGGING},${Addon.ORG_COMPARISON_LOGGING}`;
    expect(parseAddons(csv)).toEqual([Addon.ORG_COMPARISON_LOGGING]);
  });

  it('keeps valid tokens while discarding unrecognized ones mixed into the same list', () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation();
    const csv = `NOT_A_REAL_ADDON,ORG_COMPARISON_LOGGING,9999`;
    expect(parseAddons(csv)).toEqual([Addon.ORG_COMPARISON_LOGGING]);
    warnSpy.mockRestore();
  });
});
