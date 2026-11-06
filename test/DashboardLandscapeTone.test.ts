/**
 * Tests how the dashboard's top bar tone for a landscape is chosen (dashboard/src/handlers/LandscapeTone.ts):
 * from the LANDSCAPE_TONE setting, else by default from a mock landscape's name.
 */
import { LANDSCAPE_TONES, LandscapeTone } from '../dashboard/src/handlers/LandscapeTone';

describe('LandscapeTone', () => {
  afterEach(() => jest.restoreAllMocks());

  it('parses each tone, ignoring case and surrounding space, with a description for each', () => {
    expect(LANDSCAPE_TONES).toEqual(['production', 'test', 'mock']);
    expect(LandscapeTone.parse('production')?.description).toBe('Production landscape');
    expect(LandscapeTone.parse(' Test ')?.name).toBe('test');
    expect(LandscapeTone.parse('MOCK')?.description).toBe('Mock landscape');
  });

  it('parses an unset or blank setting as no tone', () => {
    expect(LandscapeTone.parse(undefined)).toBeUndefined();
    expect(LandscapeTone.parse('')).toBeUndefined();
    expect(LandscapeTone.parse('  ')).toBeUndefined();
  });

  it('refuses a setting that names no tone', () => {
    expect(() => LandscapeTone.parse('prod')).toThrow("LANDSCAPE_TONE must be one of production, test, mock (got 'prod')");
  });

  it('resolves to the setting, whatever the landscape', () => {
    expect(LandscapeTone.resolve({ setting: 'production', landscape: 'prod' })?.name).toBe('production');
    expect(LandscapeTone.resolve({ setting: 'test', landscape: 'mock' })?.name).toBe('test');
  });

  it('defaults to mock for a mock landscape, and to no tone for any other', () => {
    expect(LandscapeTone.resolve({ landscape: 'mock' })?.name).toBe('mock');
    expect(LandscapeTone.resolve({ landscape: 'mock2' })?.name).toBe('mock');
    expect(LandscapeTone.resolve({ landscape: 'mockingbird' })).toBeUndefined();
    expect(LandscapeTone.resolve({ landscape: 'staging' })).toBeUndefined();
    expect(LandscapeTone.resolve({})).toBeUndefined();
  });

  it('ignores an invalid setting when rendering, with a warning, rather than failing the page', () => {
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {});
    expect(LandscapeTone.resolve({ setting: 'prod', landscape: 'staging' })).toBeUndefined();
    expect(LandscapeTone.resolve({ setting: 'prod', landscape: 'mock1' })?.name).toBe('mock');
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("Ignoring the landscape tone setting: LANDSCAPE_TONE must be one of"));
  });
});
