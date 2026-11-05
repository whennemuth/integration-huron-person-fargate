import { Addon } from './AbstractAddonPersonProcessor';

/**
 * Resolve a single token to an Addon value. Accepts either the enum's key name (e.g.
 * 'ORG_COMPARISON_LOGGING') or its underlying numeric value as a string (e.g. '0'). Returns
 * undefined if the token matches neither.
 */
const resolveToken = (token: string): Addon | undefined => {
  if (/^\d+$/.test(token)) {
    const numericValue = Number(token);
    // Reverse-mapping lookup: a numeric enum maps a valid value back to its key name
    return Addon[numericValue] !== undefined ? (numericValue as Addon) : undefined;
  }
  return Addon[token as keyof typeof Addon];
};

/**
 * Parse a comma-delimited string of Addon enum keys and/or numeric values (e.g.
 * 'ORG_COMPARISON_LOGGING,0,SOME_OTHER_ADDON') into a de-duplicated list of valid
 * Addon values, warning about and discarding any unrecognized tokens. A key and its
 * corresponding numeric value both resolve to the same Addon and are only included once.
 */
export const parseAddons = (addonsCsv: string): Addon[] => {
  const seen = new Set<Addon>();
  const result: Addon[] = [];

  addonsCsv
    .split(',')
    .map(token => token.trim())
    .filter(token => token.length > 0)
    .forEach(token => {
      const addon = resolveToken(token);
      if (addon === undefined) {
        console.warn(`Unrecognized personRecordProcessor addon: ${token}`);
        return;
      }
      if (seen.has(addon)) {
        return;
      }
      seen.add(addon);
      result.push(addon);
    });

  return result;
};
