import { Customization } from './AbstractCustomPersonProcessor';

/**
 * Resolve a single token to a Customization value. Accepts either the enum's key name (e.g.
 * 'ORG_COMPARISON_LOGGING') or its underlying numeric value as a string (e.g. '0'). Returns
 * undefined if the token matches neither.
 */
const resolveToken = (token: string): Customization | undefined => {
  if (/^\d+$/.test(token)) {
    const numericValue = Number(token);
    // Reverse-mapping lookup: a numeric enum maps a valid value back to its key name
    return Customization[numericValue] !== undefined ? (numericValue as Customization) : undefined;
  }
  return Customization[token as keyof typeof Customization];
};

/**
 * Parse a comma-delimited string of Customization enum keys and/or numeric values (e.g.
 * 'ORG_COMPARISON_LOGGING,0,SOME_OTHER_CUSTOMIZATION') into a de-duplicated list of valid
 * Customization values, warning about and discarding any unrecognized tokens. A key and its
 * corresponding numeric value both resolve to the same Customization and are only included once.
 */
export const parseCustomizations = (customizationsCsv: string): Customization[] => {
  const seen = new Set<Customization>();
  const result: Customization[] = [];

  customizationsCsv
    .split(',')
    .map(token => token.trim())
    .filter(token => token.length > 0)
    .forEach(token => {
      const customization = resolveToken(token);
      if (customization === undefined) {
        console.warn(`Unrecognized personRecordProcessor customization: ${token}`);
        return;
      }
      if (seen.has(customization)) {
        return;
      }
      seen.add(customization);
      result.push(customization);
    });

  return result;
};
