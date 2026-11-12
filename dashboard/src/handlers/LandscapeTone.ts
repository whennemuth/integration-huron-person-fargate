import { isMockLandscape } from '../../../src/Utils';

/** The tones a landscape can be marked with in the top bar, from most to least cautionary */
export const LANDSCAPE_TONES = ['production', 'test', 'mock'] as const;

export type LandscapeToneName = typeof LANDSCAPE_TONES[number];

/** What a tone tells the reader, for those who don't see its color */
const DESCRIPTIONS: Record<LandscapeToneName, string> = {
  production: 'Production landscape',
  test: 'Test landscape',
  mock: 'Mock landscape'
};

/**
 * How the top bar marks the current landscape, so that with no colored banner it is still obvious
 * which one you are working in: a red tint for production, amber for test, gray for mock, and none
 * for an unmarked landscape.
 *
 * The tone comes from the dashboard context's optional LANDSCAPE_TONE setting, which CDK passes to
 * the Lambda as the LANDSCAPE_TONE environment variable. Without one, a mock landscape name
 * (isMockLandscape(): mock, mock1, ...) is marked mock and any other landscape is left unmarked.
 */
export class LandscapeTone {

  private constructor(public readonly name: LandscapeToneName) {}

  /** What the tone tells the reader (e.g. "Production landscape") */
  get description(): string {
    return DESCRIPTIONS[this.name];
  }

  static isToneName(value: unknown): value is LandscapeToneName {
    return typeof value === 'string' && (LANDSCAPE_TONES as readonly string[]).includes(value);
  }

  /**
   * Parse a LANDSCAPE_TONE setting: undefined when it's unset or blank. Throws for a value that names
   * no tone, so synth refuses a misspelled setting instead of deploying an unmarked landscape.
   */
  static parse(setting?: string): LandscapeTone | undefined {
    const value = (setting ?? '').trim().toLowerCase();
    if (value === '') {
      return undefined;
    }
    if ( ! LandscapeTone.isToneName(value)) {
      throw new Error(`LANDSCAPE_TONE must be one of ${LANDSCAPE_TONES.join(', ')} (got '${setting}')`);
    }
    return new LandscapeTone(value);
  }

  /**
   * The tone the top bar uses for a landscape: the setting, else mock for a mock landscape name,
   * else none. Synth has already refused an invalid setting, so at render time one (e.g. a typo in a
   * local .env) is reported and ignored rather than failing the page.
   */
  static resolve(params: { setting?: string; landscape?: string }): LandscapeTone | undefined {
    const { setting, landscape } = params;
    try {
      const tone = LandscapeTone.parse(setting);
      if (tone) {
        return tone;
      }
    } catch (error) {
      console.warn(`Ignoring the landscape tone setting: ${(error as Error).message}`);
    }
    return isMockLandscape(landscape) ? new LandscapeTone('mock') : undefined;
  }
}
