import { GetParametersByPathCommand, SSMClient } from '@aws-sdk/client-ssm';
import { LandscapeListing, LandscapeListingService, PickerLandscape } from './ServiceTypes';

/**
 * Every dashboard stack registers its landscape as an SSM parameter under this path
 * (`<path>/<landscape>`, created and deleted with the stack - see dashboard/lib/LandscapeRegistration.ts).
 * A landscape deployed in another account is listed by creating a parameter here by hand.
 */
export const LANDSCAPES_PARAMETER_PATH = '/hp-dashboard/landscapes';

/** The current landscape when the dashboard runs outside Lambda */
export const LOCAL_LANDSCAPE = 'local';

/** A registry parameter's value (JSON) */
export type LandscapeEntry = {
  landscape: string;
  /** Default: the landscape name */
  label?: string;
  /** Default: https://<landscape>.<this dashboard's hosted zone> */
  url?: string;
  isDefault?: boolean;
};

/**
 * Read every registry parameter under the path. A parameter whose value is not a JSON object is
 * skipped with a warning; its landscape is the value's `landscape`, else the last part of its name.
 */
export const readLandscapeEntries = async (params: {
  path?: string; region?: string; client?: SSMClient
} = {}): Promise<LandscapeEntry[]> => {
  const { path = LANDSCAPES_PARAMETER_PATH, region, client = new SSMClient({ region }) } = params;
  const entries: LandscapeEntry[] = [];
  let NextToken: string | undefined;
  do {
    const response = await client.send(new GetParametersByPathCommand({ Path: path, Recursive: false, NextToken }));
    for (const { Name, Value } of response.Parameters ?? []) {
      const entry = parseEntry(Name, Value);
      if (entry) entries.push(entry);
    }
    NextToken = response.NextToken;
  } while (NextToken);
  return entries;
}

const parseEntry = (name: string = '', value: string = ''): LandscapeEntry | undefined => {
  let parsed: any;
  try {
    parsed = JSON.parse(value);
  } catch {
    parsed = undefined;
  }
  const landscape = parsed?.landscape ?? name.split('/').pop();
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed) || !landscape) {
    console.warn(`Skipping landscape registry parameter ${name}: its value is not a JSON object`);
    return undefined;
  }
  const { label, url, isDefault } = parsed;
  return {
    landscape: `${landscape}`,
    ...(label && { label: `${label}` }),
    ...(url && { url: `${url}` }),
    isDefault: isDefault === true
  };
}

export type LandscapeRegistryParams = {
  /** This dashboard's landscape */
  currentLandscape: string;
  path?: string;
  /** For the default url of an entry without one */
  hostedZone?: string;
  region?: string;
  client?: SSMClient;
};

/**
 * The landscape picker's data: the registry, read once per Lambda instance (like DashboardCache's
 * config), so a newly deployed landscape appears in other dashboards after their next cold start.
 */
export class LandscapeRegistry implements LandscapeListingService {
  private entries?: Promise<LandscapeEntry[]>;

  constructor(private params: LandscapeRegistryParams) { }

  public static fromEnvironment = (env: NodeJS.ProcessEnv = process.env): LandscapeRegistry => {
    const { LANDSCAPE, LANDSCAPES_PARAMETER_PATH: path, HOSTED_ZONE, REGION } = env;
    return new LandscapeRegistry({
      currentLandscape: LANDSCAPE || LOCAL_LANDSCAPE,
      path: path || undefined,
      hostedZone: HOSTED_ZONE || undefined,
      region: REGION || undefined
    });
  }

  public getLandscapes = async (): Promise<LandscapeListing> => {
    const { currentLandscape, hostedZone } = this.params;
    if (!this.entries) {
      // Forget a failed read, so the next request retries it
      this.entries = readLandscapeEntries(this.params).catch(error => {
        this.entries = undefined;
        throw error;
      });
    }
    const landscapes: PickerLandscape[] = (await this.entries)
      .map(({ landscape, label, url, isDefault }) => ({
        landscape,
        label: label ?? landscape,
        url: url ?? (hostedZone ? `https://${landscape}.${hostedZone}` : undefined),
        isDefault: isDefault === true,
        current: landscape === currentLandscape
      }))
      .sort((a, b) => a.landscape.localeCompare(b.landscape));
    return { current: currentLandscape, landscapes };
  }
}
