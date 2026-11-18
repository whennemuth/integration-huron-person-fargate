/**
 * What the local launcher (LocalLaunch.ts) does, decided from dashboard/scripts/LocalLaunch.env and the command
 * line, without side effects: the switches, what to check before starting, and the environment to run the
 * local server with. LocalLaunch.ts does the reading, the Lambda lookup and the starting.
 *
 * The environment, lowest precedence first:
 * 1. The landscape's deployed dashboard Lambda environment, without its secrets (not offline).
 * 2. LocalLaunch.env's variables, except the launcher's own switches: a variable the Lambda also has, with
 *    another value, is an override (listed, so the launcher can say so).
 * 3. What the switches call for: NODE_ENV=development, the offline services or not, and public mode or the local
 *    sign-in (LOCAL_SIGN_IN; the Lambda's own sign-in settings are kept, so its Authenticator runs as deployed).
 *    Offline, LANDSCAPE is the landscape the offline services play: --landscape's, else `local` (LocalLaunch.env's
 *    LANDSCAPE names a deployed landscape for a run online, and isn't used offline).
 *
 * Offline means the offline services: built-in data, no AWS access (OfflineServices.ts). It isn't a mock landscape,
 * whose pipeline has a mocked target; `--offline --landscape mock` plays one.
 *
 * Command-line flags override a switch for one run:
 *   --offline | --online          OFFLINE
 *   --auth <none|local>           AUTHENTICATION
 *   --watch | --no-watch          WATCH
 *   --storage-mode <dynamodb|s3>  OFFLINE_STORAGE_MODE (offline only)
 *   --landscape <name>            the landscape the offline services play, e.g. mock for a mock landscape's page
 *                                 (offline only; npm run dashboard:dev:offline:mock)
 *   --allow-production            ALLOW_PRODUCTION
 *   --roles <Role,...>            USER_ROLES: the local user's roles, offline (none: no role)
 *
 * The names before 2026-11-15 still work, each with a warning saying what it is now (RENAMED): MOCKED,
 * MOCK_STORAGE_MODE and AUTHENTICATION=mock in LocalLaunch.env, and --mocks, --no-mocks and --auth mock.
 */
import { LambdaFunction } from './LambdaFunction';

export const AUTHENTICATION_MODES = ['none', 'local'] as const;
export type AuthenticationMode = typeof AUTHENTICATION_MODES[number];

/** The offline storage modes (OfflineRunReporting.ts's OFFLINE_STORAGE_MODES, not imported: nothing of the server may load before its environment is set) */
export const STORAGE_MODES = ['dynamodb', 's3'] as const;
export type StorageMode = typeof STORAGE_MODES[number];

/** LocalLaunch.env's switches: read by the launcher, never passed to the server */
export const LAUNCHER_SWITCHES = ['OFFLINE', 'AUTHENTICATION', 'WATCH', 'ALLOW_PRODUCTION'] as const;

/**
 * Names in use before 2026-11-15, when "mock" was kept for a mock landscape (one whose pipeline's target is mocked):
 * each still works, with a warning saying what it is now
 */
export const RENAMED = {
  settings: { MOCKED: 'OFFLINE', MOCK_STORAGE_MODE: 'OFFLINE_STORAGE_MODE' } as Record<string, string>,
  flags: { '--mocks': '--offline', '--no-mocks': '--online' } as Record<string, string>,
  /** An AUTHENTICATION value (the setting, or --auth's) */
  authentication: { mock: 'local' } as Record<string, string>
};

/**
 * Tells the process that starts the server (this one, or each nodemon restart) to put the local sign-in
 * (LocalSignIn) in front of the handler. In the environment so that a restart, which reads no settings, knows.
 */
export const LOCAL_SIGN_IN = 'LOCAL_SIGN_IN';

/** The app login/logout header names an offline run uses when signed in, having no Lambda to take them from */
export const LOCAL_SIGN_IN_HEADERS = { APP_LOGIN_HEADER: 'app-login', APP_LOGOUT_HEADER: 'app-logout' };

const SIGN_IN_HEADERS = Object.keys(LOCAL_SIGN_IN_HEADERS);

/**
 * The landscape the offline services play unless --landscape names another (LandscapeRegistry.ts's LOCAL_LANDSCAPE, not imported:
 * nothing of the server may load before its environment is set)
 */
export const OFFLINE_LANDSCAPE = 'local';

/**
 * The roles USER_ROLES may name (StaticRoleCatalog's, not imported: nothing of the server may load before its
 * environment is set; a test keeps them the same)
 */
export const ROLE_NAMES = ['Viewer', 'Operator', 'Admin'] as const;

/** A landscape's name, as the stacks name them: lower case letters and digits, after a letter */
const LANDSCAPE_NAME = /^[a-z][a-z0-9]*$/;

export type LaunchEnvironment = {
  /** The variables to set */
  variables: Record<string, string>;
  /** Variables to remove, should the shell have set them */
  unset: string[];
  /** LocalLaunch.env's variables that replace a different Lambda value */
  overrides: string[];
}

type Flags = {
  offline?: boolean;
  authentication?: string;
  watch?: boolean;
  storageMode?: string;
  allowProduction?: boolean;
  landscape?: string;
  roles?: string;
}

export class LocalLaunchOptions {
  readonly offline: boolean;
  readonly authentication: AuthenticationMode;
  readonly watch: boolean;
  readonly storageMode: StorageMode;
  readonly allowProduction: boolean;
  /** Offline: the landscape the offline services play (--landscape, else OFFLINE_LANDSCAPE) */
  readonly offlineLandscape?: string;
  /**
   * Offline with the local sign-in: the local user's roles (USER_ROLES, or --roles), which the offline role store is
   * seeded with. Empty: no role. Undefined when they don't apply (online, the landscape's access table decides).
   */
  readonly roles?: string[];
  /** Old names used (RENAMED), each with what it is now: for the launcher to print */
  readonly warnings: string[] = [];
  /** LocalLaunch.env's variables, with any old setting names given their new ones */
  private readonly file: Record<string, string>;
  readonly landscape?: string;
  readonly region?: string;
  readonly stackId?: string;

  /**
   * @param file LocalLaunch.env's variables (as dotenv parses them)
   * @param args The command line's arguments
   * @throws for an unknown or malformed option or setting, or settings that can't work together
   */
  constructor(file: Record<string, string>, args: string[] = []) {
    this.file = this.renameSettings(file);
    const flags = LocalLaunchOptions.parseArgs(this.renameFlags(args));
    const setting = (name: string): string | undefined => this.file[name]?.trim() || undefined;

    this.offline = flags.offline ?? LocalLaunchOptions.parseBoolean('OFFLINE', setting('OFFLINE'));
    this.watch = flags.watch ?? LocalLaunchOptions.parseBoolean('WATCH', setting('WATCH'));
    this.allowProduction = flags.allowProduction ?? LocalLaunchOptions.parseBoolean('ALLOW_PRODUCTION', setting('ALLOW_PRODUCTION'));
    this.authentication = LocalLaunchOptions.parseAuthentication(this.renameAuthentication(flags.authentication ?? setting('AUTHENTICATION') ?? 'none'));
    this.storageMode = LocalLaunchOptions.parseStorageMode(flags.storageMode ?? setting('OFFLINE_STORAGE_MODE') ?? 'dynamodb');
    this.landscape = setting('LANDSCAPE');
    this.region = setting('REGION');
    this.stackId = setting('STACK_ID');

    if (flags.storageMode && !this.offline) {
      throw new Error('--storage-mode applies only offline: add --offline, or set OFFLINE=true');
    }
    if (flags.landscape && !this.offline) {
      throw new Error("--landscape applies only offline: add --offline (online, LocalLaunch.env's LANDSCAPE names the deployed landscape to run as)");
    }
    this.offlineLandscape = this.offline ? (flags.landscape ?? OFFLINE_LANDSCAPE) : undefined;
    if (!this.offline && (!this.landscape || !this.region)) {
      throw new Error('Online, LocalLaunch.env must name the landscape to run as: set LANDSCAPE and REGION (or OFFLINE=true)');
    }
    if (this.authentication === 'local' && !setting('USER_PRINCIPAL')) {
      throw new Error('AUTHENTICATION=local signs in as USER_PRINCIPAL: set it in LocalLaunch.env (a buPrincipal, e.g. jdoe), or use AUTHENTICATION=none');
    }
    const roles = flags.roles ?? this.file.USER_ROLES;
    if (roles !== undefined) {
      const names = LocalLaunchOptions.parseRoles(roles);
      if (!this.offline) {
        this.warnings.push(`USER_ROLES${flags.roles !== undefined ? ' (--roles)' : ''} is ignored online: the ${this.landscape} landscape's access table ` +
          'holds the roles (seed them with the DASHBOARD_ROLE_ASSIGNMENTS harness)');
      } else if (this.authentication !== 'local') {
        this.warnings.push('USER_ROLES is ignored with AUTHENTICATION=none: a public dashboard authorizes everything');
      } else {
        this.roles = names;
      }
    }
  }

  /** USER_ROLES: comma-delimited role names (none: no role) */
  static parseRoles(value: string): string[] {
    const names = value.split(',').map(name => name.trim()).filter(Boolean);
    if (names.length === 1 && names[0].toLowerCase() === 'none') {
      return [];
    }
    const unknown = names.filter(name => !(ROLE_NAMES as readonly string[]).includes(name));
    if (unknown.length) {
      throw new Error(`USER_ROLES (or --roles) names unknown role(s): ${unknown.join(', ')} (known: ${ROLE_NAMES.join(', ')}, or none)`);
    }
    return [...new Set(names)];
  }

  /** LocalLaunch.env with each old setting name (RENAMED.settings) given its new one, unless the new one is also set */
  private renameSettings(file: Record<string, string>): Record<string, string> {
    const renamed = { ...file };
    for (const [old, name] of Object.entries(RENAMED.settings)) {
      if (old in renamed) {
        this.warnings.push(`LocalLaunch.env's ${old} is now ${name}: rename it`);
        if (!(name in renamed)) {
          renamed[name] = renamed[old];
        }
        delete renamed[old];
      }
    }
    return renamed;
  }

  /** The command line with each old flag (RENAMED.flags) replaced by its new one */
  private renameFlags(args: string[]): string[] {
    return args.map((arg, index) => {
      if (args[index - 1] === '--auth') {
        // An old --auth value
        return this.renameAuthentication(arg);
      }
      const name = RENAMED.flags[arg];
      if (name) {
        this.warnings.push(`${arg} is now ${name}`);
      }
      return name ?? arg;
    });
  }

  /** An old AUTHENTICATION value (RENAMED.authentication) as its new one */
  private renameAuthentication(value: string): string {
    const name = RENAMED.authentication[value];
    if (name) {
      this.warnings.push(`AUTHENTICATION=${value} (or --auth ${value}) is now ${name}`);
    }
    return name ?? value;
  }

  /** Online, the server runs with the landscape's deployed dashboard Lambda environment, looked up at launch */
  get needsLambdaEnvironment(): boolean {
    return !this.offline;
  }

  /** The deployed dashboard Lambda to run as (online) */
  lambdaFunction(): LambdaFunction {
    const { landscape, region, stackId } = this;
    return new LambdaFunction({ landscape: landscape!, region: region!, stackId });
  }

  /**
   * The environment to run the server with.
   * @param lambdaEnvironment The deployed dashboard Lambda's environment (ignored offline)
   * @throws if the landscape is production, unless that is allowed
   */
  environment(lambdaEnvironment: Record<string, string> = {}): LaunchEnvironment {
    const lambda = this.offline ? {} : LambdaFunction.withoutSecrets(lambdaEnvironment);
    const fromFile = Object.fromEntries(Object.entries(this.file)
      .filter(([name]) => !(LAUNCHER_SWITCHES as readonly string[]).includes(name)));
    const overrides = Object.keys(fromFile).filter(name => name in lambda && lambda[name] !== fromFile[name]).sort();

    const variables: Record<string, string> = {
      ...lambda,
      ...fromFile,
      NODE_ENV: 'development',
      LOCAL_OFFLINE: `${this.offline}`,
      OFFLINE_STORAGE_MODE: this.storageMode,
      [LOCAL_SIGN_IN]: `${this.authentication === 'local'}`,
      // Offline, the landscape the offline services play (a mock landscape's name shows its page as it is there)
      ...(this.offlineLandscape ? { LANDSCAPE: this.offlineLandscape } : {})
    };
    const unset: string[] = [];
    // The offline role store's seed (ServiceProvider.getRoleAssignmentStore()); nowhere else is it read
    if (this.roles) {
      variables.USER_ROLES = this.roles.join(',');
    } else {
      delete variables.USER_ROLES;
    }
    if (this.authentication === 'none') {
      // Public mode: no app login/logout headers, so nothing requires a sign-in
      SIGN_IN_HEADERS.forEach(name => { delete variables[name]; unset.push(name); });
    }
    else if (this.offline && SIGN_IN_HEADERS.some(name => !variables[name])) {
      Object.assign(variables, LOCAL_SIGN_IN_HEADERS);
    }

    this.assertNotProduction(lambda.LANDSCAPE_TONE, variables.LANDSCAPE_TONE);
    return { variables, unset, overrides };
  }

  /**
   * Sync on the Individual page, and Bulk sync, write to the landscape's real target from uncommitted code.
   * Both the Lambda's tone and an override count, so an override can't hide a production landscape.
   */
  private assertNotProduction(...tones: (string | undefined)[]): void {
    if (!this.offline && !this.allowProduction && tones.some(tone => tone?.toLowerCase() === 'production')) {
      throw new Error(`The ${this.landscape} landscape is production (LANDSCAPE_TONE): a local run writes to its real target from local code. ` +
        'Set ALLOW_PRODUCTION=true in LocalLaunch.env, or pass --allow-production, to run against it anyway.');
    }
  }

  static parseArgs(args: string[]): Flags {
    const flags: Flags = {};
    const unknown: string[] = [];
    const valueOf = (option: string, index: number, allowed: readonly string[]): string => {
      const value = args[index];
      if (!allowed.includes(value)) {
        throw new Error(`${option} must be followed by ${allowed.join(' or ')} (got ${value === undefined ? 'nothing' : `'${value}'`})`);
      }
      return value;
    };
    for (let i = 0; i < args.length; i++) {
      switch (args[i]) {
        case '--offline': flags.offline = true; break;
        case '--online': flags.offline = false; break;
        case '--watch': flags.watch = true; break;
        case '--no-watch': flags.watch = false; break;
        case '--allow-production': flags.allowProduction = true; break;
        case '--roles': {
          const value = args[++i];
          if (value === undefined || value.startsWith('--')) {
            throw new Error(`--roles must be followed by role names, e.g. Operator or Viewer,Admin, or none (got ${value === undefined ? 'nothing' : `'${value}'`})`);
          }
          flags.roles = value;
          break;
        }
        case '--auth': flags.authentication = valueOf('--auth', ++i, AUTHENTICATION_MODES); break;
        case '--storage-mode': flags.storageMode = valueOf('--storage-mode', ++i, STORAGE_MODES); break;
        case '--landscape': {
          const name = args[++i];
          if (!name || !LANDSCAPE_NAME.test(name)) {
            throw new Error(`--landscape must be followed by a landscape's name, e.g. mock (got ${name === undefined ? 'nothing' : `'${name}'`})`);
          }
          flags.landscape = name;
          break;
        }
        default: unknown.push(args[i]);
      }
    }
    if (unknown.length > 0) {
      throw new Error(`Unknown option(s): ${unknown.join(', ')} (expected --offline, --online, --auth <${AUTHENTICATION_MODES.join('|')}>, ` +
        `--watch, --no-watch, --storage-mode <${STORAGE_MODES.join('|')}>, --landscape <name>, --allow-production, --roles <Role,...>)`);
    }
    return flags;
  }

  static parseBoolean(name: string, value: string | undefined): boolean {
    if (value === undefined) {
      return false;
    }
    const normalized = value.toLowerCase();
    if (normalized !== 'true' && normalized !== 'false') {
      throw new Error(`${name} in LocalLaunch.env must be true or false (got '${value}')`);
    }
    return normalized === 'true';
  }

  static parseAuthentication(value: string): AuthenticationMode {
    if (value === 'shibboleth') {
      throw new Error('AUTHENTICATION=shibboleth (the real sign-in) is not built: it is deferred until phase 5 is done. Use local or none.');
    }
    if (!(AUTHENTICATION_MODES as readonly string[]).includes(value)) {
      throw new Error(`AUTHENTICATION in LocalLaunch.env must be ${AUTHENTICATION_MODES.join(' or ')} (got '${value}')`);
    }
    return value as AuthenticationMode;
  }

  static parseStorageMode(value: string): StorageMode {
    if (!(STORAGE_MODES as readonly string[]).includes(value)) {
      throw new Error(`OFFLINE_STORAGE_MODE in LocalLaunch.env must be ${STORAGE_MODES.join(' or ')} (got '${value}')`);
    }
    return value as StorageMode;
  }
}
