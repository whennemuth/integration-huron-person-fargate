/**
 * What the local launcher (LocalLaunch.ts) does, decided from dashboard/scripts/LocalLaunch.env and the command
 * line, without side effects: the switches, what to check before starting, and the environment to run the
 * local server with. LocalLaunch.ts does the reading, the Lambda lookup and the starting.
 *
 * The environment, lowest precedence first:
 * 1. The landscape's deployed dashboard Lambda environment, without its secrets (not with mocks).
 * 2. LocalLaunch.env's variables, except the launcher's own switches: a variable the Lambda also has, with
 *    another value, is an override (listed, so the launcher can say so).
 * 3. What the switches call for: NODE_ENV=development, the mock services or not, and public mode or the mock
 *    sign-in (LOCAL_MOCK_SIGN_IN; the Lambda's own sign-in settings are kept, so its Authenticator runs as deployed).
 *
 * Command-line flags override a switch for one run:
 *   --mocks | --no-mocks          MOCKED
 *   --auth <none|mock>            AUTHENTICATION
 *   --watch | --no-watch          WATCH
 *   --storage-mode <dynamodb|s3>  MOCK_STORAGE_MODE (with mocks only)
 *   --allow-production            ALLOW_PRODUCTION
 */
import { LambdaFunction } from './LambdaFunction';

export const AUTHENTICATION_MODES = ['none', 'mock'] as const;
export type AuthenticationMode = typeof AUTHENTICATION_MODES[number];

/** The mock storage modes (MockRunReporting.ts's MOCK_STORAGE_MODES, not imported: nothing of the server may load before its environment is set) */
export const STORAGE_MODES = ['dynamodb', 's3'] as const;
export type StorageMode = typeof STORAGE_MODES[number];

/** LocalLaunch.env's switches: read by the launcher, never passed to the server */
export const LAUNCHER_SWITCHES = ['MOCKED', 'AUTHENTICATION', 'WATCH', 'ALLOW_PRODUCTION'] as const;

/**
 * Tells the process that starts the server (this one, or each nodemon restart) to put the mock sign-in
 * (MockSignIn) in front of the handler. In the environment so that a restart, which reads no settings, knows.
 */
export const LOCAL_MOCK_SIGN_IN = 'LOCAL_MOCK_SIGN_IN';

/** The app login/logout header names a mock run uses when signed in, having no Lambda to take them from */
export const MOCK_SIGN_IN_HEADERS = { APP_LOGIN_HEADER: 'app-login', APP_LOGOUT_HEADER: 'app-logout' };

const SIGN_IN_HEADERS = Object.keys(MOCK_SIGN_IN_HEADERS);

export type LaunchEnvironment = {
  /** The variables to set */
  variables: Record<string, string>;
  /** Variables to remove, should the shell have set them */
  unset: string[];
  /** LocalLaunch.env's variables that replace a different Lambda value */
  overrides: string[];
}

type Flags = {
  mocked?: boolean;
  authentication?: string;
  watch?: boolean;
  storageMode?: string;
  allowProduction?: boolean;
}

export class LocalLaunchOptions {
  readonly mocked: boolean;
  readonly authentication: AuthenticationMode;
  readonly watch: boolean;
  readonly storageMode: StorageMode;
  readonly allowProduction: boolean;
  readonly landscape?: string;
  readonly region?: string;
  readonly stackId?: string;

  /**
   * @param file LocalLaunch.env's variables (as dotenv parses them)
   * @param args The command line's arguments
   * @throws for an unknown or malformed option or setting, or settings that can't work together
   */
  constructor(private readonly file: Record<string, string>, args: string[] = []) {
    const flags = LocalLaunchOptions.parseArgs(args);
    const setting = (name: string): string | undefined => file[name]?.trim() || undefined;

    this.mocked = flags.mocked ?? LocalLaunchOptions.parseBoolean('MOCKED', setting('MOCKED'));
    this.watch = flags.watch ?? LocalLaunchOptions.parseBoolean('WATCH', setting('WATCH'));
    this.allowProduction = flags.allowProduction ?? LocalLaunchOptions.parseBoolean('ALLOW_PRODUCTION', setting('ALLOW_PRODUCTION'));
    this.authentication = LocalLaunchOptions.parseAuthentication(flags.authentication ?? setting('AUTHENTICATION') ?? 'none');
    this.storageMode = LocalLaunchOptions.parseStorageMode(flags.storageMode ?? setting('MOCK_STORAGE_MODE') ?? 'dynamodb');
    this.landscape = setting('LANDSCAPE');
    this.region = setting('REGION');
    this.stackId = setting('STACK_ID');

    if (flags.storageMode && !this.mocked) {
      throw new Error('--storage-mode applies only to the mocks: add --mocks, or set MOCKED=true');
    }
    if (!this.mocked && (!this.landscape || !this.region)) {
      throw new Error('Without mocks, LocalLaunch.env must name the landscape to run as: set LANDSCAPE and REGION (or MOCKED=true)');
    }
    if (this.authentication === 'mock' && !setting('USER_PRINCIPAL')) {
      throw new Error('AUTHENTICATION=mock signs in as USER_PRINCIPAL: set it in LocalLaunch.env (a buPrincipal, e.g. jdoe), or use AUTHENTICATION=none');
    }
  }

  /** Without mocks the server runs with the landscape's deployed dashboard Lambda environment, looked up at launch */
  get needsLambdaEnvironment(): boolean {
    return !this.mocked;
  }

  /** The deployed dashboard Lambda to run as (without mocks) */
  lambdaFunction(): LambdaFunction {
    const { landscape, region, stackId } = this;
    return new LambdaFunction({ landscape: landscape!, region: region!, stackId });
  }

  /**
   * The environment to run the server with.
   * @param lambdaEnvironment The deployed dashboard Lambda's environment (ignored with mocks)
   * @throws if the landscape is production, unless that is allowed
   */
  environment(lambdaEnvironment: Record<string, string> = {}): LaunchEnvironment {
    const lambda = this.mocked ? {} : LambdaFunction.withoutSecrets(lambdaEnvironment);
    const fromFile = Object.fromEntries(Object.entries(this.file)
      .filter(([name]) => !(LAUNCHER_SWITCHES as readonly string[]).includes(name)));
    const overrides = Object.keys(fromFile).filter(name => name in lambda && lambda[name] !== fromFile[name]).sort();

    const variables: Record<string, string> = {
      ...lambda,
      ...fromFile,
      NODE_ENV: 'development',
      LOCALHOST_WITH_MOCKS: `${this.mocked}`,
      MOCK_STORAGE_MODE: this.storageMode,
      [LOCAL_MOCK_SIGN_IN]: `${this.authentication === 'mock'}`
    };
    const unset: string[] = [];
    if (this.authentication === 'none') {
      // Public mode: no app login/logout headers, so nothing requires a sign-in
      SIGN_IN_HEADERS.forEach(name => { delete variables[name]; unset.push(name); });
    }
    else if (this.mocked && SIGN_IN_HEADERS.some(name => !variables[name])) {
      Object.assign(variables, MOCK_SIGN_IN_HEADERS);
    }

    this.assertNotProduction(lambda.LANDSCAPE_TONE, variables.LANDSCAPE_TONE);
    return { variables, unset, overrides };
  }

  /**
   * Sync on the Individual page, and Bulk sync, write to the landscape's real target from uncommitted code.
   * Both the Lambda's tone and an override count, so an override can't hide a production landscape.
   */
  private assertNotProduction(...tones: (string | undefined)[]): void {
    if (!this.mocked && !this.allowProduction && tones.some(tone => tone?.toLowerCase() === 'production')) {
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
        case '--mocks': flags.mocked = true; break;
        case '--no-mocks': flags.mocked = false; break;
        case '--watch': flags.watch = true; break;
        case '--no-watch': flags.watch = false; break;
        case '--allow-production': flags.allowProduction = true; break;
        case '--auth': flags.authentication = valueOf('--auth', ++i, AUTHENTICATION_MODES); break;
        case '--storage-mode': flags.storageMode = valueOf('--storage-mode', ++i, STORAGE_MODES); break;
        default: unknown.push(args[i]);
      }
    }
    if (unknown.length > 0) {
      throw new Error(`Unknown option(s): ${unknown.join(', ')} (expected --mocks, --no-mocks, --auth <${AUTHENTICATION_MODES.join('|')}>, ` +
        `--watch, --no-watch, --storage-mode <${STORAGE_MODES.join('|')}>, --allow-production)`);
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
      throw new Error('AUTHENTICATION=shibboleth (the real sign-in) is not built: it is deferred until phase 5 is done. Use mock or none.');
    }
    if (!(AUTHENTICATION_MODES as readonly string[]).includes(value)) {
      throw new Error(`AUTHENTICATION in LocalLaunch.env must be ${AUTHENTICATION_MODES.join(' or ')} (got '${value}')`);
    }
    return value as AuthenticationMode;
  }

  static parseStorageMode(value: string): StorageMode {
    if (!(STORAGE_MODES as readonly string[]).includes(value)) {
      throw new Error(`MOCK_STORAGE_MODE in LocalLaunch.env must be ${STORAGE_MODES.join(' or ')} (got '${value}')`);
    }
    return value as StorageMode;
  }
}
