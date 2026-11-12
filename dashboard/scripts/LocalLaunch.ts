/**
 * Starts the dashboard's local server (dashboard/src/LocalServer.ts) at http://localhost:3000/dashboard as a
 * landscape's dashboard Lambda would run, with the settings in dashboard/scripts/LocalLaunch.env (git-ignored;
 * see LocalLaunch.env.example). The repository's .env is never read.
 *
 *   npm run dashboard:dev [-- <flags>]     (flags: see LocalLaunchOptions.ts, e.g. -- --mocks --storage-mode s3)
 *
 * Without mocks (MOCKED=false), it looks up the deployed dashboard Lambda of LANDSCAPE in REGION at every launch
 * and runs the server with that Lambda's environment (its secrets left out), under LocalLaunch.env's
 * overrides, so the server reads the landscape's tables, config secret and registry with your AWS_PROFILE's
 * credentials. With mocks, nothing is looked up and no AWS access is needed.
 *
 * With AUTHENTICATION=mock, the mock sign-in (MockSignIn.ts) stands in front of the handler for the sign-in edge
 * functions: every request arrives signed in as USER_PRINCIPAL, with a signed JWT cookie, and /logout is answered
 * locally.
 *
 * With WATCH=true it runs the server under nodemon, which restarts it when a file under dashboard/ changes.
 * The environment is resolved once, here, and handed to each restart (LOCAL_LAUNCH_RESOLVED), so AWS is called
 * once per launch.
 *
 * The launcher is TypeScript rather than a shell script because npm runs scripts in cmd.exe on Windows, where
 * neither VAR=value prefixes nor sh work. Build the templates first (the npm script does).
 */
import { spawn } from 'child_process';
import { existsSync, readFileSync } from 'fs';
import { join, relative } from 'path';
import { parse } from 'dotenv';
import { DashboardFunctionNotFoundError, LambdaFunction } from './LambdaFunction';
import { LaunchEnvironment, LOCAL_MOCK_SIGN_IN, LocalLaunchOptions } from './LocalLaunchOptions';
import type { LocalRequestDecorator } from '../src/LocalServer';

export const ENV_FILE = join(__dirname, 'LocalLaunch.env');
const EXAMPLE_FILE = join(__dirname, 'LocalLaunch.env.example');

/** Set in the environment handed to a nodemon restart: the environment is already resolved */
export const LOCAL_LAUNCH_RESOLVED = 'LOCAL_LAUNCH_RESOLVED';

/** What nodemon watches, and the files whose change restarts the server */
const WATCH_DIRECTORY = join(__dirname, '..');
const WATCH_EXTENSIONS = 'ts,js,mustache,css,json';

const readSettings = (): Record<string, string> => {
  if (!existsSync(ENV_FILE)) {
    throw new Error(`No ${relative(process.cwd(), ENV_FILE)}: copy ${relative(process.cwd(), EXAMPLE_FILE)} to it and fill it in`);
  }
  return parse(readFileSync(ENV_FILE));
};

/**
 * Why the Lambda lookup failed, with a hint at the likely cause: the settings when AWS answered that the landscape
 * has no dashboard function, otherwise (an expired session, missing credentials, no access) the AWS session.
 */
export const lookupFailure = (landscape: string | undefined, error: unknown): Error => {
  const hint = error instanceof DashboardFunctionNotFoundError
    ? 'Check LANDSCAPE and REGION in LocalLaunch.env, or set MOCKED=true.'
    : 'Check that your AWS session is current (e.g. aws sso login), or set MOCKED=true.';
  const message = error instanceof Error ? error.message : String(error);
  return new Error(`Can't look up the ${landscape} dashboard Lambda's environment: ${message}\n${hint}`);
};

/** The deployed dashboard Lambda's environment */
const lookUpLambdaEnvironment = async (options: LocalLaunchOptions, awsProfile?: string): Promise<Record<string, string>> => {
  const lambdaFunction = options.lambdaFunction();
  console.log(`Looking up ${lambdaFunction.name} (${lambdaFunction.region})${awsProfile ? ` as AWS_PROFILE=${awsProfile}` : ''}...`);
  try {
    return await lambdaFunction.getEnvironment();
  }
  catch (error) {
    throw lookupFailure(options.landscape, error);
  }
};

/** Says how the server is about to run, and with what (secrets never reach here, and are masked anyway) */
const describe = (options: LocalLaunchOptions, { variables, unset, overrides }: LaunchEnvironment): void => {
  const source = options.mocked ? 'the mock services (no AWS access)' : `the ${options.landscape} landscape's dashboard Lambda environment`;
  console.log(`\nLocal launch: ${source}; sign-in: ${options.authentication}; watch: ${options.watch}`);
  const shown = LambdaFunction.masked(variables);
  const width = Math.max(...Object.keys(shown).map(name => name.length));
  Object.entries(shown).forEach(([name, value]) => {
    console.log(`  ${name.padEnd(width)}  ${value}${overrides.includes(name) ? '   (LocalLaunch.env override)' : ''}`);
  });
  if (unset.length > 0) {
    console.log(`  unset: ${unset.join(', ')} (public mode)`);
  }
  console.log('');
};

const applyEnvironment = ({ variables, unset }: LaunchEnvironment): void => {
  unset.forEach(name => { delete process.env[name]; });
  Object.assign(process.env, variables);
};

/**
 * Start the server, with the mock sign-in in front of its handler if the environment asks for it. Required, not
 * imported: the server's modules read some variables when they load.
 */
const startServer = (): void => {
  const { startLocalServer } = require('../src/LocalServer') as typeof import('../src/LocalServer');
  let requestDecorator: LocalRequestDecorator | undefined;
  if (process.env[LOCAL_MOCK_SIGN_IN] === 'true') {
    const { MockSignIn } = require('./MockSignIn') as typeof import('./MockSignIn');
    const mockSignIn = MockSignIn.fromEnvironment(process.env);
    console.log(`Mock sign-in: every request is signed in as ${mockSignIn.userName} (JWT cookie ${mockSignIn.cookieName})`);
    requestDecorator = mockSignIn;
  }
  startLocalServer({ requestDecorator });
};

/** Run the server under nodemon, each restart taking the environment already applied here */
const startWatching = (): void => {
  const nodemon = require.resolve('nodemon/bin/nodemon.js');
  const exec = `node -r ts-node/register/transpile-only "${__filename}"`;
  const child = spawn(process.execPath, [nodemon, '--watch', WATCH_DIRECTORY, '--ext', WATCH_EXTENSIONS, '--exec', exec], {
    stdio: 'inherit',
    env: { ...process.env, [LOCAL_LAUNCH_RESOLVED]: 'true' }
  });
  child.on('exit', code => process.exit(code ?? 0));
};

export const launch = async (args: string[]): Promise<void> => {
  if (process.env[LOCAL_LAUNCH_RESOLVED] === 'true') {
    // A nodemon restart: the launcher that started nodemon resolved the environment
    startServer();
    return;
  }
  const settings = readSettings();
  const options = new LocalLaunchOptions(settings, args);
  let lambdaEnvironment: Record<string, string> = {};
  if (options.needsLambdaEnvironment) {
    // The lookup uses the credentials LocalLaunch.env names
    if (settings.AWS_PROFILE) {
      process.env.AWS_PROFILE = settings.AWS_PROFILE;
    }
    lambdaEnvironment = await lookUpLambdaEnvironment(options, settings.AWS_PROFILE);
  }
  const environment = options.environment(lambdaEnvironment);
  describe(options, environment);
  applyEnvironment(environment);
  if (options.watch) {
    startWatching();
  }
  else {
    startServer();
  }
};

if (require.main === module) {
  launch(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
