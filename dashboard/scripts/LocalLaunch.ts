/**
 * Starts the dashboard's local server (dashboard/src/LocalServer.ts) at http://localhost:3000/dashboard
 * with the environment a launch option calls for:
 *
 *   ts-node dashboard/scripts/LocalLaunch.ts [--signed-in] [--mocks [--storage-mode dynamodb|s3]]
 *
 * --signed-in  Show the page as a landscape that requires sign-in does: sets NODE_ENV=development and
 *              placeholder APP_LOGIN_HEADER/APP_LOGOUT_HEADER names, which turn off public mode and so
 *              show the sign-out control. No user name shows (there is no JWT cookie on localhost), and
 *              /logout is a 404 here: only the deployed edge functions answer it.
 * --mocks      Use the mock services (LOCALHOST_WITH_MOCKS=true), so no AWS access is needed; the
 *              navigator lists the mock registry's made-up landscapes. Without it, .env is loaded
 *              (AWS_PROFILE, REGION, LANDSCAPE), and the navigator lists the real landscape registry
 *              with LANDSCAPE selected.
 * --storage-mode  With --mocks, the storage mode the mock run reporting reports (MOCK_STORAGE_MODE):
 *              dynamodb (the default) or s3, which keeps no per-person change lists. From npm, pass it
 *              after --: npm run dashboard:dev:signed-in:mocks -- --storage-mode s3
 *
 * The variables are set here rather than in the npm script because npm runs scripts in cmd.exe on
 * Windows, which can't take VAR=value prefixes. Build the templates first (the npm scripts do).
 */

const FLAGS = ['--signed-in', '--mocks'] as const;
type Flag = typeof FLAGS[number];

/** The mock storage modes (MockRunReporting.ts's MOCK_STORAGE_MODES, not imported so nothing loads before the environment is set) */
const STORAGE_MODES = ['dynamodb', 's3'] as const;
type StorageMode = typeof STORAGE_MODES[number];

export type LaunchOptions = {
  flags: Set<Flag>;
  storageMode?: StorageMode;
};

export const parseOptions = (args: string[]): LaunchOptions => {
  const flags = new Set<Flag>();
  let storageMode: StorageMode | undefined;
  const unknown: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--storage-mode') {
      const value = args[++i];
      if (!STORAGE_MODES.includes(value as StorageMode)) {
        throw new Error(`--storage-mode must be followed by ${STORAGE_MODES.join(' or ')} (got ${value === undefined ? 'nothing' : `'${value}'`})`);
      }
      storageMode = value as StorageMode;
    } else if (FLAGS.includes(arg as Flag)) {
      flags.add(arg as Flag);
    } else {
      unknown.push(arg);
    }
  }
  if (unknown.length > 0) {
    throw new Error(`Unknown option(s): ${unknown.join(', ')} (expected ${FLAGS.join(' and/or ')}, and --storage-mode with --mocks)`);
  }
  if (storageMode && !flags.has('--mocks')) {
    throw new Error('--storage-mode applies only to the mocks: add --mocks');
  }
  return { flags, storageMode };
};

/** The variables the options call for, which take precedence over .env (dotenv doesn't override) */
export const launchEnvironment = ({ flags, storageMode }: LaunchOptions): Record<string, string> => ({
  NODE_ENV: 'development',
  ...(flags.has('--signed-in') ? { APP_LOGIN_HEADER: 'app-login', APP_LOGOUT_HEADER: 'app-logout' } : {}),
  ...(flags.has('--mocks') ? { LOCALHOST_WITH_MOCKS: 'true' } : {}),
  ...(storageMode ? { MOCK_STORAGE_MODE: storageMode } : {})
});

if (require.main === module) {
  let options: LaunchOptions;
  try {
    options = parseOptions(process.argv.slice(2));
  } catch (error) {
    console.error((error as Error).message);
    process.exit(1);
  }
  Object.assign(process.env, launchEnvironment(options));
  if (!options.flags.has('--mocks')) {
    require('dotenv').config();
  }
  // Required, not imported, so it loads after the environment is set: it reads some variables at load
  // time (e.g. the app login/logout header names) and starts listening as soon as it loads.
  require('../src/LocalServer');
}
