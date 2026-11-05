/**
 * Starts the dashboard's local server (dashboard/src/LocalServer.ts) at http://localhost:3000/dashboard
 * with the environment a launch option calls for:
 *
 *   ts-node dashboard/scripts/LocalLaunch.ts [--signed-in] [--mocks]
 *
 * --signed-in  Show the page as a landscape that requires sign-in does: sets NODE_ENV=development and
 *              placeholder APP_LOGIN_HEADER/APP_LOGOUT_HEADER names, which turn off public mode and so
 *              show the sign-out control. No user name shows (there is no JWT cookie on localhost), and
 *              /logout is a 404 here: only the deployed edge functions answer it.
 * --mocks      Use the mock services (LOCALHOST_WITH_MOCKS=true), so no AWS access is needed; the
 *              navigator lists the mock registry's made-up landscapes. Without it, .env is loaded
 *              (AWS_PROFILE, REGION, LANDSCAPE), and the navigator lists the real landscape registry
 *              with LANDSCAPE selected.
 *
 * The variables are set here rather than in the npm script because npm runs scripts in cmd.exe on
 * Windows, which can't take VAR=value prefixes. Build the templates first (the npm scripts do).
 */

const OPTIONS = ['--signed-in', '--mocks'] as const;
type Option = typeof OPTIONS[number];

export const parseOptions = (args: string[]): Set<Option> => {
  const unknown = args.filter(arg => !OPTIONS.includes(arg as Option));
  if (unknown.length > 0) {
    throw new Error(`Unknown option(s): ${unknown.join(', ')} (expected ${OPTIONS.join(' and/or ')})`);
  }
  return new Set(args as Option[]);
};

/** The variables an option set calls for, which take precedence over .env (dotenv doesn't override) */
export const launchEnvironment = (options: Set<Option>): Record<string, string> => ({
  NODE_ENV: 'development',
  ...(options.has('--signed-in') ? { APP_LOGIN_HEADER: 'app-login', APP_LOGOUT_HEADER: 'app-logout' } : {}),
  ...(options.has('--mocks') ? { LOCALHOST_WITH_MOCKS: 'true' } : {})
});

if (require.main === module) {
  let options: Set<Option>;
  try {
    options = parseOptions(process.argv.slice(2));
  } catch (error) {
    console.error((error as Error).message);
    process.exit(1);
  }
  Object.assign(process.env, launchEnvironment(options));
  if (!options.has('--mocks')) {
    require('dotenv').config();
  }
  // Required, not imported, so it loads after the environment is set: it reads some variables at load
  // time (e.g. the app login/logout header names) and starts listening as soon as it loads.
  require('../src/LocalServer');
}
