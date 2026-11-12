/**
 * Tests what the dashboard's local launcher decides (dashboard/scripts/LocalLaunchOptions.ts) from
 * LocalLaunch.env and the command line: the switches, flags overriding them, the checks before starting, and
 * the environment the local server runs with. Then the hint LocalLaunch.ts gives when the Lambda lookup fails.
 */
import { DashboardFunctionNotFoundError, LambdaFunction } from '../dashboard/scripts/LambdaFunction';
import { lookupFailure } from '../dashboard/scripts/LocalLaunch';
import { LOCAL_MOCK_SIGN_IN, LocalLaunchOptions, MOCK_SIGN_IN_HEADERS } from '../dashboard/scripts/LocalLaunchOptions';

/** A LocalLaunch.env for a real landscape, signed out (public mode) */
const FILE = {
  LANDSCAPE: 'preview',
  REGION: 'us-east-2',
  AWS_PROFILE: 'infnprd',
  MOCKED: 'false',
  AUTHENTICATION: 'none',
  WATCH: 'false',
  MOCK_STORAGE_MODE: 'dynamodb',
  USER_PRINCIPAL: 'jdoe'
};

/** A deployed dashboard Lambda's environment */
const LAMBDA = {
  LANDSCAPE: 'preview',
  REGION: 'us-east-2',
  APP_AUTHORIZATION: 'true',
  APP_LOGIN_HEADER: 'SHIB-HANDLER',
  APP_LOGOUT_HEADER: 'SHIB-IDP-LOGOUT',
  DYNAMODB_STATISTICS_TABLE_NAME: 'huron-person-fargate-processor-statistics-preview',
  ORIGIN_VERIFY_SECRET: 'cf-origin-verify-secret'
};

const MOCKED_FILE = { ...FILE, MOCKED: 'true' };

describe('LocalLaunchOptions', () => {

  describe('switches', () => {
    it('reads the switches from LocalLaunch.env', () => {
      const options = new LocalLaunchOptions({ ...MOCKED_FILE, AUTHENTICATION: 'mock', WATCH: 'TRUE', MOCK_STORAGE_MODE: 's3' });
      expect(options).toMatchObject({ mocked: true, authentication: 'mock', watch: true, storageMode: 's3', allowProduction: false });
      expect(options.needsLambdaEnvironment).toBe(false);
    });

    it('defaults to a real landscape in public mode, without watching', () => {
      const options = new LocalLaunchOptions({ LANDSCAPE: 'preview', REGION: 'us-east-2' });
      expect(options).toMatchObject({ mocked: false, authentication: 'none', watch: false, storageMode: 'dynamodb' });
      expect(options.needsLambdaEnvironment).toBe(true);
      expect(options.lambdaFunction().name).toBe('hp-dashboard-preview-app-function');
    });

    it('lets a flag override a switch for one run', () => {
      expect(new LocalLaunchOptions(FILE, ['--mocks', '--watch', '--auth', 'mock', '--storage-mode', 's3']))
        .toMatchObject({ mocked: true, watch: true, authentication: 'mock', storageMode: 's3' });
      expect(new LocalLaunchOptions({ ...MOCKED_FILE, WATCH: 'true' }, ['--no-mocks', '--no-watch']))
        .toMatchObject({ mocked: false, watch: false });
      expect(new LocalLaunchOptions(FILE, ['--allow-production']).allowProduction).toBe(true);
    });

    it('names the setting or option that is malformed or unknown', () => {
      expect(() => new LocalLaunchOptions({ ...FILE, MOCKED: 'yes' })).toThrow("MOCKED in LocalLaunch.env must be true or false (got 'yes')");
      expect(() => new LocalLaunchOptions({ ...FILE, AUTHENTICATION: 'saml' })).toThrow("AUTHENTICATION in LocalLaunch.env must be none or mock (got 'saml')");
      expect(() => new LocalLaunchOptions({ ...FILE, MOCK_STORAGE_MODE: 'file' })).toThrow("MOCK_STORAGE_MODE in LocalLaunch.env must be dynamodb or s3 (got 'file')");
      expect(() => new LocalLaunchOptions(FILE, ['--bogus'])).toThrow('Unknown option(s): --bogus');
      expect(() => new LocalLaunchOptions(FILE, ['--auth'])).toThrow('--auth must be followed by none or mock (got nothing)');
      expect(() => new LocalLaunchOptions(MOCKED_FILE, ['--storage-mode', 'file'])).toThrow("(got 'file')");
    });

    it('says the real sign-in is not built', () => {
      expect(() => new LocalLaunchOptions({ ...FILE, AUTHENTICATION: 'shibboleth' })).toThrow('AUTHENTICATION=shibboleth (the real sign-in) is not built');
    });
  });

  describe('settings that cannot work together', () => {
    it('needs the landscape and region without mocks, and not with them', () => {
      expect(() => new LocalLaunchOptions({ ...FILE, LANDSCAPE: '' })).toThrow('set LANDSCAPE and REGION');
      expect(() => new LocalLaunchOptions({ ...FILE, REGION: undefined as unknown as string })).toThrow('set LANDSCAPE and REGION');
      expect(() => new LocalLaunchOptions({ MOCKED: 'true' })).not.toThrow();
    });

    it('takes --storage-mode only with mocks', () => {
      expect(() => new LocalLaunchOptions(FILE, ['--storage-mode', 's3'])).toThrow('--storage-mode applies only to the mocks');
    });

    it('needs a user to sign in as for the mock sign-in', () => {
      expect(() => new LocalLaunchOptions({ ...MOCKED_FILE, AUTHENTICATION: 'mock', USER_PRINCIPAL: ' ' })).toThrow('AUTHENTICATION=mock signs in as USER_PRINCIPAL');
    });

  });

  describe('environment', () => {
    it('runs with the Lambda environment, without its secrets, under LocalLaunch.env', () => {
      const { variables, overrides } = new LocalLaunchOptions({ ...FILE, AUTHENTICATION: 'none' }).environment(LAMBDA);
      expect(variables).toMatchObject({
        DYNAMODB_STATISTICS_TABLE_NAME: LAMBDA.DYNAMODB_STATISTICS_TABLE_NAME,
        APP_AUTHORIZATION: 'true',
        AWS_PROFILE: 'infnprd',
        NODE_ENV: 'development',
        LOCALHOST_WITH_MOCKS: 'false'
      });
      expect(variables).not.toHaveProperty('ORIGIN_VERIFY_SECRET');
      // LANDSCAPE and REGION are the Lambda's own values: not overrides
      expect(overrides).toEqual([]);
    });

    it('never passes the launcher\'s own switches to the server', () => {
      const { variables } = new LocalLaunchOptions({ ...FILE, ALLOW_PRODUCTION: 'false' }).environment(LAMBDA);
      ['MOCKED', 'AUTHENTICATION', 'WATCH', 'ALLOW_PRODUCTION'].forEach(name => expect(variables).not.toHaveProperty(name));
    });

    it('lets LocalLaunch.env override a Lambda value, and lists the overrides', () => {
      const { variables, overrides } = new LocalLaunchOptions({ ...FILE, LANDSCAPE_TONE: 'test', DYNAMODB_STATISTICS_TABLE_NAME: 'other' })
        .environment({ ...LAMBDA, LANDSCAPE_TONE: 'mock' });
      expect(variables).toMatchObject({ LANDSCAPE_TONE: 'test', DYNAMODB_STATISTICS_TABLE_NAME: 'other' });
      expect(overrides).toEqual(['DYNAMODB_STATISTICS_TABLE_NAME', 'LANDSCAPE_TONE']);
    });

    it('removes the app login/logout headers for public mode, with no mock sign-in', () => {
      const { variables, unset } = new LocalLaunchOptions(FILE).environment(LAMBDA);
      expect(variables).not.toHaveProperty('APP_LOGIN_HEADER');
      expect(variables).not.toHaveProperty('APP_LOGOUT_HEADER');
      expect(variables[LOCAL_MOCK_SIGN_IN]).toBe('false');
      expect(unset).toEqual(['APP_LOGIN_HEADER', 'APP_LOGOUT_HEADER']);
    });

    it('signs in with the mock sign-in against a real landscape, keeping the Lambda\'s sign-in settings', () => {
      const { variables, unset } = new LocalLaunchOptions({ ...FILE, AUTHENTICATION: 'mock' }).environment(LAMBDA);
      expect(variables).toMatchObject({
        [LOCAL_MOCK_SIGN_IN]: 'true',
        APP_AUTHORIZATION: 'true',
        APP_LOGIN_HEADER: 'SHIB-HANDLER',
        APP_LOGOUT_HEADER: 'SHIB-IDP-LOGOUT',
        USER_PRINCIPAL: 'jdoe'
      });
      expect(unset).toEqual([]);
    });

    it('uses no Lambda environment with mocks, and signs in with placeholder headers', () => {
      const { variables, unset } = new LocalLaunchOptions({ ...MOCKED_FILE, AUTHENTICATION: 'mock', MOCK_STORAGE_MODE: 's3' }).environment(LAMBDA);
      expect(variables).toMatchObject({ LOCALHOST_WITH_MOCKS: 'true', MOCK_STORAGE_MODE: 's3', ...MOCK_SIGN_IN_HEADERS });
      expect(variables).not.toHaveProperty('DYNAMODB_STATISTICS_TABLE_NAME');
      expect(variables).not.toHaveProperty('APP_AUTHORIZATION');
      expect(unset).toEqual([]);
    });

    it('keeps sign-in headers LocalLaunch.env gives with mocks', () => {
      const { variables } = new LocalLaunchOptions({ ...MOCKED_FILE, AUTHENTICATION: 'mock', APP_LOGIN_HEADER: 'a', APP_LOGOUT_HEADER: 'b' }).environment();
      expect(variables).toMatchObject({ APP_LOGIN_HEADER: 'a', APP_LOGOUT_HEADER: 'b' });
    });
  });

  describe('production guard', () => {
    const PRODUCTION = { ...LAMBDA, LANDSCAPE_TONE: 'production' };

    it('refuses a production landscape without mocks', () => {
      expect(() => new LocalLaunchOptions(FILE).environment(PRODUCTION)).toThrow('The preview landscape is production (LANDSCAPE_TONE)');
    });

    it('is not hidden by an override of the tone', () => {
      expect(() => new LocalLaunchOptions({ ...FILE, LANDSCAPE_TONE: 'test' }).environment(PRODUCTION)).toThrow('is production');
      expect(() => new LocalLaunchOptions({ ...FILE, LANDSCAPE_TONE: 'production' }).environment(LAMBDA)).toThrow('is production');
    });

    it('lets ALLOW_PRODUCTION or --allow-production through, and never stops the mocks', () => {
      expect(() => new LocalLaunchOptions({ ...FILE, ALLOW_PRODUCTION: 'true' }).environment(PRODUCTION)).not.toThrow();
      expect(() => new LocalLaunchOptions(FILE, ['--allow-production']).environment(PRODUCTION)).not.toThrow();
      expect(() => new LocalLaunchOptions(MOCKED_FILE).environment(PRODUCTION)).not.toThrow();
    });
  });

  it('masks nothing it shows, having no secrets', () => {
    const { variables } = new LocalLaunchOptions(FILE).environment(LAMBDA);
    expect(Object.values(LambdaFunction.masked(variables))).not.toContain(LambdaFunction.MASK);
  });
});

describe('lookupFailure', () => {
  const SESSION_HINT = 'Check that your AWS session is current';

  it('points at the settings, not the AWS session, when the landscape has no dashboard function', () => {
    const { message } = lookupFailure('nosuch', new DashboardFunctionNotFoundError('hp-dashboard-nosuch-app-function', 'nosuch', 'us-east-2'));
    expect(message).toContain("Can't look up the nosuch dashboard Lambda's environment: No dashboard Lambda function hp-dashboard-nosuch-app-function");
    expect(message).toContain('Check LANDSCAPE and REGION in LocalLaunch.env, or set MOCKED=true.');
    expect(message).not.toContain(SESSION_HINT);
  });

  it('points at the AWS session for any other failure', () => {
    const expired = Object.assign(new Error('The security token included in the request is expired'), { name: 'ExpiredTokenException' });
    expect(lookupFailure('preview', expired).message).toContain(`The security token included in the request is expired\n${SESSION_HINT}`);
    expect(lookupFailure('preview', 'not an Error').message).toContain(`not an Error\n${SESSION_HINT}`);
  });
});
