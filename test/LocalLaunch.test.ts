/**
 * Tests the option handling of the dashboard's local launcher (dashboard/scripts/LocalLaunch.ts).
 */
import { launchEnvironment, parseOptions } from '../dashboard/scripts/LocalLaunch';

describe('LocalLaunch', () => {
  it('sets only NODE_ENV without options (public mode, .env loaded)', () => {
    expect(launchEnvironment(parseOptions([]))).toEqual({ NODE_ENV: 'development' });
  });

  it('sets the app login/logout header names for --signed-in', () => {
    expect(launchEnvironment(parseOptions(['--signed-in']))).toEqual({
      NODE_ENV: 'development', APP_LOGIN_HEADER: 'app-login', APP_LOGOUT_HEADER: 'app-logout'
    });
  });

  it('adds the mock services for --mocks, in either order', () => {
    const expected = {
      NODE_ENV: 'development', APP_LOGIN_HEADER: 'app-login', APP_LOGOUT_HEADER: 'app-logout', LOCALHOST_WITH_MOCKS: 'true'
    };
    expect(launchEnvironment(parseOptions(['--signed-in', '--mocks']))).toEqual(expected);
    expect(launchEnvironment(parseOptions(['--mocks', '--signed-in']))).toEqual(expected);
  });

  it('rejects unknown options', () => {
    expect(() => parseOptions(['--signed-in', '--bogus'])).toThrow('Unknown option(s): --bogus');
  });
});
