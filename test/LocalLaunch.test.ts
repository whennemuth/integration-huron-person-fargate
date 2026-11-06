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

  it('sets the mock storage mode for --storage-mode, with --mocks', () => {
    expect(launchEnvironment(parseOptions(['--mocks', '--storage-mode', 's3']))).toEqual({
      NODE_ENV: 'development', LOCALHOST_WITH_MOCKS: 'true', MOCK_STORAGE_MODE: 's3'
    });
    expect(launchEnvironment(parseOptions(['--storage-mode', 'dynamodb', '--signed-in', '--mocks'])).MOCK_STORAGE_MODE).toBe('dynamodb');
  });

  it('rejects --storage-mode without a storage mode, or without --mocks', () => {
    expect(() => parseOptions(['--mocks', '--storage-mode'])).toThrow('--storage-mode must be followed by dynamodb or s3 (got nothing)');
    expect(() => parseOptions(['--mocks', '--storage-mode', 'file'])).toThrow("(got 'file')");
    expect(() => parseOptions(['--storage-mode', 's3'])).toThrow('--storage-mode applies only to the mocks: add --mocks');
  });
});
