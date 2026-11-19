/**
 * The dashboard handler, offline, for tests that drive it as a browser would: loaded fresh with an environment
 * (Authentication.ts reads the sign-in settings when it loads), and requests with or without a sign-in cookie.
 */
import { LambdaFunctionUrlEvent, LambdaFunctionUrlResult } from '../../dashboard/src/handlers/DashboardTypes';
import { TestSignIn } from './TestSignIn';

export type Handler = (event: LambdaFunctionUrlEvent) => Promise<LambdaFunctionUrlResult>;

export const COOKIE = 'hp-dashboard-auth';

/** A landscape's sign-in settings, offline, playing a mock landscape (for Start from scratch), trusting signIn's tokens */
export const signedInEnvironment = (signIn: TestSignIn): Record<string, string> => ({
  LOCAL_OFFLINE: 'true',
  LANDSCAPE: 'mock',
  APP_LOGIN_HEADER: 'SHIB-HANDLER',
  APP_LOGOUT_HEADER: 'SHIB-IDP-LOGOUT',
  APP_AUTHORIZATION: 'true',
  AUTH_COOKIE_NAME: COOKIE,
  USER_PRINCIPAL: 'jdoe',
  ...signIn.environment
});

/** The handler and ServiceProvider, loaded fresh with the environment; restore() puts the environment back */
export const loadHandler = (env: Record<string, string>, unset: string[] = []): { handler: Handler; ServiceProvider: any; restore: () => void } => {
  const saved = process.env;
  process.env = { ...saved, ...env };
  ['AWS_LAMBDA_FUNCTION_NAME', ...unset].forEach(name => delete process.env[name]);
  let loaded: any;
  jest.isolateModules(() => {
    loaded = {
      handler: require('../../dashboard/src/handlers/DashboardHandler').handler,
      ServiceProvider: require('../../dashboard/src/services/ServiceProvider').ServiceProvider
    };
  });
  return { ...loaded, restore: () => { process.env = saved; } };
};

export const event = (
  method: string, path: string, options: { body?: object; cookie?: string; query?: Record<string, string> } = {}
): LambdaFunctionUrlEvent => ({
  rawPath: path,
  headers: { authenticated: 'true', ...(options.cookie !== undefined && { cookie: options.cookie }) },
  queryStringParameters: options.query ?? {},
  requestContext: { http: { method, path } },
  body: options.body ? JSON.stringify(options.body) : undefined,
  isBase64Encoded: false
} as unknown as LambdaFunctionUrlEvent);

export const json = (result: LambdaFunctionUrlResult): any => JSON.parse(`${result.body}`);
