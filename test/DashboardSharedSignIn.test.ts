/**
 * Tests DASHBOARD.sharedSignIn (dashboard/lib/SharedSignIn.ts): the cookie settings it gives the
 * Shibboleth construct, the cookie name the dashboard Lambda reads (AUTH_COOKIE_NAME), and the
 * synth failures for settings that can't work.
 */
import { App } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { JwtTools } from 'shibboleth-sp';
import { ShibbolethAtEdgeConstruct } from 'shibboleth-sp-at-edge';
import { IContext as LandscapeContext } from '../context/IContext';
import { AuthenticatedContext, AuthenticationMode } from '../dashboard/context/IContext';
import { createDashboardStack } from '../dashboard/lib/DashboardStacks';
import { applySharedSignIn, SHARED_SIGN_IN_COOKIE_NAME } from '../dashboard/lib/SharedSignIn';

const SUBDOMAIN = 'mock.person.huron.cssnprd.warhen.work';

const makeDashboard = (overrides: Record<string, any> = {}): Record<string, any> => ({
  STACK_ID: 'hp-dashboard',
  AUTHENTICATION_MODE: AuthenticationMode.AUTHENTICATED,
  ORIGIN: { originType: 'function-url', httpsPort: 443, subdomain: SUBDOMAIN, appAuthorization: true },
  APP_LOGIN_HEADER: 'SHIB-HANDLER',
  APP_LOGOUT_HEADER: 'SHIB-IDP-LOGOUT',
  SHIBBOLETH: {
    entityId: 'test-entity', idpCert: 'test-cert', entryPoint: 'test-entry', logoutUrl: 'test-logout',
    secret: {
      secretArn: 'arn:aws:secretsmanager:us-east-2:111111111111:secret:test-aBcDeF', refreshInterval: '3600000',
      samlPrivateKeySecretFld: 'saml-pk', samlCertSecretFld: 'saml-cert', jwtPrivateKeySecretFld: 'jwt-pk', jwtPublicKeySecretFld: 'jwt-pub'
    }
  },
  ...overrides,
});

const makeLandscape = (dashboard: Record<string, any>): LandscapeContext => ({
  STACK_ID: 'huron-person-fargate-processor',
  ACCOUNT: '111111111111',
  REGION: 'us-east-2',
  S3: { chunksBucket: 'huron-person-chunks' },
  TAGS: { Landscape: 'mock', Service: 'svc', Function: 'fn' },
  PREVIOUS_STORAGE_TYPE: 'dynamodb',
  DASHBOARD: dashboard,
} as any);

describe('applySharedSignIn', () => {
  it('leaves the context unchanged when sharedSignIn is absent or false', () => {
    for (const settings of [makeDashboard(), makeDashboard({ sharedSignIn: false })]) {
      const context = settings as AuthenticatedContext;
      expect(applySharedSignIn(context)).toBe(context);
    }
  });

  it('scopes the shared cookie name to the dashboard domain', () => {
    const { SHIBBOLETH } = applySharedSignIn(makeDashboard({ sharedSignIn: true }) as AuthenticatedContext);
    expect(SHIBBOLETH).toMatchObject({ cookieDomain: 'person.huron.cssnprd.warhen.work', cookieName: SHARED_SIGN_IN_COOKIE_NAME });
    expect(SHARED_SIGN_IN_COOKIE_NAME).not.toBe(JwtTools.COOKIE_NAME);
  });

  it('fails without ORIGIN.subdomain', () => {
    const context = makeDashboard({ sharedSignIn: true, ORIGIN: { originType: 'function-url', httpsPort: 443 } });
    expect(() => applySharedSignIn(context as AuthenticatedContext)).toThrow(/needs DASHBOARD.ORIGIN.subdomain/);
  });

  it('fails when the context also sets the cookie name or domain itself', () => {
    for (const cookie of [{ cookieName: 'other' }, { cookieDomain: 'warhen.work' }]) {
      const settings = makeDashboard({ sharedSignIn: true });
      settings.SHIBBOLETH = { ...settings.SHIBBOLETH, ...cookie };
      expect(() => applySharedSignIn(settings as AuthenticatedContext)).toThrow(/remove them from the context/);
    }
  });
});

describe('createDashboardStack cookie settings', () => {
  let shibContexts: AuthenticatedContext[];

  beforeEach(() => {
    shibContexts = [];
    // The real construct reads its secret, certificate and Route 53 at synth
    jest.spyOn(ShibbolethAtEdgeConstruct, 'getInstance').mockImplementation(async (params: any) => {
      shibContexts.push(params.context);
      return undefined as any;
    });
  });

  afterEach(() => jest.restoreAllMocks());

  const synth = async (dashboard: Record<string, any>) => {
    // No stack is bundled, so the Lambda entry is not built (no esbuild or generated templates needed)
    const app = new App({ context: { 'aws:cdk:bundling-stacks': [] } });
    const stack = await createDashboardStack(app, makeLandscape(dashboard));
    const [fn] = Object.values(Template.fromStack(stack!).findResources('AWS::Lambda::Function'));
    return { shibContext: shibContexts[0], authCookieName: fn.Properties.Environment.Variables.AUTH_COOKIE_NAME };
  };

  it('gives the edge functions and the Lambda the shared cookie for a sharedSignIn landscape', async () => {
    const { shibContext, authCookieName } = await synth(makeDashboard({ sharedSignIn: true }));
    expect(shibContext.SHIBBOLETH).toMatchObject({ cookieDomain: 'person.huron.cssnprd.warhen.work', cookieName: SHARED_SIGN_IN_COOKIE_NAME });
    expect(authCookieName).toBe(SHARED_SIGN_IN_COOKIE_NAME);
  });

  it('keeps the host-only default cookie for a landscape without sharedSignIn', async () => {
    const { shibContext, authCookieName } = await synth(makeDashboard());
    expect(shibContext.SHIBBOLETH.cookieDomain).toBeUndefined();
    expect(shibContext.SHIBBOLETH.cookieName).toBeUndefined();
    expect(authCookieName).toBe(JwtTools.COOKIE_NAME);
  });

  it('gives the Lambda a cookie name set directly in the context', async () => {
    const dashboard = makeDashboard();
    dashboard.SHIBBOLETH = { ...dashboard.SHIBBOLETH, cookieName: 'mock-auth' };
    expect((await synth(dashboard)).authCookieName).toBe('mock-auth');
  });

  it('fails synth for sharedSignIn on a public dashboard', async () => {
    const dashboard = makeDashboard({ sharedSignIn: true, AUTHENTICATION_MODE: AuthenticationMode.PUBLIC });
    await expect(synth(dashboard)).rejects.toThrow(/sharedSignIn needs AUTHENTICATION_MODE "authenticated"/);
  });
});

describe('Authenticator cookie name', () => {
  const ORIGINAL_ENV = process.env;
  afterEach(() => { process.env = ORIGINAL_ENV; });

  const token = (user: string) =>
    `header.${Buffer.from(JSON.stringify({ [JwtTools.TOKEN_NAME]: { user } })).toString('base64')}.signature`;

  // Authentication.ts reads its environment when it loads, so load it fresh for each environment
  const userFor = (cookie: string, env: Record<string, string>): string | undefined => {
    process.env = { ...ORIGINAL_ENV, ...env };
    let user: string | undefined;
    jest.isolateModules(() => {
      const { Authenticator } = require('../dashboard/src/services/Authentication');
      user = new Authenticator({ headers: { cookie } } as any).user;
    });
    return user;
  };

  it('reads the cookie named by AUTH_COOKIE_NAME', () => {
    const cookie = `${JwtTools.COOKIE_NAME}=${token('host-only')}; ${SHARED_SIGN_IN_COOKIE_NAME}=${token('shared')}`;
    expect(userFor(cookie, { AUTH_COOKIE_NAME: SHARED_SIGN_IN_COOKIE_NAME })).toBe('shared');
  });

  it('reads the default cookie without AUTH_COOKIE_NAME', () => {
    const cookie = `${SHARED_SIGN_IN_COOKIE_NAME}=${token('shared')}; ${JwtTools.COOKIE_NAME}=${token('host-only')}`;
    expect(userFor(cookie, {})).toBe('host-only');
  });
});
