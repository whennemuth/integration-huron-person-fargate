/**
 * Tests the local launcher's mock sign-in (dashboard/scripts/MockSignIn.ts): the claims it signs in with, the
 * JWT it signs as the sign-in edge functions do, the request it hands the Lambda handler, and /logout. Then,
 * through the handler with a landscape's sign-in settings (APP_AUTHORIZATION=true), that a decorated request is
 * served signed in where a bare one is sent to sign in.
 */
import { JwtTools } from 'shibboleth-sp';
import { MockSignIn } from '../dashboard/scripts/MockSignIn';
import { LambdaFunctionUrlEvent } from '../dashboard/src/handlers/DashboardTypes';
import { SIGN_OUT_PATH } from '../dashboard/src/handlers/NavigationBar';

const COOKIE = 'hp-dashboard-auth';

/** The JWT a decorated request carries, as JwtTools reads it at the edge (CloudFront's header shape) */
const tokenOf = (headers: Record<string, string>, publicKey?: string): any => {
  const jwtTools = new JwtTools({ cookieName: COOKIE });
  if (publicKey) {
    jwtTools.resetPublicKey(publicKey);
  }
  return jwtTools.getToken({ headers: { cookie: [{ key: 'Cookie', value: headers.cookie }] } } as any, !!publicKey);
};

describe('MockSignIn', () => {
  const mockSignIn = MockSignIn.fromEnvironment({
    USER_PRINCIPAL: 'jdoe', USER_EPPN: 'jdoe@bu.edu', USER_MAIL: ' ', AUTH_COOKIE_NAME: COOKIE
  });

  it('signs in with array-valued claims from LocalLaunch.env, leaving out those not set', () => {
    expect(mockSignIn.claims).toEqual({ buPrincipal: ['jdoe'], eduPersonPrincipalName: ['jdoe@bu.edu'] });
    expect(MockSignIn.claimsFrom({ USER_PRINCIPAL: 'a', USER_MAIL: 'a@bu.edu', USER_GIVEN_NAME: 'Ann', USER_SURNAME: 'Lee' }))
      .toEqual({ buPrincipal: ['a'], mail: ['a@bu.edu'], givenName: ['Ann'], sn: ['Lee'] });
    expect(mockSignIn.userName).toBe('jdoe@bu.edu');
    expect(MockSignIn.fromEnvironment({ USER_PRINCIPAL: 'jdoe' }).userName).toBe('jdoe');
  });

  it('needs a user principal, and uses the default cookie name when the Lambda names none', () => {
    expect(() => MockSignIn.fromEnvironment({ USER_EPPN: 'jdoe@bu.edu' })).toThrow('needs a buPrincipal claim (USER_PRINCIPAL)');
    expect(MockSignIn.fromEnvironment({ USER_PRINCIPAL: 'jdoe' }).cookieName).toBe(JwtTools.COOKIE_NAME);
  });

  it('marks the request signed in, presenting its JWT cookie in place of one of the same name', () => {
    const headers: Record<string, string> = { cookie: `other=1; ${COOKIE}=stale; theme=dark` };
    mockSignIn.decorate({ headers });
    expect(headers.authenticated).toBe('true');
    const cookies = headers.cookie.split('; ');
    expect(cookies.filter(cookie => cookie.startsWith(`${COOKIE}=`))).toHaveLength(1);
    expect(cookies).toEqual(expect.arrayContaining(['other=1', 'theme=dark']));
    expect(cookies[0]).not.toBe(`${COOKIE}=stale`);
  });

  it('adds the cookie header to a request without one', () => {
    const headers: Record<string, string> = {};
    mockSignIn.decorate({ headers });
    expect(headers.cookie.startsWith(`${COOKIE}=`)).toBe(true);
  });

  it('signs the JWT as the sign-in does, verifiable with its public key', () => {
    const headers: Record<string, string> = {};
    mockSignIn.decorate({ headers });
    const verified = tokenOf(headers, mockSignIn.publicKey);
    expect(verified[JwtTools.TOKEN_NAME]).toEqual({ sub: 'jdoe@bu.edu', user: mockSignIn.claims });
    // A key pair of its own: another mock sign-in's key does not verify it
    expect(tokenOf(headers, MockSignIn.fromEnvironment({ USER_PRINCIPAL: 'x' }).publicKey)).toBeNull();
  });

  it('answers /logout itself, and passes every other path on', () => {
    const response = mockSignIn.respond(SIGN_OUT_PATH);
    expect(response?.statusCode).toBe(200);
    expect(response?.headers?.['Content-Type']).toContain('text/html');
    expect(response?.body).toContain('Signed in as jdoe@bu.edu (mock sign-in)');
    expect(response?.body).toContain('href="/dashboard"');
    expect(mockSignIn.respond('/dashboard')).toBeUndefined();
    expect(mockSignIn.respond('/api/runs')).toBeUndefined();
  });

  it('escapes the user name on the /logout page', () => {
    expect(MockSignIn.fromEnvironment({ USER_PRINCIPAL: '<b>x</b>' }).respond(SIGN_OUT_PATH)?.body).not.toContain('<b>x</b>');
  });
});

describe('MockSignIn in front of the dashboard handler', () => {
  /** A landscape's sign-in settings, as its Lambda has them */
  const SIGN_IN_ENV = {
    APP_LOGIN_HEADER: 'SHIB-HANDLER',
    APP_LOGOUT_HEADER: 'SHIB-IDP-LOGOUT',
    APP_AUTHORIZATION: 'true',
    AUTH_COOKIE_NAME: COOKIE,
    LOCALHOST_WITH_MOCKS: 'true'
  };

  /** GET a path from a fresh handler (Authentication.ts reads the sign-in settings when it loads) */
  const get = async (path: string, decorator?: MockSignIn): Promise<{ status: number, body: string, user?: any }> => {
    const saved = { ...process.env };
    Object.assign(process.env, SIGN_IN_ENV);
    jest.spyOn(console, 'log').mockImplementation(() => {});
    try {
      let result: any;
      let user: any;
      await jest.isolateModulesAsync(async () => {
        const { handler } = require('../dashboard/src/handlers/DashboardHandler');
        const { Authenticator } = require('../dashboard/src/services/Authentication');
        const event = {
          rawPath: path,
          headers: { 'shib-handler': encodeURIComponent('https://idp.example.edu/sso') } as Record<string, string>,
          requestContext: { http: { method: 'GET', path } },
          isBase64Encoded: false
        } as unknown as LambdaFunctionUrlEvent;
        decorator?.decorate(event);
        user = new Authenticator(event).user;
        result = await handler(event);
      });
      return { status: result.statusCode, body: `${result.body}`, user };
    } finally {
      process.env = saved;
      jest.restoreAllMocks();
    }
  };

  const mockSignIn = MockSignIn.fromEnvironment({ ...SIGN_IN_ENV, USER_PRINCIPAL: 'jdoe' });

  it('sends a bare request to sign in, as the Lambda does without the edge functions', async () => {
    const { status } = await get('/dashboard');
    expect(status).toBe(302);
  });

  it('serves a decorated request signed in, naming the user in the top bar', async () => {
    const { status, body, user } = await get('/dashboard', mockSignIn);
    expect(status).toBe(200);
    expect(user).toEqual({ buPrincipal: ['jdoe'] });
    expect(body).toMatch(/href="(\/|&#x2F;)logout"/);
    expect(body).toContain('jdoe');
  });

  it('lets a decorated request reach the API', async () => {
    const { status } = await get('/api/capabilities', mockSignIn);
    expect(status).toBe(200);
  });
});
