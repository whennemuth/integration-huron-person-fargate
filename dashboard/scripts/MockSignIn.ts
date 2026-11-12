/**
 * The mock sign-in (LocalLaunch.env AUTHENTICATION=mock): stands in, on the local server, for the CloudFront edge
 * functions that sign a user in to the deployed dashboard, so the local server runs signed in as a configured
 * user (USER_PRINCIPAL) without an IdP.
 *
 * On every request, before the Lambda handler sees it, it does what the edge functions do for a signed-in user:
 * - adds the `authenticated: true` header, which the Lambda's Authenticator requires when APP_AUTHORIZATION is true;
 * - presents the JWT cookie (AUTH_COOKIE_NAME) carrying the user's claims, which Authenticator.user reads.
 *
 * The JWT is made exactly as the sign-in makes it: shibboleth-sp's JwtTools signs { sub, user: <claims> } under
 * its token name (RS256), with array-valued claims as the IdP sends them (buPrincipal: ["jdoe"]). The key pair
 * is generated when the server starts, and its public key is kept (publicKey) for a signature check.
 *
 * /logout is answered here: the deployed edge functions answer it, and the mock user can't sign out.
 */
import { generateKeyPairSync } from 'crypto';
import { JwtTools } from 'shibboleth-sp';
import { SIGN_OUT_PATH } from '../src/handlers/NavigationBar';
import type { LocalRequestDecorator, LocalResponse } from '../src/LocalServer';

/** The SAML attributes the JWT carries, each array-valued as the IdP sends them */
export type MockClaims = Record<string, string[]>;

/** LocalLaunch.env's settings for the mock user, and the claims they become */
const CLAIM_SETTINGS: Record<string, string> = {
  USER_PRINCIPAL: 'buPrincipal',
  USER_EPPN: 'eduPersonPrincipalName',
  USER_MAIL: 'mail',
  USER_GIVEN_NAME: 'givenName',
  USER_SURNAME: 'sn'
};

export class MockSignIn implements LocalRequestDecorator {
  readonly cookieName: string;
  readonly claims: MockClaims;
  /** The public key of the pair the JWT is signed with (PEM) */
  readonly publicKey: string;
  private readonly token: string;

  constructor(params: { claims: MockClaims, cookieName?: string }) {
    const { claims, cookieName = JwtTools.COOKIE_NAME } = params;
    if (!claims.buPrincipal?.[0]) {
      throw new Error('The mock sign-in needs a buPrincipal claim (USER_PRINCIPAL)');
    }
    this.claims = claims;
    this.cookieName = cookieName;
    const { publicKey, privateKey } = generateKeyPairSync('rsa', {
      modulusLength: 2048,
      publicKeyEncoding: { type: 'spki', format: 'pem' },
      privateKeyEncoding: { type: 'pkcs8', format: 'pem' }
    });
    this.publicKey = publicKey;
    this.token = MockSignIn.sign(cookieName, privateKey, { sub: this.subject, user: claims });
  }

  /**
   * The mock sign-in for the user LocalLaunch.env names (USER_PRINCIPAL, and optionally USER_EPPN, USER_MAIL,
   * USER_GIVEN_NAME, USER_SURNAME), with the JWT cookie the landscape's Lambda reads (AUTH_COOKIE_NAME).
   */
  static fromEnvironment(env: NodeJS.ProcessEnv | Record<string, string | undefined>): MockSignIn {
    return new MockSignIn({ claims: MockSignIn.claimsFrom(env), cookieName: env.AUTH_COOKIE_NAME || undefined });
  }

  static claimsFrom(env: NodeJS.ProcessEnv | Record<string, string | undefined>): MockClaims {
    return Object.fromEntries(Object.entries(CLAIM_SETTINGS)
      .map(([setting, claim]) => [claim, env[setting]?.trim()])
      .filter((entry): entry is [string, string] => !!entry[1])
      .map(([claim, value]) => [claim, [value]]));
  }

  /** A JWT as the sign-in signs it, through JwtTools, which writes it only into a Set-Cookie header */
  private static sign(cookieName: string, privateKey: string, payload: object): string {
    const jwtTools = new JwtTools({ cookieName });
    jwtTools.resetPrivateKey(privateKey);
    const response: { headers: Record<string, { key: string, value: string }[]> } = { headers: {} };
    jwtTools.setCookieInResponse(response, payload);
    const setCookie = response.headers['set-cookie']?.[0]?.value ?? '';
    const token = setCookie.startsWith(`${cookieName}=`) ? setCookie.slice(cookieName.length + 1).split(';')[0] : '';
    if (!token) {
      throw new Error(`JwtTools set no ${cookieName} cookie to take the mock sign-in's token from`);
    }
    return token;
  }

  /** The user's name as the top bar shows it */
  get userName(): string {
    return (this.claims.eduPersonPrincipalName ?? this.claims.buPrincipal)[0];
  }

  private get subject(): string {
    return this.userName;
  }

  /** Mark the request signed in, and present the JWT cookie in place of any of the same name */
  decorate(event: { headers: { [name: string]: string } }): void {
    const { headers } = event;
    const others = (headers.cookie ?? '').split(/;\s*/).filter(cookie => cookie && cookie.split('=')[0] !== this.cookieName);
    headers.cookie = [`${this.cookieName}=${this.token}`, ...others].join('; ');
    headers.authenticated = 'true';
  }

  /** /logout, which the edge functions answer when deployed: the mock user can't sign out */
  respond(path: string): LocalResponse | undefined {
    if (path !== SIGN_OUT_PATH) {
      return undefined;
    }
    const user = this.userName.replace(/[&<>"']/g, c => `&#${c.charCodeAt(0)};`);
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
      body: '<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Mock sign-in</title>' +
        '<meta name="viewport" content="width=device-width, initial-scale=1"></head>' +
        '<body style="font-family: system-ui, sans-serif; max-width: 36rem; margin: 3rem auto; padding: 0 1rem; line-height: 1.5">' +
        `<h1 style="font-size: 1.25rem">Signed in as ${user} (mock sign-in)</h1>` +
        '<p>This local server signs every request in as the user LocalLaunch.env names (AUTHENTICATION=mock), so there is ' +
        'no signing out here. On the deployed dashboard, the sign-in edge functions answer Sign out.</p>' +
        '<p><a href="/dashboard">Back to the dashboard</a></p></body></html>'
    };
  }
}
