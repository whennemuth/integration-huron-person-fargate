/**
 * Sign-in tokens for tests, signed as the sign-in edge functions sign them (shibboleth-sp's JwtTools, RS256) with a key
 * pair made for the test file. The dashboard Lambda verifies them (ClaimsReader) against JWT_PUBLIC_KEY, which
 * `environment` gives.
 */
import { generateKeyPairSync } from 'crypto';
import { JwtTools } from 'shibboleth-sp';

export class TestSignIn {
  readonly publicKey: string;
  private readonly privateKey: string;

  constructor() {
    const { publicKey, privateKey } = generateKeyPairSync('rsa', {
      modulusLength: 2048,
      publicKeyEncoding: { type: 'spki', format: 'pem' },
      privateKeyEncoding: { type: 'pkcs8', format: 'pem' }
    });
    this.publicKey = publicKey;
    this.privateKey = privateKey;
  }

  /** The variable that makes the Lambda trust this key pair's tokens */
  get environment(): { JWT_PUBLIC_KEY: string } {
    return { JWT_PUBLIC_KEY: this.publicKey };
  }

  /** A token carrying these user claims, as the sign-in puts it in its cookie */
  token(user: object, options: { cookieName?: string; expiresIn?: string } = {}): string {
    const { cookieName = JwtTools.COOKIE_NAME, expiresIn } = options;
    const jwtTools = new JwtTools({ cookieName });
    jwtTools.resetPrivateKey(this.privateKey);
    if (expiresIn) {
      jwtTools.setExpiration(expiresIn);
    }
    const response: { headers: Record<string, { key: string; value: string }[]> } = { headers: {} };
    jwtTools.setCookieInResponse(response, { sub: 'test', user });
    return response.headers['set-cookie'][0].value.slice(cookieName.length + 1).split(';')[0];
  }

  /** The cookie header presenting a token for these claims */
  cookie(user: object, cookieName: string = JwtTools.COOKIE_NAME): string {
    return `${cookieName}=${this.token(user, { cookieName })}`;
  }
}
