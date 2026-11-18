import { JwtTools } from 'shibboleth-sp';

/**
 * Reads the signed-in user's claims from the sign-in token cookie (authorization-plan.md, decision 8). Roles depend on
 * those claims, so the Lambda verifies the token's signature itself rather than trusting that the edge functions and
 * origin verification let only signed tokens through.
 */
export interface ClaimsReader {
  /** The token's user claims, or undefined without a token whose signature and expiry check out */
  claims(cookieHeader: string | undefined): unknown | undefined;
}

/** The Lambda's environment variable holding the sign-in's JWT public key (PEM), set at synth from the Shibboleth secret */
export const JWT_PUBLIC_KEY = 'JWT_PUBLIC_KEY';

/**
 * Verifies the token (RS256, its expiry) with shibboleth-sp's JwtTools against the sign-in's public key: the key the
 * edge functions' private key pairs with, or the local sign-in's.
 */
export class VerifiedClaimsReader implements ClaimsReader {
  private readonly jwtTools: JwtTools;

  constructor(publicKeyPem: string, private readonly cookieName: string = JwtTools.COOKIE_NAME) {
    this.jwtTools = new JwtTools({ cookieName });
    this.jwtTools.resetPublicKey(publicKeyPem);
  }

  claims(cookieHeader: string | undefined): unknown | undefined {
    if (!cookieHeader) {
      return undefined;
    }
    // JwtTools returns a token made from any IMPERSONATE_* environment variables, unverified, in place of the
    // cookie's: a local testing aid that must never decide who someone is here
    if (Object.keys(process.env).some(name => name.toUpperCase().startsWith('IMPERSONATE_'))) {
      console.error('Refusing to read sign-in claims: IMPERSONATE_* variables are set, which shibboleth-sp would use unverified');
      return undefined;
    }
    // JwtTools reads a CloudFront-style request (header values as [{ key, value }])
    const token = this.jwtTools.getToken({ headers: { cookie: [{ key: 'cookie', value: cookieHeader }] } } as any, true);
    return token?.[JwtTools.TOKEN_NAME]?.user ?? undefined;
  }
}

/** Without a public key nothing can be verified, so no one is signed in: every API request is denied */
export class NoKeyClaimsReader implements ClaimsReader {
  private warned = false;

  claims(cookieHeader: string | undefined): unknown | undefined {
    if (cookieHeader && !this.warned) {
      this.warned = true;
      console.error(`${JWT_PUBLIC_KEY} is not set: sign-in tokens can't be verified, so every request is treated as no one's`);
    }
    return undefined;
  }
}

/**
 * The reader for the environment: verified with JWT_PUBLIC_KEY, reading the AUTH_COOKIE_NAME cookie. Read at each
 * call, not when the module loads, so the local sign-in can set its own key before the first request.
 */
export const claimsReaderFromEnvironment = (env: NodeJS.ProcessEnv = process.env): ClaimsReader => {
  const publicKey = env[JWT_PUBLIC_KEY] || '';
  const cookieName = env.AUTH_COOKIE_NAME || JwtTools.COOKIE_NAME;
  const key = `${cookieName}\n${publicKey}`;
  if (cached?.key !== key) {
    cached = { key, reader: publicKey ? new VerifiedClaimsReader(publicKey, cookieName) : new NoKeyClaimsReader() };
  }
  return cached.reader;
};

let cached: { key: string; reader: ClaimsReader } | undefined;
