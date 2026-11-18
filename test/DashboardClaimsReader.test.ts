/**
 * ClaimsReader (dashboard/src/authorization/ClaimsReader.ts, authorization-plan.md decision 8): the Lambda believes a
 * sign-in token's claims only when its signature checks out against the sign-in's public key.
 */
import { JwtTools } from 'shibboleth-sp';
import {
  claimsReaderFromEnvironment, NoKeyClaimsReader, VerifiedClaimsReader
} from '../dashboard/src/authorization/ClaimsReader';
import { TestSignIn } from './support/TestSignIn';

const signIn = new TestSignIn();
const user = { buPrincipal: ['jdoe'], eduPersonPrincipalName: ['jdoe@bu.edu'] };

describe('VerifiedClaimsReader', () => {
  const reader = new VerifiedClaimsReader(signIn.publicKey);

  it('reads the claims of a token signed with the pair of its key', () => {
    expect(reader.claims(`other=1; ${signIn.cookie(user)}; more=2`)).toEqual(user);
  });

  it('reads the cookie it is given the name of, and no other', () => {
    const shared = new VerifiedClaimsReader(signIn.publicKey, 'hp-dashboard-auth');
    expect(shared.claims(signIn.cookie(user, 'hp-dashboard-auth'))).toEqual(user);
    expect(shared.claims(signIn.cookie(user))).toBeUndefined();
  });

  it('refuses a token signed with another key pair', () => {
    expect(reader.claims(new TestSignIn().cookie(user))).toBeUndefined();
  });

  it('refuses a token whose claims were changed after signing', () => {
    const [header, , signature] = signIn.token(user).split('.');
    const payload = Buffer.from(JSON.stringify({ [JwtTools.TOKEN_NAME]: { user: { buPrincipal: ['admin'] } } })).toString('base64url');
    expect(reader.claims(`${JwtTools.COOKIE_NAME}=${header}.${payload}.${signature}`)).toBeUndefined();
  });

  it('refuses an unsigned token, as the Lambda used to read them', () => {
    const payload = Buffer.from(JSON.stringify({ [JwtTools.TOKEN_NAME]: { user } })).toString('base64');
    expect(reader.claims(`${JwtTools.COOKIE_NAME}=header.${payload}.signature`)).toBeUndefined();
  });

  it('refuses an expired token', () => {
    const expired = signIn.token(user, { expiresIn: '-1s' });
    expect(reader.claims(`${JwtTools.COOKIE_NAME}=${expired}`)).toBeUndefined();
  });

  it('reads nothing without a cookie', () => {
    expect(reader.claims(undefined)).toBeUndefined();
    expect(reader.claims('')).toBeUndefined();
  });

  it("never takes shibboleth-sp's IMPERSONATE_ variables for a sign-in", () => {
    process.env.IMPERSONATE_buPrincipal = 'admin';
    jest.spyOn(console, 'error').mockImplementation(() => {});
    try {
      expect(reader.claims(signIn.cookie(user))).toBeUndefined();
    } finally {
      delete process.env.IMPERSONATE_buPrincipal;
      jest.restoreAllMocks();
    }
  });
});

describe('claimsReaderFromEnvironment', () => {
  it('verifies with JWT_PUBLIC_KEY, reading AUTH_COOKIE_NAME', () => {
    const reader = claimsReaderFromEnvironment({ JWT_PUBLIC_KEY: signIn.publicKey, AUTH_COOKIE_NAME: 'hp-dashboard-auth' });
    expect(reader).toBeInstanceOf(VerifiedClaimsReader);
    expect(reader.claims(signIn.cookie(user, 'hp-dashboard-auth'))).toEqual(user);
  });

  it('keeps one reader while the environment is the same', () => {
    const env = { JWT_PUBLIC_KEY: signIn.publicKey };
    expect(claimsReaderFromEnvironment(env)).toBe(claimsReaderFromEnvironment({ ...env }));
  });

  it('signs no one in without a key, saying why once', () => {
    const error = jest.spyOn(console, 'error').mockImplementation(() => {});
    try {
      const reader = claimsReaderFromEnvironment({});
      expect(reader).toBeInstanceOf(NoKeyClaimsReader);
      expect(reader.claims(signIn.cookie(user))).toBeUndefined();
      reader.claims(signIn.cookie(user));
      expect(error).toHaveBeenCalledTimes(1);
    } finally {
      jest.restoreAllMocks();
    }
  });
});
