import { AuthenticatedContext } from '../context/IContext';
import { ShibbolethSecret } from '../src/ShibbolethSecret';

/**
 * The sign-in's JWT public key, for the Lambda to verify sign-in tokens with (authorization-plan.md, decision 8;
 * ClaimsReader). Read at synth from the landscape's Shibboleth secret (SHIBBOLETH.secret), whose JWT key pair the edge
 * functions sign tokens with, and passed to the Lambda as JWT_PUBLIC_KEY. A public key is no secret; the private key
 * stays out of the Lambda, which is given no access to the secret.
 */
export class JwtPublicKey {
  /** The PEM public key, or a synth failure saying what to do: without it, no request could be authorized */
  static async read(context: AuthenticatedContext): Promise<string> {
    const values = await new ShibbolethSecret(context).getValues();
    const publicKey = values?.jwtPublicKey?.trim();
    if (!publicKey) {
      const { secretArn, jwtPublicKeySecretFld } = context.SHIBBOLETH.secret;
      throw new Error(values
        ? `The Shibboleth secret ${secretArn} has no JWT public key (field "${jwtPublicKeySecretFld}"): the dashboard ` +
          `Lambda verifies sign-in tokens with it. Give the secret a JWT key pair (dashboard/src/ShibbolethSecret.ts).`
        : `The Shibboleth secret ${secretArn} can't be found: the dashboard Lambda verifies sign-in tokens with its JWT public key.`);
    }
    if (!/^-----BEGIN (RSA )?PUBLIC KEY-----/.test(publicKey)) {
      throw new Error(`The JWT public key in ${context.SHIBBOLETH.secret.secretArn} is not a PEM public key`);
    }
    return publicKey;
  }
}
