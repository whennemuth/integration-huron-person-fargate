/**
 * The dashboard's Shibboleth secret (DASHBOARD.SHIBBOLETH.secret): the SAML service provider key and
 * certificate, the JWT key pair that signs the sign-in cookie, and the CloudFront challenge. Only the
 * Shibboleth edge functions read it at runtime (the dashboard Lambda never does), and the Shibboleth
 * construct checks that it exists at synth.
 *
 * Reads and creates the secret with shibboleth-sp-at-edge's SecretsManagerSecret, which formats the
 * keys as PEM and generates the JWT key pair. A SAML key and certificate not given are generated with
 * shibboleth-sp's Keys. Its SecretsManager.createOrUpdateSecrets() is not used:
 * it names the secret per landscape (every landscape that shares a sign-in shares one secret), takes
 * its values from the environment and command line, and updating an existing secret with it drops
 * the JWT keys.
 */
import { SecretsManagerClient, Tag } from '@aws-sdk/client-secrets-manager';
import { randomBytes } from 'crypto';
import { readFileSync } from 'fs';
import { TestEnvironment } from 'integration-core';
import {
  SecretsManagerSecret,
  SecretsManagerSecretParms,
  SecretFieldNames,
  IContext as ShibSpContext
} from 'shibboleth-sp-at-edge';
import { Keys } from 'shibboleth-sp';
import { AuthenticationMode, toDashboardContext } from '../context/IContext';

/** The secret's values by role, whatever the context names their fields */
export type ShibbolethSecretValues = {
  samlPrivateKey?: string;
  samlCert?: string;
  jwtPrivateKey?: string;
  jwtPublicKey?: string;
  cloudFrontChallenge?: string;
}

export type CreateShibbolethSecretParms = {
  /**
   * Default: a new key and self-signed certificate. Give both or neither. The IdP knows the service
   * provider (entityId) by its certificate, so a new one must be registered with the IdP before
   * sign-in works; keep an existing registration's key and certificate by giving them here.
   */
  samlPrivateKey?: string;
  samlCert?: string;
  /** Default: a new key pair. Give both or neither. */
  jwtPrivateKey?: string;
  jwtPublicKey?: string;
  /** Default: random. Only an ALB origin checks it; the dashboard's function URL origin ignores it. */
  cloudFrontChallenge?: string;
  /** Default: the name in SHIBBOLETH.secret.secretArn */
  secretName?: string;
}

export type CreatedShibbolethSecret = {
  /** For SHIBBOLETH.secret.secretArn */
  arn: string;
  /** The landscape the secret is for (its name ends with "/<landscape>"), or undefined if shared */
  landscape?: string;
  /** The SAML certificate, if it was generated: the IdP must be given it */
  generatedSamlCert?: string;
}

// Field name of the CloudFront challenge (CLOUDFRONT_CHALLENGE_HEADER_NAME, not exported by the package)
const CLOUDFRONT_CHALLENGE_FLD = 'cloudfront-challenge';

/**
 * The secret name in a secret ARN, without the random suffix Secrets Manager appends
 * (arn:aws:secretsmanager:<region>:<account>:secret:<name>-AbCdEf). Anything else is taken as a name.
 */
export const secretNameOf = (secretArn: string): string => {
  const [, name] = secretArn.split(':secret:');
  return name === undefined ? secretArn : name.replace(/-[A-Za-z0-9]{6}$/, '');
}

/** The authenticated dashboard context of the active landscape (context/context.json) */
const activeDashboardContext = (): ShibSpContext => {
  const landscapeContext = require('../../context/context.json');
  const { DASHBOARD } = landscapeContext;
  if (DASHBOARD?.AUTHENTICATION_MODE !== AuthenticationMode.AUTHENTICATED) {
    throw new Error('context/context.json has no authenticated DASHBOARD section, so no Shibboleth secret');
  }
  return toDashboardContext(landscapeContext, DASHBOARD) as ShibSpContext;
}

/**
 * The landscape a secret name is for: by convention a landscape's own secret ends with
 * "/<landscape>" (e.g. shibsp/hp-dashboard/staging), and any other name is a shared secret
 * (e.g. shibsp/hp-dashboard), whose landscapes share one sign-in.
 * @returns The landscape, or undefined for a shared secret
 */
export const secretLandscapeOf = (secretName: string, landscape?: string): string | undefined =>
  landscape && secretName.split('/').pop() === landscape ? landscape : undefined;

export class ShibbolethSecret {
  private secret?: SecretsManagerSecret;
  private readonly secretArn: string;
  private readonly fldNames: SecretFieldNames;
  private readonly region: string;
  private readonly readRegion: string;
  private readonly entityId: string;
  private readonly tags: ShibSpContext['TAGS'];
  private readonly client?: SecretsManagerClient;

  /**
   * @param context The Shibboleth context whose secret this is. Default: the active landscape's
   * dashboard (context/context.json).
   * @param client Default: a client for the secret's region
   */
  constructor(context?: ShibSpContext, client?: SecretsManagerClient) {
    const { REGION, TAGS, SHIBBOLETH: { entityId, secret } } = context ?? activeDashboardContext();
    const { secretArn, samlPrivateKeySecretFld, samlCertSecretFld, jwtPrivateKeySecretFld, jwtPublicKeySecretFld } = secret;
    this.secretArn = secretArn;
    this.fldNames = { samlPrivateKeySecretFld, samlCertSecretFld, jwtPrivateKeySecretFld, jwtPublicKeySecretFld };
    // A new secret goes in the context's region
    this.region = REGION || 'us-east-1';
    // An existing one is in the region its ARN names, which is where the edge functions read it
    this.readRegion = secretArn.startsWith('arn:') ? secretArn.split(':')[3] : this.region;
    this.entityId = entityId;
    this.tags = TAGS;
    this.client = client;
  }

  private newSecret = (parms: Partial<SecretsManagerSecretParms> = {}): SecretsManagerSecret => {
    const { secretArn: secretName, fldNames, readRegion: region, client } = this;
    return new SecretsManagerSecret({ secretName, fldNames, region, client, ...parms });
  }

  /**
   * The context's tags, without Landscape for a shared secret: it belongs to no one landscape
   */
  private tagsFor = (landscape: string | undefined): Tag[] => {
    const { Service, Function, CostCenter, Ticket } = this.tags ?? {};
    return Object.entries({ Service, Function, Landscape: landscape, CostCenter, Ticket })
      .filter(([, Value]) => !!Value)
      .map(([Key, Value]) => ({ Key, Value }));
  }

  /**
   * @returns The secret, or undefined if it does not exist. Its read() gives the field values.
   */
  public async getSecret(): Promise<SecretsManagerSecret | undefined> {
    if (!this.secret) {
      const secret = this.newSecret();
      if (await secret.exists()) {
        this.secret = secret;
      }
    }
    return this.secret;
  }

  /**
   * @returns The secret's values by role, or undefined if it does not exist
   */
  public async getValues(): Promise<ShibbolethSecretValues | undefined> {
    const fields = await (await this.getSecret())?.read();
    if (!fields) {
      return undefined;
    }
    const { samlPrivateKeySecretFld, samlCertSecretFld, jwtPrivateKeySecretFld, jwtPublicKeySecretFld } = this.fldNames;
    return {
      samlPrivateKey: fields[samlPrivateKeySecretFld],
      samlCert: fields[samlCertSecretFld],
      jwtPrivateKey: fields[jwtPrivateKeySecretFld],
      jwtPublicKey: fields[jwtPublicKeySecretFld],
      cloudFrontChallenge: fields[CLOUDFRONT_CHALLENGE_FLD]
    };
  }

  /**
   * Create the secret, under the context's field names. Fails if it already exists: replacing its JWT
   * key pair would end every sign-in of every landscape that shares it.
   * @returns The new secret's ARN, and the SAML certificate if it was generated
   */
  public async createSecret(parms: CreateShibbolethSecretParms = {}): Promise<CreatedShibbolethSecret> {
    const {
      jwtPrivateKey, jwtPublicKey,
      cloudFrontChallenge = randomBytes(32).toString('hex'),
      secretName = secretNameOf(this.secretArn)
    } = parms;
    let { samlPrivateKey, samlCert } = parms;
    if (!samlPrivateKey !== !samlCert) {
      throw new Error('Give both the SAML private key and certificate, or neither for a new pair');
    }
    if (!jwtPrivateKey !== !jwtPublicKey) {
      throw new Error('Give both JWT keys, or neither for a new key pair');
    }

    const landscape = secretLandscapeOf(secretName, this.tags?.Landscape);
    const scope = landscape
      ? `the ${landscape} landscape of the person to Huron dashboard`
      : 'the person to Huron dashboard landscapes that share a sign-in';
    const secret = this.newSecret({
      secretName,
      region: this.region,
      description: `SAML service provider key and certificate (${this.entityId}), JWT keys and CloudFront challenge for ${scope}`,
      Tags: this.tagsFor(landscape)
    });
    if (await secret.exists()) {
      throw new Error(`Secret ${secretName} already exists`);
    }
    let generatedSamlCert: string | undefined;
    if (!samlPrivateKey || !samlCert) {
      // node-forge writes CRLF; the secret's PEM values use LF
      const keys = new Keys();
      samlPrivateKey = keys.privateKeyPEM.replace(/\r\n/g, '\n');
      samlCert = generatedSamlCert = keys.certificatePEM.replace(/\r\n/g, '\n').trim();
    }
    secret
      .setCloudFrontChallenge(cloudFrontChallenge)
      .setSamlPrivateKeySecret(samlPrivateKey)
      .setSamlCertSecret(samlCert);
    if (jwtPrivateKey && jwtPublicKey) {
      secret.setJwtPrivateKeySecret(jwtPrivateKey).setJwtPublicKeySecret(jwtPublicKey);
    }
    const { ARN } = await secret.save();
    if (!ARN) {
      throw new Error(`Secret ${secretName} was saved, but no ARN came back`);
    }
    this.secret = undefined;
    return { arn: ARN, landscape, generatedSamlCert };
  }
}



/**
 * RUN MANUALLY: read or create the active landscape's dashboard Shibboleth secret
 * (DASHBOARD.SHIBBOLETH.secret in context/context.json).
 */
if (require.main === module) {
  const testEnvironment = TestEnvironment('DASHBOARD_SHIBBOLETH_SECRET');
  [
    'TASK',
    // create: the SAML key and certificate from another secret, or from PEM files, else a new pair
    'SOURCE_SECRET_ARN',
    'SOURCE_SAML_PRIVATE_KEY_FLD',
    'SOURCE_SAML_CERT_FLD',
    'SAML_PRIVATE_KEY_FILE',
    'SAML_CERT_FILE',
    // create, optional
    'SECRET_NAME',
    'CLOUDFRONT_CHALLENGE'
  ].forEach(testEnvironment.getVar);

  const task = testEnvironment.getVar('TASK');
  const requireVar = (key: string): string => {
    const value = testEnvironment.getVar(key);
    if (!value) {
      throw new Error(`Missing required DASHBOARD_SHIBBOLETH_SECRET_${key} environment variable for the ${task} task!`);
    }
    return value;
  };

  // The SAML key and certificate to create the secret with: none for a new pair
  const samlValues = async (context: ShibSpContext): Promise<{ samlPrivateKey?: string, samlCert?: string }> => {
    const sourceSecretArn = testEnvironment.getVar('SOURCE_SECRET_ARN');
    if (!sourceSecretArn) {
      if (!testEnvironment.getVar('SAML_PRIVATE_KEY_FILE') && !testEnvironment.getVar('SAML_CERT_FILE')) {
        return {};
      }
      return {
        samlPrivateKey: readFileSync(requireVar('SAML_PRIVATE_KEY_FILE'), 'utf8'),
        samlCert: readFileSync(requireVar('SAML_CERT_FILE'), 'utf8')
      };
    }
    // The source secret's own field names, which needn't be the context's
    const { secret } = context.SHIBBOLETH;
    const source = new ShibbolethSecret({ ...context, SHIBBOLETH: { ...context.SHIBBOLETH, secret: {
      ...secret,
      secretArn: sourceSecretArn,
      samlPrivateKeySecretFld: requireVar('SOURCE_SAML_PRIVATE_KEY_FLD'),
      samlCertSecretFld: requireVar('SOURCE_SAML_CERT_FLD')
    } } });
    const { samlPrivateKey, samlCert } = await source.getValues() ?? {};
    if (!samlPrivateKey || !samlCert) {
      throw new Error(`Secret ${sourceSecretArn} has no SAML private key and certificate under the given field names`);
    }
    return { samlPrivateKey, samlCert };
  }

  (async () => {
    const context = activeDashboardContext();
    const shibbolethSecret = new ShibbolethSecret(context);
    const { secretArn } = context.SHIBBOLETH.secret;

    switch (task) {
      case 'read': {
        const values = await shibbolethSecret.getValues();
        if (!values) {
          console.log(`Secret ${secretArn} does not exist`);
          break;
        }
        // Which values are present, never the private ones
        const { samlCert, jwtPublicKey } = values;
        const present = Object.fromEntries(Object.entries(values).map(([role, value]) => [role, !!value]));
        console.log(`Secret ${secretArn}:`, JSON.stringify({ present, samlCert, jwtPublicKey }, null, 2));
        break;
      }
      case 'create': {
        const { arn, landscape, generatedSamlCert } = await shibbolethSecret.createSecret({
          ...await samlValues(context),
          secretName: testEnvironment.getVar('SECRET_NAME'),
          cloudFrontChallenge: testEnvironment.getVar('CLOUDFRONT_CHALLENGE')
        });
        console.log(landscape
          ? `Created secret ${arn} for the ${landscape} landscape.\nSet it as DASHBOARD.SHIBBOLETH.secret.secretArn in context.${landscape}.json.`
          : `Created shared secret ${arn} (its name does not end with "/${context.TAGS.Landscape}").\n` +
            `Set it as DASHBOARD.SHIBBOLETH.secret.secretArn in the context of each landscape that shares the sign-in.`);
        if (generatedSamlCert) {
          console.warn(`\nThe SAML key and certificate are new. Sign-in fails until the IdP has this certificate ` +
            `for ${context.SHIBBOLETH.entityId} (the service provider's metadata):\n${generatedSamlCert}`);
        }
        break;
      }
      default:
        throw new Error(`Unknown DASHBOARD_SHIBBOLETH_SECRET_TASK: ${task} (read | create)`);
    }
  })().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
