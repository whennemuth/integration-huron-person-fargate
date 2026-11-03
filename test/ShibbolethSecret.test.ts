/**
 * Tests dashboard/src/ShibbolethSecret.ts against an in-memory Secrets Manager: reading the secret by
 * role under the context's field names, and creating it (new JWT key pair, new SAML key and certificate
 * when none are given, tags, description and region by whether it is shared or a landscape's own,
 * refusal to replace one).
 */
import { SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { createPublicKey, generateKeyPairSync, X509Certificate } from 'crypto';
import { IContext as ShibSpContext } from 'shibboleth-sp-at-edge';
import { secretLandscapeOf, secretNameOf, ShibbolethSecret } from '../dashboard/src/ShibbolethSecret';

const SECRET_NAME = 'shibsp/dashboard';
const SECRET_ARN = `arn:aws:secretsmanager:us-east-2:111111111111:secret:${SECRET_NAME}-AbCdEf`;

const TAGS = { Service: 'svc', Function: 'fn', Landscape: 'staging', CostCenter: 'cc', Ticket: 'tkt' };

const makeContext = (secretArn: string = SECRET_ARN): ShibSpContext => ({
  REGION: 'us-east-1',
  TAGS,
  SHIBBOLETH: {
    entityId: 'https://sp.example.edu/shibboleth',
    secret: {
      secretArn, refreshInterval: '3600000',
      samlPrivateKeySecretFld: 'sp-key', samlCertSecretFld: 'sp-cert', jwtPrivateKeySecretFld: 'jwt-prikey', jwtPublicKeySecretFld: 'jwt-pubkey'
    }
  }
} as ShibSpContext);

const pemPair = () => generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding: { type: 'spki', format: 'pem' },
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' }
});

/** Secrets Manager in memory: GetSecretValue and CreateSecret by name or ARN */
const fakeSecretsManager = () => {
  const secrets = new Map<string, string>();
  const arnOf = (name: string) => `arn:aws:secretsmanager:us-east-2:111111111111:secret:${name}-XyZ123`;
  const sent: string[] = [];
  const created: any[] = [];
  const client = {
    send: async (command: any) => {
      // The package may resolve its own copy of the SDK, so match commands by name
      const { constructor: { name: commandName }, input } = command;
      sent.push(commandName);
      switch (commandName) {
        case 'GetSecretValueCommand': {
          const SecretString = secrets.get(secretNameOf(input.SecretId));
          if (SecretString === undefined) {
            throw Object.assign(new Error('not found'), { name: 'ResourceNotFoundException' });
          }
          return { SecretString };
        }
        case 'CreateSecretCommand':
          created.push(input);
          secrets.set(input.Name, input.SecretString);
          return { ARN: arnOf(input.Name), Name: input.Name };
        default:
          throw new Error(`Unexpected command ${commandName}`);
      }
    }
  } as unknown as SecretsManagerClient;
  return { client, secrets, sent, created, arnOf };
}

// The package logs every read and create
beforeAll(() => jest.spyOn(console, 'log').mockImplementation(() => {}));
afterAll(() => jest.restoreAllMocks());

const samlPair = pemPair();
const SAML = { samlPrivateKey: samlPair.privateKey, samlCert: 'MIIBszCCARygAwIBAgIJAK' };

describe('secretNameOf', () => {
  it('takes the name from an ARN, without the random suffix', () => {
    expect(secretNameOf(SECRET_ARN)).toBe(SECRET_NAME);
  });

  it('leaves a name as it is', () => {
    expect(secretNameOf(SECRET_NAME)).toBe(SECRET_NAME);
  });
});

describe('secretLandscapeOf', () => {
  it('takes a name ending with "/<landscape>" as that landscape\'s', () => {
    expect(secretLandscapeOf('shibsp/hp-dashboard/staging', 'staging')).toBe('staging');
  });

  it('takes any other name as shared', () => {
    for (const name of ['shibsp/hp-dashboard', 'shibsp/hp-dashboard/staging-old', 'staging/shibsp', 'shibsp/hp-dashboard/mock']) {
      expect(secretLandscapeOf(name, 'staging')).toBeUndefined();
    }
    expect(secretLandscapeOf('shibsp/hp-dashboard/staging', undefined)).toBeUndefined();
  });
});

describe('ShibbolethSecret', () => {
  it('finds nothing when the secret does not exist', async () => {
    const { client } = fakeSecretsManager();
    const shibbolethSecret = new ShibbolethSecret(makeContext(), client);
    expect(await shibbolethSecret.getSecret()).toBeUndefined();
    expect(await shibbolethSecret.getValues()).toBeUndefined();
  });

  it('reads the values by role under the context field names', async () => {
    const { client, secrets } = fakeSecretsManager();
    secrets.set(SECRET_NAME, JSON.stringify({
      'sp-key': 'saml key', 'sp-cert': 'saml cert', 'jwt-prikey': 'jwt private', 'jwt-pubkey': 'jwt public', 'cloudfront-challenge': 'challenge'
    }));
    const shibbolethSecret = new ShibbolethSecret(makeContext(), client);
    expect(await shibbolethSecret.getSecret()).toBeDefined();
    expect(await shibbolethSecret.getValues()).toEqual({
      samlPrivateKey: 'saml key', samlCert: 'saml cert', jwtPrivateKey: 'jwt private', jwtPublicKey: 'jwt public', cloudFrontChallenge: 'challenge'
    });
  });

  it('creates the secret under the ARN\'s name with a new, matching JWT key pair', async () => {
    const { client, secrets, arnOf } = fakeSecretsManager();
    const { arn, generatedSamlCert } = await new ShibbolethSecret(makeContext(), client).createSecret(SAML);
    expect(arn).toBe(arnOf(SECRET_NAME));
    expect(generatedSamlCert).toBeUndefined();

    const fields = JSON.parse(secrets.get(SECRET_NAME)!);
    expect(Object.keys(fields).sort()).toEqual(['cloudfront-challenge', 'jwt-prikey', 'jwt-pubkey', 'sp-cert', 'sp-key']);
    // An already formatted PEM is copied as it is
    expect(fields['sp-key']).toBe(SAML.samlPrivateKey.trim());
    expect(fields['sp-cert']).toBe(`-----BEGIN CERTIFICATE-----\n${SAML.samlCert}\n-----END CERTIFICATE-----`);
    expect(fields['cloudfront-challenge']).toMatch(/^[0-9a-f]{64}$/);
    // The public key is the private key's
    const derived = createPublicKey(fields['jwt-prikey']).export({ type: 'spki', format: 'pem' }).toString().trim();
    expect(derived).toBe(createPublicKey(fields['jwt-pubkey']).export({ type: 'spki', format: 'pem' }).toString().trim());

    // A new instance reads it back
    const values = await new ShibbolethSecret(makeContext(arn), client).getValues();
    expect(values?.jwtPublicKey).toBe(fields['jwt-pubkey']);
  });

  it('creates the secret with given JWT keys, challenge and name', async () => {
    const { client, secrets } = fakeSecretsManager();
    const jwt = pemPair();
    await new ShibbolethSecret(makeContext(), client).createSecret({
      ...SAML, jwtPrivateKey: jwt.privateKey, jwtPublicKey: jwt.publicKey, cloudFrontChallenge: 'challenge', secretName: 'other/name'
    });
    const fields = JSON.parse(secrets.get('other/name')!);
    expect(fields['jwt-prikey']).toBe(jwt.privateKey.trim());
    expect(fields['jwt-pubkey']).toBe(jwt.publicKey.trim());
    expect(fields['cloudfront-challenge']).toBe('challenge');
    expect(secrets.has(SECRET_NAME)).toBe(false);
  });

  it('generates the SAML key and certificate when neither is given', async () => {
    const { client, secrets } = fakeSecretsManager();
    const { generatedSamlCert } = await new ShibbolethSecret(makeContext(), client).createSecret();

    const fields = JSON.parse(secrets.get(SECRET_NAME)!);
    expect(Object.keys(fields).sort()).toEqual(['cloudfront-challenge', 'jwt-prikey', 'jwt-pubkey', 'sp-cert', 'sp-key']);
    expect(generatedSamlCert).toBe(fields['sp-cert']);
    expect(fields['sp-key']).not.toMatch(/\r/);
    // The certificate is the key's, and not the JWT key pair's
    const certKey = new X509Certificate(fields['sp-cert']).publicKey.export({ type: 'spki', format: 'pem' }).toString();
    expect(createPublicKey(fields['sp-key']).export({ type: 'spki', format: 'pem' }).toString()).toBe(certKey);
    expect(createPublicKey(fields['jwt-pubkey']).export({ type: 'spki', format: 'pem' }).toString()).not.toBe(certKey);
  });

  it('tags and describes a shared secret without a landscape', async () => {
    const { client, created } = fakeSecretsManager();
    const { landscape } = await new ShibbolethSecret(makeContext(), client).createSecret(SAML);
    expect(landscape).toBeUndefined();
    const [{ Tags, Description }] = created;
    expect(Tags).toEqual([
      { Key: 'Service', Value: 'svc' }, { Key: 'Function', Value: 'fn' }, { Key: 'CostCenter', Value: 'cc' }, { Key: 'Ticket', Value: 'tkt' }
    ]);
    expect(Description).toMatch(/landscapes that share a sign-in/);
    expect(Description).toContain('https://sp.example.edu/shibboleth');
  });

  it('tags and describes a landscape\'s own secret with its landscape', async () => {
    const { client, created } = fakeSecretsManager();
    const { landscape } = await new ShibbolethSecret(makeContext(), client).createSecret({ ...SAML, secretName: `${SECRET_NAME}/staging` });
    expect(landscape).toBe('staging');
    const [{ Name, Tags, Description }] = created;
    expect(Name).toBe(`${SECRET_NAME}/staging`);
    expect(Tags).toContainEqual({ Key: 'Landscape', Value: 'staging' });
    expect(Description).toMatch(/the staging landscape/);
  });

  it('leaves out tags the context does not set', async () => {
    const { client, created } = fakeSecretsManager();
    const context = { ...makeContext(), TAGS: { Service: 'svc', Function: 'fn', Landscape: 'staging' } } as ShibSpContext;
    await new ShibbolethSecret(context, client).createSecret(SAML);
    expect(created[0].Tags).toEqual([{ Key: 'Service', Value: 'svc' }, { Key: 'Function', Value: 'fn' }]);
  });

  describe('region', () => {
    // No client given: record the region of the client the package builds for each command
    let regions: string[];
    beforeEach(() => {
      regions = [];
      jest.spyOn(SecretsManagerClient.prototype, 'send').mockImplementation(async function (this: SecretsManagerClient, command: any) {
        regions.push(`${command.constructor.name}:${await this.config.region()}`);
        if (command.constructor.name === 'GetSecretValueCommand') {
          throw Object.assign(new Error('not found'), { name: 'ResourceNotFoundException' });
        }
        return { ARN: 'arn:new' };
      } as any);
    });
    afterEach(() => (SecretsManagerClient.prototype.send as jest.Mock).mockRestore());

    it('creates the secret in the context\'s region', async () => {
      const context = { ...makeContext(), REGION: 'us-east-2' } as ShibSpContext;
      await new ShibbolethSecret(context).createSecret(SAML);
      expect(regions).toContain('CreateSecretCommand:us-east-2');
    });

    it('creates the secret in us-east-1 when the context has no region', async () => {
      const context = { ...makeContext(), REGION: undefined } as unknown as ShibSpContext;
      await new ShibbolethSecret(context).createSecret(SAML);
      expect(regions).toContain('CreateSecretCommand:us-east-1');
    });

    it('reads an existing secret in its ARN\'s region', async () => {
      await new ShibbolethSecret(makeContext('arn:aws:secretsmanager:us-west-2:111111111111:secret:x-AbCdEf')).getSecret();
      expect(regions).toEqual(['GetSecretValueCommand:us-west-2']);
    });
  });

  it('refuses to replace an existing secret', async () => {
    const { client, secrets, sent } = fakeSecretsManager();
    secrets.set(SECRET_NAME, JSON.stringify({ 'jwt-prikey': 'kept' }));
    await expect(new ShibbolethSecret(makeContext(), client).createSecret(SAML)).rejects.toThrow(/already exists/);
    expect(sent).not.toContain('CreateSecretCommand');
    expect(JSON.parse(secrets.get(SECRET_NAME)!)).toEqual({ 'jwt-prikey': 'kept' });
  });

  it('refuses incomplete values', async () => {
    const { client, sent } = fakeSecretsManager();
    const shibbolethSecret = new ShibbolethSecret(makeContext(), client);
    await expect(shibbolethSecret.createSecret({ ...SAML, samlCert: '' })).rejects.toThrow(/both the SAML private key and certificate/);
    await expect(shibbolethSecret.createSecret({ samlCert: SAML.samlCert })).rejects.toThrow(/both the SAML private key and certificate/);
    await expect(shibbolethSecret.createSecret({ ...SAML, jwtPrivateKey: 'only one' })).rejects.toThrow(/both JWT keys/);
    expect(sent).toEqual([]);
  });
});
