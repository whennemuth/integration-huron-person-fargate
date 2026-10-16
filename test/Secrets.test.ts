/**
 * Tests the storage block of the integration config secret (lib/Secrets.ts): its storage type comes
 * from context.PREVIOUS_STORAGE_TYPE first, then the integration config, then DEFAULT_STORAGE_TYPE.
 */
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { IContext } from '../context/IContext';
import {
  buildSecretValue,
  DEFAULT_STORAGE_TYPE,
  readConfiguredStorage,
  resolveSecretStorage
} from '../lib/Secrets';

const CHUNKS_BUCKET = 'huron-person-chunks-mock';
const CURRENT_STATE_TABLE = 'huron-person-fargate-processor-person-current-state-mock';
const HISTORY_TABLE = 'huron-person-fargate-processor-person-history-mock';

const s3Storage = { type: 's3', config: { bucketName: 'some-other-bucket', keyPrefix: 'baseline/', region: 'us-east-1' } } as any;
const dynamoDbStorage = {
  type: 'dynamodb',
  config: { personCurrentStateTableName: 'configured-current-state', personHistoryTableName: 'configured-history', region: 'us-east-1' }
} as any;

const makeContext = (overrides: Partial<IContext> = {}): IContext => ({
  STACK_ID: 'huron-person-fargate-processor',
  REGION: 'us-east-2',
  S3: { chunksBucket: 'huron-person-chunks' },
  TAGS: { Landscape: 'Mock' },
  ...overrides
} as any);

describe('resolveSecretStorage', () => {
  describe('storage type precedence', () => {
    it('takes the type from context.PREVIOUS_STORAGE_TYPE over the integration config', () => {
      const context = makeContext({ PREVIOUS_STORAGE_TYPE: 's3' });
      expect(resolveSecretStorage(context, dynamoDbStorage).type).toBe('s3');
    });

    it('takes the type from the integration config when the context names none', () => {
      expect(resolveSecretStorage(makeContext(), dynamoDbStorage).type).toBe('dynamodb');
    });

    it('falls back to the default type when neither names one', () => {
      expect(DEFAULT_STORAGE_TYPE).toBe('s3');
      expect(resolveSecretStorage(makeContext(), undefined).type).toBe(DEFAULT_STORAGE_TYPE);
      expect(resolveSecretStorage(makeContext(), { config: {} } as any).type).toBe(DEFAULT_STORAGE_TYPE);
    });
  });

  describe('s3 storage', () => {
    it('keeps the integration config settings of the same type, with the chunks bucket of the landscape', () => {
      const context = makeContext({ PREVIOUS_STORAGE_TYPE: 's3' });
      expect(resolveSecretStorage(context, s3Storage)).toEqual({
        type: 's3',
        config: { bucketName: CHUNKS_BUCKET, keyPrefix: 'baseline/', region: 'us-east-1' }
      });
    });

    it('drops integration config settings that belong to another storage type', () => {
      const context = makeContext({ PREVIOUS_STORAGE_TYPE: 's3' });
      expect(resolveSecretStorage(context, dynamoDbStorage)).toEqual({
        type: 's3',
        config: { bucketName: CHUNKS_BUCKET }
      });
    });
  });

  describe('dynamodb storage', () => {
    it('uses the table names the stack creates, keeping the configured region', () => {
      const context = makeContext({ PREVIOUS_STORAGE_TYPE: 'dynamodb' });
      expect(resolveSecretStorage(context, dynamoDbStorage)).toEqual({
        type: 'dynamodb',
        config: { personCurrentStateTableName: CURRENT_STATE_TABLE, personHistoryTableName: HISTORY_TABLE, region: 'us-east-1' }
      });
    });

    it('takes the region from the context when the integration config has no dynamodb settings', () => {
      const context = makeContext({ PREVIOUS_STORAGE_TYPE: 'dynamodb' });
      expect(resolveSecretStorage(context, s3Storage)).toEqual({
        type: 'dynamodb',
        config: { personCurrentStateTableName: CURRENT_STATE_TABLE, personHistoryTableName: HISTORY_TABLE, region: 'us-east-2' }
      });
    });
  });
});

describe('readConfiguredStorage and buildSecretValue', () => {
  let tempDir: string;
  let savedEnv: NodeJS.ProcessEnv;

  // A minimal integration config that passes validation in 'none' execution mode
  const integrationConfig = (storage: any) => ({
    landscape: 'mock',
    dataSource: {},
    dataTarget: {
      endpointConfig: {
        baseUrl: 'https://target.example.com',
        authMethod: 'basic',
        loginSvcPath: '/login',
        username: 'user',
        password: 'pass'
      },
      personsPath: '/persons',
      organizationsPath: '/organizations'
    },
    integration: { clientId: 'huron-person-integration' },
    storage
  });

  const writeConfigFile = (storage: any): string => {
    const configPath = join(tempDir, 'config.json');
    writeFileSync(configPath, JSON.stringify(integrationConfig(storage)));
    return configPath;
  };

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'secrets-test-'));
    // buildSecretValue() loads environment variables first - keep any in the test process out of it
    savedEnv = process.env;
    process.env = {};
  });

  afterEach(() => {
    process.env = savedEnv;
    rmSync(tempDir, { recursive: true, force: true });
  });

  it('reads the storage block from a config file referenced by configPath', () => {
    const context = makeContext({ HURON_PERSON_CONFIG: { configPath: writeConfigFile(dynamoDbStorage) } } as any);
    expect(readConfiguredStorage(context)).toEqual(dynamoDbStorage);
  });

  it('reads the storage block from an inline partial config', () => {
    const context = makeContext({ HURON_PERSON_CONFIG: { storage: s3Storage } } as any);
    expect(readConfiguredStorage(context)).toEqual(s3Storage);
  });

  it('returns undefined when there is no integration config or it cannot be read', () => {
    expect(readConfiguredStorage(makeContext())).toBeUndefined();
    const context = makeContext({ HURON_PERSON_CONFIG: { configPath: join(tempDir, 'missing.json') } } as any);
    expect(readConfiguredStorage(context)).toBeUndefined();
  });

  it('writes exactly the resolved storage into the secret when the context type differs from the config file', () => {
    const context = makeContext({
      PREVIOUS_STORAGE_TYPE: 's3',
      HURON_PERSON_CONFIG: { configPath: writeConfigFile(dynamoDbStorage) }
    } as any);

    const secret = JSON.parse(buildSecretValue(context));

    expect(secret.storage).toEqual({ type: 's3', config: { bucketName: CHUNKS_BUCKET } });
    // The rest of the config still comes from the file
    expect(secret.dataTarget.personsPath).toBe('/persons');
  });

  it('keeps the config file storage when the context names no storage type', () => {
    const context = makeContext({ HURON_PERSON_CONFIG: { configPath: writeConfigFile(s3Storage) } } as any);

    const secret = JSON.parse(buildSecretValue(context));

    expect(secret.storage).toEqual({
      type: 's3',
      config: { bucketName: CHUNKS_BUCKET, keyPrefix: 'baseline/', region: 'us-east-1' }
    });
  });

  it('still applies the data target override', () => {
    const context = makeContext({
      PREVIOUS_STORAGE_TYPE: 'dynamodb',
      HURON_PERSON_CONFIG: { configPath: writeConfigFile(s3Storage) }
    } as any);

    const secret = JSON.parse(buildSecretValue(context, { baseUrl: 'https://simulator.example.com', externalToken: 'token' }));

    expect(secret.storage.type).toBe('dynamodb');
    expect(secret.dataTarget.endpointConfig).toMatchObject({
      authMethod: 'externalToken',
      baseUrl: 'https://simulator.example.com',
      externalToken: 'token'
    });
  });
});
