import { Construct } from "constructs";
import { Config, ConfigManager } from "integration-huron-person";
import { IContext } from "../context/IContext";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import { RemovalPolicy, SecretValue } from "aws-cdk-lib";
import { readFileSync } from "fs";
import { resolve } from "path";
import { DYNAMODB_TABLE_NAME as personCurrentStateTableName } from "../src/dynamodb/PersonCurrentStateTable";
import { DYNAMODB_TABLE_NAME as personHistoryTableName } from "../src/dynamodb/PersonHistoryTable";

type StorageConfig = Config['storage'];
type StorageType = StorageConfig['type'];

/**
 * Replaces the data target endpoint in the integration config - used by mock landscapes to point
 * the ECS tasks at the target simulator. Values may be CDK tokens (resolved at deploy time).
 */
export type DataTargetOverride = {
  baseUrl: string;
  externalToken: string;
};

/** Name of a landscape's integration config secret (also read by the landscape's dashboard) */
export const integrationSecretName = (context: IContext): string => {
  const { STACK_ID, TAGS: { Landscape = 'dev' } = {} } = context;
  return `${STACK_ID}/integration/_config/${Landscape}`;
}

/**
 * A Secrets Manager secret that holds the Huron Person integration configuration and secrets.
 * This secret is injected into ECS Fargate tasks as an environment variable at runtime,
 * ensuring sensitive credentials never appear in CloudFormation templates or logs.
 */
export class HuronPersonSecrets {
  private _secret: Secret;

  constructor(scope: Construct, context: IContext, dataTargetOverride?: DataTargetOverride) {
    const integrationConfig = buildSecretValue(context, dataTargetOverride);

    const secretName = integrationSecretName(context);

    const secret = new Secret(scope, 'Secret', {
      secretName,
      description: 'Huron Person integration configuration and secrets for Fargate tasks',
      secretStringValue: SecretValue.unsafePlainText(integrationConfig),
      removalPolicy: RemovalPolicy.DESTROY,
    });
    this._secret = secret;
  }

  public get secret(): Secret {
    return this._secret;
  }

  public get secretName(): string {
    return this._secret.secretName;
  }

  public get secretArn(): string {
    return this._secret.secretArn;
  }
}

/** Storage type used when neither the context nor the integration config names one */
export const DEFAULT_STORAGE_TYPE: StorageType = 's3';

/**
 * Reads the storage block of the integration config that HURON_PERSON_CONFIG refers to (a file
 * path or an inline partial config), without merging it with anything else.
 *
 * @returns The configured storage, or undefined if there is none (or the file can't be read - the
 * ConfigManager load that follows reports that case)
 */
export const readConfiguredStorage = (context: IContext): Partial<StorageConfig> | undefined => {
  const { HURON_PERSON_CONFIG } = context;
  if (!HURON_PERSON_CONFIG) {
    return undefined;
  }
  if ('configPath' in HURON_PERSON_CONFIG && HURON_PERSON_CONFIG.configPath) {
    try {
      // Resolved against the working directory, as ConfigManager.fromFileSystem() does
      const fileContent = readFileSync(resolve(HURON_PERSON_CONFIG.configPath), 'utf-8');
      return (JSON.parse(fileContent) as Partial<Config>).storage;
    }
    catch {
      return undefined;
    }
  }
  return (HURON_PERSON_CONFIG as Partial<Config>).storage;
};

/**
 * Resolves the storage block of the secret. The storage type is taken from, in order:
 * 1. context.PREVIOUS_STORAGE_TYPE - the deploy-time switch that also decides which tables CDK
 *    creates and which mode the ECS tasks run in, so the secret always agrees with them
 * 2. The integration config's storage.type (HURON_PERSON_CONFIG), if the context names none
 * 3. DEFAULT_STORAGE_TYPE, as a last resort
 *
 * The integration config's storage.config is kept only if it belongs to the resolved type (a
 * config for another storage type is dropped rather than mixed in). Settings that the stack
 * itself determines are then filled in from the context:
 * - s3: bucketName is this landscape's chunks bucket, where the delta baseline lives
 * - dynamodb: the person current-state and history table names this stack creates, and its region
 *
 * @param configuredStorage - The integration config's storage block (see readConfiguredStorage())
 */
export const resolveSecretStorage = (context: IContext, configuredStorage?: Partial<StorageConfig>): StorageConfig => {
  const { PREVIOUS_STORAGE_TYPE, REGION, S3: { chunksBucket } = {}, TAGS: { Landscape = 'dev' } = {} } = context;

  const type = PREVIOUS_STORAGE_TYPE || configuredStorage?.type || DEFAULT_STORAGE_TYPE;
  const config: Record<string, any> = configuredStorage?.type === type ? { ...(configuredStorage.config ?? {}) } : {};

  switch (type) {
    case 's3':
      config.bucketName = `${chunksBucket}-${Landscape.toLowerCase()}`;
      break;
    case 'dynamodb':
      config.region ??= REGION;
      config.personCurrentStateTableName = personCurrentStateTableName(context);
      config.personHistoryTableName = personHistoryTableName(context);
      break;
  }

  return { type, config } as StorageConfig;
};

/**
 * Builds the complete configuration JSON string for the secret.
 * 
 * Configuration precedence (earlier sources override later ones):
 * 1. Environment variables (highest priority) - except storage, which they don't set here
 * 2. HURON_PERSON_CONFIG from context (if provided as config object)
 * 3. HURON_PERSON_CONFIG.configPath from context (if config is a path reference)
 * 
 * The storage block is resolved separately (see resolveSecretStorage()): its type comes from
 * context.PREVIOUS_STORAGE_TYPE first, then the integration config, then a default.
 * 
 * @param context - CDK context containing HURON_PERSON_CONFIG
 * @param dataTargetOverride - Optional data target endpoint to substitute (mock landscapes)
 * @returns JSON string of the complete configuration
 */
export const buildSecretValue = (context: IContext, dataTargetOverride?: DataTargetOverride): string => {
  const { HURON_PERSON_CONFIG } = context;

  const storage = resolveSecretStorage(context, readConfiguredStorage(context));

  let cfgMgr = ConfigManager.getInstance().reset();

  /**
   * Load configuration from environment variables first.
   * These will take precedence over any config loaded later because earlier sources
   * take precedence in ConfigManager's merge logic.
   */
  cfgMgr = cfgMgr.fromEnvironment();

  /**
   * Load the resolved storage next, so it takes precedence over the storage of the integration
   * config loaded below. It is also set again, exactly, after the config is resolved: ConfigManager
   * fills in settings missing from it with those of the integration config, which belong to another
   * storage type if the two types differ.
   */
  cfgMgr = cfgMgr.fromPartial({ storage });

  /**
   * Load from HURON_PERSON_CONFIG in context.
   * This can be either:
   * - A Partial<Config> object with actual values
   * - An object with configPath property pointing to a file
   */
  if (HURON_PERSON_CONFIG) {
    if ('configPath' in HURON_PERSON_CONFIG && HURON_PERSON_CONFIG.configPath) {
      // Load from file path
      cfgMgr = cfgMgr.fromFileSystem(HURON_PERSON_CONFIG.configPath);
    } else {
      // Load as partial config object
      cfgMgr = cfgMgr.fromPartial(HURON_PERSON_CONFIG as any);
    }
  }

  const integrationConfig = cfgMgr.getConfig('none');

  // Mutate a copy: ConfigManager hands out a shared instance that is validated again elsewhere
  const secretConfig = JSON.parse(JSON.stringify(integrationConfig));
  secretConfig.storage = storage;

  /**
   * Applied after the config is resolved (rather than as a ConfigManager source) because the values
   * are CDK tokens: they survive JSON.stringify as placeholders and are resolved by CloudFormation.
   * A token placeholder is not a valid URL until CloudFormation resolves it.
   */
  if (dataTargetOverride) {
    const { baseUrl, externalToken } = dataTargetOverride;
    const endpointConfig = secretConfig.dataTarget.endpointConfig;
    endpointConfig.authMethod = 'externalToken';
    endpointConfig.baseUrl = baseUrl;
    endpointConfig.externalToken = externalToken;
  }

  return JSON.stringify(secretConfig);
};
