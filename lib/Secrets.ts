import { Construct } from "constructs";
import { ConfigManager } from "integration-huron-person";
import { IContext } from "../context/IContext";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import { RemovalPolicy, SecretValue } from "aws-cdk-lib";

/**
 * A Secrets Manager secret that holds the Huron Person integration configuration and secrets.
 * This secret is injected into ECS Fargate tasks as an environment variable at runtime,
 * ensuring sensitive credentials never appear in CloudFormation templates or logs.
 */
export class HuronPersonSecrets {
  private _secret: Secret;

  constructor(scope: Construct, context: IContext) {
    const { STACK_ID, S3: { chunksBucket } = {}, TAGS: { Landscape = 'dev' } = {} } = context;

    const integrationConfig = buildSecretValue(context);

    const secretName = `${STACK_ID}/integration/_config/${Landscape}`;

    const secret = new Secret(scope, 'huron-person-secret', {
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

/**
 * Builds the complete configuration JSON string for the secret.
 * 
 * Configuration precedence (earlier sources override later ones):
 * 1. Environment variables (highest priority)
 * 2. HURON_PERSON_CONFIG from context (if provided as config object)
 * 3. HURON_PERSON_CONFIG.configPath from context (if config is a path reference)
 * 
 * @param context - CDK context containing HURON_PERSON_CONFIG
 * @returns JSON string of the complete configuration
 */
export const buildSecretValue = (context: IContext): string => {
  const { HURON_PERSON_CONFIG, S3: { chunksBucket } = {}, TAGS: { Landscape = 'dev' } = {} } = context;

  const bucketName = `${chunksBucket}-${Landscape.toLowerCase()}`;

  let cfgMgr = ConfigManager.getInstance().reset();

  /**
   * Load configuration from environment variables first.
   * These will take precedence over any config loaded later because earlier sources
   * take precedence in ConfigManager's merge logic.
   */
  cfgMgr = cfgMgr.fromEnvironment();

  /**
   * Assume the storage type is S3 and override the bucket name from context.
   * This ensures that the S3 bucket used for chunk storage is always taken from the context, regardless of what may be specified in the config.
   * This is important for ensuring that the ECS tasks use the correct S3 bucket for chunk storage.
   * The storage type is assumed to be 's3' here, but if the config specifies a different type, it will be overridden by this setting.
   */
  cfgMgr = cfgMgr.fromPartial({ storage: { type: 's3', config: { bucketName } } });

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

  return JSON.stringify(integrationConfig);
};
