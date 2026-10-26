import { Construct } from "constructs";
import { ConfigManager } from "integration-huron-person";
import { IContext } from "../context/IContext";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import { RemovalPolicy, SecretValue } from "aws-cdk-lib";

/**
 * A secrets manager secret that holds the integration configuration and secrets.
 */
export class SecretsManagerSecret {
  private _secret: Secret;

  constructor(scope: Construct, context:IContext) {
    const { STACK_ID, TAGS: { Landscape='dev' } = {}} = context;

    const integrationConfig = buildSecretValue(context);

    const secretName = `${STACK_ID}/integration/config/${Landscape}`;

    const secret = new Secret(scope, 'integration-secret', {
      secretName,
      description: 'Huron Person integration configuration and secrets for dashboard app',
      secretStringValue: SecretValue.unsafePlainText(integrationConfig),
      removalPolicy: RemovalPolicy.DESTROY,
    });
    this._secret = secret;
  }

  public get secretName(): string {
    return this._secret.secretName;
  }

  public get secretArn(): string {
    return this._secret.secretArn;
  }
}

export const buildSecretValue = (context: IContext): string => {
    const { INTEGRATION_CONFIG } = context;
    let cfgMgr = ConfigManager.getInstance();

    /**
     * Load configuration from environment variables and merge with existing config.
     * These will take precedence over any secrets loaded later because earlier sources
     * take precedence over later sources.
     */
    cfgMgr = cfgMgr.fromEnvironment();

    /**
     * Load the config manager with context's INTEGRATION_CONFIG as partial config.
     * Secrets from the environment will take precedence over any found in the partial
     * because earlier sources take precedence over later sources.
     */
    cfgMgr = cfgMgr.fromPartial(INTEGRATION_CONFIG);

    const integrationConfig = cfgMgr.getConfig('none');

    return JSON.stringify(integrationConfig);
}