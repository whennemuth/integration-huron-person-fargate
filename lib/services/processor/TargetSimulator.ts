import { CfnOutput, Duration, RemovalPolicy } from 'aws-cdk-lib';
import { HttpApi } from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { ITable } from 'aws-cdk-lib/aws-dynamodb';
import { ManagedPolicy, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import { ENVIRONMENT_VARIABLES_NAMES, FUNCTION_BASE_NAME } from '../../../src/target-simulator/TargetSimulator';

export interface TargetSimulatorProps {
  landscape: string;
  stackId: string;
  region: string;
  /** The mock target table the simulator stores persons in */
  table: ITable;
  /** Lambda timeout (default: 29 - an HTTP API integration times out at 30s regardless) */
  timeoutSeconds?: number;
  memorySizeMb?: number;
  /** How long a full person listing may be served from Lambda memory (default: 30) */
  listCacheTtlSeconds?: number;
  tags?: { [key: string]: string };
}

/**
 * Creates the target simulator (mock landscapes only): a Lambda behind an API Gateway HTTP API that
 * impersonates the Huron person API, backed by the mock target table. See
 * src/target-simulator/TargetSimulator.ts.
 *
 * Why an HTTP API rather than a (simpler) Lambda Function URL: the Huron client deliberately sends
 * UNENCODED square brackets in query strings (e.g. pagination[offset]=0 - see integration-huron-person
 * UrlSerializer.ts; the real Huron API rejects the encoded form). A Function URL rejects those
 * requests with 400 {"message":null} before ever invoking the function. An HTTP API delivers the
 * same payload v2.0 event, so the handler is identical either way.
 *
 * Also creates the generated "external token" clients authenticate with. Exposes:
 * - baseUrl: the HTTP API endpoint (no trailing slash), for dataTarget.endpointConfig.baseUrl
 * - externalToken: a dynamic reference to the generated token, for dataTarget.endpointConfig.externalToken
 *
 * Both are injected into the landscape's integration config secret (see HuronPersonSecrets), so the
 * ECS tasks talk to the simulator through their ordinary Huron client code. The simulator reads the
 * token from its own secret (never from the config secret), which keeps the dependency one-way:
 * config secret -> simulator.
 */
export class TargetSimulator extends Construct {
  public readonly function: NodejsFunction;
  public readonly endpoint: string;
  public readonly baseUrl: string;
  public readonly tokenSecret: Secret;

  constructor(scope: Construct, id: string, props: TargetSimulatorProps) {
    super(scope, id);

    const { landscape, stackId, region, table, timeoutSeconds = 29, memorySizeMb = 512, listCacheTtlSeconds = 30 } = props;

    this.tokenSecret = new Secret(this, 'TokenSecret', {
      secretName: `${stackId}/target-simulator/token/${landscape}`,
      description: 'External token for the target simulator (stands in for the Huron external token)',
      generateSecretString: { excludePunctuation: true, passwordLength: 48 },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/aws/lambda/${FUNCTION_BASE_NAME}-${landscape}`,
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY
    });

    const lambdaRole = new Role(this, 'FunctionRole', {
      roleName: `${FUNCTION_BASE_NAME}-lambda-role-${landscape}`,
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      description: 'Role for target simulator Lambda function',
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    const { TABLE_NAME, TOKEN_SECRET_ARN, REGION, LIST_CACHE_TTL_SECONDS } = ENVIRONMENT_VARIABLES_NAMES;

    this.function = new NodejsFunction(this, 'Function', {
      functionName: `${FUNCTION_BASE_NAME}-${landscape}`,
      description: 'Mock API that simulates the Huron person API (target system) for mock landscapes',
      runtime: Runtime.NODEJS_20_X,
      architecture: Architecture.ARM_64,
      handler: 'handler',
      entry: 'src/target-simulator/TargetSimulator.ts',
      timeout: Duration.seconds(timeoutSeconds),
      memorySize: memorySizeMb,
      role: lambdaRole,
      logGroup,
      environment: {
        [TABLE_NAME]: table.tableName,
        [TOKEN_SECRET_ARN]: this.tokenSecret.secretArn,
        [REGION]: region,
        [LIST_CACHE_TTL_SECONDS]: `${listCacheTtlSeconds}`,
      },
      bundling: {
        externalModules: [
          '@aws-sdk/*',
        ]
      },
    });

    table.grantReadWriteData(lambdaRole);
    this.tokenSecret.grantRead(lambdaRole);

    // Public endpoint; every call but the token request requires a JWT issued by the simulator, and
    // the token request requires the generated external token. The catch-all $default route sends
    // every method/path to the Lambda, and the auto-deployed $default stage keeps rawPath free of a
    // stage prefix. The integration grants API Gateway permission to invoke the function.
    const httpApi = new HttpApi(this, 'HttpApi', {
      apiName: `${FUNCTION_BASE_NAME}-${landscape}`,
      description: 'Mock Huron person API (target simulator) for mock landscapes',
      defaultIntegration: new HttpLambdaIntegration('Integration', this.function),
    });

    // "https://{id}.execute-api.{region}.amazonaws.com" - no trailing slash (the client appends "/api/v2/persons" etc.)
    this.endpoint = httpApi.apiEndpoint;
    this.baseUrl = httpApi.apiEndpoint;

    new CfnOutput(this, 'TargetSimulatorUrl', {
      value: this.endpoint,
      description: 'URL for the Target Simulator mock Huron API',
      exportName: `TargetSimulatorUrl-${landscape}`,
    });
  }

  /** Dynamic reference to the generated external token (resolved by CloudFormation at deploy time) */
  public get externalToken(): string {
    return this.tokenSecret.secretValue.unsafeUnwrap();
  }
}
