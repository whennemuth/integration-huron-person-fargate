import { CfnOutput, Duration, RemovalPolicy } from 'aws-cdk-lib';
import { ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Architecture, FunctionUrlAuthType, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { ENVIRONMENT_VARIABLES_NAMES, FUNCTION_BASE_NAME } from "../../../src/chunking/fetch/SourceSimulator";
import { HuronPersonSecrets } from '../../Secrets';
import { DYNAMODB_TABLE_NAME } from '../../../src/AtomicCounter';

export interface SourceSimulatorProps {
  huronPersonSecrets: HuronPersonSecrets;  
  landscape: string;
  stackId: string;
  region: string;
  account: string;
  timeoutSeconds: number;
  memorySizeMb: number;
  mockTotalPopulation: number;
  mockErrorRate?: number;
  simulatedDelaySeconds?: number;
  secretArn: string;
  tags?: { [key: string]: string };
}

/**
 * Creates a Lambda Function URL that simulates the source API for testing purposes.
 * 
 * This mock API eliminates the 30-minute cooldown constraint of the real source API,
 * enabling rapid testing and development of the parallel chunking flow.
 * 
 * **Features:**
 * - Stateless design: Uses `offset` and `recordCount` query params to calculate responses
 * - API key validation: Validates incoming requests match configured API key
 * - Deterministic data: Generates consistent person IDs using formula-based approach
 * - Parallel execution: Supports multiple concurrent requests
 * - Minimal field set: Returns only fields DataMapper uses
 * - Response structure matches real API: `[{ response_code: 200, response: [persons] }]`
 * 
 * **Usage in Runner.ts:**
 * ```typescript
 * const mockApiBaseUrl = testEnvironment.getVar('MOCK_API_BASE_URL');
 * const mockApiFetchPath = testEnvironment.getVar('MOCK_API_FETCH_PATH');
 * 
 * if (mockApiBaseUrl) {
 *   messageBody.baseUrl = mockApiBaseUrl;
 *   messageBody.fetchPath = mockApiFetchPath || '/';
 * }
 * ```
 * 
 * **Environment Variables:**
 * - MOCK_TOTAL_POPULATION: Total persons available (e.g., 10000)
 * - MOCK_ERROR_RATE: Probability of simulated errors (0.0-1.0, default: 0.0)
 * - MOCK_API_KEY: Expected API key for validation
 */
export class SourceSimulator extends Construct {
  public readonly function: NodejsFunction;
  public readonly functionUrl: string;

  constructor(scope: Construct, id: string, props: SourceSimulatorProps) {
    super(scope, id);

    // Create Lambda log group
    const logGroup = new LogGroup(this, 'LogGroup', {
      logGroupName: `/aws/lambda/source-simulator-${props.landscape}`,
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY
    });

    // Create IAM role for Lambda function
    const lambdaRole = new Role(this, 'FunctionRole', {
      roleName: `${FUNCTION_BASE_NAME}-lambda-role-${props.landscape}`,
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      description: 'Role for source simulator Lambda function',
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    const { 
      MOCK_TOTAL_POPULATION, MOCK_ERROR_RATE, MOCK_SIMULATED_DELAY_SECONDS, SECRET_ARN,
      STACK_ID, REGION, LANDSCAPE
    } = ENVIRONMENT_VARIABLES_NAMES;

    // Create Lambda function with arm64 architecture (Graviton2) for cost savings
    this.function = new NodejsFunction(this, 'Function', {
      functionName: `${FUNCTION_BASE_NAME}-${props.landscape}`,
      description: 'Mock API that simulates the source person API for testing parallel chunking flow',
      runtime: Runtime.NODEJS_20_X,
      architecture: Architecture.ARM_64, // Graviton2 for better price/performance
      handler: 'handler',
      entry: 'src/chunking/fetch/SourceSimulator.ts',
      timeout: Duration.seconds(props.timeoutSeconds),
      memorySize: props.memorySizeMb,
      role: lambdaRole,
      logGroup,
      environment: {
        [MOCK_TOTAL_POPULATION]: props.mockTotalPopulation.toString(),
        [MOCK_ERROR_RATE]: (props.mockErrorRate || 0.0).toString(),
        [MOCK_SIMULATED_DELAY_SECONDS]: (props.simulatedDelaySeconds || 0).toString(),
        [SECRET_ARN]: props.secretArn, // ARN of Secrets Manager secret containing API key
        [STACK_ID]: props.stackId,
        [REGION]: props.region,
        [LANDSCAPE]: props.landscape,
      },
      bundling: {
        externalModules: [
          '@aws-sdk/*',
        ]
      },
      // Set reserved concurrency to control parallel execution
      reservedConcurrentExecutions: 20,
    });

    // Grant Lambda permission to read from Secrets Manager
    props.huronPersonSecrets.secret.grantRead(lambdaRole);

    // Grant simulator permission to use the atomic counter table for stateful depletion allocation.
    const tableName = DYNAMODB_TABLE_NAME({
      STACK_ID: props.stackId,
      TAGS: { Landscape: props.landscape }
    } as any);
    lambdaRole.addToPolicy(new PolicyStatement({
      actions: ['dynamodb:GetItem', 'dynamodb:UpdateItem', 'dynamodb:DescribeTable'],
      resources: [
        `arn:aws:dynamodb:${props.region}:${props.account}:table/${tableName}`
      ]
    }));

    // Create Function URL (public access with API key validation)
    // Note: As of Oct 2025, addFunctionUrl() automatically adds both required permissions:
    // - lambda:InvokeFunctionUrl
    // - lambda:InvokeFunction (with InvokedViaFunctionUrl: true)
    // See: https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_lambda-readme.html#important-function-url-permission-update---oct-2025
    const functionUrl = this.function.addFunctionUrl({
      authType: FunctionUrlAuthType.NONE, // Public, but protected by API key validation in code
    });

    this.functionUrl = functionUrl.url;

    // Output the Function URL for easy access
    new CfnOutput(this, 'SourceSimulatorUrl', {
      value: this.functionUrl,
      description: 'URL for the Source Simulator mock API',
      exportName: `SourceSimulatorUrl-${props.landscape}`,
    });

    // Output usage instructions
    new CfnOutput(this, 'SourceSimulatorUsage', {
      value: `Set RUNNER_MOCK_API_BASE_URL="${this.functionUrl.replace(/\/$/, '')}" in Runner.ts environment`,
      description: 'How to use the Source Simulator in Runner.ts',
    });
  }
}
