/**
 * A landscape's deployed dashboard Lambda function, found by the name CDK gives it
 * (dashboardFunctionName(), shared with lib/FunctionUrl.ts). Reads AWS only: it never changes the function.
 *
 * The local launcher looks up the function's environment at every launch and runs the local server with it
 * (secrets left out), so the server reads the same tables, config secret and registry as the deployed
 * dashboard, and a redeploy that changes the environment is picked up at the next launch.
 */
import { GetFunctionConfigurationCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { TestEnvironment } from 'integration-core';
import { dashboardFunctionName } from '../lib/DashboardFunctionName';

export type LambdaFunctionParms = {
  landscape: string;
  region: string;
  /** DASHBOARD.STACK_ID. Default: LambdaFunction.DEFAULT_STACK_ID */
  stackId?: string;
  /** Default: a client for the region, with the default credential chain (e.g. AWS_PROFILE) */
  client?: LambdaClient;
}

/**
 * The landscape has no dashboard Lambda function in the region: its dashboard stack isn't deployed there, or the
 * landscape or region is wrong. AWS answered, so the credentials are not the cause.
 */
export class DashboardFunctionNotFoundError extends Error {
  constructor(readonly functionName: string, readonly landscape: string, readonly region: string) {
    super(`No dashboard Lambda function ${functionName} for the ${landscape} landscape in ${region}: is its dashboard stack deployed there?`);
    this.name = 'DashboardFunctionNotFoundError';
  }
}

export class LambdaFunction {
  static readonly DEFAULT_STACK_ID = 'hp-dashboard';

  /**
   * The function's secret variables. A local run never reads them, so the launcher leaves them out, and they
   * are masked wherever an environment is shown. ORIGIN_VERIFY_SECRET: origin verification runs only in
   * Lambda (AWS_LAMBDA_FUNCTION_NAME set).
   */
  static readonly SECRET_VARIABLES: readonly string[] = ['ORIGIN_VERIFY_SECRET'];

  static readonly MASK = '<secret>';

  readonly landscape: string;
  readonly region: string;
  readonly name: string;
  private readonly client: LambdaClient;

  constructor(parms: LambdaFunctionParms) {
    const { landscape, region, stackId = LambdaFunction.DEFAULT_STACK_ID, client } = parms;
    if (!landscape || !region) {
      throw new Error('A dashboard Lambda function needs a landscape and a region');
    }
    this.landscape = landscape;
    this.region = region;
    this.name = dashboardFunctionName(stackId, landscape);
    this.client = client ?? new LambdaClient({ region });
  }

  static isSecret(variable: string): boolean {
    return LambdaFunction.SECRET_VARIABLES.includes(variable);
  }

  /** An environment without its secret variables */
  static withoutSecrets(environment: Record<string, string>): Record<string, string> {
    return Object.fromEntries(Object.entries(environment).filter(([variable]) => !LambdaFunction.isSecret(variable)));
  }

  /** An environment to show: sorted by name, with secret values masked */
  static masked(environment: Record<string, string>): Record<string, string> {
    return Object.fromEntries(Object.keys(environment).sort().map(variable => [
      variable, LambdaFunction.isSecret(variable) ? LambdaFunction.MASK : environment[variable]
    ]));
  }

  /**
   * The function's environment variables, as configured, secrets included (Lambda's own runtime variables,
   * such as AWS_LAMBDA_FUNCTION_NAME, are not part of it).
   * @throws DashboardFunctionNotFoundError if the function doesn't exist
   */
  async getEnvironment(): Promise<Record<string, string>> {
    const { name, landscape, region } = this;
    try {
      const { Environment } = await this.client.send(new GetFunctionConfigurationCommand({ FunctionName: name }));
      if (Environment?.Error) {
        const { ErrorCode, Message } = Environment.Error;
        throw new Error(`Lambda can't give the environment of ${name}: ${ErrorCode} ${Message}`);
      }
      return { ...Environment?.Variables };
    }
    catch (error: unknown) {
      if ((error as { name?: string })?.name === 'ResourceNotFoundException') {
        throw new DashboardFunctionNotFoundError(name, landscape, region);
      }
      throw error;
    }
  }
}



/**
 * RUN MANUALLY: show a landscape's dashboard Lambda environment, with its secrets masked: what the local
 * launcher looks up. LANDSCAPE, REGION and STACK_ID default to the active landscape's (context/context.json).
 */
if (require.main === module) {
  const testEnvironment = TestEnvironment('DASHBOARD_LAMBDA_FUNCTION');
  [
    'TASK',
    'LANDSCAPE',
    'REGION',
    'STACK_ID'
  ].forEach(testEnvironment.getVar);

  const task = testEnvironment.getVar('TASK');

  /** The active landscape's context, for defaults, if it can be read */
  const activeContext = (): any => {
    try {
      return require('../../context/context.json');
    }
    catch {
      return {};
    }
  };

  (async () => {
    const context = activeContext();
    const lambdaFunction = new LambdaFunction({
      landscape: testEnvironment.getVar('LANDSCAPE') || context.TAGS?.Landscape,
      region: testEnvironment.getVar('REGION') || context.REGION,
      stackId: testEnvironment.getVar('STACK_ID') || context.DASHBOARD?.STACK_ID
    });

    switch (task) {
      case 'show': {
        const environment = LambdaFunction.masked(await lambdaFunction.getEnvironment());
        console.log(`Environment of ${lambdaFunction.name} (${lambdaFunction.region}):`, JSON.stringify(environment, null, 2));
        break;
      }
      default:
        throw new Error(`Unknown DASHBOARD_LAMBDA_FUNCTION_TASK: ${task} (show)`);
    }
  })().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
