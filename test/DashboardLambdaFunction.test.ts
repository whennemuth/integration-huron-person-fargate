/**
 * Tests the deployed dashboard Lambda reader (dashboard/scripts/LambdaFunction.ts) against a stubbed Lambda
 * client, and that it finds the function by the name CDK gives it.
 */
import { GetFunctionConfigurationCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { mockClient } from 'aws-sdk-client-mock';
import { dashboardFunctionName } from '../dashboard/lib/DashboardFunctionName';
import { DashboardFunctionNotFoundError, LambdaFunction } from '../dashboard/scripts/LambdaFunction';

const lambdaMock = mockClient(LambdaClient);

describe('LambdaFunction', () => {
  beforeEach(() => lambdaMock.reset());

  const lambdaFunction = (stackId?: string) =>
    new LambdaFunction({ landscape: 'preview', region: 'us-east-2', stackId, client: new LambdaClient({ region: 'us-east-2' }) });

  it('is named as CDK names the landscape\'s dashboard function', () => {
    expect(lambdaFunction().name).toBe('hp-dashboard-preview-app-function');
    expect(lambdaFunction('other-dashboard').name).toBe('other-dashboard-preview-app-function');
    expect(dashboardFunctionName('hp-dashboard', 'preview')).toBe(lambdaFunction().name);
  });

  it('needs a landscape and a region', () => {
    expect(() => new LambdaFunction({ landscape: '', region: 'us-east-2' })).toThrow('needs a landscape and a region');
    expect(() => new LambdaFunction({ landscape: 'preview', region: undefined as unknown as string })).toThrow('needs a landscape and a region');
  });

  it('gives the function\'s environment variables', async () => {
    lambdaMock.on(GetFunctionConfigurationCommand, { FunctionName: 'hp-dashboard-preview-app-function' }).resolves({
      Environment: { Variables: { LANDSCAPE: 'preview', REGION: 'us-east-2', ORIGIN_VERIFY_SECRET: 'shh' } }
    });
    expect(await lambdaFunction().getEnvironment()).toEqual({ LANDSCAPE: 'preview', REGION: 'us-east-2', ORIGIN_VERIFY_SECRET: 'shh' });
  });

  it('gives an empty environment for a function that has none', async () => {
    lambdaMock.on(GetFunctionConfigurationCommand).resolves({});
    expect(await lambdaFunction().getEnvironment()).toEqual({});
  });

  it('names the function, landscape and region when the function does not exist', async () => {
    lambdaMock.on(GetFunctionConfigurationCommand).rejects(Object.assign(new Error('Function not found'), { name: 'ResourceNotFoundException' }));
    const failure = lambdaFunction().getEnvironment();
    await expect(failure).rejects.toThrow(
      'No dashboard Lambda function hp-dashboard-preview-app-function for the preview landscape in us-east-2'
    );
    await expect(failure).rejects.toBeInstanceOf(DashboardFunctionNotFoundError);
  });

  it('passes other failures on as AWS gave them', async () => {
    lambdaMock.on(GetFunctionConfigurationCommand).rejects(Object.assign(new Error('The security token included in the request is expired'), { name: 'ExpiredTokenException' }));
    const failure = lambdaFunction().getEnvironment();
    await expect(failure).rejects.toThrow('The security token included in the request is expired');
    await expect(failure).rejects.not.toBeInstanceOf(DashboardFunctionNotFoundError);
  });

  it('leaves out the secret variables for a local run, and masks them when shown', () => {
    const environment = { REGION: 'us-east-2', ORIGIN_VERIFY_SECRET: 'shh', LANDSCAPE: 'preview' };
    expect(LambdaFunction.isSecret('ORIGIN_VERIFY_SECRET')).toBe(true);
    expect(LambdaFunction.isSecret('LANDSCAPE')).toBe(false);
    expect(LambdaFunction.withoutSecrets(environment)).toEqual({ REGION: 'us-east-2', LANDSCAPE: 'preview' });
    const masked = LambdaFunction.masked(environment);
    expect(Object.keys(masked)).toEqual(['LANDSCAPE', 'ORIGIN_VERIFY_SECRET', 'REGION']);
    expect(masked.ORIGIN_VERIFY_SECRET).toBe(LambdaFunction.MASK);
    expect(JSON.stringify(masked)).not.toContain('shh');
  });

  it('reports an environment Lambda cannot give, and passes other errors on', async () => {
    lambdaMock.on(GetFunctionConfigurationCommand).resolves({ Environment: { Error: { ErrorCode: 'KMSAccessDeniedException', Message: 'denied' } } });
    await expect(lambdaFunction().getEnvironment()).rejects.toThrow('KMSAccessDeniedException denied');

    lambdaMock.on(GetFunctionConfigurationCommand).rejects(Object.assign(new Error('expired'), { name: 'ExpiredTokenException' }));
    await expect(lambdaFunction().getEnvironment()).rejects.toThrow('expired');
  });
});
