/**
 * Tests the landscape dashboard's resource wiring (dashboard/lib): the names of the pipeline
 * resources it reads, the Lambda's environment and read-only grants on them, and the landscape's
 * registry parameter.
 */
import { App, Stack } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { IContext as LandscapeContext } from '../context/IContext';
import { AuthenticationMode, IContext as DashboardContext, DashboardSettings, toDashboardContext } from '../dashboard/context/IContext';
import { FunctionUrlOrigin } from '../dashboard/lib/FunctionUrl';
import { landscapeResources } from '../dashboard/lib/LandscapeResources';
import { getDashboardStackName } from '../dashboard/lib/DashboardStacks';

const makeLandscape = (storage: 's3' | 'dynamodb', withDashboard = true): LandscapeContext => ({
  STACK_ID: 'huron-person-fargate-processor',
  ACCOUNT: '111111111111',
  REGION: 'us-east-2',
  S3: { chunksBucket: 'huron-person-chunks' },
  TAGS: { Landscape: 'mock', Service: 'svc', Function: 'fn' },
  PREVIOUS_STORAGE_TYPE: storage,
  ...(withDashboard && {
    DASHBOARD: {
      STACK_ID: 'huron-person-dashboard',
      AUTHENTICATION_MODE: AuthenticationMode.PUBLIC,
      LAMBDA_TIMEOUT_SECONDS: 180,
      LAMBDA_MEMORY_SIZE_MB: 1024,
    }
  })
} as any);

describe('landscapeResources', () => {
  it('names the DynamoDB-mode resources, including the person history table', () => {
    expect(landscapeResources(makeLandscape('dynamodb'))).toEqual({
      landscape: 'mock',
      storageMode: 'dynamodb',
      integrationSecretName: 'huron-person-fargate-processor/integration/_config/mock',
      statisticsTableName: 'huron-person-fargate-processor-statistics-mock',
      personHistoryTableName: 'huron-person-fargate-processor-person-history-mock',
      chunksBucketName: 'huron-person-chunks-mock',
    });
  });

  it('has no person history table in S3 mode', () => {
    const resources = landscapeResources(makeLandscape('s3'));
    expect(resources.storageMode).toBe('s3');
    expect(resources.personHistoryTableName).toBeUndefined();
    expect(resources.statisticsTableName).toBe('huron-person-fargate-processor-statistics-mock');
  });
});

describe('getDashboardStackName', () => {
  it('is the dashboard STACK_ID suffixed with the landscape', () => {
    expect(getDashboardStackName(makeLandscape('s3'))).toBe('huron-person-dashboard-mock');
  });

  it('is undefined when the landscape has no dashboard', () => {
    expect(getDashboardStackName(makeLandscape('s3', false))).toBeUndefined();
  });
});

describe('FunctionUrlOrigin', () => {
  const synth = (storage: 's3' | 'dynamodb', dashboardOverrides: Record<string, any> = {}) => {
    // No stack is bundled, so the Lambda entry is not built (no esbuild or generated templates needed)
    const app = new App({ context: { 'aws:cdk:bundling-stacks': [] } });
    const stack = new Stack(app, 'test-stack', { env: { account: '111111111111', region: 'us-east-2' } });
    const landscape = makeLandscape(storage);
    const settings = { ...landscape.DASHBOARD, ...dashboardOverrides } as unknown as DashboardSettings;
    const context: DashboardContext = toDashboardContext(landscape, settings);
    new FunctionUrlOrigin({ stack, context, landscapeResources: landscapeResources(landscape) });
    return Template.fromStack(stack);
  };

  it('points the Lambda at the landscape resources', () => {
    synth('dynamodb').hasResourceProperties('AWS::Lambda::Function', {
      FunctionName: 'huron-person-dashboard-mock-app-function',
      Timeout: 180,
      MemorySize: 1024,
      Environment: {
        Variables: Match.objectLike({
          INTEGRATION_SECRETS_ARN: 'huron-person-fargate-processor/integration/_config/mock',
          INTEGRATION_SECRETS_REGION: 'us-east-2',
          LANDSCAPE: 'mock',
          PREVIOUS_STORAGE_TYPE: 'dynamodb',
          DYNAMODB_STATISTICS_TABLE_NAME: 'huron-person-fargate-processor-statistics-mock',
          DYNAMODB_PERSON_HISTORY_TABLE_NAME: 'huron-person-fargate-processor-person-history-mock',
          CHUNKS_BUCKET: 'huron-person-chunks-mock',
        })
      }
    });
  });

  it('omits the person history table in S3 mode', () => {
    const template = synth('s3');
    const [fn] = Object.values(template.findResources('AWS::Lambda::Function'));
    expect(fn.Properties.Environment.Variables.DYNAMODB_PERSON_HISTORY_TABLE_NAME).toBeUndefined();
    expect(fn.Properties.Environment.Variables.PREVIOUS_STORAGE_TYPE).toBe('s3');
  });

  it('grants read-only access to the secret, tables and chunks bucket', () => {
    const template = synth('dynamodb');
    const statements = Object.values(template.findResources('AWS::IAM::Policy'))
      .flatMap((policy: any) => policy.Properties.PolicyDocument.Statement);
    const actions = statements.flatMap((s: any) => [s.Action].flat());

    expect(actions).toEqual(expect.arrayContaining([
      'secretsmanager:GetSecretValue', 'dynamodb:Query', 'dynamodb:GetItem', 's3:GetObject', 's3:ListBucket'
    ]));
    expect(actions.some((a: string) => /^(dynamodb:(Put|Update|Delete|BatchWrite)|s3:(Put|Delete))/.test(a))).toBe(false);

    const resources = JSON.stringify(statements.map((s: any) => s.Resource));
    expect(resources).toContain('table/huron-person-fargate-processor-statistics-mock/index/*');
    expect(resources).toContain('table/huron-person-fargate-processor-person-history-mock');
    expect(resources).toContain(':::huron-person-chunks-mock/*');
  });

  it('registers the landscape in the landscape registry, with values derived from the context', () => {
    const template = synth('dynamodb', {
      LABEL: 'Mock', isDefault: true, ORIGIN: { subdomain: 'mock.cssnprd.warhen.work' }, DNS: { hostedZone: 'cssnprd.warhen.work' }
    });
    const [registration] = Object.values(template.findResources('AWS::SSM::Parameter', {
      Properties: { Name: '/hp-dashboard/landscapes/mock' }
    }));
    expect(JSON.parse(registration.Properties.Value)).toEqual({
      landscape: 'mock', label: 'Mock', url: 'https://mock.cssnprd.warhen.work', isDefault: true
    });
    expect(registration.Properties.Type).toBe('String');
    expect(registration.Properties.Description.length).toBeLessThanOrEqual(1024);
    template.hasResourceProperties('AWS::Lambda::Function', {
      Environment: { Variables: Match.objectLike({
        LANDSCAPES_PARAMETER_PATH: '/hp-dashboard/landscapes', HOSTED_ZONE: 'cssnprd.warhen.work'
      }) }
    });
  });

  it('defaults the registry label to the landscape and isDefault to false, with no url without a subdomain', () => {
    const template = synth('dynamodb');
    const [registration] = Object.values(template.findResources('AWS::SSM::Parameter', {
      Properties: { Name: '/hp-dashboard/landscapes/mock' }
    }));
    expect(JSON.parse(registration.Properties.Value)).toEqual({ landscape: 'mock', label: 'mock', isDefault: false });
    const [fn] = Object.values(template.findResources('AWS::Lambda::Function'));
    expect(fn.Properties.Environment.Variables.HOSTED_ZONE).toBeUndefined();
  });

  it('grants read-only access to the registry path', () => {
    const statements = Object.values(synth('dynamodb').findResources('AWS::IAM::Policy'))
      .flatMap((policy: any) => policy.Properties.PolicyDocument.Statement);
    const ssm = statements.filter((s: any) => [s.Action].flat().some((a: string) => a.startsWith('ssm:')));
    expect(ssm.flatMap((s: any) => [s.Action].flat())).toEqual(['ssm:GetParametersByPath']);
    const resources = JSON.stringify(ssm.map((s: any) => s.Resource));
    expect(resources).toContain(':parameter/hp-dashboard/landscapes\"');
    expect(resources).toContain(':parameter/hp-dashboard/landscapes/*');
  });

  it('references no other stack and keeps the origin verify secret per landscape', () => {
    const template = synth('dynamodb');
    expect(JSON.stringify(template.toJSON())).not.toContain('Fn::ImportValue');
    template.hasResourceProperties('AWS::SSM::Parameter', {
      Name: '/cloudfront/huron-person-dashboard/mock/origin-verify-secret'
    });
  });
});
