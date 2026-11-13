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
  ITEMS_PER_CHUNK: 200,
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
      chunkerFunctionName: 'chunker-subscriber-mock',
      processorSubscriberFunctionName: 'processor-subscriber-mock',
      chunkerClusterName: 'huron-person-cluster-mock',
      chunkerServiceName: 'Chunker',
      pipelineStackId: 'huron-person-fargate-processor',
      itemsPerChunk: 200,
    });
  });

  it("takes none of a start's choices from the context: not its chunksPerTask, nor its previous-state flags", () => {
    const resources = landscapeResources({
      ...makeLandscape('dynamodb'), ECS: { chunkerService: { chunksPerTask: 3 } }, BULK_RESET: true, TRUST_PREVIOUS_STORAGE: true
    } as any);
    expect(resources).not.toHaveProperty('chunksPerTask');
    expect(resources).not.toHaveProperty('bulkReset');
    expect(resources).not.toHaveProperty('trustPreviousStorage');
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
  const synthWithOrigin = (storage: 's3' | 'dynamodb', dashboardOverrides: Record<string, any> = {}) => {
    // No stack is bundled, so the Lambda entry is not built (no esbuild or generated templates needed)
    const app = new App({ context: { 'aws:cdk:bundling-stacks': [] } });
    const stack = new Stack(app, 'test-stack', { env: { account: '111111111111', region: 'us-east-2' } });
    const landscape = makeLandscape(storage);
    const settings = { ...landscape.DASHBOARD, ...dashboardOverrides } as unknown as DashboardSettings;
    const context: DashboardContext = toDashboardContext(landscape, settings);
    const origin = new FunctionUrlOrigin({ stack, context, landscapeResources: landscapeResources(landscape) });
    return { stack, origin, template: Template.fromStack(stack) };
  };
  const synth = (storage: 's3' | 'dynamodb', dashboardOverrides: Record<string, any> = {}) =>
    synthWithOrigin(storage, dashboardOverrides).template;

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
    // No writes: starting a run invokes the chunker subscriber, which resets the chunker's counters itself
    expect(actions.some((a: string) => /^(dynamodb:(Put|Update|Delete|BatchWrite)|s3:(Put|Delete))/.test(a))).toBe(false);

    const resources = JSON.stringify(statements.map((s: any) => s.Resource));
    expect(resources).toContain('table/huron-person-fargate-processor-statistics-mock/index/*');
    expect(resources).toContain('table/huron-person-fargate-processor-person-history-mock');
    expect(resources).toContain(':::huron-person-chunks-mock/*');
    expect(resources).not.toContain('atomic-counter');
  });

  it('may start a run as the schedule does: invoke the chunker subscriber, read both subscribers, nothing else', () => {
    const template = synth('dynamodb');
    const statements = Object.values(template.findResources('AWS::IAM::Policy'))
      .flatMap((policy: any) => policy.Properties.PolicyDocument.Statement)
      .filter((s: any) => [s.Action].flat().some((a: string) => a.startsWith('lambda:')));
    const resourcesOf = (action: string) => JSON.stringify(statements.filter((s: any) => [s.Action].flat().includes(action)).map((s: any) => s.Resource));

    expect(resourcesOf('lambda:InvokeFunction')).toContain('function:chunker-subscriber-mock');
    expect(resourcesOf('lambda:InvokeFunction')).not.toContain('processor-subscriber');
    expect(resourcesOf('lambda:GetFunctionConfiguration')).toContain('function:chunker-subscriber-mock');
    expect(resourcesOf('lambda:GetFunctionConfiguration')).toContain('function:processor-subscriber-mock');
    expect(statements.flatMap((s: any) => [s.Action].flat()).every((a: string) => ['lambda:InvokeFunction', 'lambda:GetFunctionConfiguration'].includes(a))).toBe(true);
  });

  it("may read the chunker service's maximum for Parallel start, and change no scaling itself", () => {
    const template = synth('dynamodb');
    const actions = Object.values(template.findResources('AWS::IAM::Policy'))
      .flatMap((policy: any) => policy.Properties.PolicyDocument.Statement)
      .flatMap((s: any) => [s.Action].flat());
    expect(actions.filter((a: string) => a.startsWith('application-autoscaling:'))).toEqual(['application-autoscaling:DescribeScalableTargets']);
    expect(actions.filter((a: string) => a.startsWith('ecs:') || a.startsWith('cloudwatch:'))).toEqual([]);
    template.hasResourceProperties('AWS::Lambda::Function', {
      Environment: { Variables: Match.objectLike({ CHUNKER_CLUSTER_NAME: 'huron-person-cluster-mock', CHUNKER_SERVICE_NAME: 'Chunker' }) }
    });

    template.hasResourceProperties('AWS::Lambda::Function', {
      Environment: {
        Variables: Match.objectLike({
          CHUNKER_FUNCTION_NAME: 'chunker-subscriber-mock',
          PROCESSOR_SUBSCRIBER_FUNCTION_NAME: 'processor-subscriber-mock',
          PIPELINE_STACK_ID: 'huron-person-fargate-processor',
          ITEMS_PER_CHUNK: '200'
        })
      }
    });
  });

  it('registers the landscape in the landscape registry, with values derived from the context', () => {
    const template = synth('dynamodb', {
      LABEL: 'Mock', isDefault: true, ORIGIN: { subdomain: 'mock.person.huron.cssnprd.warhen.work' }, DNS: { hostedZone: 'cssnprd.warhen.work' }
    });
    const [registration] = Object.values(template.findResources('AWS::SSM::Parameter', {
      Properties: { Name: '/hp-dashboard/landscapes/mock' }
    }));
    expect(JSON.parse(registration.Properties.Value)).toEqual({
      landscape: 'mock', label: 'Mock', url: 'https://mock.person.huron.cssnprd.warhen.work', isDefault: true
    });
    expect(registration.Properties.Type).toBe('String');
    expect(registration.Properties.Description.length).toBeLessThanOrEqual(1024);
    template.hasResourceProperties('AWS::Lambda::Function', {
      Environment: { Variables: Match.objectLike({
        LANDSCAPES_PARAMETER_PATH: '/hp-dashboard/landscapes', DASHBOARD_DOMAIN: 'person.huron.cssnprd.warhen.work'
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
    expect(fn.Properties.Environment.Variables.DASHBOARD_DOMAIN).toBeUndefined();
  });

  it('passes the landscape tone to the Lambda only when set, and refuses one that names no tone', () => {
    const variables = (template: Template) => Object.values(template.findResources('AWS::Lambda::Function'))[0].Properties.Environment.Variables;
    expect(variables(synth('dynamodb', { LANDSCAPE_TONE: 'production' })).LANDSCAPE_TONE).toBe('production');
    // The mock-landscape default is applied by the Lambda, from LANDSCAPE: nothing is baked in for it
    expect(variables(synth('dynamodb')).LANDSCAPE_TONE).toBeUndefined();
    expect(() => synth('dynamodb', { LANDSCAPE_TONE: 'prod' })).toThrow("LANDSCAPE_TONE must be one of production, test, mock (got 'prod')");
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

  // Assertions compare booleans and lengths, never the value, so a failure cannot print a secret
  it('makes a long, header-safe origin verify secret, shared by the parameter, Lambda and CloudFront header, new on every synth', () => {
    const secretOf = ({ stack, origin, template }: ReturnType<typeof synthWithOrigin>) => {
      const [parameter] = Object.values(template.findResources('AWS::SSM::Parameter', {
        Properties: { Name: '/cloudfront/huron-person-dashboard/mock/origin-verify-secret' }
      }));
      const [fn] = Object.values(template.findResources('AWS::Lambda::Function'));
      const fromParameter: string = parameter.Properties.Value;
      const fromLambda: string = fn.Properties.Environment.Variables.ORIGIN_VERIFY_SECRET;
      // The header is not in the template (no distribution here), so read it from the origin's CloudFront config
      const headers = (origin.origin.bind(stack, { originId: 'test-origin' }).originProperty?.originCustomHeaders ?? []) as any[];
      const fromHeader = headers.find(({ headerName }) => headerName === 'X-Origin-Verify')?.headerValue;
      expect(fromParameter === fromLambda).toBe(true);
      expect(fromParameter === fromHeader).toBe(true);
      return fromParameter;
    };
    const first = secretOf(synthWithOrigin('dynamodb'));
    const second = secretOf(synthWithOrigin('dynamodb'));

    for (const secret of [first, second]) {
      // The prefix and 32 random bytes, base64url encoded (43 characters)
      expect(secret.length).toBe('cf-origin-verify-'.length + 43);
      expect(/^cf-origin-verify-[A-Za-z0-9_-]{43}$/.test(secret)).toBe(true);
    }
    expect(first === second).toBe(false);
  });
});
