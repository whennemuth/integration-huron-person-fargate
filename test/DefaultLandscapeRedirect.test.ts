/**
 * Tests the dashboard domain (dashboard/lib/DashboardDomain.ts) and the default landscape's redirect
 * of it (dashboard/lib/DefaultLandscapeRedirect.ts): the synth-time checks, the CloudFront Function's
 * redirect, and the resources the default landscape's stack gets.
 */
import { App, Stack } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { IContext } from '../dashboard/context/IContext';
import { assertLandscapeHostname, dashboardDomain } from '../dashboard/lib/DashboardDomain';
import {
  certificateCovers, DefaultLandscapeRedirect, DefaultLandscapeRedirectPlan, planDefaultLandscapeRedirect,
  RedirectLookups, redirectFunctionCode
} from '../dashboard/lib/DefaultLandscapeRedirect';

const CERTIFICATE_ARN = 'arn:aws:acm:us-east-1:111111111111:certificate/abc';
const STACK_NAME = 'hp-dashboard-staging';

const context = (overrides: Record<string, any> = {}): IContext => ({
  REGION: 'us-east-2',
  ORIGIN: { subdomain: 'staging.person.huron.cssnprd.warhen.work' },
  DNS: { hostedZone: 'cssnprd.warhen.work', certificateARN: CERTIFICATE_ARN },
  isDefault: true,
  ...overrides
} as any);

const lookups = (overrides: Partial<RedirectLookups> = {}): RedirectLookups => ({
  certificateNames: jest.fn().mockResolvedValue(['person.huron.cssnprd.warhen.work', '*.person.huron.cssnprd.warhen.work']),
  hostedZoneId: jest.fn().mockResolvedValue('Z123'),
  aRecordExists: jest.fn().mockResolvedValue(false),
  recordOwner: jest.fn().mockResolvedValue(undefined),
  ...overrides
});

describe('dashboard domain', () => {
  it('is the landscape hostname minus its first label', () => {
    expect(dashboardDomain('staging.person.huron.cssnprd.warhen.work')).toBe('person.huron.cssnprd.warhen.work');
  });

  it('requires the hostname to start with the landscape', () => {
    expect(() => assertLandscapeHostname({ landscape: 'staging', subdomain: 'staging.person.huron.cssnprd.warhen.work' })).not.toThrow();
    expect(() => assertLandscapeHostname({ landscape: 'staging' })).not.toThrow();
    expect(() => assertLandscapeHostname({ landscape: 'staging', subdomain: 'preview.person.huron.cssnprd.warhen.work' }))
      .toThrow(/must be <landscape>.<dashboard domain>/);
    expect(() => assertLandscapeHostname({ landscape: 'staging', subdomain: 'staging.com' })).toThrow();
  });
});

describe('certificateCovers', () => {
  it('matches the exact name, or a wildcard one label up', () => {
    expect(certificateCovers('person.huron.cssnprd.warhen.work', 'person.huron.cssnprd.warhen.work')).toBe(true);
    expect(certificateCovers('*.huron.cssnprd.warhen.work', 'person.huron.cssnprd.warhen.work')).toBe(true);
    expect(certificateCovers('*.cssnprd.warhen.work', 'person.huron.cssnprd.warhen.work')).toBe(false);
    expect(certificateCovers('*.person.huron.cssnprd.warhen.work', 'person.huron.cssnprd.warhen.work')).toBe(false);
  });
});

describe('redirect function', () => {
  const handler = new Function(`${redirectFunctionCode('staging.person.huron.cssnprd.warhen.work')}; return handler;`)();
  const redirect = (uri: string, querystring: Record<string, any> = {}) => handler({ request: { uri, querystring } });

  it('answers with a 302 to the default landscape, keeping the path', () => {
    const response = redirect('/dashboard');
    expect(response.statusCode).toBe(302);
    expect(response.headers.location.value).toBe('https://staging.person.huron.cssnprd.warhen.work/dashboard');
    expect(response.headers['cache-control'].value).toBe('no-store');
  });

  it('keeps the query string as sent, including repeated and valueless keys', () => {
    const response = redirect('/api/runs', {
      limit: { value: '5' },
      tag: { value: 'a', multiValue: [{ value: 'a' }, { value: 'b%20c' }] },
      flag: { value: '' },
    });
    expect(response.headers.location.value)
      .toBe('https://staging.person.huron.cssnprd.warhen.work/api/runs?limit=5&tag=a&tag=b%20c&flag');
  });
});

describe('planDefaultLandscapeRedirect', () => {
  beforeEach(() => jest.spyOn(console, 'warn').mockImplementation(() => {}));
  afterEach(() => jest.restoreAllMocks());

  it('redirects the dashboard domain to the landscape and creates its record when none exists', async () => {
    const plan = await planDefaultLandscapeRedirect({ context: context(), stackName: STACK_NAME, lookups: lookups() });
    expect(plan).toEqual({
      domain: 'person.huron.cssnprd.warhen.work',
      targetHost: 'staging.person.huron.cssnprd.warhen.work',
      certificateArn: CERTIFICATE_ARN,
      hostedZone: 'cssnprd.warhen.work',
      hostedZoneId: 'Z123',
      createRecord: true,
    });
  });

  it('keeps a record this stack owns, and leaves alone one it does not', async () => {
    const owned = lookups({ aRecordExists: jest.fn().mockResolvedValue(true), recordOwner: jest.fn().mockResolvedValue(STACK_NAME) });
    expect((await planDefaultLandscapeRedirect({ context: context(), stackName: STACK_NAME, lookups: owned })).createRecord).toBe(true);

    const foreign = lookups({ aRecordExists: jest.fn().mockResolvedValue(true), recordOwner: jest.fn().mockResolvedValue('hp-dashboard-preview') });
    expect((await planDefaultLandscapeRedirect({ context: context(), stackName: STACK_NAME, lookups: foreign })).createRecord).toBe(false);
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('stack hp-dashboard-preview owns'));

    const unowned = lookups({ aRecordExists: jest.fn().mockResolvedValue(true) });
    expect((await planDefaultLandscapeRedirect({ context: context(), stackName: STACK_NAME, lookups: unowned })).createRecord).toBe(false);
  });

  it('fails when the certificate does not cover the dashboard domain', async () => {
    const wrongCertificate = lookups({ certificateNames: jest.fn().mockResolvedValue(['*.cssnprd.warhen.work']) });
    await expect(planDefaultLandscapeRedirect({ context: context(), stackName: STACK_NAME, lookups: wrongCertificate }))
      .rejects.toThrow(/does not cover person.huron.cssnprd.warhen.work/);
  });

  it('fails without a hostname, hosted zone or certificate, or when the zone is missing', async () => {
    for (const overrides of [{ ORIGIN: {} }, { DNS: { certificateARN: CERTIFICATE_ARN } }, { DNS: { hostedZone: 'cssnprd.warhen.work' } }]) {
      await expect(planDefaultLandscapeRedirect({ context: context(overrides), stackName: STACK_NAME, lookups: lookups() }))
        .rejects.toThrow(/needs DASHBOARD.ORIGIN.subdomain/);
    }
    const noZone = lookups({ hostedZoneId: jest.fn().mockResolvedValue(undefined) });
    await expect(planDefaultLandscapeRedirect({ context: context(), stackName: STACK_NAME, lookups: noZone }))
      .rejects.toThrow(/hosted zone cssnprd.warhen.work/);
  });
});

describe('DefaultLandscapeRedirect', () => {
  const plan: DefaultLandscapeRedirectPlan = {
    domain: 'person.huron.cssnprd.warhen.work',
    targetHost: 'staging.person.huron.cssnprd.warhen.work',
    certificateArn: CERTIFICATE_ARN,
    hostedZone: 'cssnprd.warhen.work',
    hostedZoneId: 'Z123',
    createRecord: true,
  };
  const synth = (overrides: Partial<DefaultLandscapeRedirectPlan> = {}) => {
    const stack = new Stack(new App(), STACK_NAME, { env: { account: '111111111111', region: 'us-east-2' } });
    new DefaultLandscapeRedirect(stack, 'default-landscape-redirect', { plan: { ...plan, ...overrides }, stackName: STACK_NAME });
    return Template.fromStack(stack);
  };

  it('serves the dashboard domain from a distribution whose viewer-request function redirects', () => {
    const template = synth();
    template.hasResourceProperties('AWS::CloudFront::Distribution', {
      DistributionConfig: Match.objectLike({
        Aliases: ['person.huron.cssnprd.warhen.work'],
        ViewerCertificate: Match.objectLike({ AcmCertificateArn: CERTIFICATE_ARN }),
        DefaultCacheBehavior: Match.objectLike({
          FunctionAssociations: [Match.objectLike({ EventType: 'viewer-request' })]
        })
      })
    });
    const [fn] = Object.values(template.findResources('AWS::CloudFront::Function'));
    expect(fn.Properties.FunctionConfig.Runtime).toBe('cloudfront-js-2.0');
    expect(fn.Properties.FunctionCode).toContain('"staging.person.huron.cssnprd.warhen.work"');
  });

  it('creates the alias A record and its ownership parameter', () => {
    const template = synth();
    template.hasResourceProperties('AWS::Route53::RecordSet', {
      Name: 'person.huron.cssnprd.warhen.work.', Type: 'A', HostedZoneId: 'Z123'
    });
    template.hasResourceProperties('AWS::SSM::Parameter', {
      Name: '/route53/arecords/person.huron.cssnprd.warhen.work', Value: STACK_NAME
    });
  });

  it('creates no record or ownership parameter when another stack owns the record', () => {
    const template = synth({ createRecord: false });
    template.resourceCountIs('AWS::CloudFront::Distribution', 1);
    template.resourceCountIs('AWS::Route53::RecordSet', 0);
    template.resourceCountIs('AWS::SSM::Parameter', 0);
  });
});
