import { ACMClient, DescribeCertificateCommand } from "@aws-sdk/client-acm";
import { ListHostedZonesByNameCommand, ListResourceRecordSetsCommand, Route53Client } from "@aws-sdk/client-route-53";
import { GetParameterCommand, ParameterNotFound, SSMClient } from "@aws-sdk/client-ssm";
import { CfnOutput } from "aws-cdk-lib";
import { Certificate } from "aws-cdk-lib/aws-certificatemanager";
import { CachePolicy, Distribution, Function as CloudFrontFunction, FunctionCode, FunctionEventType, FunctionRuntime, PriceClass, ViewerProtocolPolicy } from "aws-cdk-lib/aws-cloudfront";
import { HttpOrigin } from "aws-cdk-lib/aws-cloudfront-origins";
import { ARecord, HostedZone, RecordTarget } from "aws-cdk-lib/aws-route53";
import { CloudFrontTarget } from "aws-cdk-lib/aws-route53-targets";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { IContext } from "../context/IContext";
import { dashboardDomain } from "./DashboardDomain";

/** The SSM parameter that names the stack owning an A record, as the Shibboleth construct writes it */
export const recordOwnershipParameterName = (recordName: string): string => `/route53/arecords/${recordName}`;

/** Whether a certificate name (possibly a wildcard, which matches exactly one label) covers the domain */
export const certificateCovers = (certificateName: string, domain: string): boolean => {
  if (certificateName === domain) {
    return true;
  }
  return certificateName.startsWith('*.') && dashboardDomain(domain) === certificateName.slice(2);
}

/**
 * The CloudFront Function (cloudfront-js-2.0) that answers every request with a 302 to the target
 * host, keeping the path and query string. Query string keys and values are copied without
 * re-encoding, on the assumption that CloudFront hands them over as the viewer sent them (the
 * event structure docs don't say): check the redirect of an encoded value (e.g. `?q=a%20b`) after
 * deploying. A duplicated key's values are all in `multiValue`, the first repeated in `value`.
 */
export const redirectFunctionCode = (targetHost: string): string => `
function handler(event) {
  var request = event.request;
  var querystring = request.querystring;
  var query = Object.keys(querystring).map(function (key) {
    var param = querystring[key];
    var values = param.multiValue ? param.multiValue.map(function (v) { return v.value; }) : [param.value];
    return values.map(function (value) { return value === '' ? key : key + '=' + value; }).join('&');
  }).join('&');
  return {
    statusCode: 302,
    statusDescription: 'Found',
    headers: {
      location: { value: 'https://' + ${JSON.stringify(targetHost)} + request.uri + (query ? '?' + query : '') },
      'cache-control': { value: 'no-store' }
    }
  };
}`;

/** What the redirect needs to know from AWS at synth */
export type RedirectLookups = {
  certificateNames: (certificateArn: string) => Promise<string[]>;
  hostedZoneId: (hostedZone: string) => Promise<string | undefined>;
  aRecordExists: (hostedZoneId: string, recordName: string) => Promise<boolean>;
  recordOwner: (recordName: string) => Promise<string | undefined>;
};

export const awsRedirectLookups = (region?: string): RedirectLookups => {
  const route53 = new Route53Client({ region });
  return {
    certificateNames: async (certificateArn) => {
      // ACM is regional: ask the certificate's own region (us-east-1 for CloudFront)
      const acm = new ACMClient({ region: certificateArn.split(':')[3] });
      const { Certificate } = await acm.send(new DescribeCertificateCommand({ CertificateArn: certificateArn }));
      return Certificate?.SubjectAlternativeNames ?? (Certificate?.DomainName ? [Certificate.DomainName] : []);
    },
    hostedZoneId: async (hostedZone) => {
      const { HostedZones = [] } = await route53.send(new ListHostedZonesByNameCommand({ DNSName: hostedZone, MaxItems: 1 }));
      const zone = HostedZones.find(zone => zone.Name === `${hostedZone}.` && !zone.Config?.PrivateZone);
      return zone?.Id?.replace('/hostedzone/', '');
    },
    aRecordExists: async (hostedZoneId, recordName) => {
      const { ResourceRecordSets = [] } = await route53.send(new ListResourceRecordSetsCommand({
        HostedZoneId: hostedZoneId, StartRecordName: recordName, StartRecordType: 'A', MaxItems: 1
      }));
      return ResourceRecordSets.some(record => record.Name === `${recordName}.` && record.Type === 'A');
    },
    recordOwner: async (recordName) => {
      try {
        const { Parameter } = await new SSMClient({ region }).send(
          new GetParameterCommand({ Name: recordOwnershipParameterName(recordName) })
        );
        return Parameter?.Value;
      }
      catch (error) {
        if (error instanceof ParameterNotFound) {
          return undefined;
        }
        throw error;
      }
    },
  };
}

export type DefaultLandscapeRedirectPlan = {
  /** The dashboard domain, which redirects */
  domain: string;
  /** The default landscape's hostname, where it redirects to */
  targetHost: string;
  certificateArn: string;
  hostedZone: string;
  hostedZoneId: string;
  /** False when an A record for the domain exists that another stack (or nobody) owns */
  createRecord: boolean;
};

/**
 * The synth-time checks for the default landscape's redirect: the context has a hostname, a hosted
 * zone and a certificate covering the dashboard domain, and whether this stack may own the domain's
 * A record (only when no record exists, or its ownership parameter names this stack - the rule the
 * Shibboleth construct applies to the landscape's own record).
 */
export const planDefaultLandscapeRedirect = async (params: {
  context: IContext, stackName: string, lookups?: RedirectLookups
}): Promise<DefaultLandscapeRedirectPlan> => {
  const { context: { REGION, ORIGIN, DNS }, stackName, lookups = awsRedirectLookups(REGION) } = params;
  const { subdomain: targetHost } = ORIGIN ?? {};
  const { hostedZone, certificateARN: certificateArn } = DNS ?? {};
  if (!targetHost || !hostedZone || !certificateArn) {
    throw new Error(
      'The default landscape (DASHBOARD.isDefault) redirects its dashboard domain, so it needs ' +
      'DASHBOARD.ORIGIN.subdomain, DASHBOARD.DNS.hostedZone and DASHBOARD.DNS.certificateARN'
    );
  }
  const domain = dashboardDomain(targetHost);

  const names = await lookups.certificateNames(certificateArn);
  if (!names.some(name => certificateCovers(name, domain))) {
    throw new Error(
      `Certificate ${certificateArn} (${names.join(', ')}) does not cover ${domain}, which the default ` +
      `landscape redirects. Use a certificate naming both ${domain} and *.${domain}.`
    );
  }

  const hostedZoneId = await lookups.hostedZoneId(hostedZone);
  if (!hostedZoneId) {
    throw new Error(`Public hosted zone ${hostedZone} (DASHBOARD.DNS.hostedZone) not found`);
  }

  let createRecord = true;
  if (await lookups.aRecordExists(hostedZoneId, domain)) {
    const owner = await lookups.recordOwner(domain);
    if (owner !== stackName) {
      createRecord = false;
      console.warn(
        `An A record for ${domain} exists that ${owner ? `stack ${owner} owns` : 'no stack owns'} ` +
        `(${recordOwnershipParameterName(domain)}), so ${stackName} leaves it alone and creates no record ` +
        `for its redirect. Remove that record (and its parameter) and redeploy to have the redirect answer ${domain}.`
      );
    }
  }
  return { domain, targetHost, certificateArn, hostedZone, hostedZoneId, createRecord };
}

/**
 * The default landscape's dashboard domain (e.g. person.huron.cssnprd.warhen.work) redirects to
 * the default landscape's hostname: a CloudFront distribution whose viewer-request function answers
 * every request with a 302 (not a 301, because the default can move and browsers cache a 301), so
 * its origin is never reached and no sign-in is involved. Plus the domain's alias A record and its
 * ownership parameter, unless another stack owns the record.
 */
export class DefaultLandscapeRedirect extends Construct {
  private _distribution: Distribution;

  constructor(scope: Construct, id: string, props: { plan: DefaultLandscapeRedirectPlan, stackName: string }) {
    super(scope, id);
    const { plan: { domain, targetHost, certificateArn, hostedZone, hostedZoneId, createRecord }, stackName } = props;

    const redirectFunction = new CloudFrontFunction(this, 'function', {
      code: FunctionCode.fromInline(redirectFunctionCode(targetHost)),
      runtime: FunctionRuntime.JS_2_0,
      comment: `Redirects ${domain} to ${targetHost}`,
    });

    this._distribution = new Distribution(this, 'distribution', {
      comment: `${stackName}-default-landscape-redirect`,
      priceClass: PriceClass.PRICE_CLASS_100,
      domainNames: [domain],
      certificate: Certificate.fromCertificateArn(this, 'certificate', certificateArn),
      defaultBehavior: {
        // Never reached: the function answers every request
        origin: new HttpOrigin(targetHost),
        // The function redirects http and https alike, straight to https
        viewerProtocolPolicy: ViewerProtocolPolicy.ALLOW_ALL,
        cachePolicy: CachePolicy.CACHING_DISABLED,
        functionAssociations: [{ function: redirectFunction, eventType: FunctionEventType.VIEWER_REQUEST }],
      },
    });

    if (createRecord) {
      const record = new ARecord(this, 'a-record', {
        zone: HostedZone.fromHostedZoneAttributes(this, 'hosted-zone', { hostedZoneId, zoneName: hostedZone }),
        recordName: domain,
        target: RecordTarget.fromAlias(new CloudFrontTarget(this._distribution)),
        comment: `ALIAS record for ${domain}, redirecting to the default landscape ${targetHost}`,
      });
      new StringParameter(record, 'ownership', {
        parameterName: recordOwnershipParameterName(domain),
        stringValue: stackName,
        description: `Tracks that A record ${domain} was created by stack ${stackName}`,
      });
    }

    new CfnOutput(scope, 'DefaultLandscapeRedirectURL', {
      value: `https://${domain}`,
      description: `Redirects to the default landscape, https://${targetHost}`,
    });
  }

  public get distribution(): Distribution {
    return this._distribution;
  }
}
