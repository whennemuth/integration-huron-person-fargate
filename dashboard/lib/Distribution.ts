import { CfnOutput, RemovalPolicy } from "aws-cdk-lib";
import { Certificate, ICertificate } from "aws-cdk-lib/aws-certificatemanager";
import { AllowedMethods, BehaviorOptions, CachePolicy, Distribution, DistributionProps, OriginRequestPolicy, PriceClass, ViewerProtocolPolicy } from "aws-cdk-lib/aws-cloudfront";
import { Bucket, ObjectOwnership } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import { IContext } from "../context/IContext";
import { ParameterTester } from "../src/Utils";
import { FunctionUrlOrigin } from "./FunctionUrl";

export class CloudFrontDistribution extends Construct {
  private _distribution: Distribution;

  constructor(parms: { 
    scope: Construct, id: string, context: IContext, functionUrlOrigin: FunctionUrlOrigin 
  }) {
    super(parms.scope, parms.id);

    const { scope, context, functionUrlOrigin: { origin, url } } = parms;

    // Throw an error if the context is not valid.
    validateContext(context);

    // Destructure context for use below.
    const {
      STACK_ID, 
      TAGS: { Landscape }, 
      ORIGIN: { subdomain } = {}, 
      DNS: { certificateARN } = {} 
    } = context;

    // Define names
    const distributionName = `${STACK_ID}-cloudfront-distribution-${Landscape}`;

    // Determine if a custom domain is being used.
    const subdomains = [] as string[];
    if(subdomain) {
      subdomains.push(subdomain);
    }
    const customDomain = ():boolean => subdomains.length > 0;

    // Define the default behavior for the distribution (Lambda function)
    const { ALLOW_ALL, REDIRECT_TO_HTTPS } = ViewerProtocolPolicy;
    const defaultBehavior = {
      origin,
      allowedMethods: AllowedMethods.ALLOW_ALL,
      cachedMethods: AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
      viewerProtocolPolicy: customDomain() ? REDIRECT_TO_HTTPS : ALLOW_ALL,
      originRequestPolicy: OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
      cachePolicy: CachePolicy.CACHING_DISABLED       
    } as BehaviorOptions;

    // Configure distribution properties
    let distributionProps = {
      priceClass: PriceClass.PRICE_CLASS_100,
      logBucket: new Bucket(scope, `distribution-logs-bucket`, {
        removalPolicy: RemovalPolicy.DESTROY,    
        autoDeleteObjects: true,
        objectOwnership: ObjectOwnership.OBJECT_WRITER
      }),
      comment: `${STACK_ID}-${Landscape}-distribution`,  
      domainNames: subdomains, 
      defaultBehavior
    } as DistributionProps;

    // Extend distribution properties to include certificate and domain if indicated.
    if(customDomain()) {
      const certificate:ICertificate = Certificate.fromCertificateArn(this, `${distributionName}-acm-cert`, certificateARN!);
      distributionProps = Object.assign({
        certificate, 
        domainNames: subdomains
      }, distributionProps);
    }

    // Create the cloudFront distribution
    this._distribution = new Distribution(scope, distributionName, distributionProps);

    // Set relevant stack outputs
    if(subdomain) {
      new CfnOutput(scope, 'AppURL', {
        value: `https://${subdomain}`,
        description: 'CloudFront Distribution URL',
      });
    }

    new CfnOutput(scope, 'CloudFrontDistributionURL', {
      value: `https://${this._distribution.distributionDomainName}`,
      description: 'CloudFront Distribution URL',
    });

    new CfnOutput(scope, 'FunctionURL', {
      value: url,
      description: 'Function URL',
    });

  }

  public get distribution(): Distribution {
    return this._distribution;
  }
}

const validateContext = (context: IContext) => {
  const { ORIGIN: { subdomain } = {}, DNS: { certificateARN, hostedZone } = {} } = context;
  const { isNotBlank, anyBlank, someBlankSomeNot} = ParameterTester;

  // DNS should not be partially configured - Certificate and Route53 must go together.
  if(someBlankSomeNot(certificateARN, hostedZone)) {
    throw new Error('hostedZone and certificateARN are mutually inclusive');
  }

  // DNS should be configured if a subdomain is specified for the origin.
  if(isNotBlank(subdomain) && anyBlank(certificateARN, hostedZone)) {
    throw new Error('An origin subdomain must be supported by DNS.certificateARN and DNS.hostedZone');
  }

  // subdomain must be a subdomain of, or equal to, hostedZone
  if(isNotBlank(subdomain) && subdomain != hostedZone) {
    if( ! subdomain!.endsWith(`.${hostedZone}`)) {
      throw new Error(`${subdomain} is not a subdomain of ${hostedZone}`);
    }
  }
}

