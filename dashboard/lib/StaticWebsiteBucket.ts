import { RemovalPolicy } from "aws-cdk-lib";
import { AllowedMethods, BehaviorOptions, CachePolicy, Distribution, ViewerProtocolPolicy } from "aws-cdk-lib/aws-cloudfront";
import { S3StaticWebsiteOrigin } from "aws-cdk-lib/aws-cloudfront-origins";
import { BlockPublicAccess, Bucket, ObjectOwnership } from "aws-cdk-lib/aws-s3";
import { BucketDeployment, Source } from "aws-cdk-lib/aws-s3-deployment";
import { Construct } from "constructs";
import { join } from "path";
import { IContext } from "../context/IContext";

export type StaticWebsiteBucketParams = {
  scope: Construct;
  distribution: Distribution;
  context: IContext;
};

/**
 * Creates an S3 bucket for static website assets and configures a provided CloudFront 
 * distribution to serve them by adding behaviors for JavaScript and CSS assets.
 * 
 * USAGE: new StaticWebsiteBucket({ scope: dashboardApp, distribution, context });
 */
export class StaticWebsiteBucket {
  private _staticAssetsBucket: Bucket;

  constructor(parms: StaticWebsiteBucketParams) { 
    const { scope, distribution, context } = parms;
    const { STACK_ID, TAGS: { Landscape }, ORIGIN: { subdomain } = {} } = context;
    const staticAssetsBucketName = `${STACK_ID}-static-assets-${Landscape}`;
    const customDomain = !!subdomain;

    // Create S3 bucket for static assets with website hosting enabled
    this._staticAssetsBucket = new Bucket(scope, staticAssetsBucketName, {
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      objectOwnership: ObjectOwnership.OBJECT_WRITER,
      publicReadAccess: true,
      blockPublicAccess: BlockPublicAccess.BLOCK_ACLS,
      websiteIndexDocument: 'index.html',
      websiteErrorDocument: 'error.html'
    });

    // Deploy static assets to S3
    new BucketDeployment(scope, `${staticAssetsBucketName}-deployment`, {
      sources: [Source.asset(join(__dirname, '../public'))],
      destinationBucket: this._staticAssetsBucket,
    });

    // Create S3 static website origin for static assets
    const staticAssetsOrigin = new S3StaticWebsiteOrigin(this._staticAssetsBucket);
    
    // Define behavior for static assets
    const { ALLOW_ALL, REDIRECT_TO_HTTPS } = ViewerProtocolPolicy;
    const staticAssetsBehaviorOptions = {
      allowedMethods: AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
      cachedMethods: AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
      viewerProtocolPolicy: customDomain ? REDIRECT_TO_HTTPS : ALLOW_ALL,
      cachePolicy: CachePolicy.CACHING_OPTIMIZED,
      compress: true
    };

    distribution.addBehavior('/js/*', staticAssetsOrigin, staticAssetsBehaviorOptions);
    distribution.addBehavior('/css/*', staticAssetsOrigin, staticAssetsBehaviorOptions);
  }

  public get staticAssetsBucket(): Bucket {
    return this._staticAssetsBucket;
  }
}