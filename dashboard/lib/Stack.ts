import { HttpOrigin } from "aws-cdk-lib/aws-cloudfront-origins";
import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import { OriginFunctionUrl, OriginType } from "shibboleth-sp-at-edge";
import { AuthenticatedContext, PublicContext } from '../context/IContext';
import { FunctionUrlOrigin } from "./FunctionUrl";
import { CloudFrontDistribution } from "./Distribution";
import { Distribution } from "aws-cdk-lib/aws-cloudfront";
import { SecretsManagerSecret } from "./Secrets";

export type StackParams = {
  scope: Construct;
  stackName: string;
  context: AuthenticatedContext | PublicContext,
  stackProps?: cdk.StackProps;
};

export abstract class AbstractStack extends cdk.Stack {
  protected _secret: SecretsManagerSecret;
  protected _functionUrlOrigin: FunctionUrlOrigin;

  constructor(stackParams: StackParams) {
    const { scope, stackName, context, stackProps } = stackParams;

    super(scope, stackName, stackProps);

    this._secret = new SecretsManagerSecret(this, context);
    this._functionUrlOrigin = new FunctionUrlOrigin({
      stack: this, 
      context, 
      secretArn: this._secret.secretArn
    });
  }

  public get functionOrigin (): HttpOrigin {
    return this._functionUrlOrigin.origin;
  }

  public get functionUrl (): string {
    return this._functionUrlOrigin.url;
  }
}

/**
 * Define a CloudFront distribution with a Lambda Function URL as the origin.
 */
export class PublicStack extends AbstractStack {
  private _distribution: CloudFrontDistribution;

  constructor(private stackParams: StackParams) {
    super(stackParams);

    // Create the distribution with the function URL as origin
    this._distribution = new CloudFrontDistribution({
      scope: this, 
      id: 'distribution', 
      context: stackParams.context, 
      functionUrlOrigin: this._functionUrlOrigin
    });
  }

  public get distribution (): Distribution {
    return this._distribution.distribution;
  }
}

/**
 * Define a CloudFront distribution with the Shibboleth at Edge Construct
 * and create a Lambda Function URL separately to "plug in" as its primary origin.
 */
export class AuthenticatedStack extends AbstractStack {

  constructor(private stackParams: StackParams) {
    super(stackParams);
  }

  public get modifiedContextOrigin (): OriginFunctionUrl {
    let { stackParams: { context: { ORIGIN } = {} }, _functionUrlOrigin: { url } } = this;
    let origin: OriginFunctionUrl;

    if(ORIGIN) {
      origin = {
        ...(ORIGIN as OriginFunctionUrl),
        url
      }
    }
    else {
      origin = {
        originType: OriginType.FUNCTION_URL,
        httpsPort: 443,
        appAuthorization: true,
        url, 
      }
    }

    if(origin.originType !== OriginType.FUNCTION_URL) {
      throw new Error(`Unsupported origin type for cloudfront: ${origin.originType}`);
    }
    return origin;
  }
}