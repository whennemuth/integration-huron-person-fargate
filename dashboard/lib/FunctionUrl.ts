import { Duration, Fn, RemovalPolicy } from "aws-cdk-lib";
import { OriginProtocolPolicy } from "aws-cdk-lib/aws-cloudfront";
import { HttpOrigin, HttpOriginProps } from "aws-cdk-lib/aws-cloudfront-origins";
import { FunctionUrl, FunctionUrlAuthType, Runtime } from "aws-cdk-lib/aws-lambda";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { LogGroup } from "aws-cdk-lib/aws-logs";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { PolicyStatement, Effect } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { AuthenticatedContext, AuthenticationMode, IContext } from "../context/IContext";
import { CACHE_SECRET } from "../src/handlers/DashboardCache";

export class FunctionUrlOrigin {
  private lambda: NodejsFunction;
  private functionUrl: FunctionUrl;
  private funcUrlOrigin: HttpOrigin;
  private originVerifySecret: string;
  
  constructor(private params: { stack: Construct, context: IContext, secretArn?: string }) {
    const { stack, secretArn, context, context: { 
      STACK_ID, 
      TAGS,
      REGION,
      AUTHENTICATION_MODE,
      LAMBDA_TIMEOUT_SECONDS,
      LAMBDA_MEMORY_SIZE_MB,
    } } = this.params;

    // Generate a secure random secret for origin verification
    this.originVerifySecret = `cf-origin-verify-${Math.random().toString(36).substring(2, 15)}${Math.random().toString(36).substring(2, 15)}${Date.now().toString(36)}`;

    // Start with base environment variable(s)
    const {env: { arn, region } } = CACHE_SECRET;
    let environment = {
      ORIGIN_VERIFY_SECRET: this.originVerifySecret,
      [arn]: secretArn,
      [region]: REGION,
    } as any;

    // Add authentication-related environment variables if applicable
    if(AUTHENTICATION_MODE === AuthenticationMode.AUTHENTICATED) {
      const {
        ORIGIN: { appAuthorization=true } = {}, 
        APP_LOGIN_HEADER, 
        APP_LOGOUT_HEADER
      } = context as AuthenticatedContext;
      const APP_AUTHORIZATION = appAuthorization;
      environment = {
        ...environment,
        APP_AUTHORIZATION,
        APP_LOGIN_HEADER, 
        APP_LOGOUT_HEADER
      };
    }

    const functionName = `${STACK_ID}-${TAGS.Landscape}-app-function`;
    
    // Store secret in Systems Manager Parameter Store
    new StringParameter(stack, 'secret', {
      parameterName: `/cloudfront/${STACK_ID}/origin-verify-secret`,
      stringValue: this.originVerifySecret,
      description: 'Secret header value for CloudFront origin verification',
    });

    // Simple lambda-based web app
    this.lambda = new NodejsFunction(stack, 'lambda', {
      runtime: Runtime.NODEJS_LATEST,
      memorySize: LAMBDA_MEMORY_SIZE_MB ?? 512,
      entry: 'src/handlers/DashboardHandler.ts',
      timeout: Duration.seconds(LAMBDA_TIMEOUT_SECONDS ?? 10),
      functionName,
      environment,
      logGroup: new LogGroup(stack, `logs`, {
        logGroupName: `/aws/lambda/${functionName}`,
        removalPolicy: RemovalPolicy.DESTROY,
      }),
      bundling: {
        externalModules: [
          '@aws-sdk/*',
        ]
      },
    });

    // Add policy to allow reading from Secrets Manager secret
    if (secretArn) {
      this.lambda.addToRolePolicy(new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['secretsmanager:GetSecretValue'],
        resources: [secretArn],
      }));
    }

    // Lambda function url for the web app.
    this.functionUrl = new FunctionUrl(this.lambda, 'url', {
      function: this.lambda,
      authType: FunctionUrlAuthType.NONE,
    });

    let httpOriginProps = {
      protocolPolicy: OriginProtocolPolicy.HTTPS_ONLY,
      httpsPort: 443,
      // Add origin verification header for CloudFront
      customHeaders: {
        'X-Origin-Verify': this.originVerifySecret
      }
    } as HttpOriginProps

    // Customize for deployment of an app that requires authentication on access.
    if(AUTHENTICATION_MODE === AuthenticationMode.AUTHENTICATED) {

      // Authentication headers
      const { 
        APP_LOGIN_HEADER, 
        APP_LOGOUT_HEADER, 
        ORIGIN: { appAuthorization=true } = {} 
      } = context as AuthenticatedContext;
      this.lambda.addEnvironment('APP_AUTHORIZATION', `${appAuthorization}`);
      this.lambda.addEnvironment('APP_LOGIN_HEADER', APP_LOGIN_HEADER);
      this.lambda.addEnvironment('APP_LOGOUT_HEADER', APP_LOGOUT_HEADER);

      // Custom header(s) - merge with origin verification
      httpOriginProps = {
        ...httpOriginProps,
        customHeaders: {
          ...httpOriginProps.customHeaders,
          APP_AUTHORIZATION: `${appAuthorization}`
        }
      }
    }

    /**
     * This split function should take a function url like this:
     *    https://dg4qdeehraanv7q33ljsrfztae0fwyvz.lambda-url.us-east-2.on.aws/
     * and extract its domain like this:
     *    dg4qdeehraanv7q33ljsrfztae0fwyvz.lambda-url.us-east-2.on.aws
     * 'https://' is removed (Note: trailing '/' is also removed)
     */
    this.funcUrlOrigin = new HttpOrigin(Fn.select(2, Fn.split('/', this.functionUrl.url)), httpOriginProps);    
  }

  public get origin(): HttpOrigin {
    return this.funcUrlOrigin;
  }

  public get url(): string {
    return this.functionUrl.url;
  }

  public get lambdaFunction(): NodejsFunction {
    return this.lambda;
  }
}
