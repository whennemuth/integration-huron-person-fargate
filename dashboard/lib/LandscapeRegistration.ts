import { Stack } from "aws-cdk-lib";
import { Effect, PolicyStatement } from "aws-cdk-lib/aws-iam";
import { Function as LambdaFunction } from "aws-cdk-lib/aws-lambda";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { IContext } from "../context/IContext";
import { LandscapeEntry, LANDSCAPES_PARAMETER_PATH, readLandscapeEntries } from "../src/services/LandscapeRegistry";

/**
 * Register the landscape in the landscape registry: an SSM parameter `<path>/<landscape>` this
 * stack owns, so deploying registers the landscape and tearing the stack down deregisters it.
 * The value is derived from the context: url from ORIGIN.subdomain, label from LABEL, isDefault
 * from isDefault. Also lets the dashboard Lambda read the registry, and tells it where it is.
 */
export const registerLandscape = (params: {
  stack: Construct, context: IContext, landscape: string, lambda: LambdaFunction
}): StringParameter => {
  const { stack, landscape, lambda, context: { ORIGIN, DNS, LABEL, isDefault = false } } = params;
  const entry: LandscapeEntry = {
    landscape,
    label: LABEL ?? landscape,
    ...(ORIGIN?.subdomain && { url: `https://${ORIGIN.subdomain}` }),
    isDefault
  };

  const parameter = new StringParameter(stack, 'landscape-registration', {
    parameterName: `${LANDSCAPES_PARAMETER_PATH}/${landscape}`,
    stringValue: JSON.stringify(entry),
    description: registryDescription(Stack.of(stack).stackName),
  });

  // GetParametersByPath is authorized against the path itself
  const parameterArn = (name: string) => Stack.of(stack).formatArn({
    service: 'ssm', resource: 'parameter', resourceName: name.replace(/^\//, '')
  });
  lambda.addToRolePolicy(new PolicyStatement({
    effect: Effect.ALLOW,
    actions: ['ssm:GetParametersByPath'],
    resources: [parameterArn(LANDSCAPES_PARAMETER_PATH), parameterArn(`${LANDSCAPES_PARAMETER_PATH}/*`)],
  }));
  lambda.addEnvironment('LANDSCAPES_PARAMETER_PATH', LANDSCAPES_PARAMETER_PATH);
  if (DNS?.hostedZone) {
    lambda.addEnvironment('HOSTED_ZONE', DNS.hostedZone);
  }
  return parameter;
}

/** SSM parameter descriptions are limited to 1024 characters */
export const registryDescription = (stackName: string): string => [
  `Landscape picker registry entry, owned by the ${stackName} stack: CloudFormation creates it on deploy and deletes it on teardown.`,
  `Every landscape dashboard in this account and region reads all parameters under ${LANDSCAPES_PARAMETER_PATH} on a cold start and serves them at GET /api/landscapes.`,
  `Value: JSON {landscape, label?, url?, isDefault?}; label defaults to the landscape, url to https://<landscape>.<hosted zone>.`,
  `It is set from the landscape's DASHBOARD context (ORIGIN.subdomain, LABEL, isDefault), so change that and redeploy rather than editing this.`,
  `At most one landscape may be the default: synth fails while another parameter here has isDefault true.`,
  `To list a landscape deployed in another account, create a parameter by hand under ${LANDSCAPES_PARAMETER_PATH} with an explicit url; remove it by hand too.`,
].join(' ');

/**
 * Fail synth if this landscape claims the default while another landscape's registry parameter
 * already does. Reads SSM only when this landscape claims it.
 */
export const assertSingleDefaultLandscape = async (params: {
  landscape: string, isDefault?: boolean, region?: string, readEntries?: () => Promise<LandscapeEntry[]>
}): Promise<void> => {
  const { landscape, isDefault, region, readEntries = () => readLandscapeEntries({ region }) } = params;
  if (!isDefault) {
    return;
  }
  const other = (await readEntries()).find(entry => entry.isDefault && entry.landscape !== landscape);
  if (other) {
    throw new Error(
      `DASHBOARD.isDefault is true for the ${landscape} landscape, but ${other.landscape} is already the default ` +
      `(${LANDSCAPES_PARAMETER_PATH}/${other.landscape}). Set isDefault to false in the ${other.landscape} landscape's ` +
      `context and deploy it first (or, if no stack owns that parameter, edit it by hand), or remove isDefault here.`
    );
  }
}
