/**
 * The name of a landscape's dashboard Lambda function. CDK names the function with it (FunctionUrl.ts), and
 * local tooling finds the deployed function by it (scripts/LambdaFunction.ts), so the two can't drift apart.
 * Kept in a module of its own, free of CDK imports, so local tooling loads quickly.
 *
 * @param stackId The dashboard's DASHBOARD.STACK_ID, shared by every landscape (e.g. hp-dashboard)
 * @param landscape The landscape (TAGS.Landscape)
 */
export const dashboardFunctionName = (stackId: string, landscape: string): string =>
  `${stackId}-${landscape}-app-function`;
