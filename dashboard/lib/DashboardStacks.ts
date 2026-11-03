import * as cdk from 'aws-cdk-lib/core';
import { OriginType, ShibbolethAtEdgeConstruct } from 'shibboleth-sp-at-edge';
import { IContext as LandscapeContext } from '../../context/IContext';
import { AuthenticatedContext, AuthenticationMode, DashboardSettings, IContext, PublicContext, toDashboardContext } from '../context/IContext';
import { assertLandscapeHostname } from './DashboardDomain';
import { DefaultLandscapeRedirect, planDefaultLandscapeRedirect } from './DefaultLandscapeRedirect';
import { landscapeResources } from './LandscapeResources';
import { assertSingleDefaultLandscape } from './LandscapeRegistration';
import { applySharedSignIn } from './SharedSignIn';
import { AuthenticatedStack, PublicStack } from './Stack';

export const getDashboardStackName = (context: LandscapeContext): string | undefined => {
  const { DASHBOARD, TAGS: { Landscape } } = context;
  return DASHBOARD ? `${DASHBOARD.STACK_ID}-${Landscape}` : undefined;
}

export const echoDashboardStackName = () => {
  const context = require('../../context/context.json') as LandscapeContext;
  console.log(getDashboardStackName(context) ?? '');
}

/**
 * Add the landscape's dashboard stack (and, when authenticated, the Shibboleth construct with its
 * us-east-1 edge function stack) to the app. Does nothing if the context has no DASHBOARD section.
 * The default landscape's stack also redirects the dashboard domain to it.
 */
export const createDashboardStack = async (app: cdk.App, landscapeContext: LandscapeContext): Promise<cdk.Stack | undefined> => {
  const stackName = getDashboardStackName(landscapeContext);
  if (!stackName) {
    return undefined;
  }
  const context: IContext = toDashboardContext(landscapeContext, landscapeContext.DASHBOARD as unknown as DashboardSettings);
  const { ACCOUNT: account, REGION: region, TAGS: { Landscape, CostCenter = '', Ticket = '', Service, Function } } = context;

  const stackProps = {
    stackName,
    description: `Dashboard for the ${Landscape} landscape of the BU person to Huron integration`,
    env: { account, region },
    tags: { Landscape, CostCenter, Ticket, Service, Function },
  } satisfies cdk.StackProps;
  const resources = landscapeResources(landscapeContext);
  assertLandscapeHostname({ landscape: resources.landscape, subdomain: context.ORIGIN?.subdomain });
  await assertSingleDefaultLandscape({ landscape: resources.landscape, isDefault: context.isDefault, region });
  const redirectPlan = context.isDefault ? await planDefaultLandscapeRedirect({ context, stackName }) : undefined;
  const addRedirect = (stack: cdk.Stack): cdk.Stack => {
    if (redirectPlan) {
      new DefaultLandscapeRedirect(stack, 'default-landscape-redirect', { plan: redirectPlan, stackName });
    }
    return stack;
  }

  if (context.AUTHENTICATION_MODE === AuthenticationMode.AUTHENTICATED) {
    // The Lambda (AUTH_COOKIE_NAME) and the edge functions must read the same cookie
    const authContext = applySharedSignIn(context);
    const dashboardStack = new AuthenticatedStack({
      scope: app, stackName, context: authContext, landscapeResources: resources, stackProps
    });

    // The Shibboleth construct fronts the function URL, so it needs the URL in its context
    const shibContext: AuthenticatedContext = { ...authContext, ORIGIN: dashboardStack.modifiedContextOrigin };
    await ShibbolethAtEdgeConstruct.getInstance({
      scope: dashboardStack,
      id: 'shibboleth-sp-at-edge',
      context: shibContext,
      httpOriginBase: {
        originType: OriginType.FUNCTION_URL,
        httpOrigin: dashboardStack.functionOrigin
      }
    });
    return addRedirect(dashboardStack);
  }

  if ('sharedSignIn' in context && context.sharedSignIn) {
    throw new Error(`DASHBOARD.sharedSignIn needs AUTHENTICATION_MODE "${AuthenticationMode.AUTHENTICATED}": a public dashboard has no sign-in`);
  }
  const publicContext: PublicContext = {
    ...(context as PublicContext),
    ORIGIN: context.ORIGIN ?? { originType: OriginType.FUNCTION_URL, httpsPort: 443, appAuthorization: false }
  };
  return addRedirect(new PublicStack({ scope: app, stackName, context: publicContext, landscapeResources: resources, stackProps }));
}
