import * as cdk from 'aws-cdk-lib/core';
import { OriginType, ShibbolethAtEdgeConstruct } from 'shibboleth-sp-at-edge';
import { IContext as LandscapeContext } from '../../context/IContext';
import { AuthenticatedContext, AuthenticationMode, DashboardSettings, IContext, PublicContext, toDashboardContext } from '../context/IContext';
import { landscapeResources } from './LandscapeResources';
import { assertSingleDefaultLandscape } from './LandscapeRegistration';
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
  await assertSingleDefaultLandscape({ landscape: resources.landscape, isDefault: context.isDefault, region });

  if (context.AUTHENTICATION_MODE === AuthenticationMode.AUTHENTICATED) {
    const dashboardStack = new AuthenticatedStack({
      scope: app, stackName, context, landscapeResources: resources, stackProps
    });

    // The Shibboleth construct fronts the function URL, so it needs the URL in its context
    const shibContext: AuthenticatedContext = { ...context, ORIGIN: dashboardStack.modifiedContextOrigin };
    await ShibbolethAtEdgeConstruct.getInstance({
      scope: dashboardStack,
      id: 'shibboleth-sp-at-edge',
      context: shibContext,
      httpOriginBase: {
        originType: OriginType.FUNCTION_URL,
        httpOrigin: dashboardStack.functionOrigin
      }
    });
    return dashboardStack;
  }

  const publicContext: PublicContext = {
    ...(context as PublicContext),
    ORIGIN: context.ORIGIN ?? { originType: OriginType.FUNCTION_URL, httpsPort: 443, appAuthorization: false }
  };
  return new PublicStack({ scope: app, stackName, context: publicContext, landscapeResources: resources, stackProps });
}
