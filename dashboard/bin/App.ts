#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { OriginType, ShibbolethAtEdgeConstruct } from 'shibboleth-sp-at-edge';
import { AuthenticationMode, IContext } from '../context/IContext';
import { AuthenticatedStack, PublicStack } from '../lib/Stack';
import { getStackName } from '../src/Utils';
import * as dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

const app = new cdk.App();
const context = require('../context/context.json') as IContext;
const { AUTHENTICATION_MODE, REGION: region, STACK_ID, ACCOUNT: account, TAGS: {
  Landscape, CostCenter='', Ticket='', Service, Function
} } = context;
const { AUTHENTICATED, PUBLIC } = AuthenticationMode;

const stackName = getStackName(context);

(async () => {

  const stackProps = {
    stackName,
    description: 'Lambda-based integration dashboard and engine for exporting BU person data to Huron via API',
    env: { account, region },
    tags: { Landscape, CostCenter, Ticket, Service, Function },
  } satisfies cdk.StackProps;

  if(AUTHENTICATION_MODE === AUTHENTICATED) {
    const dashboardApp = new AuthenticatedStack({ scope: app, stackName, context, stackProps });

    context.ORIGIN = dashboardApp.modifiedContextOrigin;

    const shibSpConstruct = await ShibbolethAtEdgeConstruct.getInstance({
      scope: dashboardApp, 
      id: 'shibboleth-sp-at-edge', 
      context,
      httpOriginBase: {
        originType: OriginType.FUNCTION_URL,
        httpOrigin: dashboardApp.functionOrigin
      }
    });

  } 
  else if(AUTHENTICATION_MODE === PUBLIC) {
    // Public Stack - no Shibboleth authentication
    if( ! context.ORIGIN) {
      context.ORIGIN = {
        originType: OriginType.FUNCTION_URL,
        httpsPort: 443,
        appAuthorization: false, // Does not apply, but required field
      };
    }
    const dashboardApp = new PublicStack({ 
      scope: app, stackName: `${stackName}-public`, context, stackProps 
    });
  }

})();

