#!/usr/bin/env node
import { App } from 'aws-cdk-lib/core';
import * as dotenv from 'dotenv';
import { IContext } from '../context/IContext';
import { IntegrationHuronPersonLambdaStack } from '../lib/Stack';
import { BU_NameTagAspect, TaggingAspect } from '../lib/Tagging';
import { getStackName } from '../src/Utils';

// Load environment variables from .env file
dotenv.config();

const context = require('../context/context.json') as IContext;

const app = new App();
const stackName = getStackName(context);

const stack = new IntegrationHuronPersonLambdaStack(app, stackName, {
  context,
  env: {
    account: context.ACCOUNT,
    region: context.REGION,
  },
  description: 'Fargate-based service for processing large Huron person JSON files with two-phase parallel processing',
});

// Apply standard tags to all resources in each stack
// SEE: https://github.com/bu-ist/buaws-istcloud-information/blob/main/aws-tagging-standard.md#costcenter
// NOTE: The CostCenter value is "AWS Word Press Migration to AWS", not "AWS WordPress Migration to AWS"
const { Service, Function, Landscape, CostCenter='', Ticket='' } = context.TAGS; // Destructure to get individual tag values for easier use
const standardTags = { 
  Service, 
  Function, 
  Landscape, 
  CostCenter, 
  Ticket 
};
new TaggingAspect(stack, standardTags).applyTags({ 
  aspect: new BU_NameTagAspect(standardTags) 
});
