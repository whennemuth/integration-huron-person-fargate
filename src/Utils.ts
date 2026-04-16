import { IContext } from "../context/IContext";
import { log, warn, error} from "integration-huron-person";

/**
 * @returns The name of the stack
 */
export const getStackName = (context:IContext):string => {
  const { STACK_ID, TAGS: { Landscape } } = context;
  return `${STACK_ID}-${Landscape}`;
}

export const echoStackName = () => {
  const contextModule = require('../context/context.json') as IContext;
  const stackName = getStackName(contextModule);
  console.log(stackName);
}

export const isRunningInLambda = (): boolean => {
   return !!process.env.AWS_LAMBDA_FUNCTION_NAME;
}

/**
 * Returns the path up to and including the first appearance of a specified segment.
 * @param params An object containing the full path, the segment to search for, and an optional separator.
 * @returns The path up to and including the first appearance of the specified segment.
 */
export const pathUpTo = (params: { fullPath: string, segment: string, separator?: string }): string => {
  const { fullPath, segment, separator = '/'   } = params;
  const pathParts = fullPath.split(separator);
  let foundSegment = false;
  let newPath = '';
  for (const part of pathParts) {
    if (foundSegment) {
      break;
    }
    if (part === segment) {
      foundSegment = true;
    }
    newPath = newPath ? `${newPath}${separator}${part}` : part;
  }
  return newPath.endsWith(separator) ? newPath.substring(0, newPath.length - separator.length) : newPath;
}


/**
 * (Local mode - config may be in file system) Load configuration from the integration-huron-person
 * working directory when running locally with the provided launch configuration in the 
 * integration-huron-person-fargate/.vscode/launch.json file.
 * 
 * NOTE: This function expects to find a config.json file up one directory from the current working 
 * directory, in a "integration-huron-person" folder. This is assumes a you have created a 
 * integration.code-workspace and have arranged your directories accordingly. Adjust the path as 
 * necessary if your local setup differs.
 * @returns The path to the local configuration file, or undefined if not found.
 */
export const getLocalConfig = (params?: { projectFolder?: string, configFileName?: string }): string | undefined => {
  const { projectFolder='integration-huron-person', configFileName='config.json' } = params || {};
  const args = process?.argv || [];
  try {
    const workspaceFolderArg = args.find(arg => arg.startsWith('workspaceFolder='));
    const workspaceFolder = workspaceFolderArg ? workspaceFolderArg.split('=')[1] : undefined;
    if (!workspaceFolder) {
      return undefined;
    }
    return require('path').resolve(workspaceFolder, `../${projectFolder}/${configFileName}`);
  }
  catch (error) {
    console.error('Error determining local config path:', error);
    return undefined;
  }
}

export const logAxiosResponse = (params: { 
  response: any, 
  msg?: string, 
  flat?: boolean, 
  logAs: 'log' | 'warn' | 'error', 
  level?: 'terse' | 'normal' | 'verbose'
}) => {
  const { response, msg, flat=false, logAs='log', level='terse' } = params;
  const { status, statusText, config: { headers={} }, data={} } = response || {};
  let loggable = {};
  switch(level) {
    case 'terse':
      loggable = { status, statusText, data };
      break;
    case 'normal':
      const { baseURL, params={}, responseType, method, url } = headers;
      loggable = {
        headers: { baseURL, params, responseType, method, url }, status, statusText, data 
      };
      break;
    case 'verbose':
      loggable = response;
      break;
  }
  
  switch(logAs) {
    case 'log':
      log({ o: loggable, msg, flat });
      break;
    case 'warn':
      warn({ o: loggable, msg, flat });
      break;
    case 'error':
      error({ o: loggable, msg, flat });
      break;
  }
}

export const logAxiosError = (params: { 
  error: any, 
  msg?: string, 
  flat?: boolean, 
  logAs: 'log' | 'warn' | 'error', 
  level?: 'terse' | 'normal' | 'verbose'
}) => {
  const { error={}, msg, flat=false, logAs='error', level='terse' } = params;
  const { response } = error || {};
  if (response) {
    logAxiosResponse({ response, msg, flat, logAs, level });
  } else {
    switch(logAs) {
      case 'log':  
        log({ o: error, msg, flat });
        break;
      case 'warn':
        warn({ o: error, msg, flat });
        break;
      case 'error':
        error({ o: error, msg, flat });
        break;
    }
  }
}

export const logShortAxiosError = (error: any, msg: string) => {
  logAxiosError({ error, msg, flat: false, logAs: 'error', level: 'terse' });
}

if(require.main === module) {
  console.log(pathUpTo({ fullPath: '/a/b/c/d', segment: 'c' }));
  console.log(pathUpTo({ fullPath: '/a/b/c/d', segment: 'e' }));
  console.log(pathUpTo({ fullPath: '/a/b/c/d', segment: 'a' }));
  console.log(pathUpTo({ fullPath: 'a/b/c/d', segment: 'a' }));
  console.log(pathUpTo({ fullPath: 'a/b/c/d', segment: 'd' }));
  console.log(pathUpTo({ fullPath: 'a/b/c/d/', segment: 'd' }));
}