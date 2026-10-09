import { HeadObjectCommand } from "@aws-sdk/client-s3";
import { IContext } from "../context/IContext";
import { Config, ConfigManager, log, warn, error} from "integration-huron-person";
import { S3StreamProvider } from "integration-core";

/**
 * @returns The name of the stack
 */
export const getStackName = (context:IContext):string => {
  const { STACK_ID, TAGS: { Landscape } } = context;
  return `${STACK_ID}-${Landscape}`;
}

/**
 * A landscape named "mock", "mock1", "mock2", etc. is dedicated to mocked runs (source simulator
 * and/or mock target). Its stack holds no real data, so mock runs use its standard tables as-is.
 * This name is the single signal of mock mode - for CDK (deploy time) and for the ECS tasks/Runner
 * (runtime, via the LANDSCAPE environment variable).
 */
export const MOCK_LANDSCAPE_PATTERN = /^mock\d*$/;

export const isMockLandscape = (landscape?: string): boolean => {
  return MOCK_LANDSCAPE_PATTERN.test(landscape ?? '');
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

/**
 * Get configuration from environment variables, Secrets Manager, or local file system
 * (for local dev). Priority:
 *   HURON_PERSON_CONFIG_JSON (TaskDef secret injection) >
 *   SECRET_ARN (Secrets Manager) >
 *   Environment >
 *   FileSystem (local dev)
 */
export const getConfig = async (): Promise<Config> => {
  const {
    /** SECRET_ARN: Secrets Manager ARN containing config */
    SECRET_ARN,
    /** HURON_PERSON_CONFIG_PATH: Path to config.json (fallback for local dev only) */
    HURON_PERSON_CONFIG_PATH
  } = process.env;

  const configManager = ConfigManager.getInstance();
  const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
  return await configManager
    .reset()
    .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
    .fromSecretManager(SECRET_ARN)                // ← Fallback to Secrets Manager
    .fromEnvironment()                            // ← Fallback to individual env var overrides
    .fromFileSystem(localConfigPath)              // ← Local dev only
    .getConfigAsync('people');
}

export const objectExistsInS3 = async (Bucket: string, Key: string, region?: string): Promise<boolean> => {
  return await (new S3StreamProvider({ bucketName: Bucket, region })).resourceExists(Key);
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