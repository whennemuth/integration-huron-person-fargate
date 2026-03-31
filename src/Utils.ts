import { IContext } from "../context/IContext";

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


if(require.main === module) {
  console.log(pathUpTo({ fullPath: '/a/b/c/d', segment: 'c' }));
  console.log(pathUpTo({ fullPath: '/a/b/c/d', segment: 'e' }));
  console.log(pathUpTo({ fullPath: '/a/b/c/d', segment: 'a' }));
  console.log(pathUpTo({ fullPath: 'a/b/c/d', segment: 'a' }));
  console.log(pathUpTo({ fullPath: 'a/b/c/d', segment: 'd' }));
  console.log(pathUpTo({ fullPath: 'a/b/c/d/', segment: 'd' }));
}