import { IContext } from "../context/IContext";

const isBlank = (s:string|null|undefined):boolean => {
  return s === undefined || s === null || `${s}`.trim() == '';
}
const isNotBlank = (s:string|null|undefined) => !isBlank(s);
const anyBlank = (...a:any) => a.findIndex((s:any) =>  isBlank(s)) > -1;
const anyNotBlank = (...a:any) => a.findIndex((s:any) => isNotBlank(s)) > -1;
const allBlank = (...a:any) => !anyNotBlank(...a);
const noneBlank = (...a:any) => !anyBlank(...a);
const someBlankSomeNot = (...a:any) => anyBlank(...a) && anyNotBlank(...a);

export const ParameterTester = {
  isBlank, isNotBlank, anyBlank, anyNotBlank, allBlank, noneBlank, someBlankSomeNot
}

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

export const isTruthy = (value?:any):boolean => {
  const val = `${value}`.trim().toLowerCase();
  return val === 'true' || val === 'yes' || val === 'y' || val === '1';
}

export const isRunningInLambda = (): boolean => {
   return !!process.env.AWS_LAMBDA_FUNCTION_NAME;
}

export const equalsIgnoreCase = (a:string, b:string):boolean => {
  return `${a}`.toLowerCase() === `${b}`.toLowerCase();
}

export const areTheSame = (a:string, b:string):boolean => {
  return equalsIgnoreCase(`${a}`.trim(), `${b}`.trim());
}

export const isABuid = (id:string) => /^U[0-9]{8,9}$/.test(id);
