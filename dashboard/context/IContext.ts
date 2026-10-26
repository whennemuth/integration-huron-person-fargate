import { Config } from 'integration-huron-person';
import { IContext as ShibSpContext } from 'shibboleth-sp-at-edge';

/**
 * Core infrastructure properties that apply to all deployments and derive from ShibSpContext
 */
type CoreInfrastructureProperties = Pick<
  ShibSpContext, 
  'STACK_ID' | 
  'ACCOUNT' |
  'REGION' | 
  'TAGS' | 
  'ORIGIN' |
  'DNS'
> & {
  LAMBDA_TIMEOUT_SECONDS?: number;
  LAMBDA_MEMORY_SIZE_MB?: number;
  INTEGRATION_CONFIG: Config;
};

export enum AuthenticationMode {
  AUTHENTICATED = 'authenticated',
  PUBLIC = 'public',
}

/**
 * Context interface that supports both authenticated and non-authenticated deployments
 * using a discriminated union approach.
 */
export type IContext = 
  | AuthenticatedContext
  | PublicContext;

/**
 * Authenticated deployment context - includes all ShibSpContext properties
 */
export type AuthenticatedContext = CoreInfrastructureProperties & ShibSpContext & {
  AUTHENTICATION_MODE: AuthenticationMode.AUTHENTICATED;
};

/**
 * Public deployment context - includes only core infrastructure properties
 */
export type PublicContext = CoreInfrastructureProperties & {
  AUTHENTICATION_MODE: AuthenticationMode.PUBLIC;
};