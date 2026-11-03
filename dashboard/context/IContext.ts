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
  /** The landscape's name in the landscape picker (default: the landscape) */
  LABEL?: string;
  /**
   * Whether this is the default landscape (the one the bare hostname redirects to). At most one
   * landscape may be: synth fails if another landscape's registry parameter already claims it.
   */
  isDefault?: boolean;
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
  /**
   * Whether this landscape shares its sign-in with the other landscapes that set it (default:
   * false). When true, synth sets SHIBBOLETH.cookieDomain to the dashboard domain and
   * SHIBBOLETH.cookieName to SHARED_SIGN_IN_COOKIE_NAME (see dashboard/lib/SharedSignIn.ts), so one
   * sign-in serves every such landscape until the token expires. When false, the landscape signs in
   * on its own with a host-only cookie under the default name.
   */
  sharedSignIn?: boolean;
};

/**
 * Public deployment context - includes only core infrastructure properties
 */
export type PublicContext = CoreInfrastructureProperties & {
  AUTHENTICATION_MODE: AuthenticationMode.PUBLIC;
};

type DistributiveOmit<T, K extends PropertyKey> = T extends any ? Omit<T, K> : never;

/**
 * The DASHBOARD section of a fargate landscape's context: the dashboard's context minus the
 * account, region and tags it shares with the landscape.
 */
export type DashboardSettings = DistributiveOmit<IContext, 'ACCOUNT' | 'REGION' | 'TAGS'>;

/**
 * Builds the dashboard's context from a fargate landscape context. The landscape's DASHBOARD section
 * is typed loosely in context/IContext.ts, because the task images compile that file and must not
 * need Shibboleth types, so callers cast it to DashboardSettings here, at CDK synth time.
 * Nothing checks that cast: a missing or misspelled setting fails at synth (or, for Shibboleth
 * settings, at sign-in), not at compile time.
 */
export const toDashboardContext = (
  landscape: Pick<IContext, 'ACCOUNT' | 'REGION' | 'TAGS'>, settings: DashboardSettings
): IContext => {
  const { ACCOUNT, REGION, TAGS } = landscape;
  return { ...settings, ACCOUNT, REGION, TAGS } as IContext;
}