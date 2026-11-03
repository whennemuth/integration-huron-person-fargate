import { AuthenticatedContext } from '../context/IContext';
import { dashboardDomain } from './DashboardDomain';

/**
 * The JWT cookie name every sharedSignIn landscape uses. It must differ from the default
 * (auth-token-cookie) that a landscape without sharedSignIn keeps: the browser also sends the
 * shared cookie to that landscape, since it is under the dashboard domain, and with one name the
 * landscape couldn't tell its own cookie from the shared one.
 */
export const SHARED_SIGN_IN_COOKIE_NAME = 'hp-dashboard-auth';

/**
 * Applies DASHBOARD.sharedSignIn: when true, returns the context with SHIBBOLETH.cookieDomain set to
 * the dashboard domain and SHIBBOLETH.cookieName to SHARED_SIGN_IN_COOKIE_NAME, so the landscape's
 * sign-in serves every other sharedSignIn landscape (shibboleth-sp-at-edge passes both to the edge
 * functions). Otherwise returns the context unchanged.
 * Fails synth when sharedSignIn is true without ORIGIN.subdomain (there is no dashboard domain to
 * share), or when the context also sets cookieName or cookieDomain itself.
 */
export const applySharedSignIn = (context: AuthenticatedContext): AuthenticatedContext => {
  if (!context.sharedSignIn) {
    return context;
  }
  const { ORIGIN: { subdomain } = {}, SHIBBOLETH } = context;
  if (!subdomain) {
    throw new Error(
      'DASHBOARD.sharedSignIn needs DASHBOARD.ORIGIN.subdomain (<landscape>.<dashboard domain>): ' +
      'the shared cookie is scoped to the dashboard domain'
    );
  }
  if (SHIBBOLETH.cookieName || SHIBBOLETH.cookieDomain) {
    throw new Error(
      'DASHBOARD.sharedSignIn sets DASHBOARD.SHIBBOLETH.cookieName and cookieDomain: ' +
      'remove them from the context, or remove sharedSignIn'
    );
  }
  return {
    ...context,
    SHIBBOLETH: { ...SHIBBOLETH, cookieDomain: dashboardDomain(subdomain), cookieName: SHARED_SIGN_IN_COOKIE_NAME }
  };
}
