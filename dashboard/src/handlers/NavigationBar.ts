import { LandscapeListing } from '../services/ServiceTypes';

/**
 * The edge functions (shibboleth-sp's handler) answer this path themselves, never the Lambda: they
 * expire the JWT cookie (with the cookie's Domain, so a shared sign-in ends in every landscape),
 * then send the browser to SHIBBOLETH.logoutUrl to end the IdP session.
 */
export const SIGN_OUT_PATH = '/logout';

/** One entry of the landscape navigator. An entry without a url is listed but can't be chosen */
export type NavigatorOption = {
  label: string;
  url?: string;
  selected: boolean;
  disabled: boolean;
};

/** The dashboard template's navigation bar data */
export type NavigationBar = {
  /** Absent when the registry lists no landscape, or could not be read */
  landscapeNavigator?: { options: NavigatorOption[] };
  /** Absent in public mode, where there is no sign-in to end */
  signOut?: { url: string; user?: string };
};

/**
 * Build the navigation bar: a landscape navigator over the registry (this landscape selected), and
 * a sign-out control when the dashboard requires sign-in.
 */
export const buildNavigationBar = (params: {
  listing?: LandscapeListing;
  signInRequired: boolean;
  /** The JWT's user attributes (Authenticator.user) */
  user?: any;
}): NavigationBar => {
  const { listing, signInRequired, user } = params;
  const navigationBar: NavigationBar = {};

  if (listing && listing.landscapes.length > 0) {
    const options: NavigatorOption[] = listing.landscapes.map(({ label, url, current }) => ({
      label, url, selected: current, disabled: !url && !current
    }));
    if (!listing.landscapes.some(({ current }) => current)) {
      // This landscape isn't registered (e.g. run locally): show it, so another can be chosen
      options.unshift({ label: listing.current, selected: true, disabled: true });
    }
    navigationBar.landscapeNavigator = { options };
  }

  if (signInRequired) {
    navigationBar.signOut = { url: SIGN_OUT_PATH, user: userName(user) };
  }
  return navigationBar;
}

/** The signed-in user's principal name, from the JWT's SAML attributes */
const userName = (user: any): string | undefined => {
  if (!user || typeof user !== 'object') return undefined;
  for (const attribute of ['eduPersonPrincipalName', 'buPrincipal']) {
    const value = [user[attribute]].flat()[0];
    if (value) return `${value}`;
  }
  return undefined;
}
