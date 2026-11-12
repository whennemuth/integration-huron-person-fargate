import { LandscapeListing } from '../services/ServiceTypes';
import { LandscapeTone, LandscapeToneName } from './LandscapeTone';

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
  /** This landscape's name, shown in place of the navigator when there is none */
  landscapeBadge?: { label: string };
  /** How the navigator or badge is tinted. Absent for an unmarked landscape */
  landscapeTone?: { name: LandscapeToneName; description: string };
  /** Absent in public mode, where there is no sign-in to end */
  signOut?: { url: string; user?: string };
};

/**
 * Build the navigation bar: a landscape navigator over the registry (this landscape selected), or a
 * badge naming this landscape when there's no navigator, either one tinted by the landscape's tone;
 * and a sign-out control when the dashboard requires sign-in.
 */
export const buildNavigationBar = (params: {
  listing?: LandscapeListing;
  /** This landscape, for when the registry could not be read (the listing names it otherwise) */
  landscape?: string;
  /** The LANDSCAPE_TONE setting (see LandscapeTone) */
  landscapeTone?: string;
  signInRequired: boolean;
  /** The JWT's user attributes (Authenticator.user) */
  user?: any;
}): NavigationBar => {
  const { listing, signInRequired, user } = params;
  const navigationBar: NavigationBar = {};
  const landscape = listing?.current ?? params.landscape;

  if (listing && listing.landscapes.length > 0) {
    const options: NavigatorOption[] = listing.landscapes.map(({ label, url, current }) => ({
      label, url, selected: current, disabled: !url && !current
    }));
    if (!listing.landscapes.some(({ current }) => current)) {
      // This landscape isn't registered (e.g. run locally): show it, so another can be chosen
      options.unshift({ label: listing.current, selected: true, disabled: true });
    }
    navigationBar.landscapeNavigator = { options };
  } else if (landscape) {
    navigationBar.landscapeBadge = { label: landscape };
  }

  const tone = LandscapeTone.resolve({ setting: params.landscapeTone, landscape });
  if (tone) {
    navigationBar.landscapeTone = { name: tone.name, description: tone.description };
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
