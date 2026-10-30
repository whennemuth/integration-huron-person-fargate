/**
 * The dashboard domain is the parent of every landscape's hostname: a landscape's dashboard is at
 * `<landscape>.<dashboard domain>` (ORIGIN.subdomain), and the dashboard domain itself redirects
 * to the default landscape. It has no context value of its own: it is ORIGIN.subdomain minus its
 * first label, which assertLandscapeHostname() keeps equal to the landscape name.
 */
export const dashboardDomain = (subdomain: string): string => subdomain.split('.').slice(1).join('.');

/**
 * Fail synth unless ORIGIN.subdomain is `<landscape>.<dashboard domain>`. Does nothing when the
 * landscape has no custom hostname.
 */
export const assertLandscapeHostname = (params: { landscape: string, subdomain?: string }): void => {
  const { landscape, subdomain } = params;
  if (!subdomain) {
    return;
  }
  const [firstLabel] = subdomain.split('.');
  if (firstLabel !== landscape || !dashboardDomain(subdomain).includes('.')) {
    throw new Error(
      `DASHBOARD.ORIGIN.subdomain ${subdomain} must be <landscape>.<dashboard domain>, ` +
      `e.g. ${landscape}.person.huron.cssnprd.warhen.work, for the ${landscape} landscape`
    );
  }
}
