/**
 * Tests the dashboard's navigation bar (dashboard/src/handlers/NavigationBar.ts): the landscape
 * navigator built from the landscape registry, and the sign-out control, both as data and as
 * rendered by the dashboard route (dashboard/src/handlers/DashboardHandler.ts).
 */
import { buildNavigationBar, SIGN_OUT_PATH } from '../dashboard/src/handlers/NavigationBar';
import { LambdaFunctionUrlEvent } from '../dashboard/src/handlers/DashboardTypes';
import { LandscapeListing } from '../dashboard/src/services/ServiceTypes';

const listing: LandscapeListing = {
  current: 'preview',
  landscapes: [
    { landscape: 'mock', label: 'Mock', url: 'https://mock.example.com', isDefault: false, current: false },
    { landscape: 'preview', label: 'Preview', url: 'https://preview.example.com', isDefault: false, current: true },
    { landscape: 'remote', label: 'Remote', isDefault: false, current: false }
  ]
};

describe('buildNavigationBar', () => {
  it('lists every landscape, selects this one, and disables one without a url', () => {
    expect(buildNavigationBar({ listing, signInRequired: false }).landscapeNavigator).toEqual({
      options: [
        { label: 'Mock', url: 'https://mock.example.com', selected: false, disabled: false },
        { label: 'Preview', url: 'https://preview.example.com', selected: true, disabled: false },
        { label: 'Remote', url: undefined, selected: false, disabled: true }
      ]
    });
  });

  it('adds this landscape, selected, when the registry does not list it', () => {
    const { landscapeNavigator } = buildNavigationBar({
      listing: { current: 'local', landscapes: [listing.landscapes[0]] }, signInRequired: false
    });
    expect(landscapeNavigator?.options).toEqual([
      { label: 'local', selected: true, disabled: true },
      { label: 'Mock', url: 'https://mock.example.com', selected: false, disabled: false }
    ]);
  });

  it('has no navigator without registered landscapes or when the registry could not be read', () => {
    expect(buildNavigationBar({ listing: { current: 'preview', landscapes: [] }, signInRequired: false }).landscapeNavigator).toBeUndefined();
    expect(buildNavigationBar({ signInRequired: false }).landscapeNavigator).toBeUndefined();
  });

  it('names this landscape in a badge only when there is no navigator to show it', () => {
    expect(buildNavigationBar({ listing, signInRequired: false }).landscapeBadge).toBeUndefined();
    expect(buildNavigationBar({ listing: { current: 'preview', landscapes: [] }, signInRequired: false }).landscapeBadge)
      .toEqual({ label: 'preview' });
    // The registry could not be read: the landscape comes from the caller
    expect(buildNavigationBar({ landscape: 'staging', signInRequired: false }).landscapeBadge).toEqual({ label: 'staging' });
    expect(buildNavigationBar({ signInRequired: false }).landscapeBadge).toBeUndefined();
  });

  it('tints the landscape by its tone setting, else marks a mock landscape, else leaves it unmarked', () => {
    expect(buildNavigationBar({ listing, landscapeTone: 'production', signInRequired: false }).landscapeTone)
      .toEqual({ name: 'production', description: 'Production landscape' });
    expect(buildNavigationBar({ listing: { ...listing, current: 'mock' }, signInRequired: false }).landscapeTone)
      .toEqual({ name: 'mock', description: 'Mock landscape' });
    expect(buildNavigationBar({ landscape: 'mock1', signInRequired: false }).landscapeTone?.name).toBe('mock');
    expect(buildNavigationBar({ listing, signInRequired: false }).landscapeTone).toBeUndefined();
  });

  it('offers sign-out only when sign-in is required, with the user principal name', () => {
    expect(buildNavigationBar({ listing, signInRequired: false }).signOut).toBeUndefined();
    expect(buildNavigationBar({ signInRequired: true, user: { eduPersonPrincipalName: ['jdoe@bu.edu'] } }).signOut)
      .toEqual({ url: SIGN_OUT_PATH, user: 'jdoe@bu.edu' });
    expect(buildNavigationBar({ signInRequired: true, user: { buPrincipal: 'jdoe' } }).signOut)
      .toEqual({ url: SIGN_OUT_PATH, user: 'jdoe' });
    expect(buildNavigationBar({ signInRequired: true }).signOut).toEqual({ url: SIGN_OUT_PATH, user: undefined });
  });
});

describe('Dashboard page navigation bar', () => {
  const APP_HEADERS = { APP_LOGIN_HEADER: 'app-login', APP_LOGOUT_HEADER: 'app-logout' };

  /** A JWT cookie value whose payload holds the user (the signature is never checked by the Lambda) */
  const jwtCookie = (user: any) => {
    const payload = Buffer.from(JSON.stringify({ 'auth-token': { user } })).toString('base64');
    return `header.${payload}.signature`;
  };

  /**
   * Render /dashboard with a fresh copy of the handler, since Authentication.ts reads the app
   * login/logout headers' names from the environment when it loads.
   */
  const renderDashboard = async (params: {
    env?: Record<string, string>; headers?: Record<string, string>; query?: Record<string, string>; getLandscapes: jest.Mock
  }): Promise<{ status: number; body: string }> => {
    const { env = {}, headers = {}, query, getLandscapes } = params;
    const saved = { ...process.env };
    Object.assign(process.env, env);
    try {
      let result: any;
      await jest.isolateModulesAsync(async () => {
        const { ServiceProvider } = require('../dashboard/src/services/ServiceProvider');
        jest.spyOn(ServiceProvider, 'getLandscapeListingService').mockReturnValue({ getLandscapes });
        const { handler } = require('../dashboard/src/handlers/DashboardHandler');
        result = await handler({
          rawPath: '/dashboard',
          headers,
          queryStringParameters: query,
          requestContext: { http: { method: 'GET', path: '/dashboard' } },
          isBase64Encoded: false
        } as unknown as LambdaFunctionUrlEvent);
      });
      // Mustache escapes "/" in attribute values, which the browser decodes
      return { status: result.statusCode, body: `${result.body}`.replace(/&#x2F;/g, '/') };
    } finally {
      process.env = saved;
    }
  };

  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  it('renders the landscape navigator with this landscape selected', async () => {
    const { status, body } = await renderDashboard({ getLandscapes: jest.fn().mockResolvedValue(listing) });
    expect(status).toBe(200);
    expect(body).toContain('id="landscapeNavigator"');
    expect(body).toMatch(/<option value="https:\/\/mock\.example\.com"\s+>Mock<\/option>/);
    expect(body).toMatch(/<option value="https:\/\/preview\.example\.com" selected\s*>Preview<\/option>/);
    expect(body).toMatch(/<option value=""\s+disabled>Remote<\/option>/);
  });

  it('renders without the navigator when the registry cannot be read', async () => {
    const { status, body } = await renderDashboard({ getLandscapes: jest.fn().mockRejectedValue(new Error('AccessDenied')) });
    expect(status).toBe(200);
    expect(body).not.toContain('id="landscapeNavigator"');
    expect(console.error).toHaveBeenCalledWith('Error listing landscapes for the navigator:', expect.any(Error));
  });

  it('tints the landscape navigator by the LANDSCAPE_TONE setting', async () => {
    const { body } = await renderDashboard({ env: { LANDSCAPE_TONE: 'production' }, getLandscapes: jest.fn().mockResolvedValue(listing) });
    expect(body).toMatch(/<select id="landscapeNavigator" class="[^"]*landscape-tone-production"/);
    expect(body).toContain('title="Production landscape"');
    expect(body).not.toContain('class="app-topbar-item landscape-badge');
  });

  it('shows this landscape in a badge, tinted mock for a mock landscape, when the registry cannot be read', async () => {
    const { body } = await renderDashboard({ env: { LANDSCAPE: 'mock' }, getLandscapes: jest.fn().mockRejectedValue(new Error('AccessDenied')) });
    expect(body).not.toContain('id="landscapeNavigator"');
    expect(body).toMatch(/class="app-topbar-item landscape-badge landscape-tone-mock"[^>]*>\s*<i[^>]*><\/i>mock\s/);
  });

  it('leaves an unmarked landscape untinted', async () => {
    const { body } = await renderDashboard({ getLandscapes: jest.fn().mockResolvedValue(listing) });
    expect(body).not.toMatch(/class="[^"]*landscape-tone-/);
  });

  it('has no sign-out control in public mode', async () => {
    const { body } = await renderDashboard({ getLandscapes: jest.fn().mockResolvedValue(listing) });
    expect(body).not.toContain(`href="${SIGN_OUT_PATH}"`);
  });

  it('renders the sign-out control and the signed-in user when sign-in is required', async () => {
    const { status, body } = await renderDashboard({
      env: { ...APP_HEADERS, APP_AUTHORIZATION: 'true', AUTH_COOKIE_NAME: 'hp-dashboard-auth' },
      headers: { authenticated: 'true', cookie: `other=1; hp-dashboard-auth=${jwtCookie({ eduPersonPrincipalName: 'jdoe@bu.edu' })}` },
      getLandscapes: jest.fn().mockResolvedValue(listing)
    });
    expect(status).toBe(200);
    expect(body).toContain(`href="${SIGN_OUT_PATH}"`);
    expect(body).toContain('jdoe@bu.edu');
  });
});

describe('Dashboard page sidebar', () => {
  const render = async (query?: Record<string, string>): Promise<string> => {
    let body = '';
    await jest.isolateModulesAsync(async () => {
      const { ServiceProvider } = require('../dashboard/src/services/ServiceProvider');
      jest.spyOn(ServiceProvider, 'getLandscapeListingService').mockReturnValue({ getLandscapes: jest.fn().mockResolvedValue(listing) });
      const { handler } = require('../dashboard/src/handlers/DashboardHandler');
      const result = await handler({
        rawPath: '/dashboard',
        headers: {},
        queryStringParameters: query,
        requestContext: { http: { method: 'GET', path: '/dashboard' } },
        isBase64Encoded: false
      } as unknown as LambdaFunctionUrlEvent);
      body = `${result.body}`;
    });
    return body;
  };

  /** The sidebar item for a page: its opening <button ...> tag */
  const navItem = (body: string, page: string): string => {
    const match = body.match(new RegExp(`<button[^>]*id="${page}-tab"[^>]*>`));
    if (!match) throw new Error(`No sidebar item for ${page}`);
    return match[0];
  };

  /** The page's tab pane: its opening <div ...> tag */
  const pane = (body: string, page: string): string => {
    const match = body.match(new RegExp(`<div[^>]*id="${page}"[^>]*>`));
    if (!match) throw new Error(`No pane for ${page}`);
    return match[0];
  };

  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  it('renders the grouped sidebar in place of the tab strip', async () => {
    const body = await render();
    expect(body).not.toContain('nav-tabs');
    for (const group of ['Person', 'Runs', 'System']) {
      expect(body).toMatch(new RegExp(`class="app-nav-group"[^>]*>${group}<`));
    }
    for (const page of ['overview', 'individual', 'mapping', 'runs', 'bulk', 'system']) {
      expect(navItem(body, page)).toContain(`data-page="${page}"`);
      expect(navItem(body, page)).toContain('data-bs-toggle="pill"');
    }
  });

  it('shows the default page when none is requested', async () => {
    const body = await render();
    expect(navItem(body, 'overview')).toMatch(/class="nav-link active"/);
    expect(pane(body, 'overview')).toContain('show active');
    expect(pane(body, 'system')).not.toContain('show active');
  });

  it('shows the requested page', async () => {
    const body = await render({ tab: 'system' });
    expect(navItem(body, 'system')).toMatch(/class="nav-link active"/);
    expect(navItem(body, 'overview')).not.toMatch(/class="nav-link active"/);
    expect(pane(body, 'system')).toContain('show active');
    expect(pane(body, 'overview')).not.toContain('show active');
  });

  it('gives every page a page header, and loads the state patterns before the components using them', async () => {
    const body = await render();
    const titles = body.match(/<h1 class="page-title">[^<]*<\/h1>/g) || [];
    expect(titles.map(title => title.replace(/<[^>]+>/g, ''))).toEqual(
      ['Overview', 'Lookup and sync', 'Mapping experiment', 'Bulk sync', 'Run history', 'Status']
    );
    const pageStateAt = body.indexOf('class PageState');
    expect(pageStateAt).toBeGreaterThan(-1);
    expect(pageStateAt).toBeLessThan(body.indexOf('class PersonCard'));
    expect(pageStateAt).toBeLessThan(body.indexOf('class MappingExperimentCard'));
    // The run history page's classes, defined before dashboard.js creates the page
    expect(pageStateAt).toBeLessThan(body.indexOf('class RunHistoryPage'));
    expect(body.indexOf('class RunHistoryPage')).toBeLessThan(body.indexOf('new RunHistoryPage('));
    // The Overview builds on the run history and service status scripts
    expect(body.indexOf('class RunHistoryPage')).toBeLessThan(body.indexOf('class OverviewPage'));
    expect(body.indexOf('class ServiceStatusTable')).toBeLessThan(body.indexOf('class OverviewPage'));
    expect(body.indexOf('class OverviewPage')).toBeLessThan(body.indexOf('new OverviewPage('));
    // The fixed-height inner scroll boxes are gone: pages scroll as a whole
    expect(body).not.toContain('activity-log');
  });

  it('shows the default page, not a blank one, when an unknown page is requested', async () => {
    const body = await render({ tab: 'nonsense' });
    expect(navItem(body, 'overview')).toMatch(/class="nav-link active"/);
    expect(pane(body, 'overview')).toContain('show active');
  });

  it('lists the Overview first, with no group heading above it', async () => {
    const body = await render();
    const nav = body.slice(body.indexOf('id="dashboardTabs"'));
    expect(nav.indexOf('id="overview-tab"')).toBeLessThan(nav.indexOf('class="app-nav-group"'));
  });
});
