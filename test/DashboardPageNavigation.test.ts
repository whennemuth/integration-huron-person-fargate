import { DEFAULT_PAGE, DEFAULT_PAGE_GROUPS, PageGroupDefinition, PageNavigation } from '../dashboard/src/handlers/PageNavigation';

describe('PageNavigation', () => {
  const navigation = new PageNavigation();

  it('lists every page in sidebar order', () => {
    expect(navigation.pageIds).toEqual(['individual', 'mapping', 'history', 'bulk', 'system']);
  });

  it('makes the requested page active', () => {
    expect(navigation.activePage('system')).toBe('system');
  });

  it('falls back to the default page when no page, or an unknown one, is requested', () => {
    expect(navigation.activePage()).toBe(DEFAULT_PAGE);
    expect(navigation.activePage('nonsense')).toBe(DEFAULT_PAGE);
    expect(navigation.activePage('')).toBe(DEFAULT_PAGE);
  });

  it('groups the sidebar items with exactly one active', () => {
    const groups = navigation.navigationGroups('history');
    expect(groups.map(({ label }) => label)).toEqual(['Person', 'Runs', 'System']);
    const active = groups.flatMap(({ items }) => items).filter(({ active }) => active);
    expect(active).toEqual([{ id: 'history', label: 'Activity history', icon: 'fa-clock-rotate-left', active: true }]);
  });

  it('sets one <page>Active flag per page, true only for the active page', () => {
    expect(navigation.activeFlags('bulk')).toEqual({
      individualActive: false,
      mappingActive: false,
      historyActive: false,
      bulkActive: true,
      systemActive: false
    });
  });

  it('gives the template the flags, the groups and the active page together', () => {
    const data = navigation.templateData('mapping');
    expect(data.activePage).toBe('mapping');
    expect(data.mappingActive).toBe(true);
    expect(data.navigation).toEqual(navigation.navigationGroups('mapping'));
  });

  it('accepts other groups and a default page of its own', () => {
    const groups: PageGroupDefinition[] = [{ label: 'Only', pages: [{ id: 'system', label: 'Status', icon: 'fa-heart-pulse' }] }];
    const custom = new PageNavigation(groups, 'system');
    expect(custom.pageIds).toEqual(['system']);
    expect(custom.activePage('individual')).toBe('system');
  });

  it('refuses a default page that is not one of its pages', () => {
    const groups: PageGroupDefinition[] = [{ label: 'Only', pages: [{ id: 'system', label: 'Status', icon: 'fa-heart-pulse' }] }];
    expect(() => new PageNavigation(groups, 'individual')).toThrow(/default page "individual"/);
  });

  it('defaults to the dashboard\'s own groups', () => {
    expect(navigation.navigationGroups().map(({ label }) => label)).toEqual(DEFAULT_PAGE_GROUPS.map(({ label }) => label));
  });
});
