/**
 * The dashboard's pages and how the sidebar groups them.
 *
 * Each page is a tab pane in the dashboard template (its partial's element id is the page id), shown
 * by the sidebar item of the same id. The server renders one page active: the one the request asks
 * for (`?tab=<id>`), else the default. In the browser, a URL fragment (`#<id>`) then takes precedence
 * (`public/js/dashboard.js`), since the server never sees it.
 */
export type PageId = 'individual' | 'mapping' | 'bulk' | 'runs' | 'history' | 'system';

/** A page as the sidebar shows it */
export type PageDefinition = {
  id: PageId;
  label: string;
  /** Font Awesome icon class, e.g. 'fa-search' */
  icon: string;
};

export type PageGroupDefinition = {
  label: string;
  pages: PageDefinition[];
};

/** A sidebar item, as the dashboard template renders it */
export type NavigationItem = PageDefinition & { active: boolean };

/** A sidebar group, as the dashboard template renders it */
export type NavigationGroup = { label: string; items: NavigationItem[] };

/** One flag per page (`individualActive`, ...), which the page partials use to render themselves shown */
export type PageActiveFlags = { [Id in PageId as `${Id}Active`]: boolean };

/** The data the dashboard template needs for its sidebar and pages */
export type PageNavigationData = PageActiveFlags & {
  navigation: NavigationGroup[];
  activePage: PageId;
};

export const DEFAULT_PAGE_GROUPS: PageGroupDefinition[] = [
  {
    label: 'Person',
    pages: [
      { id: 'individual', label: 'Lookup and sync', icon: 'fa-magnifying-glass' },
      { id: 'mapping', label: 'Mapping experiment', icon: 'fa-flask' },
    ]
  },
  {
    label: 'Runs',
    pages: [
      { id: 'runs', label: 'Run history', icon: 'fa-clock-rotate-left' },
      { id: 'history', label: 'Activity history', icon: 'fa-scroll' },
      { id: 'bulk', label: 'Bulk sync', icon: 'fa-play' },
    ]
  },
  {
    label: 'System',
    pages: [
      { id: 'system', label: 'Status', icon: 'fa-heart-pulse' },
    ]
  }
];

export const DEFAULT_PAGE: PageId = 'individual';

export class PageNavigation {

  constructor(
    private readonly groups: PageGroupDefinition[] = DEFAULT_PAGE_GROUPS,
    private readonly defaultPage: PageId = DEFAULT_PAGE
  ) {
    if ( ! this.pageIds.includes(defaultPage)) {
      throw new Error(`The default page "${defaultPage}" is not one of the navigation's pages`);
    }
  }

  /** Every page id, in sidebar order */
  public get pageIds(): PageId[] {
    return this.groups.flatMap(({ pages }) => pages.map(({ id }) => id));
  }

  public isPage = (id?: string): id is PageId => {
    return !!id && (this.pageIds as string[]).includes(id);
  }

  /** The requested page if it is one, else the default: an unknown id never leaves every page hidden */
  public activePage = (requested?: string): PageId => {
    return this.isPage(requested) ? requested : this.defaultPage;
  }

  /** The sidebar's groups, with the active page's item marked */
  public navigationGroups = (requested?: string): NavigationGroup[] => {
    const active = this.activePage(requested);
    return this.groups.map(({ label, pages }) => ({
      label,
      items: pages.map(page => ({ ...page, active: page.id === active }))
    }));
  }

  /** The `<page>Active` flags the page partials use */
  public activeFlags = (requested?: string): PageActiveFlags => {
    const active = this.activePage(requested);
    const flags = {} as PageActiveFlags;
    for (const id of this.pageIds) {
      flags[`${id}Active`] = id === active;
    }
    return flags;
  }

  /** Everything the dashboard template needs for the sidebar and the pages */
  public templateData = (requested?: string): PageNavigationData => {
    return {
      ...this.activeFlags(requested),
      navigation: this.navigationGroups(requested),
      activePage: this.activePage(requested)
    };
  }
}
