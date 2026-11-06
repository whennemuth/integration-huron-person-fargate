/**
 * OverviewPage - the landing page: this landscape's latest sync run (its status, figures and a link to its
 * page) and the health of the services the integration depends on. It loads each time it is shown, and on
 * Refresh; the run and the services load independently, so one failing leaves the other shown.
 *
 * Builds on run-history.js (RunsApi, RunFormat, RunDetailView, RunHistoryPage) and system-status.js
 * (ServiceStatusTable), which load first. Tests: test/DashboardOverview.test.ts.
 */
class OverviewPage {

    /**
     * @param {{ api: RunsApi, getJson: function(string): Promise<any>, root: HTMLElement, now?: function(): number }} options
     *   getJson reads a route's JSON (dashboard.js's fetchSectionData); now is the clock "ago" times are taken from
     */
    constructor({ api, getJson, root, now = () => Date.now() }) {
        this.api = api;
        this.getJson = getJson;
        this.now = now;
        this.runContainer = root.querySelector('[data-latest-run]');
        this.servicesContainer = root.querySelector('[data-services]');
        this.stamp = root.querySelector('[data-refresh-stamp="overview"]');
        this.loads = 0;
        root.querySelector('[data-action="refresh-overview"]').addEventListener('click', () => this.load());
    }

    /** Load the latest run and the service checks. Resolves true once they are shown (unless a later load overtook it) */
    async load() {
        const load = ++this.loads;
        PageState.loading(this.runContainer, 'Loading the latest run');
        PageState.loading(this.servicesContainer, 'Checking services');
        const [runs, status] = await Promise.allSettled([
            this.api.listRuns({ limit: 1 }),
            this.getJson('/api/status')
        ]);
        if (load !== this.loads) {
            return false;
        }
        this.renderRun(runs);
        this.renderServices(status);
        if (runs.status === 'fulfilled' || status.status === 'fulfilled') {
            RefreshStamp.set(this.stamp);
        }
        return true;
    }

    renderRun(result) {
        if (result.status === 'rejected') {
            PageState.error(this.runContainer, 'The latest run could not be loaded', result.reason && result.reason.message);
            return;
        }
        const run = (result.value.items || [])[0];
        if ( ! run) {
            PageState.empty(this.runContainer, 'No runs yet', 'The latest sync run in this landscape will be shown here once the first one starts.');
            return;
        }
        this.runContainer.innerHTML = OverviewPage.latestRunHtml(run, this.now());
    }

    renderServices(result) {
        if (result.status === 'rejected') {
            PageState.error(this.servicesContainer, 'Service status could not be loaded', result.reason && result.reason.message);
            return;
        }
        ServiceStatusTable.show(this.servicesContainer, result.value.services);
    }

    /** The latest run: when and how it ran, linked to its page, then its note (if any) and its figures */
    static latestRunHtml(run, now) {
        const href = Html.escape(RunHistoryPage.detailHref(run.syncRunId));
        const when = run.completedAt
            ? `Finished ${RunFormat.ago(run.completedAt, now)}`
            : `Started ${RunFormat.ago(run.startedAt, now)}`;
        return `
            <div class="surface latest-run">
                <div>
                    <p class="latest-run-title">
                        <a class="run-link" href="${href}">Run of ${Html.escape(RunFormat.dateTime(run.startedAt))}</a>
                        ${RunFormat.statusBadge(run.status)}
                    </p>
                    <p class="latest-run-facts">${[...run.facts, when].map(Html.escape).join(' &middot; ')}</p>
                </div>
                <a class="btn btn-sm btn-outline-secondary" href="${href}">View run</a>
            </div>
            ${RunDetailView.noteHtml(run)}
            <div class="latest-run-figures">${RunDetailView.tilesHtml(run)}</div>
        `;
    }
}
