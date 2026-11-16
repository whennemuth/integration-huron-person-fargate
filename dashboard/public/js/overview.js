/**
 * OverviewPage - the landing page: this landscape's latest sync run (its status, figures and a link to its
 * page) and the health of the services the integration depends on. It loads each time it is shown, and on
 * Refresh; the run and the services load independently, so one failing leaves the other shown.
 *
 * In a mock landscape it also shows the Mock landscape section (GET /api/mock-landscape/reset): the last reset of the
 * landscape's run state and how long each table took, and Reset landscape, which, once confirmed, has the chunker
 * subscriber empty the target simulator, Run history and the fingerprints in the background (POST). In any other
 * landscape the section stays hidden.
 *
 * Builds on run-history.js (RunsApi, RunFormat, RunDetailView, RunHistoryPage) and system-status.js
 * (ServiceStatusTable), which load first. Tests: test/DashboardOverview.test.ts.
 */
class OverviewPage {

    /**
     * @param {{ api: RunsApi, getJson: function(string): Promise<any>, postJson?: function(string, any): Promise<any>,
     *   root: HTMLElement, now?: function(): number, confirm?: function(string): boolean }} options
     *   getJson and postJson read and send a route's JSON (dashboard.js); now is the clock "ago" times are taken from;
     *   confirm asks before a reset (the browser's confirm)
     */
    constructor({ api, getJson, postJson, root, now = () => Date.now(), confirm = message => window.confirm(message) }) {
        this.api = api;
        this.getJson = getJson;
        this.postJson = postJson;
        this.now = now;
        this.confirm = confirm;
        this.runContainer = root.querySelector('[data-latest-run]');
        this.servicesContainer = root.querySelector('[data-services]');
        this.resetSection = root.querySelector('[data-mock-reset-section]');
        this.resetContainer = root.querySelector('[data-mock-reset]');
        this.stamp = root.querySelector('[data-refresh-stamp="overview"]');
        this.loads = 0;
        root.querySelector('[data-action="refresh-overview"]').addEventListener('click', () => this.load());
        if (this.resetContainer) {
            this.resetContainer.addEventListener('click', event => {
                const action = event.target.closest && event.target.closest('[data-action="reset-landscape"]');
                if (action) this.reset();
            });
        }
    }

    /** Load the latest run and the service checks. Resolves true once they are shown (unless a later load overtook it) */
    async load() {
        const load = ++this.loads;
        PageState.loading(this.runContainer, 'Loading the latest run');
        PageState.loading(this.servicesContainer, 'Checking services');
        const [runs, status, reset] = await Promise.allSettled([
            this.api.listRuns({ limit: 1 }),
            this.getJson('/api/status'),
            this.getJson('/api/mock-landscape/reset')
        ]);
        if (load !== this.loads) {
            return false;
        }
        this.renderRun(runs);
        this.renderServices(status);
        this.renderReset(reset);
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

    /** The Mock landscape section: shown only in a mock landscape (or when its state can't be read, to say so) */
    renderReset(result) {
        if ( ! this.resetSection || ! this.resetContainer) {
            return;
        }
        if (result.status === 'rejected') {
            this.resetSection.hidden = false;
            PageState.error(this.resetContainer, "The mock landscape's last reset could not be read", result.reason && result.reason.message);
            return;
        }
        this.resetSection.hidden = ! result.value.mockLandscape;
        if (result.value.mockLandscape) {
            this.resetContainer.innerHTML = OverviewPage.resetHtml(result.value, this.now());
        }
    }

    /** Reset landscape: confirmed, then handed to the chunker subscriber. Resolves true once it has accepted it */
    async reset() {
        const message = this.resetContainer.querySelector('[data-mock-reset-message]');
        const button = this.resetContainer.querySelector('[data-action="reset-landscape"]');
        const say = (text, failed) => {
            if (message) {
                message.textContent = text;
                message.hidden = false;
                message.className = failed ? 'mock-reset-message mock-reset-failed' : 'mock-reset-message';
            }
        };
        if ( ! this.postJson || ! this.confirm(OverviewPage.RESET_CONFIRMATION)) {
            return false;
        }
        if (button) button.disabled = true;
        try {
            await this.postJson('/api/mock-landscape/reset', {});
            say('The reset has started in the background and takes up to 10 minutes. Refresh to check whether it has finished.', false);
            return true;
        } catch (error) {
            if (button) button.disabled = false;
            say(`The reset wasn't started: ${error.message}`, true);
            return false;
        }
    }

    /** What Reset landscape asks before it starts */
    static get RESET_CONFIRMATION() {
        return "Reset this mock landscape? Its target simulator, Run history and every person's fingerprint are emptied, so the next run starts from scratch. This can't be undone.";
    }

    /** A reset's time to the second, for timing it: 0.4 s, 45 s, 1 min 42 s */
    static resetDuration(ms) {
        if ( ! RunFormat.isNumber(ms) || ms < 0) {
            return RunFormat.NONE;
        }
        if (ms < 10000) {
            return `${(ms / 1000).toFixed(1)} s`;
        }
        const seconds = Math.round(ms / 1000);
        return seconds < 60 ? `${seconds} s` : `${Math.floor(seconds / 60)} min ${String(seconds % 60).padStart(2, '0')} s`;
    }

    /** The words for a reset's tables (BulkPurgerTables) */
    static get RESET_TABLE_LABELS() {
        return {
            mock_target_person: "Target simulator's people",
            person_current_state: 'Fingerprints (person current state)',
            person_history: 'Person history',
            statistics: 'Run history (statistics)'
        };
    }

    /**
     * The Mock landscape section's content, from MockLandscapeResetState. A reset that can't be started because one is
     * running says nothing more than the running one's line does: its button is just disabled.
     */
    static resetHtml(state, now) {
        const { last, lastError, blocked } = state;
        return `
            <div class="surface mock-reset">
                <p class="mock-reset-intro">
                    This is a mock landscape: its target is the target simulator, a stand-in for Huron. Reset landscape
                    empties the target simulator, Run history and every person's fingerprint, so the next run starts from
                    scratch, as Bulk sync's Start from scratch does before a run, but without starting one. The chunker
                    subscriber does it in the background, in up to 10 minutes.
                </p>
                ${lastError ? `<p class="mock-reset-last">The last reset could not be read: ${Html.escape(lastError)}</p>` : OverviewPage.lastResetHtml(last, now)}
                ${blocked && blocked.code !== 'RESET_UNDERWAY' ? `<p class="mock-reset-blocked" role="note">A reset can't be started now: ${Html.escape(blocked.message)}</p>` : ''}
                <button class="btn btn-sm btn-outline-danger" type="button" data-action="reset-landscape"${blocked ? ` disabled title="${Html.escape(blocked.message)}"` : ''}>
                    <i class="fas fa-eraser me-1" aria-hidden="true"></i>Reset landscape
                </button>
                <p class="mock-reset-message" data-mock-reset-message role="status" hidden></p>
            </div>
        `;
    }

    /** The last reset in words: who started it and how, what it did, and how long each table took */
    static lastResetHtml(last, now) {
        if ( ! last) {
            return '<p class="mock-reset-last">No reset recorded yet.</p>';
        }
        const how = last.via === 'bulk-sync' ? "by Bulk sync's Start from scratch" : 'from Overview';
        const who = last.startedBy && last.startedBy.principal ? ` by ${last.startedBy.principal}` : '';
        const started = `started ${RunFormat.ago(last.startedAt, now)} ${how}${who}`;
        const took = last.durationMs === undefined ? '' : OverviewPage.resetDuration(last.durationMs);
        if (last.status === 'running') {
            const running = `A reset has been running for ${RunFormat.span(now - Date.parse(last.startedAt))} ` +
                `(started ${RunFormat.clockTime(Date.parse(last.startedAt), now)} ${how}${who}).`;
            return `<p class="mock-reset-last">${Html.escape(running)} It takes up to 10 minutes. Refresh to check whether it has finished.</p>`;
        }
        if (last.status === 'stopped') {
            return `<p class="mock-reset-last mock-reset-failed">The last reset, ${Html.escape(started)}, never finished: it most likely ran out of the chunker subscriber's 10 minutes. Its tables may be partly emptied: reset again to finish.</p>`;
        }
        if (last.status === 'failed') {
            return `<p class="mock-reset-last mock-reset-failed">The last reset, ${Html.escape(started)}, failed after ${Html.escape(took)}: ${Html.escape(last.error || 'no reason recorded')}</p>`;
        }
        const labels = OverviewPage.RESET_TABLE_LABELS;
        const rows = (last.tables || []).map(table => `
            <tr>
                <td>${Html.escape(labels[table.table] || table.tableName)}</td>
                <td class="num">${Html.escape(RunFormat.number(table.itemsDeleted))}</td>
                <td class="num">${Html.escape(OverviewPage.resetDuration(table.durationMs))}</td>
            </tr>
        `).join('');
        const baseline = last.baselineObjectsDeleted === undefined ? ''
            : `<p class="mock-reset-last">It also deleted ${Html.escape(RunFormat.number(last.baselineObjectsDeleted))} S3 baseline file(s).</p>`;
        return `
            <p class="mock-reset-last">The last reset, ${Html.escape(started)}, finished in ${Html.escape(took)}.</p>
            ${rows ? `
                <div class="table-responsive">
                    <table class="table data-table mock-reset-tables">
                        <thead><tr><th scope="col">Emptied</th><th scope="col" class="num">Items deleted</th><th scope="col" class="num">Took</th></tr></thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
            ` : ''}
            ${baseline}
        `;
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
                        ${run.statusBadgeHtml(now)}
                    </p>
                    <p class="latest-run-facts">${[...run.facts, when].map(Html.escape).join(' &middot; ')}</p>
                </div>
                <a class="btn btn-sm btn-outline-secondary" href="${href}">View run</a>
            </div>
            ${RunDetailView.noteHtml(run)}
            <div class="latest-run-figures">${RunDetailView.tilesHtml(run, now)}</div>
        `;
    }
}
