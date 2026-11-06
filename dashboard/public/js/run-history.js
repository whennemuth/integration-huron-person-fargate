/**
 * Run history page (the "runs" page): this landscape's sync runs, newest first, and one run's own page
 * (#runs/<syncRunId>) with its facts, its figures, and its Overview, Chunks, Errors and Changes sections.
 *
 * Built from small classes, each testable on its own (test/DashboardRunHistory.test.ts):
 *   RunHtml         escaping, badges and tags for HTML built from template literals
 *   RunFormat       numbers, times, durations and the labels for statuses and codes
 *   RunView         one run's report (from the run reporting routes) reduced to what the page shows
 *   RunsApi         the run reporting routes
 *   CursorPager     "load more" paging over a cursor-paged route
 *   PagedTable      a table filled a page at a time, with a "load more" footer
 *   RunDetailView   one run's page
 *   RunHistoryPage  shows the list or a run by the URL fragment, and owns the list's Refresh
 *
 * Editing note: the Lambda build minifies this file with a regex minifier (dashboard/scripts/BuildTemplates.ts)
 * that drops the rest of a line after a double slash and joins lines. So no string may hold a double slash,
 * and every statement ends with a semicolon.
 */

/** Escaping and small markup pieces for HTML built from template literals */
class RunHtml {

    static escape(value) {
        return String(value === undefined || value === null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    /** A status badge (dot and word); tone is success, warning, danger, or none for neutral */
    static badge(label, tone) {
        return `<span class="status-badge${tone ? ` status-badge-${tone}` : ''}">${RunHtml.escape(label)}</span>`;
    }

    /** A small neutral marker beside a value, e.g. "Dry run" */
    static tag(label) {
        return `<span class="run-tag">${RunHtml.escape(label)}</span>`;
    }
}

/** How run figures, times and codes read on the page */
class RunFormat {

    /** Shown where a figure or time is unknown */
    static get NONE() {
        return '–';
    }

    static get STATUSES() {
        return {
            SUCCEEDED: { label: 'Succeeded', tone: 'success' },
            FAILED: { label: 'Failed', tone: 'danger' },
            POST_PROCESSING_FAILED: { label: 'Post-processing failed', tone: 'danger' },
            INCOMPLETE: { label: 'Incomplete', tone: 'warning' },
            UNKNOWN: { label: 'Unknown', tone: null }
        };
    }

    static get DELETION_STATUSES() {
        return {
            COMPLETED: { label: 'Completed', tone: 'success' },
            SKIPPED: { label: 'Skipped', tone: null },
            FAILED: { label: 'Failed', tone: 'danger' }
        };
    }

    static get SKIP_REASONS() {
        return {
            TERMINAL_ERROR: 'the run failed',
            PERSON_DELTA: 'delta sync',
            DELETES_DISABLED: 'deletes disabled',
            NO_MERGE_OUTPUT: 'no merge output',
            MISSING_TABLES: 'tables missing'
        };
    }

    static get POPULATIONS() {
        return { 'person-full': 'Full sync', 'person-delta': 'Delta sync' };
    }

    static get STORAGE_MODES() {
        return { dynamodb: 'DynamoDB', s3: 'S3' };
    }

    static get HTTP_STATUS_TEXT() {
        return {
            400: 'Bad Request', 401: 'Unauthorized', 403: 'Forbidden', 404: 'Not Found', 409: 'Conflict',
            422: 'Unprocessable Content', 429: 'Too Many Requests', 500: 'Internal Server Error',
            502: 'Bad Gateway', 503: 'Service Unavailable', 504: 'Gateway Timeout'
        };
    }

    /** The change types a run's person changes are listed by, as the page names them */
    static get CHANGE_TYPES() {
        return [
            { type: 'NEW', label: 'Added' },
            { type: 'UPDATED', label: 'Updated' },
            { type: 'DELETED', label: 'Deactivated' }
        ];
    }

    static status(status) {
        return RunFormat.STATUSES[status] || { label: status ? String(status) : 'Unknown', tone: null };
    }

    static statusBadge(status) {
        const { label, tone } = RunFormat.status(status);
        return RunHtml.badge(label, tone);
    }

    static deletionBadge(status) {
        const { label, tone } = RunFormat.DELETION_STATUSES[status] || { label: status || 'Unknown', tone: null };
        return RunHtml.badge(label, tone);
    }

    static isNumber(value) {
        return value !== undefined && value !== null && value !== '' && Number.isFinite(Number(value));
    }

    static number(value) {
        return RunFormat.isNumber(value) ? Number(value).toLocaleString() : RunFormat.NONE;
    }

    /** A date and time, to the minute (or the second, for an event within a run) */
    static dateTime(iso, options = {}) {
        const date = new Date(iso);
        if (Number.isNaN(date.getTime())) {
            return iso ? String(iso) : RunFormat.NONE;
        }
        return date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: options.seconds ? 'medium' : 'short' });
    }

    /** A duration in the largest unit that reads well: 45 s, 47 min, 2 h 05 min */
    static duration(ms) {
        if ( ! RunFormat.isNumber(ms) || ms < 0) {
            return RunFormat.NONE;
        }
        const seconds = Math.round(ms / 1000);
        if (seconds < 60) {
            return `${seconds} s`;
        }
        const minutes = Math.round(seconds / 60);
        if (minutes < 60) {
            return `${minutes} min`;
        }
        return `${Math.floor(minutes / 60)} h ${String(minutes % 60).padStart(2, '0')} min`;
    }

    static population(population) {
        return RunFormat.POPULATIONS[population] || (population ? String(population) : RunFormat.NONE);
    }

    static storageMode(mode) {
        return RunFormat.STORAGE_MODES[String(mode || '').toLowerCase()] || (mode ? String(mode) : RunFormat.NONE);
    }

    static httpStatus(code) {
        const text = RunFormat.HTTP_STATUS_TEXT[code];
        return text ? `${code} ${text}` : String(code);
    }

    static yesNo(value) {
        return value ? 'Yes' : 'No';
    }
}

/**
 * One run as the page shows it, from its report: the post-processor's summary when there is one, else what
 * the run's metadata, flags and terminal error say (a run that failed early, or is incomplete).
 */
class RunView {

    constructor(report) {
        const item = report || {};
        const summary = item.summary;
        const metadata = item.metadata || {};
        const flags = item.flags || {};
        const results = (summary && summary.results) || {};
        const first = RunView.first;

        this.syncRunId = item.syncRunId;
        this.status = item.status || 'UNKNOWN';
        this.hasSummary = !!summary;
        this.flags = item.flags;
        this.terminalError = item.terminalError;

        this.startedAt = first(summary && summary.createdAt, metadata.createdAt, item.syncRunId);
        this.completedAt = summary ? summary.completedAt : undefined;
        const started = Date.parse(this.startedAt);
        const completed = Date.parse(this.completedAt);
        this.durationMs = Number.isFinite(started) && Number.isFinite(completed) ? completed - started : undefined;

        this.population = first(summary && summary.syncPopulation, metadata.syncPopulation, flags.syncPopulation);
        this.buid = RunView.buidFrom(metadata.source);
        this.storageMode = summary ? summary.storageMode : undefined;
        this.dryRun = !!(summary && summary.dryRun);
        this.bulkReset = !!((summary && summary.bulkReset) || metadata.bulkReset || flags.bulkReset);

        this.chunkCount = first(summary && summary.chunkCount, metadata.chunkCount);
        this.chunksReported = summary ? summary.chunksReported : undefined;
        this.records = first(summary && summary.totalRecords, metadata.totalRecords);
        this.processed = results.processed;
        this.succeeded = results.succeeded;
        this.failed = results.failed;
        this.skipped = results.skipped;
        this.added = results.added;
        this.updated = results.updated;

        this.deletion = summary ? summary.deletion : undefined;
        const deactivatedByDeletion = this.deletion && this.deletion.status === 'COMPLETED' ? (this.deletion.deletedCount || 0) : 0;
        this.deactivated = summary ? (results.removed || 0) + deactivatedByDeletion : undefined;

        this.errors = summary ? summary.totalErrors : undefined;
        this.throttled = summary ? summary.throttleCount : undefined;
        this.errorsByStatus = summary ? summary.errorsByStatus : undefined;
        this.failureMessage = first(item.terminalError && item.terminalError.errorMessage, flags.runFailureMessage);
        this.failedAt = first(item.terminalError && item.terminalError.errorTimestamp, flags.runFailureTimestamp);
    }

    /** "Person U12345678" for a single-person run whose source names the person, else the population */
    get populationLabel() {
        return this.buid ? `Person ${this.buid}` : RunFormat.population(this.population);
    }

    /** Markers shown beside the population */
    get tags() {
        const tags = [];
        if (this.dryRun) tags.push('Dry run');
        if (this.bulkReset) tags.push('Bulk reset');
        return tags;
    }

    /** How many people the run changed in one way (a change type: NEW, UPDATED or DELETED) */
    changeCount(type) {
        return { NEW: this.added, UPDATED: this.updated, DELETED: this.deactivated }[type];
    }

    static first(...values) {
        return values.find(value => value !== undefined && value !== null);
    }

    /** The BUID a single-person run fetched (its source URL's buid parameter), if any */
    static buidFrom(source) {
        if ( ! source) {
            return undefined;
        }
        try {
            return new URL(source).searchParams.get('buid') || undefined;
        } catch (error) {
            return undefined;
        }
    }
}

/**
 * The run reporting routes. getJson(path) resolves to a route's JSON body, or rejects with an Error carrying
 * the server's message and the response status (dashboard.js's fetchSectionData).
 */
class RunsApi {

    constructor(getJson) {
        this.getJson = getJson;
        this.capabilitiesRequest = undefined;
    }

    static query(params) {
        const query = new URLSearchParams();
        Object.entries(params || {}).forEach(([name, value]) => {
            if (value !== undefined && value !== null && value !== '') {
                query.set(name, String(value));
            }
        });
        const text = query.toString();
        return text ? `?${text}` : '';
    }

    static runPath(syncRunId, report, params) {
        return `/api/runs/${encodeURIComponent(syncRunId)}/${report}${RunsApi.query(params)}`;
    }

    /** A page of runs, newest first; cursor is the previous page's (the route's `before`) */
    async listRuns({ cursor, limit } = {}) {
        const data = await this.getJson(`/api/runs${RunsApi.query({ include: 'report', limit, before: cursor })}`);
        return { items: (data.items || []).map(item => new RunView(item)), cursor: data.before };
    }

    async run(syncRunId) {
        return new RunView(await this.getJson(RunsApi.runPath(syncRunId, 'summary')));
    }

    async chunks(syncRunId) {
        const data = await this.getJson(RunsApi.runPath(syncRunId, 'chunks'));
        return Array.isArray(data.items) ? data.items : [];
    }

    async errors(syncRunId, { statusCode, cursor, limit } = {}) {
        const data = await this.getJson(RunsApi.runPath(syncRunId, 'errors', { statusCode, cursor, limit }));
        return { items: data.items || [], cursor: data.cursor };
    }

    async changes(syncRunId, { changeType, cursor, limit } = {}) {
        const data = await this.getJson(RunsApi.runPath(syncRunId, 'changes', { changeType, cursor, limit }));
        return { items: data.items || [], cursor: data.cursor };
    }

    /** The landscape's storage mode and whether it keeps change lists: asked once, unless the asking fails */
    capabilities() {
        if ( ! this.capabilitiesRequest) {
            this.capabilitiesRequest = this.getJson('/api/capabilities').catch(error => {
                this.capabilitiesRequest = undefined;
                throw error;
            });
        }
        return this.capabilitiesRequest;
    }
}

/**
 * Pages through a cursor-paged route a page at a time. loadPage(cursor) resolves to { items, cursor }, whose
 * cursor is absent on the last page. A page that arrives after reset() is discarded: next() resolves to null.
 */
class CursorPager {

    constructor(loadPage) {
        this.loadPage = loadPage;
        this.generation = 0;
        this.reset();
    }

    reset() {
        this.generation += 1;
        this.items = [];
        this.cursor = undefined;
        this.started = false;
    }

    get hasMore() {
        return ! this.started || this.cursor !== undefined;
    }

    /** The next page's items ([] when there are no more, null if reset while it loaded) */
    async next() {
        if ( ! this.hasMore) {
            return [];
        }
        const generation = this.generation;
        const page = await this.loadPage(this.cursor);
        if (generation !== this.generation) {
            return null;
        }
        const items = (page && page.items) || [];
        this.started = true;
        this.items.push(...items);
        this.cursor = (page && page.cursor) || undefined;
        return items;
    }
}

/**
 * A table filled a page at a time: the first page replaces the container's content, and a footer offers the
 * next ("Load older runs") until there are no more, or explains a page that failed and offers to try again.
 *
 * options: container, loadPage(cursor), headHtml (the header row's cells), rowHtml(item), and text:
 *   { loading, loadingMore, error, errorMore, empty, emptyMessage, more, noun: [one, many] }
 * plus total (how many items there are, when known) and rowLinks (rows carry data-href and open it on click).
 */
class PagedTable {

    constructor(options) {
        this.options = options;
        this.pager = new CursorPager(options.loadPage);
        this.body = undefined;
        this.footer = undefined;
    }

    /** Load the first page. Resolves true once it is shown */
    async load() {
        const { container, text } = this.options;
        this.pager.reset();
        PageState.loading(container, text.loading);
        let items;
        try {
            items = await this.pager.next();
        } catch (error) {
            PageState.error(container, text.error, error.message);
            return false;
        }
        if (items === null) {
            return false;
        }
        if (items.length === 0) {
            PageState.empty(container, text.empty, text.emptyMessage);
            return true;
        }
        container.innerHTML = `
            <div class="table-responsive">
                <table class="table data-table${this.options.rowLinks ? ' data-table-links' : ''}">
                    <thead><tr>${this.options.headHtml}</tr></thead>
                    <tbody>${this.rowsHtml(items)}</tbody>
                </table>
            </div>
            <div class="table-footer"></div>
        `;
        this.body = container.querySelector('tbody');
        this.footer = container.querySelector('.table-footer');
        if (this.options.rowLinks) {
            this.body.addEventListener('click', PagedTable.followRowLink);
        }
        this.renderFooter();
        return true;
    }

    async loadMore() {
        const { text } = this.options;
        PageState.loading(this.footer, text.loadingMore, { compact: true });
        let items;
        try {
            items = await this.pager.next();
        } catch (error) {
            this.renderFooter(error);
            return;
        }
        if (items === null) {
            return;
        }
        this.body.insertAdjacentHTML('beforeend', this.rowsHtml(items));
        this.renderFooter();
    }

    rowsHtml(items) {
        return items.map(item => this.options.rowHtml(item)).join('');
    }

    /** How many items are shown, of how many when that's known */
    countText() {
        const { text, total } = this.options;
        const shown = this.pager.items.length;
        const noun = (count) => (count === 1 ? text.noun[0] : text.noun[1]);
        if (this.pager.hasMore && RunFormat.isNumber(total) && total >= shown) {
            return `Showing ${RunFormat.number(shown)} of ${RunFormat.number(total)} ${noun(total)}`;
        }
        return this.pager.hasMore
            ? `Showing ${RunFormat.number(shown)} ${noun(shown)}`
            : `All ${RunFormat.number(shown)} ${noun(shown)} shown`;
    }

    renderFooter(error) {
        const { text } = this.options;
        this.footer.replaceChildren();
        if (error) {
            this.footer.appendChild(PageState.element('error', { title: text.errorMore, message: error.message, compact: true }));
        }
        const count = document.createElement('span');
        count.className = 'table-footer-count';
        count.textContent = this.countText();
        this.footer.appendChild(count);
        if (this.pager.hasMore) {
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'btn btn-sm btn-outline-secondary';
            button.textContent = error ? 'Try again' : text.more;
            button.addEventListener('click', () => this.loadMore());
            this.footer.appendChild(button);
        }
    }

    /** A click anywhere on a linked row opens its link (the link itself stays the keyboard's way in) */
    static followRowLink(event) {
        if (event.target.closest('a, button')) {
            return;
        }
        const row = event.target.closest('tr[data-href]');
        if (row) {
            window.location.hash = row.dataset.href;
        }
    }
}

/** The list of runs: one table, a page of runs at a time */
class RunListTable {

    static get HEAD_HTML() {
        return `
            <th scope="col">Started</th>
            <th scope="col">Status</th>
            <th scope="col">Population</th>
            <th scope="col" class="num">Records</th>
            <th scope="col" class="num">Added</th>
            <th scope="col" class="num">Updated</th>
            <th scope="col" class="num">Deactivated</th>
            <th scope="col" class="num">Errors</th>
            <th scope="col" class="num">Duration</th>
        `;
    }

    static rowHtml(run) {
        const href = RunHtml.escape(RunHistoryPage.detailHref(run.syncRunId));
        const tags = run.tags.map(RunHtml.tag).join(' ');
        const number = RunFormat.number;
        return `
            <tr data-href="${href}">
                <td><a class="run-link tabular-nums" href="${href}">${RunHtml.escape(RunFormat.dateTime(run.startedAt))}</a></td>
                <td>${RunFormat.statusBadge(run.status)}</td>
                <td>${RunHtml.escape(run.populationLabel)}${tags ? ` ${tags}` : ''}</td>
                <td class="num">${number(run.records)}</td>
                <td class="num">${number(run.added)}</td>
                <td class="num">${number(run.updated)}</td>
                <td class="num">${number(run.deactivated)}</td>
                <td class="num">${number(run.errors)}</td>
                <td class="num">${RunHtml.escape(RunFormat.duration(run.durationMs))}</td>
            </tr>
        `;
    }

    static create(api, container, pageSize = 20) {
        return new PagedTable({
            container,
            loadPage: cursor => api.listRuns({ cursor, limit: pageSize }),
            headHtml: RunListTable.HEAD_HTML,
            rowHtml: RunListTable.rowHtml,
            rowLinks: true,
            text: {
                loading: 'Loading runs',
                loadingMore: 'Loading older runs',
                error: 'Runs could not be loaded',
                errorMore: 'Older runs could not be loaded',
                empty: 'No runs yet',
                emptyMessage: 'Sync runs in this landscape will be listed here once the first one starts.',
                more: 'Load older runs',
                noun: ['run', 'runs']
            }
        });
    }
}

/**
 * One run's page: a back link, a header with the run's status and key facts, its figures as stat tiles, a note
 * when the run failed or is incomplete, and four sections (Overview, Chunks, Errors, Changes), each loaded the
 * first time it is opened.
 */
class RunDetailView {

    constructor({ api, container, syncRunId }) {
        this.api = api;
        this.container = container;
        this.syncRunId = syncRunId;
        this.root = document.createElement('div');
        this.root.className = 'run-detail';
        this.section = 'overview';
        this.loads = 0;
        this.reset();
    }

    static get SECTIONS() {
        return [
            { id: 'overview', label: 'Overview' },
            { id: 'chunks', label: 'Chunks' },
            { id: 'errors', label: 'Errors' },
            { id: 'changes', label: 'Changes' }
        ];
    }

    static get PAGE_SIZE() {
        return 50;
    }

    reset() {
        this.run = undefined;
        this.capabilities = {};
        this.loadedSections = new Set();
        this.chunksRequest = undefined;
    }

    /** Load (or reload) the run and show its page. Resolves true once it is shown */
    async load() {
        const load = ++this.loads;
        this.reset();
        this.container.replaceChildren(this.root);
        this.root.innerHTML = `
            <a class="back-link" href="#runs"><i class="fas fa-arrow-left" aria-hidden="true"></i>Run history</a>
            <div class="run-detail-body"></div>
        `;
        const body = this.root.querySelector('.run-detail-body');
        PageState.loading(body, 'Loading the run');
        let run;
        let capabilities;
        try {
            [run, capabilities] = await Promise.all([
                this.api.run(this.syncRunId),
                this.api.capabilities().catch(() => undefined)
            ]);
        } catch (error) {
            if (load !== this.loads) {
                return false;
            }
            if (error.status === 404) {
                PageState.empty(body, 'No such run', `Nothing is recorded for a run started at ${this.syncRunId}.`);
            } else {
                PageState.error(body, 'The run could not be loaded', error.message);
            }
            return false;
        }
        if (load !== this.loads) {
            return false;
        }
        this.run = run;
        this.capabilities = capabilities || {};
        body.innerHTML = this.headerHtml() + this.tilesHtml() + this.noteHtml() + this.sectionsHtml();
        body.querySelector('[data-action="refresh"]').addEventListener('click', () => this.load());
        RefreshStamp.set(body.querySelector('.refresh-stamp'));
        body.querySelectorAll('[data-section]').forEach(tab => {
            tab.addEventListener('click', () => this.showSection(tab.dataset.section));
        });
        this.showSection(this.section);
        return true;
    }

    headerHtml() {
        const run = this.run;
        const facts = [
            run.populationLabel,
            run.storageMode ? `${RunFormat.storageMode(run.storageMode)} storage` : undefined,
            run.durationMs !== undefined ? RunFormat.duration(run.durationMs) : undefined,
            ...run.tags
        ].filter(Boolean);
        return `
            <header class="page-header">
                <div>
                    <h1 class="page-title run-title">
                        <span>Run of ${RunHtml.escape(RunFormat.dateTime(run.startedAt))}</span>
                        ${RunFormat.statusBadge(run.status)}
                    </h1>
                    <p class="page-description">${facts.map(RunHtml.escape).join(' &middot; ')}</p>
                    <p class="run-id">Run ID <code>${RunHtml.escape(run.syncRunId)}</code></p>
                </div>
                <div class="page-actions">
                    <span class="refresh-stamp"></span>
                    <button class="btn btn-sm btn-outline-secondary" type="button" data-action="refresh">
                        <i class="fas fa-rotate-right me-1"></i>Refresh
                    </button>
                </div>
            </header>
        `;
    }

    tilesHtml() {
        const run = this.run;
        const tiles = [
            ['Processed', run.processed],
            ['Added', run.added],
            ['Updated', run.updated],
            ['Deactivated', run.deactivated],
            ['Errors', run.errors]
        ];
        return `
            <section class="page-section" aria-label="Run figures">
                <div class="stat-grid">
                    ${tiles.map(([label, value]) => `
                        <div class="stat-tile">
                            <div class="stat-label">${RunHtml.escape(label)}</div>
                            <div class="stat-value">${RunFormat.number(value)}</div>
                        </div>
                    `).join('')}
                </div>
            </section>
        `;
    }

    /** What went wrong, or why the figures are missing: none for a run that succeeded */
    noteHtml() {
        const run = this.run;
        const deletion = run.deletion || {};
        let note;
        if (run.status === 'FAILED') {
            note = {
                tone: 'danger',
                title: run.failureMessage ? 'The run failed while fetching people from the source' : 'The run was marked failed',
                message: run.failureMessage || deletion.message || '',
                at: run.failedAt
            };
        } else if (run.status === 'POST_PROCESSING_FAILED') {
            note = {
                tone: 'danger',
                title: 'Post-processing failed',
                message: deletion.status === 'FAILED' ? `Deletion handling failed: ${deletion.message}` : (deletion.message || '')
            };
        } else if (run.status === 'INCOMPLETE') {
            note = {
                tone: 'warning',
                title: 'This run has no summary yet',
                message: 'It may still be running, or it may have stopped without being marked failed. Its figures appear once post-processing summarizes it. Refresh to check again.'
            };
        }
        if ( ! note) {
            return '';
        }
        const icon = note.tone === 'danger' ? 'fa-circle-exclamation' : 'fa-hourglass-half';
        return `
            <div class="run-note run-note-${note.tone}" role="note">
                <i class="fas ${icon} run-note-icon" aria-hidden="true"></i>
                <div>
                    <p class="run-note-title">${RunHtml.escape(note.title)}</p>
                    ${note.message ? `<p class="run-note-message">${RunHtml.escape(note.message)}</p>` : ''}
                    ${note.at ? `<p class="run-note-message">At ${RunHtml.escape(RunFormat.dateTime(note.at, { seconds: true }))}</p>` : ''}
                </div>
            </div>
        `;
    }

    /** The count shown on a section's tab, if any */
    sectionCount(id) {
        const run = this.run;
        if (id === 'chunks') return run.chunkCount;
        if (id === 'errors') return run.errors;
        if (id === 'changes' && this.capabilities.changesSupported !== false && run.hasSummary) {
            return (run.added || 0) + (run.updated || 0) + (run.deactivated || 0);
        }
        return undefined;
    }

    sectionsHtml() {
        const tabs = RunDetailView.SECTIONS.map(({ id, label }) => {
            const count = this.sectionCount(id);
            return `
                <button class="nav-link" type="button" role="tab" id="run-section-${id}-tab" data-section="${id}"
                        aria-controls="run-section-${id}" aria-selected="false">
                    ${RunHtml.escape(label)}${RunFormat.isNumber(count) ? ` <span class="run-section-count">${RunFormat.number(count)}</span>` : ''}
                </button>
            `;
        }).join('');
        const panels = RunDetailView.SECTIONS.map(({ id }) => `
            <div class="run-section-panel" role="tabpanel" id="run-section-${id}" aria-labelledby="run-section-${id}-tab" data-panel="${id}" hidden></div>
        `).join('');
        return `
            <section class="page-section run-detail-sections">
                <nav class="nav nav-underline run-sections" role="tablist" aria-label="Run sections">${tabs}</nav>
                ${panels}
            </section>
        `;
    }

    showSection(id) {
        this.section = id;
        this.root.querySelectorAll('[data-section]').forEach(tab => {
            const selected = tab.dataset.section === id;
            tab.classList.toggle('active', selected);
            tab.setAttribute('aria-selected', String(selected));
        });
        this.root.querySelectorAll('[data-panel]').forEach(panel => {
            panel.hidden = panel.dataset.panel !== id;
        });
        if (this.loadedSections.has(id)) {
            return;
        }
        this.loadedSections.add(id);
        const panel = this.root.querySelector(`[data-panel="${id}"]`);
        if (id === 'overview') this.renderOverview(panel);
        if (id === 'chunks') this.renderChunks(panel);
        if (id === 'errors') this.renderErrors(panel);
        if (id === 'changes') this.renderChanges(panel);
    }

    /** The run's chunk statistics, asked for once (the Chunks and Errors sections both use them) */
    chunks() {
        if ( ! this.chunksRequest) {
            this.chunksRequest = this.api.chunks(this.syncRunId).catch(error => {
                this.chunksRequest = undefined;
                throw error;
            });
        }
        return this.chunksRequest;
    }

    // Overview

    renderOverview(panel) {
        panel.innerHTML = `
            <div class="page-columns">
                <div class="page-section">
                    <div class="section-heading"><h2 class="section-title">Summary</h2></div>
                    <div class="surface">${RunDetailView.factsHtml(this.summaryFacts())}</div>
                </div>
                <div class="run-aside">
                    <div class="run-aside-section">
                        <div class="section-heading"><h2 class="section-title">Deletion</h2></div>
                        <div class="surface">${this.deletionHtml()}</div>
                    </div>
                    ${this.flagsHtml()}
                </div>
            </div>
        `;
    }

    summaryFacts() {
        const run = this.run;
        const number = RunFormat.number;
        const chunks = RunFormat.isNumber(run.chunksReported) && RunFormat.isNumber(run.chunkCount)
            ? `${number(run.chunksReported)} of ${number(run.chunkCount)} reported`
            : number(run.chunkCount);
        const facts = [
            ['Started', RunFormat.dateTime(run.startedAt, { seconds: true })],
            ['Completed', run.completedAt ? RunFormat.dateTime(run.completedAt, { seconds: true }) : 'Not recorded'],
            ['Duration', RunFormat.duration(run.durationMs)],
            ['Population', run.populationLabel],
            ['Storage mode', RunFormat.storageMode(run.storageMode || this.capabilities.storageMode)],
            ['Chunks', chunks],
            ['Records fetched', number(run.records)],
            ['Succeeded', number(run.succeeded)],
            ['Failed', number(run.failed)],
            ['Skipped', number(run.skipped)],
            ['Throttled requests', number(run.throttled)],
            ['Dry run', run.hasSummary ? RunFormat.yesNo(run.dryRun) : RunFormat.NONE],
            ['Bulk reset', RunFormat.yesNo(run.bulkReset)]
        ];
        // Without a summary most figures are unknown: list only what is known, rather than a column of dashes
        return run.hasSummary ? facts : facts.filter(([, value]) => value !== RunFormat.NONE);
    }

    static factsHtml(facts) {
        return `<dl class="fact-list">${facts.map(([term, value]) => `
            <dt>${RunHtml.escape(term)}</dt><dd>${RunHtml.escape(value)}</dd>
        `).join('')}</dl>`;
    }

    deletionHtml() {
        const deletion = this.run.deletion;
        if ( ! deletion) {
            return '<p class="fact-note">No deletion step is recorded. Post-processing has not summarized this run.</p>';
        }
        const reason = deletion.status === 'SKIPPED' && RunFormat.SKIP_REASONS[deletion.reason]
            ? ` <span class="fact-note">(${RunHtml.escape(RunFormat.SKIP_REASONS[deletion.reason])})</span>`
            : '';
        const counts = deletion.status === 'COMPLETED'
            ? RunDetailView.factsHtml([
                ['Deactivated', RunFormat.number(deletion.deletedCount)],
                ['Failed', RunFormat.number(deletion.failedCount)],
                ['Considered', RunFormat.number(deletion.totalProcessed)]
            ])
            : '';
        return `
            <p class="deletion-status">${RunFormat.deletionBadge(deletion.status)}${reason}</p>
            ${deletion.message ? `<p class="deletion-message">${RunHtml.escape(deletion.message)}</p>` : ''}
            ${counts}
        `;
    }

    /** The run's flags (recorded for a run that has no summary), less the failure, which the note shows */
    flagsHtml() {
        const flags = this.run.flags;
        if ( ! flags) {
            return '';
        }
        const labels = {
            syncPopulation: 'Population',
            bulkReset: 'Bulk reset',
            trustPreviousStorage: 'Trust previous storage',
            personRecordProcessorAddons: 'Record processor addons',
            termsEndpoint: 'Terms endpoint'
        };
        const hidden = ['runFailed', 'runFailureMessage', 'runFailureTimestamp'];
        const facts = Object.entries(flags)
            .filter(([name]) => ! hidden.includes(name))
            .map(([name, value]) => [
                labels[name] || name,
                typeof value === 'boolean' ? RunFormat.yesNo(value)
                    : name === 'syncPopulation' ? RunFormat.population(value)
                    : value !== null && typeof value === 'object' ? JSON.stringify(value)
                    : String(value)
            ]);
        if (facts.length === 0) {
            return '';
        }
        return `
            <div class="run-aside-section">
                <div class="section-heading"><h2 class="section-title">Flags</h2></div>
                <div class="surface">${RunDetailView.factsHtml(facts)}</div>
            </div>
        `;
    }

    // Chunks

    static get CHUNK_COLUMNS() {
        return [
            ['Records', 'totalRecords'],
            ['Succeeded', 'successCount'],
            ['Failed', 'failureCount'],
            ['Added', 'addedCount'],
            ['Updated', 'updatedCount'],
            ['Errors', 'totalErrors'],
            ['Throttled', 'throttleCount']
        ];
    }

    async renderChunks(panel) {
        PageState.loading(panel, 'Loading chunk statistics');
        let chunks;
        try {
            chunks = await this.chunks();
        } catch (error) {
            this.loadedSections.delete('chunks');
            PageState.error(panel, 'Chunk statistics could not be loaded', error.message);
            return;
        }
        if (chunks.length === 0) {
            PageState.empty(panel, 'No chunk statistics', this.run.status === 'INCOMPLETE'
                ? 'No processor task has reported on a chunk of this run yet.'
                : 'No processor task reported on a chunk of this run.');
            return;
        }
        const run = this.run;
        const columns = RunDetailView.CHUNK_COLUMNS;
        const sum = key => chunks.reduce((total, chunk) => total + (Number(chunk[key]) || 0), 0);
        const missing = RunFormat.isNumber(run.chunkCount) && chunks.length < run.chunkCount
            ? `<p class="section-note">${RunFormat.number(chunks.length)} of ${RunFormat.number(run.chunkCount)} chunks reported statistics. A chunk without them was not processed, or its processor task stopped before reporting.</p>`
            : '';
        const rows = chunks.map(chunk => `
            <tr>
                <td class="tabular-nums">${RunHtml.escape(chunk.chunkId || chunk.eventType)}</td>
                <td class="tabular-nums">${RunHtml.escape(RunFormat.dateTime(chunk.startTimestamp, { seconds: true }))}</td>
                <td class="num">${RunHtml.escape(RunFormat.duration(Date.parse(chunk.endTimestamp) - Date.parse(chunk.startTimestamp)))}</td>
                ${columns.map(([, key]) => `<td class="num">${RunFormat.number(chunk[key])}</td>`).join('')}
            </tr>
        `).join('');
        panel.innerHTML = `
            ${missing}
            <div class="surface surface-flush">
                <div class="table-responsive">
                    <table class="table data-table">
                        <thead><tr>
                            <th scope="col">Chunk</th>
                            <th scope="col">Started</th>
                            <th scope="col" class="num">Duration</th>
                            ${columns.map(([label]) => `<th scope="col" class="num">${label}</th>`).join('')}
                        </tr></thead>
                        <tbody>${rows}</tbody>
                        <tfoot><tr>
                            <th scope="row" colspan="3">Total</th>
                            ${columns.map(([, key]) => `<td class="num">${RunFormat.number(sum(key))}</td>`).join('')}
                        </tr></tfoot>
                    </table>
                </div>
            </div>
        `;
    }

    // Errors

    /**
     * Error counts by status code: from the summary, else summed from the chunk statistics. Null when neither
     * can be had, so the counts are unknown (not zero).
     */
    async errorGroups() {
        let byStatus = this.run.errorsByStatus;
        if ( ! byStatus) {
            byStatus = {};
            try {
                (await this.chunks()).forEach(chunk => {
                    Object.entries(chunk.errorsByStatus || {}).forEach(([code, count]) => {
                        byStatus[code] = (byStatus[code] || 0) + (Number(count) || 0);
                    });
                });
            } catch (error) {
                return null;
            }
        }
        return Object.entries(byStatus)
            .map(([code, count]) => ({ code: Number(code), count: Number(count) || 0 }))
            .filter(({ count }) => count > 0)
            .sort((a, b) => a.code - b.code);
    }

    static errorRowHtml(item) {
        const details = RunDetailView.parseJson(item.detailsObj);
        const person = details && (details.sourceIdentifier || details.hrn);
        return `
            <tr>
                <td class="tabular-nums">${RunHtml.escape(RunFormat.dateTime(item.timestamp, { seconds: true }))}</td>
                <td>${RunHtml.escape(RunFormat.httpStatus(item.statusCode))}${item.isThrottled ? ` ${RunHtml.tag('Throttled')}` : ''}</td>
                <td class="tabular-nums">${RunHtml.escape(person || '')}</td>
                <td class="wrap">${RunHtml.escape(item.message)}</td>
            </tr>
        `;
    }

    static parseJson(value) {
        if (typeof value !== 'string') {
            return value;
        }
        try {
            return JSON.parse(value);
        } catch (error) {
            return undefined;
        }
    }

    async renderErrors(panel) {
        PageState.loading(panel, 'Loading errors');
        const groups = await this.errorGroups();
        const total = RunFormat.isNumber(this.run.errors) ? this.run.errors
            : groups ? groups.reduce((sum, { count }) => sum + count, 0)
            : undefined;
        if (total === 0) {
            PageState.empty(panel, 'No errors recorded', this.run.status === 'INCOMPLETE'
                ? 'None so far. Errors are recorded as the run\'s chunks are processed.'
                : 'Every request this run made succeeded.');
            return;
        }
        const filters = [{ value: '', label: 'All', count: total }]
            .concat((groups || []).map(({ code, count }) => ({ value: String(code), label: RunFormat.httpStatus(code), count })));
        this.renderFilteredList(panel, {
            filters,
            filterLabel: 'Filter errors by status code',
            selected: '',
            createTable: (container, filter) => new PagedTable({
                container,
                total: filter.count,
                loadPage: cursor => this.api.errors(this.syncRunId, {
                    statusCode: filter.value || undefined, cursor, limit: RunDetailView.PAGE_SIZE
                }),
                headHtml: `
                    <th scope="col">Time</th>
                    <th scope="col">Status</th>
                    <th scope="col">Person</th>
                    <th scope="col">Message</th>
                `,
                rowHtml: RunDetailView.errorRowHtml,
                text: {
                    loading: 'Loading errors',
                    loadingMore: 'Loading more errors',
                    error: 'Errors could not be loaded',
                    errorMore: 'More errors could not be loaded',
                    empty: 'No errors recorded',
                    emptyMessage: filter.value ? `No ${filter.label} errors are recorded for this run.` : '',
                    more: 'Load more errors',
                    noun: ['error', 'errors']
                }
            })
        });
    }

    // Changes

    async renderChanges(panel) {
        const run = this.run;
        if (this.capabilities.changesSupported === false) {
            const mode = RunFormat.storageMode(this.capabilities.storageMode);
            PageState.empty(panel, `Changes aren't recorded in ${mode} storage mode`,
                `This landscape keeps its delta state in ${mode} files, which hold each person's latest state but no record of which people a run changed. The counts above come from the run's summary.`);
            return;
        }
        const filters = RunFormat.CHANGE_TYPES.map(({ type, label }) => ({ value: type, label, count: run.changeCount(type) }));
        const first = filters.find(({ count }) => count > 0) || filters[0];
        this.renderFilteredList(panel, {
            filters,
            filterLabel: 'Filter changes by type',
            selected: first.value,
            createTable: (container, filter) => new PagedTable({
                container,
                total: filter.count,
                loadPage: cursor => this.api.changes(this.syncRunId, {
                    changeType: filter.value, cursor, limit: RunDetailView.PAGE_SIZE
                }),
                headHtml: '<th scope="col">Person</th><th scope="col">Change</th>',
                rowHtml: item => `
                    <tr>
                        <td class="tabular-nums">${RunHtml.escape(item.personId)}</td>
                        <td>${RunHtml.escape(filter.label)}</td>
                    </tr>
                `,
                text: {
                    loading: 'Loading changes',
                    loadingMore: 'Loading more changes',
                    error: 'Changes could not be loaded',
                    errorMore: 'More changes could not be loaded',
                    empty: `No people ${filter.label.toLowerCase()}`,
                    emptyMessage: `This run ${filter.label.toLowerCase()} nobody.`,
                    more: 'Load more changes',
                    noun: ['person', 'people']
                }
            })
        });
    }

    /**
     * Filter buttons over a paged table: choosing a filter loads the table afresh for it.
     * filters: [{ value, label, count }]; createTable(container, filter) returns the filter's PagedTable.
     */
    renderFilteredList(panel, { filters, filterLabel, selected, createTable }) {
        panel.innerHTML = `
            <div class="filter-chips" role="group" aria-label="${RunHtml.escape(filterLabel)}">
                ${filters.map(({ value, label, count }) => `
                    <button type="button" class="filter-chip" data-value="${RunHtml.escape(value)}" aria-pressed="false">
                        ${RunHtml.escape(label)}${RunFormat.isNumber(count) ? ` <span class="filter-chip-count">${RunFormat.number(count)}</span>` : ''}
                    </button>
                `).join('')}
            </div>
            <div class="surface surface-flush" data-list></div>
        `;
        const container = panel.querySelector('[data-list]');
        const choose = (value) => {
            panel.querySelectorAll('.filter-chip').forEach(chip => {
                chip.setAttribute('aria-pressed', String(chip.dataset.value === value));
            });
            createTable(container, filters.find(filter => filter.value === value)).load();
        };
        panel.querySelectorAll('.filter-chip').forEach(chip => {
            chip.addEventListener('click', () => choose(chip.dataset.value));
        });
        choose(selected);
    }
}

/**
 * The Run history page: the list of runs, or one run's page when the URL fragment names one (#runs/<id>).
 * The list keeps what it has loaded while a run is open, so going back returns to the same place; Refresh
 * reloads it from the newest run.
 */
class RunHistoryPage {

    /** The fragment of a run's page. A syncRunId is an ISO timestamp, which a fragment carries as it is */
    static detailHref(syncRunId) {
        return `#runs/${syncRunId}`;
    }

    constructor({ api, root }) {
        this.api = api;
        this.listView = root.querySelector('[data-view="list"]');
        this.detailContainer = root.querySelector('[data-view="detail"]');
        this.stamp = root.querySelector('[data-refresh-stamp="runs"]');
        this.list = RunListTable.create(api, root.querySelector('[data-run-list]'));
        this.listLoaded = false;
        this.detail = undefined;
        root.querySelector('[data-action="refresh-runs"]').addEventListener('click', () => this.refreshList());
    }

    /** Show the list, or the run a syncRunId names */
    show(syncRunId) {
        return syncRunId ? this.showRun(syncRunId) : this.showList();
    }

    showList() {
        this.listView.hidden = false;
        this.detailContainer.hidden = true;
        if ( ! this.listLoaded) {
            return this.refreshList();
        }
        return Promise.resolve(true);
    }

    async refreshList() {
        this.listLoaded = true;
        const shown = await this.list.load();
        if (shown) {
            RefreshStamp.set(this.stamp);
        }
        return shown;
    }

    showRun(syncRunId) {
        this.listView.hidden = true;
        this.detailContainer.hidden = false;
        if (this.detail && this.detail.syncRunId === syncRunId) {
            return Promise.resolve(true);
        }
        this.detail = new RunDetailView({ api: this.api, container: this.detailContainer, syncRunId });
        window.scrollTo(0, 0);
        return this.detail.load();
    }
}
