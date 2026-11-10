/**
 * Run history page (the "runs" page): this landscape's sync runs, newest first, and one run's own page
 * (#runs/<syncRunId>) with its facts, its figures, and its Overview, Chunks, Errors and Changes sections.
 *
 * Built from small classes, each testable on its own (test/DashboardRunHistory.test.ts). Markup goes through Html
 * (page-state.js, loaded first):
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
        return Html.badge(label, tone);
    }

    static deletionBadge(status) {
        const { label, tone } = RunFormat.DELETION_STATUSES[status] || { label: status || 'Unknown', tone: null };
        return Html.badge(label, tone);
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

    /** How long ago a time was, as a person would say it: just now, 5 min ago, 3 h ago, 2 days ago */
    static ago(iso, now = Date.now()) {
        const then = Date.parse(iso);
        if ( ! Number.isFinite(then)) {
            return RunFormat.NONE;
        }
        return now - then < 60000 ? 'just now' : `${RunFormat.span(now - then)} ago`;
    }

    /** A length of time as a person would say it, to its largest whole unit: 5 min, 3 h, 1 day, 3 days */
    static span(ms) {
        const minutes = Math.floor(ms / 60000);
        if (minutes < 1) {
            return 'under a minute';
        }
        if (minutes < 60) {
            return `${minutes} min`;
        }
        const hours = Math.floor(minutes / 60);
        if (hours < 24) {
            return `${hours} h`;
        }
        const days = Math.floor(hours / 24);
        return days === 1 ? '1 day' : `${days} days`;
    }

    /** A time of day ("7:45 PM"), with its date when it isn't today */
    static clockTime(ms, now = Date.now()) {
        const date = new Date(ms);
        if (Number.isNaN(date.getTime())) {
            return RunFormat.NONE;
        }
        return date.toDateString() === new Date(now).toDateString()
            ? date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })
            : RunFormat.dateTime(date.toISOString());
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
 * the run's metadata, flags and terminal error say (a run that failed early, or is incomplete), with the
 * figures so far that the report's progress sums from the chunks processed (soFar).
 */
class RunView {

    constructor(report) {
        const item = report || {};
        const summary = item.summary;
        const progress = summary ? undefined : item.progress;
        const totals = summary || progress;
        const metadata = item.metadata || {};
        const flags = item.flags || {};
        const results = (totals && totals.results) || {};
        const first = RunView.first;

        this.syncRunId = item.syncRunId;
        this.status = item.status || 'UNKNOWN';
        this.hasSummary = !!summary;
        this.soFar = !!progress;
        this.flags = item.flags;
        this.terminalError = item.terminalError;

        this.startedAt = first(summary && summary.createdAt, metadata.createdAt, item.syncRunId);
        this.completedAt = summary ? summary.completedAt : undefined;
        this.lastChunkAt = progress ? progress.lastChunkAt : undefined;
        // An unfinished run's duration runs to its last chunk, not to now, which would keep growing for a run that has died
        const started = Date.parse(this.startedAt);
        const ended = Date.parse(summary ? this.completedAt : this.lastChunkAt);
        this.durationMs = Number.isFinite(started) && Number.isFinite(ended) ? ended - started : undefined;

        this.population = first(summary && summary.syncPopulation, metadata.syncPopulation, flags.syncPopulation);
        this.buid = RunView.buidFrom(metadata.source);
        this.storageMode = summary ? summary.storageMode : undefined;
        this.dryRun = !!(summary && summary.dryRun);
        this.bulkReset = !!((summary && summary.bulkReset) || metadata.bulkReset || flags.bulkReset);

        this.chunkCount = first(summary && summary.chunkCount, metadata.chunkCount);
        // The chunker has found the end of the source records, so the records fetched won't grow
        this.chunkingFinished = metadata.partialOrEmptyChunkEncountered === true;
        this.chunksReported = totals ? totals.chunksReported : undefined;
        this.records = first(summary && summary.totalRecords, metadata.totalRecords);
        this.processed = results.processed;
        this.succeeded = results.succeeded;
        this.failed = results.failed;
        this.skipped = results.skipped;
        this.added = results.added;
        this.updated = results.updated;

        this.deletion = summary ? summary.deletion : undefined;
        const deactivatedByDeletion = this.deletion && this.deletion.status === 'COMPLETED' ? (this.deletion.deletedCount || 0) : 0;
        this.deactivated = totals ? (results.removed || 0) + deactivatedByDeletion : undefined;

        this.errors = totals ? totals.totalErrors : undefined;
        this.throttled = totals ? totals.throttleCount : undefined;
        this.errorsByStatus = totals ? totals.errorsByStatus : undefined;
        this.failureMessage = first(item.terminalError && item.terminalError.errorMessage, flags.runFailureMessage);
        this.failedAt = first(item.terminalError && item.terminalError.errorTimestamp, flags.runFailureTimestamp);
    }

    /** "Person U12345678" for a single-person run whose source names the person, else the population */
    get populationLabel() {
        return this.buid ? `Person ${this.buid}` : RunFormat.population(this.population);
    }

    /** The run's key facts, in the order a header lists them: population, storage mode, duration, markers */
    get facts() {
        return [
            this.populationLabel,
            this.storageMode ? `${RunFormat.storageMode(this.storageMode)} storage` : undefined,
            this.durationMs !== undefined ? this.durationText : undefined,
            ...this.tags
        ].filter(Boolean);
    }

    /** The duration as the page shows it: "47 min", or "47 min so far" for an unfinished run */
    get durationText() {
        const duration = RunFormat.duration(this.durationMs);
        return this.soFar && duration !== RunFormat.NONE ? `${duration} so far` : duration;
    }

    /**
     * Why an unfinished run's Deactivated figure will grow: a full sync deactivates the people missing from the
     * source in post-processing, which hasn't run yet. None once the run is summarized, for a delta sync (no
     * deletion step), or for a failed run (whose post-processing skips deletions).
     */
    get deactivationNote() {
        return this.soFar && this.status === 'INCOMPLETE' && this.population === 'person-full'
            ? 'Post-processing adds deletions'
            : undefined;
    }

    /** A run is stalled when no chunk has finished for this many times its average gap between chunks... */
    static get STALL_FACTOR() {
        return 10;
    }

    /** ...and for at least this long */
    static get MIN_STALL_MS() {
        return 15 * 60000;
    }

    /**
     * How much of an unfinished run is done: processed so far as a percent of the records fetched, rounded down to
     * one decimal (so it reads 100.0 only when every fetched record is processed) and capped at 100. Its state says
     * how to read it: done (every fetched record processed, and chunking has finished), underway, fetching (the
     * chunker hasn't found the end of the source records, so the records fetched are still growing) or failed (a
     * run marked failed, which won't go on). Undefined for a summarized run, or one without a records figure.
     */
    get completion() {
        if ( ! this.soFar || ! RunFormat.isNumber(this.records) || Number(this.records) <= 0 || ! RunFormat.isNumber(this.processed)) {
            return undefined;
        }
        const records = Number(this.records);
        const processed = Math.min(Number(this.processed), records);
        // Whole tenths of a percent, rounded down: integer arithmetic, so no float can round up to 100.0
        const tenths = Math.floor(processed * 1000 / records);
        let state = 'underway';
        if (this.status === 'FAILED') state = 'failed';
        else if ( ! this.chunkingFinished) state = 'fetching';
        else if (tenths === 1000) state = 'done';
        return { percent: tenths / 10, text: (tenths / 10).toFixed(1), processed, records, state };
    }

    /**
     * When an unfinished run's processing should finish, or that it has stalled. The rate is the run's so far
     * (processed ÷ time from its start to its last chunk), applied to the records still to process, from the last
     * chunk's end. Stalled when no chunk has finished for STALL_FACTOR times the average gap between chunks, and at
     * least MIN_STALL_MS. Neither for a run that is done or failed, or before any chunk has finished; while chunking
     * continues a run can stall, but has no estimate, since its total isn't known yet.
     * @returns {{ kind: 'estimate', at: number, inMs: number } | { kind: 'stalled', sinceMs: number } | undefined}
     */
    estimate(now = Date.now()) {
        const completion = this.completion;
        if ( ! completion || ! ['underway', 'fetching'].includes(completion.state) || ! this.lastChunkAt) {
            return undefined;
        }
        const started = Date.parse(this.startedAt);
        const last = Date.parse(this.lastChunkAt);
        const chunks = Number(this.chunksReported);
        if ( ! Number.isFinite(started) || ! Number.isFinite(last) || last <= started || ! (chunks > 0) || ! (completion.processed > 0)) {
            return undefined;
        }
        const elapsed = last - started;
        const sinceLast = now - last;
        if (sinceLast > Math.max(RunView.MIN_STALL_MS, RunView.STALL_FACTOR * elapsed / chunks)) {
            return { kind: 'stalled', sinceMs: sinceLast };
        }
        if (completion.state === 'fetching') {
            return undefined;
        }
        const at = last + (completion.records - completion.processed) * elapsed / completion.processed;
        return { kind: 'estimate', at, inMs: Math.max(at - now, 0) };
    }

    /**
     * How far an unfinished run has got, as the parts of its progress line, each { text, kind? }: its chunks, when
     * its last chunk finished (or that it has stalled), and when its processing should finish (or why there is no
     * estimate). kind marks a part shown differently: 'stalled' or 'estimate'.
     */
    progressParts(now = Date.now()) {
        if ( ! this.soFar) {
            return undefined;
        }
        const reported = RunFormat.number(this.chunksReported);
        const parts = [{
            text: RunFormat.isNumber(this.chunkCount)
                ? `${reported} of ${RunFormat.number(this.chunkCount)} chunks processed`
                : `${reported} ${Number(this.chunksReported) === 1 ? 'chunk' : 'chunks'} processed`
        }];
        const estimate = this.estimate(now);
        if (estimate && estimate.kind === 'stalled') {
            parts.push({ text: `Stalled: no chunk has finished for ${RunFormat.span(estimate.sinceMs)}`, kind: 'stalled' });
        } else {
            parts.push({ text: this.lastChunkAt ? `last chunk finished ${RunFormat.ago(this.lastChunkAt, now)}` : 'no chunk has finished yet' });
        }
        const completion = this.completion;
        if (estimate && estimate.kind === 'estimate') {
            parts.push({
                text: `processing should finish around ${RunFormat.clockTime(estimate.at, now)} (in about ${RunFormat.duration(estimate.inMs)})`,
                kind: 'estimate'
            });
        } else if (completion && completion.state === 'fetching' && ! estimate) {
            parts.push({ text: 'still fetching: no estimate until every record is fetched' });
        } else if (completion && completion.state === 'done') {
            parts.push({ text: 'post-processing comes next' });
        }
        return parts;
    }

    /** The progress line as text: "12 of 20 chunks processed · last chunk finished 5 min ago · ..." */
    progressText(now = Date.now()) {
        const parts = this.progressParts(now);
        return parts ? parts.map(part => part.text).join(' · ') : undefined;
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

    /**
     * One run's row. An unfinished run's figures are those so far: shown muted, and said so beside its status.
     * Its Records come from the run's metadata, so they aren't marked.
     */
    static rowHtml(run) {
        const href = Html.escape(RunHistoryPage.detailHref(run.syncRunId));
        const tags = run.tags.map(Html.tag).join(' ');
        const number = RunFormat.number;
        const figure = run.soFar ? 'num so-far' : 'num';
        // Beside the status: an unfinished run's percent done, or "so far" when it has no records figure
        const completion = run.completion;
        const title = Html.escape(`Summed from the chunks processed so far: ${run.progressText()}`);
        let soFar = '';
        if (completion) {
            soFar = ` <span class="completion-note completion-${completion.state}" title="${title}">${completion.text}%</span>`;
        } else if (run.soFar) {
            soFar = ` <span class="so-far-note" title="${title}">so far</span>`;
        }
        return `
            <tr data-href="${href}">
                <td><a class="run-link tabular-nums" href="${href}">${Html.escape(RunFormat.dateTime(run.startedAt))}</a></td>
                <td>${RunFormat.statusBadge(run.status)}${soFar}</td>
                <td>${Html.escape(run.populationLabel)}${tags ? ` ${tags}` : ''}</td>
                <td class="num">${number(run.records)}</td>
                <td class="${figure}">${number(run.added)}</td>
                <td class="${figure}">${number(run.updated)}</td>
                <td class="${figure}">${number(run.deactivated)}</td>
                <td class="${figure}">${number(run.errors)}</td>
                <td class="${figure}">${Html.escape(RunFormat.duration(run.durationMs))}</td>
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
        body.innerHTML = this.headerHtml() + RunDetailView.tilesHtml(run) + RunDetailView.noteHtml(run) + this.sectionsHtml();
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
        return `
            <header class="page-header">
                <div>
                    <h1 class="page-title run-title">
                        <span>Run of ${Html.escape(RunFormat.dateTime(run.startedAt))}</span>
                        ${RunFormat.statusBadge(run.status)}
                    </h1>
                    <p class="page-description">${run.facts.map(Html.escape).join(' &middot; ')}</p>
                    <p class="run-id">Run ID <code>${Html.escape(run.syncRunId)}</code></p>
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

    /**
     * A run's figures as stat tiles (also the Overview page's, for the latest run). An unfinished run's tiles
     * are labeled "so far", and followed by how far it has got. now is the clock "ago" times are taken from.
     */
    static tilesHtml(run, now = Date.now()) {
        const tiles = [
            ['Processed', run.processed, undefined, RunDetailView.completionHtml(run)],
            ['Added', run.added],
            ['Updated', run.updated],
            ['Deactivated', run.deactivated, run.deactivationNote],
            ['Errors', run.errors]
        ];
        const suffix = run.soFar ? ' so far' : '';
        const progress = RunDetailView.progressHtml(run, now);
        return `
            <section class="page-section" aria-label="Run figures">
                <div class="stat-grid">
                    ${tiles.map(([label, value, note, extra]) => `
                        <div class="stat-tile${run.soFar ? ' stat-tile-so-far' : ''}">
                            <div class="stat-label">${Html.escape(label + suffix)}</div>
                            <div class="stat-value">${RunFormat.number(value)}</div>
                            ${extra || ''}
                            ${note ? `<div class="stat-note">${Html.escape(note)}</div>` : ''}
                        </div>
                    `).join('')}
                </div>
                ${progress}
            </section>
        `;
    }

    /**
     * An unfinished run's percent done, for its Processed tile: the percent, of how many records fetched ("so
     * far" while chunking continues), and a bar, in the color of its state. None without a percent.
     */
    static completionHtml(run) {
        const completion = run.completion;
        if ( ! completion) {
            return '';
        }
        const fetched = `of ${RunFormat.number(completion.records)} fetched${completion.state === 'fetching' ? ' so far' : ''}`;
        return `
            <div class="stat-completion completion-${completion.state}">
                <div class="stat-completion-text">
                    <span class="stat-completion-percent">${completion.text}%</span>
                    <span class="stat-completion-of">${Html.escape(fetched)}</span>
                </div>
                <div class="stat-completion-bar" role="progressbar" aria-label="Percent of fetched records processed"
                     aria-valuemin="0" aria-valuemax="100" aria-valuenow="${completion.text}">
                    <span style="width: ${completion.text}%"></span>
                </div>
            </div>
        `;
    }

    /** The progress line under an unfinished run's tiles, its stalled notice and estimate marked; none when summarized */
    static progressHtml(run, now = Date.now()) {
        const parts = run.progressParts(now);
        if ( ! parts) {
            return '';
        }
        const icons = { stalled: 'fa-triangle-exclamation', estimate: 'fa-clock' };
        const html = parts.map(({ text, kind }) => kind
            ? `<span class="progress-${kind}"><i class="fas ${icons[kind]} me-1" aria-hidden="true"></i>${Html.escape(text)}</span>`
            : Html.escape(text));
        return `<p class="stat-progress">${html.join(' · ')}</p>`;
    }

    /** What went wrong, or why the figures are missing: none for a run that succeeded (also the Overview page's) */
    static noteHtml(run) {
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
                message: run.soFar
                    ? 'Its figures are summed from the chunks processed so far. It may still be running, or it may have stopped without being marked failed. The final figures appear once post-processing summarizes it. Refresh to check again.'
                    : 'It may still be running, or it may have stopped without being marked failed. Its figures appear once post-processing summarizes it. Refresh to check again.'
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
                    <p class="run-note-title">${Html.escape(note.title)}</p>
                    ${note.message ? `<p class="run-note-message">${Html.escape(note.message)}</p>` : ''}
                    ${note.at ? `<p class="run-note-message">At ${Html.escape(RunFormat.dateTime(note.at, { seconds: true }))}</p>` : ''}
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
                    ${Html.escape(label)}${RunFormat.isNumber(count) ? ` <span class="run-section-count">${RunFormat.number(count)}</span>` : ''}
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
            ? `${number(run.chunksReported)} of ${number(run.chunkCount)} ${run.soFar ? 'processed' : 'reported'}`
            : number(run.chunkCount);
        const facts = [
            ['Started', RunFormat.dateTime(run.startedAt, { seconds: true })],
            ['Completed', run.completedAt ? RunFormat.dateTime(run.completedAt, { seconds: true }) : 'Not recorded'],
            ...(run.soFar ? [['Last chunk finished', run.lastChunkAt ? RunFormat.dateTime(run.lastChunkAt, { seconds: true }) : RunFormat.NONE]] : []),
            ['Duration', run.durationText],
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
            <dt>${Html.escape(term)}</dt><dd>${Html.escape(value)}</dd>
        `).join('')}</dl>`;
    }

    deletionHtml() {
        const deletion = this.run.deletion;
        if ( ! deletion) {
            return '<p class="fact-note">No deletion step is recorded. Post-processing has not summarized this run.</p>';
        }
        const reason = deletion.status === 'SKIPPED' && RunFormat.SKIP_REASONS[deletion.reason]
            ? ` <span class="fact-note">(${Html.escape(RunFormat.SKIP_REASONS[deletion.reason])})</span>`
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
            ${deletion.message ? `<p class="deletion-message">${Html.escape(deletion.message)}</p>` : ''}
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
                <td class="tabular-nums">${Html.escape(chunk.chunkId || chunk.eventType)}</td>
                <td class="tabular-nums">${Html.escape(RunFormat.dateTime(chunk.startTimestamp, { seconds: true }))}</td>
                <td class="num">${Html.escape(RunFormat.duration(Date.parse(chunk.endTimestamp) - Date.parse(chunk.startTimestamp)))}</td>
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
                <td class="tabular-nums">${Html.escape(RunFormat.dateTime(item.timestamp, { seconds: true }))}</td>
                <td>${Html.escape(RunFormat.httpStatus(item.statusCode))}${item.isThrottled ? ` ${Html.tag('Throttled')}` : ''}</td>
                <td class="tabular-nums">${Html.escape(person || '')}</td>
                <td class="wrap">${Html.escape(item.message)}</td>
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
                        <td class="tabular-nums">${Html.escape(item.personId)}</td>
                        <td>${Html.escape(filter.label)}</td>
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
            <div class="filter-chips" role="group" aria-label="${Html.escape(filterLabel)}">
                ${filters.map(({ value, label, count }) => `
                    <button type="button" class="filter-chip" data-value="${Html.escape(value)}" aria-pressed="false">
                        ${Html.escape(label)}${RunFormat.isNumber(count) ? ` <span class="filter-chip-count">${RunFormat.number(count)}</span>` : ''}
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
