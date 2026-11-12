/**
 * Bulk sync page (the "bulk" page): start a sync run in this landscape, the way its EventBridge schedule starts one
 * (GET and POST /api/bulk-sync, POST /api/bulk-sync/plan, dashboard/src/services/BulkSync.ts). The page says up
 * front whether a start is allowed now, asks for the run's options, shows what will run and the event the chunker
 * would be sent before anything is sent, and once a run has started, links to it when it appears.
 *
 *   BulkSyncView  the page's words and markup, without the DOM
 *   BulkSyncPage  loads the state, reads the form, reviews, starts, and looks for the new run
 *
 * Builds on page-state.js (Html, PageState, RefreshStamp) and run-history.js (RunFormat, RunHistoryPage), which load
 * first. Tests: test/DashboardBulkSyncPage.test.ts. Editing note: the Lambda build's regex minifier drops the rest of
 * a line after a double slash and joins lines (see run-history.js), so no string may hold one, and every statement
 * ends with a semicolon.
 */

/** The page's words and markup */
class BulkSyncView {

    /** A BUID: U followed by digits (BulkSyncRequest.BUID on the server) */
    static get BUID() {
        return /^U\d+$/;
    }

    /** The newest run's state (RunActivityState), as the status panel names it */
    static latestRunHtml(latestRun) {
        const { kind, syncRunId } = latestRun || {};
        if ( ! syncRunId || kind === 'none') {
            return '<p class="bulk-status-line">No runs yet in this landscape.</p>';
        }
        const words = { finished: 'finished', failed: 'failed', underway: 'is still underway', stalled: 'has stalled without finishing' }[kind] || kind;
        return `
            <p class="bulk-status-line">
                The latest run,
                <a class="run-link" href="${Html.escape(RunHistoryPage.detailHref(syncRunId))}">${Html.escape(RunFormat.dateTime(syncRunId))}</a>,
                ${Html.escape(words)}.
            </p>
        `;
    }

    static radioHtml({ name, value, label, hint, checked, disabled }) {
        const id = `bulk-${name}-${value}`;
        return `
            <div class="form-check">
                <input class="form-check-input" type="radio" name="${name}" id="${id}" value="${Html.escape(value)}"${checked ? ' checked' : ''}${disabled ? ' disabled' : ''}>
                <label class="form-check-label" for="${id}">${Html.escape(label)}</label>
                <div class="form-text">${Html.escape(hint)}</div>
            </div>
        `;
    }

    static checkboxHtml({ name, label, hint, checked }) {
        const id = `bulk-${name}`;
        return `
            <div class="form-check mb-3">
                <input class="form-check-input" type="checkbox" name="${name}" id="${id}"${checked ? ' checked' : ''}>
                <label class="form-check-label" for="${id}">${Html.escape(label)}</label>
                <div class="form-text">${Html.escape(hint)}</div>
            </div>
        `;
    }

    static noteHtml(tone, title, message) {
        const icon = tone === 'danger' ? 'fa-circle-exclamation' : tone === 'warning' ? 'fa-triangle-exclamation' : 'fa-circle-info';
        return `
            <div class="run-note run-note-${tone}" role="note">
                <i class="fas ${icon} run-note-icon" aria-hidden="true"></i>
                <div>
                    <p class="run-note-title">${Html.escape(title)}</p>
                    ${message ? `<p class="run-note-message">${Html.escape(message)}</p>` : ''}
                </div>
            </div>
        `;
    }

    /** The form's values: what was entered, else the landscape's defaults (BulkSyncState.defaults) */
    static formValues(state, values) {
        const { defaults } = state;
        const entered = values || {};
        const pick = (name) => entered[name] === undefined ? defaults[name] : entered[name];
        const iterationLimit = pick('iterationLimit');
        return {
            runType: entered.runType || defaults.runType,
            buid: entered.buid || '',
            bulkReset: pick('bulkReset'),
            custom: !! entered.custom,
            singleTask: iterationLimit === 0,
            chunksPerTask: iterationLimit === 0 ? defaults.iterationLimit : iterationLimit,
            parallelStart: pick('parallelStart'),
            overrideStalled: !! entered.overrideStalled
        };
    }

    /**
     * The form, from the state (BulkSyncState) and the values last entered (so a refresh keeps them). A start the
     * server would refuse now is said above the button, which is then disabled; a stalled run instead offers the
     * override. A test shows the BUID box; Custom settings shows the custom fields.
     */
    static formHtml(state, values) {
        const {
            latestRun = {}, blocked, warnings = [], iterationLimit: chunks = { max: 50 }, parallelStart: parallel = { max: 20, enabled: false }
        } = state;
        const form = BulkSyncView.formValues(state, values);
        const stalled = latestRun.kind === 'stalled';
        const refused = blocked && blocked.code !== 'RUN_STALLED' ? blocked : undefined;
        const options = (from, to, selected) => {
            const list = [];
            for (let count = from; count <= to; count++) {
                list.push(`<option value="${count}"${count === selected ? ' selected' : ''}>${count}</option>`);
            }
            return list.join('');
        };
        return `
            ${warnings.map(warning => BulkSyncView.noteHtml('info', 'Before you start', warning)).join('')}
            <form data-bulk-sync-form novalidate>
                <fieldset class="bulk-fields" data-bulk-fields>
                <fieldset class="mb-3">
                    <legend class="form-label">Run type</legend>
                    ${BulkSyncView.radioHtml({
                        name: 'runType', value: 'full', label: 'Full sync (with deactivations)', checked: form.runType === 'full',
                        hint: 'Everyone in the source. Afterward, people in Huron who are no longer in the source are deactivated.'
                    })}
                    ${BulkSyncView.radioHtml({
                        name: 'runType', value: 'full-no-deactivations', label: 'Full sync (no deactivations)', checked: form.runType === 'full-no-deactivations',
                        hint: 'Everyone in the source, but no one is deactivated, even people who are no longer in the source.'
                    })}
                    ${BulkSyncView.radioHtml({
                        name: 'runType', value: 'delta', label: 'Delta sync', disabled: true,
                        hint: 'Only the people who changed. Not available yet: a source of only the people who changed does not exist yet.'
                    })}
                    ${BulkSyncView.radioHtml({
                        name: 'runType', value: 'test', label: 'Test', checked: form.runType === 'test',
                        hint: 'One person, to check the integration end to end.'
                    })}
                </fieldset>
                <div class="bulk-test mb-3" data-bulk-test${form.runType === 'test' ? '' : ' hidden'}>
                    <p class="bulk-test-note">
                        A test puts a single person through the whole sync, from the source's person endpoint through
                        the chunker, the processor and post-processing into Huron, to check that the integration works
                        end to end. No one else is touched, and no one is deactivated.
                    </p>
                    <label class="form-label" for="bulk-buid">BUID</label>
                    <input class="form-control bulk-buid" type="text" name="buid" id="bulk-buid" value="${Html.escape(form.buid)}"
                           placeholder="U12345678" autocomplete="off" spellcheck="false">
                </div>
                ${BulkSyncView.checkboxHtml({
                    name: 'bulkReset', label: 'Bulk reset', checked: form.bulkReset,
                    hint: 'Check every person against Huron. Each time someone is synced, the integration keeps a fingerprint of their record as it came from the source. Normally, anyone whose record has not changed since then (the same fingerprint) is skipped, and Huron is not contacted for them. With a bulk reset, everyone is looked up in Huron and created or updated as needed: slower, but it also puts right records that were changed in Huron directly.'
                })}
                ${BulkSyncView.checkboxHtml({
                    name: 'custom', label: 'Custom settings', checked: form.custom,
                    hint: `How the run's work is divided up and started. Unchecked, the defaults are used: ${state.defaults.iterationLimit} chunks per task, and a start with one message.`
                })}
                <fieldset class="bulk-custom mb-3" data-bulk-custom aria-label="Custom settings"${form.custom ? '' : ' hidden'}>
                    <fieldset class="mb-3">
                        <legend class="form-label">Chunker tasks</legend>
                        <div class="form-text mt-0 mb-2">
                            The source is read a chunk of people at a time, by chunker tasks. A test reads one person, so this doesn't apply to it.
                        </div>
                        ${BulkSyncView.radioHtml({
                            name: 'chunkMode', value: 'perTask', label: 'Chunks per task', checked: ! form.singleTask,
                            hint: 'Each task reads this many chunks, then hands the rest on to a new task, so several tasks share the work.'
                        })}
                        <div class="bulk-chunks" data-bulk-chunks${form.singleTask ? ' hidden' : ''}>
                            <label class="visually-hidden" for="bulk-chunksPerTask">Chunks per task</label>
                            <select class="form-select bulk-number" name="chunksPerTask" id="bulk-chunksPerTask">
                                ${options(1, chunks.max, form.chunksPerTask)}
                            </select>
                        </div>
                        ${BulkSyncView.radioHtml({
                            name: 'chunkMode', value: 'single', label: 'Single task', checked: form.singleTask,
                            hint: 'One task reads every chunk itself. With no other task to share the work, reading the source takes much longer.'
                        })}
                    </fieldset>
                    <div>
                        <label class="form-label" for="bulk-parallelStart">Parallel start</label>
                        <select class="form-select bulk-number" name="parallelStart" id="bulk-parallelStart"${parallel.enabled ? '' : ' disabled'}>
                            ${options(0, parallel.max, form.parallelStart)}
                        </select>
                        <div class="form-text">
                            How many chunker messages start the run, and the chunker tasks to start them with (0: one message,
                            and auto-scaling adds tasks).${parallel.enabled ? '' : ' Read-only at 0 until shared queue seeding is built.'}
                        </div>
                    </div>
                </fieldset>
                ${stalled ? BulkSyncView.checkboxHtml({
                    name: 'overrideStalled', label: 'Start despite the stalled run', checked: form.overrideStalled,
                    hint: `The run of ${RunFormat.dateTime(latestRun.syncRunId)} has stalled without finishing. Check that it has stopped first.`
                }) : ''}
                </fieldset>
                <p class="bulk-form-error" data-bulk-form-error role="alert" hidden></p>
                ${refused ? BulkSyncView.noteHtml('warning', "A run can't be started now", refused.message) : ''}
                <div data-bulk-confirm>
                    <button class="btn btn-primary" type="submit"${refused ? ' disabled' : ''}>
                        <i class="fas fa-play me-2" aria-hidden="true"></i>Review run
                    </button>
                </div>
            </form>
        `;
    }

    /** What a run will do, one sentence a line, from its choices (BulkSyncChoices) */
    static summaryLines(choices, landscape, latestRun) {
        const lines = [];
        if (choices.runType === 'test') {
            lines.push(`Test of ${choices.buid} alone, into ${landscape}'s Huron. No one is deactivated.`);
        } else if (choices.runType === 'full') {
            lines.push(`Full sync of everyone in the source, into ${landscape}'s Huron. People no longer in the source are deactivated afterward.`);
        } else {
            lines.push(`Full sync of everyone in the source, into ${landscape}'s Huron. No one is deactivated.`);
        }
        lines.push(choices.bulkReset
            ? 'Bulk reset: everyone is looked up in Huron and created or updated as needed.'
            : 'People whose record has not changed since their last sync are skipped, and Huron is not contacted for them.');
        if (choices.runType !== 'test') {
            lines.push(choices.iterationLimit === 0
                ? 'A single chunker task reads the whole source.'
                : `Each chunker task reads ${choices.iterationLimit} chunks, then hands the rest on to a new task.`);
        }
        if (choices.overrideStalled && latestRun && latestRun.syncRunId) {
            lines.push(`The stalled run of ${RunFormat.dateTime(latestRun.syncRunId)} is left as it is.`);
        }
        return lines;
    }

    /** A setting's value as shown: text as it is, objects as JSON, and nothing as a dash */
    static valueText(value) {
        if (value === null || value === undefined || value === '') {
            return '—';
        }
        return typeof value === 'object' ? JSON.stringify(value) : String(value);
    }

    /** A table of rows, each { name, value } */
    static settingsTableHtml(caption, rows) {
        return `
            <div class="table-responsive bulk-settings">
                <table class="table data-table">
                    <caption>${Html.escape(caption)}</caption>
                    <thead>
                        <tr><th scope="col">Setting</th><th scope="col">Value</th></tr>
                    </thead>
                    <tbody>
                        ${rows.map(row => `
                            <tr>
                                <td><code>${Html.escape(row.name)}</code></td>
                                <td class="wrap"><code>${Html.escape(BulkSyncView.valueText(row.value))}</code></td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /** The review: what will run, and the event the chunker subscriber will receive (from BulkSyncPlan) */
    static confirmationHtml(plan, latestRun) {
        const eventRows = Object.entries(plan.event).map(([name, value]) => ({ name, value }));
        return `
            <div class="bulk-confirm" role="group" aria-label="Confirm the run">
                <p class="bulk-confirm-title">Start this run?</p>
                <ul class="bulk-confirm-list">
                    ${BulkSyncView.summaryLines(plan.choices, plan.landscape, latestRun).map(line => `<li>${Html.escape(line)}</li>`).join('')}
                </ul>
                ${BulkSyncView.settingsTableHtml('The event sent to the chunker subscriber', eventRows)}
                <p class="bulk-form-error" data-bulk-start-error role="alert" hidden></p>
                <div class="bulk-confirm-actions">
                    <button class="btn btn-primary btn-sm" type="button" data-action="confirm-start">
                        <i class="fas fa-play me-1" aria-hidden="true"></i>Start run
                    </button>
                    <button class="btn btn-outline-secondary btn-sm" type="button" data-action="cancel-start">Back</button>
                </div>
            </div>
        `;
    }

    /** A request the server would refuse as invalid, said before sending it; undefined when it is fine */
    static requestError(request) {
        if (request.runType === 'test' && ! request.buid) {
            return 'A test needs the BUID of the person to sync.';
        }
        if (request.buid && ! BulkSyncView.BUID.test(request.buid)) {
            return `A BUID is a U followed by digits (got '${request.buid}').`;
        }
        if (request.iterationLimit !== undefined && ! (Number.isInteger(request.iterationLimit) && request.iterationLimit >= 0)) {
            return 'Choose how many chunks each chunker task reads, or a single task.';
        }
        return undefined;
    }

    /** The status panel once a run has been started: when, what, and the run itself once it appears */
    static startedHtml(started, now) {
        const { result, run } = started;
        const when = `Started ${RunFormat.ago(result.startedAt, now)}`;
        const labels = { full: 'Full sync (with deactivations)', 'full-no-deactivations': 'Full sync (no deactivations)' };
        const what = result.run.buid ? `Test of ${result.run.buid}` : (labels[result.run.runType] || RunFormat.population(result.run.population));
        if (run) {
            return `
                <p class="bulk-status-line">
                    ${Html.escape(when)}:
                    <a class="run-link" href="${Html.escape(RunHistoryPage.detailHref(run.syncRunId))}">Run of ${Html.escape(RunFormat.dateTime(run.startedAt))}</a>
                    ${run.statusBadgeHtml ? run.statusBadgeHtml(now) : RunFormat.statusBadge(run.status)}
                </p>
                <p class="bulk-status-hint">${Html.escape(what)}. Follow it in Run history or on Overview.</p>
            `;
        }
        return `
            <p class="bulk-status-line">${Html.escape(when)}: ${Html.escape(what)}.</p>
            <p class="bulk-status-hint">It appears in Run history once its chunker has written its first records. Refresh to look for it.</p>
        `;
    }
}

/** The Bulk sync page */
class BulkSyncPage {

    /** How much earlier than its start a run's syncRunId may be (the chunker's clock against the Lambda's) */
    static get CLOCK_SLACK_MS() {
        return 2 * 60000;
    }

    /**
     * @param {{ getJson: function(string): Promise<any>, postJson: function(string, any): Promise<any>, runs: RunsApi,
     *   root: HTMLElement, now?: function(): number }} options
     *   getJson and postJson read and send a route's JSON (dashboard.js); runs finds the started run
     */
    constructor({ getJson, postJson, runs, root, now = () => Date.now() }) {
        this.getJson = getJson;
        this.postJson = postJson;
        this.runs = runs;
        this.now = now;
        this.formContainer = root.querySelector('[data-bulk-form]');
        this.statusContainer = root.querySelector('[data-bulk-status]');
        this.stamp = root.querySelector('[data-refresh-stamp="bulk"]');
        this.state = undefined;
        this.values = undefined;
        this.request = undefined;
        this.started = undefined;
        this.loads = 0;
        root.querySelector('[data-action="refresh-bulk"]').addEventListener('click', () => this.load());
        this.formContainer.addEventListener('submit', event => {
            event.preventDefault();
            this.review(BulkSyncPage.readRequest(event.target));
        });
        this.formContainer.addEventListener('change', event => this.onChange(event));
        this.formContainer.addEventListener('click', event => {
            const action = event.target.closest && event.target.closest('[data-action]');
            if (action && action.dataset.action === 'confirm-start') this.confirm();
            if (action && action.dataset.action === 'cancel-start') this.cancel();
        });
    }

    /**
     * The request a form's fields make. A BUID is sent only for a test, and the custom settings only when Custom
     * settings is checked (otherwise the server uses its defaults). A single task is iterationLimit 0. custom says
     * which, for redrawing the form.
     */
    static readRequest(form) {
        const fields = form.elements;
        const checked = name => ([...fields[name]].find(radio => radio.checked) || {}).value;
        const runType = checked('runType');
        const buid = String(fields.buid.value || '').trim().toUpperCase();
        const custom = !! fields.custom.checked;
        return {
            runType,
            ...(runType === 'test' ? { buid } : {}),
            bulkReset: !! fields.bulkReset.checked,
            ...(custom ? {
                iterationLimit: checked('chunkMode') === 'single' ? 0 : Number(fields.chunksPerTask.value),
                parallelStart: Number(fields.parallelStart.value)
            } : {}),
            ...(fields.overrideStalled ? { overrideStalled: !! fields.overrideStalled.checked } : {}),
            custom
        };
    }

    /** Show the BUID box for a test, the custom fields while Custom settings is checked, and the chunks per task for that choice */
    onChange(event) {
        const form = event.target.form;
        if ( ! form) {
            return;
        }
        if (event.target.name === 'runType') {
            const test = form.querySelector('[data-bulk-test]');
            if (test) test.hidden = event.target.value !== 'test';
        }
        if (event.target.name === 'chunkMode') {
            const chunks = form.querySelector('[data-bulk-chunks]');
            if (chunks) chunks.hidden = event.target.value !== 'perTask';
        }
        if (event.target.name === 'custom') {
            const custom = form.querySelector('[data-bulk-custom]');
            if (custom) custom.hidden = ! event.target.checked;
        }
    }

    /** Load what a start would do, and, after a start, look for the run it started. Resolves true once shown */
    async load() {
        const load = ++this.loads;
        PageState.loading(this.formContainer, 'Checking whether a run can start');
        if ( ! this.started) {
            PageState.loading(this.statusContainer, 'Reading the latest run', { compact: true });
        }
        let state;
        try {
            state = await this.getJson('/api/bulk-sync');
        } catch (error) {
            if (load !== this.loads) {
                return false;
            }
            PageState.error(this.formContainer, 'Bulk sync could not be read', error.message);
            PageState.error(this.statusContainer, 'The latest run could not be read', undefined, { compact: true });
            return false;
        }
        if (this.started && ! this.started.run) {
            await this.findStartedRun();
        }
        if (load !== this.loads) {
            return false;
        }
        this.state = state;
        this.request = undefined;
        this.render();
        RefreshStamp.set(this.stamp);
        return true;
    }

    render() {
        this.formContainer.innerHTML = BulkSyncView.formHtml(this.state, this.values);
        this.statusContainer.innerHTML = this.started
            ? BulkSyncView.startedHtml(this.started, this.now())
            : BulkSyncView.latestRunHtml(this.state.latestRun);
    }

    /** Say a problem with the form under its fields, or clear it */
    showFormError(problem) {
        const error = this.formContainer.querySelector('[data-bulk-form-error]');
        if (error) {
            error.textContent = problem || '';
            error.hidden = ! problem;
        }
    }

    /** Lock or unlock the form's fields and its Review button */
    lock(locked) {
        const fields = this.formContainer.querySelector('[data-bulk-fields]');
        if (fields) {
            fields.disabled = locked;
        }
        const button = this.formContainer.querySelector('[data-bulk-confirm] button[type="submit"]');
        if (button) {
            button.disabled = locked;
        }
    }

    /**
     * Check a request here, then ask the server what it would start (POST /api/bulk-sync/plan, which applies the
     * same checks as a start), and show that with Start and Back. Resolves true once the review shows
     */
    async review(request) {
        this.values = request;
        const problem = BulkSyncView.requestError(request);
        if (problem) {
            this.showFormError(problem);
            return false;
        }
        this.showFormError(undefined);
        const { custom, ...body } = request;
        // The fields are locked while the review is fetched and shown, so what is started is what was reviewed; Back unlocks them
        this.lock(true);
        let plan;
        try {
            plan = await this.postJson('/api/bulk-sync/plan', body);
        } catch (error) {
            this.lock(false);
            this.showFormError(`The run can't be started: ${error.message}`);
            return false;
        }
        this.request = body;
        this.plan = plan;
        this.formContainer.querySelector('[data-bulk-confirm]').innerHTML =
            BulkSyncView.confirmationHtml(plan, this.state.latestRun);
        return true;
    }

    /** Back to the form, keeping what was entered */
    cancel() {
        this.request = undefined;
        this.plan = undefined;
        this.render();
    }

    /** Start the reviewed run. Resolves true once started; a refusal is shown under the review */
    async confirm() {
        if ( ! this.request) {
            return false;
        }
        const buttons = [...this.formContainer.querySelectorAll('[data-bulk-confirm] button')];
        buttons.forEach(button => { button.disabled = true; });
        try {
            const result = await this.postJson('/api/bulk-sync', this.request);
            this.started = { result };
            this.values = undefined;
            this.plan = undefined;
            await this.load();
            return true;
        } catch (error) {
            buttons.forEach(button => { button.disabled = false; });
            const message = this.formContainer.querySelector('[data-bulk-start-error]');
            if (message) {
                message.textContent = `The run wasn't started: ${error.message}`;
                message.hidden = false;
            }
            return false;
        }
    }

    /** The newest run, once it is one this page started (its syncRunId no earlier than the start, give or take the clocks) */
    async findStartedRun() {
        try {
            const { items } = await this.runs.listRuns({ limit: 1 });
            const newest = items[0];
            const earliest = new Date(Date.parse(this.started.result.startedAt) - BulkSyncPage.CLOCK_SLACK_MS).toISOString();
            if (newest && newest.syncRunId >= earliest) {
                this.started.run = newest;
            }
        } catch (error) {
            // Not found yet is the same as not listed: the next Refresh looks again
        }
    }
}
