/**
 * Bulk sync schedule page (the "schedule" page): this landscape's recurring bulk sync, EventBridge Scheduler's
 * chunker-api-schedule-<landscape> (GET, PUT and DELETE /api/bulk-schedule; POST /api/bulk-schedule/when, /plan and
 * /state; dashboard/src/services/BulkSchedule.ts). With a schedule, the page shows it (when, the run it starts, on or
 * off, who changed it last, the last scheduled run skipped) with Activate or Deactivate, Edit schedule and Delete
 * schedule. Without one, it shows Bulk sync's form, with When first, and Review run; the review asks "Schedule this
 * run?" and Schedule run saves it.
 *
 * When is gathered as choices (a time and days, or a cron expression) and sent to the server, which builds and checks
 * the cron and answers with the expression, its words, the next runs, any problem by field, and any other landscape's
 * schedule it would start too close to (POST /api/bulk-schedule/when). The page never builds or parses cron.
 *
 *   BulkScheduleView  the page's words and markup, without the DOM
 *   BulkSchedulePage  loads the schedule, shows the card or the form, previews When, reviews, saves, turns it on or
 *                     off, and deletes it
 *
 * Builds on page-state.js (Html, PageState, RefreshStamp), run-history.js (RunFormat) and bulk-sync.js (BulkSyncView's
 * form and review in schedule mode, BulkSyncPage's form reading), which load first. Tests:
 * test/DashboardBulkSchedulePage.test.ts. Editing note: the Lambda build's regex minifier drops the rest of a line
 * after a double slash and joins lines (see run-history.js), so no string may hold one, and every statement ends with
 * a semicolon.
 */

/** The page's words and markup */
class BulkScheduleView {

    /** The days of the week, as AWS cron names them, with their labels */
    static get DAYS() {
        return [['SUN', 'Sun'], ['MON', 'Mon'], ['TUE', 'Tue'], ['WED', 'Wed'], ['THU', 'Thu'], ['FRI', 'Fri'], ['SAT', 'Sat']];
    }

    /** The named sets of days the simple form offers, and chosen days */
    static get DAY_SETS() {
        return [['every-day', 'Every day'], ['weekdays', 'Weekdays (Monday to Friday)'], ['weekends', 'Weekends'], ['chosen', 'Chosen days']];
    }

    /**
     * Bulk sync's form markup for a schedule's run, its ids and label targets renamed (bulk- to schedule-), so they
     * don't clash with Bulk sync's own form elsewhere on the page
     */
    static runFormHtml(form, values, layout) {
        const html = BulkSyncView.formHtml(form, values, Object.assign({ schedule: true }, layout));
        return html.replace(/(id|for)="bulk-/g, '$1="schedule-');
    }

    /**
     * The When fieldset, from choices (ScheduleChoices) and the state's when (time zone, spacing): Simple (a time of day
     * and days) or Cron expression, and the preview the server answers with ([data-schedule-preview])
     */
    static whenHtml(choices, when) {
        const simple = ! choices || choices.mode !== 'cron';
        const days = simple && choices ? choices.days : 'every-day';
        const named = typeof days === 'string' ? days : 'chosen';
        const chosen = Array.isArray(days) ? days : [];
        const time = simple && choices && choices.time ? choices.time : '04:00';
        const expression = choices && choices.mode === 'cron' ? choices.expression : '';
        const zone = when && when.timezone ? when.timezone : 'America/New_York';
        const spacing = when && when.minSpacingMinutes ? when.minSpacingMinutes : 60;
        const radio = (value, label) => `
            <div class="form-check form-check-inline">
                <input class="form-check-input" type="radio" name="whenMode" id="schedule-whenMode-${value}" value="${value}"${(value === 'cron') === ! simple ? ' checked' : ''}>
                <label class="form-check-label" for="schedule-whenMode-${value}">${Html.escape(label)}</label>
            </div>
        `;
        return `
            <fieldset class="mb-3 schedule-when" data-schedule-when>
                <legend class="form-label">When</legend>
                <div class="form-text mt-0 mb-2">
                    Times are ${Html.escape(zone)} time. A bulk sync schedule runs at most once a day. Schedules of
                    landscapes that read the real source start at least ${Html.escape(String(spacing))} minutes apart,
                    since every landscape reads the same source.
                </div>
                <div class="mb-2">
                    ${radio('simple', 'A time and days')}
                    ${radio('cron', 'Cron expression')}
                </div>
                <div class="schedule-when-simple" data-when-simple${simple ? '' : ' hidden'}>
                    <div class="mb-2">
                        <label class="form-label" for="schedule-whenTime">Time of day</label>
                        <input class="form-control bulk-number" type="time" step="60" name="whenTime" id="schedule-whenTime" value="${Html.escape(time)}">
                    </div>
                    <div class="mb-2">
                        <label class="form-label" for="schedule-whenDays">Days</label>
                        <select class="form-select schedule-days-select" name="whenDays" id="schedule-whenDays">
                            ${BulkScheduleView.DAY_SETS.map(([value, label]) => `<option value="${value}"${value === named ? ' selected' : ''}>${Html.escape(label)}</option>`).join('')}
                        </select>
                    </div>
                    <div class="schedule-days mb-2" data-when-chosen${named === 'chosen' ? '' : ' hidden'}>
                        ${BulkScheduleView.DAYS.map(([value, label]) => `
                            <div class="form-check form-check-inline">
                                <input class="form-check-input" type="checkbox" name="whenDay" id="schedule-whenDay-${value}" value="${value}"${chosen.includes(value) ? ' checked' : ''}>
                                <label class="form-check-label" for="schedule-whenDay-${value}">${label}</label>
                            </div>
                        `).join('')}
                    </div>
                </div>
                <div class="schedule-when-cron" data-when-cron${simple ? ' hidden' : ''}>
                    <label class="form-label" for="schedule-whenExpression">Cron expression</label>
                    <input class="form-control schedule-cron" type="text" name="whenExpression" id="schedule-whenExpression" value="${Html.escape(expression)}"
                           placeholder="cron(0 4 ? * MON-FRI *)" autocomplete="off" spellcheck="false">
                    <div class="form-text">
                        AWS cron: minutes, hours, day of month, month, day of week, year. Day of week counts from Sunday as 1,
                        and one of day of month and day of week is ?. One minute and one hour: at most once a day.
                    </div>
                </div>
                <div class="schedule-preview" data-schedule-preview aria-live="polite"></div>
            </fieldset>
        `;
    }

    /** Whether the schedule is to be on when saved */
    static enabledHtml(enabled) {
        return BulkSyncView.checkboxHtml({
            name: 'scheduleEnabled', label: 'Turn the schedule on', checked: enabled,
            hint: 'Off, the schedule is kept but starts nothing until it is turned on.'
        });
    }

    /**
     * What the server answered for When (SchedulePreview): the problems, or the words, the expression, the next runs, a
     * schedule it would start too close to, and notes (daylight saving, an expression that never runs)
     */
    static previewHtml(preview) {
        if ( ! preview) {
            return '';
        }
        if (preview.errors && preview.errors.length) {
            const problems = preview.errors.map(({ field, message }) => (field ? `${field}: ${message}` : message));
            return BulkSyncView.noteHtml('danger', 'Not a schedule yet', problems.join('; '));
        }
        const next = preview.nextRuns && preview.nextRuns.length
            ? `Next: ${preview.nextRuns.join('; ')}.`
            : 'It never runs.';
        return `
            <p class="schedule-preview-line"><strong>${Html.escape(preview.description || '')}</strong> <code>${Html.escape(preview.expression || '')}</code></p>
            <p class="schedule-preview-line">${Html.escape(next)}</p>
            ${preview.conflict ? BulkSyncView.noteHtml('warning', `Too close to ${preview.conflict.landscape}'s schedule`, BulkScheduleView.conflictText(preview.conflict)) : ''}
            ${(preview.notes || []).map(note => BulkSyncView.noteHtml('info', 'Daylight saving', note)).join('')}
        `;
    }

    /** A schedule too close to another landscape's, in words (ScheduleConflict) */
    static conflictText(conflict) {
        const { landscape, description, at, otherAt, minutesApart } = conflict;
        return `${landscape}'s schedule (${description}) starts at ${otherAt}, ${minutesApart} minutes from this one's run at ${at}. Choose a time at least an hour from it, or leave this schedule off.`;
    }

    /** The review's When: the next runs and notes, and whether it will be on */
    static reviewWhenHtml(preview, enabled) {
        const next = preview.nextRuns && preview.nextRuns.length ? preview.nextRuns.join('; ') : 'none';
        return `
            <p class="schedule-preview-line"><code>${Html.escape(preview.expression || '')}</code>. Next runs: ${Html.escape(next)}.</p>
            ${(preview.notes || []).map(note => BulkSyncView.noteHtml('info', 'Daylight saving', note)).join('')}
            ${enabled ? '' : BulkSyncView.noteHtml('info', 'Saved off', 'The schedule is kept but starts nothing until it is turned on.')}
        `;
    }

    /**
     * A schedule's run as BulkSyncChoices for summaryLines(), from its form values (BulkScheduleView.run): in a mock
     * landscape a run without a source reads the real one
     */
    static runChoices(run, mockLandscape) {
        const choices = Object.assign({ iterationLimit: 0, parallelStart: 0, bulkReset: false }, run || {});
        if (mockLandscape && ! choices.source) {
            choices.source = 'real';
        }
        return choices;
    }

    /** The schedule card (BulkScheduleState.schedule): what it is, and what can be done with it */
    static cardHtml(state) {
        const { schedule, lastRefusal, landscape, form } = state;
        const choices = BulkScheduleView.runChoices(schedule.run, !! form.mock);
        const lines = BulkSyncView.summaryLines(choices, landscape, undefined, form.itemsPerChunk, undefined, { when: schedule.description });
        const next = schedule.nextRuns && schedule.nextRuns.length ? schedule.nextRuns.join('; ') : 'none';
        const changed = schedule.changedAt
            ? `Changed ${schedule.changedBy ? `by ${schedule.changedBy} ` : ''}on ${RunFormat.dateTime(schedule.changedAt)}.`
            : '';
        return `
            <div class="schedule-card" data-schedule-card>
                <p class="schedule-card-status">
                    <span class="status-badge ${schedule.enabled ? 'status-badge-success' : 'status-badge-warning'}">${schedule.enabled ? 'On' : 'Off'}</span>
                    <strong>${Html.escape(schedule.description)}</strong>
                    <code>${Html.escape(schedule.expression)}</code>
                </p>
                <p class="schedule-preview-line">${Html.escape(schedule.enabled ? `Next runs: ${next}.` : `It starts nothing while it is off. On, its next runs would be: ${next}.`)}</p>
                ${schedule.staleEndpoint ? BulkSyncView.noteHtml('warning', 'The schedule calls an endpoint the config no longer names',
                    `It calls ${schedule.staleEndpoint.scheduled.baseUrl}${schedule.staleEndpoint.scheduled.fetchPath}; the integration config now names ${schedule.staleEndpoint.configured.baseUrl}${schedule.staleEndpoint.configured.fetchPath}. Edit the schedule and save it to bring it up to date.`) : ''}
                ${lastRefusal ? BulkSyncView.noteHtml('info', `A scheduled run was skipped on ${RunFormat.dateTime(lastRefusal.refusedAt)}`, lastRefusal.reason) : ''}
                <ul class="bulk-confirm-list">
                    ${lines.map(line => `<li>${Html.escape(line)}</li>`).join('')}
                </ul>
                ${changed ? `<p class="schedule-changed">${Html.escape(changed)}</p>` : ''}
                <p class="bulk-form-error" data-schedule-error role="alert" hidden></p>
                <div class="bulk-confirm-actions" data-schedule-actions>
                    <button class="btn btn-sm ${schedule.enabled ? 'btn-outline-secondary' : 'btn-primary'}" type="button" data-action="schedule-toggle">
                        <i class="fas ${schedule.enabled ? 'fa-pause' : 'fa-play'} me-1" aria-hidden="true"></i>${schedule.enabled ? 'Deactivate' : 'Activate'}
                    </button>
                    <button class="btn btn-sm btn-outline-secondary" type="button" data-action="schedule-edit">
                        <i class="fas fa-pen me-1" aria-hidden="true"></i>Edit schedule
                    </button>
                    <button class="btn btn-sm btn-outline-danger" type="button" data-action="schedule-delete">
                        <i class="fas fa-trash me-1" aria-hidden="true"></i>Delete schedule
                    </button>
                </div>
                <div class="bulk-confirm" data-schedule-delete-confirm hidden>
                    <p class="bulk-confirm-title">Delete this schedule?</p>
                    <p class="run-note-message">Nothing runs on a schedule in this landscape afterward, until a new one is made here.</p>
                    <div class="bulk-confirm-actions">
                        <button class="btn btn-danger btn-sm" type="button" data-action="schedule-delete-confirm">Delete schedule</button>
                        <button class="btn btn-outline-secondary btn-sm" type="button" data-action="schedule-delete-cancel">Keep it</button>
                    </div>
                </div>
            </div>
        `;
    }

    /** The other landscapes' schedules (BulkScheduleState.otherSchedules) */
    static othersHtml(others) {
        if ( ! others || others.length === 0) {
            return '<p class="bulk-status-line">No other landscape in this account has a bulk sync schedule.</p>';
        }
        return `
            <div class="table-responsive">
                <table class="table data-table">
                    <thead>
                        <tr><th scope="col">Landscape</th><th scope="col">State</th><th scope="col">When</th><th scope="col">Source</th></tr>
                    </thead>
                    <tbody>
                        ${others.map(other => `
                            <tr>
                                <td>${Html.escape(other.landscape)}</td>
                                <td><span class="status-badge ${other.enabled ? 'status-badge-success' : 'status-badge-warning'}">${other.enabled ? 'On' : 'Off'}</span></td>
                                <td class="wrap">${Html.escape(other.description)}</td>
                                <td>${other.realSource ? 'Real source' : 'Source simulator'}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
            <p class="form-text">Only schedules that are on and read the real source keep this landscape's an hour away.</p>
        `;
    }

    /** The When choices a form's fields make (ScheduleChoices), for the server to build and check */
    static readWhen(form) {
        const fields = form.elements;
        const mode = ([...(fields.whenMode || [])].find(radio => radio.checked) || {}).value || 'simple';
        if (mode === 'cron') {
            return { mode: 'cron', expression: String(fields.whenExpression.value || '').trim() };
        }
        const named = fields.whenDays.value;
        const days = named === 'chosen'
            ? [...(fields.whenDay || [])].filter(box => box.checked).map(box => box.value)
            : named;
        return { mode: 'simple', time: String(fields.whenTime.value || '').trim(), days };
    }
}

/** The Bulk sync schedule page */
class BulkSchedulePage {

    /**
     * @param {{ getJson: function(string): Promise<any>, postJson: function(string, any): Promise<any>,
     *   sendJson: function(string, string, any=): Promise<any>, root: HTMLElement }} options
     *   getJson, postJson and sendJson (PUT, DELETE) read and send a route's JSON (dashboard.js)
     */
    constructor({ getJson, postJson, sendJson, root }) {
        this.getJson = getJson;
        this.postJson = postJson;
        this.sendJson = sendJson;
        this.main = root.querySelector('[data-schedule-main]');
        // BulkSyncPage's lock() and showFormError() work on the form's container by this name
        this.formContainer = this.main;
        this.othersContainer = root.querySelector('[data-schedule-others]');
        this.stamp = root.querySelector('[data-refresh-stamp="schedule"]');
        this.state = undefined;
        this.editing = false;
        this.values = undefined;
        this.when = undefined;
        this.enabled = undefined;
        this.request = undefined;
        this.loads = 0;
        this.previews = 0;
        root.querySelector('[data-action="refresh-schedule"]').addEventListener('click', () => this.load());
        this.main.addEventListener('submit', event => {
            event.preventDefault();
            this.review(event.target);
        });
        this.main.addEventListener('change', event => this.onChange(event));
        this.main.addEventListener('click', event => {
            const action = event.target.closest && event.target.closest('[data-action]');
            if ( ! action) return;
            const name = action.dataset.action;
            if (name === 'confirm-start') this.confirm();
            if (name === 'cancel-start') this.cancel();
            if (name === 'schedule-toggle') this.toggle();
            if (name === 'schedule-edit') this.edit();
            if (name === 'schedule-edit-cancel') this.stopEditing();
            if (name === 'schedule-delete') this.askDelete(true);
            if (name === 'schedule-delete-cancel') this.askDelete(false);
            if (name === 'schedule-delete-confirm') this.remove();
        });
    }

    /** Load the schedule, the form's state and the other landscapes'. Resolves true once shown */
    async load() {
        const load = ++this.loads;
        PageState.loading(this.main, 'Reading the schedule');
        PageState.loading(this.othersContainer, "Reading the other landscapes' schedules", { compact: true });
        let state;
        try {
            state = await this.getJson('/api/bulk-schedule');
        } catch (error) {
            if (load !== this.loads) {
                return false;
            }
            PageState.failed(this.main, 'The schedule could not be read', error);
            PageState.failed(this.othersContainer, "The other landscapes' schedules could not be read", PageState.isForbidden(error) ? error : undefined, { compact: true });
            return false;
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

    /** The card when there is a schedule and it isn't being edited, else the form */
    render() {
        const { state } = this;
        this.othersContainer.innerHTML = BulkScheduleView.othersHtml(state.otherSchedules);
        if (state.schedule && ! this.editing) {
            this.main.innerHTML = BulkScheduleView.cardHtml(state);
            return;
        }
        this.renderForm();
    }

    /**
     * The form: a new schedule's (from the defaults), or the schedule's being edited (from its choices and run), keeping
     * what was entered across a refresh or a Back
     */
    renderForm() {
        const { state } = this;
        const schedule = this.editing ? state.schedule : undefined;
        const when = this.when || (schedule ? schedule.choices : undefined) || state.when.defaults;
        // A schedule's run opens Custom settings, so what it has is what is saved again (closed, the defaults would be)
        const values = this.values || (schedule ? Object.assign({ custom: true }, schedule.run) : undefined);
        const enabled = this.enabled !== undefined ? this.enabled : (schedule ? schedule.enabled : true);
        const blocked = state.blocked ? BulkSyncView.noteHtml('warning', "The schedule can't be changed here", state.blocked.message) : '';
        const cancel = this.editing
            ? '<div class="mb-3"><button class="btn btn-sm btn-outline-secondary" type="button" data-action="schedule-edit-cancel">Cancel editing</button></div>'
            : '';
        this.main.innerHTML = blocked + BulkScheduleView.runFormHtml(state.form, values, {
            leadHtml: BulkScheduleView.whenHtml(when, state.when),
            tailHtml: BulkScheduleView.enabledHtml(enabled) + cancel
        });
        this.previewWhen();
    }

    /** The form on the page, if it shows */
    get form() {
        return this.main.querySelector('form');
    }

    /** Ask the server what the When choices make, and show its answer under them; a later ask replaces an earlier one */
    async previewWhen() {
        const form = this.form;
        const container = this.main.querySelector('[data-schedule-preview]');
        if ( ! form || ! container) {
            return;
        }
        const preview = ++this.previews;
        const source = form.elements.source ? ([...form.elements.source].find(radio => radio.checked) || {}).value : undefined;
        try {
            const answer = await this.postJson('/api/bulk-schedule/when', { when: BulkScheduleView.readWhen(form), ...(source ? { run: { source } } : {}) });
            if (preview === this.previews) {
                container.innerHTML = BulkScheduleView.previewHtml(answer);
            }
        } catch (error) {
            if (preview === this.previews) {
                container.innerHTML = BulkSyncView.noteHtml('warning', "When couldn't be checked", error.message);
            }
        }
    }

    /**
     * A field changed: When's own fields show or hide their parts and ask the server again; a change of source asks
     * again too (a schedule on the source simulator is never too close to another); the run's fields behave as Bulk
     * sync's (BulkSyncPage.onChange())
     */
    onChange(event) {
        const form = event.target.form;
        if ( ! form) {
            return;
        }
        const name = event.target.name;
        if (name === 'whenMode') {
            const cron = event.target.value === 'cron';
            form.querySelector('[data-when-simple]').hidden = cron;
            form.querySelector('[data-when-cron]').hidden = ! cron;
        }
        if (name === 'whenDays') {
            form.querySelector('[data-when-chosen]').hidden = event.target.value !== 'chosen';
        }
        if (name === 'scheduleEnabled') {
            this.enabled = !! event.target.checked;
            return;
        }
        if (name && name.indexOf('when') === 0) {
            this.previewWhen();
            return;
        }
        BulkSyncPage.prototype.onChange.call(this, event);
        if (name === 'source') {
            this.previewWhen();
        }
    }

    /**
     * Check the run here, then ask the server to review when and the run (POST /api/bulk-schedule/plan, the checks a
     * save makes), and show that with Schedule run (or Save schedule) and Back. Resolves true once the review shows
     */
    async review(form) {
        const { custom, ...run } = BulkSyncPage.readRequest(form);
        const when = BulkScheduleView.readWhen(form);
        const enabled = !! (form.elements.scheduleEnabled && form.elements.scheduleEnabled.checked);
        this.values = Object.assign({ custom }, run);
        this.when = when;
        this.enabled = enabled;
        const problem = BulkSyncView.requestError(run);
        if (problem) {
            BulkSyncPage.prototype.showFormError.call(this, problem);
            return false;
        }
        BulkSyncPage.prototype.showFormError.call(this, undefined);
        BulkSyncPage.prototype.lock.call(this, true);
        let plan;
        try {
            plan = await this.postJson('/api/bulk-schedule/plan', { when, run, enabled });
        } catch (error) {
            BulkSyncPage.prototype.lock.call(this, false);
            BulkSyncPage.prototype.showFormError.call(this, `The schedule can't be saved: ${error.message}`);
            return false;
        }
        this.request = { when, run, enabled };
        const confirm = this.main.querySelector('[data-bulk-confirm]');
        confirm.innerHTML = BulkSyncView.confirmationHtml(
            Object.assign({ landscape: plan.landscape }, plan.run), undefined, BulkSyncPage.simulatorPeople(this.state.form), {
                schedule: true,
                when: plan.when.description,
                whenHtml: BulkScheduleView.reviewWhenHtml(plan.when, enabled),
                title: plan.replaces ? 'Save this schedule?' : 'Schedule this run?',
                button: plan.replaces ? 'Save schedule' : 'Schedule run'
            });
        return true;
    }

    /** Back to the form, keeping what was entered */
    cancel() {
        this.request = undefined;
        this.render();
    }

    /** Save the reviewed schedule (PUT /api/bulk-schedule). Resolves true once saved; a refusal is shown under the review */
    async confirm() {
        if ( ! this.request) {
            return false;
        }
        const buttons = [...this.main.querySelectorAll('[data-bulk-confirm] button')];
        buttons.forEach(button => { button.disabled = true; });
        try {
            await this.sendJson('PUT', '/api/bulk-schedule', this.request);
            this.forgetEntries();
            await this.load();
            return true;
        } catch (error) {
            buttons.forEach(button => { button.disabled = false; });
            const message = this.main.querySelector('[data-bulk-start-error]');
            if (message) {
                message.textContent = `The schedule wasn't saved: ${error.message}`;
                message.hidden = false;
            }
            return false;
        }
    }

    /** Edit the schedule: the form, filled from it */
    edit() {
        this.forgetEntries();
        this.editing = true;
        this.render();
    }

    /** Back to the card, dropping what was entered */
    stopEditing() {
        this.forgetEntries();
        this.render();
    }

    forgetEntries() {
        this.editing = false;
        this.values = undefined;
        this.when = undefined;
        this.enabled = undefined;
        this.request = undefined;
    }

    /** Say a problem under the card's buttons, or clear it */
    showCardError(problem) {
        const error = this.main.querySelector('[data-schedule-error]');
        if (error) {
            error.textContent = problem || '';
            error.hidden = ! problem;
        }
    }

    /** Turn the schedule on or off (POST /api/bulk-schedule/state). Resolves true once done; a refusal shows on the card */
    async toggle() {
        const schedule = this.state && this.state.schedule;
        if ( ! schedule) {
            return false;
        }
        const buttons = [...this.main.querySelectorAll('[data-schedule-actions] button')];
        buttons.forEach(button => { button.disabled = true; });
        try {
            await this.postJson('/api/bulk-schedule/state', { enabled: ! schedule.enabled });
            await this.load();
            return true;
        } catch (error) {
            buttons.forEach(button => { button.disabled = false; });
            this.showCardError(`The schedule wasn't turned ${schedule.enabled ? 'off' : 'on'}: ${error.message}`);
            return false;
        }
    }

    /** Show or hide the question before deleting */
    askDelete(asking) {
        const question = this.main.querySelector('[data-schedule-delete-confirm]');
        const actions = this.main.querySelector('[data-schedule-actions]');
        if (question) question.hidden = ! asking;
        if (actions) actions.hidden = asking;
    }

    /** Delete the schedule (DELETE /api/bulk-schedule). Resolves true once deleted */
    async remove() {
        try {
            await this.sendJson('DELETE', '/api/bulk-schedule');
            this.forgetEntries();
            await this.load();
            return true;
        } catch (error) {
            this.askDelete(false);
            this.showCardError(`The schedule wasn't deleted: ${error.message}`);
            return false;
        }
    }
}
