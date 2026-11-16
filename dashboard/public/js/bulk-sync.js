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

    /**
     * A mock landscape's source simulator settings: the form field each is read from, its label, its hint, and its hint
     * for a test (testHint), which reads one person. People has none: a test doesn't send it, so the simulator keeps its
     * own population, which only says which BUIDs it has (the BUID box says so)
     */
    static get SIMULATOR_FIELDS() {
        return [
            { name: 'totalPopulation', field: 'simTotalPopulation', label: 'People', step: '1',
                hint: 'How many people the simulator makes up. The run reads them all, then finds the end.' },
            { name: 'simulatedDelaySeconds', field: 'simDelay', label: 'Seconds per response', step: 'any',
                hint: 'How long each response takes, as a slow source would.',
                testHint: "How long the person's one response takes, as a slow source would." },
            { name: 'errorRate', field: 'simErrorRate', label: 'Failing responses (0 to 1)', step: 'any',
                hint: 'The share of responses that fail, to see how the run copes. 0: none fail.',
                testHint: "The chance that the person's one response fails, to see how the run copes. 0: it succeeds." },
            { name: 'nonCurrentTermRate', field: 'simNonCurrentTermRate', label: 'Students not in a current term (0 to 1)', step: 'any',
                hint: 'The share of students given a term that is not current. 0: every student is in a current term.',
                testHint: 'The share of students given a term that is not current; whether this person is one of them depends on their BUID. 0: every student is in a current term.' }
        ];
    }

    /** The simulator's BUID for a person's number from 1 (U0000042 is the 42nd person), as its people endpoint makes them */
    static simulatorBuid(number) {
        return `U${String(number).padStart(7, '0')}`;
    }

    /** A test with the source simulator: the form's class while one is chosen (BulkSyncPage.markSimulatorTest()) */
    static get SIMULATOR_TEST_CLASS() {
        return 'bulk-simulator-test';
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
        const simulatorDefaults = state.mock ? state.mock.simulator.defaults : {};
        return {
            runType: entered.runType || defaults.runType,
            source: entered.source || 'real',
            simulator: Object.assign({}, simulatorDefaults, entered.simulator || {}),
            resetState: !! entered.resetState,
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
     * override. A test shows the BUID box; Custom settings shows the custom fields. A test is always a single chunker
     * task, so while Test is chosen, Single task is checked and the chunk mode can't be changed (the server makes a
     * test a single task whatever is sent: BulkSyncRequest.parse()). The fieldset keeps the mode the user chose
     * (data-chosen-mode), to restore when another run type is chosen. Parallel start is likewise held at 0 for a test or
     * a single task (seeded offsets need chunks per task), its own choice kept on its wrapper (data-chosen-parallel).
     */
    static formHtml(state, values) {
        const {
            latestRun = {}, blocked, warnings = [], iterationLimit: chunks = { max: 50 }, parallelStart: parallel = { max: 20, enabled: false }
        } = state;
        const form = BulkSyncView.formValues(state, values);
        const test = form.runType === 'test';
        const single = form.singleTask || test;
        const simulator = state.mock && state.mock.simulator && state.mock.simulator.available ? state.mock.simulator : undefined;
        const simulatorTest = test && !! simulator && form.source === 'simulator';
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
            <form data-bulk-sync-form novalidate${simulatorTest ? ` class="${BulkSyncView.SIMULATOR_TEST_CLASS}"` : ''}>
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
                           placeholder="${simulatorTest ? BulkSyncView.SIMULATOR_BUID_PLACEHOLDER : BulkSyncView.BUID_PLACEHOLDER}" autocomplete="off" spellcheck="false">
                    ${simulator ? `<div class="form-text bulk-buid-simulator">${Html.escape(BulkSyncView.simulatorBuidHint(simulator.defaults.totalPopulation))}</div>` : ''}
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
                    <fieldset class="mb-3" data-bulk-chunk-mode data-chosen-mode="${form.singleTask ? 'single' : 'perTask'}">
                        <legend class="form-label">Chunker tasks</legend>
                        <div class="form-text mt-0 mb-2">
                            The source is read a chunk of people at a time, by chunker tasks. A test reads one person, so it is always a single task.
                        </div>
                        ${BulkSyncView.radioHtml({
                            name: 'chunkMode', value: 'perTask', label: 'Chunks per task', checked: ! single, disabled: test,
                            hint: 'Each task reads this many chunks, then hands the rest on to a new task, so several tasks share the work.'
                        })}
                        <div class="bulk-chunks" data-bulk-chunks${single ? ' hidden' : ''}>
                            <label class="visually-hidden" for="bulk-chunksPerTask">Chunks per task</label>
                            <select class="form-select bulk-number" name="chunksPerTask" id="bulk-chunksPerTask">
                                ${options(1, chunks.max, form.chunksPerTask)}
                            </select>
                        </div>
                        ${BulkSyncView.radioHtml({
                            name: 'chunkMode', value: 'single', label: 'Single task', checked: single, disabled: test,
                            hint: 'One task reads every chunk itself. With no other task to share the work, reading the source takes much longer.'
                        })}
                    </fieldset>
                    <div data-bulk-parallel data-enabled="${parallel.enabled ? 'true' : 'false'}" data-chosen-parallel="${form.parallelStart}">
                        <label class="form-label" for="bulk-parallelStart">Parallel start</label>
                        <select class="form-select bulk-number" name="parallelStart" id="bulk-parallelStart"${parallel.enabled && ! single ? '' : ' disabled'}>
                            ${options(0, parallel.max, single ? 0 : form.parallelStart)}
                        </select>
                        <div class="form-text">
                            How many chunker tasks start the run at once. 0 starts it with one, and auto-scaling adds tasks as
                            the work builds up. Above 0, the start takes up to a few minutes while the chunker's scaling catches
                            up. A test or a single task always starts with one.
                        </div>
                    </div>
                </fieldset>
                ${state.mock ? BulkSyncView.mockHtml(state.mock, form) : ''}
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

    /**
     * A mock landscape's own options (BulkSyncState.mock): the source, the simulator's settings while it is the
     * source, and Start from scratch. The simulator's settings start at its deployed ones.
     */
    static mockHtml(mock, form) {
        const { simulator } = mock;
        const simulated = form.source === 'simulator' && simulator.available;
        return `
            <fieldset class="bulk-mock mb-3" data-bulk-mock aria-label="Mock landscape">
                <legend class="form-label">Mock landscape</legend>
                <div class="form-text mt-0 mb-2">
                    This landscape's target is the target simulator, a stand-in for Huron, so nothing here reaches the real Huron.
                </div>
                ${BulkSyncView.radioHtml({
                    name: 'source', value: 'real', label: 'Real source', checked: ! simulated,
                    hint: 'Read people from the real source API, as any landscape does. Its people are real, and reading them uses the source as any run does.'
                })}
                ${BulkSyncView.radioHtml({
                    name: 'source', value: 'simulator', label: 'Source simulator', checked: simulated, disabled: ! simulator.available,
                    hint: simulator.available
                        ? 'Read made-up people from the source simulator instead, without touching the real source.'
                        : `Not available: ${simulator.reason || 'the source simulator is not deployed in this landscape'}.`
                })}
                <div class="bulk-simulator" data-bulk-simulator${simulated ? '' : ' hidden'}>
                    ${BulkSyncView.SIMULATOR_FIELDS.map(({ name, field, label, step, hint, testHint }) => `
                        <div class="mb-2${testHint ? '' : ' bulk-sim-run-only'}" data-sim-field="${name}">
                            <label class="form-label" for="bulk-${field}">${Html.escape(label)}</label>
                            <input class="form-control bulk-number" type="number" name="${field}" id="bulk-${field}" min="0"
                                   step="${step}" value="${Html.escape(String(form.simulator[name] === undefined ? '' : form.simulator[name]))}">
                            <div class="form-text${testHint ? ' bulk-sim-run-hint' : ''}">${Html.escape(hint)}</div>
                            ${testHint ? `<div class="form-text bulk-sim-test-hint">${Html.escape(testHint)}</div>` : ''}
                        </div>
                    `).join('')}
                </div>
                ${BulkSyncView.checkboxHtml({
                    name: 'resetState', label: 'Start from scratch', checked: form.resetState,
                    hint: "Before the run starts, empty this landscape's target simulator and its Run history, and forget every person's fingerprint, so everyone is created again. Use it to see a first sync as it would happen. Run history then starts again from this run."
                })}
            </fieldset>
        `;
    }

    /**
     * What a run will do, one sentence a line, from its choices (BulkSyncChoices) and the chunker's chunk size.
     * simulatorPeople: the source simulator's own population, which a test with it doesn't send (BulkSyncState.mock)
     */
    static summaryLines(choices, landscape, latestRun, itemsPerChunk, simulatorPeople) {
        const lines = [];
        // A mock landscape's run (its choices have a source) goes into its target simulator
        const target = choices.source ? `${landscape}'s target simulator` : `${landscape}'s Huron`;
        if (choices.runType === 'test') {
            lines.push(`Test of ${choices.buid} alone, into ${target}. No one is deactivated.`);
        } else if (choices.runType === 'full') {
            lines.push(`Full sync of everyone in the source, into ${target}. People no longer in the source are deactivated afterward.`);
        } else {
            lines.push(`Full sync of everyone in the source, into ${target}. No one is deactivated.`);
        }
        if (choices.source === 'simulator') {
            lines.push(choices.runType === 'test'
                ? BulkSyncView.simulatorTestLine(choices.simulator || {}, choices.buid, simulatorPeople)
                : BulkSyncView.simulatorLine(choices.simulator || {}));
        } else if (choices.source === 'real') {
            lines.push('The source is the real source API, read as any landscape reads it.');
        }
        if (choices.resetState) {
            lines.push("It starts from scratch: the target simulator, Run history and every person's fingerprint are emptied first, so everyone is created again.");
        }
        // A mock landscape's target is its target simulator; after a start from scratch no one has a fingerprint to skip by
        const huron = choices.source ? 'the target simulator' : 'Huron';
        if (choices.bulkReset) {
            lines.push(`Bulk reset: everyone is looked up in ${huron} and created or updated as needed.`);
        } else if ( ! choices.resetState) {
            lines.push(`People whose record has not changed since their last sync are skipped, and ${huron} is not contacted for them.`);
        }
        if (choices.runType !== 'test') {
            lines.push(choices.iterationLimit === 0
                ? 'A single chunker task reads the whole source.'
                : `Each chunker task reads ${choices.iterationLimit} chunks, then hands the rest on to a new task.`);
        }
        if (choices.parallelStart > 0) {
            lines.push(`It starts with ${choices.parallelStart} chunker tasks at once. The start takes up to a few minutes while the chunker's scaling catches up.`);
            const people = choices.source === 'simulator' && choices.simulator ? choices.simulator.totalPopulation : undefined;
            const perTask = choices.iterationLimit * (itemsPerChunk || 0);
            if (people && perTask > 0 && choices.parallelStart * perTask > people + perTask) {
                lines.push(`Its ${choices.parallelStart} tasks are set to read ${RunFormat.number(choices.parallelStart * perTask)} people, more than the simulator's ${RunFormat.number(people)}: the tasks past the end find no one and stop.`);
            }
        }
        if (choices.overrideStalled && latestRun && latestRun.syncRunId) {
            lines.push(`The stalled run of ${RunFormat.dateTime(latestRun.syncRunId)} is left as it is.`);
        }
        return lines;
    }

    /**
     * The simulator as a test's source, in words: one made-up person, and whether the simulator has them (a BUID past
     * its people reads as someone who isn't in the source, which a test may want to see)
     */
    static simulatorTestLine(simulator, buid, simulatorPeople) {
        const people = simulator.totalPopulation !== undefined ? simulator.totalPopulation : simulatorPeople;
        const number = buid ? Number(buid.slice(1)) : NaN;
        const parts = [`one made-up person, ${buid}`];
        if (simulator.simulatedDelaySeconds) {
            parts.push(`its response taking ${simulator.simulatedDelaySeconds} s`);
        }
        if (simulator.errorRate) {
            parts.push(`a ${Math.round(simulator.errorRate * 100)}% chance its response fails`);
        }
        if (simulator.nonCurrentTermRate) {
            parts.push(`${Math.round(simulator.nonCurrentTermRate * 100)}% of students not in a current term`);
        }
        const line = `The source is the source simulator: ${parts.join(', ')}.`;
        return Number.isInteger(people) && number > people
            ? `${line} ${buid} isn't in the source: the simulator has ${RunFormat.number(people)} people, so the run sees someone missing from it.`
            : line;
    }

    /** The BUID box's hint with the source simulator: its range, and what a BUID past it is for */
    static simulatorBuidHint(people) {
        const last = Number.isInteger(people) && people >= 1 ? BulkSyncView.simulatorBuid(people) : undefined;
        return last
            ? `A made-up person from the source simulator, ${BulkSyncView.simulatorBuid(1)} to ${last}. One past the end is someone who isn't in the source, to test that case.`
            : `A made-up person from the source simulator, from ${BulkSyncView.simulatorBuid(1)}. One past its people is someone who isn't in the source, to test that case.`;
    }

    static get BUID_PLACEHOLDER() {
        return 'U12345678';
    }

    static get SIMULATOR_BUID_PLACEHOLDER() {
        return 'U0000042';
    }

    /** The simulator as the source, in words, from the settings sent to it */
    static simulatorLine(simulator) {
        const parts = [];
        if (simulator.totalPopulation !== undefined) {
            parts.push(`${RunFormat.number(simulator.totalPopulation)} made-up people`);
        }
        if (simulator.simulatedDelaySeconds) {
            parts.push(`each response taking ${simulator.simulatedDelaySeconds} s`);
        }
        if (simulator.errorRate) {
            parts.push(`${Math.round(simulator.errorRate * 100)}% of responses failing`);
        }
        if (simulator.nonCurrentTermRate) {
            parts.push(`${Math.round(simulator.nonCurrentTermRate * 100)}% of students not in a current term`);
        }
        return `The source is the source simulator${parts.length ? `: ${parts.join(', ')}` : ''}.`;
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
    /** The review of a run (BulkSyncPlan); simulatorPeople is the source simulator's own population, as summaryLines() takes */
    static confirmationHtml(plan, latestRun, simulatorPeople) {
        const eventRows = Object.entries(plan.event).map(([name, value]) => ({ name, value }));
        const chunkSize = (plan.settings || []).find(setting => setting.name === 'itemsPerChunk');
        const itemsPerChunk = chunkSize && Number(chunkSize.value) > 0 ? Number(chunkSize.value) : undefined;
        return `
            <div class="bulk-confirm" role="group" aria-label="Confirm the run">
                <p class="bulk-confirm-title">Start this run?</p>
                <ul class="bulk-confirm-list">
                    ${BulkSyncView.summaryLines(plan.choices, plan.landscape, latestRun, itemsPerChunk, simulatorPeople).map(line => `<li>${Html.escape(line)}</li>`).join('')}
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
        return request.source === 'simulator' ? BulkSyncView.simulatorError(request) : undefined;
    }

    /**
     * A problem with the simulator's settings, or a test's BUID it can't have (U0000000); undefined when they are fine.
     * A test doesn't send People, and its BUID may be past them: that person reads as missing from the source.
     */
    static simulatorError(request) {
        const simulator = request.simulator || {};
        const people = simulator.totalPopulation;
        if (request.runType !== 'test' && ! (Number.isInteger(people) && people >= 1)) {
            return 'The simulator needs a whole number of people, at least 1.';
        }
        if ( ! (Number.isFinite(simulator.simulatedDelaySeconds) && simulator.simulatedDelaySeconds >= 0)) {
            return 'Seconds per response is a number, 0 or more.';
        }
        for (const name of ['errorRate', 'nonCurrentTermRate']) {
            const value = simulator[name];
            if ( ! (Number.isFinite(value) && value >= 0 && value <= 1)) {
                const label = BulkSyncView.SIMULATOR_FIELDS.find(field => field.name === name).label;
                return `${label} is a number from 0 to 1.`;
            }
        }
        if (request.buid && BulkSyncView.BUID.test(request.buid)) {
            const index = Number(request.buid.slice(1)) - 1;
            if (index < 0) {
                return `The simulator's BUIDs start at U0000001 (got '${request.buid}').`;
            }
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
        const parallel = result.run.parallelStart > 0
            ? `Its ${result.run.parallelStart} chunker tasks start within a few minutes, once the chunker's scaling catches up. `
            : '';
        const scratch = result.run.resetState
            ? 'Run history was emptied first, so this run will be the only one listed. '
            : '';
        return `
            <p class="bulk-status-line">${Html.escape(when)}: ${Html.escape(what)}.</p>
            <p class="bulk-status-hint">${Html.escape(scratch + parallel)}It appears in Run history once its chunker has written its first records. Refresh to look for it.</p>
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
     * settings is checked (otherwise the server uses its defaults). A single task is iterationLimit 0, and a test is
     * always a single task. custom says which, for redrawing the form.
     */
    static readRequest(form) {
        const fields = form.elements;
        const checked = name => ([...fields[name]].find(radio => radio.checked) || {}).value;
        const runType = checked('runType');
        const buid = String(fields.buid.value || '').trim().toUpperCase();
        const custom = !! fields.custom.checked;
        const single = runType === 'test' || checked('chunkMode') === 'single';
        // A mock landscape's own options, only when its section is on the page
        const source = fields.source ? (checked('source') || 'real') : undefined;
        const simulator = {};
        if (source === 'simulator') {
            for (const { name, field } of BulkSyncView.SIMULATOR_FIELDS) {
                if (runType === 'test' && name === 'totalPopulation') {
                    continue;
                }
                const value = String(fields[field].value).trim();
                simulator[name] = value === '' ? NaN : Number(value);
            }
        }
        return {
            runType,
            ...(runType === 'test' ? { buid } : {}),
            bulkReset: !! fields.bulkReset.checked,
            ...(custom ? {
                iterationLimit: single ? 0 : Number(fields.chunksPerTask.value),
                parallelStart: single ? 0 : Number(fields.parallelStart.value)
            } : {}),
            ...(fields.overrideStalled ? { overrideStalled: !! fields.overrideStalled.checked } : {}),
            ...(source ? { source, ...(source === 'simulator' ? { simulator } : {}), resetState: !! fields.resetState.checked } : {}),
            custom
        };
    }

    /**
     * Show the BUID box for a test, the custom fields while Custom settings is checked, and the chunks per task for
     * that choice. Choosing Test checks Single task and locks the chunk mode; choosing another run type unlocks it and
     * restores the mode the user chose.
     */
    onChange(event) {
        const form = event.target.form;
        if ( ! form) {
            return;
        }
        if (event.target.name === 'runType') {
            const test = form.querySelector('[data-bulk-test]');
            if (test) test.hidden = event.target.value !== 'test';
            const mode = BulkSyncPage.lockChunkMode(form, event.target.value === 'test');
            BulkSyncPage.lockParallelStart(form, mode === 'single');
            BulkSyncPage.markSimulatorTest(form);
        }
        if (event.target.name === 'chunkMode') {
            const chunks = form.querySelector('[data-bulk-chunks]');
            if (chunks) chunks.hidden = event.target.value !== 'perTask';
            const modes = form.querySelector('[data-bulk-chunk-mode]');
            if (modes) modes.dataset.chosenMode = event.target.value;
            BulkSyncPage.lockParallelStart(form, event.target.value === 'single');
        }
        if (event.target.name === 'parallelStart') {
            const parallel = form.querySelector('[data-bulk-parallel]');
            if (parallel) parallel.dataset.chosenParallel = event.target.value;
        }
        if (event.target.name === 'custom') {
            const custom = form.querySelector('[data-bulk-custom]');
            if (custom) custom.hidden = ! event.target.checked;
        }
        if (event.target.name === 'source') {
            const simulator = form.querySelector('[data-bulk-simulator]');
            if (simulator) simulator.hidden = event.target.value !== 'simulator';
            BulkSyncPage.markSimulatorTest(form);
        }
    }

    /** The source simulator's own population (its deployed setting, from its /health), if this landscape has one */
    static simulatorPeople(state) {
        const simulator = state && state.mock && state.mock.simulator;
        return simulator && simulator.defaults ? simulator.defaults.totalPopulation : undefined;
    }

    /**
     * While Test and the source simulator are both chosen, mark the form (SIMULATOR_TEST_CLASS): People is hidden, the
     * simulator's hints speak of one person, and the BUID box says which BUIDs the simulator has (dashboard.css)
     */
    static markSimulatorTest(form) {
        const fields = form.elements || {};
        const checked = name => ([...(fields[name] || [])].find(radio => radio.checked) || {}).value;
        const simulatorTest = checked('runType') === 'test' && checked('source') === 'simulator';
        if (form.classList) {
            form.classList.toggle(BulkSyncView.SIMULATOR_TEST_CLASS, simulatorTest);
        }
        if (fields.buid) {
            fields.buid.placeholder = simulatorTest ? BulkSyncView.SIMULATOR_BUID_PLACEHOLDER : BulkSyncView.BUID_PLACEHOLDER;
        }
    }

    /**
     * For a test, check Single task and lock both chunk modes (a test is always a single chunker task); otherwise
     * unlock them and restore the mode the user chose (the fieldset's data-chosen-mode), with its dropdown.
     */
    static lockChunkMode(form, test) {
        const radios = form.elements ? [...(form.elements.chunkMode || [])] : [];
        const modes = form.querySelector('[data-bulk-chunk-mode]');
        const chunks = form.querySelector('[data-bulk-chunks]');
        const mode = test ? 'single' : ((modes && modes.dataset.chosenMode) || 'perTask');
        for (const radio of radios) {
            radio.disabled = test;
            radio.checked = radio.value === mode;
        }
        if (chunks) chunks.hidden = mode !== 'perTask';
        return mode;
    }

    /**
     * Hold Parallel start at 0, unchangeable, for a single chunker task (a test is one); otherwise restore the value the
     * user chose (the wrapper's data-chosen-parallel), changeable unless the landscape doesn't offer it (data-enabled).
     */
    static lockParallelStart(form, single) {
        const select = form.elements ? form.elements.parallelStart : undefined;
        const parallel = form.querySelector('[data-bulk-parallel]');
        if ( ! select) {
            return;
        }
        const enabled = ! parallel || parallel.dataset.enabled !== 'false';
        select.disabled = single || ! enabled;
        select.value = single ? '0' : ((parallel && parallel.dataset.chosenParallel) || '0');
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
            BulkSyncView.confirmationHtml(plan, this.state.latestRun, BulkSyncPage.simulatorPeople(this.state));
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
