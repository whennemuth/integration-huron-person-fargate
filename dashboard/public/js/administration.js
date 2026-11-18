/**
 * The Administration pages and the who-am-I dialog (authorization-plan.md, decisions 3, 9, 10 and 11):
 *
 *   WhoAmI          GET /api/me: the top bar's who-am-I dialog, and the notice for a user with no role
 *   AccessPage      who holds which roles: look a person up, check their roles, save; everyone with a role
 *   AuditTrailPage  the audit trail, newest first, filtered by person and action, a page at a time
 *   ImpersonateControl  the top bar's Impersonate dialog and Stop impersonating button (decision 14)
 *
 * Every section that loads content reports a failed load through PageState.failed(), so a 403 shows as redacted. An
 * action the user triggers (Look up, Save) shows the error pattern with the server's message. Markup goes through Html
 * (page-state.js); the audit list is a PagedTable (run-history.js). Both load before this file.
 * Tests: test/DashboardAdministrationPages.test.ts.
 */

/** The landscape as GET /api/me sees the signed-in user, read once per page load */
class WhoAmI {

    constructor(options) {
        this.getJson = options.getJson;
        this.root = options.root;
        this.loaded = undefined;
    }

    /** /api/me's answer, fetched once; null if it can't be read (e.g. no one signed in) */
    me() {
        if ( ! this.loaded) {
            this.loaded = this.getJson('/api/me').catch(error => {
                console.warn('Who am I could not be read:', error && error.message);
                this.loaded = undefined;
                return null;
            });
        }
        return this.loaded;
    }

    /** Fill the dialog when it opens, and show the no-role notice if it applies */
    async start() {
        const dialog = this.root.querySelector('#whoAmIDialog');
        if (dialog) {
            dialog.addEventListener('show.bs.modal', () => this.renderDialog());
        }
        const me = await this.me();
        this.renderNotice(me);
    }

    renderNotice(me) {
        const notice = this.root.querySelector('[data-no-role-notice]');
        if ( ! notice) {
            return;
        }
        notice.hidden = ! WhoAmI.hasNoRole(me);
    }

    static hasNoRole(me) {
        return !! me && me.signedIn && ! me.publicMode && (me.roles || []).length === 0;
    }

    async renderDialog() {
        const body = this.root.querySelector('[data-who-am-i]');
        if ( ! body) {
            return;
        }
        PageState.loading(body, 'Reading who you are');
        this.loaded = undefined;
        const me = await this.me();
        if ( ! me) {
            PageState.error(body, 'Who you are could not be read');
            return;
        }
        body.innerHTML = WhoAmI.dialogHtml(me);
    }

    static dialogHtml(me) {
        const user = me.user || {};
        const identity = [
            ['Name', user.displayName],
            ['buPrincipal', user.buPrincipal],
            ['eduPersonPrincipalName', user.eppn],
            ['Mail', user.mail]
        ].filter(([, value]) => value);
        const roles = me.roles || [];
        const viewedAs = me.viewedAs;
        return `
            <dl class="who-am-i-identity">
                ${identity.map(([label, value]) => `<dt>${Html.escape(label)}</dt><dd>${Html.escape(value)}</dd>`).join('')}
            </dl>
            ${viewedAs ? `
                <p class="who-am-i-impersonating"><i class="fas fa-user-secret me-1" aria-hidden="true"></i>You are seeing this
                    dashboard as <strong>${Html.escape(viewedAs.label)}</strong>${viewedAs.label === viewedAs.buPrincipal ? '' : ` (${Html.escape(viewedAs.buPrincipal)})`}, for viewing only.
                    Their roles are the ones in force below; yours return when you stop impersonating.</p>
            ` : ''}
            <h3 class="who-am-i-heading">${viewedAs ? `${Html.escape(viewedAs.label)}'s roles in this landscape` : 'Your roles in this landscape'}</h3>
            ${roles.length === 0
                ? `<p class="secondary">None. ${viewedAs ? 'They see' : 'You can see'} every page, but not its data.${viewedAs ? '' : ' Ask a dashboard administrator for a role.'}</p>`
                : roles.map(WhoAmI.roleHtml).join('')}
            ${(me.unknownRoles || []).length
                ? `<p class="secondary">Also assigned, but unknown to this dashboard (ignored): ${Html.escape(me.unknownRoles.join(', '))}</p>`
                : ''}
        `;
    }

    static roleHtml(role) {
        return `
            <div class="who-am-i-role">
                <p class="who-am-i-role-name">${Html.escape(role.name)} <span class="secondary">${Html.escape(role.description)}</span></p>
                <ul class="who-am-i-policies">
                    ${(role.policies || []).map(policy => `
                        <li><span class="who-am-i-policy">${Html.escape(policy.name)}</span>
                            ${(policy.actions || []).map(action => Html.tag(action)).join(' ')}</li>
                    `).join('')}
                </ul>
            </div>
        `;
    }
}

/** Who holds which roles in this landscape */
class AccessPage {

    /**
     * @param {{ getJson, sendJson, me: () => Promise<object|null>, root: HTMLElement, confirm?: (text) => boolean }} options
     *   sendJson(method, path, body) sends a change; confirm asks before an Admin removes their own Admin role
     */
    constructor(options) {
        this.options = options;
        this.root = options.root;
        this.list = this.root.querySelector('[data-access-list]');
        this.person = this.root.querySelector('[data-access-person]');
        this.form = this.root.querySelector('[data-access-lookup]');
        this.input = this.root.querySelector('#accessPrincipal');
        this.stamp = this.root.querySelector('[data-refresh-stamp="access"]');
        this.roles = undefined;
        this.shown = undefined;
        this.form.addEventListener('submit', event => {
            event.preventDefault();
            this.lookUp(this.input.value);
        });
        const refresh = this.root.querySelector('[data-action="refresh-access"]');
        if (refresh) {
            refresh.addEventListener('click', () => this.load());
        }
        this.list.addEventListener('click', event => {
            const row = event.target.closest('tr[data-principal]');
            if (row) {
                this.input.value = row.dataset.principal;
                this.lookUp(row.dataset.principal);
            }
        });
    }

    /** The catalog's roles and everyone with a role */
    async load() {
        PageState.loading(this.list, 'Loading role assignments');
        try {
            const [roles, assignments] = await Promise.all([
                this.options.getJson('/api/access/roles'),
                this.options.getJson('/api/access/assignments')
            ]);
            this.roles = roles.roles || [];
            this.renderList(assignments.assignments || []);
            RefreshStamp.set(this.stamp);
            return true;
        } catch (error) {
            PageState.failed(this.list, 'Role assignments could not be loaded', error);
            return false;
        }
    }

    renderList(assignments) {
        if (assignments.length === 0) {
            PageState.empty(this.list, 'No one holds a role', 'Look a person up above to give them one.');
            return;
        }
        this.list.innerHTML = `
            <div class="table-responsive">
                <table class="table data-table data-table-links">
                    <thead><tr>
                        <th scope="col">Person</th>
                        <th scope="col">Name</th>
                        <th scope="col">Roles</th>
                        <th scope="col">Granted</th>
                    </tr></thead>
                    <tbody>${assignments.map(AccessPage.rowHtml).join('')}</tbody>
                </table>
            </div>
        `;
    }

    static rowHtml(assignment) {
        const profile = assignment.profile || {};
        const grants = assignment.grants || [];
        const latest = grants.reduce((last, grant) => (! last || grant.grantedAt > last.grantedAt ? grant : last), undefined);
        return `
            <tr data-principal="${Html.escape(assignment.buPrincipal)}" title="Edit ${Html.escape(assignment.buPrincipal)}'s roles">
                <td><button type="button" class="btn btn-link p-0">${Html.escape(assignment.buPrincipal)}</button></td>
                <td class="wrap">${Html.escape(profile.displayName || profile.eppn || '')}</td>
                <td>${grants.map(grant => Html.tag(grant.role)).join(' ')}</td>
                <td class="secondary">${latest ? Html.escape(`by ${latest.grantedBy}, ${AccessPage.when(latest.grantedAt)}`) : ''}</td>
            </tr>
        `;
    }

    static when(iso) {
        const date = new Date(iso);
        return isNaN(date.getTime()) ? '' : date.toLocaleString();
    }

    /** Look a person up: their profile and roles, with a checkbox per catalog role */
    async lookUp(value) {
        const principal = String(value || '').trim();
        if ( ! principal) {
            PageState.error(this.person, 'Enter a person to look up', 'Their BU login (buPrincipal), or their eduPersonPrincipalName ending in @bu.edu.', { compact: true });
            return;
        }
        PageState.loading(this.person, `Looking up ${principal}`, { compact: true });
        try {
            if ( ! this.roles) {
                this.roles = (await this.options.getJson('/api/access/roles')).roles || [];
            }
            const person = await this.options.getJson(`/api/access/assignments/${encodeURIComponent(principal)}`);
            this.shown = person;
            this.chosen = new Set(person.roles || []);
            this.person.innerHTML = this.personHtml(person);
            const form = this.person.querySelector('[data-access-form]');
            form.addEventListener('submit', event => {
                event.preventDefault();
                this.save();
            });
            form.addEventListener('change', event => this.choose(event.target));
            this.applyCoverage();
        } catch (error) {
            this.shown = undefined;
            PageState.error(this.person, `${principal} could not be looked up`, error.message, { compact: true });
        }
    }

    personHtml(person) {
        const profile = person.profile;
        const held = person.roles || [];
        const about = profile
            ? [profile.displayName, profile.eppn, profile.mail].filter(Boolean).map(Html.escape).join(' · ')
                + `<span class="secondary"> · last signed in ${Html.escape(AccessPage.when(profile.lastSeen))}</span>`
            : `<span class="secondary">Hasn't signed in to this landscape's dashboard yet, so their name isn't known here.</span>`;
        return `
            <div class="access-person-profile">
                <p class="access-person-name"><i class="fas fa-circle-user me-1" aria-hidden="true"></i>${Html.escape(person.buPrincipal)}
                    ${person.from === 'eppn' ? '<span class="secondary">(their buPrincipal)</span>' : ''}</p>
                <p class="access-person-about">${about}</p>
            </div>
            <form data-access-form>
                <fieldset>
                    <legend class="form-label fs-6">Roles</legend>
                    ${this.roles.map(role => AccessPage.roleCheckboxHtml(role, held.includes(role.name))).join('')}
                </fieldset>
                <div class="access-person-actions">
                    <button class="btn btn-primary btn-sm" type="submit"><i class="fas fa-floppy-disk me-1"></i>Save</button>
                    <span data-access-result></span>
                </div>
            </form>
        `;
    }

    static roleCheckboxHtml(role, checked) {
        const id = `accessRole${role.name}`;
        return `
            <div class="form-check access-role">
                <input class="form-check-input" type="checkbox" id="${Html.escape(id)}" name="role" value="${Html.escape(role.name)}" ${checked ? 'checked' : ''}>
                <label class="form-check-label" for="${Html.escape(id)}">
                    <span class="access-role-name">${Html.escape(role.name)}</span>
                    <span class="access-role-included" data-included="${Html.escape(role.name)}"></span>
                    <span class="secondary">${Html.escape(role.description)}: ${Html.escape((role.policies || []).map(policy => policy.name).join(', '))}</span>
                </label>
            </div>
        `;
    }

    /**
     * Which of the chosen roles another chosen role covers, by the server's rule (pruneCoveredRoles(), decision 15): a
     * role whose every action another chosen role also grants; of two granting the same actions, the later is kept.
     * @param roles the catalog's roles, in its order, with their policies' actions (GET /api/access/roles)
     * @returns {{ kept: string[], coveredBy: Object<string, string> }} the roles to save, and each covered one's coverer
     */
    static coverage(roles, chosen) {
        const covers = AccessPage.covering(roles);
        const known = roles.map(role => role.name).filter(name => chosen.includes(name));
        const kept = known.filter(role => ! known.some(by => covers(by, role)));
        const coveredBy = {};
        known.filter(role => ! kept.includes(role)).forEach(role => {
            coveredBy[role] = kept.find(by => covers(by, role));
        });
        return { kept, coveredBy };
    }

    /** covers(by, role): whether role by covers role, by the rule above */
    static covering(roles) {
        const order = roles.map(role => role.name);
        const actionsOf = new Map(roles.map(role => [role.name, new Set((role.policies || []).flatMap(policy => policy.actions || []))]));
        return (by, role) => {
            const mine = actionsOf.get(role);
            const theirs = actionsOf.get(by);
            if ( ! mine || ! theirs || by === role || [...mine].some(action => ! theirs.has(action))) {
                return false;
            }
            return theirs.size > mine.size || order.indexOf(by) > order.indexOf(role);
        };
    }

    /** Every catalog role a kept role covers, chosen or not, with the kept role covering it: what the form shows as included */
    static includedIn(roles, kept) {
        const covers = AccessPage.covering(roles);
        const included = {};
        roles.forEach(({ name }) => {
            const by = kept.find(role => covers(role, name));
            if (by) {
                included[name] = by;
            }
        });
        return included;
    }

    /** A role checked or unchecked: the roles chosen change, and so does what they cover */
    choose(input) {
        if ( ! input || input.name !== 'role' || input.disabled) {
            return;
        }
        if (input.checked) {
            this.chosen.add(input.value);
        } else {
            this.chosen.delete(input.value);
        }
        this.applyCoverage();
    }

    /** Show every role a chosen role covers as checked, disabled and "included in <role>"; free the rest */
    applyCoverage() {
        const roles = this.roles || [];
        const coveredBy = AccessPage.includedIn(roles, AccessPage.coverage(roles, [...this.chosen]).kept);
        this.person.querySelectorAll('input[name="role"]').forEach(input => {
            const coverer = coveredBy[input.value];
            input.disabled = !! coverer;
            input.checked = !! coverer || this.chosen.has(input.value);
            const note = this.person.querySelector(`[data-included="${input.value}"]`);
            if (note) {
                note.textContent = coverer ? `included in ${coverer}` : '';
            }
        });
    }

    /** The roles to save: those chosen, less the ones another chosen role covers */
    checkedRoles() {
        return AccessPage.coverage(this.roles || [], [...(this.chosen || [])]).kept;
    }

    /** Save the checked roles, asking first when an Admin takes away their own Admin role */
    async save() {
        const person = this.shown;
        if ( ! person) {
            return;
        }
        const roles = this.checkedRoles();
        const result = this.person.querySelector('[data-access-result]');
        const me = await this.options.me();
        const removingOwnAdmin = me && me.user && me.user.buPrincipal === person.buPrincipal
            && (person.roles || []).includes('Admin') && ! roles.includes('Admin');
        if (removingOwnAdmin && ! this.confirm(AccessPage.OWN_ADMIN_CONFIRMATION)) {
            return;
        }
        PageState.loading(result, 'Saving', { compact: true });
        try {
            const change = await this.options.sendJson('PUT', `/api/access/assignments/${encodeURIComponent(person.buPrincipal)}`, { roles });
            this.shown = { ...person, roles: change.roles, grants: change.grants };
            this.chosen = new Set(change.roles || []);
            this.applyCoverage();
            result.replaceChildren(AccessPage.savedElement(change));
            this.load();
        } catch (error) {
            PageState.error(result, 'The roles could not be saved', error.message, { compact: true });
        }
    }

    static get OWN_ADMIN_CONFIRMATION() {
        return 'You are about to remove your own Admin role. You will no longer be able to manage roles or read the audit trail here. Remove it?';
    }

    confirm(text) {
        return (this.options.confirm || ((message) => window.confirm(message)))(text);
    }

    static savedElement(change) {
        const parts = [];
        if ((change.added || []).length) {
            parts.push(`added ${change.added.join(', ')}`);
        }
        if ((change.removed || []).length) {
            parts.push(`removed ${change.removed.join(', ')}`);
        }
        if ((change.pruned || []).length) {
            parts.push(`not kept ${change.pruned.map(({ role, coveredBy }) => `${role} (included in ${coveredBy})`).join(', ')}`);
        }
        const element = document.createElement('span');
        element.className = 'access-saved';
        element.setAttribute('role', 'status');
        element.textContent = parts.length ? `Saved: ${parts.join('; ')}` : 'No change';
        return element;
    }
}

/** The landscape's audit trail */
class AuditTrailPage {

    constructor(options) {
        this.options = options;
        this.root = options.root;
        this.form = this.root.querySelector('[data-audit-filters]');
        this.stamp = this.root.querySelector('[data-refresh-stamp="audit"]');
        this.filters = {};
        this.table = new PagedTable({
            container: this.root.querySelector('[data-audit-list]'),
            loadPage: cursor => this.options.getJson(AuditTrailPage.path(this.filters, cursor)),
            headHtml: AuditTrailPage.HEAD_HTML,
            rowHtml: AuditTrailPage.rowHtml,
            text: {
                loading: 'Loading the audit trail',
                loadingMore: 'Loading older records',
                error: 'The audit trail could not be loaded',
                errorMore: 'Older records could not be loaded',
                empty: 'No records',
                emptyMessage: 'Nothing has been recorded here yet, or nothing matches the filters.',
                more: 'Load older records',
                noun: ['record', 'records']
            }
        });
        this.form.addEventListener('submit', event => {
            event.preventDefault();
            this.applyFilters();
        });
        const clear = this.root.querySelector('[data-action="clear-audit-filters"]');
        if (clear) {
            clear.addEventListener('click', () => {
                this.form.reset();
                this.applyFilters();
            });
        }
        const refresh = this.root.querySelector('[data-action="refresh-audit"]');
        if (refresh) {
            refresh.addEventListener('click', () => this.load());
        }
    }

    static path(filters, cursor) {
        const query = new URLSearchParams();
        if (filters.principal) {
            query.set('principal', filters.principal);
        }
        if (filters.action) {
            query.set('action', filters.action);
        }
        if (cursor) {
            query.set('cursor', cursor);
        }
        const text = query.toString();
        return `/api/audit${text ? `?${text}` : ''}`;
    }

    applyFilters() {
        const data = new FormData(this.form);
        this.filters = {
            principal: String(data.get('principal') || '').trim(),
            action: String(data.get('action') || '')
        };
        return this.load();
    }

    async load() {
        const shown = await this.table.load();
        if (shown) {
            RefreshStamp.set(this.stamp);
        }
        return shown;
    }

    static get HEAD_HTML() {
        return `
            <th scope="col">When</th>
            <th scope="col">Who</th>
            <th scope="col">Action</th>
            <th scope="col">Target</th>
            <th scope="col">Decision</th>
            <th scope="col">Outcome</th>
            <th scope="col">Detail</th>
        `;
    }

    static rowHtml(record) {
        const when = new Date(record.at);
        const decision = record.decision === 'DENIED' ? Html.badge('Denied', 'danger') : Html.badge('Allowed', 'success');
        const outcome = record.outcome === 'SUCCEEDED' ? Html.badge('Succeeded', 'success')
            : record.outcome === 'FAILED' ? Html.badge('Failed', 'warning') : '';
        return `
            <tr>
                <td class="tabular-nums" title="${Html.escape(record.at)}">${Html.escape(isNaN(when.getTime()) ? record.at : when.toLocaleString())}</td>
                <td>${Html.escape(record.buPrincipal)}${record.viewedAs ? ` ${Html.tag(`as ${record.viewedAs}`)}` : ''}</td>
                <td>${Html.escape(record.action)}</td>
                <td>${Html.escape(record.target || '')}</td>
                <td>${decision}</td>
                <td>${outcome}</td>
                <td class="wrap">${Html.escape(record.detail || '')}</td>
            </tr>
        `;
    }
}

/**
 * Impersonation in the top bar (decision 14): the Impersonate dialog (choose a person, see their roles, start) for a
 * user whose own roles allow it, and the Stop impersonating button while impersonating. Starting and stopping set or
 * clear a cookie on the server, then the page reloads, so every section is read again through the roles now in force.
 */
class ImpersonateControl {

    /** @param {{ getJson, sendJson, root, reload?: () => void }} options */
    constructor(options) {
        this.options = options;
        this.root = options.root;
        this.chosen = undefined;
    }

    reload() {
        (this.options.reload || (() => window.location.reload()))();
    }

    start() {
        const stop = this.root.querySelector('[data-action="stop-impersonating"]');
        if (stop) {
            stop.addEventListener('click', () => this.stop());
        }
        const dialog = this.root.querySelector('#impersonateDialog');
        if ( ! dialog) {
            return;
        }
        this.form = dialog.querySelector('[data-impersonate-form]');
        this.input = dialog.querySelector('#impersonatePrincipal');
        this.person = dialog.querySelector('[data-impersonate-person]');
        this.startButton = dialog.querySelector('[data-action="start-impersonating"]');
        dialog.addEventListener('show.bs.modal', () => this.listPeople(dialog.querySelector('#impersonatePeople')));
        this.form.addEventListener('submit', event => {
            event.preventDefault();
            this.lookUp(this.input.value);
        });
        this.input.addEventListener('input', () => this.choose(undefined));
        this.startButton.addEventListener('click', () => this.begin());
    }

    /** The pick list: everyone with a role (left empty if the assignments can't be read) */
    async listPeople(datalist) {
        if ( ! datalist) {
            return;
        }
        try {
            const { assignments } = await this.options.getJson('/api/access/assignments');
            datalist.innerHTML = (assignments || []).map(assignment => {
                const name = assignment.profile && assignment.profile.displayName;
                const roles = (assignment.grants || []).map(grant => grant.role).join(', ');
                return `<option value="${Html.escape(assignment.buPrincipal)}">${Html.escape([name, roles].filter(Boolean).join(': '))}</option>`;
            }).join('');
        } catch (error) {
            datalist.innerHTML = '';
        }
    }

    choose(person) {
        this.chosen = person;
        if (this.startButton) {
            this.startButton.disabled = ! person;
        }
    }

    /** Show the person and their roles before starting */
    async lookUp(value) {
        const principal = String(value || '').trim();
        this.choose(undefined);
        if ( ! principal) {
            PageState.error(this.person, 'Enter a person', 'Their BU login (buPrincipal), or their eduPersonPrincipalName ending in @bu.edu.', { compact: true });
            return;
        }
        PageState.loading(this.person, `Looking up ${principal}`, { compact: true });
        try {
            const person = await this.options.getJson(`/api/access/assignments/${encodeURIComponent(principal)}`);
            this.person.innerHTML = ImpersonateControl.personHtml(person);
            this.choose(person);
        } catch (error) {
            PageState.error(this.person, `${principal} could not be looked up`, error.message, { compact: true });
        }
    }

    static personHtml(person) {
        const profile = person.profile || {};
        const roles = person.roles || [];
        return `
            <p class="impersonate-person-name"><i class="fas fa-circle-user me-1" aria-hidden="true"></i>${Html.escape(profile.displayName || person.buPrincipal)}
                ${profile.displayName ? `<span class="secondary">${Html.escape(person.buPrincipal)}</span>` : ''}</p>
            <p>${roles.length ? `Roles: ${roles.map(role => Html.tag(role)).join(' ')}` : '<span class="secondary">No role: they see every page, but none of its data.</span>'}</p>
        `;
    }

    async begin() {
        if ( ! this.chosen) {
            return;
        }
        this.startButton.disabled = true;
        try {
            await this.options.sendJson('POST', '/api/impersonation', { principal: this.chosen.buPrincipal });
            this.reload();
        } catch (error) {
            this.startButton.disabled = false;
            PageState.error(this.person, `${this.chosen.buPrincipal} could not be impersonated`, error.message, { compact: true });
        }
    }

    async stop() {
        try {
            await this.options.sendJson('DELETE', '/api/impersonation');
        } catch (error) {
            console.warn('Stopping impersonation failed:', error && error.message);
        }
        this.reload();
    }
}
