/**
 * The Administration pages and the who-am-I dialog (authorization-plan.md, decisions 3, 9, 10 and 11):
 *
 *   WhoAmI          GET /api/me: the top bar's who-am-I dialog, and the notice for a user with no role
 *   AccessPage      who holds which roles: look a person up, check their roles, save; everyone with a role
 *   AuditTrailPage  the audit trail, newest first, filtered by person and action, a page at a time
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
        return `
            <dl class="who-am-i-identity">
                ${identity.map(([label, value]) => `<dt>${Html.escape(label)}</dt><dd>${Html.escape(value)}</dd>`).join('')}
            </dl>
            <h3 class="who-am-i-heading">Your roles in this landscape</h3>
            ${roles.length === 0
                ? `<p class="secondary">None. You can see every page, but not its data. Ask a dashboard administrator for a role.</p>`
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
            this.person.innerHTML = this.personHtml(person);
            this.person.querySelector('[data-access-form]').addEventListener('submit', event => {
                event.preventDefault();
                this.save();
            });
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
                    <legend class="form-label">Roles</legend>
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
                    <span class="secondary">${Html.escape(role.description)}: ${Html.escape((role.policies || []).map(policy => policy.name).join(', '))}</span>
                </label>
            </div>
        `;
    }

    /** The roles checked in the form */
    checkedRoles() {
        return Array.from(this.person.querySelectorAll('input[name="role"]:checked')).map(input => input.value);
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
