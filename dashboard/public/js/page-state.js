/**
 * PageState - the dashboard's one pattern for a section that has no content to show: it is loading, there is
 * nothing to show, the content could not be fetched, or the viewer's role doesn't allow it (redacted).
 *
 * Every page and component renders these states through this class, so they look and read the same
 * everywhere. Text is set with textContent, never as HTML, so a message may safely carry server output.
 *
 *   PageState.loading('historyContent', 'Loading activity history');
 *   PageState.error(container, 'Activity history could not be loaded', error.message);
 *   container.appendChild(PageState.element('empty', { title: 'No runs yet' }));
 */
class PageState {

    /** The states, with the icon and default wording each one uses */
    static get KINDS() {
        return {
            loading: { icon: null, title: 'Loading', role: 'status' },
            empty: { icon: 'fa-inbox', title: 'Nothing to show', role: null },
            error: { icon: 'fa-circle-exclamation', title: 'Something went wrong', role: 'alert' },
            redacted: {
                icon: 'fa-lock',
                title: 'Content redacted',
                message: 'Your role does not allow you to view this content. Ask a dashboard administrator to give you a role, or to raise the one you have.',
                role: null
            }
        };
    }

    /**
     * Build a state element.
     * @param {'loading'|'empty'|'error'|'redacted'} kind
     * @param {{ title?: string, message?: string, compact?: boolean }} [options] title replaces the kind's
     *   default heading; message is a line of explanation below it; compact renders a single line with less
     *   padding, for a state inside a form or beside other content.
     * @returns {HTMLElement}
     */
    static element(kind, options = {}) {
        const definition = PageState.KINDS[kind];
        if ( ! definition) {
            throw new Error(`PageState: unknown state '${kind}'`);
        }
        const { title = definition.title, message = definition.message, compact = false } = options;

        const state = document.createElement('div');
        state.className = `page-state page-state-${kind}${compact ? ' page-state-compact' : ''}`;
        state.dataset.state = kind;
        if (definition.role) {
            state.setAttribute('role', definition.role);
        }

        const icon = document.createElement(definition.icon ? 'i' : 'span');
        icon.className = definition.icon
            ? `fas ${definition.icon} page-state-icon`
            : 'spinner-border spinner-border-sm page-state-icon';
        icon.setAttribute('aria-hidden', 'true');
        state.appendChild(icon);

        const text = document.createElement('div');
        text.className = 'page-state-text';
        const heading = document.createElement('p');
        heading.className = 'page-state-title';
        heading.textContent = title;
        text.appendChild(heading);
        if (message) {
            const detail = document.createElement('p');
            detail.className = 'page-state-message';
            detail.textContent = message;
            text.appendChild(detail);
        }
        state.appendChild(text);
        return state;
    }

    /**
     * Replace a container's content with a state.
     * @param {HTMLElement|string} target the container, or its id
     * @returns {HTMLElement|null} the state element, or null if there is no such container
     */
    static show(target, kind, options = {}) {
        const container = typeof target === 'string' ? document.getElementById(target) : target;
        if ( ! container) {
            return null;
        }
        const state = PageState.element(kind, options);
        container.replaceChildren(state);
        return state;
    }

    static loading(target, title, options = {}) {
        return PageState.show(target, 'loading', { ...options, title });
    }

    static empty(target, title, message, options = {}) {
        return PageState.show(target, 'empty', { ...options, title, message });
    }

    static error(target, title, message, options = {}) {
        return PageState.show(target, 'error', { ...options, title, message });
    }

    /** The viewer's role doesn't allow this content (a 403 answering the section's data load) */
    static redacted(target, options = {}) {
        return PageState.show(target, 'redacted', options);
    }
}

/**
 * RefreshStamp - the "Refreshed HH:MM" beside a page's Refresh button, saying how fresh what the page shows is.
 * Pages refresh only when asked (a run that never finishes would keep a polling page busy for ever), so the
 * time tells the reader when to ask again.
 *
 *   RefreshStamp.set(document.getElementById('systemRefreshed'));
 */
class RefreshStamp {

    static text(date) {
        return `Refreshed ${date.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}`;
    }

    /** Stamp an element with a time (now by default); its title gives the full date and time */
    static set(element, date = new Date()) {
        if ( ! element) {
            return;
        }
        element.textContent = RefreshStamp.text(date);
        element.setAttribute('title', `Refreshed ${date.toLocaleString()}`);
    }
}
