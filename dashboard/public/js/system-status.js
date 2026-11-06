/**
 * ServiceStatusTable - the health checks of the services the integration depends on (GET /api/status's
 * services), as a data table. The Status page and the Overview page both show it.
 *
 *   ServiceStatusTable.show(container, data.services);
 *
 * Markup goes through Html (page-state.js, loaded first). Tests: test/DashboardOverview.test.ts.
 */
class ServiceStatusTable {

    /** Display names for the services /api/status reports on */
    static get LABELS() {
        return {
            sourceApi: 'Source API (Boston University)',
            targetApi: 'Target API (Huron)',
            database: 'Database',
            s3Storage: 'S3 storage'
        };
    }

    static rowHtml([service, health]) {
        const check = health || {};
        const lastCheck = check.lastCheck ? new Date(check.lastCheck).toLocaleString() : '';
        return `
            <tr>
                <td>${Html.escape(ServiceStatusTable.LABELS[service] || service)}</td>
                <td>${Html.statusBadge(check.status)}</td>
                <td class="num">${check.responseTime === undefined ? '' : `${Html.escape(check.responseTime)} ms`}</td>
                <td class="secondary tabular-nums">${Html.escape(lastCheck)}</td>
            </tr>
        `;
    }

    /** The table for a services object ({ sourceApi: { status, responseTime, lastCheck }, ... }) */
    static html(services) {
        return `
            <div class="table-responsive">
                <table class="table data-table">
                    <thead>
                        <tr>
                            <th scope="col">Service</th>
                            <th scope="col">Status</th>
                            <th scope="col" class="num">Response time</th>
                            <th scope="col">Last checked</th>
                        </tr>
                    </thead>
                    <tbody>${Object.entries(services || {}).map(ServiceStatusTable.rowHtml).join('')}</tbody>
                </table>
            </div>
        `;
    }

    /** Show the table in a container, or the empty state when no service is reported */
    static show(container, services) {
        if (Object.keys(services || {}).length === 0) {
            PageState.empty(container, 'No services reported');
            return;
        }
        container.innerHTML = ServiceStatusTable.html(services);
    }
}
