// API Base URL - adjust for production vs development
const API_BASE = window.location.origin;

// Initialize components
let personCard;
let mappingExperimentCard;
let searchTypeManager;

// Tab management - use 'load' event instead of 'DOMContentLoaded' for Lambda compatibility
window.addEventListener('load', function() {
    console.log('Window load event fired');
    console.log('Document ready state:', document.readyState);
    console.log('Body exists:', !!document.body);
    
    // Check if mapping container exists
    const mappingContainer = document.getElementById('mappingExperimentContainer');
    console.log('mappingExperimentContainer element:', mappingContainer);
    
    // Initialize components
    personCard = new PersonCard('personResults');
    mappingExperimentCard = new MappingExperimentCard('mappingExperimentContainer');
    searchTypeManager = new SearchTypeManager();
    
    // Initialize mapping experiment form
    mappingExperimentCard.renderForm();

    // Landscape navigator: go to the chosen landscape's dashboard
    const landscapeNavigator = document.getElementById('landscapeNavigator');
    if (landscapeNavigator) {
        landscapeNavigator.addEventListener('change', function() {
            if (this.value) {
                window.location.assign(this.value);
            }
        });
    }
    
    // Add event listener for experiment preview button
    const experimentPreviewBtn = document.getElementById('experimentPreviewBtn');
    if (experimentPreviewBtn) {
        experimentPreviewBtn.addEventListener('click', handleMappingExperiment);
    }
    
    // Add person lookup form event listener
    const personLookupForm = document.getElementById('personLookupForm');
    if (personLookupForm) {
        personLookupForm.addEventListener('submit', handlePersonLookupSubmit);
    }
    
    initializePageNavigation();
});

/**
 * Sidebar page navigation.
 * The server renders one page active (?tab=<id>, else the default). A URL fragment takes precedence,
 * since the server never sees it: #<page>, optionally followed by /<detail> (e.g. #history/<id>), so a
 * page can be linked to and a refresh stays on it. Choosing a page records its fragment in the browser
 * history, so back and forward move between pages.
 */
function initializePageNavigation() {
    const navItems = Array.from(document.querySelectorAll('#dashboardTabs [data-page]'));
    const navItemFor = (page) => navItems.find(item => item.dataset.page === page);
    // The page the server rendered: where going back to a URL with no fragment returns to
    const renderedItem = navItems.find(item => item.classList.contains('active'));
    const renderedPage = renderedItem && renderedItem.dataset.page;

    // True while a page is being shown because the URL changed (back/forward, initial fragment): that
    // URL is already in the browser history, and pushing another entry would cut off "forward"
    let followingUrl = false;

    navItems.forEach(item => {
        item.addEventListener('shown.bs.tab', function () {
            const page = this.dataset.page;
            if ( ! followingUrl && pageFromFragment() !== page) {
                history.pushState(null, '', `#${page}`);
            }
            followingUrl = false;
            closeSidebarMenu();
            onPageShown(page);
        });
    });

    // Back/forward, or a fragment link followed from within the page
    const showPageFromFragment = () => {
        const item = navItemFor(pageFromFragment()) || navItemFor(renderedPage);
        if (item && !item.classList.contains('active')) {
            followingUrl = true;
            bootstrap.Tab.getOrCreateInstance(item).show();
        }
    };
    window.addEventListener('popstate', showPageFromFragment);
    window.addEventListener('hashchange', showPageFromFragment);

    // Initial page: the fragment's, else the one the server rendered active
    const requested = navItemFor(pageFromFragment());
    if (requested && !requested.classList.contains('active')) {
        followingUrl = true;
        bootstrap.Tab.getOrCreateInstance(requested).show(); // fires shown.bs.tab, which loads the page
    } else if (renderedPage) {
        onPageShown(renderedPage);
    }
}

/** The page named by the URL fragment (#<page>[/<detail>]), if any */
function pageFromFragment() {
    const fragment = decodeURIComponent(window.location.hash.replace(/^#/, ''));
    return fragment.split('/')[0] || undefined;
}

/** Load the content of a page that fetches its data when shown */
function onPageShown(page) {
    if (page === 'history') {
        loadHistory();
    } else if (page === 'system') {
        loadSystemStatus();
    }
}

/** Below the lg breakpoint the sidebar is an off-canvas menu: close it once a page is chosen */
function closeSidebarMenu() {
    const sidebar = document.getElementById('appSidebar');
    const menu = sidebar && bootstrap.Offcanvas.getInstance(sidebar);
    if (menu) {
        menu.hide();
    }
}

// Person Lookup Handler
async function handlePersonLookupSubmit(e) {
    e.preventDefault();
    
    if (!searchTypeManager) {
        console.error('SearchTypeManager not initialized');
        return;
    }
    
    const searchData = searchTypeManager.getSearchData();
    const form = e.target;
    
    // Validate required fields
    if (searchData.searchType === 'name') {
        if ((!searchData.firstName || searchData.firstName.trim() === '') && 
            (!searchData.lastName || searchData.lastName.trim() === '')) {
            // Show fading alert
            const alertDiv = document.createElement('div');
            alertDiv.id = 'validationAlert';
            alertDiv.className = 'alert alert-warning fade show mt-2';
            alertDiv.textContent = 'You must enter first name and/or last name';
            form.appendChild(alertDiv);
            // Auto fade after 3 seconds
            setTimeout(() => {
                alertDiv.classList.remove('show');
                setTimeout(() => alertDiv.remove(), 150);
            }, 3000);
            return;
        }
    } else if (!searchData.personId || searchData.personId.trim() === '') {
        alert('Please enter a search value');
        return;
    }
    
    // Hide any existing validation alert
    const alert = document.getElementById('validationAlert');
    if (alert) alert.remove();
    
    // Show loading state
    form.classList.add('tab-loading');
    personCard.renderWaiting();
    
    try {
        // All cookies (including auth-token-cookie) are sent automatically with fetch for same-origin requests.
        const response = await fetch(`${API_BASE}/api/person-lookup`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                personId: searchData.personId, 
                system: searchData.system,
                searchType: searchData.searchType,
                ...(searchData.firstName && { firstName: searchData.firstName }),
                ...(searchData.lastName && { lastName: searchData.lastName })
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            // Use PersonCard component to render results
            personCard.render(result, searchData.personId);
        } else {
            personCard.renderError('Error looking up person');
        }
    } catch (error) {
        personCard.renderError('Error looking up person');
        console.error('Lookup error:', error);
    } finally {
        form.classList.remove('tab-loading');
    }
}

// Sync individual person
async function syncPerson(personId, operation, hrn) {
    
    // Show loading modal
    showSyncLoadingModal();
    
    try {
        const requestBody = { personId, operation };
        
        // Include hrn if it exists (indicates target record exists for update)
        if (hrn) {
            requestBody.hrn = hrn;
        }
        
        const response = await fetch(`${API_BASE}/api/person-sync`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });
        
        const result = await response.json();

        // Hide loading modal
        await hideSyncLoadingModal();

        if (response.ok) {
            showSyncResultModal('Success', result, true);
        } else {
            showSyncResultModal('Error', result, false);
        }
    } catch (error) {
        // Hide loading modal
        await hideSyncLoadingModal();
        
        showSyncResultModal('Error', { error: 'Sync error', message: error.message }, false);
        console.error('Sync error:', error);
    }
}

// Preview sync for individual person
async function syncPersonPreview(personId, operation, hrn) {
    
    // Show loading modal
    showSyncLoadingModal('Preparing Sync Preview', 'Mapping the person for Huron. Nothing will be sent to Huron.');

    try {
        const requestBody = { personId, operation };

        // Include hrn if it exists
        if (hrn) {
            requestBody.hrn = hrn;
        }
        
        const response = await fetch(`${API_BASE}/api/person-sync/preview`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });
        
        const result = await response.json();
        
        // Hide loading modal
        await hideSyncLoadingModal();

        if (response.ok) {
            showPreviewModal(result);
        } else {
            showSyncResultModal('Preview Failed', result, false);
        }
    } catch (error) {
        // Hide loading modal
        await hideSyncLoadingModal();
        
        showSyncResultModal('Preview Error', { error: 'Preview error', message: error.message }, false);
        console.error('Preview error:', error);
    }
}

// Handle mapping experiment preview
async function handleMappingExperiment() {
    if (!mappingExperimentCard) {
        console.error('MappingExperimentCard not initialized');
        return;
    }
    
    const personJson = mappingExperimentCard.getPersonJson();
    
    // Validate JSON format and required fields
    const validation = mappingExperimentCard.validateJson(personJson);
    if (!validation.valid) {
        mappingExperimentCard.renderError(validation.error);
        return;
    }
    
    // Extract personId from JSON
    const personId = validation.personId;
    
    // Show loading state
    mappingExperimentCard.renderLoading();
    
    try {
        const response = await fetch(`${API_BASE}/api/person-sync/preview-experiment`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                personId: personId,
                personJson: personJson
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            mappingExperimentCard.renderResults(result);
        } else {
            const errorMsg = result.error || 'Unknown error';
            const detailMsg = result.message ? `: ${result.message}` : '';
            mappingExperimentCard.renderError(`Mapping failed: ${errorMsg}${detailMsg}`);
        }
    } catch (error) {
        mappingExperimentCard.renderError(`Request failed: ${error.message}`);
        console.error('Mapping experiment error:', error);
    }
}

// Show preview modal
function showPreviewModal(data) {
    // Create modal if it doesn't exist
    let modal = document.getElementById('previewModal');
    if (!modal) {
        modal = createPreviewModal();
        document.body.appendChild(modal);
    }
    
    // Update modal content
    const modalBody = modal.querySelector('.modal-body');
    modalBody.innerHTML = `<pre class="bg-light p-3 rounded" style="max-height: 400px; overflow-y: auto;">${JSON.stringify(data, null, 2)}</pre>`;
    
    // Show modal using Bootstrap
    const bootstrapModal = new bootstrap.Modal(modal);
    bootstrapModal.show();
}

// Resolves once the sync loading modal has finished appearing
let syncLoadingModalShown = Promise.resolve();

// Show sync loading modal, titled for what is being waited on (a sync or a preview)
function showSyncLoadingModal(title = 'Syncing Person Data', message = 'Please wait while the sync operation completes...') {
    let modal = document.getElementById('syncLoadingModal');
    if (!modal) {
        modal = createSyncLoadingModal();
        document.body.appendChild(modal);
    }
    modal.querySelector('.sync-loading-title').textContent = title;
    modal.querySelector('.sync-loading-message').textContent = message;

    const bootstrapModal = bootstrap.Modal.getOrCreateInstance(modal, {
        backdrop: 'static',
        keyboard: false
    });
    if (!modal.classList.contains('show')) {
        syncLoadingModalShown = new Promise(resolve => modal.addEventListener('shown.bs.modal', resolve, { once: true }));
        bootstrapModal.show();
    }
}

/**
 * Hide sync loading modal, resolving once it is gone, so the result modal that follows doesn't open
 * on top of it. Bootstrap ignores hide() while the modal is still fading in, which a fast response
 * (e.g. from the local mock services) would otherwise hit, leaving the loading modal stuck open.
 */
async function hideSyncLoadingModal() {
    const modal = document.getElementById('syncLoadingModal');
    const bootstrapModal = modal && bootstrap.Modal.getInstance(modal);
    if (!bootstrapModal) {
        return;
    }
    await syncLoadingModalShown;
    if (!modal.classList.contains('show')) {
        return;
    }
    const hidden = new Promise(resolve => modal.addEventListener('hidden.bs.modal', resolve, { once: true }));
    bootstrapModal.hide();
    await hidden;
}

// Parse nested JSON strings recursively
function parseNestedJson(obj) {
    if (obj === null || obj === undefined) {
        return obj;
    }
    
    // Handle arrays
    if (Array.isArray(obj)) {
        return obj.map(item => parseNestedJson(item));
    }
    
    // Handle objects
    if (typeof obj === 'object') {
        const result = {};
        for (const [key, value] of Object.entries(obj)) {
            result[key] = parseNestedJson(value);
        }
        return result;
    }
    
    // Handle strings that might contain JSON
    if (typeof obj === 'string') {
        const trimmed = obj.trim();
        // Check if string looks like JSON (starts with { or [)
        if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || 
            (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
            try {
                // Try to parse as JSON
                const parsed = JSON.parse(trimmed);
                // Recursively parse the result in case it contains more nested JSON
                return parseNestedJson(parsed);
            } catch (e) {
                // If parsing fails, return original string
                return obj;
            }
        }
    }
    
    // Return primitive values as-is
    return obj;
}

// Show sync result modal
function showSyncResultModal(title, data, isSuccess) {
    let modal = document.getElementById('syncResultModal');
    if (!modal) {
        modal = createSyncResultModal();
        document.body.appendChild(modal);
    }
    
    // Update modal title and styling
    const modalTitle = modal.querySelector('.modal-title');
    const modalHeader = modal.querySelector('.modal-header');
    const icon = isSuccess ? '<i class="fas fa-check-circle me-2"></i>' : '<i class="fas fa-exclamation-triangle me-2"></i>';
    
    modalTitle.innerHTML = `${icon}${title}`;
    modalHeader.className = `modal-header ${isSuccess ? 'bg-success' : 'bg-danger'} text-white`;
    
    // Parse nested JSON strings in the data
    const parsedData = parseNestedJson(data);
    
    // Update modal content
    const modalBody = modal.querySelector('.modal-body');
    modalBody.innerHTML = `<pre class="bg-light p-3 rounded" style="max-height: 400px; overflow-y: auto; white-space: pre-wrap;">${JSON.stringify(parsedData, null, 2)}</pre>`;
    
    // Show modal using Bootstrap
    const bootstrapModal = new bootstrap.Modal(modal);
    bootstrapModal.show();
}

// Create preview modal DOM structure
function createPreviewModal() {
    const modal = document.createElement('div');
    modal.id = 'previewModal';
    modal.className = 'modal fade';
    modal.setAttribute('tabindex', '-1');
    modal.setAttribute('aria-labelledby', 'previewModalLabel');
    modal.setAttribute('aria-hidden', 'true');
    
    modal.innerHTML = `
        <div class="modal-dialog modal-lg">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="previewModalLabel">
                        <i class="fas fa-eye me-2"></i>Sync Preview
                    </h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body">
                    <!-- Content will be dynamically inserted here -->
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                </div>
            </div>
        </div>
    `;
    
    return modal;
}

// Create sync loading modal DOM structure
function createSyncLoadingModal() {
    const modal = document.createElement('div');
    modal.id = 'syncLoadingModal';
    modal.className = 'modal fade';
    modal.setAttribute('tabindex', '-1');
    modal.setAttribute('aria-labelledby', 'syncLoadingModalLabel');
    modal.setAttribute('aria-hidden', 'true');
    
    modal.innerHTML = `
        <div class="modal-dialog modal-dialog-centered">
            <div class="modal-content">
                <div class="modal-body text-center py-5">
                    <div class="spinner-border text-primary mb-3" role="status" style="width: 3rem; height: 3rem;">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <h5 class="sync-loading-title"></h5>
                    <p class="text-muted sync-loading-message"></p>
                </div>
            </div>
        </div>
    `;
    
    return modal;
}

// Create sync result modal DOM structure
function createSyncResultModal() {
    const modal = document.createElement('div');
    modal.id = 'syncResultModal';
    modal.className = 'modal fade';
    modal.setAttribute('tabindex', '-1');
    modal.setAttribute('aria-labelledby', 'syncResultModalLabel');
    modal.setAttribute('aria-hidden', 'true');
    
    modal.innerHTML = `
        <div class="modal-dialog modal-lg">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="syncResultModalLabel">
                        Sync Result
                    </h5>
                    <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body">
                    <!-- Content will be dynamically inserted here -->
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                </div>
            </div>
        </div>
    `;
    
    return modal;
}

// Load activity history
async function loadHistory() {
    try {
        const response = await fetch(`${API_BASE}/api/history`);
        const data = await response.json();
        
        if (response.ok && data.history) {
            const historyHtml = data.history.map(item => `
                <div class="border-bottom pb-2 mb-2">
                    <div class="d-flex justify-content-between align-items-center">
                        <span><strong>${item.operation}</strong> - ${item.recordsProcessed} records</span>
                        <span class="badge bg-${item.status === 'completed' ? 'success' : 'danger'}">${item.status}</span>
                    </div>
                    <small class="text-muted">${new Date(item.timestamp).toLocaleString()} (${item.duration})</small>
                </div>
            `).join('');
            
            document.getElementById('historyContent').innerHTML = historyHtml || '<p class="text-muted">No history available</p>';
        }
    } catch (error) {
        document.getElementById('historyContent').innerHTML = '<div class="alert alert-danger">Error loading history</div>';
        console.error('History load error:', error);
    }
}

// Load system status
async function loadSystemStatus() {
    try {
        const response = await fetch(`${API_BASE}/api/status`);
        const data = await response.json();
        
        if (response.ok) {
            // Service status
            const servicesHtml = Object.entries(data.services).map(([service, status]) => `
                <div class="d-flex justify-content-between align-items-center mb-2 p-2 border rounded">
                    <span><span class="status-indicator status-${status.status}"></span>${service}</span>
                    <span class="text-muted">${status.responseTime}ms</span>
                </div>
            `).join('');
            
            document.getElementById('serviceStatus').innerHTML = servicesHtml;
            
            // Metrics
            const metricsHtml = `
                <div class="text-center">
                    <div class="metric-card p-3 mb-2 bg-light rounded">
                        <h4>${data.metrics.dailySyncs}</h4>
                        <small>Daily Syncs</small>
                    </div>
                    <div class="metric-card p-3 mb-2 bg-light rounded">
                        <h4>${data.metrics.weeklyErrors}</h4>
                        <small>Weekly Errors</small>
                    </div>
                    <div class="metric-card p-3 bg-light rounded">
                        <h4>${data.metrics.avgProcessingTime}</h4>
                        <small>Avg Processing Time</small>
                    </div>
                </div>
            `;
            
            document.getElementById('systemMetrics').innerHTML = metricsHtml;
        }
    } catch (error) {
        document.getElementById('serviceStatus').innerHTML = '<div class="alert alert-danger">Error loading status</div>';
        console.error('Status load error:', error);
    }
}

// Refresh functions
function refreshHistory() {
    document.getElementById('historyContent').innerHTML = '<div class="text-center"><div class="spinner-border text-primary"></div></div>';
    loadHistory();
}
// Coming back to this page from the browser's history shows the landscape it was left for:
// select this landscape again
window.addEventListener('pageshow', function() {
    const landscapeNavigator = document.getElementById('landscapeNavigator');
    if (landscapeNavigator) {
        Array.from(landscapeNavigator.options).forEach(option => option.selected = option.defaultSelected);
    }
});
