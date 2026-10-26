/**
 * PersonCard - DOM-based person lookup results rendering
 * Replaces innerHTML string concatenation with proper DOM manipulation
 */

// Global cache for raw JSON data (accessible for debugging/clipboard operations)
window.cachedSourceData = null;
window.cachedTargetData = null;

class PersonCard {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.currentData = null;
        this.searchPersonId = null; // Store the original search personId
        this.targetHrn = undefined; // Store the target hrn if it exists
    }

    /**
     * Render person lookup results using DOM manipulation
     */
    render(data, searchPersonId = null) {
        this.currentData = data;

        const { sourceData } = data || {};
        if(sourceData && Array.isArray(sourceData) && sourceData.length === 1 && sourceData[0].personid) {
            // Extract the actual personId from the source data
            this.searchPersonId = sourceData[0].personid;
        }
        else {
            /**
             * No single sourceData result with a valid personid, so use the original search term.
             * If the systemType radio selection was target, searchPersonId will probably not have 
             * an actual source personid value, but a name, email, etc. value. This is still useful 
             * to store so that if the user clicks "Sync Preview" or "Sync Now", we can display a 
             * message like "No person ID found for [search term]" instead of just "No person ID found".
             */
            this.searchPersonId = searchPersonId;
        }
        
        // Clear any previously cached hrn from prior lookups
        this.targetHrn = undefined;
        
        // Extract and store target hrn if target data exists
        // This handles Scenario 1: Single source lookup with corresponding target record
        if (data.targetData && Array.isArray(data.targetData) && data.targetData.length > 0) {
            // Get hrn from first target record (only used if there's exactly one target record)
            this.targetHrn = data.targetData[0].hrn || data.targetData[0].id || null;
        }

        this._renderContent();
    }

    /**
     * Internal method to render content without resetting state
     */
    _renderContent() {
        // Clear existing content
        this.container.innerHTML = '';
        
        const hasSourceData = this.currentData.sourceData && Array.isArray(this.currentData.sourceData) && this.currentData.sourceData.length > 0;
        const hasTargetData = this.currentData.targetData && Array.isArray(this.currentData.targetData) && this.currentData.targetData.length > 0;
        
        if (!hasSourceData && !hasTargetData) {
            // Clear global cache when no data
            window.cachedSourceData = null;
            window.cachedTargetData = null;
            this.renderNotFound();
            return;
        }
        
        // Update global cache with current data (extract first element if array)
        if (hasSourceData) {
            const sourceData = this.currentData.sourceData;
            window.cachedSourceData = Array.isArray(sourceData) && sourceData.length > 0 ? sourceData[0] : sourceData;
        } else {
            window.cachedSourceData = null;
        }
        
        if (hasTargetData) {
            const targetData = this.currentData.targetData;
            window.cachedTargetData = Array.isArray(targetData) && targetData.length > 0 ? targetData[0] : targetData;
        } else {
            window.cachedTargetData = null;
        }
        
        this.renderResults(this.currentData, hasSourceData, hasTargetData);
    }

    /**
     * Render "not found" message
     */
    renderNotFound() {
        // Clear global cache
        window.cachedSourceData = null;
        window.cachedTargetData = null;
        
        const alert = document.createElement('div');
        alert.className = 'alert alert-warning';
        
        const icon = document.createElement('i');
        icon.className = 'fas fa-exclamation-triangle me-2';
        
        alert.appendChild(icon);
        alert.appendChild(document.createTextNode('Person not found in either source or target systems'));
        
        this.container.appendChild(alert);
    }

    /**
     * Render waiting/loading message
     */
    renderWaiting() {
        this.container.innerHTML = '';
        
        // Clear global cache while waiting
        window.cachedSourceData = null;
        window.cachedTargetData = null;
        
        const alert = document.createElement('div');
        alert.className = 'alert alert-info';
        
        const icon = document.createElement('i');
        icon.className = 'fas fa-search me-2';
        
        alert.appendChild(icon);
        alert.appendChild(document.createTextNode('Looking up person...'));
        
        this.container.appendChild(alert);
    }

    /**
     * Render error message
     */
    renderError(message = 'Error looking up person') {
        this.container.innerHTML = '';
        
        // Clear global cache on error
        window.cachedSourceData = null;
        window.cachedTargetData = null;
        
        const alert = document.createElement('div');
        alert.className = 'alert alert-danger';
        
        const icon = document.createElement('i');
        icon.className = 'fas fa-exclamation-circle me-2';
        
        alert.appendChild(icon);
        alert.appendChild(document.createTextNode(message));
        
        this.container.appendChild(alert);
    }

    /**
     * Render the main results structure
     */
    renderResults(data, hasSourceData, hasTargetData) {
        // Main container
        const resultDiv = document.createElement('div');
        resultDiv.className = 'person-result p-3 rounded';
        
        // Header
        const header = document.createElement('h6');
        const headerIcon = document.createElement('i');
        headerIcon.className = 'fas fa-user me-2';
        header.appendChild(headerIcon);
        header.appendChild(document.createTextNode('Lookup Results'));
        
        // Row container
        const row = document.createElement('div');
        row.className = 'row';
        
        // Source column
        if (hasSourceData) {
            row.appendChild(this.createSourceColumn(data.sourceData));
        } else {
            row.appendChild(this.createEmptySourceColumn());
        }

        
        // Target column
        if (hasTargetData) {
            row.appendChild(this.createTargetColumn(data.targetData));
        } else {
            row.appendChild(this.createEmptyTargetColumn());
        }
        
        // Sync button (only if source data exists)
        if (hasSourceData) {
            const syncContainer = this.createSyncButton();
            resultDiv.appendChild(header);
            resultDiv.appendChild(row);
            resultDiv.appendChild(syncContainer);
        } else {
            resultDiv.appendChild(header);
            resultDiv.appendChild(row);
        }
        
        this.container.appendChild(resultDiv);
    }

    /**
     * Create source data column
     */
    createSourceColumn(sourceDataArray) {
        const col = document.createElement('div');
        col.className = 'col-md-6';
        
        const box = document.createElement('div');
        box.className = 'border rounded p-2 bg-light position-relative';
        box.style.overflowX = 'auto';
        box.style.overflowY = 'auto';
        box.style.whiteSpace = 'nowrap';
        
        // Header
        const header = document.createElement('strong');
        header.className = 'text-success d-flex align-items-center';
        
        const icon = document.createElement('i');
        icon.className = 'fas fa-check-circle me-1';
        
        header.appendChild(icon);
        header.appendChild(document.createTextNode('Source Data Found:'));
        header.style.marginBottom = '0.5rem';
        
        // Add copy button
        const copyBtn = document.createElement('button');
        copyBtn.className = 'btn btn-sm btn-outline-warning ms-2';
        copyBtn.style.padding = '0.15rem 0.5rem';
        copyBtn.style.fontSize = '0.75rem';
        copyBtn.style.backgroundColor = '#f8f5e0';
        copyBtn.style.borderColor = '#d4a843';
        copyBtn.style.color = '#856404';
        copyBtn.title = 'Copy source JSON to clipboard';
        copyBtn.innerHTML = '<i class="fas fa-copy"></i> Copy to clipboard';
        copyBtn.onclick = () => this.copyToClipboard(window.cachedSourceData, 'source');
        header.appendChild(copyBtn);
        
        // Data list
        const list = this.createSourceDataList(sourceDataArray);
        
        box.appendChild(header);
        box.appendChild(list);
        col.appendChild(box);
        
        return col;
    }

    /**
     * Create target data column
     */
    createTargetColumn(targetDataArray) {
        const col = document.createElement('div');
        col.className = 'col-md-6';
        
        const box = document.createElement('div');
        box.className = 'border rounded p-2 bg-light position-relative';
        box.style.overflowX = 'auto';
        box.style.overflowY = 'auto';
        box.style.whiteSpace = 'nowrap';
        box.style.marginBottom = '0.5rem';
        
        // Header
        const header = document.createElement('strong');
        header.className = 'text-success d-flex align-items-center';
        
        const icon = document.createElement('i');
        icon.className = 'fas fa-check-circle me-1';
        
        header.appendChild(icon);
        header.appendChild(document.createTextNode('Target Data Found:'));
        header.style.marginBottom = '0.5rem';
        
        // Add copy button
        const copyBtn = document.createElement('button');
        copyBtn.className = 'btn btn-sm btn-outline-warning ms-2';
        copyBtn.style.padding = '0.15rem 0.5rem';
        copyBtn.style.fontSize = '0.75rem';
        copyBtn.style.backgroundColor = '#f8f5e0';
        copyBtn.style.borderColor = '#d4a843';
        copyBtn.style.color = '#856404';
        copyBtn.title = 'Copy target JSON to clipboard';
        copyBtn.innerHTML = '<i class="fas fa-copy"></i> Copy to clipboard';
        copyBtn.onclick = () => this.copyToClipboard(window.cachedTargetData, 'target');
        header.appendChild(copyBtn);
        
        // Data list
        const list = this.createTargetDataList(targetDataArray);
        
        box.appendChild(header);
        box.appendChild(list);
        col.appendChild(box);
        
        return col;
    }

    /**
     * Copy JSON data to clipboard
     */
    async copyToClipboard(data, type) {
        try {
            const jsonString = JSON.stringify(data, null, 2);
            await navigator.clipboard.writeText(jsonString);
            
            // Show temporary success message
            const toast = document.createElement('div');
            toast.className = 'position-fixed top-0 end-0 m-3 alert alert-success';
            toast.style.zIndex = '9999';
            toast.innerHTML = `<i class="fas fa-check-circle me-2"></i>${type.charAt(0).toUpperCase() + type.slice(1)} JSON copied to clipboard!`;
            document.body.appendChild(toast);
            
            setTimeout(() => toast.remove(), 2000);
        } catch (err) {
            console.error('Failed to copy:', err);
            alert('Failed to copy to clipboard');
        }
    }

    /**
     * Create empty source column
     */
    createEmptySourceColumn() {
        const col = document.createElement('div');
        col.className = 'col-md-6';
        
        const box = document.createElement('div');
        box.className = 'border rounded p-2 bg-light';
        
        const targetData = this.currentData.targetData;
        if (targetData && Array.isArray(targetData) && targetData.length > 1) {
            const info = document.createElement('div');
            info.className = 'text-info';
            
            const icon = document.createElement('i');
            icon.className = 'fas fa-info-circle me-1';
            
            info.appendChild(icon);
            info.appendChild(document.createTextNode('Select from the right'));
            
            box.appendChild(info);
        } else {
            const warning = document.createElement('div');
            warning.className = 'text-warning';
            
            const icon = document.createElement('i');
            icon.className = 'fas fa-exclamation-triangle me-1';
            
            warning.appendChild(icon);
            warning.appendChild(document.createTextNode('Source: Person not found'));
            
            box.appendChild(warning);
        }
        
        col.appendChild(box);
        
        return col;
    }

    /**
     * Create empty target column
     */
    createEmptyTargetColumn() {
        const col = document.createElement('div');
        col.className = 'col-md-6';
        
        const box = document.createElement('div');
        box.className = 'border rounded p-2 bg-light';
        
        const warning = document.createElement('div');
        warning.className = 'text-warning';
        
        const icon = document.createElement('i');
        icon.className = 'fas fa-exclamation-triangle me-1';
        
        warning.appendChild(icon);
        warning.appendChild(document.createTextNode('Target: Person not found'));
        
        box.appendChild(warning);
        col.appendChild(box);
        
        return col;
    }

    /**
     * Create source data list - handles array of person objects
     */
    createSourceDataList(sourceDataArray) {
        const container = document.createElement('div');
        container.className = 'person-list-container';
        
        if (!Array.isArray(sourceDataArray) || sourceDataArray.length === 0) {
            const emptyMsg = document.createElement('div');
            emptyMsg.className = 'text-muted';
            emptyMsg.textContent = 'No source data available';
            container.appendChild(emptyMsg);
            return container;
        }
        
        sourceDataArray.forEach((personData, index) => {
            const personBox = this.createPersonDataBox(personData, 'source', index);
            container.appendChild(personBox);
        });
        
        return container;
    }

    /**
     * Create target data list - handles array of person objects
     */
    createTargetDataList(targetDataArray) {
        const container = document.createElement('div');
        container.className = 'person-list-container';
        
        if (!Array.isArray(targetDataArray) || targetDataArray.length === 0) {
            const emptyMsg = document.createElement('div');
            emptyMsg.className = 'text-muted';
            emptyMsg.textContent = 'No target data available';
            container.appendChild(emptyMsg);
            return container;
        }
        
        targetDataArray.forEach((personData, index) => {
            const personBox = this.createPersonDataBox(personData, 'target', index);
            if (targetDataArray.length > 1) {
                const button = document.createElement('button');
                button.className = 'btn btn-sm btn-outline-primary mt-2';
                button.innerHTML = '<i class="fas fa-arrow-left me-1"></i>CDM lookup';
                button.addEventListener('click', () => this.performCdmLookup(personData, personBox));
                personBox.appendChild(button);
            }
            container.appendChild(personBox);
        });
        
        return container;
    }

    /**
     * Create an expandable box for a single person's data
     */
    createPersonDataBox(personData, type, index) {
        const box = document.createElement('div');
        box.className = 'person-data-box border rounded p-2 mb-2 bg-light';
        
        // Extract name and identifier
        const { name, identifier } = this.extractPersonInfo(personData, type);
        const displayLabel = `${name} - ${identifier}`;
        
        // Header with toggle
        const header = document.createElement('div');
        header.className = 'd-flex justify-content-between align-items-center cursor-pointer';
        header.style.cursor = 'pointer';
        
        const title = document.createElement('strong');
        title.className = 'text-primary';
        title.textContent = displayLabel;
        title.title = displayLabel; // Full text on hover in case it's long
        
        const toggleIcon = document.createElement('i');
        toggleIcon.className = 'fas fa-chevron-down';
        
        header.appendChild(title);
        header.appendChild(toggleIcon);
        
        // Content area (initially hidden)
        const content = document.createElement('div');
        content.className = 'person-details mt-2';
        content.style.display = 'none';
        
        // Add click handler to toggle
        header.addEventListener('click', () => {
            const isExpanded = content.style.display !== 'none';
            content.style.display = isExpanded ? 'none' : 'block';
            toggleIcon.className = isExpanded ? 'fas fa-chevron-down' : 'fas fa-chevron-up';
        });
        
        // Render all fields recursively
        this.renderObjectFields(personData, content, 0);
        
        box.appendChild(header);
        box.appendChild(content);
        
        return box;
    }

    /**
     * Extract person's name and identifier from data
     */
    extractPersonInfo(personData, type) {
        let name = 'Unknown Person';
        let identifier = 'N/A';
        
        if (type === 'source') {
            // For source data, identifier is personid
            identifier = personData.personid || 'N/A';
            
            // Try to get full name from personBasic.names array
            if (personData.personBasic && personData.personBasic.names && Array.isArray(personData.personBasic.names)) {
                // Look for PRI (Primary) name first, then any name
                const priName = personData.personBasic.names.find(n => n.nameType === 'PRI');
                const anyName = personData.personBasic.names[0];
                const nameObj = priName || anyName;
                
                if (nameObj && nameObj.fullName) {
                    name = nameObj.fullName;
                } else if (nameObj && nameObj.firstName && nameObj.lastName) {
                    name = `${nameObj.firstName} ${nameObj.lastName}`;
                }
            }
        } else if (type === 'target') {
            // For target data, identifier is hrn
            identifier = personData.hrn || personData.id || 'N/A';
            
            // Try to get name from firstName and lastName
            if (personData.firstName && personData.lastName) {
                name = `${personData.firstName} ${personData.lastName}`;
            } else if (personData.firstName) {
                name = personData.firstName;
            } else if (personData.lastName) {
                name = personData.lastName;
            }
        }
        
        return { name, identifier };
    }

    /**
     * Recursively render object fields with proper indentation
     */
    renderObjectFields(obj, container, depth = 0) {
        if (obj === null || obj === undefined) {
            const nullValue = document.createElement('div');
            nullValue.className = 'text-muted';
            nullValue.style.marginLeft = `${depth * 20}px`;
            nullValue.textContent = 'null';
            container.appendChild(nullValue);
            return;
        }
        
        if (typeof obj !== 'object') {
            // Primitive value
            const value = document.createElement('div');
            value.style.marginLeft = `${depth * 20}px`;
            value.className = 'field-value';
            
            if (typeof obj === 'string' && obj.length > 50) {
                // Long strings get truncated with expand option
                const truncated = obj.substring(0, 50) + '...';
                value.textContent = `"${truncated}"`;
                value.title = obj; // Full text on hover
                value.style.cursor = 'pointer';
                value.addEventListener('click', () => {
                    if (value.textContent.endsWith('...')) {
                        value.textContent = `"${obj}"`;
                    } else {
                        value.textContent = `"${truncated}"`;
                    }
                });
            } else {
                value.textContent = typeof obj === 'string' ? `"${obj}"` : String(obj);
            }
            
            container.appendChild(value);
            return;
        }
        
        if (Array.isArray(obj)) {
            // Array
            if (obj.length === 0) {
                const emptyArray = document.createElement('div');
                emptyArray.className = 'text-muted';
                emptyArray.style.marginLeft = `${depth * 20}px`;
                emptyArray.textContent = '[] (empty array)';
                container.appendChild(emptyArray);
                return;
            }
            
            obj.forEach((item, index) => {
                const arrayItem = document.createElement('div');
                arrayItem.style.marginLeft = `${depth * 20}px`;
                
                const itemHeader = document.createElement('div');
                itemHeader.className = 'array-item-header fw-bold';
                itemHeader.textContent = `[${index}]`;
                itemHeader.style.cursor = 'pointer';
                
                const itemContent = document.createElement('div');
                itemContent.className = 'array-item-content';
                itemContent.style.display = 'none';
                
                itemHeader.addEventListener('click', () => {
                    const isExpanded = itemContent.style.display !== 'none';
                    itemContent.style.display = isExpanded ? 'none' : 'block';
                    itemHeader.classList.toggle('text-primary');
                });
                
                this.renderObjectFields(item, itemContent, depth + 1);
                
                arrayItem.appendChild(itemHeader);
                arrayItem.appendChild(itemContent);
                container.appendChild(arrayItem);
            });
        } else {
            // Object
            const entries = Object.entries(obj);
            if (entries.length === 0) {
                const emptyObj = document.createElement('div');
                emptyObj.className = 'text-muted';
                emptyObj.style.marginLeft = `${depth * 20}px`;
                emptyObj.textContent = '{} (empty object)';
                container.appendChild(emptyObj);
                return;
            }
            
            entries.forEach(([key, value]) => {
                const field = document.createElement('div');
                field.style.marginLeft = `${depth * 20}px`;
                
                const fieldHeader = document.createElement('div');
                fieldHeader.className = 'field-header fw-bold text-success';
                fieldHeader.textContent = `${key}:`;
                fieldHeader.style.cursor = 'pointer';
                
                const fieldContent = document.createElement('div');
                fieldContent.className = 'field-content';
                
                // For simple values, show inline
                if (typeof value !== 'object' || value === null) {
                    fieldHeader.classList.add('d-inline');
                    const valueSpan = document.createElement('span');
                    valueSpan.className = 'ms-2 text-dark fw-normal';
                    if (typeof value === 'string') {
                        valueSpan.textContent = `"${value}"`;
                    } else {
                        valueSpan.textContent = String(value);
                    }
                    fieldHeader.appendChild(valueSpan);
                } else {
                    // For complex objects/arrays, make expandable
                    fieldContent.style.display = 'none';
                    fieldHeader.addEventListener('click', () => {
                        const isExpanded = fieldContent.style.display !== 'none';
                        fieldContent.style.display = isExpanded ? 'none' : 'block';
                        fieldHeader.classList.toggle('text-primary');
                    });
                    
                    this.renderObjectFields(value, fieldContent, depth + 1);
                }
                
                field.appendChild(fieldHeader);
                if (fieldContent.children.length > 0) {
                    field.appendChild(fieldContent);
                }
                container.appendChild(field);
            });
        }
    }

    /**
     * Create sync buttons (Preview and Sync Now)
     */
    createSyncButton(personId = null) {
        const container = document.createElement('div');
        container.className = 'mt-3';
        
        // Create button group
        const buttonGroup = document.createElement('div');
        buttonGroup.className = 'd-flex gap-2';
        
        // Create Preview button
        const previewButton = document.createElement('button');
        previewButton.className = 'btn btn-sm btn-outline-warning';
        previewButton.style.backgroundColor = '#f8f5e0';
        previewButton.style.borderColor = '#d4a843';
        previewButton.style.color = '#856404';
        
        const previewIcon = document.createElement('i');
        previewIcon.className = 'fas fa-eye me-1';
        
        previewButton.appendChild(previewIcon);
        previewButton.appendChild(document.createTextNode('Sync Preview'));
        
        // Create Sync Now button
        const syncButton = document.createElement('button');
        syncButton.className = 'btn btn-sm btn-primary';
        
        const syncIcon = document.createElement('i');
        syncIcon.className = 'fas fa-sync me-1';
        
        syncButton.appendChild(syncIcon);
        syncButton.appendChild(document.createTextNode('Sync Now'));
        
        // Add event listener for preview functionality
        previewButton.addEventListener('click', () => {
            // Always use the current searchPersonId to avoid closure issues
            let syncPersonId = this.searchPersonId;
            let hrn = this.targetHrn;
            
            if (syncPersonId && window.syncPersonPreview) {
                // Determine operation: 'update' if hrn exists, 'create' otherwise
                const operation = hrn ? 'update' : 'create';
                window.syncPersonPreview(syncPersonId, operation, hrn);
            } else {
                console.error('No person ID available for sync preview. searchPersonId:', this.searchPersonId);
            }
        });
        
        // Add event listener for sync functionality
        syncButton.addEventListener('click', () => {
            // Always use the current searchPersonId to avoid closure issues
            let syncPersonId = this.searchPersonId;
            let hrn = this.targetHrn;
            
            if (syncPersonId && window.syncPerson) {
                // Determine operation: 'update' if hrn exists, 'create' otherwise
                const operation = hrn ? 'update' : 'create';
                window.syncPerson(syncPersonId, operation, hrn);
            } else {
                console.error('No person ID available for sync. searchPersonId:', this.searchPersonId);
            }
        });
        
        buttonGroup.appendChild(previewButton);
        buttonGroup.appendChild(syncButton);
        container.appendChild(buttonGroup);
        return container;
    }

    // Perform CDM lookup for a specific target person
    async performCdmLookup(personData, personBox) {
        const { id, sourceIdentifier, hrn } = personData;

        const isABuid = (id) => /^U[0-9]{8}$/.test(id);

        let buid = sourceIdentifier;
        if( ! isABuid(buid)) {
            buid = id;
        }
        if( ! isABuid(buid)) {
            alert(`No valid BUID found in person data for id: ${hrn}`);
            return;
        }

        // Show loading
        const loading = document.createElement('div');
        loading.className = 'mt-2 text-info';
        loading.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Looking up CDM data...';
        personBox.appendChild(loading);

        try {
            const API_BASE = window.location.origin;
            const response = await fetch(`${API_BASE}/api/person-lookup`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ 
                    personId: buid, 
                    system: 'source-only',
                    searchType: 'buid'
                })
            });

            const result = await response.json();

            loading.remove();

            if (response.ok && result.sourceData && result.sourceData.length > 0) {
                // Update global cache with the newly retrieved source data (extract first element if array)
                const sourceData = result.sourceData;
                window.cachedSourceData = Array.isArray(sourceData) && sourceData.length > 0 ? sourceData[0] : sourceData;
                
                // Find and update the existing source column
                const row = this.container.querySelector('.person-result .row');
                if (row) {
                    const sourceCol = row.querySelector('.col-md-6:first-child');
                    if (sourceCol) {
                        // Replace the empty source column content with actual data
                        sourceCol.innerHTML = '';
                        const sourceBox = this.createSourceColumn(result.sourceData);
                        sourceCol.appendChild(sourceBox.firstElementChild); // Get the box content, not the col wrapper
                    }
                }
                
                // Update the personId element value with the BUID used for lookup
                const personIdElement = document.getElementById('personId');
                if (personIdElement) {
                    personIdElement.value = buid;
                }
                
                // Update the stored searchPersonId for sync operations
                this.searchPersonId = buid;
                
                // Cache the hrn from the specific target person that was clicked
                // This handles Scenario 2: Multiple target results, user clicked CDM lookup for this specific one
                this.targetHrn = personData.hrn || personData.id || null;
                
                // Add sync button if it doesn't exist yet
                const resultDiv = this.container.querySelector('.person-result');
                if (resultDiv && !resultDiv.querySelector('.btn-primary')) {
                    const syncContainer = this.createSyncButton(buid);
                    resultDiv.appendChild(syncContainer);
                }
            } else {
                // Update the existing source column to show "no data found"
                const row = this.container.querySelector('.person-result .row');
                if (row) {
                    const sourceCol = row.querySelector('.col-md-6:first-child');
                    if (sourceCol) {
                        const box = sourceCol.querySelector('.border');
                        if (box) {
                            box.innerHTML = '';
                            const noData = document.createElement('div');
                            noData.className = 'text-warning';
                            noData.innerHTML = '<i class="fas fa-exclamation-triangle me-1"></i>CDM: No source data found';
                            box.appendChild(noData);
                        }
                    }
                }
                // Clear cached hrn since there's no matching source record
                this.targetHrn = undefined;
            }
        } catch (error) {
            loading.remove();
            const row = this.container.querySelector('.person-result .row');
            if (row) {
                const sourceCol = row.querySelector('.col-md-6:first-child');
                if (sourceCol) {
                    const box = sourceCol.querySelector('.border');
                    if (box) {
                        box.innerHTML = '';
                        const errorMsg = document.createElement('div');
                        errorMsg.className = 'text-danger';
                        errorMsg.innerHTML = '<i class="fas fa-exclamation-circle me-1"></i>Error looking up CDM data';
                        box.appendChild(errorMsg);
                    }
                }
            }
            // Clear cached hrn on error
            this.targetHrn = undefined;
            console.error('CDM lookup error:', error);
        }
    }
}

/**
 * SearchTypeManager - Handles dynamic search type options for Huron system
 */
class SearchTypeManager {
    constructor() {
        this.searchTypeContainer = document.getElementById('searchTypeContainer');
        this.inputFieldContainer = document.getElementById('inputFieldContainer');
        this.currentSearchType = 'hrn'; // default
        this.init();
    }

    init() {
        // Add event listeners for system type radio buttons
        const buidRadio = document.getElementById('buidRadio');
        const hrnRadio = document.getElementById('hrnRadio');
        
        if (buidRadio && hrnRadio) {
            buidRadio.addEventListener('change', () => this.handleSystemTypeChange());
            hrnRadio.addEventListener('change', () => this.handleSystemTypeChange());
        }
    }

    handleSystemTypeChange() {
        const hrnRadio = document.getElementById('hrnRadio');
        
        if (hrnRadio && hrnRadio.checked) {
            this.showSearchTypeOptions();
        } else {
            this.hideSearchTypeOptions();
            this.resetToSingleInput();
        }
    }

    showSearchTypeOptions() {
        if (!this.searchTypeContainer) return;
        
        // Clear existing content
        this.searchTypeContainer.innerHTML = '';
        
        // Create main container
        const container = document.createElement('div');
        container.className = 'border rounded p-2 bg-light';
        
        // Create label
        const label = document.createElement('small');
        label.className = 'text-muted d-block mb-2';
        label.textContent = 'Search by:';
        container.appendChild(label);
        
        // Create radio button group container
        const radioGroup = document.createElement('div');
        radioGroup.className = 'd-flex gap-3 flex-wrap';
        
        // Define radio button options
        const options = [
            { id: 'hrnType', value: 'hrn', label: 'HRN', checked: true },
            { id: 'emailType', value: 'email', label: 'Email', checked: false },
            { id: 'userIdType', value: 'uid', label: 'User ID', checked: false },
            { id: 'sourceIdType', value: 'sid', label: 'Source ID', checked: false },
            { id: 'nameType', value: 'name', label: 'Name', checked: false }
        ];
        
        // Create radio buttons
        options.forEach(option => {
            const formCheck = document.createElement('div');
            formCheck.className = 'form-check';
            
            const input = document.createElement('input');
            input.className = 'form-check-input';
            input.type = 'radio';
            input.name = 'huronSearchType';
            input.id = option.id;
            input.value = option.value;
            input.checked = option.checked;
            
            const label = document.createElement('label');
            label.className = 'form-check-label';
            label.setAttribute('for', option.id);
            label.textContent = option.label;
            
            formCheck.appendChild(input);
            formCheck.appendChild(label);
            radioGroup.appendChild(formCheck);
        });
        
        container.appendChild(radioGroup);
        this.searchTypeContainer.appendChild(container);
        this.searchTypeContainer.style.display = 'block';
        
        // Add event listeners for search type changes
        this.addSearchTypeListeners();
        
        // Set initial state
        this.currentSearchType = 'hrn';
        this.updateInputForSearchType();
    }

    hideSearchTypeOptions() {
        if (!this.searchTypeContainer) return;
        
        this.searchTypeContainer.style.display = 'none';
        this.searchTypeContainer.innerHTML = '';
        
        // Reset to single input and update placeholder for BUID
        this.resetToSingleInput();
        this.updatePlaceholder('Enter BUID here...');
    }

    addSearchTypeListeners() {
        const searchTypeRadios = document.querySelectorAll('input[name="huronSearchType"]');
        searchTypeRadios.forEach(radio => {
            radio.addEventListener('change', (e) => {
                this.currentSearchType = e.target.value;
                this.updateInputForSearchType();
            });
        });
        
        // System radio listener
        const hrnRadio = document.getElementById('hrnRadio');
        if (hrnRadio) {
            hrnRadio.addEventListener('change', () => {
                if (hrnRadio.checked) {
                    this.currentSearchType = 'hrn';
                } else {
                    this.currentSearchType = 'buid';
                }
                this.updateInputForSearchType();
            });
        }
    }

    updateInputForSearchType() {
        switch (this.currentSearchType) {
            case 'hrn':
                this.resetToSingleInput();
                this.updatePlaceholder('Enter HRN here...');
                break;
            case 'email':
                this.resetToSingleInput();
                this.updatePlaceholder('Enter email address here...');
                break;
            case 'uid':
                this.resetToSingleInput();
                this.updatePlaceholder('Enter User ID here...');
                break;
            case 'sid':
                this.resetToSingleInput();
                this.updatePlaceholder('Enter Source ID here...');
                break;
            case 'name':
                this.createNameInputs();
                break;
            case 'buid':
                this.resetToSingleInput();
                this.updatePlaceholder('Enter BUID here...');
                break;
        }
    }

    resetToSingleInput() {
        if (!this.inputFieldContainer) return;
        
        // Clear existing content
        this.inputFieldContainer.innerHTML = '';
        
        // Create input group container
        const inputGroup = document.createElement('div');
        inputGroup.className = 'input-group';
        
        // Create text input
        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'form-control';
        input.id = 'personId';
        input.placeholder = 'Enter BUID here...';
        input.required = true;
        
        // Create submit button
        const button = document.createElement('button');
        button.className = 'btn btn-primary';
        button.type = 'submit';
        
        // Create search icon for button
        const searchIcon = document.createElement('i');
        searchIcon.className = 'fa-solid fa-magnifying-glass';
        
        button.appendChild(searchIcon);
        button.appendChild(document.createTextNode(' Lookup'));
        
        // Assemble the input group
        inputGroup.appendChild(input);
        inputGroup.appendChild(button);
        
        this.inputFieldContainer.appendChild(inputGroup);
    }

    createNameInputs() {
        if (!this.inputFieldContainer) return;
        
        // Clear existing content
        this.inputFieldContainer.innerHTML = '';
        
        // Create input group container
        const inputGroup = document.createElement('div');
        inputGroup.className = 'input-group';
        
        // Create first name input
        const firstNameInput = document.createElement('input');
        firstNameInput.type = 'text';
        firstNameInput.className = 'form-control';
        firstNameInput.id = 'firstName';
        firstNameInput.placeholder = 'First Name';
        
        // Create last name input
        const lastNameInput = document.createElement('input');
        lastNameInput.type = 'text';
        lastNameInput.className = 'form-control';
        lastNameInput.id = 'lastName';
        lastNameInput.placeholder = 'Last Name';
        
        // Create submit button
        const button = document.createElement('button');
        button.className = 'btn btn-primary';
        button.type = 'submit';
        
        // Create search icon for button
        const searchIcon = document.createElement('i');
        searchIcon.className = 'fa-solid fa-magnifying-glass';
        
        button.appendChild(searchIcon);
        button.appendChild(document.createTextNode(' Lookup'));
        
        // Assemble the input group
        inputGroup.appendChild(firstNameInput);
        inputGroup.appendChild(lastNameInput);
        inputGroup.appendChild(button);
        
        this.inputFieldContainer.appendChild(inputGroup);
        
        // Add listeners to hide validation alert when user starts typing
        firstNameInput.addEventListener('input', () => {
            const alert = document.getElementById('validationAlert');
            if (alert) alert.remove();
        });
        
        lastNameInput.addEventListener('input', () => {
            const alert = document.getElementById('validationAlert');
            if (alert) alert.remove();
        });
    }

    updatePlaceholder(placeholder) {
        const personIdInput = document.getElementById('personId');
        if (personIdInput) {
            personIdInput.placeholder = placeholder;
        }
    }

    // Get search data for form submission
    getSearchData() {
        const hrnRadio = document.getElementById('hrnRadio');
        
        if (!hrnRadio || !hrnRadio.checked) {
            // Boston University system - return BUID
            const personId = document.getElementById('personId');
            return {
                system: 'source',
                searchType: 'buid',
                personId: personId ? personId.value : ''
            };
        }
        
        // Huron system
        if (this.currentSearchType === 'name') {
            const firstName = document.getElementById('firstName');
            const lastName = document.getElementById('lastName');
            return {
                system: 'target',
                searchType: 'name',
                firstName: firstName ? firstName.value : '',
                lastName: lastName ? lastName.value : '',
                personId: `${firstName?.value || ''} ${lastName?.value || ''}`.trim()
            };
        } else {
            const personId = document.getElementById('personId');
            return {
                system: 'target',
                searchType: this.currentSearchType,
                personId: personId ? personId.value : ''
            };
        }
    }
}

// Export for use in dashboard.js
window.PersonCard = PersonCard;
window.SearchTypeManager = SearchTypeManager;