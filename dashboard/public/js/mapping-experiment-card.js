/**
 * MappingExperimentCard - Handles the Person Mapping experiment tab
 * Allows users to paste raw person JSON and see how it maps to target format
 */
class MappingExperimentCard {
    constructor(containerId) {
        this.containerId = containerId;
        this.container = document.getElementById(containerId);
        this.textareaId = 'personJsonInput';
        this.resultsId = 'mappingResults';
        
        if (!this.container) {
            console.error(`MappingExperimentCard: Container element with id '${containerId}' not found in DOM`);
            console.log('DOM body:', document.body ? 'exists' : 'null');
            console.log('All elements with id:', document.querySelectorAll(`#${containerId}`).length);
        }
    }

    /**
     * Render the initial textarea and button
     */
    renderForm() {
        if (!this.container) {
            console.error('MappingExperimentCard.renderForm(): Container is null, cannot render');
            return;
        }
        
        this.container.innerHTML = '';
        
        const formGroup = document.createElement('div');
        formGroup.className = 'mb-3';
        
        // JSON textarea label
        const label = document.createElement('label');
        label.htmlFor = this.textareaId;
        label.className = 'form-label';
        label.innerHTML = '<i class="fas fa-code me-2"></i>Raw Person JSON (from Source API)';
        
        // Textarea
        const textarea = document.createElement('textarea');
        textarea.id = this.textareaId;
        textarea.className = 'form-control font-monospace';
        textarea.rows = 15;
        textarea.placeholder = 'Paste raw person JSON data here...\n\nExample:\n{\n  "personid": "U12345678",\n  "personBasic": {\n    "names": [{\n      "firstName": "John",\n      "lastName": "Doe"\n    }]\n  },\n  "employeeInfo": {...},\n  ...\n}\n\nNote: JSON must include a "personid" property';
        
        // Helper text
        const helpText = document.createElement('small');
        helpText.className = 'form-text text-muted';
        helpText.textContent = 'Paste the raw JSON data as it would be returned from the BU CDM person API lookup';
        
        // Preview button
        const buttonContainer = document.createElement('div');
        buttonContainer.className = 'mt-3';
        
        const previewButton = document.createElement('button');
        previewButton.type = 'button';
        previewButton.className = 'btn btn-primary';
        previewButton.id = 'experimentPreviewBtn';
        previewButton.innerHTML = '<i class="fas fa-eye me-2"></i>Preview Mapping';
        
        const clearButton = document.createElement('button');
        clearButton.type = 'button';
        clearButton.className = 'btn btn-secondary ms-2';
        clearButton.innerHTML = '<i class="fas fa-eraser me-2"></i>Clear';
        clearButton.onclick = () => {
            textarea.value = '';
            this.clearResults();
        };
        
        buttonContainer.appendChild(previewButton);
        buttonContainer.appendChild(clearButton);
        
        // Assemble form
        formGroup.appendChild(label);
        formGroup.appendChild(textarea);
        formGroup.appendChild(helpText);
        formGroup.appendChild(buttonContainer);
        
        // Results container
        const resultsContainer = document.createElement('div');
        resultsContainer.id = this.resultsId;
        resultsContainer.className = 'mt-4';
        
        this.container.appendChild(formGroup);
        this.container.appendChild(resultsContainer);
    }

    /**
     * Get the current textarea value
     */
    getPersonJson() {
        const textarea = document.getElementById(this.textareaId);
        return textarea ? textarea.value.trim() : '';
    }

    /**
     * Extract personId from the JSON string
     */
    extractPersonId(jsonString) {
        try {
            const parsed = JSON.parse(jsonString);
            return parsed.personid || parsed.personId || null;
        } catch (error) {
            return null;
        }
    }

    /**
     * Render loading state
     */
    renderLoading() {
        const resultsContainer = document.getElementById(this.resultsId);
        if (!resultsContainer) return;
        
        resultsContainer.innerHTML = '';
        
        const alert = document.createElement('div');
        alert.className = 'alert alert-info';
        alert.innerHTML = '<div class="spinner-border spinner-border-sm me-2"></div>Processing mapping experiment...';
        
        resultsContainer.appendChild(alert);
    }

    /**
     * Render the mapping results
     */
    renderResults(data) {
        const resultsContainer = document.getElementById(this.resultsId);
        if (!resultsContainer) return;
        
        resultsContainer.innerHTML = '';
        
        // Success header
        const card = document.createElement('div');
        card.className = 'card border-success';
        
        const cardHeader = document.createElement('div');
        cardHeader.className = 'card-header bg-success text-white';
        cardHeader.innerHTML = '<i class="fas fa-check-circle me-2"></i>Mapping Result';
        
        const cardBody = document.createElement('div');
        cardBody.className = 'card-body';
        
        // JSON display
        const pre = document.createElement('pre');
        pre.className = 'bg-light p-3 rounded';
        pre.style.maxHeight = '500px';
        pre.style.overflowY = 'auto';
        pre.textContent = JSON.stringify(data.data, null, 2);
        
        cardBody.appendChild(pre);
        card.appendChild(cardHeader);
        card.appendChild(cardBody);
        resultsContainer.appendChild(card);
    }

    /**
     * Render error message
     */
    renderError(message) {
        const resultsContainer = document.getElementById(this.resultsId);
        if (!resultsContainer) return;
        
        resultsContainer.innerHTML = '';
        
        const alert = document.createElement('div');
        alert.className = 'alert alert-danger';
        alert.innerHTML = `<i class="fas fa-exclamation-triangle me-2"></i>${message}`;
        
        resultsContainer.appendChild(alert);
    }

    /**
     * Clear results
     */
    clearResults() {
        const resultsContainer = document.getElementById(this.resultsId);
        if (resultsContainer) {
            resultsContainer.innerHTML = '';
        }
    }

    /**
     * Validate JSON format and required fields
     */
    validateJson(jsonString) {
        // Check if empty
        if (!jsonString || jsonString.trim() === '') {
            return { 
                valid: false, 
                error: 'Please paste person JSON data'
            };
        }
        
        // Check if valid JSON
        let parsed;
        try {
            parsed = JSON.parse(jsonString);
        } catch (error) {
            return { 
                valid: false, 
                error: error instanceof Error ? error.message : 'Invalid JSON format'
            };
        }
        
        // Check for personid property (case-insensitive)
        const hasPersonId = parsed.personid || parsed.personId;
        if (!hasPersonId) {
            return { 
                valid: false, 
                error: 'JSON must include a "personid" property'
            };
        }
        
        return { valid: true, personId: hasPersonId };
    }
}
