import Mustache from 'mustache';
import { Authenticator } from '../services/Authentication';
import { Request } from '../services/Request';
import { ServiceProvider } from '../services/ServiceProvider';
import { TemplateService } from '../services/TemplateService';
import { DashboardCache } from './DashboardCache';
import { LambdaFunctionUrlEvent, LambdaFunctionUrlResult } from './DashboardTypes';
import { OriginHeader } from './OriginVerification';
import { CrudOperation, Status } from 'integration-core';
import { PersonSyncResult } from '../services/ServiceTypes';
import { areTheSame } from '../Utils';

// import { ConfigManager } from 'integration-huron-person';

let cache: DashboardCache = new DashboardCache();

/**
 * Lambda event handler for the Integration Dashboard
 * Supports multiple routes and operations via path-based routing
 */
export const handler = async (event: LambdaFunctionUrlEvent): Promise<LambdaFunctionUrlResult> => {
  console.log('Dashboard handler invoked:', JSON.stringify(event, null, 2));
  
  try {
    // Get request object to handle auth and origin verification
    const request = new Request(new Authenticator(event), new OriginHeader(event));
    const {path, body, queryParams } = request;

    // Verify origin header to ensure request came from CloudFront
    if ( ! request.originVerified) {
      console.log('Request origin verification failed. Returning error response.');
      return request.getOriginVerifiedErrorResponse()!;
    }

    // Verify authentication if required
    if ( ! request.isAuthenticated && path.startsWith('/api/')) {
      return createResponse(400, 'application/json', JSON.stringify({ 
        error: 'Request is not authenticated. Requires valid JWT token.' 
      }));
    }
   
    // Route handling
    switch (path) {
      case '/': case '/dashboard':
        if ( ! request.isAuthenticated) {
          console.log('Request is not authenticated. Returning login response.');
          return { statusCode: 302, headers: {
            ["content-type"]: "text/html",
            Location: '/login-page'
          }}

        }
        return await renderDashboard(queryParams.tab);

      case '/login-page':
        if(request.isAuthenticated) {
          return { statusCode: 302, headers: {
            ["content-type"]: "text/html",
            Location: '/dashboard'
          }}
        }
        return await renderLoginPage();

      case '/login-redirect':
        if(request.isAuthenticated) {
          return { statusCode: 302, headers: {
            ["content-type"]: "text/html",
            Location: '/dashboard'
          }}
        }
        return request.getLoginResponse();
      
      case '/api/person-lookup':
        return await handlePersonLookup({requestBody: body, cache });

      case '/api/person-sync/preview-experiment':
        return await handlePersonSyncPreviewExperiment({requestBody: body, cache });

      case '/api/person-sync/preview':
        return await handlePersonSyncPreview({requestBody: body, cache });
      
      case '/api/person-sync':
        return await handlePersonSync({requestBody: body, cache });
      
      case '/api/bulk-sync':
        return await handleBulkSync({requestBody: body, cache });
      
      case '/api/history':
        return await getHistory({queryParams, cache});
      
      case '/api/status':
        return await getSystemStatus({ cache });
      
      default:
        return createResponse(404, 'text/html', '<h1>404 - Page Not Found</h1>');
    }
  } catch (error) {
    console.error('Dashboard handler error:', error);
    return createResponse(500, 'application/json', JSON.stringify({ 
      error: 'Internal Server Error',
      message: error instanceof Error ? error.message : 'Unknown error'
    }));
  }
};

/**
 * Render the main dashboard HTML with tabs
 */
async function renderDashboard(activeTab?: string): Promise<LambdaFunctionUrlResult> {
  try {
    const template = TemplateService.getDashboardTemplate();
    const partials = TemplateService.getPartials();
    const cssAssets = TemplateService.getCssAssets();
    const jsAssets = TemplateService.getJsAssets();
    
    const data = {
      title: 'Integration Dashboard - Boston University Person → Huron',
      activeTab: activeTab || 'individual',
      tabs: [
        { id: 'individual', label: 'Individual Sync', active: (activeTab || 'individual') === 'individual' },
        { id: 'mapping', label: 'Person Mapping', active: activeTab === 'mapping' },
        { id: 'bulk', label: 'Bulk Operations', active: activeTab === 'bulk' },
        { id: 'history', label: 'Activity History', active: activeTab === 'history' },
        { id: 'system', label: 'System Status', active: activeTab === 'system' }
      ],
      individualActive: (activeTab || 'individual') === 'individual',
      mappingActive: activeTab === 'mapping',
      bulkActive: activeTab === 'bulk',
      historyActive: activeTab === 'history',
      systemActive: activeTab === 'system',
      currentTime: new Date().toISOString(),
      cssAssets,
      jsAssets
    };
    
    const html = Mustache.render(template, data, partials);
    return createResponse(200, 'text/html', html);
  } catch (error) {
    console.error('Error rendering dashboard:', error);
    return createResponse(500, 'text/html', '<h1>Error Loading Dashboard</h1>');
  }
}

async function renderLoginPage(): Promise<LambdaFunctionUrlResult> {
  try {
    const template = TemplateService.getLoginTemplate();
    const cssAssets = TemplateService.getCssAssets();
    
    const data = {
      title: 'Integration Dashboard - Boston University Person → Huron',
      currentTime: new Date().toISOString(),
      cssAssets
    };
    
    const html = Mustache.render(template, data);
    return createResponse(200, 'text/html', html);
  } catch (error) {
    console.error('Error rendering login page:', error);
    return createResponse(500, 'text/html', '<h1>Error Loading Login Page</h1>');
  }
}

/**
 * Handle person lookup by BUID or other identifier
 */
async function handlePersonLookup(params: { requestBody: string | null, cache: DashboardCache }): Promise<LambdaFunctionUrlResult> {
  const { requestBody, cache } = params;
  if (!requestBody) {
    return createResponse(400, 'application/json', JSON.stringify({ error: 'Request body required' }));
  }

  try {
    const { personId, system, searchType, firstName, lastName } = JSON.parse(requestBody);
    
    if (!personId) {
      return createResponse(400, 'application/json', JSON.stringify({ error: 'personId is required' }));
    }

    const config = await cache.getConfiguration();
    if (config.dataSource.person) {
      // Remove fieldsOfInterest from config before passing to service, as it will interfere with 
      // getting a full person from the lookup, where we don't want any fields to be filtered out.
      delete config.dataSource.person.fieldsOfInterest;
    }
    const personLookupService = ServiceProvider.getPersonLookupService(config);
    const personLookupResult = await personLookupService.lookup({
      personId, system, searchType, firstName, lastName
    });

    return createResponse(200, 'application/json', JSON.stringify(personLookupResult));
  } catch (error) {
    console.error('Error in person lookup:', error);
    return createResponse(500, 'application/json', JSON.stringify({ 
      error: 'Lookup failed', 
      message: error instanceof Error ? error.message : 'Unknown error' 
    }));
  }
}

/**
 * Provide a preview of how a person sync operation would be processed, by returning the result of
 * mapping the source person data (indicated by the personId) to the target format for sync
 * operations. This allows users to experiment with different person records, and see how the sync 
 * logic would handle them, without actually performing a sync. 
 * @param params 
 * @returns 
 */
async function handlePersonSyncPreview(params: { requestBody: string | null, cache: DashboardCache }): Promise<LambdaFunctionUrlResult> {
  const { requestBody, cache } = params;
  if (!requestBody) {
    return createResponse(400, 'application/json', JSON.stringify({ error: 'Request body required' }));
  }

  try {
    let { personId: buid, operation } = JSON.parse(requestBody);

    // Make sure buid is provided
    if (!buid) {
      return createResponse(400, 'application/json', JSON.stringify({ 
        error: 'BUID is required' 
      }));
    }

    let crudOperation: CrudOperation | undefined = undefined;

    // Make sure operation is a member of CrudOperation
    if(operation) {
      const validOperations = Object.values(CrudOperation);
      if (!validOperations.includes(operation)) {
        return createResponse(400, 'application/json', JSON.stringify({ 
          error: `Invalid operation. Must be one of: ${validOperations.join(', ')}`
        }));
      }
      crudOperation = operation as CrudOperation;
    }

    const config = await cache.getConfiguration();
    const personSyncService = ServiceProvider.getPersonSyncService(config);
    const pushRequest = await personSyncService.preview(buid, crudOperation);
    return createResponse(200, 'application/json', JSON.stringify(pushRequest));

  } catch (error) {
    console.error('Error in person sync preview:', error);
    return createResponse(500, 'application/json', JSON.stringify({ 
      error: 'Person sync preview failed', 
      message: error instanceof Error ? error.message : 'Unknown error' 
    }));
  }
}

/**
 * The user is experimenting with the sync preview endpoint that accepts raw person JSON. This json
 * should follow the same structure as the data handlePersonSyncPreview would normally retrieve 
 * from the person lookup service, but in this case the user is supplying a "mock" value to see 
 * how the backend would map to the target format for sync operations. This is meant to be a 
 * flexible endpoint for testing different person data inputs and seeing how the sync preview 
 * logic handles them, without needing to set up specific records in the source system.
 * @param params 
 * @returns 
 */
async function handlePersonSyncPreviewExperiment(params: { requestBody: string | null, cache: DashboardCache }): Promise<LambdaFunctionUrlResult> {
  const { requestBody, cache } = params;
  if (!requestBody) {
    return createResponse(400, 'application/json', JSON.stringify({ error: 'Request body required' }));
  }

  try {
    let { personId: buid, personJson } = JSON.parse(requestBody);

    // Make sure personJson is provided
    if (!personJson) {
      return createResponse(400, 'application/json', JSON.stringify({ 
        error: 'personJson is required' 
      }));
    }
    
    // Make sure personJson is valid JSON
    let parsedPerson:any;
    try {
      parsedPerson = JSON.parse(personJson);
    } catch {
      return createResponse(400, 'application/json', JSON.stringify({ 
        error: 'personJson must be valid JSON' 
      }));
    }

    const config = await cache.getConfiguration();
    const personSyncService = ServiceProvider.getPersonSyncService(config);
    const pushRequest = await personSyncService.previewExperiment(buid, [ parsedPerson ]);
    return createResponse(200, 'application/json', JSON.stringify(pushRequest));
    
  } catch (error) {
    console.error('Error in person sync preview experiment:', error);
    return createResponse(500, 'application/json', JSON.stringify({ 
      error: 'Person sync preview experiment failed', 
      message: error instanceof Error ? error.message : 'Unknown error' 
    }));
  }

}
/**
 * Handle individual person synchronization
 */
async function handlePersonSync(params: { requestBody: string | null, cache: DashboardCache }): Promise<LambdaFunctionUrlResult> {
  const { requestBody, cache } = params;
  if (!requestBody) {
    return createResponse(400, 'application/json', JSON.stringify({ error: 'Request body required' }));
  }

  try {
    const { personId, operation, hrn } = JSON.parse(requestBody);
    
    if (!personId || !operation) {
      return createResponse(400, 'application/json', JSON.stringify({ 
        error: 'personId and operation are required' 
      }));
    }
    
    const crudOperation = operation as CrudOperation;
    const config = await cache.getConfiguration();
    const personSyncService = ServiceProvider.getPersonSyncService(config);
    const syncResult: PersonSyncResult = await personSyncService.sync(personId, crudOperation, hrn);
    if(!areTheSame(syncResult.status, Status.SUCCESS)) {
      return createResponse(500, 'application/json', JSON.stringify({ 
        error: 'Person sync failed', 
        message: syncResult.details || 'Unknown error during synchronization'
      }));
    }
    return createResponse(200, 'application/json', JSON.stringify(syncResult));

  } catch (error) {
    console.error('Error in person sync:', error);
    return createResponse(500, 'application/json', JSON.stringify({ 
      error: 'Person sync failed', 
      message: error instanceof Error ? error.message : 'Unknown error' 
    }));
  }
}

/**
 * Handle bulk synchronization operations
 */
async function handleBulkSync(params: { requestBody: string | null, cache: DashboardCache }): Promise<LambdaFunctionUrlResult> {
  const { requestBody, cache } = params;
  if (!requestBody) {
    return createResponse(400, 'application/json', JSON.stringify({ error: 'Request body required' }));
  }

  try {
    const { operation, batchSize, filters } = JSON.parse(requestBody);
    
    // TODO: Implement actual bulk sync
    const config = await cache.getConfiguration();
    const bulkSyncService = ServiceProvider.getBulkSyncService(config);
    const mockResult = await bulkSyncService.start(operation, batchSize, filters);

    return createResponse(200, 'application/json', JSON.stringify(mockResult));
  } catch (error) {
    console.error('Error in bulk sync:', error);
    return createResponse(500, 'application/json', JSON.stringify({ 
      error: 'Bulk sync failed', 
      message: error instanceof Error ? error.message : 'Unknown error' 
    }));
  }
}

/**
 * Get activity history from S3 or database
 */
async function getHistory(params: { queryParams: any, cache: DashboardCache }): Promise<LambdaFunctionUrlResult> {
  const { queryParams, cache } = params;
  try {
    const { startDate, endDate, limit = 50 } = queryParams;
    
    // TODO: Implement actual S3 history retrieval
    const config = await cache.getConfiguration();
    const historyService = ServiceProvider.getHistoryService(config);
    const historyData = await historyService.getHistory(startDate, endDate, limit);

    return createResponse(200, 'application/json', JSON.stringify(historyData));
  } catch (error) {
    console.error('Error getting history:', error);
    return createResponse(500, 'application/json', JSON.stringify({ 
      error: 'Failed to retrieve history', 
      message: error instanceof Error ? error.message : 'Unknown error' 
    }));
  }
}

/**
 * Get current system status and health
 */
async function getSystemStatus(params: { cache: DashboardCache }): Promise<LambdaFunctionUrlResult> {
  const { cache } = params;
  try {
    // TODO: Implement actual health checks
    const config = await cache.getConfiguration();
    const systemStatusService = ServiceProvider.getSystemStatusService(config);
    const status = await systemStatusService.getStatus();

    return createResponse(200, 'application/json', JSON.stringify(status));
  } catch (error) {
    console.error('Error getting system status:', error);
    return createResponse(500, 'application/json', JSON.stringify({ 
      error: 'Failed to retrieve status', 
      message: error instanceof Error ? error.message : 'Unknown error' 
    }));
  }
}

/**
 * Helper function to create standardized Lambda Function URL responses
 */
function createResponse(statusCode: number, contentType: string, body: string): LambdaFunctionUrlResult {
  return {
    statusCode,
    headers: {
      'Content-Type': contentType,
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
      'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS'
    },
    body
  };
}