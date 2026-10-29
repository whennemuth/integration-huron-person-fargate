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
import { ChangeType, ReportingError } from '../../../src/reporting/RunReporting';

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
        return await getHistory({ queryParams });
      
      case '/api/status':
        return await getSystemStatus();

      case '/api/runs':
        return await handleListRuns(request.method);

      case '/api/capabilities':
        return handleCapabilities(request.method);

      case '/api/landscapes':
        return await handleLandscapes(request.method);
      
      default:
        if (path.startsWith('/api/runs/')) {
          return await handleRunRoute({ method: request.method, path, queryParams });
        }
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
async function getHistory(params: { queryParams: any }): Promise<LambdaFunctionUrlResult> {
  const { queryParams } = params;
  try {
    const { startDate, endDate, limit = 50 } = queryParams;
    
    // TODO: Implement actual S3 history retrieval
    const historyService = ServiceProvider.getHistoryService();
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
async function getSystemStatus(): Promise<LambdaFunctionUrlResult> {
  try {
    // TODO: Implement actual health checks
    const systemStatusService = ServiceProvider.getSystemStatusService();
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

/** /api/runs/{syncRunId}/{summary|chunks|errors|changes} */
const RUN_ROUTE = /^\/api\/runs\/([^/]+)\/(summary|chunks|errors|changes)$/;
/** A syncRunId is the ISO timestamp the run's chunker started it with */
const SYNC_RUN_ID = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;
const CHANGE_TYPES: ChangeType[] = ['NEW', 'UPDATED', 'DELETED'];

/**
 * List this landscape's sync runs (syncRunIds, oldest first)
 */
async function handleListRuns(method: string): Promise<LambdaFunctionUrlResult> {
  if (method !== 'GET') return methodNotAllowed();
  try {
    const runs = await ServiceProvider.getRunReportingService().listRuns();
    return jsonResponse(200, { runs });
  } catch (error) {
    return runReportingErrorResponse(error, 'Failed to list runs');
  }
}

/**
 * This landscape's storage mode, and whether it keeps per-person change lists
 */
function handleCapabilities(method: string): LambdaFunctionUrlResult {
  if (method !== 'GET') return methodNotAllowed();
  try {
    return jsonResponse(200, ServiceProvider.getRunReportingService().getCapabilities());
  } catch (error) {
    return runReportingErrorResponse(error, 'Failed to get capabilities');
  }
}

/**
 * The landscape picker's data: the registered landscapes, with this one flagged as current
 */
async function handleLandscapes(method: string): Promise<LambdaFunctionUrlResult> {
  if (method !== 'GET') return methodNotAllowed();
  try {
    return jsonResponse(200, await ServiceProvider.getLandscapeListingService().getLandscapes());
  } catch (error) {
    console.error('Error listing landscapes:', error);
    return jsonResponse(500, {
      error: 'Failed to list landscapes',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
}

/**
 * One run's summary, chunk statistics, or a page of its errors or person changes
 */
async function handleRunRoute(params: { method: string, path: string, queryParams: { [key: string]: string } }): Promise<LambdaFunctionUrlResult> {
  const { method, path, queryParams } = params;
  const match = RUN_ROUTE.exec(path);
  if (!match) {
    return jsonResponse(404, { error: 'Not found' });
  }
  if (method !== 'GET') return methodNotAllowed();

  const [, encodedSyncRunId, report] = match;
  const syncRunId = decodePathSegment(encodedSyncRunId);
  if (!syncRunId || !SYNC_RUN_ID.test(syncRunId)) {
    return jsonResponse(400, { error: 'syncRunId must be an ISO timestamp, e.g. 2026-10-01T02:00:00.000Z' });
  }

  const { cursor, limit, statusCode, changeType } = queryParams;
  if (limit !== undefined && !/^\d+$/.test(limit)) {
    return jsonResponse(400, { error: 'limit must be a positive integer', code: 'INVALID_LIMIT' });
  }
  if (statusCode !== undefined && !/^\d{3}$/.test(statusCode)) {
    return jsonResponse(400, { error: 'statusCode must be a 3-digit HTTP status code' });
  }
  const page = { cursor: cursor || undefined, limit: limit === undefined ? undefined : Number(limit) };

  try {
    const service = ServiceProvider.getRunReportingService();
    switch (report) {
      case 'summary': {
        const runReport = await service.getRunReport(syncRunId);
        return runReport
          ? jsonResponse(200, runReport)
          : jsonResponse(404, { error: `No run recorded for ${syncRunId}` });
      }
      case 'chunks':
        return jsonResponse(200, { items: await service.getRunChunkStatistics(syncRunId) });
      case 'errors':
        return jsonResponse(200, await service.getRunErrors(syncRunId, {
          ...page, statusCode: statusCode === undefined ? undefined : Number(statusCode)
        }));
      default: {
        if (!CHANGE_TYPES.includes(changeType as ChangeType)) {
          return jsonResponse(400, { error: `changeType must be one of: ${CHANGE_TYPES.join(', ')}` });
        }
        return jsonResponse(200, await service.getRunChanges(syncRunId, changeType as ChangeType, page));
      }
    }
  } catch (error) {
    return runReportingErrorResponse(error, `Failed to get run ${report}`);
  }
}

const decodePathSegment = (segment: string): string | undefined => {
  try {
    return decodeURIComponent(segment);
  } catch {
    return undefined;
  }
}

/**
 * A ReportingError (bad cursor or limit, or a report the storage mode doesn't keep) is the
 * caller's to fix: 400 with its code. Anything else is a 500, with the message but no stack.
 */
function runReportingErrorResponse(error: unknown, description: string): LambdaFunctionUrlResult {
  if (error instanceof ReportingError) {
    return jsonResponse(400, { error: error.message, code: error.code });
  }
  console.error(`${description}:`, error);
  return jsonResponse(500, {
    error: description,
    message: error instanceof Error ? error.message : 'Unknown error'
  });
}

function methodNotAllowed(): LambdaFunctionUrlResult {
  const response = jsonResponse(405, { error: 'Method not allowed' });
  return { ...response, headers: { ...response.headers, Allow: 'GET' } };
}

function jsonResponse(statusCode: number, body: unknown): LambdaFunctionUrlResult {
  return createResponse(statusCode, 'application/json', JSON.stringify(body));
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