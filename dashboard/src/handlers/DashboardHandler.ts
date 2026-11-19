import Mustache from 'mustache';
import { Authenticator } from '../services/Authentication';
import { Request } from '../services/Request';
import { ServiceProvider } from '../services/ServiceProvider';
import { TemplateService } from '../services/TemplateService';
import { DashboardCache } from './DashboardCache';
import { LambdaFunctionUrlEvent, LambdaFunctionUrlResult } from './DashboardTypes';
import { OriginHeader } from './OriginVerification';
import { CrudOperation, Status } from 'integration-core';
import { BulkSyncRequestBody, LandscapeListing, PersonSyncResult } from '../services/ServiceTypes';
import { BulkSync, BulkSyncError } from '../services/BulkSync';
import { buildNavigationBar } from './NavigationBar';
import { LOCAL_LANDSCAPE } from '../services/LandscapeRegistry';
import { PageNavigation } from './PageNavigation';
import { RUN_LIST_DEFAULT_LIMIT, RUN_LIST_MAX_LIMIT, RunList } from './RunList';
import { areTheSame } from '../Utils';
import { ChangeType, ReportingError } from '../../../src/reporting/RunReporting';
import { Config } from 'integration-huron-person';
import { ForbiddenError, RequestAccess, RequestGate } from '../authorization/RequestGate';
import { Impersonation } from '../authorization/Impersonation';
import { PrincipalResolver } from '../authorization/Principal';
import { RequestAuthorizer } from '../authorization/Authorizer';
import { AuditTrail, auditsAllowed, describeRequest } from '../audit/AuditTrail';
import { AuditError } from '../audit/AuditStore';
import { AccessRoutes, UnchangedPerson } from './AccessRoutes';
import { ACTIONS } from '../authorization/Actions';

// import { ConfigManager } from 'integration-huron-person';

let cache: DashboardCache = new DashboardCache();

/**
 * The integration config for the person and bulk sync services. The offline services ignore it, so
 * none is read when they're in use: running locally offline needs no config secret.
 */
async function serviceConfiguration(cache: DashboardCache): Promise<Config> {
  return ServiceProvider.isOffline() ? {} as Config : cache.getConfiguration();
}

/**
 * Lambda event handler for the Integration Dashboard
 * Supports multiple routes and operations via path-based routing
 */
export const handler = async (event: LambdaFunctionUrlEvent): Promise<LambdaFunctionUrlResult> => {
  console.log('Dashboard handler invoked:', JSON.stringify(event, null, 2));
  
  try {
    // Get request object to handle auth and origin verification
    const request = new Request(new Authenticator(event), new OriginHeader(event));
    const { path, body } = request;

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

    // Authorization (authorization-plan.md, decision 7): one gate before routing, deny by default
    const gate = new RequestGate(ServiceProvider.getRoleAssignmentStore(), ServiceProvider.getRoleCatalog());
    const { access, refusal } = await gate.check({
      method: request.method, path, publicMode: !request.signInRequired, claims: request.user, cookieHeader: event.headers?.cookie
    });
    // The audit trail (decision 9): every denial, and the allowed requests that read personal data or change something
    const audit = new AuditTrail({
      store: ServiceProvider.getAuditStore(), landscape: process.env.LANDSCAPE || LOCAL_LANDSCAPE,
      principal: access.principal, viewedAs: access.viewedAs
    });
    // An impersonation cookie refused (its user's Admin role is gone, or it was made by hand): ignored and cleared
    if (access.impersonationDenial) {
      await audit.record({ action: 'access:impersonate', decision: 'DENIED', detail: access.impersonationDenial });
    }
    if (refusal) {
      if (access.decision) {
        access.decision.allowed
          // Allowed by the person's roles, refused because impersonating is viewing only
          ? await audit.record({ action: access.decision.action, decision: 'DENIED', ...describeRequest(path, body), detail: `${refusal.body.message}` })
          : await audit.denied(access.decision, describeRequest(path, body).target);
      }
      const response = jsonResponse(refusal.status, refusal.body);
      return withCookies({ ...response, headers: { ...response.headers, ...refusal.headers } }, access);
    }

    let response: LambdaFunctionUrlResult;
    try {
      response = await route(request, access, audit);
    } catch (error) {
      console.error('Dashboard handler error:', error);
      response = internalError(error);
    }
    // A choice refused within the route (Bulk sync's) was recorded as a denial already
    if (auditsAllowed(access.permission) && !audit.denialRecorded) {
      await audit.allowed(access.permission.action, response.statusCode, describeRequest(path, body));
    }
    return withCookies(response, access);
  } catch (error) {
    console.error('Dashboard handler error:', error);
    return internalError(error);
  }
};

/** The response, carrying the Set-Cookie the gate asked for (clearing a refused impersonation cookie), unless it sets its own */
const withCookies = (response: LambdaFunctionUrlResult, access: RequestAccess): LambdaFunctionUrlResult =>
  access.setCookies.length && !response.headers?.['Set-Cookie']
    ? { ...response, headers: { ...response.headers, 'Set-Cookie': access.setCookies[0] } }
    : response;

const internalError = (error: unknown): LambdaFunctionUrlResult => createResponse(500, 'application/json', JSON.stringify({
  error: 'Internal Server Error',
  message: error instanceof Error ? error.message : 'Unknown error'
}));

/** Answer a request the gate let through */
async function route(request: Request, access: RequestAccess, audit: AuditTrail): Promise<LambdaFunctionUrlResult> {
  const { path, body, queryParams } = request;
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
      await recordProfile(access);
      return await renderDashboard(request, queryParams.tab, access);

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
    
    case '/api/me':
      return await handleMe(access);

    case '/api/impersonation':
      return await handleImpersonation(request, access, audit);

    case '/api/person-lookup':
      return await handlePersonLookup({requestBody: body, cache });

    case '/api/person-sync/preview-experiment':
      return await handlePersonSyncPreviewExperiment({requestBody: body, cache });

    case '/api/person-sync/preview':
      return await handlePersonSyncPreview({requestBody: body, cache });
    
    case '/api/person-sync':
      return await handlePersonSync({requestBody: body, cache });
    
    case '/api/bulk-sync':
      return await handleBulkSync({ method: request.method, requestBody: body, cache, user: request.user, authorizer: access.authorizer, audit });

    case '/api/bulk-sync/plan':
      return await handleBulkSync({ method: request.method, requestBody: body, cache, user: request.user, authorizer: access.authorizer, audit, plan: true });

    case '/api/mock-landscape/reset':
      return await handleMockLandscapeReset({ method: request.method, cache, user: request.user });
    
    case '/api/status':
      return await getSystemStatus();

    case '/api/runs':
      return await handleListRuns(request.method, queryParams);

    case '/api/capabilities':
      return handleCapabilities(request.method);

    case '/api/landscapes':
      return await handleLandscapes(request.method);

    case '/api/audit':
      return await handleAudit(queryParams);

    case '/api/access/roles':
    case '/api/access/assignments':
      return await handleAccess(request, access, audit);
    
    default:
      if (path.startsWith('/api/runs/')) {
        return await handleRunRoute({ method: request.method, path, queryParams });
      }
      if (path.startsWith('/api/access/assignments/')) {
        return await handleAccess(request, access, audit);
      }
      return createResponse(404, 'text/html', '<h1>404 - Page Not Found</h1>');
  }
}

/**
 * Render the dashboard page: the sidebar and top bar, with the requested page (`?tab=<id>`) shown
 */
async function renderDashboard(request: Request, activeTab: string | undefined, access: RequestAccess): Promise<LambdaFunctionUrlResult> {
  try {
    const navigationBar = buildNavigationBar({
      listing: await landscapeListingOrNone(),
      landscape: process.env.LANDSCAPE || LOCAL_LANDSCAPE,
      landscapeTone: process.env.LANDSCAPE_TONE,
      signInRequired: request.signInRequired,
      user: request.user,
      // The Impersonate button for those who may, and while impersonating, Stop and the impersonating tone
      impersonation: {
        mayImpersonate: access.mayImpersonate,
        ...(access.viewedAs && { person: await personLabel(access.viewedAs.buPrincipal!) })
      }
    });
    const template = TemplateService.getDashboardTemplate();
    const partials = TemplateService.getPartials();
    const cssAssets = TemplateService.getCssAssets();
    const jsAssets = TemplateService.getJsAssets();
    
    const data = {
      title: 'Integration Dashboard - Boston University Person → Huron',
      ...new PageNavigation().templateData(activeTab),
      // The Audit trail page's action filter
      auditActions: ACTIONS,
      ...navigationBar,
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

/**
 * The registry's landscapes for the navigator (cached by the LandscapeRegistry for the life of the
 * Lambda instance), or none if it can't be read: the dashboard renders without the navigator.
 */
async function landscapeListingOrNone(): Promise<LandscapeListing | undefined> {
  try {
    return await ServiceProvider.getLandscapeListingService().getLandscapes();
  } catch (error) {
    console.error('Error listing landscapes for the navigator:', error);
    return undefined;
  }
}

async function renderLoginPage(): Promise<LambdaFunctionUrlResult> {
  try {
    const template = TemplateService.getLoginTemplate();
    const cssAssets = TemplateService.getCssAssets();
    
    const data = {
      title: 'Integration Dashboard - Boston University Person → Huron',
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

    const config = await serviceConfiguration(cache);
    if (config.dataSource?.person) {
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

    const config = await serviceConfiguration(cache);
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

    const config = await serviceConfiguration(cache);
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
    const config = await serviceConfiguration(cache);
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
 * Starting a sync run in this landscape (BulkSync):
 * - GET /api/bulk-sync: what a start would do and whether one is allowed now (BulkSyncState)
 * - POST /api/bulk-sync/plan (plan): what a start from a BulkSyncRequestBody would send, and every setting of the
 *   run (BulkSyncPlan), after the same checks; 200, and nothing is started
 * - POST /api/bulk-sync: start one, from a BulkSyncRequestBody; 202 with the BulkSyncResult once the chunker has
 *   accepted it
 * A refused review or start is a BulkSyncError, answered with its status (400 invalid, 403 production landscape,
 * 409 a run underway or stalled or a subscriber in DRY_RUN, 503 not configured) and { error, code }.
 */
async function handleBulkSync(params: {
  method: string, requestBody: string | null, cache: DashboardCache, user?: any, authorizer: RequestAuthorizer, audit: AuditTrail,
  plan?: boolean
}): Promise<LambdaFunctionUrlResult> {
  const { method, requestBody, cache, user, authorizer, audit, plan = false } = params;
  const allowed = plan ? ['POST'] : ['GET', 'POST'];
  if (!allowed.includes(method)) {
    const response = jsonResponse(405, { error: 'Method not allowed' });
    return { ...response, headers: { ...response.headers, Allow: allowed.join(', ') } };
  }
  try {
    const config = await serviceConfiguration(cache);
    const bulkSyncService = ServiceProvider.getBulkSyncService(config);
    if (method === 'GET') {
      return jsonResponse(200, await bulkSyncService.describe());
    }
    let body: BulkSyncRequestBody;
    try {
      body = JSON.parse(requestBody || '');
    } catch {
      return jsonResponse(400, { error: 'The request body must be JSON', code: 'INVALID_REQUEST' });
    }
    // Who is starting it: the signed-in user, from the request's sign-in token
    const startedBy = BulkSync.startedBy(user);
    // The request's choices are checked against the user's roles too (Start from scratch, Processor tasks)
    return plan
      ? jsonResponse(200, await bulkSyncService.plan(body, startedBy, authorizer))
      : jsonResponse(202, await bulkSyncService.start(body, startedBy, authorizer));
  } catch (error) {
    if (error instanceof BulkSyncError) {
      return jsonResponse(error.status, { error: error.message, code: error.code });
    }
    if (error instanceof ForbiddenError) {
      await audit.denied(error.decision);
      return jsonResponse(403, error.body);
    }
    console.error('Error in bulk sync:', error);
    return jsonResponse(500, {
      error: method === 'GET' ? 'Bulk sync state could not be read' : plan ? 'The run could not be reviewed' : 'Bulk sync failed',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
}

/**
 * A mock landscape's reset on its own (Overview's Reset landscape; BulkSync.resetState() / requestReset()):
 * - GET /api/mock-landscape/reset: the last reset and whether one can be started now (MockLandscapeResetState;
 *   `{ mockLandscape: false }` in any other landscape)
 * - POST /api/mock-landscape/reset: start one; 202 once the chunker subscriber has accepted it
 * A refusal is a BulkSyncError, answered with its status (400 not a mock landscape, 409 a run underway, a reset
 * running or the subscriber in DRY_RUN, 503 not configured) and { error, code }.
 */
async function handleMockLandscapeReset(params: { method: string, cache: DashboardCache, user?: any }): Promise<LambdaFunctionUrlResult> {
  const { method, cache, user } = params;
  if (!['GET', 'POST'].includes(method)) {
    const response = jsonResponse(405, { error: 'Method not allowed' });
    return { ...response, headers: { ...response.headers, Allow: 'GET, POST' } };
  }
  try {
    const bulkSyncService = ServiceProvider.getBulkSyncService(await serviceConfiguration(cache));
    return method === 'GET'
      ? jsonResponse(200, await bulkSyncService.resetState())
      : jsonResponse(202, await bulkSyncService.requestReset(BulkSync.startedBy(user)));
  } catch (error) {
    if (error instanceof BulkSyncError) {
      return jsonResponse(error.status, { error: error.message, code: error.code });
    }
    console.error('Error in the mock landscape reset:', error);
    return jsonResponse(500, {
      error: method === 'GET' ? 'The last reset could not be read' : 'The reset could not be started',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
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
 * List this landscape's sync runs. Without `include`: { runs }, every syncRunId, oldest first. With
 * `include=report`: a page of runs with their reports, newest first ({ items, before? }), of up to `limit`
 * runs (default 20, at most 50) older than `before` (a syncRunId: the previous page's `before`).
 */
async function handleListRuns(method: string, queryParams: { [key: string]: string } = {}): Promise<LambdaFunctionUrlResult> {
  if (method !== 'GET') return methodNotAllowed();
  const { include, limit, before } = queryParams;
  if (include !== undefined && include !== 'report') {
    return jsonResponse(400, { error: "include must be 'report'" });
  }
  if (include === undefined) {
    try {
      const runs = await ServiceProvider.getRunReportingService().listRuns();
      return jsonResponse(200, { runs });
    } catch (error) {
      return runReportingErrorResponse(error, 'Failed to list runs');
    }
  }

  const limitValue = limit === undefined ? RUN_LIST_DEFAULT_LIMIT : Number(limit);
  if (!/^\d+$/.test(limit ?? `${RUN_LIST_DEFAULT_LIMIT}`) || limitValue < 1 || limitValue > RUN_LIST_MAX_LIMIT) {
    return jsonResponse(400, { error: `limit must be an integer from 1 to ${RUN_LIST_MAX_LIMIT}`, code: 'INVALID_LIMIT' });
  }
  if (before !== undefined && !SYNC_RUN_ID.test(before)) {
    return jsonResponse(400, { error: 'before must be a syncRunId (an ISO timestamp, e.g. 2026-10-01T02:00:00.000Z)' });
  }
  try {
    return jsonResponse(200, await new RunList(ServiceProvider.getRunReportingService(), ServiceProvider.getLaunchFlag()).page({ limit: limitValue, before }));
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
 * caller's to fix: 400 with its code; except RUN_LIST_UNAVAILABLE (the statistics table's run-list-index
 * isn't ready), a 503 whose message says what to do. Anything else is a 500, with the message but no stack.
 */
function runReportingErrorResponse(error: unknown, description: string): LambdaFunctionUrlResult {
  if (error instanceof ReportingError) {
    // The run list's index not being ready is the landscape's state, not the request's fault
    const status = error.code === 'RUN_LIST_UNAVAILABLE' ? 503 : 400;
    return jsonResponse(status, { error: error.message, code: error.code });
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

/** The Access page's routes (AccessRoutes), behind the gate's access:admin; a change is recorded as granted by the user */
async function handleAccess(request: Request, access: RequestAccess, audit: AuditTrail): Promise<LambdaFunctionUrlResult> {
  const routes = new AccessRoutes({ store: ServiceProvider.getRoleAssignmentStore(), catalog: ServiceProvider.getRoleCatalog() });
  const { status, body } = await routes.answer({
    method: request.method, path: request.path, body: request.body, grantedBy: access.principal.buPrincipal ?? 'anonymous'
  });
  const pruned = (body.pruned as { role: string; coveredBy: string }[] | undefined) ?? [];
  const dropped = pruned.length ? `; dropped ${pruned.map(({ role, coveredBy }) => `${role} (covered by ${coveredBy})`).join(', ')}` : '';
  // A saved change is audited with the roles saved, after pruning, and what was dropped
  if (status === 200 && Array.isArray(body.roles) && body.added !== undefined) {
    audit.note(`roles=${(body.roles as string[]).join(',') || 'none'}${dropped}`);
  }
  // A bulk grant: a record for each person given the roles, and the request's own record saying who was left as they were
  if (status === 200 && Array.isArray(body.granted)) {
    const roles = `roles=${(body.roles as string[]).join(',')}`;
    for (const person of body.granted as string[]) {
      await audit.record({ action: 'access:admin', decision: 'ALLOWED', outcome: 'SUCCEEDED', target: person, detail: `${roles} (bulk)` });
    }
    const unchanged = body.unchanged as UnchangedPerson[];
    audit.note(`bulk ${roles}${dropped}; granted ${(body.granted as string[]).length}`
      + (unchanged.length ? `; left as they were ${unchanged.length}: ${unchanged.map(({ buPrincipal, roles: held }) => `${buPrincipal} (${held.join(',')})`).join(', ')}` : ''));
  }
  return jsonResponse(status, body);
}

/**
 * GET /api/audit?cursor&limit&principal&action: the landscape's audit trail, newest first, a page at a time
 * (`{ items, cursor? }`; the cursor continues the list), optionally one person's (a buPrincipal, <id>@bu.edu or
 * anonymous) or one action's. A bad cursor, limit or filter is 400 { error, code }.
 */
async function handleAudit(queryParams: { [key: string]: string }): Promise<LambdaFunctionUrlResult> {
  const { cursor, limit, principal, action } = queryParams;
  try {
    return jsonResponse(200, await ServiceProvider.getAuditStore().list({
      cursor: cursor || undefined,
      limit: limit === undefined || limit === '' ? undefined : Number(limit),
      buPrincipal: principal || undefined,
      action: action || undefined
    }));
  } catch (error) {
    if (error instanceof AuditError) {
      return jsonResponse(400, { error: error.message, code: error.code });
    }
    throw error;
  }
}

/**
 * GET /api/me: who the dashboard takes the request to be, and what they may do in this landscape (authorization-plan.md,
 * decision 10's who-am-I dialog): their identity, roles with each role's policies and actions, and every action they
 * hold. Needs only sign-in, so a user with no role can see that they have none.
 */
async function handleMe(access: RequestAccess): Promise<LambdaFunctionUrlResult> {
  const { principal, authorizer, publicMode, viewedAs, mayImpersonate } = access;
  await recordProfile(access);
  const { buPrincipal, eppn, mail, displayName } = principal;
  return jsonResponse(200, {
    signedIn: !principal.isAnonymous,
    ...(buPrincipal && { user: { buPrincipal, eppn, mail, displayName } }),
    // The roles in force: the impersonated person's while impersonating (decision 14)
    ...await authorizer.effectivePermissions(),
    mayImpersonate,
    ...(viewedAs && { viewedAs: await personLabel(viewedAs.buPrincipal!) }),
    publicMode
  });
}

/** A person as the top bar and /api/me name them: their buPrincipal, and their name if they have signed in here */
async function personLabel(buPrincipal: string): Promise<{ buPrincipal: string; label: string; displayName?: string }> {
  let profile;
  try {
    profile = await ServiceProvider.getRoleAssignmentStore().getProfile(buPrincipal);
  } catch (error) {
    console.warn(`Couldn't read the profile of ${buPrincipal}: ${error instanceof Error ? error.message : error}`);
  }
  const displayName = profile?.displayName;
  return { buPrincipal, label: displayName || buPrincipal, ...(displayName && { displayName }) };
}

/**
 * Impersonation (decision 14), behind the gate (starting needs the user's own access:impersonate; stopping, only sign-in):
 * - `POST /api/impersonation` `{ principal }` (a buPrincipal or <id>@bu.edu): sets the cookie naming the person, whose
 *   roles decide every later request, for viewing only, until stopped. Starting another replaces it. Not yourself.
 * - `DELETE /api/impersonation`: clears it.
 * Both are audited (access:impersonate, the person as the target).
 */
async function handleImpersonation(request: Request, access: RequestAccess, audit: AuditTrail): Promise<LambdaFunctionUrlResult> {
  if (request.method === 'DELETE') {
    if (access.viewedAs) {
      await audit.record({ action: 'access:impersonate', decision: 'ALLOWED', outcome: 'SUCCEEDED', target: access.viewedAs.buPrincipal, detail: 'stopped' });
    }
    const response = jsonResponse(200, { impersonating: null });
    return { ...response, headers: { ...response.headers, 'Set-Cookie': Impersonation.clearCookieValue() } };
  }
  let input: unknown;
  try {
    input = JSON.parse(request.body || '').principal;
  } catch {
    return jsonResponse(400, { error: 'The request body must be JSON: { "principal": "..." }', code: 'INVALID_REQUEST' });
  }
  const resolution = PrincipalResolver.resolve(input);
  if (!resolution.ok) {
    return jsonResponse(400, { error: resolution.reason, code: 'INVALID_PRINCIPAL' });
  }
  const refusal = Impersonation.startRefusal(access.principal, resolution.buPrincipal);
  if (refusal) {
    return jsonResponse(400, { error: refusal, code: 'INVALID_PRINCIPAL' });
  }
  const person = await personLabel(resolution.buPrincipal);
  const roles = await ServiceProvider.getRoleAssignmentStore().getRoles(resolution.buPrincipal);
  await audit.record({ action: 'access:impersonate', decision: 'ALLOWED', outcome: 'SUCCEEDED', target: person.buPrincipal, detail: 'started' });
  const response = jsonResponse(200, { impersonating: { ...person, roles } });
  return { ...response, headers: { ...response.headers, 'Set-Cookie': Impersonation.setCookie(person.buPrincipal) } };
}

/**
 * Keep what the signed-in user's token says of them (name, eppn, mail), so the Access page can show who people are.
 * On a page load and /api/me only, not on every request. Never fails the request.
 */
async function recordProfile(access: RequestAccess): Promise<void> {
  if (access.publicMode || access.principal.isAnonymous) {
    return;
  }
  try {
    await ServiceProvider.getRoleAssignmentStore().recordProfile(access.principal);
  } catch (error) {
    console.warn(`Couldn't record the profile of ${access.principal.buPrincipal}: ${error instanceof Error ? error.message : error}`);
  }
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