/**
 * Tests the dashboard's run reporting and landscape routes (dashboard/src/handlers/DashboardHandler.ts):
 * validation, the limit cap, ReportingError mapping and the mock services.
 */
import { handler } from '../dashboard/src/handlers/DashboardHandler';
import { LambdaFunctionUrlEvent } from '../dashboard/src/handlers/DashboardTypes';
import { ServiceProvider } from '../dashboard/src/services/ServiceProvider';
import { MAX_PAGE_LIMIT, ReportingError } from '../src/reporting/RunReporting';

const syncRunId = '2026-10-01T02:00:00.000Z';

const request = async (path: string, queryStringParameters?: Record<string, string>, method = 'GET') => {
  const event = {
    rawPath: path,
    queryStringParameters,
    headers: {},
    requestContext: { http: { method, path } },
    isBase64Encoded: false
  } as unknown as LambdaFunctionUrlEvent;
  const result = await handler(event);
  return { status: result.statusCode, body: result.body ? JSON.parse(result.body) : undefined, headers: result.headers };
};

describe('Dashboard run reporting routes', () => {
  const service = {
    getCapabilities: jest.fn().mockReturnValue({ landscape: 'mock', storageMode: 's3', changesSupported: false }),
    listRuns: jest.fn().mockResolvedValue([syncRunId]),
    getRunReport: jest.fn().mockResolvedValue({ syncRunId, status: 'SUCCEEDED' }),
    getRunChunkStatistics: jest.fn().mockResolvedValue([{ eventType: 'STATISTICS-chunk-0000' }]),
    getRunErrors: jest.fn().mockResolvedValue({ items: [{ statusCode: 500 }], cursor: 'next' }),
    getRunChanges: jest.fn().mockResolvedValue({ items: [] }),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
    jest.spyOn(ServiceProvider, 'getRunReportingService').mockReturnValue(service);
  });
  afterEach(() => jest.restoreAllMocks());

  it('lists runs', async () => {
    expect(await request('/api/runs')).toMatchObject({ status: 200, body: { runs: [syncRunId] } });
  });

  it('returns capabilities', async () => {
    expect(await request('/api/capabilities')).toMatchObject({
      status: 200, body: { landscape: 'mock', storageMode: 's3', changesSupported: false }
    });
  });

  it('returns a run summary, or 404 when nothing is recorded for the run', async () => {
    expect(await request(`/api/runs/${syncRunId}/summary`)).toMatchObject({ status: 200, body: { syncRunId, status: 'SUCCEEDED' } });
    service.getRunReport.mockResolvedValueOnce(undefined);
    expect((await request(`/api/runs/${syncRunId}/summary`)).status).toBe(404);
  });

  it('accepts a percent-encoded syncRunId', async () => {
    await request(`/api/runs/${encodeURIComponent(syncRunId)}/summary`);
    expect(service.getRunReport).toHaveBeenCalledWith(syncRunId);
  });

  it('returns chunk statistics', async () => {
    expect(await request(`/api/runs/${syncRunId}/chunks`)).toMatchObject({
      status: 200, body: { items: [{ eventType: 'STATISTICS-chunk-0000' }] }
    });
  });

  it('passes the error page and status code filter through', async () => {
    const response = await request(`/api/runs/${syncRunId}/errors`, { cursor: 'abc', limit: '50', statusCode: '500' });
    expect(response).toMatchObject({ status: 200, body: { items: [{ statusCode: 500 }], cursor: 'next' } });
    expect(service.getRunErrors).toHaveBeenCalledWith(syncRunId, { cursor: 'abc', limit: 50, statusCode: 500 });
  });

  it('passes the change type and page through', async () => {
    await request(`/api/runs/${syncRunId}/changes`, { changeType: 'UPDATED', limit: '10' });
    expect(service.getRunChanges).toHaveBeenCalledWith(syncRunId, 'UPDATED', { cursor: undefined, limit: 10 });
  });

  describe('validation', () => {
    it.each([
      ['a syncRunId that is not an ISO timestamp', '/api/runs/not-a-run/summary', undefined],
      ['a non-numeric limit', `/api/runs/${syncRunId}/errors`, { limit: 'ten' }],
      ['a negative limit', `/api/runs/${syncRunId}/errors`, { limit: '-1' }],
      ['a malformed status code', `/api/runs/${syncRunId}/errors`, { statusCode: '5xx' }],
      ['a missing change type', `/api/runs/${syncRunId}/changes`, undefined],
      ['an unknown change type', `/api/runs/${syncRunId}/changes`, { changeType: 'CREATED' }],
    ])('rejects %s with 400', async (_label, path, query) => {
      expect((await request(path, query)).status).toBe(400);
      expect(service.getRunErrors).not.toHaveBeenCalled();
      expect(service.getRunChanges).not.toHaveBeenCalled();
      expect(service.getRunReport).not.toHaveBeenCalled();
    });

    it('returns 404 for an unknown run report', async () => {
      expect((await request(`/api/runs/${syncRunId}/everything`)).status).toBe(404);
    });

    it('allows GET only', async () => {
      const response = await request('/api/runs', undefined, 'POST');
      expect(response.status).toBe(405);
      expect(response.headers?.Allow).toBe('GET');
      expect((await request(`/api/runs/${syncRunId}/summary`, undefined, 'DELETE')).status).toBe(405);
    });
  });

  describe('reporting errors', () => {
    it('maps a limit over the cap to 400 with its code', async () => {
      service.getRunErrors.mockRejectedValueOnce(new ReportingError('INVALID_LIMIT', `limit must be an integer from 1 to ${MAX_PAGE_LIMIT}`));
      const response = await request(`/api/runs/${syncRunId}/errors`, { limit: `${MAX_PAGE_LIMIT + 1}` });
      expect(response).toMatchObject({ status: 400, body: { code: 'INVALID_LIMIT' } });
      expect(service.getRunErrors).toHaveBeenCalledWith(syncRunId, expect.objectContaining({ limit: MAX_PAGE_LIMIT + 1 }));
    });

    it('maps an unsupported storage mode to 400 with its code', async () => {
      service.getRunChanges.mockRejectedValueOnce(new ReportingError('UNSUPPORTED_IN_STORAGE_MODE', 'not in s3 mode'));
      const response = await request(`/api/runs/${syncRunId}/changes`, { changeType: 'NEW' });
      expect(response).toMatchObject({ status: 400, body: { code: 'UNSUPPORTED_IN_STORAGE_MODE', error: 'not in s3 mode' } });
    });

    it('returns other failures as 500 without a stack trace', async () => {
      service.listRuns.mockRejectedValueOnce(new Error('table not found'));
      const response = await request('/api/runs');
      expect(response).toMatchObject({ status: 500, body: { error: 'Failed to list runs', message: 'table not found' } });
      expect(JSON.stringify(response.body)).not.toContain('at ');
    });
  });
});

describe('Dashboard landscapes route', () => {
  beforeEach(() => jest.spyOn(console, 'log').mockImplementation(() => {}));
  afterEach(() => jest.restoreAllMocks());

  it('returns the landscape listing', async () => {
    const listing = { current: 'mock', landscapes: [{ landscape: 'mock', label: 'mock', isDefault: false, current: true }] };
    jest.spyOn(ServiceProvider, 'getLandscapeListingService').mockReturnValue({ getLandscapes: jest.fn().mockResolvedValue(listing) });
    expect(await request('/api/landscapes')).toEqual(expect.objectContaining({ status: 200, body: listing }));
  });

  it('returns 500 when the registry cannot be read', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    jest.spyOn(ServiceProvider, 'getLandscapeListingService').mockReturnValue({
      getLandscapes: jest.fn().mockRejectedValue(new Error('AccessDenied'))
    });
    expect(await request('/api/landscapes')).toMatchObject({ status: 500, body: { error: 'Failed to list landscapes' } });
  });
});

describe('Dashboard routes with mock services (LOCALHOST_WITH_MOCKS)', () => {
  beforeAll(() => { process.env.LOCALHOST_WITH_MOCKS = 'true'; });
  afterAll(() => { delete process.env.LOCALHOST_WITH_MOCKS; });
  beforeEach(() => jest.spyOn(console, 'log').mockImplementation(() => {}));
  afterEach(() => jest.restoreAllMocks());

  it('serves runs, a summary, a change page and the landscapes from the mocks', async () => {
    const { body: { runs } } = await request('/api/runs');
    expect(runs.length).toBeGreaterThan(0);
    expect(await request(`/api/runs/${runs[0]}/summary`)).toMatchObject({ status: 200, body: { status: 'SUCCEEDED' } });
    expect(await request(`/api/runs/${runs[0]}/changes`, { changeType: 'NEW' })).toMatchObject({
      status: 200, body: { items: [{ changeType: 'NEW' }] }
    });
    expect(await request('/api/landscapes')).toMatchObject({ status: 200, body: { current: 'local' } });
  });

  it('serves the person and bulk sync routes from the mocks without reading the config secret', async () => {
    const savedArn = process.env.INTEGRATION_SECRETS_ARN;
    delete process.env.INTEGRATION_SECRETS_ARN;
    const post = async (path: string, body: any) => {
      const result = await handler({
        rawPath: path,
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        requestContext: { http: { method: 'POST', path } },
        isBase64Encoded: false
      } as unknown as LambdaFunctionUrlEvent);
      return { status: result.statusCode, body: JSON.parse(`${result.body}`) };
    };
    try {
      expect(await post('/api/person-lookup', { personId: 'U1234567', system: 'source' }))
        .toMatchObject({ status: 200, body: { personId: 'U1234567', sourceData: [{ name: 'John Doe' }] } });
      expect(await post('/api/person-sync/preview', { personId: 'U1234567', operation: 'update' }))
        .toMatchObject({ status: 200, body: { operation: 'update' } });
      expect(await post('/api/person-sync/preview-experiment', { personId: 'U1234567', personJson: '{}' }))
        .toMatchObject({ status: 200, body: { data: { personId: 'U1234567' } } });
      expect(await post('/api/person-sync', { personId: 'U1234567', operation: 'update' }))
        .toMatchObject({ status: 200, body: { personId: 'U1234567', status: 'success' } });
      expect(await post('/api/bulk-sync', { operation: 'full' }))
        .toMatchObject({ status: 200, body: { operation: 'full', status: 'started' } });
    } finally {
      if (savedArn !== undefined) process.env.INTEGRATION_SECRETS_ARN = savedArn;
    }
  });
});
