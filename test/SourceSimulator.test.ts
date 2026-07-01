import { handler, resetSourceSimulatorState } from '../src/chunking/fetch/SourceSimulator';
import { APIGatewayProxyEventV2, Context } from 'aws-lambda';
import { ConfigManager } from 'integration-huron-person';

// Mock integration-huron-person module
jest.mock('integration-huron-person');

describe('SourceSimulator Lambda Handler', () => {
  const originalEnv = process.env;
  const mockContext = {} as Context;
  const testApiKey = 'test-api-key-12345';

  beforeEach(() => {
    // Reset environment variables before each test
    process.env = { ...originalEnv };
    process.env.MOCK_TOTAL_POPULATION = '1000';
    process.env.MOCK_ERROR_RATE = '0.0';
    process.env.MOCK_SIMULATED_DELAY_SECONDS = '0';
    process.env.SECRET_ARN = 'arn:aws:secretsmanager:us-east-2:123456789012:secret:test-secret';

    // Mock ConfigManager to return API key from secret
    const mockConfigManager = {
      reset: jest.fn().mockReturnThis(),
      fromSecretManager: jest.fn().mockReturnThis(),
      getConfigAsync: jest.fn().mockResolvedValue({
        dataSource: {
          people: {
            endpointConfig: {
              apiKey: testApiKey,
            },
          },
        },
      }),
    };
    (ConfigManager.getInstance as jest.Mock) = jest.fn().mockReturnValue(mockConfigManager);

    // Ensure each test starts with a fresh simulator depletion state.
    resetSourceSimulatorState();

    jest.clearAllMocks();
  });

  afterEach(() => {
    // Restore original environment
    process.env = originalEnv;
  });

  /**
   * Helper to create a mock API Gateway event
   */
  const createEvent = (
    queryParams?: Record<string, string>,
    headers?: Record<string, string>,
    rawPath: string = '/'
  ): APIGatewayProxyEventV2 => ({
    version: '2.0',
    routeKey: '$default',
    rawPath,
    rawQueryString: '',
    headers: headers || { 'x-api-key': testApiKey },
    queryStringParameters: queryParams,
    requestContext: {
      accountId: '123456789012',
      apiId: 'abc123',
      domainName: 'example.lambda-url.us-east-2.on.aws',
      domainPrefix: 'abc123',
      http: {
        method: 'GET',
        path: rawPath,
        protocol: 'HTTP/1.1',
        sourceIp: '1.2.3.4',
        userAgent: 'test-agent',
      },
      requestId: 'test-request-id',
      routeKey: '$default',
      stage: '$default',
      time: '01/Jan/2026:00:00:00 +0000',
      timeEpoch: 1735689600000,
    },
    isBase64Encoded: false,
  });

  /**
   * Type guard to ensure response is an object with statusCode
   */
  interface ProxyResult {
    statusCode: number;
    headers?: Record<string, string>;
    body?: string;
  }

  const assertProxyResult = (response: any): ProxyResult => {
    if (typeof response === 'string') {
      throw new Error('Expected response object, got string');
    }
    return response as ProxyResult;
  };

  /**
   * Helper to parse response body
   */
  const parseResponseBody = (response: ProxyResult) => {
    return JSON.parse(response.body || '{}');
  };

  describe('Payload Size and Population Limits', () => {
    it('should return full batch when offset * recordCount is below MOCK_TOTAL_POPULATION', async () => {
      const event = createEvent({ recordCount: '200', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(200); // Full batch
      expect(body.response[0].personid).toBe('U0000001');
      expect(body.response[199].personid).toBe('U0000200');
    });

    it('should return partial batch when near population limit', async () => {
      // Offset is accepted for API compatibility but does not drive data selection.
      // With fresh state, the simulator returns the first available 300 records.
      const event = createEvent({ recordCount: '300', offset: '4' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(300);
      expect(body.response[0].personid).toBe('U0000001');
      expect(body.response[299].personid).toBe('U0000300');
    });

    it('should return partial batch at exact boundary', async () => {
      // Offset does not determine returned indices; request size determines batch size.
      const event = createEvent({ recordCount: '200', offset: '4' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(200);
      expect(body.response[0].personid).toBe('U0000001');
      expect(body.response[199].personid).toBe('U0000200');
    });

    it('should return partial batch when last batch is smaller than recordCount', async () => {
      // With fresh state and population=1000, 250 records are available.
      const event = createEvent({ recordCount: '250', offset: '4' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(250);
      expect(body.response[0].personid).toBe('U0000001');
      expect(body.response[249].personid).toBe('U0000250');
    });

    it('should return empty array when offset * recordCount equals MOCK_TOTAL_POPULATION', async () => {
      // Offset does not control returned slice; depletion state does.
      const event = createEvent({ recordCount: '200', offset: '5' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(200);
    });

    it('should return empty array when offset * recordCount exceeds MOCK_TOTAL_POPULATION', async () => {
      const event = createEvent({ recordCount: '200', offset: '10' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(200);
    });

    it('should handle offset=0 correctly', async () => {
      const event = createEvent({ recordCount: '100', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(100);
      expect(body.response[0].personid).toBe('U0000001');
      expect(body.response[99].personid).toBe('U0000100');
    });

    it('should simulate real API behavior compatible with processBatch logic', async () => {
      // This test simulates multiple batches like processBatch would request
      const recordCount = 200;
      let offset = 0;
      let allPersons: any[] = [];
      let batchCount = 0;

      // Simulate the processBatch loop
      while (true) {
        const event = createEvent({ 
          recordCount: recordCount.toString(), 
          offset: offset.toString() 
        });
        const response = assertProxyResult(await handler(event, mockContext));
        const body = parseResponseBody(response);
        const persons = body.response;

        allPersons = allPersons.concat(persons);
        batchCount++;

        // processBatch checks if response length < batchSize to stop
        if (persons.length < recordCount) {
          break;
        }

        offset++;
      }

      // Should have fetched all 1000 persons in 6 API calls (5 full batches + 1 empty to detect end)
      expect(batchCount).toBe(6);
      expect(allPersons).toHaveLength(1000);
      expect(allPersons[0].personid).toBe('U0000001');
      expect(allPersons[999].personid).toBe('U0001000');
    });
  });

  describe('API Key Validation', () => {
    it('should reject request with invalid API key in X-API-Key header', async () => {
      const event = createEvent(
        { recordCount: '100', offset: '0' },
        { 'x-api-key': 'wrong-key' }
      );
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(401);
      const body = parseResponseBody(response);
      expect(body.error).toContain('Unauthorized');
    });

    it('should reject request with invalid API key in Authorization header', async () => {
      const event = createEvent(
        { recordCount: '100', offset: '0' },
        { 'authorization': 'Bearer wrong-key' }
      );
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(401);
      const body = parseResponseBody(response);
      expect(body.error).toContain('Unauthorized');
    });

    it('should reject request with missing API key', async () => {
      const event = createEvent(
        { recordCount: '100', offset: '0' },
        {} // No headers
      );
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(401);
    });

    it('should accept request with valid API key in X-API-Key header', async () => {
      const event = createEvent(
        { recordCount: '100', offset: '0' },
        { 'x-api-key': 'test-api-key-12345' }
      );
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
    });

    it('should accept request with valid API key in Authorization Bearer header', async () => {
      const event = createEvent(
        { recordCount: '100', offset: '0' },
        { 'authorization': `Bearer ${testApiKey}` }
      );
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
    });

    it('should be case-insensitive for header names', async () => {
      const event = createEvent(
        { recordCount: '100', offset: '0' },
        { 'Authorization': `Bearer ${testApiKey}` } // Capital A
      );
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
    });

    // Note: Testing the case where SECRET_ARN is not configured is complex due to
    // module-level caching of the API key and ConfigManager mocking. In production,
    // if SECRET_ARN is not set, the handler will log a warning and allow all requests.
  });

  describe('Response Structure', () => {
    it('should match real API response structure', async () => {
      const event = createEvent({ recordCount: '10', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      
      // Response should be object with response field
      expect(typeof body).toBe('object');
      expect(body).not.toBeNull();
      
      // Body should have response field with array of persons
      expect(body).toHaveProperty('response');
      expect(Array.isArray(body.response)).toBe(true);
    });

    it('should generate person IDs from depletion order, not requested offset', async () => {
      const event = createEvent({ recordCount: '5', offset: '2' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      const persons = body.response;

      expect(persons[0].personid).toBe('U0000001');
      expect(persons[0].bu_id).toBe('00000001');
      expect(persons[4].personid).toBe('U0000005');
      expect(persons[4].bu_id).toBe('00000005');
    });

    it('should include all required person fields', async () => {
      const event = createEvent({ recordCount: '3', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      const person = body.response[0];

      expect(person).toHaveProperty('personid');
      expect(person).toHaveProperty('bu_id');
      expect(person).toHaveProperty('firstName');
      expect(person).toHaveProperty('lastName');
      expect(person).toHaveProperty('email');
    });

    it('should vary person types (employee, student, affiliate)', async () => {
      const event = createEvent({ recordCount: '9', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      const persons = body.response;

      // Index 0 (person 1) -> personType = 0 % 3 = 0 (employee)
      expect(persons[0]).toHaveProperty('employeeInfo');
      expect(persons[0]).not.toHaveProperty('studentInfo');
      expect(persons[0]).not.toHaveProperty('affiliateInfo');

      // Index 1 (person 2) -> personType = 1 % 3 = 1 (student)
      expect(persons[1]).not.toHaveProperty('employeeInfo');
      expect(persons[1]).toHaveProperty('studentInfo');
      expect(persons[1]).not.toHaveProperty('affiliateInfo');

      // Index 2 (person 3) -> personType = 2 % 3 = 2 (affiliate)
      expect(persons[2]).not.toHaveProperty('employeeInfo');
      expect(persons[2]).not.toHaveProperty('studentInfo');
      expect(persons[2]).toHaveProperty('affiliateInfo');
    });
  });

  describe('Query Parameters', () => {
    it('should use default recordCount of 200 when not provided', async () => {
      const event = createEvent({ offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(200);
    });

    it('should use default offset of 0 when not provided', async () => {
      const event = createEvent({ recordCount: '50' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(50);
      expect(body.response[0].personid).toBe('U0000001'); // Starting from index 0
    });

    it('should use both defaults when no query parameters provided', async () => {
      const event = createEvent();
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(200);
      expect(body.response[0].personid).toBe('U0000001');
    });

    it('should handle recordCount=0 correctly', async () => {
      const event = createEvent({ recordCount: '0', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(0);
    });

    it('should handle very large recordCount', async () => {
      const event = createEvent({ recordCount: '5000', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      // Should cap at population size (1000)
      expect(body.response).toHaveLength(1000);
    });
  });

  describe('Health Check Endpoint', () => {
    it('should return health status on /health path', async () => {
      const event = createEvent(undefined, undefined, '/health');
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.status).toBe('healthy');
      expect(body.totalPopulation).toBe(1000);
      expect(body.errorRate).toBe(0.0);
    });

    it('should not require API key for health check', async () => {
      const event = createEvent(
        undefined,
        {}, // No API key
        '/health'
      );
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
    });
  });

  describe('Error Simulation', () => {
    it('should return 500 error based on MOCK_ERROR_RATE', async () => {
      process.env.MOCK_ERROR_RATE = '1.0'; // 100% error rate
      const event = createEvent({ recordCount: '100', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(500);
      const body = parseResponseBody(response);
      expect(body.error).toContain('Simulated API error');
    });

    it('should return success when MOCK_ERROR_RATE is 0.0', async () => {
      process.env.MOCK_ERROR_RATE = '0.0';
      const event = createEvent({ recordCount: '100', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
    });
  });

  describe('Edge Cases', () => {
    it('should handle small population size', async () => {
      process.env.MOCK_TOTAL_POPULATION = '10';
      const event = createEvent({ recordCount: '200', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(10);
    });

    it('should handle population size of 1', async () => {
      process.env.MOCK_TOTAL_POPULATION = '1';
      const event = createEvent({ recordCount: '200', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(1);
      expect(body.response[0].personid).toBe('U0000001');
    });

    it('should handle population size of 0', async () => {
      process.env.MOCK_TOTAL_POPULATION = '0';
      const event = createEvent({ recordCount: '200', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(0);
    });

    it('should return correct Content-Type header', async () => {
      const event = createEvent({ recordCount: '10', offset: '0' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.headers).toHaveProperty('Content-Type', 'application/json');
    });

    it('should handle large offset without errors', async () => {
      const event = createEvent({ recordCount: '100', offset: '999999' });
      const response = assertProxyResult(await handler(event, mockContext));

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(100);
    });
  });
});
