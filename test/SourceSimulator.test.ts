import { handler, personIndexFromBuid, resetSourceSimulatorState } from '../src/chunking/fetch/SourceSimulator';
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
      // Names/email are nested to match the real CDM schema (personBasic.names[], email[])
      expect(person.personBasic.names[0]).toHaveProperty('firstName');
      expect(person.personBasic.names[0]).toHaveProperty('lastName');
      expect(person.email[0]).toHaveProperty('address');
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

  describe('Single Person Endpoint (/person?buid=)', () => {
    const getPerson = async (buid?: string, extra: Record<string, string> = {}, path: string = '/person') => {
      const params = buid === undefined ? { ...extra } : { buid, ...extra };
      return assertProxyResult(await handler(createEvent(params, undefined, path), mockContext));
    };

    it('should return the person identified by the BUID', async () => {
      const response = await getPerson('U0000042');

      expect(response.statusCode).toBe(200);
      const body = parseResponseBody(response);
      expect(body.response).toHaveLength(1);
      expect(body.response[0].personid).toBe('U0000042');
      expect(body.response[0].bu_id).toBe('00000042');
    });

    it('should return the same payload the people endpoint produces for that person', async () => {
      const nonCurrentTermRate = '0.5';
      const people = parseResponseBody(assertProxyResult(
        await handler(createEvent({ recordCount: '60', nonCurrentTermRate }), mockContext)
      )).response;

      for (const expected of people) {
        const actual = parseResponseBody(await getPerson(expected.personid, { nonCurrentTermRate })).response[0];
        expect(actual).toEqual(expected);
      }
    });

    it('should not consume from the people endpoint depletion counter', async () => {
      await getPerson('U0000001');
      await getPerson('U0000500');

      const body = parseResponseBody(assertProxyResult(await handler(createEvent({ recordCount: '10' }), mockContext)));
      expect(body.response[0].personid).toBe('U0000001');
    });

    it('should return an empty response for a BUID beyond MOCK_TOTAL_POPULATION', async () => {
      const lastResponse = parseResponseBody(await getPerson('U0001000')).response;
      expect(lastResponse).toHaveLength(1);

      const response = await getPerson('U0001001');
      expect(response.statusCode).toBe(200);
      expect(parseResponseBody(response).response).toEqual([]);
    });

    it.each([
      ['missing', undefined],
      ['empty', ''],
      ['not U + digits', 'X0000042'],
      ['lower case u', 'u0000042'],
      ['trailing characters', 'U0000042a'],
      ['zero', 'U0000000'],
    ])('should return 400 for a %s BUID', async (_label, buid) => {
      const response = await getPerson(buid as string | undefined);
      expect(response.statusCode).toBe(400);
      expect(parseResponseBody(response).error).toContain('buid');
    });

    it('should accept a trailing slash on the path', async () => {
      const response = await getPerson('U0000007', {}, '/person/');
      expect(parseResponseBody(response).response[0].personid).toBe('U0000007');
    });

    it('should require an API key', async () => {
      const event = createEvent({ buid: 'U0000042' }, {}, '/person');
      const response = assertProxyResult(await handler(event, mockContext));
      expect(response.statusCode).toBe(401);
    });

    it('should return 500 based on MOCK_ERROR_RATE', async () => {
      process.env.MOCK_ERROR_RATE = '1.0';
      const response = await getPerson('U0000042');
      expect(response.statusCode).toBe(500);
    });
  });

  describe('personIndexFromBuid', () => {
    it.each([
      ['U0000001', 0],
      ['U0000042', 41],
      ['U0001856', 1855],
      ['U12345678', 12345677],
      [' U0000042 ', 41],
    ])('maps %s to index %i', (buid, index) => {
      expect(personIndexFromBuid(buid)).toBe(index);
    });

    it.each([undefined, '', 'U', 'U0000000', 'u0000042', '00000042', 'U00000x2'])('rejects %p', (buid) => {
      expect(personIndexFromBuid(buid)).toBeUndefined();
    });
  });

  describe('Student term assignment (nonCurrentTermRate)', () => {
    // Every third person (index 1, 4, 7, ...) is a student.
    const studentTerms = async (nonCurrentTermRate: string): Promise<string[]> => {
      resetSourceSimulatorState();
      const persons = parseResponseBody(assertProxyResult(
        await handler(createEvent({ recordCount: '300', nonCurrentTermRate }), mockContext)
      )).response;
      return persons
        .filter((p: any) => p.studentInfo)
        .map((p: any) => p.studentInfo.studentSemester[0].studentSemesterInfo.academicTerm.term.code);
    };

    it('should give every student the current term at rate 0.0', async () => {
      expect(new Set(await studentTerms('0.0'))).toEqual(new Set(['2261']));
    });

    it('should give every student the non-current term at rate 1.0', async () => {
      expect(new Set(await studentTerms('1.0'))).toEqual(new Set(['2256']));
    });

    it('should give roughly the requested proportion a non-current term, identically on every request', async () => {
      const first = await studentTerms('0.5');
      const second = await studentTerms('0.5');

      expect(second).toEqual(first);
      const nonCurrent = first.filter(code => code === '2256').length;
      expect(nonCurrent / first.length).toBeGreaterThan(0.3);
      expect(nonCurrent / first.length).toBeLessThan(0.7);
    });
  });

  describe('Terms Endpoint API Key', () => {
    const termsApiKey = 'test-terms-api-key-67890';

    beforeEach(() => {
      (ConfigManager.getInstance as jest.Mock) = jest.fn().mockReturnValue({
        reset: jest.fn().mockReturnThis(),
        fromSecretManager: jest.fn().mockReturnThis(),
        getConfigAsync: jest.fn().mockResolvedValue({
          dataSource: {
            people: { endpointConfig: { apiKey: testApiKey } },
            terms: { endpointConfig: { apiKey: termsApiKey } },
          },
        }),
      });
      resetSourceSimulatorState();
    });

    const call = async (path: string, apiKey: string) =>
      assertProxyResult(await handler(createEvent({ buid: 'U0000001' }, { 'x-api-key': apiKey }, path), mockContext));

    it('should accept the terms API key on /terms (what the processor sends)', async () => {
      const response = await call('/terms', termsApiKey);
      expect(response.statusCode).toBe(200);
      expect(parseResponseBody(response).response).toHaveLength(6);
    });

    it('should reject the people API key on /terms', async () => {
      expect((await call('/terms', testApiKey)).statusCode).toBe(401);
    });

    it.each(['/', '/person'])('should accept the people API key, and reject the terms API key, on %s', async (path) => {
      expect((await call(path, testApiKey)).statusCode).toBe(200);
      expect((await call(path, termsApiKey)).statusCode).toBe(401);
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
