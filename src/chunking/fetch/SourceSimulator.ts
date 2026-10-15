/**
 * Source Simulator Lambda - Mock API for Person and Terms Data
 * 
 * This Lambda simulates both the source person API and current terms API for testing,
 * eliminating the 30-minute cooldown constraint of the real APIs.
 * 
 * **Endpoints:**
 * - `/` or empty path: Person data endpoint (people API)
 * - `/person?buid=U0000042`: Single person endpoint (person API)
 * - `/terms` or `/current-terms`: Current terms endpoint
 *
 * **Design (People Endpoint):**
 * - Stateful depletion model: Every request claims the next contiguous slice of available records
 * - Non-deterministic by request params: `offset` is accepted for compatibility but does not drive allocation
 * - Validates the API key against the source keys in the config secret (see "API keys" below)
 * - Returns minimal field set (only fields DataMapper uses)
 * - Matches real API response structure: `{ response: [persons] }`
 * - Simulates slow API: Optional delay before responding via MOCK_SIMULATED_DELAY_SECONDS
 * 
 * **Design (Single Person Endpoint):**
 * - Mirrors the real person API: one person, selected by the `buid` query parameter. The `buid`
 *   parameter is also what makes the chunker treat the fetch as a single request rather than
 *   paging through it (see `BigJsonFetch.isNotBatchable()`)
 * - Stateless and idempotent: `U0000042` is the person at index 41, the same payload the people
 *   endpoint produces when its depletion reaches that index. The depletion counter is not touched.
 * - A BUID beyond MOCK_TOTAL_POPULATION returns `{ response: [] }` (not in the source), and a
 *   missing or malformed BUID (not `U` + digits) returns 400
 * - Accepts `nonCurrentTermRate` like the people endpoint
 *
 * **Design (Terms Endpoint):**
 * - Called by the processor tasks for simulated-source runs (the Runner sends the terms endpoint
 *   along with the run, see Flags.termsEndpoint)
 * - Returns static list of 6 terms (2 current, 2 past, 2 future)
 * - No pagination (terms are small dataset)
 * - Matches real API response structure: `{ response: [terms] }`
 * 
 * **Query Parameters (People Endpoint):**
 * - `recordCount`: Number of records to return (default: 200)
 * - `offset`: Batch offset for pagination (default: 0)
 * - `nonCurrentTermRate`: Proportion (0.0-1.0) of students with a non-current term (default: 0.0).
 *   Which students get one is decided by a hash of the person's index, not by chance, so a person's
 *   payload is the same on every request.
 *
 * **Query Parameters (Single Person Endpoint):**
 * - `buid`: The person's BUID, e.g. `U0000042` (required)
 * - `nonCurrentTermRate`: As for the people endpoint
 * 
 * **Headers:**
 * - `Authorization`: Bearer token containing API key
 *   OR
 * - `X-API-Key`: Direct API key header
 * 
 * **API keys:** every endpoint accepts ANY of the three source API keys in the integration config
 * secret (people, person and terms - dataSource.<people|person|terms>.endpointConfig.apiKey), not
 * just the key of the real API it stands in for. A request without one of them is still rejected.
 * The reason: callers choose the key by URL, and the simulator's URLs cannot satisfy all of them at
 * once while mock landscapes keep supporting hybrid runs (real source -> mock target):
 * - Hybrid runs need the config secret's source URLs to be the REAL ones. So the chunker, which
 *   picks the person key only when a request's URL matches the configured person URL
 *   (ChunkConfigOverride.getOverriddenApiKey()), never recognizes the simulator's `/person` and
 *   sends it the people key in a simulated single-person run from the Runner.
 * - A client given the simulator's URLs directly (e.g. integration-huron-person's SyncPerson.ts with
 *   a mock .env) sends each endpoint its own key: the person key to `/person`.
 * - The processor always sends the terms key, whichever URL it fetches terms from.
 * So `/person` must accept both the people and the person key, and the same reasoning applies to
 * the other endpoints. Matching each endpoint to one key would instead require the simulator to use
 * the real paths and the chunker to match keys on path alone.
 * 
 * **Environment Variables:**
 * - MOCK_TOTAL_POPULATION: Total persons available (e.g., 10000)
 * - MOCK_ERROR_RATE: Probability of simulated errors (0.0-1.0)
 * - MOCK_SIMULATED_DELAY_SECONDS: Delay in seconds before responding (simulates slow API)
 * - SECRET_ARN: ARN of Secrets Manager secret containing the API keys
 * 
 * **Response Format:**
 * ```json
 * {
 *   "response": [
 *     { "personid": "U0000001", "bu_id": "00000001", ... },
 *     { "personid": "U0000002", "bu_id": "00000002", ... }
 *   ]
 * }
 * ```
 */

// Lambda Function URLs use APIGatewayProxyEventV2 format (same as API Gateway HTTP API v2.0)
// See: https://docs.aws.amazon.com/lambda/latest/dg/urls-invocation.html
import { GetFunctionUrlConfigCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { APIGatewayProxyEventV2, APIGatewayProxyResultV2, Context } from 'aws-lambda';
import { ConfigManager, DataSourceConfig, EndpointConfigForApiKey } from 'integration-huron-person';
import { LambdaFunctionEnvironmentVariable } from '../../runner/LambdaFunctionEnvironmentVariable';
import { AbstractAtomicCounter } from '../../dynamodb/AtomicCounter';

/**
 * Term interface matching CurrentTermsDataSource
 */
interface Term {
  term: string;
  termDescription: string;
  academicCareer: string;
  termBeginDate: string;
  termEndDate: string;
  currentInd: string;
}

export const FUNCTION_BASE_NAME = 'source-simulator';
export const SIMULATOR_COUNTER_NAME = 'simulator-offset-counter';
export const PERSON_PATH = '/person';
export const TERMS_PATH = '/terms';
const BUID_PATTERN = /^U(\d+)$/;

/**
 * Convert a BUID into the index of the simulated person it identifies (the inverse of the personid
 * built by generateMockPerson): `U0000001` -> 0, `U0000042` -> 41.
 *
 * @param buid - BUID of a simulated person, e.g. `U0000042`
 * @returns The zero-based person index, or undefined if the BUID is not `U` + digits or is `U0000000`
 */
export function personIndexFromBuid(buid: string | undefined): number | undefined {
  const match = BUID_PATTERN.exec((buid ?? '').trim());
  if (!match) {
    return undefined;
  }
  const index = parseInt(match[1], 10) - 1;
  return index >= 0 ? index : undefined;
}

/**
 * Deterministic stand-in for Math.random() keyed on a person index: maps the index to a well-mixed
 * value in [0, 1) (MurmurHash3's 32-bit finalizer), so a person's random-looking attributes come out
 * the same on every request and from either the people or the single person endpoint.
 *
 * @param index - Zero-based person index
 * @returns A value in [0, 1)
 */
function unitIntervalHash(index: number): number {
  let h = (index + 1) >>> 0;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return (h >>> 0) / 4294967296;
}
export enum ENVIRONMENT_VARIABLES_NAMES {
  MOCK_TOTAL_POPULATION = 'MOCK_TOTAL_POPULATION',
  MOCK_ERROR_RATE = 'MOCK_ERROR_RATE',
  MOCK_SIMULATED_DELAY_SECONDS = 'MOCK_SIMULATED_DELAY_SECONDS',
  SECRET_ARN = 'SECRET_ARN',
  STACK_ID = 'STACK_ID',
  REGION = 'REGION',
  LANDSCAPE = 'LANDSCAPE'
}

// Cache the API key between Lambda invocations (Lambda reuses execution environments)
let cachedApiKeys: string[] | null = null;
let cachedCounter: AbstractAtomicCounter | null = null;
let inMemoryNextIndex = 0;

export function resetSourceSimulatorState(): void {
  cachedApiKeys = null;
  cachedCounter = null;
  inMemoryNextIndex = 0;
}

interface MockPerson {
  personid: string;
  bu_id: string;
  personBasic?: {
    names: Array<{
      firstName?: string;
      lastName?: string;
      nameType?: string;
      source?: string;
    }>;
  };
  email?: Array<{
    address?: string;
    type?: string;
    source?: string;
  }>;
  employeeInfo?: {
    positions: Array<{
      positionInfo: {
        BasicData: {
          mainPernrIndicator: string;
          employmentDate: string;
          terminationDate: string;
        };
        Department: {
          organizationalUnit: string;
        };
      };
    }>;
  };
  studentInfo?: {
    studentSemester: Array<{
      studentSemesterInfo: {
        academicTerm: {
          term: {
            code: string;
          };
        };
        academicCareer: {
          code: string;
        };
        degreeProgram: Array<{
          isCurrentAcademicProgram: string;
          academicOrganization: {
            code: string;
          };
          academicGroup: {
            code: string;
          };
          academicPlan: any[];
        }>;
      };
    }>;
  };
  affiliateInfo?: {
    organizationalUnit?: {
      code: string;
    };
    department?: {
      code: string;
    };
  };
}

/**
 * Generate a mock person record with deterministic ID based on index.
 * 
 * @param index - Zero-based index for person ID generation
 * @param options - Optional configuration
 * @param options.nonCurrentTermRate - Proportion (0.0-1.0) of students with a non-current term (default: 0.0)
 * @returns Mock person object with minimal fields DataMapper uses
 */
function generateMockPerson(index: number, options?: { nonCurrentTermRate?: number }): MockPerson {
  const nonCurrentTermRate = options?.nonCurrentTermRate ?? 0.0;
  const personid = `U${String(index + 1).padStart(7, '0')}`;
  const bu_id = String(index + 1).padStart(8, '0');

  // Vary person type based on index modulo to create mix of employees/students/affiliates
  const personType = index % 3;

  const basePerson: MockPerson = {
    personid,
    bu_id,
    // Nested under personBasic.names[]/email[] to match the real CDM schema, not flat fields
    personBasic: {
      names: [
        { firstName: `FirstName${index + 1}`, lastName: `LastName${index + 1}`, nameType: 'PRI', source: 'SAP' },
      ],
    },
    email: [
      { address: `${personid}@bu.edu`, type: 'University', source: 'SAP' },
    ],
  };

  // Employee (33% of population)
  if (personType === 0) {
    basePerson.employeeInfo = {
      positions: [
        {
          positionInfo: {
            BasicData: {
              mainPernrIndicator: 'Y',
              employmentDate: '20200101',
              terminationDate: '',
            },
            Department: {
              organizationalUnit: '10003827', // Fixed org for simplicity
            },
          },
        },
      ],
    };
  }
  // Student (33% of population)
  else if (personType === 1) {
    // Term assignment based on nonCurrentTermRate, seeded by the index so the same person always
    // gets the same term. If hash < rate, assign past term "2256", else assign current term "2261"
    const useNonCurrentTerm = unitIntervalHash(index) < nonCurrentTermRate;
    const termCode = useNonCurrentTerm ? '2256' : '2261';

    basePerson.studentInfo = {
      studentSemester: [
        {
          studentSemesterInfo: {
            academicTerm: {
              term: {
                code: termCode,
              },
            },
            academicCareer: {
              code: 'UGRD',
            },
            degreeProgram: [
              {
                isCurrentAcademicProgram: 'Y',
                academicOrganization: {
                  code: 'CAS',
                },
                academicGroup: {
                  code: 'CAS',
                },
                academicPlan: [],
              },
            ],
          },
        },
      ],
    };
  }
  // Affiliate (33% of population)
  else {
    basePerson.affiliateInfo = {
      organizationalUnit: {
        code: 'AFFILIATE',
      },
    };
  }

  return basePerson;
}

/**
 * Retrieve the source API keys (people, person and terms) from the integration config secret using
 * ConfigManager. Any of them is accepted on any endpoint - see "API keys" in the header comment.
 * Uses caching to avoid repeated calls to Secrets Manager.
 * 
 * @returns The distinct keys configured, or an empty list (no validation) if SECRET_ARN is not
 * configured, the secret cannot be read, or it holds no keys
 */
async function getApiKeys(): Promise<string[]> {
  // Return cached value if available
  if (cachedApiKeys !== null) {
    return cachedApiKeys;
  }

  const secretArn = process.env.SECRET_ARN;
  if (!secretArn) {
    console.warn('SECRET_ARN not configured, skipping API key validation');
    return [];
  }

  try {
    // Use ConfigManager to retrieve config from Secrets Manager
    // This follows the same pattern as docker/processor.ts
    const configManager = ConfigManager.getInstance();
    const config = await configManager
      .reset()
      .fromSecretManager(secretArn)
      .getConfigAsync('people');
    
    // Extract the API keys from config (cast to DataSourceConfig since S3DataSourceConfig doesn't have endpointConfig)
    const { people, person, terms } = config?.dataSource ?? {};
    const keys: string[] = [];
    for (const [name, source] of Object.entries({ people, person, terms })) {
      const apiKey = (source as DataSourceConfig | undefined)?.endpointConfig?.apiKey;
      if (apiKey) {
        keys.push(apiKey);
      } else {
        console.error(`API key not found in secret at dataSource.${name}.endpointConfig.apiKey`);
      }
    }

    // Cache the API keys for subsequent invocations
    cachedApiKeys = [...new Set(keys)];
    console.log(`Successfully retrieved and cached ${cachedApiKeys.length} distinct API key(s) from Secrets Manager`);
    return cachedApiKeys;
  } catch (error) {
    console.error('Error retrieving API keys from Secrets Manager via ConfigManager:', error);
    return [];
  }
}

export function getSimulatorCounter(params?: { stackId?: string, region?: string, landscape?: string  }): AbstractAtomicCounter | null {
  if (cachedCounter) {
    return cachedCounter;
  }

  const stackId = params?.stackId ?? process.env.STACK_ID;
  const region = params?.region ?? process.env.REGION;
  const landscape = params?.landscape ?? process.env.LANDSCAPE;

  if (!stackId || !region || !landscape) {
    return null;
  }

  cachedCounter = new class extends AbstractAtomicCounter {
    public getCounterName(): string {
      return SIMULATOR_COUNTER_NAME;
    }
  }({ stackId, region, landscape });

  return cachedCounter;
}

async function allocateStatefulRange(recordCount: number, totalPopulation: number): Promise<{
  startIndex: number;
  endIndex: number;
  actualCount: number;
  mode: 'stateful';
}> {
  const counter = getSimulatorCounter();
  if (counter) {
    const claimedUpperBound = await counter.increment(recordCount);
    const startIndex = Math.max(0, claimedUpperBound - recordCount);
    const endIndex = Math.min(startIndex + recordCount, totalPopulation);
    const actualCount = Math.max(0, endIndex - startIndex);

    return { startIndex, endIndex, actualCount, mode: 'stateful' };
  }

  // Local/test fallback only: preserve depletion semantics even without DynamoDB-backed counter.
  // This keeps behavior non-deterministic by offset while avoiding hard dependency on AWS in unit tests.
  const startIndex = inMemoryNextIndex;
  inMemoryNextIndex += recordCount;
  const endIndex = Math.min(startIndex + recordCount, totalPopulation);
  const actualCount = Math.max(0, endIndex - startIndex);

  return { startIndex, endIndex, actualCount, mode: 'stateful' };
}

/**
 * Validate API key from request headers.
 * 
 * @param event - Lambda event with headers
 * @param acceptedApiKeys - The API keys from Secrets Manager, any one of which is accepted
 * @returns true if API key is valid, false otherwise
 */
function validateApiKey(event: APIGatewayProxyEventV2, acceptedApiKeys: string[]): boolean {
  if (acceptedApiKeys.length === 0) {
    // If no API key configured, allow all requests
    return true;
  }

  const headers = event.headers || {};
  
  // Check Authorization header (Bearer token)
  const authHeader = headers['authorization'] || headers['Authorization'];
  if (authHeader) {
    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    if (match && acceptedApiKeys.includes(match[1])) {
      return true;
    }
  }

  // Check X-API-Key header
  const apiKeyHeader = headers['x-api-key'] || headers['X-API-Key'];
  if (apiKeyHeader && acceptedApiKeys.includes(apiKeyHeader)) {
    return true;
  }

  return false;
}

/**
 * Determine which endpoint is being requested based on the raw path.
 * 
 * @param rawPath - The raw path from the Lambda event (e.g., '/', '/person', '/terms', '/current-terms')
 * @returns 'people' for person data endpoint, 'person' for the single person endpoint, 'terms' for
 * current terms endpoint
 */
function getEndpointType(rawPath: string): 'people' | 'person' | 'terms' {
  const normalized = (rawPath || '/').toLowerCase().trim();

  // If path contains 'term', route to terms endpoint
  if (normalized.includes('term')) {
    return 'terms';
  }

  if (normalized.replace(/\/+$/, '') === PERSON_PATH) {
    return 'person';
  }

  // Default to people endpoint
  return 'people';
}

/**
 * Generate mock current terms data for testing.
 * Returns static list of 6 terms covering current, past, and future semesters.
 * 
 * @returns Array of Term objects matching CurrentTermsDataSource format
 */
function generateMockTerms(): Term[] {
  return [
    // Current terms (currentInd='Y')
    {
      term: '2261',
      termDescription: 'Spring 2026',
      academicCareer: 'UGRD',
      termBeginDate: '2026-01-20',
      termEndDate: '2026-05-15',
      currentInd: 'Y'
    },
    {
      term: '2262',
      termDescription: 'Spring 2026',
      academicCareer: 'GRAD',
      termBeginDate: '2026-01-20',
      termEndDate: '2026-05-15',
      currentInd: 'Y'
    },
    // Past terms (currentInd='N')
    {
      term: '2256',
      termDescription: 'Fall 2025',
      academicCareer: 'UGRD',
      termBeginDate: '2025-09-03',
      termEndDate: '2025-12-20',
      currentInd: 'N'
    },
    {
      term: '2257',
      termDescription: 'Fall 2025',
      academicCareer: 'GRAD',
      termBeginDate: '2025-09-03',
      termEndDate: '2025-12-20',
      currentInd: 'N'
    },
    // Future terms (currentInd='N')
    {
      term: '2264',
      termDescription: 'Summer 2026',
      academicCareer: 'UGRD',
      termBeginDate: '2026-05-20',
      termEndDate: '2026-08-15',
      currentInd: 'N'
    },
    {
      term: '2265',
      termDescription: 'Summer 2026',
      academicCareer: 'LAW',
      termBeginDate: '2026-05-20',
      termEndDate: '2026-08-15',
      currentInd: 'N'
    }
  ];
}

/**
 * Parse the optional nonCurrentTermRate query parameter (0.0 to 1.0, default 0.0), which controls
 * the proportion of students with a non-current term. Out-of-range values are clamped.
 */
function parseNonCurrentTermRate(queryParams: Record<string, string | undefined>): number {
  const nonCurrentTermRate = parseFloat(queryParams.nonCurrentTermRate || '0.0');
  if (nonCurrentTermRate < 0.0 || nonCurrentTermRate > 1.0) {
    console.warn(`Invalid nonCurrentTermRate=${nonCurrentTermRate}, clamping to [0.0, 1.0]`);
    return Math.max(0.0, Math.min(1.0, nonCurrentTermRate));
  }
  return nonCurrentTermRate;
}

/**
 * Single person endpoint: return the simulated person identified by the `buid` query parameter.
 * Stateless - the depletion counter used by the people endpoint is not touched.
 */
async function handlePersonRequest(event: APIGatewayProxyEventV2, requestStartTime: number): Promise<APIGatewayProxyResultV2> {
  const queryParams = event.queryStringParameters || {};
  const { buid } = queryParams;
  const totalPopulation = parseInt(process.env.MOCK_TOTAL_POPULATION || '10000', 10);
  const errorRate = parseFloat(process.env.MOCK_ERROR_RATE || '0.0');
  const nonCurrentTermRate = parseNonCurrentTermRate(queryParams);

  const index = personIndexFromBuid(buid);
  if (index === undefined) {
    console.error(`Missing or malformed buid query parameter: ${buid}`);
    return {
      statusCode: 400,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: `Missing or malformed buid query parameter (expected "U" followed by digits): ${buid}` }),
    };
  }

  if (errorRate > 0 && Math.random() < errorRate) {
    console.error('Simulating API error based on MOCK_ERROR_RATE');
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Simulated API error' }),
    };
  }

  // A person beyond the population is not in the source (e.g. dropped by a smaller follow-up run).
  const persons: MockPerson[] = index < totalPopulation
    ? [generateMockPerson(index, { nonCurrentTermRate })]
    : [];
  console.log(`Returning ${persons.length} person(s) for buid=${buid} (index=${index}, totalPopulation=${totalPopulation})`);

  // Simulate API delay if configured (mimics slow real API behavior)
  const simulatedDelaySeconds = parseFloat(process.env.MOCK_SIMULATED_DELAY_SECONDS || '0');
  const remainingDelay = simulatedDelaySeconds - (Date.now() - requestStartTime) / 1000;
  if (remainingDelay > 0) {
    console.log(`Simulating API delay: waiting ${remainingDelay.toFixed(2)} more seconds before responding`);
    await new Promise(resolve => setTimeout(resolve, remainingDelay * 1000));
  }

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ response: persons }),
  };
}

/**
 * Main Lambda handler for Function URL requests.
 * Supports the person data endpoint (/), the single person endpoint (/person?buid=...) and the
 * current terms endpoint (/terms).
 */
export async function handler(event: APIGatewayProxyEventV2, context: Context): Promise<APIGatewayProxyResultV2> {
  const requestStartTime = Date.now();
  
  console.log('Source Simulator received request:', JSON.stringify({
    rawPath: event.rawPath,
    queryStringParameters: event.queryStringParameters,
    headers: Object.keys(event.headers || {}),
  }));

  // Health check endpoint (no auth required)
  if (event.rawPath === '/health') {
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        status: 'healthy',
        totalPopulation: parseInt(process.env.MOCK_TOTAL_POPULATION || '10000', 10),
        errorRate: parseFloat(process.env.MOCK_ERROR_RATE || '0.0'),
      }),
    };
  }

  // Determine which endpoint is being requested
  const endpointType = getEndpointType(event.rawPath || '/');

  // Validate API key (for non-health endpoints). Any of the people, person and terms keys is
  // accepted on any endpoint - see "API keys" in the header comment for why.
  if (validateApiKey(event, await getApiKeys())) {
    console.log('Valid API key provided');
  }
  else {
    console.error('Invalid API key provided');
    return {
      statusCode: 401,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Unauthorized: Invalid API key' }),
    };
  }

  // Route to terms endpoint if requested
  if (endpointType === 'terms') {
    console.log('Routing to terms endpoint');

    // Simulate slow API (optional delay)
    const simulatedDelaySeconds = parseFloat(process.env.MOCK_SIMULATED_DELAY_SECONDS || '0');
    if (simulatedDelaySeconds > 0) {
      console.log(`Simulating API delay: ${simulatedDelaySeconds} seconds`);
      await new Promise((resolve) => setTimeout(resolve, simulatedDelaySeconds * 1000));
    }

    // Simulate error (optional error rate)
    const errorRate = parseFloat(process.env.MOCK_ERROR_RATE || '0.0');
    if (Math.random() < errorRate) {
      console.error('Simulating random error response');
      return {
        statusCode: 500,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: 'Simulated random error' }),
      };
    }

    // Generate mock terms
    const terms = generateMockTerms();
    
    const requestDurationMs = Date.now() - requestStartTime;
    console.log(`Terms request completed in ${requestDurationMs}ms, returning ${terms.length} terms`);

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ response: terms }),
    };
  }

  if (endpointType === 'person') {
    console.log('Routing to single person endpoint');
    return handlePersonRequest(event, requestStartTime);
  }

  // Continue with people endpoint logic
  // Parse query parameters
  const queryParams = event.queryStringParameters || {};
  const recordCount = parseInt(queryParams.recordCount || '200', 10);
  const offset = parseInt(queryParams.offset || '0', 10);
  const totalPopulation = parseInt(process.env.MOCK_TOTAL_POPULATION || '10000', 10);
  const errorRate = parseFloat(process.env.MOCK_ERROR_RATE || '0.0');
  const nonCurrentTermRate = parseNonCurrentTermRate(queryParams);

  console.log(`Generating mock data: offset=${offset}, recordCount=${recordCount}, totalPopulation=${totalPopulation}, nonCurrentTermRate=${nonCurrentTermRate}`);
  console.log('Note: offset is accepted for API compatibility but ignored for stateful depletion allocation');

  // Simulate errors based on configured error rate
  if (errorRate > 0 && Math.random() < errorRate) {
    console.error('Simulating API error based on MOCK_ERROR_RATE');
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Simulated API error' }),
    };
  }

  // Depletion model: every request claims the next contiguous range from a shared/global supply.
  // This intentionally ignores offset for allocation and keeps responses non-deterministic by offset.
  let startIndex = 0;
  let endIndex = 0;
  let actualCount = 0;
  const statefulAllocation = await allocateStatefulRange(recordCount, totalPopulation);
  startIndex = statefulAllocation.startIndex;
  endIndex = statefulAllocation.endIndex;
  actualCount = statefulAllocation.actualCount;

  console.log(`Returning persons ${startIndex} to ${endIndex - 1} (${actualCount} records), mode=stateful`);

  // Generate mock persons for this range
  const persons: MockPerson[] = [];
  for (let i = startIndex; i < endIndex; i++) {
    persons.push(generateMockPerson(i, { nonCurrentTermRate }));
  }

  // Match real API response structure: { response: [persons] }
  const response = {
    response: persons,
  };

  // Simulate API delay if configured (mimics slow real API behavior)
  const simulatedDelaySeconds = parseFloat(process.env.MOCK_SIMULATED_DELAY_SECONDS || '0');
  if (simulatedDelaySeconds > 0) {
    const elapsedSeconds = (Date.now() - requestStartTime) / 1000;
    const remainingDelay = simulatedDelaySeconds - elapsedSeconds;
    
    if (remainingDelay > 0) {
      console.log(`Simulating API delay: waiting ${remainingDelay.toFixed(2)} more seconds before responding`);
      await new Promise(resolve => setTimeout(resolve, remainingDelay * 1000));
    }
  }

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(response),
  };
}


export class SourceSimulatorFunctionURL {
  private functionUrl: string;
  private lookupPerformed: boolean = false;
  private client: LambdaClient;

  constructor(private readonly params: { landscape: string; region: string }) {
    this.client = new LambdaClient({ region: params.region });
  }

  public async getUrl(): Promise<string> {
    const { 
      client, functionUrl, lookupPerformed, params: { landscape, region } = {} 
    } = this;
    if(functionUrl) {
      return functionUrl;
    }
    if(!lookupPerformed) {
      this.lookupPerformed = true;
      const command = new GetFunctionUrlConfigCommand({
        FunctionName: `${FUNCTION_BASE_NAME}-${landscape}`
      });

      try {
        const response = await client.send(command);
        console.log("Function URL:", response.FunctionUrl);
        this.functionUrl = response.FunctionUrl!;
        return response.FunctionUrl!;
      } catch (error: any) {
        if (error.name !== "ResourceNotFoundException") {
          throw error;
        }
      }
    }
    return this.functionUrl;
  }

  public async exists(): Promise<boolean> {
    return !!(await this.getUrl());
  }

  public getCurrentEnvironmentVariables = async (): Promise<{ [key: string]: string }> => {
    const { landscape, region } = this.params;
    const lambda = new LambdaFunctionEnvironmentVariable({
      lambdaFunctionName: `${FUNCTION_BASE_NAME}-${landscape}`,
      region: region
    });
    return await lambda.getEnvironmentVariables();
  }

  public getEnvironmentVariable = async (name: string): Promise<string | undefined> => {
    const variables = await this.getCurrentEnvironmentVariables();
    return variables[name];
  }
}


// =====================================================================
// Test Harness
// =====================================================================

enum Task {
  LOCAL = 'local',
  REMOTE = 'remote'
}

if (require.main === module) {
  const { TestEnvironment } = require('integration-core');

  const testEnvironment = TestEnvironment('SOURCE_SIMULATOR');

  // Required environment variables
  [
    'TASK',
    'MOCK_TOTAL_POPULATION',
    'SECRET_ARN',
    'RECORD_COUNT',
    'OFFSET',
    'PEOPLE_API_KEY',
    'FUNCTION_URL',
    'OUTPUT_FILE_PATH'
  ].forEach(testEnvironment.getVar);

  // Optional: set a BUID (e.g. U0000042) to call the single person endpoint instead of the people endpoint
  testEnvironment.getVarOrEmptyString('BUID');

  const { LOCAL, REMOTE } = Task;
  const { 
    TASK: task = LOCAL, 
    FUNCTION_URL, 
    PEOPLE_API_KEY: apiKey = 'test-api-key',
    RECORD_COUNT: recordCount = '10',
    OFFSET: offset = '0',
    OUTPUT_FILE_PATH: outputFilePath = './scrap/source-simulator-output.json',
    BUID: buid
  } = process.env;

  const path = buid ? PERSON_PATH : '/';
  const queryStringParameters: Record<string, string> = buid ? { buid } : { recordCount, offset };
  const rawQueryString = new URLSearchParams(queryStringParameters).toString();

  (async () => {
    try {
      switch (task as Task) {
        case LOCAL: {
          console.log('=== Running LOCAL task: Calling handler directly ===');
          
          // Create mock event with query parameters
          const mockEvent: APIGatewayProxyEventV2 = {
            version: '2.0',
            routeKey: '$default',
            rawPath: path,
            rawQueryString,
            headers: {
              'x-api-key': apiKey,
            },
            queryStringParameters,
            requestContext: {
              accountId: '123456789012',
              apiId: 'local-test',
              domainName: 'local-test.lambda-url.us-east-2.on.aws',
              domainPrefix: 'local-test',
              http: {
                method: 'GET',
                path,
                protocol: 'HTTP/1.1',
                sourceIp: '127.0.0.1',
                userAgent: 'test-harness',
              },
              requestId: 'local-test-request-id',
              routeKey: '$default',
              stage: '$default',
              time: new Date().toISOString(),
              timeEpoch: Date.now(),
            },
            isBase64Encoded: false,
          };

          const mockContext: Context = {
            callbackWaitsForEmptyEventLoop: false,
            functionName: 'source-simulator-local',
            functionVersion: '$LATEST',
            invokedFunctionArn: 'arn:aws:lambda:us-east-2:123456789012:function:source-simulator-local',
            memoryLimitInMB: '1024',
            awsRequestId: 'local-test-request-id',
            logGroupName: '/aws/lambda/source-simulator-local',
            logStreamName: '2026/06/27/[$LATEST]local-test-stream',
            getRemainingTimeInMillis: () => 30000,
            done: () => {},
            fail: () => {},
            succeed: () => {},
          };

          console.log(`Path: ${path}, query parameters: ${rawQueryString}`);
          console.log('Calling handler...\n');

          const result = await handler(mockEvent, mockContext);

          // Type guard to handle APIGatewayProxyResultV2 union type
          if (typeof result === 'string') {
            console.log('\n=== Response (string) ===');
            console.log(result);
          } else {
            console.log('\n=== Response ===');
            console.log(`Status Code: ${result.statusCode}`);
            console.log(`Headers: ${JSON.stringify(result.headers)}`);
            
            if (typeof result.body === 'string') {
              const body = JSON.parse(result.body);
              console.log(`\nBody:`);
              console.log(JSON.stringify(body, null, 2));
              
              if (Array.isArray(body?.response)) {
                console.log(`\nReturned ${body.response.length} persons`);
                console.log(`First person: ${JSON.stringify(body.response[0], null, 2)}`);
              }
            }
          }

          break;
        }

        case REMOTE: {
          console.log('=== Running REMOTE task: Calling deployed Lambda Function URL ===');
          
          if (!FUNCTION_URL) {
            console.error('FUNCTION_URL environment variable is required for REMOTE task');
            process.exit(1);
          }

          if (!apiKey) {
            console.error('PEOPLE_API_KEY environment variable is required for REMOTE task');
            process.exit(1);
          }

          // Extract base URL from Function URL (remove trailing path if any)
          const functionUrlObj = new URL(FUNCTION_URL);
          const baseUrl = `${functionUrlObj.protocol}//${functionUrlObj.host}`;

          console.log(`Function URL: ${FUNCTION_URL}`);
          console.log(`Base URL: ${baseUrl}`);
          console.log(`Path: ${path}, query parameters: ${rawQueryString}`);
          console.log('Making API call...\n');

          const { BuApiClientForApiKey } = require('integration-huron-person');

          const apiClient = new BuApiClientForApiKey({
            baseUrl,
            apiKey,
            timeout: 60000,
          } satisfies EndpointConfigForApiKey);

          const response = await apiClient.get({
            url: path,
            params: queryStringParameters,
          });

          console.log('\n=== Response ===');
          console.log(`Status Code: ${response.status}`);
          console.log(`Headers: ${JSON.stringify(response.headers)}`);
          
          const body = response.data;
          console.log(`\nBody:`);
          console.log(JSON.stringify(body, null, 2));
          
          if (Array.isArray(body?.response)) {
            console.log(`\nReturned ${body.response.length} persons`);
            console.log(`First person: ${JSON.stringify(body.response[0], null, 2)}`);
          }

          break;
        }

        default:
          console.error(`Unknown task: ${task}`);
          console.error(`Valid tasks: ${Object.values(Task).join(', ')}`);
          process.exit(1);
      }
    } catch (error) {
      console.error('\n=== Error ===');
      console.error(error);
      process.exit(1);
    }
  })();
}
