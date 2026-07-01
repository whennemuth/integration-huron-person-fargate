/**
 * Source Simulator Lambda - Mock API for Person Data
 * 
 * This Lambda simulates the source person API for testing purposes, eliminating the
 * 30-minute cooldown constraint of the real API.
 * 
 * **Design:**
 * - Stateful depletion model: Every request claims the next contiguous slice of available records
 * - Non-deterministic by request params: `offset` is accepted for compatibility but does not drive allocation
 * - Validates API key from Secrets Manager against incoming requests
 * - Returns minimal field set (only fields DataMapper uses)
 * - Matches real API response structure: `{ response: [persons] }`
 * - Simulates slow API: Optional delay before responding via MOCK_SIMULATED_DELAY_SECONDS
 * 
 * **Query Parameters:**
 * - `recordCount`: Number of records to return (default: 200)
 * - `offset`: Batch offset for pagination (default: 0)
 * 
 * **Headers:**
 * - `Authorization`: Bearer token containing API key
 *   OR
 * - `X-API-Key`: Direct API key header
 * 
 * **Environment Variables:**
 * - MOCK_TOTAL_POPULATION: Total persons available (e.g., 10000)
 * - MOCK_ERROR_RATE: Probability of simulated errors (0.0-1.0)
 * - MOCK_SIMULATED_DELAY_SECONDS: Delay in seconds before responding (simulates slow API)
 * - SECRET_ARN: ARN of Secrets Manager secret containing the API key
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
import { AbstractAtomicCounter } from '../../AtomicCounter';

export const FUNCTION_BASE_NAME = 'source-simulator';
export const SIMULATOR_COUNTER_NAME = 'simulator-offset-counter';
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
let cachedApiKey: string | null = null;
let cachedCounter: AbstractAtomicCounter | null = null;
let inMemoryNextIndex = 0;

export function resetSourceSimulatorState(): void {
  cachedApiKey = null;
  cachedCounter = null;
  inMemoryNextIndex = 0;
}

interface MockPerson {
  personid: string;
  bu_id: string;
  firstName?: string;
  lastName?: string;
  email?: string;
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
 * @returns Mock person object with minimal fields DataMapper uses
 */
function generateMockPerson(index: number): MockPerson {
  const personid = `U${String(index + 1).padStart(7, '0')}`;
  const bu_id = String(index + 1).padStart(8, '0');

  // Vary person type based on index modulo to create mix of employees/students/affiliates
  const personType = index % 3;

  const basePerson: MockPerson = {
    personid,
    bu_id,
    firstName: `FirstName${index + 1}`,
    lastName: `LastName${index + 1}`,
    email: `${personid}@bu.edu`,
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
    basePerson.studentInfo = {
      studentSemester: [
        {
          studentSemesterInfo: {
            academicTerm: {
              term: {
                code: '2261',
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
 * Retrieve API key from Secrets Manager using ConfigManager.
 * Uses caching to avoid repeated calls to Secrets Manager.
 * 
 * @returns API key from Secrets Manager, or null if SECRET_ARN not configured
 */
async function getApiKey(): Promise<string | null> {
  // Return cached value if available
  if (cachedApiKey !== null) {
    return cachedApiKey;
  }

  const secretArn = process.env.SECRET_ARN;
  if (!secretArn) {
    console.warn('SECRET_ARN not configured, skipping API key validation');
    return null;
  }

  try {
    // Use ConfigManager to retrieve config from Secrets Manager
    // This follows the same pattern as docker/processor.ts
    const configManager = ConfigManager.getInstance();
    const config = await configManager
      .reset()
      .fromSecretManager(secretArn)
      .getConfigAsync('people');
    
    // Extract API key from config (cast to DataSourceConfig since S3DataSourceConfig doesn't have endpointConfig)
    const peopleConfig = config?.dataSource?.people as DataSourceConfig;
    const apiKey = peopleConfig?.endpointConfig?.apiKey;
    
    if (!apiKey) {
      console.error('API key not found in secret at dataSource.people.endpointConfig.apiKey');
      return null;
    }

    // Cache the API key for subsequent invocations
    cachedApiKey = apiKey;
    console.log('Successfully retrieved and cached API key from Secrets Manager');
    return apiKey;
  } catch (error) {
    console.error('Error retrieving API key from Secrets Manager via ConfigManager:', error);
    return null;
  }
}

function getSimulatorCounter(): AbstractAtomicCounter | null {
  if (cachedCounter) {
    return cachedCounter;
  }

  const stackId = process.env.STACK_ID;
  const region = process.env.REGION;
  const landscape = process.env.LANDSCAPE;

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
 * @param expectedApiKey - Expected API key from Secrets Manager
 * @returns true if API key is valid, false otherwise
 */
function validateApiKey(event: APIGatewayProxyEventV2, expectedApiKey: string | null): boolean {
  if (!expectedApiKey) {
    // If no API key configured, allow all requests
    return true;
  }

  const headers = event.headers || {};
  
  // Check Authorization header (Bearer token)
  const authHeader = headers['authorization'] || headers['Authorization'];
  if (authHeader) {
    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    if (match && match[1] === expectedApiKey) {
      return true;
    }
  }

  // Check X-API-Key header
  const apiKeyHeader = headers['x-api-key'] || headers['X-API-Key'];
  if (apiKeyHeader === expectedApiKey) {
    return true;
  }

  return false;
}

/**
 * Main Lambda handler for Function URL requests.
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

  // Retrieve API key from Secrets Manager
  const expectedApiKey = await getApiKey();

  // Validate API key (for non-health endpoints)
  if (validateApiKey(event, expectedApiKey)) {
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
  // Parse query parameters
  const queryParams = event.queryStringParameters || {};
  const recordCount = parseInt(queryParams.recordCount || '200', 10);
  const offset = parseInt(queryParams.offset || '0', 10);
  const totalPopulation = parseInt(process.env.MOCK_TOTAL_POPULATION || '10000', 10);
  const errorRate = parseFloat(process.env.MOCK_ERROR_RATE || '0.0');

  console.log(`Generating mock data: offset=${offset}, recordCount=${recordCount}, totalPopulation=${totalPopulation}`);
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
    persons.push(generateMockPerson(i));
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

  const { LOCAL, REMOTE } = Task;
  const { 
    TASK: task = LOCAL, 
    FUNCTION_URL, 
    PEOPLE_API_KEY: apiKey = 'test-api-key',
    RECORD_COUNT: recordCount = '10',
    OFFSET: offset = '0',
    OUTPUT_FILE_PATH: outputFilePath = './scrap/source-simulator-output.json'
  } = process.env;

  (async () => {
    try {
      switch (task as Task) {
        case LOCAL: {
          console.log('=== Running LOCAL task: Calling handler directly ===');
          
          // Create mock event with query parameters
          const mockEvent: APIGatewayProxyEventV2 = {
            version: '2.0',
            routeKey: '$default',
            rawPath: '/',
            rawQueryString: `recordCount=${recordCount}&offset=${offset}`,
            headers: {
              'x-api-key': apiKey,
            },
            queryStringParameters: {
              recordCount,
              offset,
            },
            requestContext: {
              accountId: '123456789012',
              apiId: 'local-test',
              domainName: 'local-test.lambda-url.us-east-2.on.aws',
              domainPrefix: 'local-test',
              http: {
                method: 'GET',
                path: '/',
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

          console.log(`Query parameters: recordCount=${recordCount}, offset=${offset}`);
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
          console.log(`Query parameters: recordCount=${recordCount}, offset=${offset}`);
          console.log('Making API call...\n');

          const { BuApiClientForApiKey } = require('integration-huron-person');

          const apiClient = new BuApiClientForApiKey({
            baseUrl,
            apiKey,
            timeout: 60000,
          } satisfies EndpointConfigForApiKey);

          const response = await apiClient.get({
            url: '/',
            params: {
              recordCount,
              offset,
            },
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
