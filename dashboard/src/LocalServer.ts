import express from 'express';
import { join } from 'path';
import { IContext } from "../context/IContext";
import { buildSecretValue } from '../lib/Secrets';
import { CACHE_JSON } from './handlers/DashboardCache';
import { handler } from './handlers/DashboardHandler';
import { isTruthy } from './Utils';

export const OVERRIDE_CACHE_WITH_LOCAL_CONTEXT = 'OVERRIDE_CACHE_WITH_LOCAL_CONTEXT';

// Local type definition for Lambda Function URL event (based on AWS docs)
interface LambdaFunctionUrlEvent {
  version: string;
  routeKey: string;
  rawPath: string;
  rawQueryString: string;
  headers: { [name: string]: string };
  queryStringParameters?: { [name: string]: string };
  requestContext: {
    accountId: string;
    apiId: string;
    domainName: string;
    domainPrefix: string;
    http: {
      method: string;
      path: string;
      protocol: string;
      sourceIp: string;
      userAgent: string;
    };
    requestId: string;
    routeKey: string;
    stage: string;
    time: string;
    timeEpoch: number;
  };
  body?: string;
  pathParameters?: { [name: string]: string };
  isBase64Encoded: boolean;
}

const app = express();
const PORT = process.env.PORT || 3000;

if (isTruthy(process.env[OVERRIDE_CACHE_WITH_LOCAL_CONTEXT])) {
  /**
   * Get the secret values that would otherwise have been persisted in a file or secrets 
   * manager directly out of the context file and put them in an environment variable for the 
   * local server to use as a cache bypass if the environment variable is set to a truthy value. This allows developers to work with local context values without needing to persist them in a cache file or secrets manager, which can be especially useful for testing and development purposes.
   */
  console.log('Overriding cache with local context values as per environment variable setting');
  const context = require('../context/context.json') as IContext;
  process.env[CACHE_JSON] = buildSecretValue(context);
}

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(join(__dirname, '../public')));

// CORS middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token');
  res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  if (req.method === 'OPTIONS') {
    res.sendStatus(200);
  } else {
    next();
  }
});

// Transform Express request to Lambda Function URL event format
function transformRequest(req: express.Request): LambdaFunctionUrlEvent {
  const queryString = new URLSearchParams(req.query as Record<string, string>).toString();
  
  return {
    version: '2.0',
    routeKey: '$default',
    rawPath: req.path,
    rawQueryString: queryString,
    headers: req.headers as { [name: string]: string },
    queryStringParameters: Object.keys(req.query).length > 0 ? req.query as { [name: string]: string } : undefined,
    requestContext: {
      accountId: 'local-account',
      apiId: 'local-api-id',
      domainName: 'localhost',
      domainPrefix: 'local',
      http: {
        method: req.method,
        path: req.path,
        protocol: 'HTTP/1.1',
        sourceIp: req.ip || '127.0.0.1',
        userAgent: req.get('User-Agent') || ''
      },
      requestId: Math.random().toString(36).substring(7),
      routeKey: '$default',
      stage: 'local',
      time: new Date().toISOString(),
      timeEpoch: Date.now()
    },
    body: req.method === 'GET' ? undefined : JSON.stringify(req.body),
    pathParameters: undefined,
    isBase64Encoded: false
  };
}

// Main request handler - transforms Express request to Lambda event and back
app.all('*', async (req, res) => {
  try {
    console.log(`${req.method} ${req.path}`, req.query);
    
    // Transform Express request to Lambda event
    const lambdaEvent = transformRequest(req);
    
    // Call the Lambda handler
    const result = await handler(lambdaEvent);
    
    // Transform Lambda response back to Express response
    if (result.headers) {
      Object.entries(result.headers).forEach(([key, value]) => {
        if (value) {
          res.set(key, value as string);
        }
      });
    }
    
    res.status(result.statusCode);
    
    // Handle different content types
    const contentType = result.headers?.['Content-Type'] || result.headers?.['content-type'];
    const contentTypeStr = typeof contentType === 'string' ? contentType : '';
    if (contentTypeStr.includes('application/json')) {
      try {
        const jsonBody = JSON.parse(result.body || '{}');
        res.json(jsonBody);
      } catch {
        res.send(result.body);
      }
    } else {
      res.send(result.body);
    }
  } catch (error) {
    console.error('Local server error:', error);
    res.status(500).json({ 
      error: 'Internal Server Error',
      message: error instanceof Error ? error.message : 'Unknown error',
      stack: process.env.NODE_ENV === 'development' ? (error as Error).stack : undefined
    });
  }
});

// Error handling middleware
app.use((err: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error('Express error:', err);
  res.status(500).json({
    error: 'Server Error',
    message: err.message,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
  });
});

// Start the server
app.listen(PORT, () => {
  console.log(`🚀 Integration Dashboard running locally at http://localhost:${PORT}`);
  console.log(`📊 Dashboard: http://localhost:${PORT}/dashboard`);
  console.log(`🔍 API Status: http://localhost:${PORT}/api/status`);
  console.log(`\n💡 Development Mode: Express wrapper for Lambda function`);
});

export default app;