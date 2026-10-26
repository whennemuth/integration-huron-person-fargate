// Lambda Function URL Event Types (based on AWS documentation)
export type LambdaFunctionUrlEvent = {
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

export type LambdaFunctionUrlResult = {
  statusCode: number;
  headers?: { [header: string]: boolean | number | string };
  body?: string;
  isBase64Encoded?: boolean;
}
