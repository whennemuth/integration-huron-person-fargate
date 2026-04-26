/**
 * Target API Error Status Tracking and Statistics
 * 
 * This module provides error tracking and statistical analysis for API responses from the Huron target API.
 * It captures error events, detects throttling (429 errors), and persists data to DynamoDB for monitoring.
 * 
 * Key Classes:
 * - AbstractErrorByStatus: Base class for status-specific error detection and message extraction
 * - ThrottleEvent: Detects and analyzes 429 throttling errors
 * - TrackingTargetApiErrorProcessor: Full error processor that implements TargetApiErrorEventProcessor
 *   - Captures ALL target API errors (not just throttling)
 *   - Writes error events to DynamoDB asynchronously
 *   - Tracks in-memory statistics for end-of-run reporting
 *   - Provides statistics summary and throttling detection
 * 
 * Usage:
 * ```typescript
 * // Create tracker with DynamoDB configuration
 * const tracker = new TrackingTargetApiErrorProcessor({
 *   tableName: 'processor-statistics',
 *   integrationTimestamp: '2026-04-15T19:30:00.000Z',
 *   region: 'us-east-1',
 *   logToConsole: true
 * });
 * 
 * // Inject into HuronPersonIntegration
 * const integration = new HuronPersonIntegration({ 
 *   config, 
 *   errorEventProcessor: tracker 
 * });
 * 
 * // Run integration - errors automatically tracked
 * await integration.run();
 * 
 * // Write final statistics to DynamoDB
 * await tracker.writeStatistics({
 *   startTimestamp: '2026-04-15T19:30:00.000Z',
 *   endTimestamp: '2026-04-15T19:35:00.000Z',
 *   chunkCount: 1,
 *   chunkSize: 1000,
 *   totalRecords: 1000,
 *   sourceDescription: 'chunk-0001'
 * });
 * 
 * // Check for throttling
 * if (tracker.hasBeenThrottled()) {
 *   console.warn('Throttling occurred!');
 * }
 * ```
 * 
 * DynamoDB Schema:
 * - PK: integrationTimestamp (ISO string)
 * - SK: eventType ("STATISTICS" or "ERROR:<statusCode>:<timestamp>")
 * - GSI: errorType-timestamp-index for cross-run queries
 */

import { ConfigManager, ReadPerson, getLocalConfig, TargetApiErrorEventProcessor } from "integration-huron-person";
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

// Define ErrorEventDetails locally since it's not exported from integration-huron-person
export interface ErrorEventDetails {
  message?: string;
  object: any;
}

export interface SyncStatistics {
  startTimestamp: string;
  endTimestamp: string;
  chunkCount: number;
  chunkSize: number;
  totalRecords: number;
  sourceDescription: string;
}

export abstract class AbstractErrorByStatus {
  protected status: any;
  protected statusText: string | undefined;
  protected data: any;

  constructor(protected error: any, protected statusCode: number) {
    const { response: { status, statusText, data } = {} }  = this.error || {};
    this.status = status;
    this.statusText = statusText;
    this.data = data;
  }

  protected matchesStatus = (status:any): boolean => {
    return status && (status === this.statusCode || status === String(this.statusCode));
  }

  public isStatusMatch = (): boolean => {
    const { status, data: { errors=[] } = {}, matchesStatus } = this;
    if(matchesStatus(status)) {
      return true;
    }
    if(errors && Array.isArray(errors)) {
      return errors.some((err: any) => matchesStatus(err.status));
    }
    return false;
  }

  public getMessage = (): string | undefined => {
    if( ! this.isStatusMatch() ) {
      return undefined;
    }
    const { status, data: { errors=[] } = {} } = this;
    if(errors && Array.isArray(errors)) {
      let msg = errors.map((err: any) => err?.internalErrorMessage).join(', ');
      if( ! msg) {
        msg = errors.map((err: any) => (err?.detail || []).join(', ')).join(', ');
      }
      return msg || undefined;
    }
    return undefined;
  }

  public getIncidentId = (): string | undefined => {
    if( ! this.isStatusMatch() ) {
      return undefined;
    }
    const { data: { errors=[] } = {}, matchesStatus } = this;
    if(errors && Array.isArray(errors)) {
      return errors.find(err => matchesStatus(err.status))?.incidentId;
    }
    return undefined;
  }
}

export class AnonymousEvent extends AbstractErrorByStatus {
  constructor(error: any) {
    super(error, -1); // Use -1 to indicate this is not tied to a specific status code
  }
}

export class ThrottleEvent extends AbstractErrorByStatus {
  constructor(error: any) {
    super(error, 429);
  }

  private is429 = (): boolean => {
    return this.isStatusMatch();
  }

  public isThrottled = (): boolean => {
    return this.is429();
  }
}

/**
 * Basic Target API Error Event Processor that simply logs error details to the console.
 * This can be used as a default or fallback processor when no DynamoDB tracking is configured.
 * It implements the TargetApiErrorEventProcessor interface expected by ApiClientForJWT.
 * 
 * Note: This processor does not track statistics or persist data, it only logs to console.
 * For production use, consider using TrackingTargetApiErrorProcessor for full monitoring capabilities.
 */
export class LoggingTargetApiErrorProcessor implements TargetApiErrorEventProcessor {
  public async process(error: any, details?: ErrorEventDetails): Promise<void> {
    const throttleEvent = new ThrottleEvent(error);
    if (throttleEvent.isThrottled()) {
      console.warn('HURON IS THROTTLING US!!!');
    }
    const anonymousEvent = new AnonymousEvent(error);
    const { object: { hrn, sourceIdentifier} = {}, message} = details || {};
    console.error(`API Error Event: ${JSON.stringify({
      message: anonymousEvent.getMessage() || error?.message || 'Unknown error',
      incidentId: anonymousEvent.getIncidentId(),
      details: { hrn, sourceIdentifier, message },
    })}`);
  }
}

/**
 * Target API Error Event Processor that tracks all API errors and throttling events, and writes 
 * details to DynamoDB for monitoring.
 */
export class TrackingTargetApiErrorProcessor implements TargetApiErrorEventProcessor {
  private readonly tableName: string;
  private readonly integrationTimestamp: string;
  private readonly dynamoClient: DynamoDBClient;
  private readonly logToConsole: boolean;
  
  // In-memory tracking for statistics
  private throttleEvents: Array<{ timestamp: string, event: ThrottleEvent }> = [];
  private errorCounts: Map<number, number> = new Map(); // status code -> count
  private totalErrors: number = 0;

  constructor(options: {
    tableName: string;
    integrationTimestamp: string;
    region?: string;
    logToConsole?: boolean;
  }) {
    this.tableName = options.tableName;
    this.integrationTimestamp = options.integrationTimestamp;
    this.logToConsole = options.logToConsole ?? true;
    
    // Initialize DynamoDB client
    this.dynamoClient = new DynamoDBClient({ 
      region: options.region || process.env.AWS_REGION || 'us-east-1'
    });
  }

  /**
   * Process an error event (implements TargetApiErrorEventProcessor interface)
   * This is called by ApiClientForJWT whenever an axios error occurs
   * 
   * @param error - The axios error object
   * @param details - Optional context about the error (message, object being processed)
   */
  public async process(error: any, details?: ErrorEventDetails): Promise<void> {
    // Extract error information
    const statusCode = error?.response?.status || 0;
    const statusText = error?.response?.statusText;
    const timestamp = new Date().toISOString();
    
    // Check if this is a throttling event
    const throttleEvent = new ThrottleEvent(error);
    const isThrottled = throttleEvent.isThrottled();
    
    // Track throttle events separately
    if (isThrottled) {
      this.throttleEvents.push({ timestamp, event: throttleEvent });
    }
    
    // Extract message and incident ID using existing AbstractErrorByStatus methods
    const message = details?.message || throttleEvent.getMessage() || error?.message || 'Unknown error';
    const incidentId = throttleEvent.getIncidentId();
    
    // Update in-memory statistics
    this.totalErrors++;
    this.errorCounts.set(statusCode, (this.errorCounts.get(statusCode) || 0) + 1);
    
    // Log to console if enabled
    if (this.logToConsole) {
      await (new LoggingTargetApiErrorProcessor()).process(error, details); // For logging purposes, we can create an anonymous event to extract message and incident ID
    }
    
    // Write to DynamoDB asynchronously (don't await to avoid blocking API calls)
    const { object: { hrn, sourceIdentifier } = {}, message: detailsMessage } = details || {};
    this.writeErrorEvent({
      timestamp,
      statusCode,
      statusText,
      message,
      incidentId,
      isThrottled,
      detailsObj: { hrn, sourceIdentifier, message: detailsMessage }
    }).catch(err => {
      console.error('[TrackingTargetApiErrorProcessor] Failed to write error event to DynamoDB:', err);
    });
  }

  /**
   * Write an error event to DynamoDB
   */
  private async writeErrorEvent(params: {
    timestamp: string;
    statusCode: number;
    statusText?: string;
    message: string;
    incidentId?: string;
    isThrottled: boolean;
    detailsObj?: any;
  }): Promise<void> {
    const { timestamp, statusCode, statusText, message, incidentId, isThrottled, detailsObj } = params;
    
    // Sort key format: "ERROR:<statusCode>:<timestamp>"
    // This ensures chronological ordering within the integration run
    const eventType = `ERROR:${statusCode}:${timestamp}`;
    
    // Error type for GSI (enables querying all 429s across all runs)
    const errorType = `ERROR:${statusCode}`;
    
    const item = {
      integrationTimestamp: this.integrationTimestamp, // PK
      eventType, // SK
      errorType, // GSI PK for cross-run queries
      timestamp,
      statusCode,
      ...(statusText && { statusText }),
      message,
      ...(incidentId && { incidentId }),
      isThrottled,
      ...(detailsObj && { detailsObj: JSON.stringify(detailsObj) }),
    };

    const command = new PutItemCommand({
      TableName: this.tableName,
      Item: marshall(item),
    });

    await this.dynamoClient.send(command);
  }

  /**
   * Write statistics record to DynamoDB at the end of processing
   * Should be called once per integration run in the finally block
   */
  public async writeStatistics(stats: SyncStatistics): Promise<void> {
    const item = {
      integrationTimestamp: this.integrationTimestamp, // PK
      eventType: 'STATISTICS', // SK
      errorType: 'STATISTICS', // GSI PK for querying stats across runs
      ...stats,
      // Include error summary
      totalErrors: this.totalErrors,
      throttleCount: this.getThrottlingCount(),
      errorsByStatus: Object.fromEntries(this.errorCounts),
    };

    const command = new PutItemCommand({
      TableName: this.tableName,
      Item: marshall(item),
    });

    try {
      await this.dynamoClient.send(command);
      if (this.logToConsole) {
        console.log('[TrackingTargetApiErrorProcessor] Statistics written to DynamoDB');
      }
    } catch (err) {
      console.error('[TrackingTargetApiErrorProcessor] Failed to write statistics to DynamoDB:', err);
      throw err;
    }
  }

  /**
   * Get in-memory statistics (useful for logging before writing to DynamoDB)
   */
  public getStatisticsSummary(): {
    totalErrors: number;
    throttleCount: number;
    errorsByStatus: Record<number, number>;
  } {
    return {
      totalErrors: this.totalErrors,
      throttleCount: this.getThrottlingCount(),
      errorsByStatus: Object.fromEntries(this.errorCounts),
    };
  }

  /**
   * Get count of throttling events
   */
  public getThrottlingCount = (): number => {
    return this.throttleEvents.length;
  }

  /**
   * Get the number of errors for a specific status code
   */
  public getErrorCount(statusCode: number): number {
    return this.errorCounts.get(statusCode) || 0;
  }
}

export const alertIfThrottlingEvent = (error: any): void => {
  const event = new ThrottleEvent(error);
  if(event.isThrottled()) {
    console.warn('THROTTLING ALERT: Huron throttled us!');
  }
}

/**
 * Simple example usage:
 */
if (require.main === module) {
  (async () => {
    // Load env vars
    const { HURON_PERSON_CONFIG_PATH, SECRET_ARN } = process.env;

    // Load configuration.
    const configManager = ConfigManager.getInstance();
    const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
    const config = await configManager
      .reset()
      .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
      .fromSecretManager(SECRET_ARN)                // ← Fallback to Secrets Manager
      .fromEnvironment()                            // ← Fallback to individual env var overrides
      .fromFileSystem(localConfigPath)              // ← Local dev only
      .getConfigAsync('person');

    const reader = new ReadPerson(config);
    
    try {
      await reader.readPersonByHRN('invalid_hrn');
    }
    catch(error) {
      // Instantiate a class that subclasses for a 400 status error object.
      const status400Event = new class extends AbstractErrorByStatus {
        constructor(error: any, statusCode: number) {
          super(error, 400);
        }
      }(error, 400);

      // Log out what the object "knows" about the error and whether it matches the 400 status.
      console.log(`Status 400 message: ${JSON.stringify({
        is400: status400Event.isStatusMatch(),
        message: status400Event.getMessage(),
        incidentId: status400Event.getIncidentId(),
      }, null, 2)}`);

      // NOTE: Would like to test the ThrottleEvent class, but causing a 429 error is not straightforward. 
    }
    
  })();
}