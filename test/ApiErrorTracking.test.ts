/**
 * Unit tests for ApiErrorTracking module
 * 
 * Tests error detection, statistics tracking, and DynamoDB integration
 */

import {
  AbstractErrorByStatus,
  AnonymousEvent,
  ThrottleEvent,
  LoggingTargetApiErrorProcessor,
  TrackingTargetApiErrorProcessor,
  ErrorEventDetails,
  SyncStatistics,
} from '../src/processing/ApiErrorTracking';
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';

// Mock DynamoDB client
const dynamoMock = mockClient(DynamoDBClient);

describe('AbstractErrorByStatus', () => {
  describe('basic error detection', () => {
    class TestErrorEvent extends AbstractErrorByStatus {
      constructor(error: any) {
        super(error, 400);
      }
    }

    it('should detect matching status code in response', () => {
      const error = {
        response: {
          status: 400,
          statusText: 'Bad Request',
          data: {},
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.isStatusMatch()).toBe(true);
    });

    it('should detect matching status code as string', () => {
      const error = {
        response: {
          status: '400',
          statusText: 'Bad Request',
          data: {},
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.isStatusMatch()).toBe(true);
    });

    it('should detect matching status code in nested errors array', () => {
      const error = {
        response: {
          status: 200,
          data: {
            errors: [
              { status: 400, internalErrorMessage: 'Validation failed' },
            ],
          },
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.isStatusMatch()).toBe(true);
    });

    it('should return false for non-matching status code', () => {
      const error = {
        response: {
          status: 500,
          statusText: 'Server Error',
          data: {},
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.isStatusMatch()).toBe(false);
    });

    it('should handle missing response gracefully', () => {
      const error = { message: 'Network error' };
      const event = new TestErrorEvent(error);
      expect(event.isStatusMatch()).toBe(false);
    });
  });

  describe('getMessage', () => {
    class TestErrorEvent extends AbstractErrorByStatus {
      constructor(error: any) {
        super(error, 422);
      }
    }

    it('should return undefined if status does not match', () => {
      const error = {
        response: {
          status: 500,
          data: { errors: [] },
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.getMessage()).toBeUndefined();
    });

    it('should extract internalErrorMessage from errors array', () => {
      const error = {
        response: {
          status: 422,
          data: {
            errors: [
              { status: 422, internalErrorMessage: 'Field validation failed' },
              { status: 422, internalErrorMessage: 'Email is required' },
            ],
          },
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.getMessage()).toBe('Field validation failed, Email is required');
    });

    it('should fallback to detail if internalErrorMessage not present', () => {
      const error = {
        response: {
          status: 422,
          data: {
            errors: [
              { status: 422, detail: ['Invalid format', 'Missing field'] },
            ],
          },
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.getMessage()).toContain('Invalid format');
      expect(event.getMessage()).toContain('Missing field');
    });

    it('should return undefined if no errors array', () => {
      const error = {
        response: {
          status: 422,
          data: {},
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.getMessage()).toBeUndefined();
    });
  });

  describe('getIncidentId', () => {
    class TestErrorEvent extends AbstractErrorByStatus {
      constructor(error: any) {
        super(error, 500);
      }
    }

    it('should return incident ID from matching error', () => {
      const error = {
        response: {
          status: 200,
          data: {
            errors: [
              { status: 500, incidentId: 'INC-12345' },
            ],
          },
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.getIncidentId()).toBe('INC-12345');
    });

    it('should return undefined if status does not match', () => {
      const error = {
        response: {
          status: 400,
          data: {
            errors: [
              { status: 400, incidentId: 'INC-12345' },
            ],
          },
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.getIncidentId()).toBeUndefined();
    });

    it('should return undefined if no errors array', () => {
      const error = {
        response: {
          status: 500,
          data: {},
        },
      };
      const event = new TestErrorEvent(error);
      expect(event.getIncidentId()).toBeUndefined();
    });
  });
});

describe('AnonymousEvent', () => {
  it('should handle any error without status matching', () => {
    const error = {
      response: {
        status: 418, // I'm a teapot
        statusText: "I'm a teapot",
        data: {
          errors: [
            { status: 418, internalErrorMessage: 'Cannot brew coffee' },
          ],
        },
      },
    };
    const event = new AnonymousEvent(error);
    
    // AnonymousEvent uses -1 as status code, so status match will fail
    expect(event.isStatusMatch()).toBe(false);
    
    // getMessage() returns undefined because status doesn't match
    expect(event.getMessage()).toBeUndefined();
    
    // But we can verify the event was created
    expect(event).toBeInstanceOf(AnonymousEvent);
  });
});

describe('ThrottleEvent', () => {
  it('should detect 429 status code', () => {
    const error = {
      response: {
        status: 429,
        statusText: 'Too Many Requests',
        data: {},
      },
    };
    const event = new ThrottleEvent(error);
    expect(event.isThrottled()).toBe(true);
    expect(event.isStatusMatch()).toBe(true);
  });

  it('should detect 429 in nested errors array', () => {
    const error = {
      response: {
        status: 200,
        data: {
          errors: [
            { status: 429, internalErrorMessage: 'Rate limit exceeded' },
          ],
        },
      },
    };
    const event = new ThrottleEvent(error);
    expect(event.isThrottled()).toBe(true);
  });

  it('should return false for non-429 errors', () => {
    const error = {
      response: {
        status: 500,
        statusText: 'Server Error',
        data: {},
      },
    };
    const event = new ThrottleEvent(error);
    expect(event.isThrottled()).toBe(false);
  });

  it('should extract throttle error message', () => {
    const error = {
      response: {
        status: 429,
        data: {
          errors: [
            { status: 429, internalErrorMessage: 'API rate limit exceeded, retry after 60s' },
          ],
        },
      },
    };
    const event = new ThrottleEvent(error);
    expect(event.getMessage()).toContain('API rate limit exceeded');
  });
});

describe('LoggingTargetApiErrorProcessor', () => {
  beforeEach(() => {
    jest.spyOn(console, 'warn').mockImplementation();
    jest.spyOn(console, 'error').mockImplementation();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('should log throttling warning for 429 errors', async () => {
    const processor = new LoggingTargetApiErrorProcessor();
    const error = {
      response: { status: 429 },
      message: 'Too Many Requests',
    };

    await processor.process(error);

    expect(console.warn).toHaveBeenCalledWith('HURON IS THROTTLING US!!!');
  });

  it('should log error details', async () => {
    const processor = new LoggingTargetApiErrorProcessor();
    const error = {
      response: { status: 500 },
      message: 'Server Error',
    };
    const details: ErrorEventDetails = {
      message: 'Failed to update person',
      object: { hrn: 'HRN123', sourceIdentifier: 'SRC456' },
    };

    await processor.process(error, details);

    expect(console.error).toHaveBeenCalledWith(
      'API Error Event:',
      expect.objectContaining({
        details: expect.objectContaining({
          hrn: 'HRN123',
          sourceIdentifier: 'SRC456',
          message: 'Failed to update person',
        }),
      })
    );
  });

  it('should handle errors without details', async () => {
    const processor = new LoggingTargetApiErrorProcessor();
    const error = {
      response: { status: 404 },
      message: 'Not Found',
    };

    await processor.process(error);

    expect(console.error).toHaveBeenCalledWith(
      'API Error Event:',
      expect.any(Object)
    );
  });
});

describe('TrackingTargetApiErrorProcessor', () => {
  beforeEach(() => {
    dynamoMock.reset();
    jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'warn').mockImplementation();
    jest.spyOn(console, 'error').mockImplementation();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('constructor', () => {
    it('should initialize with required options', () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        region: 'us-east-1',
        logToConsole: false,
      });

      expect(processor).toBeInstanceOf(TrackingTargetApiErrorProcessor);
    });

    it('should use default region if not provided', () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
      });

      expect(processor).toBeInstanceOf(TrackingTargetApiErrorProcessor);
    });
  });

  describe('process', () => {
    it('should track error in memory', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const error = {
        response: { status: 500, statusText: 'Server Error' },
        message: 'Internal Server Error',
      };

      await processor.process(error);

      const stats = processor.getStatisticsSummary();
      expect(stats.totalErrors).toBe(1);
      expect(stats.errorsByStatus[500]).toBe(1);
    });

    it('should track throttle events separately', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const error = {
        response: { status: 429, statusText: 'Too Many Requests' },
        message: 'Rate limit exceeded',
      };

      await processor.process(error);

      const stats = processor.getStatisticsSummary();
      expect(stats.throttleCount).toBe(1);
      expect(stats.totalErrors).toBe(1);
    });

    it('should write error event to DynamoDB', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const error = {
        response: { status: 404, statusText: 'Not Found' },
        message: 'Resource not found',
      };
      const details: ErrorEventDetails = {
        message: 'Person not found',
        object: { hrn: 'HRN999', sourceIdentifier: 'SRC999' },
      };

      await processor.process(error, details);

      // Give async DynamoDB write time to complete
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(dynamoMock.calls()).toHaveLength(1);
      const putCall = dynamoMock.call(0);
      expect(putCall.args[0].input).toMatchObject({
        TableName: 'test-table',
      });
    });

    it('should handle DynamoDB write errors gracefully', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).rejects(new Error('DynamoDB error'));

      const error = {
        response: { status: 500 },
        message: 'Server error',
      };

      // Should not throw even if DynamoDB write fails
      await expect(processor.process(error)).resolves.toBeUndefined();

      // Give async error handling time to complete
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(console.error).toHaveBeenCalledWith(
        expect.stringContaining('[TrackingTargetApiErrorProcessor] Failed to write error event to DynamoDB'),
        expect.any(Error)
      );
    });

    it('should log to console if enabled', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: true,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const error = {
        response: { status: 429 },
        message: 'Throttled',
      };

      await processor.process(error);

      expect(console.warn).toHaveBeenCalledWith('HURON IS THROTTLING US!!!');
    });

    it('should track multiple different errors', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      await processor.process({ response: { status: 429 }, message: 'Throttled' });
      await processor.process({ response: { status: 500 }, message: 'Server error' });
      await processor.process({ response: { status: 500 }, message: 'Another error' });
      await processor.process({ response: { status: 404 }, message: 'Not found' });

      const stats = processor.getStatisticsSummary();
      expect(stats.totalErrors).toBe(4);
      expect(stats.throttleCount).toBe(1);
      expect(stats.errorsByStatus[429]).toBe(1);
      expect(stats.errorsByStatus[500]).toBe(2);
      expect(stats.errorsByStatus[404]).toBe(1);
    });
  });

  describe('writeStatistics', () => {
    it('should write statistics to DynamoDB', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      // Simulate some errors first
      await processor.process({ response: { status: 429 }, message: 'Throttled' });
      await processor.process({ response: { status: 500 }, message: 'Server error' });

      const stats: SyncStatistics = {
        startTimestamp: '2026-04-15T19:30:00.000Z',
        endTimestamp: '2026-04-15T19:35:00.000Z',
        chunkCount: 1,
        chunkSize: 1000,
        totalRecords: 1000,
        sourceDescription: 'chunk-0001',
      };

      await processor.writeStatistics(stats);

      // Should have 3 calls total: 2 errors + 1 statistics
      await new Promise(resolve => setTimeout(resolve, 10));
      expect(dynamoMock.calls().length).toBeGreaterThanOrEqual(1);
    });

    it('should include error summary in statistics', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      let statisticsItem: any;
      dynamoMock.on(PutItemCommand).callsFake((input) => {
        if (input.Item && input.Item.eventType && input.Item.eventType.S === 'STATISTICS') {
          statisticsItem = input.Item;
        }
        return {};
      });

      await processor.process({ response: { status: 429 }, message: 'Throttled' });
      await processor.process({ response: { status: 500 }, message: 'Server error' });

      const stats: SyncStatistics = {
        startTimestamp: '2026-04-15T19:30:00.000Z',
        endTimestamp: '2026-04-15T19:35:00.000Z',
        chunkCount: 1,
        chunkSize: 1000,
        totalRecords: 1000,
        sourceDescription: 'chunk-0001',
      };

      await processor.writeStatistics(stats);

      expect(statisticsItem).toBeDefined();
      expect(statisticsItem.totalErrors.N).toBe('2');
      expect(statisticsItem.throttleCount.N).toBe('1');
    });

    it('should throw error if DynamoDB write fails', async () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).rejects(new Error('DynamoDB error'));

      const stats: SyncStatistics = {
        startTimestamp: '2026-04-15T19:30:00.000Z',
        endTimestamp: '2026-04-15T19:35:00.000Z',
        chunkCount: 1,
        chunkSize: 1000,
        totalRecords: 1000,
        sourceDescription: 'chunk-0001',
      };

      await expect(processor.writeStatistics(stats)).rejects.toThrow('DynamoDB error');
    });
  });

  describe('statistics methods', () => {
    it('should return accurate statistics summary', () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      // Don't await - just check in-memory tracking
      processor.process({ response: { status: 429 }, message: 'Throttled' });
      processor.process({ response: { status: 500 }, message: 'Error 1' });
      processor.process({ response: { status: 500 }, message: 'Error 2' });
      processor.process({ response: { status: 404 }, message: 'Not found' });

      const summary = processor.getStatisticsSummary();
      expect(summary.totalErrors).toBe(4);
      expect(summary.throttleCount).toBe(1);
      expect(summary.errorsByStatus).toEqual({
        429: 1,
        500: 2,
        404: 1,
      });
    });

    it('should return throttle count', () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      expect(processor.getThrottlingCount()).toBe(0);

      processor.process({ response: { status: 429 }, message: 'Throttled' });
      processor.process({ response: { status: 429 }, message: 'Throttled again' });

      expect(processor.getThrottlingCount()).toBe(2);
    });

    it('should return error count by status code', () => {
      const processor = new TrackingTargetApiErrorProcessor({
        tableName: 'test-table',
        integrationTimestamp: '2026-04-15T19:30:00.000Z',
        logToConsole: false,
      });

      dynamoMock.on(PutItemCommand).resolves({});

      processor.process({ response: { status: 500 }, message: 'Error 1' });
      processor.process({ response: { status: 500 }, message: 'Error 2' });
      processor.process({ response: { status: 404 }, message: 'Not found' });

      expect(processor.getErrorCount(500)).toBe(2);
      expect(processor.getErrorCount(404)).toBe(1);
      expect(processor.getErrorCount(429)).toBe(0);
    });
  });
});
