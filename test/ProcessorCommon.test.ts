import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { IntegrationResult } from 'integration-core';
import { TrackingTargetApiErrorProcessor } from '../src/ApiErrorTracking';
import { writeTrackerStatistics } from '../src/processing/ProcessorCommon';

const dynamoMock = mockClient(DynamoDBClient);

describe('writeTrackerStatistics', () => {
  const writtenItem = () => dynamoMock.commandCalls(PutItemCommand)[0].args[0].input.Item!;
  const tracker = () => new TrackingTargetApiErrorProcessor({
    tableName: 'stats', integrationTimestamp: '2026-10-01T00:00:00.000Z', logToConsole: false
  });

  beforeEach(() => {
    dynamoMock.reset();
    dynamoMock.on(PutItemCommand).resolves({});
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
  });

  afterEach(() => jest.restoreAllMocks());

  it('records the chunk IntegrationResult counts on its STATISTICS-chunk item', async () => {
    const integrationResult: IntegrationResult = {
      totalProcessed: 200, successCount: 190, failureCount: 4, skippedCount: 6,
      addedCount: 10, updatedCount: 180, removedCount: 0
    };

    await writeTrackerStatistics({
      errorTracker: tracker(), startTimestamp: '2026-10-01T00:00:01.000Z', chunkId: '0007',
      processedRecordCount: 200, integrationResult
    });

    expect(writtenItem()).toMatchObject({
      eventType: { S: 'STATISTICS-chunk-0007' },
      totalRecords: { N: '200' },
      successCount: { N: '190' },
      failureCount: { N: '4' },
      skippedCount: { N: '6' },
      addedCount: { N: '10' },
      updatedCount: { N: '180' },
      removedCount: { N: '0' }
    });
  });

  it('omits the counts when the chunk produced no IntegrationResult', async () => {
    await writeTrackerStatistics({
      errorTracker: tracker(), startTimestamp: '2026-10-01T00:00:01.000Z', chunkId: '0007', processedRecordCount: 0
    });

    expect(writtenItem().successCount).toBeUndefined();
    expect(writtenItem().addedCount).toBeUndefined();
  });
});
