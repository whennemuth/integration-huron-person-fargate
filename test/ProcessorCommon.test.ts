import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { CrudOperation, IntegrationResult, SinglePushResult, Status } from 'integration-core';
import { TrackingTargetApiErrorProcessor } from '../src/ApiErrorTracking';
import { pushedChangeCounts, writeTrackerStatistics } from '../src/processing/ProcessorCommon';

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

  it("counts a chunk's changes from its successful pushes, not from the delta that included its failures", async () => {
    // 200 in the delta (199 creates, 1 update); 5 creates failed
    const pushes = (crud: CrudOperation, count: number, status = Status.SUCCESS): SinglePushResult[] =>
      Array.from({ length: count }, () => ({ status, crud, primaryKey: [], timestamp: new Date() }));
    const integrationResult: IntegrationResult = {
      totalProcessed: 200, successCount: 195, failureCount: 5, skippedCount: 0,
      addedCount: 199, updatedCount: 1, removedCount: 0,
      successes: [...pushes(CrudOperation.CREATE, 194), ...pushes(CrudOperation.UPDATE, 1)],
      failures: pushes(CrudOperation.CREATE, 5, Status.FAILURE)
    };

    await writeTrackerStatistics({
      errorTracker: tracker(), startTimestamp: '2026-10-01T00:00:01.000Z', chunkId: '0384',
      processedRecordCount: 200, integrationResult
    });

    expect(writtenItem()).toMatchObject({
      successCount: { N: '195' }, failureCount: { N: '5' },
      addedCount: { N: '194' }, updatedCount: { N: '1' }, removedCount: { N: '0' }
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

describe('pushedChangeCounts', () => {
  const success = (crud?: CrudOperation): SinglePushResult =>
    ({ status: Status.SUCCESS, crud, primaryKey: [], timestamp: new Date() });
  const delta = { totalProcessed: 4, successCount: 3, failureCount: 1, skippedCount: 0, addedCount: 2, updatedCount: 1, removedCount: 1 };

  it('counts the successes by operation, so they sum to the successes', () => {
    const result = { ...delta, successes: [success(CrudOperation.CREATE), success(CrudOperation.UPDATE), success(CrudOperation.DELETE)] };
    expect(pushedChangeCounts(result)).toEqual({ addedCount: 1, updatedCount: 1, removedCount: 1 });
  });

  it("keeps the delta's counts when there are no push results, or a success doesn't name its operation", () => {
    const fromDelta = { addedCount: 2, updatedCount: 1, removedCount: 1 };
    expect(pushedChangeCounts(delta)).toEqual(fromDelta);
    expect(pushedChangeCounts({ ...delta, successes: [success(CrudOperation.CREATE), success()] })).toEqual(fromDelta);
  });

  it('counts nothing when nothing succeeded', () => {
    expect(pushedChangeCounts({ ...delta, successes: [] })).toEqual({ addedCount: 0, updatedCount: 0, removedCount: 0 });
  });
});
