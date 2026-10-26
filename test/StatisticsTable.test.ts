import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBDocumentClient, GetCommand, QueryCommand, BatchWriteCommand, UpdateCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { StatisticsTable } from '../src/dynamodb/StatisticsTable';
import { IContext } from '../context/IContext';
import { StatisticsItem } from '../src/ApiErrorTracking';

const dynamoMock = mockClient(DynamoDBDocumentClient);

describe('StatisticsTable', () => {
  let statisticsTable: StatisticsTable;
  const mockContext = {
    STACK_ID: 'test-stack',
    REGION: 'us-east-1',
    ACCOUNT: '123456789012',
    TAGS: {
      Landscape: 'test',
      Service: 'integration',
      Function: 'person-sync'
    }
  } as unknown as IContext;

  const integrationTimestamp = '2026-05-14T12:00:00.000Z';

  beforeEach(() => {
    dynamoMock.reset();
    statisticsTable = new StatisticsTable(mockContext);
  });

  describe('run summary', () => {
    const chunk = (chunkId: string, overrides: Partial<StatisticsItem> = {}): StatisticsItem => ({
      integrationTimestamp,
      eventType: `STATISTICS-${chunkId}`,
      errorType: 'STATISTICS',
      chunkId,
      startTimestamp: '2026-05-14T12:00:00.000Z',
      endTimestamp: '2026-05-14T12:01:00.000Z',
      chunkCount: 1,
      chunkSize: 200,
      totalRecords: 200,
      sourceDescription: chunkId,
      totalErrors: 0,
      throttleCount: 0,
      errorsByStatus: {},
      ...overrides
    });

    const mockRun = (chunks: StatisticsItem[]) => {
      dynamoMock.on(GetCommand).resolves({ Item: {
        integrationTimestamp, eventType: 'METADATA',
        createdAt: '2026-05-14T11:59:00.000Z', syncPopulation: 'person-full', bulkReset: false,
        chunkCount: 3, totalRecords: 600
      } });
      dynamoMock.on(QueryCommand).resolves({ Items: chunks });
    };

    it('sums chunk counts and errors and folds in METADATA and the deletion outcome', async () => {
      mockRun([
        chunk('chunk-0000', { successCount: 190, failureCount: 10, addedCount: 5, updatedCount: 185,
          totalErrors: 10, throttleCount: 2, errorsByStatus: { 500: 8, 429: 2 } }),
        chunk('chunk-0001', { successCount: 200, addedCount: 1, updatedCount: 199,
          totalErrors: 1, errorsByStatus: { 500: 1 } })
      ]);
      const deletion = { status: 'COMPLETED' as const, deletedCount: 3, failedCount: 0, totalProcessed: 3, message: 'ok' };

      const summary = await statisticsTable.buildRunSummary(integrationTimestamp, {
        postProcessingSucceeded: true, storageMode: 'dynamodb', dryRun: false, deletion
      });

      expect(summary).toMatchObject({
        syncRunId: integrationTimestamp,
        status: 'SUCCEEDED',
        storageMode: 'dynamodb',
        createdAt: '2026-05-14T11:59:00.000Z',
        syncPopulation: 'person-full',
        chunkCount: 3,
        totalRecords: 600,
        chunksReported: 2,
        results: { processed: 400, succeeded: 390, failed: 10, skipped: 0, added: 6, updated: 384, removed: 0 },
        totalErrors: 11,
        throttleCount: 2,
        errorsByStatus: { '500': 9, '429': 2 },
        deletion
      });
    });

    it('is FAILED when deletions were skipped because the run was marked failed', async () => {
      mockRun([]);
      const summary = await statisticsTable.buildRunSummary(integrationTimestamp, {
        postProcessingSucceeded: true, dryRun: false,
        deletion: { status: 'SKIPPED', reason: 'TERMINAL_ERROR', message: 'marked failed' }
      });
      expect(summary.status).toBe('FAILED');
    });

    it('is POST_PROCESSING_FAILED when post-processing failed', async () => {
      mockRun([]);
      const summary = await statisticsTable.buildRunSummary(integrationTimestamp, {
        postProcessingSucceeded: false, dryRun: false
      });
      expect(summary.status).toBe('POST_PROCESSING_FAILED');
    });

    it('writes and reads the summary under the RUN_SUMMARY sort key', async () => {
      dynamoMock.on(BatchWriteCommand).resolves({});
      mockRun([]);
      const summary = await statisticsTable.buildRunSummary(integrationTimestamp, { postProcessingSucceeded: true, dryRun: true });

      await statisticsTable.writeRunSummary(summary);

      const requestItems = Object.values(dynamoMock.commandCalls(BatchWriteCommand)[0].args[0].input.RequestItems!)[0] as any[];
      expect(requestItems[0].PutRequest.Item).toMatchObject({ integrationTimestamp, eventType: 'RUN_SUMMARY', status: 'SUCCEEDED', dryRun: true });

      dynamoMock.on(GetCommand).resolves({ Item: { integrationTimestamp, eventType: 'RUN_SUMMARY', ...summary } });
      expect(await statisticsTable.readRunSummary(integrationTimestamp)).toEqual(summary);
      expect(dynamoMock.commandCalls(GetCommand).at(-1)!.args[0].input.Key).toEqual({ integrationTimestamp, eventType: 'RUN_SUMMARY' });
    });

    it('readRunSummary returns undefined when no summary was written', async () => {
      dynamoMock.on(GetCommand).resolves({ Item: undefined });
      expect(await statisticsTable.readRunSummary(integrationTimestamp)).toBeUndefined();
    });
  });

  describe('getRunErrorsPage', () => {
    it('queries one page of ERROR items, optionally for one status code', async () => {
      dynamoMock.on(QueryCommand).resolves({ Items: [{ eventType: 'ERROR:500:t1' }], LastEvaluatedKey: { k: 1 } });

      const page = await statisticsTable.getRunErrorsPage(integrationTimestamp, { limit: 10 });
      await statisticsTable.getRunErrorsPage(integrationTimestamp, { statusCode: 429, limit: 5, startKey: { k: 1 } });

      expect(page).toEqual({ items: [{ eventType: 'ERROR:500:t1' }], lastEvaluatedKey: { k: 1 } });
      const [first, second] = dynamoMock.commandCalls(QueryCommand).map(call => call.args[0].input);
      expect(first).toMatchObject({ Limit: 10, ExpressionAttributeValues: { ':pkValue': integrationTimestamp, ':skPrefix': 'ERROR:' } });
      expect(first.ExclusiveStartKey).toBeUndefined();
      expect(second).toMatchObject({ Limit: 5, ExclusiveStartKey: { k: 1 }, ExpressionAttributeValues: { ':skPrefix': 'ERROR:429:' } });
    });
  });

  describe('getChunkStatistics', () => {
    it('should retrieve chunk-specific statistics', async () => {
      const chunkId = 'chunk-0009';
      const mockChunkStats: StatisticsItem = {
        integrationTimestamp,
        eventType: `STATISTICS-${chunkId}`,
        errorType: 'STATISTICS',
        chunkId,
        startTimestamp: '2026-05-14T12:00:00.000Z',
        endTimestamp: '2026-05-14T12:01:00.000Z',
        chunkCount: 1,
        chunkSize: 1000,
        totalRecords: 1000,
        sourceDescription: chunkId,
        totalErrors: 1,
        throttleCount: 0,
        errorsByStatus: { '500': 1 }
      };

      dynamoMock.on(GetCommand).resolves({ Item: mockChunkStats });

      const result = await statisticsTable.getChunkStatistics(integrationTimestamp, chunkId);

      expect(result).toEqual(mockChunkStats);
      expect(dynamoMock.commandCalls(GetCommand)).toHaveLength(1);
      const call = dynamoMock.commandCalls(GetCommand)[0];
      expect(call.args[0].input.Key).toEqual({
        integrationTimestamp,
        eventType: `STATISTICS-${chunkId}`
      });
    });

    it('should return undefined when chunk statistics do not exist', async () => {
      dynamoMock.on(GetCommand).resolves({ Item: undefined });

      const result = await statisticsTable.getChunkStatistics(integrationTimestamp, 'chunk-9999');

      expect(result).toBeUndefined();
    });
  });

  describe('getAllChunkStatistics', () => {
    it('should retrieve all chunk statistics for an integration run', async () => {
      const mockChunks: StatisticsItem[] = [
        {
          integrationTimestamp,
          eventType: 'STATISTICS-chunk-0000',
          errorType: 'STATISTICS',
          chunkId: 'chunk-0000',
          startTimestamp: '2026-05-14T12:00:00.000Z',
          endTimestamp: '2026-05-14T12:01:00.000Z',
          chunkCount: 1,
          chunkSize: 1000,
          totalRecords: 1000,
          sourceDescription: 'chunk-0000',
          totalErrors: 0,
          throttleCount: 0,
          errorsByStatus: {}
        },
        {
          integrationTimestamp,
          eventType: 'STATISTICS-chunk-0001',
          errorType: 'STATISTICS',
          chunkId: 'chunk-0001',
          startTimestamp: '2026-05-14T12:01:00.000Z',
          endTimestamp: '2026-05-14T12:02:00.000Z',
          chunkCount: 1,
          chunkSize: 1000,
          totalRecords: 1000,
          sourceDescription: 'chunk-0001',
          totalErrors: 2,
          throttleCount: 1,
          errorsByStatus: { '500': 2 }
        },
        {
          integrationTimestamp,
          eventType: 'STATISTICS-chunk-0002',
          errorType: 'STATISTICS',
          chunkId: 'chunk-0002',
          startTimestamp: '2026-05-14T12:02:00.000Z',
          endTimestamp: '2026-05-14T12:03:00.000Z',
          chunkCount: 1,
          chunkSize: 1000,
          totalRecords: 1000,
          sourceDescription: 'chunk-0002',
          totalErrors: 1,
          throttleCount: 0,
          errorsByStatus: { '429': 1 }
        }
      ];

      dynamoMock.on(QueryCommand).resolves({ Items: mockChunks });

      const result = await statisticsTable.getAllChunkStatistics(integrationTimestamp);

      expect(result).toEqual(mockChunks);
      expect(result).toHaveLength(3);
      expect(dynamoMock.commandCalls(QueryCommand)).toHaveLength(1);
      
      const call = dynamoMock.commandCalls(QueryCommand)[0];
      expect(call.args[0].input.KeyConditionExpression).toContain('begins_with');
      expect(call.args[0].input.ExpressionAttributeValues).toEqual({
        ':pkValue': integrationTimestamp,
        ':skPrefix': 'STATISTICS-chunk-'
      });
    });

    it('should return empty array when no chunks exist', async () => {
      dynamoMock.on(QueryCommand).resolves({ Items: [] });

      const result = await statisticsTable.getAllChunkStatistics(integrationTimestamp);

      expect(result).toEqual([]);
      expect(result).toHaveLength(0);
    });

    it('should handle pagination correctly', async () => {
      const mockChunksPage1: StatisticsItem[] = [
        {
          integrationTimestamp,
          eventType: 'STATISTICS-chunk-0000',
          errorType: 'STATISTICS',
          chunkId: 'chunk-0000',
          startTimestamp: '2026-05-14T12:00:00.000Z',
          endTimestamp: '2026-05-14T12:01:00.000Z',
          chunkCount: 1,
          chunkSize: 1000,
          totalRecords: 1000,
          sourceDescription: 'chunk-0000',
          totalErrors: 0,
          throttleCount: 0,
          errorsByStatus: {}
        }
      ];

      const mockChunksPage2: StatisticsItem[] = [
        {
          integrationTimestamp,
          eventType: 'STATISTICS-chunk-0001',
          errorType: 'STATISTICS',
          chunkId: 'chunk-0001',
          startTimestamp: '2026-05-14T12:01:00.000Z',
          endTimestamp: '2026-05-14T12:02:00.000Z',
          chunkCount: 1,
          chunkSize: 1000,
          totalRecords: 1000,
          sourceDescription: 'chunk-0001',
          totalErrors: 1,
          throttleCount: 0,
          errorsByStatus: { '500': 1 }
        }
      ];

      dynamoMock.on(QueryCommand)
        .resolvesOnce({ Items: mockChunksPage1, LastEvaluatedKey: { pk: 'key1' } })
        .resolvesOnce({ Items: mockChunksPage2 });

      const result = await statisticsTable.getAllChunkStatistics(integrationTimestamp);

      expect(result).toHaveLength(2);
      expect(result).toEqual([...mockChunksPage1, ...mockChunksPage2]);
      expect(dynamoMock.commandCalls(QueryCommand)).toHaveLength(2);
    });
  });

  describe('getUniqueIntegrationTimestamps', () => {
    it('scans the distinct-run-index and returns its timestamps', async () => {
      const timestamp1 = '2026-05-14T12:00:00.000Z';
      const timestamp2 = '2026-05-14T13:00:00.000Z';

      dynamoMock.on(ScanCommand).resolves({ Items: [
        { integrationTimestamp: timestamp1, eventType: 'RUN', distinctRunId: timestamp1 },
        { integrationTimestamp: timestamp2, eventType: 'RUN', distinctRunId: timestamp2 }
      ] });

      const result = await statisticsTable.getUniqueIntegrationTimestamps();

      expect(result).toEqual([timestamp1, timestamp2]);
      expect(dynamoMock.commandCalls(ScanCommand)[0].args[0].input.IndexName).toBe('distinct-run-index');
      expect(dynamoMock.commandCalls(QueryCommand)).toHaveLength(0);
    });

    it('should handle empty results', async () => {
      dynamoMock.on(ScanCommand).resolves({ Items: [] });

      const result = await statisticsTable.getUniqueIntegrationTimestamps();

      expect(result).toEqual([]);
    });

    it('should sort timestamps chronologically and follow pagination', async () => {
      const timestamp1 = '2026-05-14T12:00:00.000Z';
      const timestamp2 = '2026-05-14T11:00:00.000Z';
      const timestamp3 = '2026-05-14T13:00:00.000Z';

      dynamoMock.on(ScanCommand)
        .resolvesOnce({ Items: [{ integrationTimestamp: timestamp1 }, { integrationTimestamp: timestamp3 }], LastEvaluatedKey: { k: 1 } })
        .resolvesOnce({ Items: [{ integrationTimestamp: timestamp2 }] });

      const result = await statisticsTable.getUniqueIntegrationTimestamps();

      expect(result).toEqual([timestamp2, timestamp1, timestamp3]);
      expect(dynamoMock.commandCalls(ScanCommand)).toHaveLength(2);
    });
  });

  describe('run marker', () => {
    const runMarker = { integrationTimestamp, eventType: 'RUN', distinctRunId: integrationTimestamp };
    const putItems = () => dynamoMock.commandCalls(BatchWriteCommand)
      .flatMap(call => Object.values(call.args[0].input.RequestItems!)[0] as any[])
      .map(request => request.PutRequest?.Item);

    beforeEach(() => {
      dynamoMock.on(BatchWriteCommand).resolves({});
    });

    it('is written by writeFlags', async () => {
      await statisticsTable.writeFlags(integrationTimestamp, { bulkReset: false });
      expect(putItems()).toContainEqual(runMarker);
    });

    it('is written by writeTerminalError', async () => {
      await statisticsTable.writeTerminalError(integrationTimestamp, { errorMessage: 'boom' });
      expect(putItems()).toContainEqual(runMarker);
    });

    it('is written by addToMetadataTotals', async () => {
      dynamoMock.on(UpdateCommand).resolves({});
      await statisticsTable.addToMetadataTotals(integrationTimestamp, {
        source: 's', chunkDirectory: 'c', itemsPerChunk: 1, bulkReset: false, trustPreviousStorage: false,
        syncPopulation: 'p', deltaStoragePath: 'd', chunkCountDelta: 1, totalRecordsDelta: 1
      });
      expect(putItems()).toContainEqual(runMarker);
    });

    it('is backfilled once per distinct run', async () => {
      const other = '2026-05-15T12:00:00.000Z';
      dynamoMock.on(ScanCommand).resolves({ Items: [
        { integrationTimestamp, eventType: 'FLAGS' },
        { integrationTimestamp, eventType: 'STATISTICS-chunk-0000' },
        { integrationTimestamp: other, eventType: 'METADATA' }
      ] });

      const count = await statisticsTable.backfillRunMarkers();

      expect(count).toBe(2);
      expect(putItems()).toEqual([
        runMarker,
        { integrationTimestamp: other, eventType: 'RUN', distinctRunId: other }
      ]);
    });
  });

  describe('claimProcessorBoost', () => {
    it('returns true and writes a PROCESSOR_BOOST_CLAIM record when none exists', async () => {
      dynamoMock.on(UpdateCommand).resolves({});
      dynamoMock.on(BatchWriteCommand).resolves({});

      const won = await statisticsTable.claimProcessorBoost(integrationTimestamp, '840');

      expect(won).toBe(true);
      expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(1);
      const updateCall = dynamoMock.commandCalls(UpdateCommand)[0];
      expect(updateCall.args[0].input.Key).toEqual({
        integrationTimestamp,
        eventType: 'PROCESSOR_BOOST_CLAIM'
      });
      expect(dynamoMock.commandCalls(BatchWriteCommand)).toHaveLength(1);
      const batchCall = dynamoMock.commandCalls(BatchWriteCommand)[0];
      const requestItems = Object.values(batchCall.args[0].input.RequestItems!)[0] as any[];
      expect(requestItems[0].PutRequest.Item).toMatchObject({
        integrationTimestamp,
        eventType: 'PROCESSOR_BOOST_CLAIM',
        claimedByChunk: '840'
      });
    });

    it('returns false and does not write when a recent claim already exists', async () => {
      const recentClaimedAt = new Date().toISOString();
      dynamoMock.on(UpdateCommand).rejects({ name: 'ConditionalCheckFailedException' });
      dynamoMock.on(GetCommand).resolves({ Item: { integrationTimestamp, eventType: 'PROCESSOR_BOOST_CLAIM', claimedAt: recentClaimedAt } });

      const won = await statisticsTable.claimProcessorBoost(integrationTimestamp);

      expect(won).toBe(false);
      expect(dynamoMock.commandCalls(BatchWriteCommand)).toHaveLength(0);
    });

    it('clears and retries when the existing claim is stale', async () => {
      const staleClaimedAt = new Date(Date.now() - 11 * 60 * 1000).toISOString();
      dynamoMock.on(UpdateCommand)
        .rejectsOnce({ name: 'ConditionalCheckFailedException' }) // initial attempt loses
        .resolvesOnce({}) // unset() clears the stale attribute
        .resolves({}); // retry succeeds
      dynamoMock.on(GetCommand).resolves({ Item: { integrationTimestamp, eventType: 'PROCESSOR_BOOST_CLAIM', claimedAt: staleClaimedAt } });
      dynamoMock.on(BatchWriteCommand).resolves({});

      const won = await statisticsTable.claimProcessorBoost(integrationTimestamp, '840');

      expect(won).toBe(true);
      expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(3);
    });
  });

  describe('releaseProcessorBoostClaim', () => {
    it('deletes the PROCESSOR_BOOST_CLAIM record', async () => {
      dynamoMock.on(BatchWriteCommand).resolves({});

      await statisticsTable.releaseProcessorBoostClaim(integrationTimestamp);

      expect(dynamoMock.commandCalls(BatchWriteCommand)).toHaveLength(1);
      const call = dynamoMock.commandCalls(BatchWriteCommand)[0];
      const requestItems = Object.values(call.args[0].input.RequestItems!)[0] as any[];
      expect(requestItems[0].DeleteRequest.Key).toEqual({
        integrationTimestamp,
        eventType: 'PROCESSOR_BOOST_CLAIM'
      });
    });
  });

  describe('claimPostProcessorTrigger', () => {
    it('returns true and writes a POST_PROCESSOR_TRIGGER_CLAIM record when none exists', async () => {
      dynamoMock.on(UpdateCommand).resolves({});
      dynamoMock.on(BatchWriteCommand).resolves({});

      const won = await statisticsTable.claimPostProcessorTrigger(integrationTimestamp, '0029');

      expect(won).toBe(true);
      expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(1);
      const updateCall = dynamoMock.commandCalls(UpdateCommand)[0];
      expect(updateCall.args[0].input.Key).toEqual({
        integrationTimestamp,
        eventType: 'POST_PROCESSOR_TRIGGER_CLAIM'
      });
      expect(dynamoMock.commandCalls(BatchWriteCommand)).toHaveLength(1);
      const batchCall = dynamoMock.commandCalls(BatchWriteCommand)[0];
      const requestItems = Object.values(batchCall.args[0].input.RequestItems!)[0] as any[];
      expect(requestItems[0].PutRequest.Item).toMatchObject({
        integrationTimestamp,
        eventType: 'POST_PROCESSOR_TRIGGER_CLAIM',
        claimedByChunk: '0029'
      });
    });

    it('returns false and does not write when the post-processor has already been triggered - no retry', async () => {
      dynamoMock.on(UpdateCommand).rejects({ name: 'ConditionalCheckFailedException' });

      const won = await statisticsTable.claimPostProcessorTrigger(integrationTimestamp, '0030');

      expect(won).toBe(false);
      expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(1);
      expect(dynamoMock.commandCalls(BatchWriteCommand)).toHaveLength(0);
    });

    it('does not write claimedByChunk when not provided', async () => {
      dynamoMock.on(UpdateCommand).resolves({});

      const won = await statisticsTable.claimPostProcessorTrigger(integrationTimestamp);

      expect(won).toBe(true);
      expect(dynamoMock.commandCalls(BatchWriteCommand)).toHaveLength(0);
    });
  });

  describe('addToMetadataTotals', () => {
    const baseParams = {
      source: 'https://api.example.com/people',
      chunkDirectory: 'chunks/person-full/2026-05-14T12:00:00.000Z',
      itemsPerChunk: 200,
      bulkReset: false,
      trustPreviousStorage: true,
      syncPopulation: 'person-full',
      deltaStoragePath: 'deltas/person-full/2026-05-14T12:00:00.000Z',
      chunkCountDelta: 3,
      totalRecordsDelta: 600
    };

    beforeEach(() => {
      dynamoMock.on(BatchWriteCommand).resolves({});
    });

    it('sends an UpdateCommand with SET if_not_exists + ADD and returns the new totals', async () => {
      dynamoMock.on(UpdateCommand).resolves({ Attributes: { chunkCount: 9, totalRecords: 1800 } });

      const result = await statisticsTable.addToMetadataTotals(integrationTimestamp, baseParams);

      expect(result).toEqual({ chunkCount: 9, totalRecords: 1800 });
      expect(dynamoMock.commandCalls(UpdateCommand)).toHaveLength(1);
      const call = dynamoMock.commandCalls(UpdateCommand)[0];
      expect(call.args[0].input.Key).toEqual({ integrationTimestamp, eventType: 'METADATA' });
      expect(call.args[0].input.UpdateExpression).toContain('if_not_exists(#source, :source)');
      expect(call.args[0].input.UpdateExpression).toContain('ADD #chunkCount :chunkCountDelta, #totalRecords :totalRecordsDelta');
      expect(call.args[0].input.ExpressionAttributeValues).toMatchObject({
        ':chunkCountDelta': 3,
        ':totalRecordsDelta': 600
      });
    });

    it('includes target in the SET clause only when provided', async () => {
      dynamoMock.on(UpdateCommand).resolves({ Attributes: { chunkCount: 3, totalRecords: 600 } });

      await statisticsTable.addToMetadataTotals(integrationTimestamp, { ...baseParams, target: 'https://target.example.com/persons' });

      const call = dynamoMock.commandCalls(UpdateCommand)[0];
      expect(call.args[0].input.UpdateExpression).toContain('if_not_exists(#target, :target)');
      expect(call.args[0].input.ExpressionAttributeValues).toMatchObject({ ':target': 'https://target.example.com/persons' });
    });

    it('omits the partialOrEmptyChunkEncountered SET clause when not provided', async () => {
      dynamoMock.on(UpdateCommand).resolves({ Attributes: { chunkCount: 3, totalRecords: 600 } });

      await statisticsTable.addToMetadataTotals(integrationTimestamp, baseParams);

      const call = dynamoMock.commandCalls(UpdateCommand)[0];
      expect(call.args[0].input.UpdateExpression).not.toContain('partialOrEmptyChunkEncountered');
    });

    it('includes the partialOrEmptyChunkEncountered SET clause only when true', async () => {
      dynamoMock.on(UpdateCommand).resolves({ Attributes: { chunkCount: 3, totalRecords: 600 } });

      await statisticsTable.addToMetadataTotals(integrationTimestamp, { ...baseParams, partialOrEmptyChunkEncountered: true });

      const call = dynamoMock.commandCalls(UpdateCommand)[0];
      expect(call.args[0].input.UpdateExpression).toContain('#partialOrEmptyChunkEncountered = :true');
      expect(call.args[0].input.ExpressionAttributeValues).toMatchObject({ ':true': true });
    });

    it('defaults to zero totals if Attributes is missing from the response', async () => {
      dynamoMock.on(UpdateCommand).resolves({});

      const result = await statisticsTable.addToMetadataTotals(integrationTimestamp, baseParams);

      expect(result).toEqual({ chunkCount: 0, totalRecords: 0 });
    });
  });

  describe('TERMINAL_ERROR record', () => {
    it('writes the marker under the TERMINAL_ERROR sort key, not METADATA', async () => {
      dynamoMock.on(BatchWriteCommand).resolves({});

      await statisticsTable.writeTerminalError(integrationTimestamp, {
        eventType: 'METADATA', errorMessage: 'boom', stage: 'chunking'
      });

      const requestItems = dynamoMock.commandCalls(BatchWriteCommand)[0].args[0].input.RequestItems!;
      const item = Object.values(requestItems)[0][0].PutRequest!.Item;
      expect(item).toEqual(expect.objectContaining({
        integrationTimestamp, eventType: 'TERMINAL_ERROR', errorMessage: 'boom', stage: 'chunking'
      }));
    });

    it('reads the marker from the TERMINAL_ERROR sort key', async () => {
      dynamoMock.on(GetCommand).resolves({
        Item: { integrationTimestamp, eventType: 'TERMINAL_ERROR', errorMessage: 'boom' }
      });

      const result = await statisticsTable.readTerminalError(integrationTimestamp);

      expect(result).toEqual({ errorMessage: 'boom' });
      expect(dynamoMock.commandCalls(GetCommand)[0].args[0].input.Key).toEqual({
        integrationTimestamp, eventType: 'TERMINAL_ERROR'
      });
    });

    it('returns undefined when the run was not marked failed', async () => {
      dynamoMock.on(GetCommand).resolves({ Item: undefined });

      expect(await statisticsTable.readTerminalError(integrationTimestamp)).toBeUndefined();
    });
  });
});
