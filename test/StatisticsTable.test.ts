import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { StatisticsTable } from '../src/statistics/StatisticsTable';
import { IContext } from '../context/IContext';
import { StatisticsItem } from '../src/processing/ApiErrorTracking';

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

  describe('getStatistics', () => {
    it('should retrieve aggregated statistics with STATISTICS sort key', async () => {
      const mockStats: StatisticsItem = {
        integrationTimestamp,
        eventType: 'STATISTICS',
        errorType: 'STATISTICS',
        startTimestamp: '2026-05-14T12:00:00.000Z',
        endTimestamp: '2026-05-14T12:05:00.000Z',
        chunkCount: 10,
        chunkSize: 1000,
        totalRecords: 10000,
        sourceDescription: 'Test source',
        totalErrors: 5,
        throttleCount: 2,
        errorsByStatus: { '500': 3, '429': 2 }
      };

      dynamoMock.on(GetCommand).resolves({ Item: mockStats });

      const result = await statisticsTable.getStatistics(integrationTimestamp);

      expect(result).toEqual(mockStats);
      expect(dynamoMock.commandCalls(GetCommand)).toHaveLength(1);
      const call = dynamoMock.commandCalls(GetCommand)[0];
      expect(call.args[0].input.Key).toEqual({
        integrationTimestamp,
        eventType: 'STATISTICS'
      });
    });

    it('should return undefined when aggregated statistics do not exist', async () => {
      dynamoMock.on(GetCommand).resolves({ Item: undefined });

      const result = await statisticsTable.getStatistics(integrationTimestamp);

      expect(result).toBeUndefined();
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
    it('should return unique integration timestamps', async () => {
      const timestamp1 = '2026-05-14T12:00:00.000Z';
      const timestamp2 = '2026-05-14T13:00:00.000Z';

      const mockItems = [
        { integrationTimestamp: timestamp1, eventType: 'STATISTICS' },
        { integrationTimestamp: timestamp1, eventType: 'STATISTICS-chunk-0000' },
        { integrationTimestamp: timestamp1, eventType: 'STATISTICS-chunk-0001' },
        { integrationTimestamp: timestamp2, eventType: 'STATISTICS' },
        { integrationTimestamp: timestamp2, eventType: 'STATISTICS-chunk-0000' }
      ];

      dynamoMock.on(QueryCommand).resolves({ Items: mockItems });

      const result = await statisticsTable.getUniqueIntegrationTimestamps();

      expect(result).toEqual([timestamp1, timestamp2]);
      expect(result).toHaveLength(2);
    });

    it('should handle empty results', async () => {
      dynamoMock.on(QueryCommand).resolves({ Items: [] });

      const result = await statisticsTable.getUniqueIntegrationTimestamps();

      expect(result).toEqual([]);
    });

    it('should sort timestamps chronologically', async () => {
      const timestamp1 = '2026-05-14T12:00:00.000Z';
      const timestamp2 = '2026-05-14T11:00:00.000Z';
      const timestamp3 = '2026-05-14T13:00:00.000Z';

      const mockItems = [
        { integrationTimestamp: timestamp1 },
        { integrationTimestamp: timestamp3 },
        { integrationTimestamp: timestamp2 }
      ];

      dynamoMock.on(QueryCommand).resolves({ Items: mockItems });

      const result = await statisticsTable.getUniqueIntegrationTimestamps();

      expect(result).toEqual([timestamp2, timestamp1, timestamp3]);
    });
  });
});
