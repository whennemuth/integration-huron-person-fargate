import { mockClient } from 'aws-sdk-client-mock';
import { BatchWriteCommand, DynamoDBDocumentClient, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { PersonHistoryTable } from '../src/dynamodb/PersonHistoryTable';

const dynamoMock = mockClient(DynamoDBDocumentClient);

describe('PersonHistoryTable', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  describe('fromTableName', () => {
    it('targets the given table name directly, bypassing IContext-based resolution', async () => {
      dynamoMock.on(QueryCommand).resolves({
        Items: [{ personId: 'U0000001', syncRunId: 'x', hash: 'h1', changeType: 'NEW' }]
      });

      const table = PersonHistoryTable.fromTableName('my-mock-person-history-table', 'us-east-2');
      const result = await table.getPersonHistory('U0000001');

      expect(result).toHaveLength(1);
      const call = dynamoMock.commandCalls(QueryCommand)[0];
      expect(call.args[0].input.TableName).toBe('my-mock-person-history-table');
    });

    it('truncates only the specified table', async () => {
      dynamoMock.on(ScanCommand).resolves({ Items: [{ personId: 'U0000001', syncRunId: 'x' }] });
      dynamoMock.on(BatchWriteCommand).resolves({});

      const table = PersonHistoryTable.fromTableName('my-mock-person-history-table', 'us-east-2');
      await table.truncate();

      const call = dynamoMock.commandCalls(ScanCommand)[0];
      expect(call.args[0].input.TableName).toBe('my-mock-person-history-table');
    });
  });

  describe('getChangesInSyncRunPage', () => {
    it('queries one page of a run\'s changes of one type on the syncRunId-changeType index', async () => {
      const items = [{ personId: 'U0000001', syncRunId: 'run1', hash: 'h1', changeType: 'NEW' }];
      dynamoMock.on(QueryCommand).resolves({ Items: items, LastEvaluatedKey: { personId: 'U0000001', syncRunId: 'run1', changeType: 'NEW' } });

      const table = PersonHistoryTable.fromTableName('history', 'us-east-2');
      const page = await table.getChangesInSyncRunPage('run1', 'NEW', { limit: 25, startKey: { k: 1 } });

      expect(page.items).toEqual(items);
      expect(page.lastEvaluatedKey).toEqual({ personId: 'U0000001', syncRunId: 'run1', changeType: 'NEW' });
      expect(dynamoMock.commandCalls(QueryCommand)[0].args[0].input).toMatchObject({
        IndexName: 'syncRunId-changeType-index',
        KeyConditionExpression: '#pk = :pkValue AND #sk = :skValue',
        ExpressionAttributeNames: { '#pk': 'syncRunId', '#sk': 'changeType' },
        ExpressionAttributeValues: { ':pkValue': 'run1', ':skValue': 'NEW' },
        Limit: 25,
        ExclusiveStartKey: { k: 1 }
      });
    });
  });
});
