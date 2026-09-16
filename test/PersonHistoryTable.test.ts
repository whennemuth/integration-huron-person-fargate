import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBDocumentClient, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
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

      const table = PersonHistoryTable.fromTableName('my-mock-person-history-table', 'us-east-2');
      await table.truncate();

      const call = dynamoMock.commandCalls(ScanCommand)[0];
      expect(call.args[0].input.TableName).toBe('my-mock-person-history-table');
    });
  });
});
