import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBDocumentClient, GetCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { PersonCurrentStateTable } from '../src/dynamodb/PersonCurrentStateTable';

const dynamoMock = mockClient(DynamoDBDocumentClient);

describe('PersonCurrentStateTable', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  describe('fromTableName', () => {
    it('targets the given table name directly, bypassing IContext-based resolution', async () => {
      dynamoMock.on(GetCommand).resolves({ Item: { personId: 'U0000001', hash: 'h1', syncRunId: 'x' } });

      const table = PersonCurrentStateTable.fromTableName('my-mock-person-current-state-table', 'us-east-2');
      const result = await table.getPersonState('U0000001');

      expect(result?.personId).toBe('U0000001');
      const call = dynamoMock.commandCalls(GetCommand)[0];
      expect(call.args[0].input.TableName).toBe('my-mock-person-current-state-table');
    });

    it('truncates only the specified table', async () => {
      dynamoMock.on(ScanCommand).resolves({ Items: [{ personId: 'U0000001' }] });

      const table = PersonCurrentStateTable.fromTableName('my-mock-person-current-state-table', 'us-east-2');
      await table.truncate();

      const call = dynamoMock.commandCalls(ScanCommand)[0];
      expect(call.args[0].input.TableName).toBe('my-mock-person-current-state-table');
    });
  });
});
