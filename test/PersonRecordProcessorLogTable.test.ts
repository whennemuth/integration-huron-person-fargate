import { PersonRecordProcessorLogTable, DYNAMODB_PARTITION_KEY, DYNAMODB_SORT_KEY } from '../src/dynamodb/PersonRecordProcessorLogTable';
import { AbstractDynamoDbTable } from '../src/dynamodb/DynamoDBTable';
import { IContext } from '../context/IContext';

describe('PersonRecordProcessorLogTable', () => {
  const mockContext = {
    STACK_ID: 'test-stack',
    REGION: 'us-east-1',
    TAGS: { Landscape: 'test' }
  } as unknown as IContext;

  let putItem: jest.Mock;
  let fakeTable: AbstractDynamoDbTable;
  let logTable: PersonRecordProcessorLogTable;

  beforeEach(() => {
    putItem = jest.fn().mockResolvedValue(undefined);
    fakeTable = { putItem } as unknown as AbstractDynamoDbTable;
    logTable = new PersonRecordProcessorLogTable(mockContext, fakeTable);
  });

  it('exposes the shared partition/sort key names', () => {
    expect(DYNAMODB_PARTITION_KEY).toBe('customization');
    expect(DYNAMODB_SORT_KEY).toBe('sortKey');
  });

  it('writes an item keyed by customization/sortKey with a generic data blob', async () => {
    await logTable.putEntry('OrgComparisonLogging', 'U12345', { personid: 'U12345', primaryOrg: 'A', secondaryOrg: 'B' });

    expect(putItem).toHaveBeenCalledTimes(1);
    const item = putItem.mock.calls[0][0];
    expect(item.customization).toBe('OrgComparisonLogging');
    expect(item.personid).toBe('U12345');
    expect(item.data).toEqual({ personid: 'U12345', primaryOrg: 'A', secondaryOrg: 'B' });
    expect(item.sortKey).toMatch(/^\d{4}-\d{2}-\d{2}T.*#U12345$/);
  });

  it('builds a distinct sortKey per personid so entries never collide', async () => {
    await logTable.putEntry('OrgComparisonLogging', 'U1', {});
    await logTable.putEntry('OrgComparisonLogging', 'U2', {});

    const [firstItem] = putItem.mock.calls[0];
    const [secondItem] = putItem.mock.calls[1];
    expect(firstItem.sortKey).not.toBe(secondItem.sortKey);
  });

  it('fromTableName constructs a usable instance without an IContext', () => {
    const table = PersonRecordProcessorLogTable.fromTableName('some-table', 'us-east-1');
    expect(table).toBeInstanceOf(PersonRecordProcessorLogTable);
  });
});
