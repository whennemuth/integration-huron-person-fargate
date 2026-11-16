/**
 * Tests truncating a table (DynamoDBTable.truncateTable(), src/dynamodb/DynamoDBTable.ts): a parallel scan, every key
 * deleted once in batches of 25 with several in flight, throttled deletes retried, the count returned; and the mock
 * target table truncating through it (MockTargetPersonTable.truncate()), keys only, with no DeleteItem.
 */
import { BatchWriteCommand, DeleteCommand, DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBTable } from '../src/dynamodb/DynamoDBTable';
import { MockTargetPersonTable } from '../src/dynamodb/MockTargetPersonTable';

const dynamo = mockClient(DynamoDBDocumentClient);

/** A table of `count` items spread over the segments a scan asks for, `pageSize` keys a page */
const tableOf = (count: number, pageSize: number, keyName = 'personId', sortName?: string) => {
  const items = Array.from({ length: count }, (_, i) => ({ [keyName]: `U${String(i + 1).padStart(7, '0')}`, ...(sortName ? { [sortName]: `r${i % 3}` } : {}) }));
  dynamo.on(ScanCommand).callsFake(input => {
    const segment = input.Segment ?? 0;
    const segments = input.TotalSegments ?? 1;
    const mine = items.filter((_, i) => i % segments === segment);
    const start = input.ExclusiveStartKey ? Number(input.ExclusiveStartKey.position) : 0;
    const page = mine.slice(start, start + pageSize);
    return { Items: page, ...(start + pageSize < mine.length ? { LastEvaluatedKey: { position: start + pageSize } } : {}) };
  });
  return items;
};

const deletedKeys = () => dynamo.commandCalls(BatchWriteCommand)
  .flatMap(call => Object.values(call.args[0].input.RequestItems!).flat().map((request: any) => request.DeleteRequest.Key));

beforeEach(() => {
  dynamo.reset();
  jest.spyOn(console, 'log').mockImplementation(() => undefined);
});
afterEach(() => jest.restoreAllMocks());

describe('DynamoDBTable.truncateTable()', () => {
  it('scans every segment for keys only, deletes every key once in batches of 25, and returns the count', async () => {
    const items = tableOf(1000, 60);
    dynamo.on(BatchWriteCommand).resolves({});
    const table = new DynamoDBTable({ region: 'us-east-2', tableName: 'people', partitionKey: 'personId' });
    expect(await table.truncateTable()).toBe(1000);

    const scans = dynamo.commandCalls(ScanCommand).map(call => call.args[0].input);
    expect(new Set(scans.map(scan => scan.Segment))).toEqual(new Set([0, 1, 2, 3, 4, 5, 6, 7]));
    expect(scans.every(scan => scan.TotalSegments === DynamoDBTable.TRUNCATE_SEGMENTS)).toBe(true);
    expect(scans[0]).toMatchObject({ ProjectionExpression: '#pk', ExpressionAttributeNames: { '#pk': 'personId' } });
    const keys = deletedKeys().map(key => key.personId).sort();
    expect(keys).toEqual(items.map(item => item.personId).sort());
    expect(dynamo.commandCalls(BatchWriteCommand).every(call => (Object.values(call.args[0].input.RequestItems!)[0] as any[]).length <= 25)).toBe(true);
  });

  it('deletes by partition and sort key for a table with both', async () => {
    tableOf(10, 100, 'personId', 'syncRunId');
    dynamo.on(BatchWriteCommand).resolves({});
    const table = new DynamoDBTable({ region: 'us-east-2', tableName: 'history', partitionKey: 'personId', sortKey: 'syncRunId' });
    expect(await table.truncateTable(25, { segments: 2 })).toBe(10);
    expect(dynamo.commandCalls(ScanCommand)[0].args[0].input).toMatchObject({
      ProjectionExpression: '#pk, #sk', ExpressionAttributeNames: { '#pk': 'personId', '#sk': 'syncRunId' }
    });
    expect(deletedKeys()[0]).toEqual({ personId: expect.any(String), syncRunId: expect.any(String) });
  });

  it('keeps several delete batches in flight at once', async () => {
    tableOf(400, 400);
    let inFlight = 0;
    let most = 0;
    dynamo.on(BatchWriteCommand).callsFake(async () => {
      inFlight++;
      most = Math.max(most, inFlight);
      await new Promise(resolve => setTimeout(resolve, 5));
      inFlight--;
      return {};
    });
    const table = new DynamoDBTable({ region: 'us-east-2', tableName: 'people', partitionKey: 'personId' });
    await table.truncateTable(25, { segments: 2, batchesInFlight: 3 });
    expect(most).toBe(6);
  });

  it('retries deletes DynamoDB leaves unprocessed (throttled)', async () => {
    tableOf(30, 100);
    let first = true;
    dynamo.on(BatchWriteCommand).callsFake(input => {
      const [tableName, requests] = Object.entries(input.RequestItems)[0] as [string, any[]];
      if (first) {
        first = false;
        return { UnprocessedItems: { [tableName]: requests.slice(0, 2) } };
      }
      return {};
    });
    const table = new DynamoDBTable({ region: 'us-east-2', tableName: 'people', partitionKey: 'personId' });
    expect(await table.truncateTable(25, { segments: 1 })).toBe(30);
    expect(dynamo.commandCalls(BatchWriteCommand)).toHaveLength(3);
  });

  it('returns 0 for an empty table, deleting nothing', async () => {
    tableOf(0, 100);
    const table = new DynamoDBTable({ region: 'us-east-2', tableName: 'people', partitionKey: 'personId' });
    expect(await table.truncateTable()).toBe(0);
    expect(dynamo.commandCalls(BatchWriteCommand)).toHaveLength(0);
  });
});

describe('MockTargetPersonTable.truncate()', () => {
  it('truncates through truncateTable(): keys only, in batches, no DeleteItem', async () => {
    tableOf(120, 50);
    dynamo.on(BatchWriteCommand).resolves({});
    const deleted = await new MockTargetPersonTable({ tableName: 'mock-target', region: 'us-east-2' }).truncate();
    expect(deleted).toBe(120);
    expect(dynamo.commandCalls(DeleteCommand)).toHaveLength(0);
    expect(dynamo.commandCalls(ScanCommand).every(call => call.args[0].input.ProjectionExpression === '#pk')).toBe(true);
  });
});
