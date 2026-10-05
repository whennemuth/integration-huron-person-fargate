/**
 * Tests the safeguards that keep PersonCurrentStateTable and PersonHistoryTable consistent with
 * each other, so that no history entry repeats the hash of the entry before it (other than a
 * reactivation) and no DELETED entry follows another DELETED entry:
 * - throttled (unprocessed) batch writes/reads are retried rather than silently dropped
 * - rollback (IntegrationRunPruner) restores state from history even though the target run's
 *   history entries are deleted first, and refuses to prune a run that later runs built on
 */

import { mockClient } from 'aws-sdk-client-mock';
import { BatchGetCommand, BatchWriteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoDBTable } from '../src/dynamodb/DynamoDBTable';
import { PersonCurrentStateTable } from '../src/dynamodb/PersonCurrentStateTable';
import { PersonHistoryTable } from '../src/dynamodb/PersonHistoryTable';
import { StatisticsTable } from '../src/dynamodb/StatisticsTable';
import { IntegrationRunPruner } from '../src/dynamodb/IntegrationRun';
import { toDeletedHash } from '../src/merging/DeletedHashMarker';
import { IContext } from '../context/IContext';

// PersonCurrentStateTable.findPreviousPersonState dynamically imports './PersonHistoryTable.js',
// which only exists in compiled output - stand it in for the TypeScript module under jest.
jest.mock('../src/dynamodb/PersonHistoryTable.js', () => jest.requireActual('../src/dynamodb/PersonHistoryTable'), { virtual: true });

const dynamoMock = mockClient(DynamoDBDocumentClient);

describe('DynamoDBTable.batchWrite', () => {
  const table = new DynamoDBTable({ region: 'us-east-2', tableName: 'state-table', partitionKey: 'personId' });

  beforeEach(() => dynamoMock.reset());

  it('resends unprocessed requests until none remain', async () => {
    dynamoMock.on(BatchWriteCommand)
      .resolvesOnce({ UnprocessedItems: { 'state-table': [{ PutRequest: { Item: { personId: 'U0000002' } } }] } })
      .resolves({});

    await table.batchWrite({ items: [{ personId: 'U0000001' }, { personId: 'U0000002' }] });

    const calls = dynamoMock.commandCalls(BatchWriteCommand);
    expect(calls).toHaveLength(2);
    expect(calls[1].args[0].input.RequestItems!['state-table']).toEqual([{ PutRequest: { Item: { personId: 'U0000002' } } }]);
  });

  it('throws instead of silently dropping requests that stay unprocessed', async () => {
    dynamoMock.on(BatchWriteCommand).callsFake(input => ({ UnprocessedItems: input.RequestItems }));

    await expect(table.batchWrite({ items: [{ personId: 'U0000001' }] }))
      .rejects.toThrow(/state-table: 1 request\(s\) still unprocessed after 5 attempts/);
    expect(dynamoMock.commandCalls(BatchWriteCommand)).toHaveLength(5);
  });
});

describe('PersonCurrentStateTable', () => {
  beforeEach(() => dynamoMock.reset());

  it('batchGetPersonState retries unprocessed keys', async () => {
    dynamoMock.on(BatchGetCommand)
      .resolvesOnce({
        Responses: { 'state-table': [] },
        UnprocessedKeys: { 'state-table': { Keys: [{ personId: 'U0000001' }] } }
      })
      .resolves({ Responses: { 'state-table': [{ personId: 'U0000001', hash: 'h1', syncRunId: 'run-1' }] } });
    const table = PersonCurrentStateTable.fromTableName('state-table', 'us-east-2');

    const states = await table.batchGetPersonState(['U0000001']);

    expect(states.get('U0000001')?.hash).toBe('h1');
  });

  describe('findPreviousPersonState', () => {
    const table = PersonCurrentStateTable.fromTableName('state-table', 'us-east-2');
    const historyOf = (entries: any[]) => ({ getPersonHistory: jest.fn().mockResolvedValue(entries) });

    it('finds the latest entry before the target run when the target run\'s entry was already deleted', async () => {
      const history = historyOf([
        { personId: 'U0000001', syncRunId: '2026-10-01T00:00:00.000Z', hash: 'h1', changeType: 'NEW' },
        { personId: 'U0000001', syncRunId: '2026-10-02T00:00:00.000Z', hash: 'h2', changeType: 'UPDATED' },
        // 2026-10-03 (the target run's) entry has already been deleted by IntegrationRunPruner
      ]);

      const previous = await (table as any).findPreviousPersonState('U0000001', '2026-10-03T00:00:00.000Z', history);

      expect(previous).toEqual({ hash: 'h2', syncRunId: '2026-10-02T00:00:00.000Z' });
    });

    it('restores the deleted marker when the latest earlier entry is DELETED', async () => {
      const history = historyOf([
        { personId: 'U0000001', syncRunId: '2026-10-01T00:00:00.000Z', hash: 'h1', changeType: 'DELETED' },
      ]);

      const previous = await (table as any).findPreviousPersonState('U0000001', '2026-10-03T00:00:00.000Z', history);

      expect(previous).toEqual({ hash: toDeletedHash('h1'), syncRunId: '2026-10-01T00:00:00.000Z' });
    });

    it('returns undefined when there is no entry before the target run', async () => {
      const history = historyOf([]);

      const previous = await (table as any).findPreviousPersonState('U0000001', '2026-10-03T00:00:00.000Z', history);

      expect(previous).toBeUndefined();
    });
  });
});

describe('IntegrationRunPruner', () => {
  const target = '2026-10-03T00:00:00.000Z';
  const context = { STACK_ID: 'stack', REGION: 'us-east-2', TAGS: { Landscape: 'test' } } as unknown as IContext;

  let deleteStatistics: jest.SpyInstance;
  let deleteHistory: jest.SpyInstance;
  let restoreState: jest.SpyInstance;

  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(PersonHistoryTable.prototype, 'getChangesInSyncRun').mockResolvedValue([
      { personId: 'U0000001', syncRunId: target, hash: 'h1', changeType: 'UPDATED' } as any,
      { personId: 'U0000002', syncRunId: target, hash: 'h2', changeType: 'NEW' } as any,
    ]);
    deleteStatistics = jest.spyOn(StatisticsTable.prototype, 'deleteByPartitionKey').mockResolvedValue(0);
    deleteHistory = jest.spyOn(PersonHistoryTable.prototype, 'deleteByPartitionKey').mockResolvedValue(2);
    restoreState = jest.spyOn(PersonCurrentStateTable.prototype, 'deleteByPartitionKeyAndRestore')
      .mockResolvedValue({ deletedCount: 1, restoredCount: 1 });
  });

  afterEach(() => jest.restoreAllMocks());

  it('prunes a run that is still the latest to have touched each of its persons', async () => {
    jest.spyOn(PersonCurrentStateTable.prototype, 'batchGetPersonState').mockResolvedValue(new Map([
      ['U0000001', { personId: 'U0000001', hash: 'h1', syncRunId: target }],
      ['U0000002', { personId: 'U0000002', hash: 'h2', syncRunId: target }],
    ]));

    await new IntegrationRunPruner(target, context).prune();

    expect(deleteHistory).toHaveBeenCalledWith(target);
    expect(restoreState).toHaveBeenCalled();
  });

  it('refuses, deleting nothing, when a later run changed one of the run\'s persons', async () => {
    jest.spyOn(PersonCurrentStateTable.prototype, 'batchGetPersonState').mockResolvedValue(new Map([
      ['U0000001', { personId: 'U0000001', hash: 'h1', syncRunId: target }],
      ['U0000002', { personId: 'U0000002', hash: 'h3', syncRunId: '2026-10-04T00:00:00.000Z' }],
    ]));

    await expect(new IntegrationRunPruner(target, context).prune())
      .rejects.toThrow(/1 of its person\(s\) were changed by a later run \(e\.g\. U0000002 in 2026-10-04T00:00:00\.000Z\)/);
    expect(deleteStatistics).not.toHaveBeenCalled();
    expect(deleteHistory).not.toHaveBeenCalled();
    expect(restoreState).not.toHaveBeenCalled();
  });
});
