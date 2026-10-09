/**
 * Tests that persons soft-deleted by the merger's deferred delete handling are recorded as such
 * in the baseline (PersonCurrentStateTable in DynamoDB mode, previous-input.ndjson in S3 mode),
 * so that subsequent full syncs don't select them for deletion again.
 */

import { TargetPersonDeleteType, HuronPersonDataTarget, ReadPerson } from 'integration-huron-person';
import { FieldSet, Status } from 'integration-core';
import { mockClient } from 'aws-sdk-client-mock';
import { S3, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';
import { sdkStreamMixin } from '@smithy/util-stream';
import { Readable } from 'stream';
import { DELETED_HASH_PREFIX, isDeletedHash, toDeletedHash } from '../src/merging/DeletedHashMarker';
import { DeferredDeleteHandlerForDynamoDB } from '../src/merging/DeferredDeleteHandlerForDynamoDB';
import { DeferredDeleteHandlerForS3 } from '../src/merging/DeferredDeleteHandlerForS3';
import { ChunkPopulationReader } from '../src/merging/ChunkPopulationReader';
import { PersonCurrentStateTable } from '../src/dynamodb/PersonCurrentStateTable';
import { PersonHistoryTable } from '../src/dynamodb/PersonHistoryTable';

const mockPushAll = jest.fn();
HuronPersonDataTarget.prototype.pushAll = mockPushAll;

const mockReadPersonBySourceIdentifier = jest.fn();
ReadPerson.prototype.readPersonBySourceIdentifier = mockReadPersonBySourceIdentifier;

// PersonCurrentStateTable.findPreviousPersonState dynamically imports './PersonHistoryTable.js',
// which only exists in compiled output - stand it in for the TypeScript module under jest.
jest.mock('../src/dynamodb/PersonHistoryTable.js', () => jest.requireActual('../src/dynamodb/PersonHistoryTable'), { virtual: true });

const s3Mock = mockClient(S3);
const dynamoMock = mockClient(DynamoDBDocumentClient);

const mockConfig = {
  executionMode: 'people',
  dataSource: {
    people: { endpointConfig: { baseUrl: 'https://api.example.com', apiKey: 'test-key' }, fetchPath: '/persons' },
    idpName: 'test-idp'
  },
  dataTarget: {
    endpointConfig: {
      baseUrl: 'https://target.example.com', authMethod: 'externalToken',
      loginSvcPath: '/auth/token', username: 'user', password: 'pass'
    },
    personsPath: '/api/v1/persons/batch',
    organizationsPath: '/api/v1/organizations',
    personDeleteType: TargetPersonDeleteType.SOFT
  },
  integration: { clientId: 'test-client', batchSize: 10, timeout: 5000 },
  storage: { type: 'file', config: {} }
} as any;

const mockCache = { get: jest.fn(), set: jest.fn() } as any;

const mockNdjsonResponse = (fieldSets: FieldSet[]) => {
  const ndjson = fieldSets.map(fs => JSON.stringify(fs)).join('\n');
  return { Body: sdkStreamMixin(Readable.from([ndjson])) };
};

const sourceIdentifierOf = (fs: FieldSet) => fs.fieldValues.find((fv: any) => fv.sourceIdentifier)?.sourceIdentifier;

describe('DeletedHashMarker', () => {
  it('marks a hash as deleted, idempotently', () => {
    const marked = toDeletedHash('abc');
    expect(marked).toBe(`${DELETED_HASH_PREFIX}abc`);
    expect(toDeletedHash(marked)).toBe(marked);
  });

  it('recognizes only marked hashes', () => {
    expect(isDeletedHash(toDeletedHash('abc'))).toBe(true);
    expect(isDeletedHash('abc')).toBe(false);
    expect(isDeletedHash(undefined)).toBe(false);
  });
});

describe('PersonCurrentStateTable', () => {
  beforeEach(() => dynamoMock.reset());

  it('markDeleted overwrites records with a marked hash, the deleting syncRunId and deletedAt', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({});
    const table = PersonCurrentStateTable.fromTableName('state-table', 'us-east-2');

    await table.markDeleted([{ personId: 'U00000001', hash: 'h1' }], 'run-2');

    const requests = dynamoMock.commandCalls(BatchWriteCommand)[0].args[0].input.RequestItems!['state-table'];
    const item = (requests[0] as any).PutRequest.Item;
    expect(item.personId).toBe('U00000001');
    expect(item.hash).toBe(toDeletedHash('h1'));
    expect(item.syncRunId).toBe('run-2');
    expect(item.deletedAt).toBeTruthy();
  });

  it('rollback restores the deleted marker when the prior history entry is DELETED', async () => {
    const table = PersonCurrentStateTable.fromTableName('state-table', 'us-east-2');
    const historyTable = {
      getPersonHistory: jest.fn().mockResolvedValue([
        { personId: 'U00000001', syncRunId: 'run-1', hash: 'h1', changeType: 'DELETED' },
        { personId: 'U00000001', syncRunId: 'run-2', hash: 'h2', changeType: 'UPDATED' },
      ])
    };

    const previous = await (table as any).findPreviousPersonState('U00000001', 'run-2', historyTable);

    expect(previous).toEqual({ hash: toDeletedHash('h1'), syncRunId: 'run-1' });
  });
});

describe('DeferredDeleteHandlerForDynamoDB', () => {
  const syncRunId = '2026-10-02T00:00:00.000Z';
  let stateTable: { getAllPersons: jest.Mock; markDeleted: jest.Mock };
  let historyTable: { batchWriteHistory: jest.Mock };

  const createHandler = () => new DeferredDeleteHandlerForDynamoDB({
    bucketName: 'bucket',
    chunkDirectory: `chunks/person-full/${syncRunId}`,
    personCurrentStateTableName: 'state-table',
    personHistoryTableName: 'history-table',
    syncRunId,
    primaryKeyFieldNames: ['sourceIdentifier'],
    region: 'us-east-2',
    config: mockConfig,
    cache: mockCache,
  });

  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    process.env.PERSON_DELETE_TYPE = 'soft';

    stateTable = {
      getAllPersons: jest.fn().mockResolvedValue([
        { personId: 'U00000001', hash: 'h1', syncRunId: 'run-1' },                  // still in source
        { personId: 'U00000002', hash: 'h2', syncRunId: 'run-1' },                  // newly removed
        { personId: 'U00000003', hash: toDeletedHash('h3'), syncRunId: 'run-1' },   // removed and already deleted
      ]),
      markDeleted: jest.fn().mockResolvedValue(undefined)
    };
    historyTable = { batchWriteHistory: jest.fn().mockResolvedValue(undefined) };
    jest.spyOn(PersonCurrentStateTable, 'fromTableName').mockReturnValue(stateTable as any);
    jest.spyOn(PersonHistoryTable, 'fromTableName').mockReturnValue(historyTable as any);
    jest.spyOn(ChunkPopulationReader.prototype, 'getCurrentPopulation')
      .mockResolvedValue([{ fieldValues: [{ sourceIdentifier: 'U00000001' }] }]);
  });

  afterEach(() => {
    jest.restoreAllMocks();
    delete process.env.PERSON_DELETE_TYPE;
  });

  it('excludes already soft-deleted persons from the removal candidates', async () => {
    const removed = await createHandler().getRemovedRecords();

    expect(removed.map(sourceIdentifierOf)).toEqual(['U00000002']);
  });

  it('marks successes (primaryKey [{ hrn }]) as deleted and writes DELETED history', async () => {
    mockReadPersonBySourceIdentifier.mockResolvedValue([{ hrn: 'hrn:person:2' }]);
    mockPushAll.mockResolvedValue({ successes: [{ status: Status.SUCCESS, primaryKey: [{ hrn: 'hrn:person:2' }] }], failures: [] });

    const result = await createHandler().processDeletes();

    expect(result.deletedCount).toBe(1);
    expect(stateTable.markDeleted).toHaveBeenCalledWith([{ personId: 'U00000002', hash: 'h2' }], syncRunId);
    expect(historyTable.batchWriteHistory).toHaveBeenCalledWith([
      { personId: 'U00000002', syncRunId, hash: 'h2', changeType: 'DELETED', previousHash: 'h2' }
    ]);
  });

  it('does not mark records whose soft-delete failed', async () => {
    mockReadPersonBySourceIdentifier.mockResolvedValue([{ hrn: 'hrn:person:2' }]);
    mockPushAll.mockResolvedValue({ successes: [], failures: [{ status: Status.FAILURE, primaryKey: [{ hrn: 'hrn:person:2' }] }] });

    await createHandler().processDeletes();

    expect(stateTable.markDeleted).not.toHaveBeenCalled();
    expect(historyTable.batchWriteHistory).not.toHaveBeenCalled();
  });
});

describe('DeferredDeleteHandlerForS3', () => {
  const bucketName = 'bucket';
  const baselineNdjsonPath = 'delta-storage/previous-input.ndjson';
  const mergedNdjsonPath = 'deltas/person-full/2026-10-02/merged.ndjson';

  const baseline: FieldSet[] = [
    { fieldValues: [{ sourceIdentifier: 'U00000001' }], hash: 'h1' },
    { fieldValues: [{ sourceIdentifier: 'U00000002' }], hash: 'h2' },
    { fieldValues: [{ sourceIdentifier: 'U00000003' }], hash: toDeletedHash('h3') },
  ];

  const createHandler = () => new DeferredDeleteHandlerForS3({
    bucketName, baselineNdjsonPath, mergedNdjsonPath,
    primaryKeyFieldNames: ['sourceIdentifier'],
    region: 'us-east-2',
    config: mockConfig,
    cache: mockCache,
  });

  beforeEach(() => {
    jest.clearAllMocks();
    s3Mock.reset();
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    process.env.PERSON_DELETE_TYPE = 'soft';

    // Return a fresh stream per call, since the baseline is read twice (detection, then marking)
    s3Mock.on(GetObjectCommand, { Bucket: bucketName, Key: baselineNdjsonPath })
      .callsFake(() => mockNdjsonResponse(baseline));
    s3Mock.on(GetObjectCommand, { Bucket: bucketName, Key: mergedNdjsonPath })
      .callsFake(() => mockNdjsonResponse([{ fieldValues: [{ sourceIdentifier: 'U00000001' }], hash: 'h1' }]));
    s3Mock.on(PutObjectCommand).resolves({});
  });

  afterEach(() => {
    jest.restoreAllMocks();
    delete process.env.PERSON_DELETE_TYPE;
  });

  it('excludes already soft-deleted persons from the removal candidates', async () => {
    const removed = await createHandler().getRemovedRecords();

    expect(removed.map(sourceIdentifierOf)).toEqual(['U00000002']);
  });

  it('rewrites the baseline with successfully soft-deleted persons marked, leaving all others intact', async () => {
    mockReadPersonBySourceIdentifier.mockResolvedValue([{ hrn: 'hrn:person:2' }]);
    mockPushAll.mockResolvedValue({ successes: [{ status: Status.SUCCESS, primaryKey: [{ hrn: 'hrn:person:2' }] }], failures: [] });

    await createHandler().processDeletes();

    const putCalls = s3Mock.commandCalls(PutObjectCommand);
    expect(putCalls).toHaveLength(1);
    const { Key, Body } = putCalls[0].args[0].input;
    expect(Key).toBe(baselineNdjsonPath);
    const written = `${Body}`.trim().split('\n').map(line => JSON.parse(line) as FieldSet);
    expect(written.map(fs => [sourceIdentifierOf(fs), fs.hash])).toEqual([
      ['U00000001', 'h1'],
      ['U00000002', toDeletedHash('h2')],
      ['U00000003', toDeletedHash('h3')],
    ]);
  });

  it('does not rewrite the baseline when every soft-delete failed', async () => {
    mockReadPersonBySourceIdentifier.mockResolvedValue([{ hrn: 'hrn:person:2' }]);
    mockPushAll.mockResolvedValue({ successes: [], failures: [{ status: Status.FAILURE, primaryKey: [{ hrn: 'hrn:person:2' }] }] });

    await createHandler().processDeletes();

    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(0);
  });
});
