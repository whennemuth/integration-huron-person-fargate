import { MetadataFactoryForBootstrap } from '../src/chunking/metadata';
import { StatisticsTable } from '../src/dynamodb/StatisticsTable';

// Mock StatisticsTable so MetadataForDynamoDb instances never touch real DynamoDB
jest.mock('../src/dynamodb/StatisticsTable');

const REAL_TABLE = 'huron-person-fargate-statistics-preview';
const MOCK_TABLE = 'huron-person-fargate-mock-statistics-preview';

describe('MetadataFactoryForBootstrap.resolveMockAwareFlags', () => {
  const chunkDirectory = 'chunks/person-full/2026-05-26T15:00:00.000Z';
  let realReadFlags: jest.Mock;
  let mockReadFlags: jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();
    process.env.PREVIOUS_STORAGE_TYPE = 'dynamodb';
    process.env.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME = 'person-current-state-preview';
    process.env.DYNAMODB_PERSON_HISTORY_TABLE_NAME = 'person-history-preview';
    process.env.DYNAMODB_STATISTICS_TABLE_NAME = REAL_TABLE;
    process.env.DYNAMODB_MOCK_STATISTICS_TABLE_NAME = MOCK_TABLE;

    realReadFlags = jest.fn();
    mockReadFlags = jest.fn();

    (StatisticsTable.fromTableName as jest.Mock) = jest.fn().mockImplementation((tableName: string) => {
      const readFlags = tableName === MOCK_TABLE ? mockReadFlags : realReadFlags;
      return { readFlags, writeFlags: jest.fn(), readMetadata: jest.fn(), writeMetadata: jest.fn() };
    });
  });

  afterEach(() => {
    delete process.env.PREVIOUS_STORAGE_TYPE;
    delete process.env.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME;
    delete process.env.DYNAMODB_PERSON_HISTORY_TABLE_NAME;
    delete process.env.DYNAMODB_STATISTICS_TABLE_NAME;
    delete process.env.DYNAMODB_MOCK_STATISTICS_TABLE_NAME;
  });

  it('resolves to the mock table when FLAGS exist there (mock run)', async () => {
    mockReadFlags.mockResolvedValue({ bulkReset: true, useMockTarget: true });

    const result = await new MetadataFactoryForBootstrap().resolveMockAwareFlags({
      bucketName: 'chunks-bucket', chunkDirectory
    });

    expect(result.flags).toEqual({ bulkReset: true, useMockTarget: true });
    expect(result.statisticsTableName).toBe(MOCK_TABLE);
    expect(mockReadFlags).toHaveBeenCalled();
    expect(realReadFlags).not.toHaveBeenCalled();
  });

  it('falls back to the real table when the mock table has no FLAGS (real run)', async () => {
    mockReadFlags.mockResolvedValue({});
    realReadFlags.mockResolvedValue({ bulkReset: false, syncPopulation: 'person-full' });

    const result = await new MetadataFactoryForBootstrap().resolveMockAwareFlags({
      bucketName: 'chunks-bucket', chunkDirectory
    });

    expect(result.flags).toEqual({ bulkReset: false, syncPopulation: 'person-full' });
    expect(result.statisticsTableName).toBe(REAL_TABLE);
    expect(mockReadFlags).toHaveBeenCalled();
    expect(realReadFlags).toHaveBeenCalled();
  });

  it('falls back to the real table (with empty flags) when neither table has a FLAGS record', async () => {
    mockReadFlags.mockResolvedValue({});
    realReadFlags.mockResolvedValue({});

    const result = await new MetadataFactoryForBootstrap().resolveMockAwareFlags({
      bucketName: 'chunks-bucket', chunkDirectory
    });

    expect(result.flags).toEqual({});
    expect(result.statisticsTableName).toBe(REAL_TABLE);
  });

  it('skips the mock table entirely when DYNAMODB_MOCK_STATISTICS_TABLE_NAME is not configured', async () => {
    delete process.env.DYNAMODB_MOCK_STATISTICS_TABLE_NAME;
    realReadFlags.mockResolvedValue({ bulkReset: false });

    const result = await new MetadataFactoryForBootstrap().resolveMockAwareFlags({
      bucketName: 'chunks-bucket', chunkDirectory
    });

    expect(result.statisticsTableName).toBe(REAL_TABLE);
    expect(mockReadFlags).not.toHaveBeenCalled();
    expect(realReadFlags).toHaveBeenCalled();
  });
});
