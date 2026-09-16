import { MetadataForDynamoDb } from '../src/chunking/metadata';
import { StatisticsTable } from '../src/dynamodb/StatisticsTable';
import { SyncPopulation } from '../docker/chunkTypes';

// Mock StatisticsTable
jest.mock('../src/dynamodb/StatisticsTable');

describe('MetadataForDynamoDb', () => {
  const mockConfig = {
    storage: { type: 'dynamodb' as const },
  };

  let metadata: MetadataForDynamoDb;
  let mockStatisticsTable: jest.Mocked<StatisticsTable>;

  const chunkDirectory = 'chunks/person-full/2026-05-26T15:00:00.000Z';
  const syncRunId = '2026-05-26T15:00:00.000Z'; // Extracted from chunkDirectory

  beforeEach(() => {
    jest.clearAllMocks();
    
    // Create mock StatisticsTable instance
    mockStatisticsTable = {
      writeFlags: jest.fn().mockResolvedValue(undefined),
      readFlags: jest.fn(),
      writeMetadata: jest.fn().mockResolvedValue(undefined),
      readMetadata: jest.fn(),
    } as any;

    // Mock the StatisticsTable.fromTableName() static factory
    (StatisticsTable.fromTableName as jest.Mock) = jest.fn().mockReturnValue(mockStatisticsTable);

    metadata = new MetadataForDynamoDb({ config: mockConfig as any, statisticsTableName: 'test-statistics-table' });
  });

  describe('write', () => {
    it('should write metadata record via writeMetadata', async () => {
      const params = {
        chunkDirectory,
        itemsPerChunk: 500,
        source: 'BU CDM People API',
        target: 'Huron Target System',
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
      };

      await metadata.write(params);

      expect(mockStatisticsTable.writeMetadata).toHaveBeenCalledWith(
        syncRunId,
        expect.objectContaining({
          itemsPerChunk: 500,
          source: 'BU CDM People API',
          target: 'Huron Target System',
          chunkDirectory,
          bulkReset: false,
          trustPreviousStorage: true,
          syncPopulation: SyncPopulation.PersonFull,
        })
      );
    });

    it('should include deltaStoragePath and createdAt', async () => {
      const params = {
        chunkDirectory,
        itemsPerChunk: 500,
        source: 'BU CDM',
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
      };

      await metadata.write(params);

      const call = mockStatisticsTable.writeMetadata.mock.calls[0][1];
      expect(call).toHaveProperty('deltaStoragePath');
      expect(call).toHaveProperty('createdAt');
      expect(call.deltaStoragePath).toContain('deltas');
    });

    it('should handle optional target field', async () => {
      const params = {
        chunkDirectory,
        itemsPerChunk: 500,
        source: 'BU CDM',
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
        // target omitted
      };

      await metadata.write(params);

      const call = mockStatisticsTable.writeMetadata.mock.calls[0][1];
      expect(call).not.toHaveProperty('target');
    });

    it('should include runFailed fields when provided', async () => {
      const params = {
        chunkDirectory,
        itemsPerChunk: 500,
        source: 'BU CDM',
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
        runFailed: true,
        runFailureMessage: 'Test failure',
        runFailureTimestamp: '2026-05-26T15:30:00.000Z',
      };

      await metadata.write(params);

      const call = mockStatisticsTable.writeMetadata.mock.calls[0][1];
      expect(call.runFailed).toBe(true);
      expect(call.runFailureMessage).toBe('Test failure');
      expect(call.runFailureTimestamp).toBe('2026-05-26T15:30:00.000Z');
    });
  });

  describe('writeFlags', () => {
    it('should write flags record via writeFlags', async () => {
      const params = {
        chunkDirectory,
        bulkReset: true,
        trustPreviousStorage: false,
        syncPopulation: SyncPopulation.PersonFull,
      };

      await metadata.writeFlags(params);

      expect(mockStatisticsTable.writeFlags).toHaveBeenCalledWith(
        syncRunId,
        expect.objectContaining({
          bulkReset: true,
          trustPreviousStorage: false,
          syncPopulation: SyncPopulation.PersonFull,
        })
      );
    });

    it('should include standard flag fields', async () => {
      const params = {
        chunkDirectory,
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonDelta,
      };

      await metadata.writeFlags(params);

      const call = mockStatisticsTable.writeFlags.mock.calls[0][1];
      expect(call.bulkReset).toBe(false);
      expect(call.trustPreviousStorage).toBe(true);
      expect(call.syncPopulation).toBe(SyncPopulation.PersonDelta);
    });

    it('should include personRecordProcessorCustomizations when present', async () => {
      const params = {
        chunkDirectory,
        bulkReset: true,
        trustPreviousStorage: false,
        syncPopulation: SyncPopulation.PersonFull,
        personRecordProcessorCustomizations: 'ORG_COMPARISON_LOGGING',
      };

      await metadata.writeFlags(params);

      const call = mockStatisticsTable.writeFlags.mock.calls[0][1];
      expect(call.personRecordProcessorCustomizations).toBe('ORG_COMPARISON_LOGGING');
    });
  });

  describe('read', () => {
    it('should read metadata record by syncRunId', async () => {
      const mockMetadata = {
        itemsPerChunk: 500,
        source: 'BU CDM',
        target: 'Huron',
        chunkDirectory,
        deltaStoragePath: 'delta-storage/PersonFull',
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
        createdAt: '2026-05-26T15:00:00.000Z',
      };

      mockStatisticsTable.readMetadata.mockResolvedValue(mockMetadata);

      const result = await metadata.read({ chunkDirectory });

      expect(mockStatisticsTable.readMetadata).toHaveBeenCalledWith(
        syncRunId
      );
      expect(result).toEqual(expect.objectContaining({
        itemsPerChunk: 500,
        source: 'BU CDM',
        chunkDirectory,
      }));
    });

    it('should return empty object if metadata not found', async () => {
      mockStatisticsTable.readMetadata.mockResolvedValue(undefined);

      const result = await metadata.read({ chunkDirectory });

      expect(result).toEqual({});
    });
  });

  describe('readFlags', () => {
    it('should read flags record by syncRunId', async () => {
      const mockFlags = {
        bulkReset: true,
        trustPreviousStorage: false,
        syncPopulation: SyncPopulation.PersonFull,
      };

      mockStatisticsTable.readFlags.mockResolvedValue(mockFlags);

      const result = await metadata.readFlags({ chunkDirectory });

      expect(mockStatisticsTable.readFlags).toHaveBeenCalledWith(
        syncRunId
      );
      expect(result).toEqual(expect.objectContaining({
        bulkReset: true,
        trustPreviousStorage: false,
        syncPopulation: SyncPopulation.PersonFull,
      }));
    });

    it('should return empty object if flags not found', async () => {
      mockStatisticsTable.readFlags.mockResolvedValue(undefined);

      const result = await metadata.readFlags({ chunkDirectory });

      expect(result).toEqual({});
    });
  });

  describe('markRunFailed', () => {
    it('should write terminal error record', async () => {
      const params = {
        chunkDirectory,
        errorMessage: 'Network timeout',
      };

      await metadata.markRunFailed(params);

      expect(mockStatisticsTable.writeMetadata).toHaveBeenCalledWith(
        syncRunId,
        expect.objectContaining({
          eventType: 'TERMINAL_ERROR',
          errorMessage: 'Network timeout',
          chunkDirectory,
          stage: 'chunking',
        })
      );
    });

    it('should include errorTimestamp', async () => {
      const params = {
        chunkDirectory,
        errorMessage: 'Test error',
      };

      await metadata.markRunFailed(params);

      const call = mockStatisticsTable.writeMetadata.mock.calls[0][1];
      expect(call).toHaveProperty('errorTimestamp');
    });
  });

  describe('terminalErrorExists', () => {
    it('should return true when terminal error exists', async () => {
      mockStatisticsTable.readMetadata.mockResolvedValue({
        eventType: 'TERMINAL_ERROR',
        errorMessage: 'Test error',
      });

      const result = await metadata.terminalErrorExists({ chunkDirectory });

      expect(result).toBe(true);
      expect(mockStatisticsTable.readMetadata).toHaveBeenCalledWith(
        syncRunId
      );
    });

    it('should return false when terminal error does not exist', async () => {
      mockStatisticsTable.readMetadata.mockResolvedValue(undefined);

      const result = await metadata.terminalErrorExists({ chunkDirectory });

      expect(result).toBe(false);
    });
  });

  describe('isRunFailed', () => {
    it('should return true when flags indicate run failed', async () => {
      mockStatisticsTable.readFlags.mockResolvedValue({
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
        runFailed: true,
      });

      const result = await metadata.isRunFailed({ chunkDirectory });

      expect(result).toBe(true);
    });

    it('should return false when flags indicate run succeeded', async () => {
      mockStatisticsTable.readFlags.mockResolvedValue({
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
        runFailed: false,
      });

      const result = await metadata.isRunFailed({ chunkDirectory });

      expect(result).toBe(false);
    });

    it('should return false when runFailed field is undefined', async () => {
      mockStatisticsTable.readFlags.mockResolvedValue({
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
      });

      const result = await metadata.isRunFailed({ chunkDirectory });

      expect(result).toBe(false);
    });
  });

  describe('readTerminalError', () => {
    it('should read terminal error details', async () => {
      const mockError = {
        eventType: 'TERMINAL_ERROR',
        errorTimestamp: '2026-05-26T15:30:00.000Z',
        errorMessage: 'Network failure',
        stage: 'chunking',
        chunkDirectory,
      };

      mockStatisticsTable.readMetadata.mockResolvedValue(mockError);

      const result = await metadata.readTerminalError({ chunkDirectory });

      expect(result).toEqual(expect.objectContaining({
        errorTimestamp: '2026-05-26T15:30:00.000Z',
        errorMessage: 'Network failure',
        stage: 'chunking',
      }));
    });

    it('should return undefined when no terminal error exists', async () => {
      mockStatisticsTable.readMetadata.mockResolvedValue(undefined);

      const result = await metadata.readTerminalError({ chunkDirectory });

      expect(result).toBeUndefined();
    });
  });

  describe('syncRunId extraction', () => {
    it('should extract syncRunId from chunkDirectory', async () => {
      const params = {
        chunkDirectory: 'chunks/PersonFull/2026-12-25T10:30:45.678Z',
        bulkReset: false,
        trustPreviousStorage: true,
        syncPopulation: SyncPopulation.PersonFull,
      };

      await metadata.writeFlags(params);

      const call = mockStatisticsTable.writeFlags.mock.calls[0];
      expect(call[0]).toBe('2026-12-25T10:30:45.678Z');
    });

    it('should handle different population types', async () => {
      const params = {
        chunkDirectory: 'chunks/PersonDelta/2026-01-01T00:00:00.000Z',
        bulkReset: true,
        trustPreviousStorage: false,
        syncPopulation: SyncPopulation.PersonDelta,
      };

      await metadata.writeFlags(params);

      const call = mockStatisticsTable.writeFlags.mock.calls[0];
      expect(call[0]).toBe('2026-01-01T00:00:00.000Z');
    });
  });
});
