import { MetadataForS3 } from '../src/chunking/metadata/MetadataForS3';
import { decodeCursor, encodeCursor, ReportingError, RunReporting } from '../src/reporting/RunReporting';

jest.mock('../src/chunking/metadata/MetadataForS3');

describe('RunReporting', () => {
  const syncRunId = '2026-10-01T00:00:00.000Z';
  const chunkDirectory = `chunks/person-full/${syncRunId}`;

  const statisticsTable = () => ({
    getUniqueIntegrationTimestamps: jest.fn().mockResolvedValue(['a', 'b']),
    readRunSummary: jest.fn().mockResolvedValue(undefined),
    readMetadata: jest.fn().mockResolvedValue(undefined),
    readFlags: jest.fn().mockResolvedValue(undefined),
    readTerminalError: jest.fn().mockResolvedValue(undefined),
    getAllChunkStatistics: jest.fn().mockResolvedValue([]),
    getRunErrorsPage: jest.fn().mockResolvedValue({ items: [] })
  });
  const historyTable = () => ({
    getChangesInSyncRunPage: jest.fn().mockResolvedValue({ items: [] })
  });

  const reporting = (storageMode: string, stats = statisticsTable(), history: any = historyTable()) => ({
    stats, history,
    reporting: new RunReporting(
      { landscape: 'mock', storageMode, statisticsTableName: 'stats', chunksBucket: 'bucket', region: 'us-east-2' },
      { statisticsTable: stats as any, personHistoryTable: history }
    )
  });

  afterEach(() => jest.clearAllMocks());

  it('reports capabilities per storage mode', () => {
    expect(reporting('dynamodb').reporting.getCapabilities()).toEqual({ landscape: 'mock', storageMode: 'dynamodb', changesSupported: true });
    expect(reporting('s3').reporting.getCapabilities().changesSupported).toBe(false);
  });

  it('lists runs from the distinct-run index', async () => {
    expect(await reporting('dynamodb').reporting.listRuns()).toEqual(['a', 'b']);
  });

  describe('getRunReport', () => {
    it('returns the RUN_SUMMARY when the post-processor wrote one', async () => {
      const { reporting: r, stats } = reporting('dynamodb');
      const summary = { syncRunId, status: 'SUCCEEDED' };
      stats.readRunSummary.mockResolvedValue(summary);

      expect(await r.getRunReport(syncRunId)).toEqual({ syncRunId, status: 'SUCCEEDED', summary });
      expect(stats.readMetadata).not.toHaveBeenCalled();
    });

    it('is FAILED in DynamoDB mode when the table holds a TERMINAL_ERROR', async () => {
      const { reporting: r, stats } = reporting('dynamodb');
      stats.readMetadata.mockResolvedValue({ chunkDirectory });
      stats.readFlags.mockResolvedValue({ bulkReset: false });
      stats.readTerminalError.mockResolvedValue({ errorMessage: 'boom' });

      expect(await r.getRunReport(syncRunId)).toEqual({
        syncRunId, status: 'FAILED', metadata: { chunkDirectory }, flags: { bulkReset: false }, terminalError: { errorMessage: 'boom' }
      });
    });

    it('is INCOMPLETE when the run has records but neither a summary nor a terminal error', async () => {
      const { reporting: r, stats } = reporting('dynamodb');
      stats.readMetadata.mockResolvedValue({ chunkDirectory });

      expect((await r.getRunReport(syncRunId))?.status).toBe('INCOMPLETE');
    });

    it('reads flags and the terminal error from S3 files in S3 mode', async () => {
      const readFlags = jest.fn().mockResolvedValue({ bulkReset: true });
      const readTerminalError = jest.fn().mockResolvedValue({ errorMessage: 'boom' });
      (MetadataForS3 as unknown as jest.Mock).mockImplementation(() => ({ readFlags, readTerminalError }));
      const { reporting: r, stats } = reporting('s3');
      stats.readMetadata.mockResolvedValue({ chunkDirectory });

      const report = await r.getRunReport(syncRunId);

      expect(readFlags).toHaveBeenCalledWith({ bucketName: 'bucket', chunkDirectory, region: 'us-east-2' });
      expect(stats.readFlags).not.toHaveBeenCalled();
      expect(report).toMatchObject({ status: 'FAILED', flags: { bulkReset: true } });
    });

    it('returns undefined for a run with no records', async () => {
      expect(await reporting('dynamodb').reporting.getRunReport(syncRunId)).toBeUndefined();
    });
  });

  describe('getRunErrors', () => {
    it('returns a page with an opaque cursor that continues the same listing', async () => {
      const { reporting: r, stats } = reporting('dynamodb');
      const lastEvaluatedKey = { integrationTimestamp: syncRunId, eventType: 'ERROR:500:t' };
      stats.getRunErrorsPage.mockResolvedValueOnce({ items: [{ e: 1 }], lastEvaluatedKey });

      const first = await r.getRunErrors(syncRunId, { statusCode: 500, limit: 1 });
      await r.getRunErrors(syncRunId, { statusCode: 500, limit: 1, cursor: first.cursor });

      expect(first.items).toEqual([{ e: 1 }]);
      expect(stats.getRunErrorsPage).toHaveBeenNthCalledWith(1, syncRunId, { statusCode: 500, limit: 1, startKey: undefined });
      expect(stats.getRunErrorsPage).toHaveBeenNthCalledWith(2, syncRunId, { statusCode: 500, limit: 1, startKey: lastEvaluatedKey });
    });

    it('rejects a cursor from another run', async () => {
      const cursor = encodeCursor({ integrationTimestamp: 'other-run', eventType: 'ERROR:500:t' });
      await expect(reporting('dynamodb').reporting.getRunErrors(syncRunId, { cursor }))
        .rejects.toMatchObject({ code: 'INVALID_CURSOR' });
    });

    it.each([0, 501, 2.5])('rejects limit %p', async (limit) => {
      await expect(reporting('dynamodb').reporting.getRunErrors(syncRunId, { limit }))
        .rejects.toMatchObject({ code: 'INVALID_LIMIT' });
    });

    it('defaults the page size', async () => {
      const { reporting: r, stats } = reporting('dynamodb');
      await r.getRunErrors(syncRunId);
      expect(stats.getRunErrorsPage).toHaveBeenCalledWith(syncRunId, expect.objectContaining({ limit: 100 }));
    });
  });

  describe('getRunChanges', () => {
    it('pages through a run\'s changes of one type in DynamoDB mode', async () => {
      const { reporting: r, history } = reporting('dynamodb');
      const lastEvaluatedKey = { personId: 'U1', syncRunId, changeType: 'DELETED' };
      history.getChangesInSyncRunPage.mockResolvedValueOnce({ items: [{ personId: 'U1' }], lastEvaluatedKey });

      const page = await r.getRunChanges(syncRunId, 'DELETED', { limit: 1 });

      expect(page.items).toEqual([{ personId: 'U1' }]);
      expect(decodeCursor(page.cursor!, { syncRunId, changeType: 'DELETED' })).toEqual(lastEvaluatedKey);
    });

    it('rejects a cursor for another change type', async () => {
      const cursor = encodeCursor({ personId: 'U1', syncRunId, changeType: 'NEW' });
      await expect(reporting('dynamodb').reporting.getRunChanges(syncRunId, 'DELETED', { cursor }))
        .rejects.toMatchObject({ code: 'INVALID_CURSOR' });
    });

    it('is unsupported in S3 mode', async () => {
      await expect(reporting('s3').reporting.getRunChanges(syncRunId, 'NEW'))
        .rejects.toEqual(expect.any(ReportingError));
      await expect(reporting('s3').reporting.getRunChanges(syncRunId, 'NEW'))
        .rejects.toMatchObject({ code: 'UNSUPPORTED_IN_STORAGE_MODE' });
    });
  });

  it('rejects a cursor that is not base64url JSON', () => {
    expect(() => decodeCursor('not-a-cursor!!', {})).toThrow(ReportingError);
  });
});
