import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Config } from 'integration-huron-person';
import { SyncPopulation } from '../docker/chunkTypes';
import { MetadataForS3 } from '../src/chunking/metadata/MetadataForS3';
import { StatisticsTable } from '../src/dynamodb/StatisticsTable';

/**
 * MetadataForS3.accumulateMetadataTotals() keeps S3-mode run totals and the end-of-records flag in
 * _metadata.json - where S3-mode decisions (chunk chaining, late-task cancellation, the record
 * safety cutoff, post-processor triggering) read them - using conditional writes so parallel
 * chunker tasks can't lose each other's contributions.
 */
describe('MetadataForS3.accumulateMetadataTotals', () => {
  const bucketName = 'chunks-bucket';
  const chunkDirectory = 'chunks/person-delta/2026-10-24T17:41:39.295Z';
  const metadataKey = `${chunkDirectory}/_metadata.json`;

  const params = (overrides: Record<string, any> = {}) => ({
    bucketName, chunkDirectory, region: 'us-east-2',
    source: 'https://source.example.com/person?buid=U0000144',
    itemsPerChunk: 200,
    bulkReset: false,
    trustPreviousStorage: true,
    syncPopulation: 'person-delta' as SyncPopulation,
    chunkCountDelta: 1,
    totalRecordsDelta: 1,
    ...overrides
  });

  /** In-memory stand-in for _metadata.json that honors IfMatch / IfNoneMatch like S3 does */
  let stored: { body: string, etag: string } | undefined;
  let etagCounter: number;
  let puts: any[];
  let sendSpy: jest.SpyInstance;

  const conflict = (name: string, status: number) => Object.assign(new Error(name), { name, $metadata: { httpStatusCode: status } });

  beforeEach(() => {
    stored = undefined;
    etagCounter = 0;
    puts = [];
    jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'warn').mockImplementation();
    jest.spyOn(global, 'setTimeout').mockImplementation(((fn: () => void) => { fn(); return 0 as any; }) as any);
    sendSpy = jest.spyOn(S3Client.prototype, 'send').mockImplementation(async (command: any) => {
      expect(command.input.Key).toBe(metadataKey);
      if (command instanceof GetObjectCommand) {
        if (!stored) {
          throw Object.assign(new Error('NoSuchKey'), { name: 'NoSuchKey' });
        }
        const body = stored.body;
        return { ETag: stored.etag, Body: { transformToString: async () => body } };
      }
      if (command instanceof PutObjectCommand) {
        const { IfMatch, IfNoneMatch, Body } = command.input;
        puts.push(command.input);
        if (IfNoneMatch === '*' && stored) throw conflict('PreconditionFailed', 412);
        if (IfMatch && (!stored || stored.etag !== IfMatch)) throw conflict('PreconditionFailed', 412);
        stored = { body: Body as string, etag: `"etag-${++etagCounter}"` };
        return {};
      }
      throw new Error(`Unexpected command: ${command.constructor.name}`);
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const storedMetadata = () => JSON.parse(stored!.body);
  const newMetadata = (statisticsTableName?: string) =>
    new MetadataForS3({ config: {} as Config, statisticsTableName });

  it('creates _metadata.json with the run description and this task\'s totals (create-only write)', async () => {
    const totals = await newMetadata().accumulateMetadataTotals(params({ chunkCountDelta: 2, totalRecordsDelta: 350 }));

    expect(totals).toEqual({ chunkCount: 2, totalRecords: 350 });
    expect(puts[0].IfNoneMatch).toBe('*');
    expect(storedMetadata()).toEqual(expect.objectContaining({
      chunkDirectory, itemsPerChunk: 200, syncPopulation: 'person-delta', chunkCount: 2, totalRecords: 350,
      deltaStoragePath: expect.any(String), createdAt: expect.any(String)
    }));
    expect(storedMetadata().partialOrEmptyChunkEncountered).toBeUndefined();
  });

  it('adds to existing totals with a write conditioned on the version read, keeping first-written fields', async () => {
    const metadata = newMetadata();
    await metadata.accumulateMetadataTotals(params({ chunkCountDelta: 2, totalRecordsDelta: 400 }));
    const { createdAt, source } = storedMetadata();

    const totals = await metadata.accumulateMetadataTotals(params({
      chunkCountDelta: 1, totalRecordsDelta: 37, source: 'https://elsewhere.example.com', partialOrEmptyChunkEncountered: true
    }));

    expect(totals).toEqual({ chunkCount: 3, totalRecords: 437 });
    expect(puts[1].IfMatch).toBe('"etag-1"');
    expect(storedMetadata()).toEqual(expect.objectContaining({
      chunkCount: 3, totalRecords: 437, partialOrEmptyChunkEncountered: true, createdAt, source
    }));
  });

  it('never clears the end-of-records flag once set', async () => {
    const metadata = newMetadata();
    await metadata.accumulateMetadataTotals(params({ partialOrEmptyChunkEncountered: true }));
    await metadata.accumulateMetadataTotals(params());

    expect(storedMetadata()).toEqual(expect.objectContaining({ chunkCount: 2, partialOrEmptyChunkEncountered: true }));
  });

  it('makes the end-of-records flag visible to read(), which the chaining decision uses', async () => {
    const metadata = newMetadata();
    await metadata.accumulateMetadataTotals(params({ partialOrEmptyChunkEncountered: true }));

    const read = await metadata.read({ bucketName, chunkDirectory, region: 'us-east-2' });
    expect(read.partialOrEmptyChunkEncountered).toBe(true);
    expect(read.totalRecords).toBe(1);
  });

  it('re-reads and re-applies its contribution after losing a race, so no totals are lost', async () => {
    const metadata = newMetadata();
    await metadata.accumulateMetadataTotals(params({ chunkCountDelta: 1, totalRecordsDelta: 200 }));

    // Another task writes between this task's read and its write, once
    const realSend = sendSpy.getMockImplementation()!;
    let interfered = false;
    sendSpy.mockImplementation(async (command: any) => {
      if (command instanceof PutObjectCommand && !interfered) {
        interfered = true;
        const other = { ...JSON.parse(stored!.body), chunkCount: 2, totalRecords: 400 };
        stored = { body: JSON.stringify(other), etag: `"etag-${++etagCounter}"` };
      }
      return realSend(command);
    });

    const totals = await metadata.accumulateMetadataTotals(params({ chunkCountDelta: 1, totalRecordsDelta: 50 }));

    expect(totals).toEqual({ chunkCount: 3, totalRecords: 450 });
    expect(storedMetadata()).toEqual(expect.objectContaining({ chunkCount: 3, totalRecords: 450 }));
  });

  it('retries when two tasks race to create the file', async () => {
    const realSend = sendSpy.getMockImplementation()!;
    let interfered = false;
    sendSpy.mockImplementation(async (command: any) => {
      if (command instanceof PutObjectCommand && !interfered) {
        interfered = true; // the other task creates the file first
        stored = { body: JSON.stringify({ chunkDirectory, chunkCount: 1, totalRecords: 1 }), etag: `"etag-${++etagCounter}"` };
      }
      return realSend(command);
    });

    const totals = await newMetadata().accumulateMetadataTotals(params());

    expect(totals).toEqual({ chunkCount: 2, totalRecords: 2 });
  });

  it('gives up with an error after repeatedly losing races', async () => {
    sendSpy.mockImplementation(async (command: any) => {
      if (command instanceof GetObjectCommand) {
        return { ETag: '"stale"', Body: { transformToString: async () => '{}' } };
      }
      throw conflict('PreconditionFailed', 412);
    });

    await expect(newMetadata().accumulateMetadataTotals(params())).rejects.toThrow('kept changing');
  });

  it('does not retry other write errors', async () => {
    sendSpy.mockImplementation(async (command: any) => {
      if (command instanceof GetObjectCommand) {
        throw Object.assign(new Error('NoSuchKey'), { name: 'NoSuchKey' });
      }
      throw conflict('AccessDenied', 403);
    });

    await expect(newMetadata().accumulateMetadataTotals(params())).rejects.toThrow('AccessDenied');
    expect(sendSpy.mock.calls.filter(([c]) => c instanceof PutObjectCommand)).toHaveLength(1);
  });

  describe('statistics copy', () => {
    it('does not touch the statistics table when none is configured', async () => {
      const fromTableName = jest.spyOn(StatisticsTable, 'fromTableName');
      await newMetadata().accumulateMetadataTotals(params());
      expect(fromTableName).not.toHaveBeenCalled();
    });

    it('copies the contribution to the statistics table when configured', async () => {
      const addToMetadataTotals = jest.fn().mockResolvedValue({ chunkCount: 1, totalRecords: 1 });
      jest.spyOn(StatisticsTable, 'fromTableName').mockReturnValue({ addToMetadataTotals } as any);

      await newMetadata('statistics-table').accumulateMetadataTotals(params({ partialOrEmptyChunkEncountered: true }));

      expect(addToMetadataTotals).toHaveBeenCalledWith('2026-10-24T17:41:39.295Z', expect.objectContaining({
        chunkCountDelta: 1, totalRecordsDelta: 1, partialOrEmptyChunkEncountered: true
      }));
    });

    it('ignores a failed statistics copy - the totals in _metadata.json still count', async () => {
      jest.spyOn(StatisticsTable, 'fromTableName').mockReturnValue({
        addToMetadataTotals: jest.fn().mockRejectedValue(new Error('table not found'))
      } as any);

      const totals = await newMetadata('statistics-table').accumulateMetadataTotals(params());

      expect(totals).toEqual({ chunkCount: 1, totalRecords: 1 });
      expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('reporting only, sync unaffected'));
    });
  });
});
