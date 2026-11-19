import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Config } from 'integration-huron-person';
import { SyncPopulation } from '../docker/chunkTypes';
import { MetadataForS3 } from '../src/chunking/metadata/MetadataForS3';
import * as Utils from '../src/Utils';

/**
 * MetadataForS3.writeFlags() honors replace as MetadataForDynamoDb.writeFlags() does: every chunker task of a run
 * calls it, and unless replace is true the first task's _flags.json stands.
 */
describe('MetadataForS3.writeFlags', () => {
  const config = {} as Config;
  const bucketName = 'chunks-bucket';
  const chunkDirectory = 'chunks/person-full/2026-01-01T00:00:00.000Z';
  const flagsKey = `${chunkDirectory}/_flags.json`;
  const params = (overrides: Record<string, any> = {}) => ({
    bucketName, chunkDirectory, region: 'us-east-2',
    bulkReset: false, trustPreviousStorage: true, syncPopulation: SyncPopulation.PersonFull,
    ...overrides
  });

  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'warn').mockImplementation();
  });
  afterEach(() => jest.restoreAllMocks());

  describe('through a storage adapter', () => {
    const adapter = (exists: boolean) => ({
      objectExists: jest.fn().mockResolvedValue(exists),
      writeFile: jest.fn().mockResolvedValue(undefined)
    });

    it('writes the flags when none exist', async () => {
      const storage = adapter(false);
      await new MetadataForS3({ config, storage: storage as any }).writeFlags(params());

      expect(storage.objectExists).toHaveBeenCalledWith(flagsKey);
      expect(storage.writeFile).toHaveBeenCalledTimes(1);
      expect(storage.writeFile.mock.calls[0][0]).toBe(flagsKey);
      expect(JSON.parse(storage.writeFile.mock.calls[0][1])).toEqual({
        bulkReset: false, trustPreviousStorage: true, syncPopulation: SyncPopulation.PersonFull
      });
    });

    it('leaves existing flags alone by default', async () => {
      const storage = adapter(true);
      await new MetadataForS3({ config, storage: storage as any }).writeFlags(params({ bulkReset: true }));

      expect(storage.writeFile).not.toHaveBeenCalled();
    });

    it('overwrites existing flags with replace: true, without checking', async () => {
      const storage = adapter(true);
      await new MetadataForS3({ config, storage: storage as any }).writeFlags(params({ runFailed: true, replace: true }));

      expect(storage.objectExists).not.toHaveBeenCalled();
      expect(JSON.parse(storage.writeFile.mock.calls[0][1]).runFailed).toBe(true);
    });

    it('neither checks nor writes in a dry run', async () => {
      const storage = adapter(false);
      await new MetadataForS3({ config, storage: storage as any }).writeFlags(params({ dryRun: true }));

      expect(storage.objectExists).not.toHaveBeenCalled();
      expect(storage.writeFile).not.toHaveBeenCalled();
    });
  });

  describe('without a storage adapter', () => {
    let sendSpy: jest.SpyInstance;
    beforeEach(() => {
      sendSpy = jest.spyOn(S3Client.prototype, 'send').mockImplementation(async () => ({}));
    });

    it('checks the bucket and leaves existing flags alone', async () => {
      const exists = jest.spyOn(Utils, 'objectExistsInS3').mockResolvedValue(true);
      await new MetadataForS3({ config }).writeFlags(params());

      expect(exists).toHaveBeenCalledWith(bucketName, flagsKey, 'us-east-2');
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('puts the flags when none exist', async () => {
      jest.spyOn(Utils, 'objectExistsInS3').mockResolvedValue(false);
      await new MetadataForS3({ config }).writeFlags(params());

      expect(sendSpy).toHaveBeenCalledTimes(1);
      const command = sendSpy.mock.calls[0][0];
      expect(command).toBeInstanceOf(PutObjectCommand);
      expect(command.input).toMatchObject({ Bucket: bucketName, Key: flagsKey });
    });
  });
});
