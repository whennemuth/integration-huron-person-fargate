import { DeleteObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import { S3StorageAdapter } from '../src/storage/S3StorageAdapter';

const s3Mock = mockClient(S3Client);

describe('S3StorageAdapter', () => {
  const bucketName = 'test-bucket';
  let adapter: S3StorageAdapter;

  beforeEach(() => {
    s3Mock.reset();
    adapter = new S3StorageAdapter({ bucketName, s3Client: s3Mock as any });
  });

  describe('writeFile', () => {
    it('puts an object with the given key and content', async () => {
      s3Mock.on(PutObjectCommand).resolves({});

      await adapter.writeFile('chunks/chunk-0000.ndjson', 'content');

      expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);
      const call = s3Mock.commandCalls(PutObjectCommand)[0];
      expect(call.args[0].input).toMatchObject({
        Bucket: bucketName,
        Key: 'chunks/chunk-0000.ndjson',
        Body: 'content'
      });
    });
  });

  describe('deleteFile', () => {
    it('deletes the object at the given key', async () => {
      s3Mock.on(DeleteObjectCommand).resolves({});

      await adapter.deleteFile('chunks/chunk-0725.ndjson');

      expect(s3Mock.commandCalls(DeleteObjectCommand)).toHaveLength(1);
      const call = s3Mock.commandCalls(DeleteObjectCommand)[0];
      expect(call.args[0].input).toEqual({
        Bucket: bucketName,
        Key: 'chunks/chunk-0725.ndjson'
      });
    });
  });
});
