import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand, HeadObjectCommandInput, HeadObjectCommandOutput } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { IStorageAdapter } from './IStorageAdapter';
import { objectExistsInS3 } from '../Utils';

/**
 * Configuration for S3 storage adapter
 */
export interface S3StorageAdapterConfig {
  /** S3 bucket name */
  bucketName: string;
  
  /** AWS region (e.g., 'us-east-2') */
  region?: string;
  
  /** Optional S3 client (useful for testing with mocks) */
  s3Client?: S3Client;
}

/**
 * Storage adapter implementation for AWS S3.
 * 
 * Wraps S3 SDK operations to implement the IStorageAdapter interface,
 * allowing BigJsonFile to operate on S3 files.
 */
export class S3StorageAdapter implements IStorageAdapter {
  private readonly s3: S3Client;
  private readonly bucketName: string;
  private readonly region?: string;

  constructor(config: S3StorageAdapterConfig) {
    this.bucketName = config.bucketName;
    this.region = config.region;
    this.s3 = config.s3Client || new S3Client({
      region: config.region
    });
  }

  async objectExists(key: string): Promise<boolean> {
    const { bucketName, region } = this;
    return await objectExistsInS3(bucketName, key, region);
  }

  /**
   * Get a readable stream from an S3 object.
   */
  async getReadStream(key: string): Promise<Readable> {
    const command = new GetObjectCommand({
      Bucket: this.bucketName,
      Key: key
    });

    const response = await this.s3.send(command);
    
    if (!response.Body) {
      throw new Error(`No body returned for s3://${this.bucketName}/${key}`);
    }

    return response.Body as Readable;
  }

  /**
   * Write content to an S3 object.
   */
  async writeFile(key: string, content: string | Buffer, contentType?: string): Promise<void> {
    console.log(`Writing file to s3://${this.bucketName}/${key})`);
    await this.s3.send(new PutObjectCommand({
      Bucket: this.bucketName,
      Key: key,
      Body: content,
      ContentType: contentType
    }));
  }
}



if (module === require.main) {
  (async () => {
    const {
      S3_STORAGE_ADAPTER_TEST_TASK: task = 'check-file-existence',
      S3_STORAGE_ADAPTER_TEST_BUCKET_NAME: bucketName = 'my-test-bucket',
      S3_STORAGE_ADAPTER_TEST_KEY: testKey = 'test-file.txt', 
      S3_STORAGE_ADAPTER_TEST_REGION: region = 'us-east-2', 
    } = process.env;
    
    const adapter = new S3StorageAdapter({ bucketName, region });

    console.log(`Checking if file exists at s3://${bucketName}/${testKey}...`);
    const existsBefore = await adapter.objectExists(testKey);
    console.log(`File exists: ${existsBefore}`);

    switch(task) {
      case 'check-file-existence':
        return; // Don't need to do anything else for this task
      case 'read-file':
        console.log(`Reading file from s3://${bucketName}/${testKey}...`);
        const readStream = await adapter.getReadStream(testKey);
        let data = '';
        for await (const chunk of readStream) {
          data += chunk;
        }
        console.log(`File content read from S3: ${data}`);
        break;
      case 'write-file':
        const testContent = 'Hello, S3!';
        await adapter.writeFile(testKey, testContent, 'text/plain');
        console.log('File written successfully.');
        break;
      default:
        console.error(`Unknown task: ${task}. Valid tasks are: check-file-existence, write-file, read-file`);
        return;
    }
  })();
}