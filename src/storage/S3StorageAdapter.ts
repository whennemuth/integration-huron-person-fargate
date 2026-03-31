import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { IStorageAdapter } from './IStorageAdapter';

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

  constructor(config: S3StorageAdapterConfig) {
    this.bucketName = config.bucketName;
    this.s3 = config.s3Client || new S3Client({
      region: config.region
    });
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
    await this.s3.send(new PutObjectCommand({
      Bucket: this.bucketName,
      Key: key,
      Body: content,
      ContentType: contentType
    }));
  }
}
