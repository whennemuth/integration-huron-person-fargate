import { Config } from 'integration-huron-person';
import { IContext } from '../../../context/IContext';
import { S3StorageAdapter } from '../../storage/S3StorageAdapter';
import { AbstractMetadata } from './AbstractMetadata';
import { MetadataForS3 } from './MetadataForS3';
import { MetadataForDynamoDb } from './MetadataForDynamoDb';

/**
 * Factory for creating metadata instances based on storage configuration.
 * 
 * Selects the appropriate metadata implementation based on Config.storage.type:
 * - 'file' | 's3' → MetadataForS3
 * - 'dynamodb' → MetadataForDynamoDb
 * - 'database' → MetadataForS3 (fallback)
 * 
 * ## Usage:
 * ```typescript
 * // S3 mode
 * const metadata = MetadataFactory.create({ config });
 * await metadata.writeFlags({ bucketName, chunkDirectory, ...flags });
 * 
 * // DynamoDB mode (requires context)
 * const metadata = MetadataFactory.create({ config, context });
 * await metadata.writeFlags({ chunkDirectory, ...flags });
 * ```
 * 
 * ## Design Pattern:
 * Factory pattern abstracts instantiation logic, allowing consumers to depend
 * on AbstractMetadata interface without knowing concrete implementation details.
 * 
 * ## Storage Mode Differences:
 * 
 * ### S3 Mode (MetadataForS3):
 * - Stores metadata as JSON files in S3
 * - Requires bucketName parameter for all operations
 * - Supports full file system operations (listChunkFiles, computeTotalRecords, buildAggregatedMetadata)
 * - Path: `s3://{bucket}/chunks/{populationType}/{timestamp}/_metadata.json`
 * 
 * ### DynamoDB Mode (MetadataForDynamoDb):
 * - Stores metadata as records in StatisticsTable
 * - Requires IContext for StatisticsTable initialization
 * - Does NOT support file system operations (chunk files still in S3)
 * - Record structure: PK=syncRunId, SK=eventType ("METADATA" | "FLAGS" | "TERMINAL_ERROR")
 */
export class MetadataFactory {
  /**
   * Create metadata instance based on storage configuration.
   * 
   * @param params.config - Configuration object with storage.type field
   * @param params.context - IContext for DynamoDB operations (required for DynamoDB mode)
   * @param params.storage - Optional S3StorageAdapter for S3 mode
   * @returns AbstractMetadata implementation (S3 or DynamoDB)
   */
  public static create(params: {
    config: Config;
    context?: IContext;
    storage?: S3StorageAdapter;
  }): AbstractMetadata {
    const { config, context, storage } = params;

    switch (config.storage.type) {
      case 's3':
      case 'file':
      case 'database': // Fallback to S3 for database mode
        return new MetadataForS3({ config, storage });
      
      case 'dynamodb':
        if (!context) {
          throw new Error('IContext is required for DynamoDB metadata storage');
        }
        return new MetadataForDynamoDb({ config, context });
      
      default:
        throw new Error(`Unsupported storage type for metadata: ${config.storage.type}`);
    }
  }
}
