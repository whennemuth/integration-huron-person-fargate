import { Config } from 'integration-huron-person';
import { IContext } from '../../../context/IContext';
import { S3StorageAdapter } from '../../storage/S3StorageAdapter';
import { IMetadataStorage } from './IMetadataStorage';
import { MetadataForS3 } from './MetadataForS3';
import { MetadataForDynamoDb } from './MetadataForDynamoDb';

/**
 * Factory for creating metadata instances based on storage configuration.
 * 
 * Selects the appropriate metadata implementation based on context.PREVIOUS_STORAGE_TYPE or config.storage.type:
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
 * on IMetadataStorage interface without knowing concrete implementation details.
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
   * @returns IMetadataStorage implementation (S3 or DynamoDB)
   */
  public static create(params: {
    config: Config;
    context?: IContext;
    storage?: S3StorageAdapter;
  }): IMetadataStorage {
    const { config, context, storage } = params;

    const previousStorageType = context?.PREVIOUS_STORAGE_TYPE || config.storage.type;

    switch (previousStorageType) {
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
        throw new Error(`Unsupported storage type for metadata: ${previousStorageType}`);
    }
  }
}


/**
 * MetadataFactoryForBootstrap
 * 
 * Creates metadata storage instances for processor entry points.
 * 
 * ## Problem
 * Processors need to read metadata/flags before config is loaded to determine
 * which storage backend was used by the chunker. But we can't use MetadataFactory
 * (which requires config) because config loading depends on metadata reading.
 * 
 * ## Solution
 * Create instances directly using environment variables to determine storage type:
 * - If PREVIOUS_STORAGE_TYPE is set to 'dynamodb' → DynamoDB mode
 * - and...
 * - If DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME is set → DynamoDB mode
 * - Otherwise → S3 mode (MetadataForS3)
 * 
 * Instances are created with minimal configuration (empty Config object for S3,
 * minimal IContext for DynamoDB) which is sufficient for bootstrap operations.
 * 
 * ## Why Check for Person Tables (Not Statistics Table)?
 * The statistics table exists in BOTH S3 and DynamoDB modes (for error logging).
 * Only PersonCurrentStateTable and PersonHistoryTable are DynamoDB-mode-specific.
 * These optional tables are set as environment variables only when context.PREVIOUS_STORAGE_TYPE is 'dynamodb' (default).
 * 
 */
export class MetadataFactoryForBootstrap {

  /**
   * Create minimal IContext from environment variables for DynamoDB bootstrap.
   * Used when config hasn't been loaded yet (e.g., early in processor initialization).
   */
  createMinimalContextForDynamoDbBootstrap = (): IContext => {
    const { 
      DYNAMODB_STATISTICS_TABLE_NAME,
      DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME,
      DYNAMODB_PERSON_HISTORY_TABLE_NAME,
      DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME
    } = process.env;

    if (!DYNAMODB_STATISTICS_TABLE_NAME || !DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME || !DYNAMODB_PERSON_HISTORY_TABLE_NAME) {
      throw new Error(
        'DynamoDB table environment variables not found. Required: ' +
        'DYNAMODB_STATISTICS_TABLE_NAME, DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME, DYNAMODB_PERSON_HISTORY_TABLE_NAME'
      );
    }

    // Create minimal context with only PREVIOUS_STORAGE_TYPE
    // Table names are read from environment variables by the table modules themselves
    // Other fields are not needed for metadata operations
    return {
      PREVIOUS_STORAGE_TYPE: 'dynamodb',
    } as unknown as IContext;
  }

  /**
   * Create metadata storage instance for processor bootstrap.
   * 
   * Determines storage type from environment variables and returns an instance
   * configured for early bootstrap operations (before full config is available).
   * 
   * Logic:
   * - If PREVIOUS_STORAGE_TYPE is 'dynamodb' and DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME is set → DynamoDB mode
   * - Otherwise → S3 mode is the only supported alternative.
   * 
   * @returns IMetadataStorage instance (MetadataForS3 or MetadataForDynamoDb)
   */
  public createMetadataForBootstrap = (): IMetadataStorage => {
    const { 
      PREVIOUS_STORAGE_TYPE,
      DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME, 
      DYNAMODB_PERSON_HISTORY_TABLE_NAME 
    } = process.env;
    
    const previousStorageType = `${PREVIOUS_STORAGE_TYPE}`.toLowerCase() as IContext['PREVIOUS_STORAGE_TYPE'];

    switch (previousStorageType) {
      case 'dynamodb':
        // Check for DynamoDB-mode-specific tables (optional tables that only exist when PREVIOUS_STORAGE_TYPE is 'dynamodb')
        if (DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME || DYNAMODB_PERSON_HISTORY_TABLE_NAME) {
          console.log('Using DynamoDB metadata storage (DynamoDB-mode-specific tables detected)');
          const context = this.createMinimalContextForDynamoDbBootstrap();
          return new MetadataForDynamoDb({ config: {} as Config, context });
        }
        throw new Error('DynamoDB storage type detected but required tables not found. Ensure DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME and DYNAMODB_PERSON_HISTORY_TABLE_NAME are set.');
      case 's3':
        console.log('Using S3 metadata storage (S3 mode detected)');
        return new MetadataForS3({ config: {} as Config });
      case 'database':
        throw new Error('Database storage type is not yet supported for bootstrap metadata.');
      case 'file':
        throw new Error('File storage type is not yet supported for bootstrap metadata.');
      default:
        throw new Error(`Unsupported storage type for bootstrap metadata: ${previousStorageType}`);
    }
  }

  /**
   * @deprecated Use createMetadataForBootstrap() instead. This function returned classes
   * requiring static method calls, which has been replaced with direct instance creation.
   */
  public getMetadataManager = (): typeof MetadataForS3 | typeof MetadataForDynamoDb => {
    console.warn('getMetadataManager() is deprecated. Use createMetadataForBootstrap() instead.');
    const { 
      PREVIOUS_STORAGE_TYPE,
      DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME, 
      DYNAMODB_PERSON_HISTORY_TABLE_NAME 
    } = process.env;

    const previousStorageType = `${PREVIOUS_STORAGE_TYPE}`.toLowerCase() as IContext['PREVIOUS_STORAGE_TYPE'];
    switch (previousStorageType) {
      case 'dynamodb':
        if (DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME || DYNAMODB_PERSON_HISTORY_TABLE_NAME) {
          return MetadataForDynamoDb;
        }
        throw new Error('DynamoDB storage type detected but required tables not found. Ensure DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME and DYNAMODB_PERSON_HISTORY_TABLE_NAME are set.');
      case 's3':
        return MetadataForS3;
      case 'file':
        throw new Error('File storage type is not yet supported for bootstrap metadata.');
      case 'database':
        throw new Error('Database storage type is not yet supported for bootstrap metadata.');
      default:
        throw new Error(`Unsupported storage type for bootstrap metadata: ${previousStorageType}`);
    }
  }
}
