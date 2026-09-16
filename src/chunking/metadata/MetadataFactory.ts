import { Config } from 'integration-huron-person';
import { IContext } from '../../../context/IContext';
import { S3StorageAdapter } from '../../storage/S3StorageAdapter';
import { Flags, IMetadataStorage } from './IMetadataStorage';
import { MetadataForS3 } from './MetadataForS3';
import { MetadataForDynamoDb } from './MetadataForDynamoDb';

/**
 * Factory for creating metadata instances based on storage configuration.
 * 
 * Selects the appropriate metadata implementation based on params.previousStorageType or config.storage.type:
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
 * // DynamoDB mode (requires statisticsTableName)
 * const metadata = MetadataFactory.create({ config, previousStorageType, statisticsTableName });
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
 * - Requires an explicit statisticsTableName (no IContext dependency - table names are
 *   baked into task definitions as env vars at deploy time)
 * - Does NOT support file system operations (chunk files still in S3)
 * - Record structure: PK=syncRunId, SK=eventType ("METADATA" | "FLAGS" | "TERMINAL_ERROR")
 */
export class MetadataFactory {
  /**
   * Create metadata instance based on storage configuration.
   * 
   * @param params.config - Configuration object with storage.type field
   * @param params.previousStorageType - Explicit storage type override (takes precedence over config.storage.type)
   * @param params.statisticsTableName - DynamoDB statistics table name (required for DynamoDB mode)
   * @param params.region - AWS region (DynamoDB mode)
   * @param params.storage - Optional S3StorageAdapter for S3 mode
   * @returns IMetadataStorage implementation (S3 or DynamoDB)
   */
  public static create(params: {
    config: Config;
    previousStorageType?: string;
    statisticsTableName?: string;
    region?: string;
    storage?: S3StorageAdapter;
  }): IMetadataStorage {
    const { config, previousStorageType, statisticsTableName, region, storage } = params;

    const resolvedStorageType = previousStorageType || config.storage.type;

    switch (resolvedStorageType) {
      case 's3':
      case 'file':
      case 'database': // Fallback to S3 for database mode
        return new MetadataForS3({ config, storage });
      
      case 'dynamodb':
        if (!statisticsTableName) {
          throw new Error('statisticsTableName is required for DynamoDB metadata storage');
        }
        return new MetadataForDynamoDb({ config, statisticsTableName, region });
      
      default:
        throw new Error(`Unsupported storage type for metadata: ${resolvedStorageType}`);
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
 * an explicit statisticsTableName read from env vars for DynamoDB) which is
 * sufficient for bootstrap operations - no IContext object is built or needed.
 * 
 * ## Why Check for Person Tables (Not Statistics Table)?
 * The statistics table exists in BOTH S3 and DynamoDB modes (for error logging).
 * Only PersonCurrentStateTable and PersonHistoryTable are DynamoDB-mode-specific.
 * These optional tables are set as environment variables only when PREVIOUS_STORAGE_TYPE is 'dynamodb' (default).
 * 
 */
export class MetadataFactoryForBootstrap {

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
      DYNAMODB_PERSON_HISTORY_TABLE_NAME,
      DYNAMODB_STATISTICS_TABLE_NAME,
      REGION
    } = process.env;
    
    const previousStorageType = `${PREVIOUS_STORAGE_TYPE}`.toLowerCase() as IContext['PREVIOUS_STORAGE_TYPE'];

    switch (previousStorageType) {
      case 'dynamodb':
        // Check for DynamoDB-mode-specific tables (optional tables that only exist when PREVIOUS_STORAGE_TYPE is 'dynamodb')
        if (DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME || DYNAMODB_PERSON_HISTORY_TABLE_NAME) {
          if (!DYNAMODB_STATISTICS_TABLE_NAME) {
            throw new Error('DYNAMODB_STATISTICS_TABLE_NAME environment variable is required for DynamoDB metadata storage bootstrap.');
          }
          console.log('Using DynamoDB metadata storage (DynamoDB-mode-specific tables detected)');
          return new MetadataForDynamoDb({ config: {} as Config, statisticsTableName: DYNAMODB_STATISTICS_TABLE_NAME, region: REGION });
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
   * Resolve which statistics table (mock or real) holds this run's FLAGS record, without needing
   * to already know flags.useMockTarget - which is exactly what this determines. Chunker writes
   * FLAGS/METADATA/TERMINAL_ERROR to the mock statistics table whenever flags.useMockTarget is
   * true (see docker/chunker.ts's createMetadataManager()) - a "mock run" being either a
   * mock-target-only run, or a source-simulator run (which always forces useMockTarget=true, per
   * Runner.ts's safety enforcement) - so an entire run's statistics-table trail lives in exactly
   * one table, never split across both.
   *
   * Tries the mock statistics table first (if DynamoDB mode and a mock table is configured); if
   * no FLAGS record is found there, falls back to the real table. Whichever table actually has
   * the FLAGS record for this run is the one bootstrap consumers (processor, merger) should keep
   * using for the rest of that run's statistics-table reads/writes (METADATA, TERMINAL_ERROR,
   * CHUNK_STATUS, STATISTICS, ERROR).
   *
   * No-op in S3 storage mode: FLAGS live in a single S3 location there (no mock/real split), so
   * this just delegates to createMetadataForBootstrap().
   */
  public resolveMockAwareFlags = async (params: {
    bucketName?: string;
    chunkDirectory: string;
    region?: string;
  }): Promise<{ metadata: IMetadataStorage; flags: Partial<Flags>; statisticsTableName?: string }> => {
    const { bucketName, chunkDirectory, region } = params;
    const { PREVIOUS_STORAGE_TYPE, DYNAMODB_MOCK_STATISTICS_TABLE_NAME, DYNAMODB_STATISTICS_TABLE_NAME } = process.env;
    const previousStorageType = `${PREVIOUS_STORAGE_TYPE}`.toLowerCase();

    if (previousStorageType === 'dynamodb' && DYNAMODB_MOCK_STATISTICS_TABLE_NAME) {
      const mockMetadata = new MetadataForDynamoDb({
        config: {} as Config,
        statisticsTableName: DYNAMODB_MOCK_STATISTICS_TABLE_NAME,
        region
      });
      const mockFlags = await mockMetadata.readFlags({ bucketName, chunkDirectory, region });
      if (Object.keys(mockFlags).length > 0) {
        return { metadata: mockMetadata, flags: mockFlags, statisticsTableName: DYNAMODB_MOCK_STATISTICS_TABLE_NAME };
      }
    }

    const realMetadata = this.createMetadataForBootstrap();
    const realFlags = await realMetadata.readFlags({ bucketName, chunkDirectory, region });
    return { metadata: realMetadata, flags: realFlags, statisticsTableName: DYNAMODB_STATISTICS_TABLE_NAME };
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
