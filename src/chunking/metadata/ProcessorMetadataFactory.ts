/**
 * ProcessorMetadataFactory
 * 
 * Dynamic metadata manager selection for processor entry points.
 * 
 * ## Problem
 * Processors need to read metadata/flags before config is loaded to determine
 * which storage backend was used by the chunker. But we can't use MetadataFactory
 * (which requires config) because config loading depends on metadata reading.
 * 
 * ## Solution
 * Use environment variables to determine storage type:
 * - If DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME is set → DynamoDB mode
 * - Otherwise → S3 mode (MetadataForS3)
 * 
 * ## Why Check for Person Tables (Not Statistics Table)?
 * The statistics table exists in BOTH S3 and DynamoDB modes (for error logging).
 * Only PersonCurrentStateTable and PersonHistoryTable are DynamoDB-mode-specific.
 * These optional tables are set as environment variables only when context.PREVIOUS_STORAGE_TYPE is 'dynamodb' (default).
 * 
 * ## Usage
 * ```typescript
 * import { getMetadataManager } from './metadata/ProcessorMetadataFactory';
 * 
 * const MetadataManager = getMetadataManager();
 * const flags = await MetadataManager.readFlagsFromChunkKey(bucket, key, region);
 * ```
 */

import { MetadataForS3 } from './MetadataForS3';
import { MetadataForDynamoDb } from './MetadataForDynamoDb';

/**
 * Determine which metadata manager implementation to use based on environment.
 * 
 * Logic:
 * - If DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME is set → DynamoDB mode
 * - Otherwise → S3 mode (default)
 * 
 * @returns MetadataForS3 or MetadataForDynamoDb class (not instance)
 */
export function getMetadataManager(): typeof MetadataForS3 | typeof MetadataForDynamoDb {
  const { 
    DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME, 
    DYNAMODB_PERSON_HISTORY_TABLE_NAME 
  } = process.env;

  // Check for DynamoDB-mode-specific tables (optional tables that only exist when PREVIOUS_STORAGE_TYPE is 'dynamodb')
  if (DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME || DYNAMODB_PERSON_HISTORY_TABLE_NAME) {
    console.log('Using DynamoDB metadata storage (DynamoDB-mode-specific tables detected)');
    return MetadataForDynamoDb;
  }

  console.log('Using S3 metadata storage (default)');
  return MetadataForS3;
}
