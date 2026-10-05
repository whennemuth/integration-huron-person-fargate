import { Config } from "integration-huron-person";
import { AbstractPersonCache } from './AbstractPersonCache';
import { PersonCacheForDynamoDb } from './PersonCacheForDynamoDb';
import { PersonCacheForS3 } from './PersonCacheForS3';
import { PersonTargetMocked } from "./PersonTargetMocked";
import { PersonTargetReal } from "./PersonTargetReal";

/**
 * Factory for creating person cache instances based on storage configuration.
 * 
 * Selects the appropriate cache implementation based on Config.storage.type:
 * - 'file' | 's3' → PersonCacheForS3
 * - 'dynamodb' → PersonCacheForDynamoDb (currently a facade to S3)
 * - 'database' → PersonCacheForS3 (fallback)
 * 
 * ## Usage:
 * ```typescript
 * const cache = PersonCacheFactory.create(config);
 * await cache.setCache({ bucketName, key, region });
 * const buids = await cache.getCache({ bucketName, key, region });
 * ```
 * 
 * ## Design Pattern:
 * Factory pattern abstracts instantiation logic, allowing consumers to depend
 * on AbstractPersonCache interface without knowing concrete implementation details.
 */
export class PersonCacheFactory {
  /**
   * Create person cache instance based on storage configuration.
   * 
   * @param config - Configuration object with storage.type field
   * @param useMockTarget - Whether to use mock target mode (query DynamoDB instead of real API)
   * @returns AbstractPersonCache implementation (S3 or DynamoDB)
   */
  public static create(
    config: Config, 
    useMockTarget?: boolean
  ): AbstractPersonCache {

    const personTarget = useMockTarget ? 
      new PersonTargetMocked() : 
      new PersonTargetReal();
      
    switch (config.storage.type) {
      case 's3':
      case 'file':
      case 'database': // Fallback to S3 for database mode
        return new PersonCacheForS3({ config, personTarget });
      
      case 'dynamodb':
        // NOTE: Currently returns facade that delegates to S3
        // See PersonCacheForDynamoDb comments for rationale
        return new PersonCacheForDynamoDb({ config, personTarget });
      
      default:
        throw new Error(`Unsupported storage type for person cache: ${config.storage.type}`);
    }
  }
}
