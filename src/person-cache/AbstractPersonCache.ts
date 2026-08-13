import { Config } from "integration-huron-person";

/**
 * Abstract base class for person cache implementations.
 * 
 * The person cache stores sourceIdentifiers (BUIDs) of all persons currently in the 
 * target system. This cache is used during bulkReset=true mode to determine whether
 * to CREATE or PATCH records without querying the target API for each person individually.
 * 
 * ## Design Pattern: Template Method
 * 
 * This abstract class defines the interface and shared behavior for person cache operations,
 * while concrete implementations provide storage-specific details (S3, DynamoDB, etc.).
 * 
 * ## Implementations:
 * - **PersonCacheForS3**: Stores cache as newline-delimited text file in S3
 * - **PersonCacheForDynamoDb**: Facade delegating to S3 (see class for rationale)
 * 
 * ## Usage:
 * ```typescript
 * const cache = PersonCacheFactory.create(config);
 * 
 * // Check if cache exists
 * const exists = await cache.cacheExists({ 
 *   bucketName, key, region 
 * });
 * 
 * // Write cache
 * await cache.setCache({ 
 *   bucketName, key, region 
 * });
 * 
 * // Read cache
 * const buids = await cache.getCache({ 
 *   bucketName, key, region 
 * });
 * ```
 */
export abstract class AbstractPersonCache {
  protected config: Config | undefined;

  public static CACHE_FILE_NAME = '_personCache.txt';

  constructor(params?: { config?: Config }) {
    this.config = params?.config;
  }

  /**
   * Check if cache exists in storage.
   * 
   * @param params - Storage location parameters
   * @returns True if cache exists, false otherwise
   */
  public abstract cacheExists(params: { 
    bucketName: string; 
    key: string; 
    region: string 
  }): Promise<boolean>;

  /**
   * Write person cache to storage.
   * 
   * Fetches full population from target API and stores sourceIdentifiers
   * for efficient lookup during bulk operations.
   * 
   * @param params - Storage location parameters
   */
  public abstract setCache(params: { 
    bucketName: string; 
    key: string; 
    region: string 
  }): Promise<void>;

  /**
   * Read person cache from storage.
   * 
   * Returns a Set of sourceIdentifiers (BUIDs) for O(1) lookup performance.
   * 
   * @param params - Storage location parameters
   * @returns Set of sourceIdentifiers from cache
   */
  public abstract getCache(params: { 
    bucketName: string; 
    key: string; 
    region: string 
  }): Promise<Set<string>>;

  /**
   * Write cache to local file (for testing/debugging).
   * 
   * @param filePath - Local filesystem path
   */
  public abstract writeToFile(filePath: string): Promise<void>;
}
