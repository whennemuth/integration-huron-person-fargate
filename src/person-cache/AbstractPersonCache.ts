import { Config } from "integration-huron-person";
import { AbstractPersonTarget } from "./PersonTargetReal";

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
  protected personTarget: AbstractPersonTarget | undefined;

  public static CACHE_FILE_NAME = '_personCache.txt';
  public static CACHE_LOCK_FILE_NAME = '_personCache.creating';

  constructor(params?: { 
    config?: Config;
    personTarget?: AbstractPersonTarget;
  }) {
    this.config = params?.config;
    this.personTarget = params?.personTarget;
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

  /**
   * Attempt to acquire cache creation lock.
   * 
   * Creates a marker file to signal that cache creation is in progress.
   * This prevents multiple concurrent tasks from attempting to create the cache.
   * 
   * @param params - Storage location parameters
   * @returns True if lock was acquired, false if another task holds the lock
   */
  public abstract acquireCacheLock(params: { 
    bucketName: string; 
    key: string; 
    region: string 
  }): Promise<boolean>;

  /**
   * Release cache creation lock.
   * 
   * Deletes the marker file created by acquireCacheLock().
   * 
   * @param params - Storage location parameters
   */
  public abstract releaseCacheLock(params: { 
    bucketName: string; 
    key: string; 
    region: string 
  }): Promise<void>;

  /**
   * Check if cache creation lock is active.
   * 
   * Checks for existence of lock marker file and validates TTL.
   * 
   * @param params - Storage location parameters
   * @returns True if lock exists and hasn't expired, false otherwise
   */
  public abstract isLockActive(params: { 
    bucketName: string; 
    key: string; 
    region: string 
  }): Promise<boolean>;

  /**
   * Ensure person cache exists, creating it if necessary.
   * 
   * Uses marker file lock pattern to prevent race conditions when multiple
   * concurrent tasks attempt cache creation simultaneously. This method is
   * thread-safe: multiple callers can safely invoke it concurrently.
   * 
   * ## Behavior:
   * - If cache exists: returns immediately
   * - If another task is creating cache: skips (returns immediately)
   * - If no cache and no lock: acquires lock, creates cache, releases lock
   * 
   * ## Race Condition Prevention:
   * Multiple concurrent chunker tasks may start simultaneously (queue seeding scenario).
   * Only one task will create the cache; others will skip gracefully.
   * 
   * @param params - Storage location parameters
   * @returns Result indicating what happened:
   *   - existed: cache already existed before this call
   *   - created: this task created the cache
   *   - skipped: another task is/was creating it
   */
  public abstract ensureCache(params: { 
    bucketName: string; 
    key: string; 
    region: string;
  }): Promise<{ existed: boolean; created: boolean; skipped: boolean }>;
}
