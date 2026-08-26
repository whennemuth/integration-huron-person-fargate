import { FieldSet } from "integration-core";
import { PersonCacheFactory } from "./PersonCacheFactory";
import { AbstractPersonCache } from "./AbstractPersonCache";
import { Config } from "integration-huron-person";

/**
 * This class implements a cache lookup for source identifiers from the target system that is
 * specific to the specified bucket and configuration. This is done to avoid expensive direct 
 * API lookups within the same chunk. These derive from an S3 file created by the chunker task.
 */
export class PersonCacheLookup {
  private cachedSourceIdentifiers?: Set<string> | undefined

  constructor(private params: { 
    config: Config, 
    region?: string,
    bucketName?: string
  }) { }

  public lookupPersonInTargetSystemCache = async ({ person, s3Key }: { person: FieldSet | string, s3Key: string }): Promise<any> => {
    let { config, region, bucketName } = this.params;
    // Lazy load cache on first access with exponential backoff if lock is active
    if (!this.cachedSourceIdentifiers) {
      const personCache = PersonCacheFactory.create(config);
      // Derive chunk directory from chunk key
      // "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson" -> "chunks/person-full/2026-03-03T19:58:41.277Z"
      const chunkDirectory = s3Key!.substring(0, s3Key!.lastIndexOf('/'));
      const key = chunkDirectory + `/${AbstractPersonCache.CACHE_FILE_NAME}`;
      const cacheParams = { bucketName: bucketName!, key, region: region! };

      // Wait for cache to be ready with exponential backoff
      let attempts = 0;
      const maxAttempts = 20; // ~2 minutes total wait time
      
      while (attempts < maxAttempts) {
        // Check if cache exists - if so, load it
        if (await personCache.cacheExists(cacheParams)) {
          this.cachedSourceIdentifiers = await personCache.getCache(cacheParams);
          break;
        }
        
        // Check if cache creation is in progress
        if (await personCache.isLockActive(cacheParams)) {
          // Another task is creating the cache - wait with exponential backoff
          const delay = Math.min(1000 * Math.pow(2, attempts), 10000); // Max 10 seconds
          console.log(`⏳ Cache creation in progress - waiting ${delay}ms (attempt ${attempts + 1}/${maxAttempts})`);
          await new Promise(resolve => setTimeout(resolve, delay));
          attempts++;
        } else {
          // No cache and no lock - cache creation may have failed or not started yet
          if (attempts === 0) {
            // First attempt - log and wait briefly
            console.log(`⚠️  Cache not found and no lock active - waiting for cache creation to start`);
            await new Promise(resolve => setTimeout(resolve, 2000));
            attempts++;
          } else {
            // Subsequent attempts - cache creation may have failed
            console.warn(`⚠️  Cache still not available after ${attempts} attempts - proceeding without cache`);
            this.cachedSourceIdentifiers = new Set<string>();
            break;
          }
        }
      }
      
      // Timeout exceeded
      if (attempts >= maxAttempts) {
        const errorMsg = `Timeout waiting for cache creation after ${maxAttempts} attempts (~2 minutes). ` +
          `Cache may be taking too long to create or lock may be stale.`;
        console.error(errorMsg);
        throw new Error(errorMsg);
      }

      if (!this.cachedSourceIdentifiers) {
        this.cachedSourceIdentifiers = new Set<string>();
      }
      
      console.log(`Loaded ${this.cachedSourceIdentifiers.size} source identifiers from target system cache`);
    }

    // Extract sourceIdentifier from person (string or FieldSet)
    let sourceIdentifier: string | undefined;
    
    if (typeof person === 'string') {
      sourceIdentifier = person;
    } else if (typeof person === 'object' && person.fieldValues) {
      // FieldSet - find sourceIdentifier field
      const field = person.fieldValues.find((fv: any) => {
        const fieldName = Object.keys(fv)[0];
        return fieldName === 'sourceIdentifier';
      });
      if (field) {
        sourceIdentifier = Object.values(field)[0] as string;
      }
    }

    // Return sourceIdentifier if found in cache, otherwise undefined
    if (sourceIdentifier && this.cachedSourceIdentifiers && this.cachedSourceIdentifiers.has(sourceIdentifier)) {
      return sourceIdentifier;
    }
    
    return undefined;
  };

  public getCachedSourceIdentifiers = (): Set<string> | undefined => {
    return this.cachedSourceIdentifiers;
  }
}