import { FieldSet } from "integration-core";
import { PersonCacheFactory } from "./PersonCacheFactory";
import { AbstractPersonCache } from "./AbstractPersonCache";
import { Config } from "integration-huron-person";

export type PersonCacheLookupParams = {
  config: Config,
  region?: string,
  bucketName?: string,
  /** Longest to wait while another task holds the cache lock (default: the lock's TTL, after which it expires anyway) */
  maxWaitMs?: number,
  /** For tests */
  sleep?: (ms: number) => Promise<void>,
  /** For tests */
  now?: () => number
};

/**
 * This class implements a cache lookup for source identifiers from the target system that is
 * specific to the specified bucket and configuration. This is done to avoid expensive direct
 * API lookups within the same chunk. These derive from an S3 file created by the chunker task.
 *
 * The cache is only a short cut: UpsertDeltaStrategy looks a person the cache doesn't hold up in the
 * target directly. So when the cache can't be had (no cache and no lock, a lock still held after
 * maxWaitMs, or an error reading it), the lookup proceeds with an empty cache (every person looked up
 * directly: slower, still correct) and **never throws**: UpsertDeltaStrategy treats a lookup that
 * throws as a person absent from the target and CREATEs them, which would add duplicates to Huron.
 */
export class PersonCacheLookup {
  private cachedSourceIdentifiers?: Set<string> | undefined

  constructor(private params: PersonCacheLookupParams) { }

  public lookupPersonInTargetSystemCache = async ({ person, s3Key }: { person: FieldSet | string, s3Key: string }): Promise<any> => {
    // Lazy load cache on first access, waiting while another task is building it
    if (!this.cachedSourceIdentifiers) {
      this.cachedSourceIdentifiers = await this.loadCache(s3Key);
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

  /**
   * Read the run's person cache, waiting while another task holds its lock (the chunker task that
   * builds it). Returns an empty Set, so every person is looked up in the target directly, when the
   * cache can't be had: never throws (see the class comment).
   */
  private loadCache = async (s3Key: string): Promise<Set<string>> => {
    const {
      config, region, bucketName,
      maxWaitMs = AbstractPersonCache.CACHE_LOCK_TTL_MS,
      sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)),
      now = Date.now
    } = this.params;
    const directLookups = 'every person in this chunk will be looked up in the target directly';

    try {
      const personCache = PersonCacheFactory.create(config);
      // Derive chunk directory from chunk key
      // "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson" -> "chunks/person-full/2026-03-03T19:58:41.277Z"
      const chunkDirectory = s3Key.substring(0, s3Key.lastIndexOf('/'));
      const key = chunkDirectory + `/${AbstractPersonCache.CACHE_FILE_NAME}`;
      const cacheParams = { bucketName: bucketName!, key, region: region! };

      const startedAt = now();
      let lockWaits = 0;
      let sawNoLock = false;

      while (true) {
        if (await personCache.cacheExists(cacheParams)) {
          return await personCache.getCache(cacheParams);
        }

        if (await personCache.isLockActive(cacheParams)) {
          // Another task is creating the cache: wait with exponential backoff (max 10 seconds) for as
          // long as the lock holds, up to maxWaitMs.
          const waitedMs = now() - startedAt;
          if (waitedMs >= maxWaitMs) {
            console.warn(`⚠️  Cache lock still held after ${Math.round(waitedMs / 1000)}s - ${directLookups}`);
            return new Set<string>();
          }
          const delay = Math.min(1000 * Math.pow(2, lockWaits), 10000);
          console.log(`⏳ Cache creation in progress - waiting ${delay}ms (waited ${Math.round(waitedMs / 1000)}s so far)`);
          await sleep(delay);
          lockWaits++;
          continue;
        }

        // No lock: the cache may have been written between the two checks above (the builder
        // writes the cache before releasing its lock), so look once more before deciding.
        if (await personCache.cacheExists(cacheParams)) {
          return await personCache.getCache(cacheParams);
        }

        if ( ! sawNoLock) {
          // Cache creation may not have started yet: wait briefly, once
          sawNoLock = true;
          console.log(`⚠️  Cache not found and no lock active - waiting for cache creation to start`);
          await sleep(2000);
          continue;
        }

        // Cache creation failed, expired, or never started
        console.warn(`⚠️  Cache not found and no lock active - ${directLookups}`);
        return new Set<string>();
      }
    }
    catch (error: any) {
      console.warn(`⚠️  Could not read the person cache (${error?.message ?? error}) - ${directLookups}`);
      return new Set<string>();
    }
  }

  public getCachedSourceIdentifiers = (): Set<string> | undefined => {
    return this.cachedSourceIdentifiers;
  }
}
