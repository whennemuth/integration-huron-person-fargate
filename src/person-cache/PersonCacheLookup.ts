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
    // Lazy load cache on first access
    if (!this.cachedSourceIdentifiers) {
      const personCache = PersonCacheFactory.create(config);
      // Derive chunk directory from chunk key
      // "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson" -> "chunks/person-full/2026-03-03T19:58:41.277Z"
      const chunkDirectory = s3Key!.substring(0, s3Key!.lastIndexOf('/'));
      const key = chunkDirectory + `/${AbstractPersonCache.CACHE_FILE_NAME}`;

      this.cachedSourceIdentifiers = await personCache.getCache({ 
        bucketName: bucketName!, key, region: region! 
      });

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