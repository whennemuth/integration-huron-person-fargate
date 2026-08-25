import { S3 } from '@aws-sdk/client-s3';
import { TestEnvironment } from 'integration-core';
import { BasicCache, Config, ConfigManager, getLocalConfig, HuronPerson } from "integration-huron-person";
import { AbstractPersonCache } from './AbstractPersonCache';
import { AbstractPersonTarget } from './PersonTargetReal';

/**
 * S3-based person cache implementation.
 * 
 * Stores cache as newline-delimited text file in S3 bucket. Each line contains
 * one sourceIdentifier (BUID). This format enables:
 * - Efficient streaming writes (no need to load full list in memory)
 * - Simple parsing (split by newline)
 * - Human-readable format for debugging
 * - Compact storage (plain text, ~10 bytes per BUID)
 * 
 * ## Why S3 for Cache Storage:
 * 
 * S3 is better suited for bulk person cache than DynamoDB because:
 * 1. **Large dataset**: Cache typically contains 10,000-100,000+ BUIDs
 * 2. **Simple structure**: Just a list of strings, no complex querying needed
 * 3. **Sequential access**: Cache is read/written once per sync, not random access
 * 4. **Cost**: S3 storage is significantly cheaper than DynamoDB for this use case
 * 5. **Simplicity**: Newline-delimited text is trivial to read/write
 * 
 * ## Cache Lifecycle:
 * 1. Chunker writes cache at start if bulkReset=true
 * 2. Processors read cache for CREATE vs PATCH decisions
 * 3. Cache is sync-scoped (stored under chunkDirectory)
 * 4. No need to persist between syncs
 */
export class PersonCacheForS3 extends AbstractPersonCache {
  private cache: BasicCache | undefined;

  constructor(private params?: { 
    cache?: BasicCache; 
    config?: Config;
    personTarget: AbstractPersonTarget;
  }) {
    super(params);
    if (params?.cache) {
      this.cache = params.cache;
    } else if (params?.config && !params.cache) {
      this.cache = BasicCache.getInstance(params.config);
    }
  }

  private getConfig = async (): Promise<Config> => {
    if(this.config) {
      return this.config;
    }
    const { 
      /** SECRET_ARN: Secrets Manager ARN containing config */
      SECRET_ARN,
      /** HURON_PERSON_CONFIG_PATH: Path to config.json (fallback for local dev only) */
      HURON_PERSON_CONFIG_PATH
    } = process.env;
  
    // Load configuration.
    const configManager = ConfigManager.getInstance();
    const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
    this.config = await configManager
      .reset()
      .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
      .fromSecretManager(SECRET_ARN)                // ← Fallback to Secrets Manager
      .fromEnvironment()                            // ← Fallback to individual env var overrides
      .fromFileSystem(localConfigPath)              // ← Local dev only
      .getConfigAsync('people');

    return this.config;
  }

  private getBasicCache = async (): Promise<BasicCache | undefined> => {
    if(this.cache) {
      return this.cache;
    }
    const config = await this.getConfig();
    return BasicCache.getInstance(config);
  }

  /**
   * Check if S3 cache file exists.
   */
  public async cacheExists(params: { bucketName: string, key: string, region: string }): Promise<boolean> {
    const { bucketName, key, region } = params;
    const s3 = new S3({ region });
    try {
      await s3.headObject({
        Bucket: bucketName,
        Key: key
      });
      return true;
    } catch (error: any) {
      if (error.name === 'NotFound') {
        return false;
      }
      throw error;
    }
  }

  /**
   * Fetch full population from target API.
   * 
   * In mock target mode, "pretends" DynamoDB mockTargetPersonTable is the Target API.
   * In real mode, calls ListPeople to query actual Huron API.
   * 
   * This is the source of truth for the cache.
   * 
   * @public Exposed primarily for testing, but can be called directly if needed.
   */
  public async getFullPopulationFromTargetAPI(): Promise<HuronPerson[]> {
    const { personTarget } = this.params || {};
    if (!personTarget) {
      throw new Error('PersonTarget is not initialized');
    }
    return personTarget.getFullPopulationFromTarget(this.config);
  }

  /**
   * Convert API response to newline-delimited format.
   */
  private async getLines(): Promise<string[]> {
    console.log(`\n🔄 Fetching full population from target API...`);
    const people = await this.getFullPopulationFromTargetAPI();
    console.log(`  Retrieved ${people.length} people from target API`);

    if (people.length === 0) {
      console.warn('  ⚠️  No people retrieved from target API - cache will be empty');
    }
    
    // Create newline-delimited content from sourceIdentifiers
    const lines: string[] = [];
    let skippedCount = 0;
    
    for (const person of people) {
      const { sourceIdentifier } = person;
      if (sourceIdentifier) {
        lines.push(sourceIdentifier);
      } else {
        skippedCount++;
      }
    }

    if (skippedCount > 0) {
      console.warn(`  ⚠️  Skipped ${skippedCount} people without sourceIdentifier`);
    }

    return lines;
  }

  /**
   * Write cache to S3.
   * Fetches full population and writes as newline-delimited text.
   */
  public async setCache(params: { bucketName: string, key: string, region: string }): Promise<void> {
    const { bucketName, key, region } = params;

    const lines = await this.getLines();
    const content = lines.join('\n');
    const contentBuffer = Buffer.from(content, 'utf-8');

    console.log(`\n📝 Writing population cache to S3: s3://${bucketName}/${key}`);

    // Write to S3
    const s3 = new S3({ region });
    
    try {
      await s3.putObject({
        Bucket: bucketName,
        Key: key,
        Body: contentBuffer,
        ContentType: 'text/plain',
        Metadata: {
          recordCount: lines.length.toString(),
          createdAt: new Date().toISOString()
        }
      });
      
      console.log(`  ✅ Successfully wrote ${lines.length} sourceIdentifiers to cache`);
      console.log(`  📊 Cache size: ${(contentBuffer.length / 1024).toFixed(2)} KB`);
    } catch (error: any) {
      console.error(`  ❌ Failed to write population cache to S3: ${error.message}`);
      throw new Error(`Failed to write population cache to S3: ${error.message}`);
    }
  }

  /**
   * Read cache from S3.
   * Returns Set of sourceIdentifiers for O(1) lookup.
   */
  public async getCache(params: { bucketName: string, key: string, region: string }): Promise<Set<string>> {
    const { bucketName, key, region } = params;

    console.log(`\n📖 Reading population cache from S3: s3://${bucketName}/${key}`);
    
    const s3 = new S3({ region });
    const sourceIdentifiers = new Set<string>();

    try {
      // Get object from S3
      const response = await s3.getObject({
        Bucket: bucketName,
        Key: key
      });

      if (!response.Body) {
        console.warn('  ⚠️  Cache file exists but has no content');
        return sourceIdentifiers;
      }

      // Convert Body to string and split by newlines
      const content = await response.Body.transformToString('utf-8');
      const lines = content.split('\n');

      // Add each non-empty line to Set
      for (const line of lines) {
        const trimmedLine = line.trim();
        if (trimmedLine) {
          sourceIdentifiers.add(trimmedLine);
        }
      }

      console.log(`  ✅ Loaded ${sourceIdentifiers.size} sourceIdentifiers from cache`);
      
      // Log metadata if available
      if (response.Metadata) {
        const { recordCount, createdAt } = response.Metadata;
        if (recordCount) {
          console.log(`  📊 Cache metadata - Records: ${recordCount}, Created: ${createdAt || 'unknown'}`);
        }
      }

      return sourceIdentifiers;

    } catch (error: any) {
      if (error.name === 'NoSuchKey') {
        console.warn(`  ⚠️  Cache file not found at s3://${bucketName}/${key}`);
        console.warn('  💡 This is expected if bulkReset was not enabled or chunker did not complete');
        return sourceIdentifiers; // Return empty Set
      }
      
      console.error(`  ❌ Failed to read population cache from S3: ${error.message}`);
      throw new Error(`Failed to read population cache from S3: ${error.message}`);
    }
  }

  /**
   * Write cache to local file for testing/debugging.
   */
  public async writeToFile(filePath: string): Promise<void> {
    const lines = await this.getLines();
    const content = lines.join('\n');
    const fs = await import('fs');
    fs.writeFileSync(filePath, content, 'utf-8');
    console.log(`\n📝 Wrote population cache to file: ${filePath}`);
    console.log(`  ✅ Successfully wrote ${lines.length} sourceIdentifiers to file`);
    console.log(`  📊 File size: ${(Buffer.byteLength(content, 'utf-8') / 1024).toFixed(2)} KB`);
  }
}


if (require.main === module) {
  (async () => {
    const testEnvironment = TestEnvironment('PERSON_CACHE');
    [
      'PERSON_CACHE_BUCKET_NAME',
      'PERSON_CACHE_KEY',
      'REGION',
      'SECRET_ARN',
      'HURON_PERSON_CONFIG_PATH',
      'HURON_PERSON_CONFIG_JSON',
      'CACHE_ENABLED',
      'CACHE_PATH',
      'OUTPUT_FILE_PATH'
    ].forEach(testEnvironment.getVar);

    const personCache = new PersonCacheForS3();
    const { PERSON_CACHE_BUCKET_NAME, PERSON_CACHE_KEY, REGION, OUTPUT_FILE_PATH } = process.env;

    if(OUTPUT_FILE_PATH) {
      // A local file path is provided, so write the population cache to that file instead of S3
      const outputFilePath = OUTPUT_FILE_PATH.endsWith(AbstractPersonCache.CACHE_FILE_NAME) 
        ? OUTPUT_FILE_PATH
        : `${OUTPUT_FILE_PATH}/${AbstractPersonCache.CACHE_FILE_NAME}`;
      console.log(`\n📝 Writing population cache to local file: ${outputFilePath}`);
      await personCache.writeToFile(outputFilePath);
      process.exit(0);
    }

    if (!PERSON_CACHE_BUCKET_NAME) {
      console.error('PERSON_CACHE_BUCKET_NAME environment variable is not set. Please set it to the name of the S3 bucket for the population cache.');
      process.exit(1);
    }

    if (!PERSON_CACHE_KEY) {
      console.error('PERSON_CACHE_KEY environment variable is not set. Please set it to the key (path) in the S3 bucket for the population cache.');
      process.exit(1);
    }

    if (!REGION) {
      console.error('REGION environment variable is not set. Please set it to the AWS region of the S3 bucket for the population cache.');
      process.exit(1);
    }

    try {
      await personCache.setCache({ 
        bucketName: PERSON_CACHE_BUCKET_NAME, 
        key: PERSON_CACHE_KEY, 
        region: REGION 
      });

      const exists = await personCache.cacheExists({
        bucketName: PERSON_CACHE_BUCKET_NAME, 
        key: PERSON_CACHE_KEY, 
        region: REGION 
      });
      console.log(`Cache exists: ${exists}`);

      const cacheSet = await personCache.getCache({ 
        bucketName: PERSON_CACHE_BUCKET_NAME, 
        key: PERSON_CACHE_KEY, 
        region: REGION 
      });

      console.log(`Cache contains ${cacheSet.size} sourceIdentifiers`);
      
    } catch (error) {
      console.error('Error managing S3 population cache:', error);
    }
  })();
}