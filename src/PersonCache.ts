import { S3 } from '@aws-sdk/client-s3';
import { TestEnvironment } from 'integration-core';
import { BasicCache, Config, ConfigManager, getLocalConfig, HuronPerson, ListPeople } from "integration-huron-person";

/**
 * Class for managing a cache of person data from the target API, stored in S3. 
 * Provides methods to set and get the cache,
 */
export class HuronPersonCache {
  private config: Config | undefined;
  private cache: BasicCache | undefined;

  public static CACHE_FILE_NAME = '_personCache.txt';  

  constructor(params?: { cache?: BasicCache, config?: Config }) {
    const { cache, config } = params || {};
    if (config) {
      this.config = config;
      if( ! cache) {
        this.cache = BasicCache.getInstance(config);
      }
    }
    else if (cache) {
      this.cache = cache;
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

  private getCache = async (): Promise<BasicCache | undefined> => {
    if(this.cache) {
      return this.cache;
    }
    const config = await this.getConfig();
    return BasicCache.getInstance(config);
  }

  public getFullPopulationFromTargetAPI = async (): Promise<HuronPerson[]> => {
    const config = await this.getConfig();
    const listPeople = new ListPeople(config, 500);
    return listPeople.listSourceIdentifiers();
  }

  private getLines = async (): Promise<string[]> => {
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
   * Sets the S3 population cache with the full population from the target API.
   * Writes one sourceIdentifier per line to enable efficient streaming reads.
   * @param params Parameters for the S3 bucket, key, and region.
   */
  public setS3PopulationCache = async (params: { bucketName: string, key: string, region: string }) => {
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
   * Retrieves the S3 population cache as a Set of sourceIdentifiers.
   * Reads newline-delimited sourceIdentifiers from S3 and loads into a Set for O(1) lookup.
   * @param params Parameters for the S3 bucket, key, and region.
   * @returns A Set of sourceIdentifiers from the S3 population cache.
   */
  public getS3PopulationCache = async (params: { bucketName: string, key: string, region: string }): Promise<Set<string>> => {
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

  public writePopulationToFile = async (filePath: string): Promise<void> => {
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

    const personCache = new HuronPersonCache();
    // const people = await personCache.getFullPopulationFromTargetAPI();
    // console.log(`Retrieved ${people.length} people from target API`);
    const { PERSON_CACHE_BUCKET_NAME, PERSON_CACHE_KEY, REGION, OUTPUT_FILE_PATH } = process.env;

    if(OUTPUT_FILE_PATH) {
      // A local file path is provided, so write the population cache to that file instead of S3
      const outputFilePath = OUTPUT_FILE_PATH.endsWith(HuronPersonCache.CACHE_FILE_NAME) 
        ? OUTPUT_FILE_PATH
        : `${OUTPUT_FILE_PATH}/${HuronPersonCache.CACHE_FILE_NAME}`;
      console.log(`\n📝 Writing population cache to local file: ${outputFilePath}`);
      const fs = await import('fs');
      await personCache.writePopulationToFile(outputFilePath);
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
      await personCache.setS3PopulationCache({ 
        bucketName: PERSON_CACHE_BUCKET_NAME, 
        key: PERSON_CACHE_KEY, 
        region: REGION 
      });

      const cacheSet = await personCache.getS3PopulationCache({ 
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