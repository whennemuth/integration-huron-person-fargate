import { GetSecretValueCommand, SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { BasicCache, Cache, Config, InMemoryCache} from "integration-huron-person";

/** 
 * If process.env[CACHE_JSON] is set, the value of the secret has somehow been set as JSON - no 
 * lookup necessary 
 */
export const CACHE_JSON = 'cache-json';

/** 
 * If process.env[CACHE_SECRET] is NOT set, the secret will be retrieved from AWS Secrets Manager 
 * (or a local file cache if running locally) 
 */
export const CACHE_SECRET = {
  key: 'integration-secrets',
  env: {
    arn: 'INTEGRATION_SECRETS_ARN',
    region: 'INTEGRATION_SECRETS_REGION'
  } 
}

/**
 * DashboardCache handles caching and retrieval of integration configuration
 * and secrets from AWS Secrets Manager (or a local file cache if running locally).
 */
export class DashboardCache {
  private cache: Cache<string, string>;
  private initialized: boolean = false;

  constructor() { 
    this.cache = BasicCache.getInstance() ?? new InMemoryCache(); // Fallback to in-memory cache if BasicCache is not available
  }

  private async initialize(): Promise<void> {
    if (this.initialized) return;

    const { key, env: { arn, region } } = CACHE_SECRET;

    const cacheJson = process.env[CACHE_JSON];
    if(cacheJson) {
      this.cache.set(key, cacheJson);
    }
    else if (!this.cache.get(key)) {
      const secretArn = process.env[arn];
      const secretRegion = process.env[region] || 'us-east-1';
      if (!secretArn) {
        throw new Error(`Environment variable ${arn} is not set.`);
      }

      // Retrieve secret from AWS Secrets Manager
      const client = new SecretsManagerClient({ region: secretRegion });
      const command = new GetSecretValueCommand({
        SecretId: secretArn,
      });

      try {
        const response = await client.send(command);
        if (response.SecretString) {
          this.cache.set(key, response.SecretString);
        } else {
          throw new Error(`Secret ${secretArn} does not contain a SecretString.`);
        }
      } catch (error: any) {
        throw new Error(`Failed to retrieve secret ${secretArn}: ${error.message}`);
      }
    }

    this.initialized = true;
  }

  public get = async (key:string): Promise<any> => {
    await this.initialize();
    return this.cache.get(key);
  }

  public getConfiguration = async (): Promise<Config> => {
    const { key } = CACHE_SECRET;
    const secretString = await this.get(key);
    const config = secretString ? JSON.parse(secretString) : {} as Config;
    // Log an error if config is empty
    if (Object.keys(config).length === 0) {
      console.error('Warning: Retrieved configuration is empty.');
    }
    return config;
  }
}



if (require.main === module) {
  (async () => {
    const dashboardCache = new DashboardCache();
    try {
      const config = await dashboardCache.getConfiguration();
      console.log('Retrieved Configuration:', JSON.stringify(config, null, 2));
    } catch (error) {
      console.error('Error retrieving configuration:', error);
    }
  })();
}