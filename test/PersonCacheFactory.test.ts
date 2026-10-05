import { PersonCacheFactory } from '../src/person-cache/PersonCacheFactory';
import { PersonCacheForS3 } from '../src/person-cache/PersonCacheForS3';
import { PersonCacheForDynamoDb } from '../src/person-cache/PersonCacheForDynamoDb';
import { PersonTargetReal } from '../src/person-cache/PersonTargetReal';
import { PersonTargetMocked } from '../src/person-cache/PersonTargetMocked';
import { Config } from 'integration-huron-person';

describe('PersonCacheFactory', () => {
  const mockS3Config: Config = {
    storage: {
      type: 's3',
      config: {
        type: 's3',
        region: 'us-east-1',
        bucketName: 'test-bucket'
      }
    }
  } as unknown as Config;

  const mockDynamoDbConfig: Config = {
    storage: {
      type: 'dynamodb',
      config: {
        type: 'dynamodb',
        region: 'us-east-1',
        currentStateTableName: 'test-current-state',
        historyTableName: 'test-history'
      }
    }
  } as unknown as Config;

  describe('create()', () => {
    it('should create PersonCacheForS3 instance for S3 storage type', () => {
      const cache = PersonCacheFactory.create(mockS3Config);
      expect(cache).toBeInstanceOf(PersonCacheForS3);
    });

    it('should create PersonCacheForDynamoDb instance for DynamoDB storage type', () => {
      const cache = PersonCacheFactory.create(mockDynamoDbConfig);
      expect(cache).toBeInstanceOf(PersonCacheForDynamoDb);
    });

    it('should create PersonCacheForS3 instance with PersonTargetReal by default', () => {
      const cache = PersonCacheFactory.create(mockS3Config) as PersonCacheForS3;
      expect(cache).toBeInstanceOf(PersonCacheForS3);
      expect((cache as any).personTarget).toBeInstanceOf(PersonTargetReal);
    });

    it('should create PersonCacheForS3 instance with PersonTargetMocked when useMockTarget=true', () => {
      const cache = PersonCacheFactory.create(mockS3Config, true) as PersonCacheForS3;
      expect(cache).toBeInstanceOf(PersonCacheForS3);
      expect((cache as any).personTarget).toBeInstanceOf(PersonTargetMocked);
    });

    it('should pass PersonTargetMocked through to PersonCacheForDynamoDb facade', () => {
      const cache = PersonCacheFactory.create(mockDynamoDbConfig, true) as PersonCacheForDynamoDb;
      expect(cache).toBeInstanceOf(PersonCacheForDynamoDb);
      expect((cache as any).personTarget).toBeInstanceOf(PersonTargetMocked);
    });

    it('should handle file storage type and return PersonCacheForS3', () => {
      const fileConfig = {
        ...mockS3Config,
        storage: { ...mockS3Config.storage, type: 'file' }
      } as unknown as Config;
      
      const cache = PersonCacheFactory.create(fileConfig);
      expect(cache).toBeInstanceOf(PersonCacheForS3);
    });

    it('should handle database storage type and return PersonCacheForS3 (fallback)', () => {
      const dbConfig = {
        ...mockS3Config,
        storage: { ...mockS3Config.storage, type: 'database' }
      } as unknown as Config;
      
      const cache = PersonCacheFactory.create(dbConfig);
      expect(cache).toBeInstanceOf(PersonCacheForS3);
    });

    it('should throw error for unsupported storage type', () => {
      const invalidConfig = {
        ...mockS3Config,
        storage: { ...mockS3Config.storage, type: 'invalid' }
      } as unknown as Config;
      
      expect(() => PersonCacheFactory.create(invalidConfig)).toThrow('Unsupported storage type for person cache: invalid');
    });
  });
});
