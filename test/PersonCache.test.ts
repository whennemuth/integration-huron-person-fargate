import { HuronPersonCache } from '../src/PersonCache';
import { S3, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import { sdkStreamMixin } from '@smithy/util-stream';
import { Readable } from 'stream';
import { BasicCache, Config, ListPeople, HuronPerson } from 'integration-huron-person';

// Mock dependencies
jest.mock('integration-huron-person', () => {
  const actual = jest.requireActual('integration-huron-person');
  return {
    ...actual,
    BasicCache: {
      getInstance: jest.fn()
    },
    ConfigManager: {
      getInstance: jest.fn().mockReturnValue({
        reset: jest.fn().mockReturnThis(),
        fromJsonString: jest.fn().mockReturnThis(),
        fromSecretManager: jest.fn().mockReturnThis(),
        fromEnvironment: jest.fn().mockReturnThis(),
        fromFileSystem: jest.fn().mockReturnThis(),
        getConfigAsync: jest.fn().mockResolvedValue({})
      })
    },
    ListPeople: jest.fn()
  };
});

const s3Mock = mockClient(S3);

describe('HuronPersonCache', () => {
  let mockConfig: Config;
  let mockCache: jest.Mocked<BasicCache>;
  let mockListPeople: jest.Mocked<ListPeople>;

  const createMockPerson = (sourceIdentifier: string): HuronPerson => ({
    sourceIdentifier,
    hrn: `HRN-${sourceIdentifier}`,
    id: `ID-${sourceIdentifier}`,
    firstName: 'Test',
    lastName: 'Person',
    employer: { hrn: 'hrn:hrs:orgs:test-employer', name: 'Test Employer' },
    organization: { hrn: 'hrn:hrs:orgs:test-org', name: 'Test Org' }
  });

  beforeEach(() => {
    jest.clearAllMocks();
    s3Mock.reset();

    mockConfig = {
      executionMode: 'people',
      dataSource: {
        person: {
          endpointConfig: {
            baseUrl: 'https://datasource.example.com',
            apiKey: 'test-api-key'
          },
          fetchPath: '/api/persons'
        },
        people: {
          endpointConfig: {
            baseUrl: 'https://datasource.example.com',
            apiKey: 'test-api-key'
          },
          fetchPath: '/api/persons'
        },
        idpName: 'test-idp'
      },
      dataTarget: {
        endpointConfig: {
          baseUrl: 'https://datatarget.example.com',
          authMethod: 'basic',
          loginSvcPath: '/auth/token',
          username: 'dt-user',
          password: 'dt-pass'
        },
        personsPath: '/api/persons/batch',
        organizationsPath: '/api/organizations'
      },
      integration: {
        clientId: 'test-client',
        batchSize: 10,
        timeout: 5000
      },
      storage: {
        type: 'file',
        config: {
          path: './test-data'
        }
      }
    } as Config;

    mockCache = {} as any;
    mockListPeople = {
      listSourceIdentifiers: jest.fn()
    } as any;
    
    (ListPeople as jest.Mock).mockImplementation(() => mockListPeople);
  });

  describe('Constructor', () => {
    it('should create instance with config', () => {
      const cache = new HuronPersonCache({ config: mockConfig });
      expect(cache).toBeDefined();
    });

    it('should create instance with cache', () => {
      const cache = new HuronPersonCache({ cache: mockCache });
      expect(cache).toBeDefined();
    });

    it('should create instance with both config and cache', () => {
      const cache = new HuronPersonCache({ config: mockConfig, cache: mockCache });
      expect(cache).toBeDefined();
    });

    it('should create instance without parameters', () => {
      const cache = new HuronPersonCache();
      expect(cache).toBeDefined();
    });
  });

  describe('getFullPopulationFromTargetAPI', () => {
    it('should fetch people from target API', async () => {
      const mockPeople = [
        createMockPerson('SRC001'),
        createMockPerson('SRC002'),
        createMockPerson('SRC003')
      ];
      
      mockListPeople.listSourceIdentifiers.mockResolvedValue(mockPeople);

      const cache = new HuronPersonCache({ config: mockConfig });
      const result = await cache.getFullPopulationFromTargetAPI();

      expect(result).toEqual(mockPeople);
      expect(result).toHaveLength(3);
      expect(ListPeople).toHaveBeenCalledWith(mockConfig, 500);
      expect(mockListPeople.listSourceIdentifiers).toHaveBeenCalled();
    });

    it('should handle empty population', async () => {
      mockListPeople.listSourceIdentifiers.mockResolvedValue([]);

      const cache = new HuronPersonCache({ config: mockConfig });
      const result = await cache.getFullPopulationFromTargetAPI();

      expect(result).toEqual([]);
      expect(result).toHaveLength(0);
    });

    it('should propagate API errors', async () => {
      mockListPeople.listSourceIdentifiers.mockRejectedValue(new Error('API connection failed'));

      const cache = new HuronPersonCache({ config: mockConfig });
      
      await expect(cache.getFullPopulationFromTargetAPI()).rejects.toThrow('API connection failed');
    });
  });

  describe('setS3PopulationCache', () => {
    it('should write population cache to S3', async () => {
      const mockPeople = [
        createMockPerson('SRC001'),
        createMockPerson('SRC002'),
        createMockPerson('SRC003')
      ];
      
      mockListPeople.listSourceIdentifiers.mockResolvedValue(mockPeople);
      s3Mock.onAnyCommand().resolves({});

      const cache = new HuronPersonCache({ config: mockConfig });
      
      await cache.setS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      // Verify S3 putObject was called
      const putCalls = s3Mock.commandCalls(PutObjectCommand);
      expect(putCalls.length).toBe(1);
      
      const putCall = putCalls[0];
      expect(putCall.args[0].input).toMatchObject({
        Bucket: 'test-bucket',
        Key: 'test-path/_personCache.txt',
        ContentType: 'text/plain'
      });

      // Verify content format (newline-delimited)
      const body = putCall.args[0].input.Body as Buffer;
      const content = body.toString('utf-8');
      const lines = content.split('\n');
      
      expect(lines).toHaveLength(3);
      expect(lines).toContain('SRC001');
      expect(lines).toContain('SRC002');
      expect(lines).toContain('SRC003');

      // Verify metadata
      expect(putCall.args[0].input.Metadata).toMatchObject({
        recordCount: '3'
      });
      expect(putCall.args[0].input.Metadata?.createdAt).toBeDefined();
    });

    it('should handle empty population', async () => {
      mockListPeople.listSourceIdentifiers.mockResolvedValue([]);
      s3Mock.onAnyCommand().resolves({});

      const cache = new HuronPersonCache({ config: mockConfig });
      
      await cache.setS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      const putCalls = s3Mock.commandCalls(PutObjectCommand);
      expect(putCalls.length).toBe(1);
      
      const body = putCalls[0].args[0].input.Body as Buffer;
      const content = body.toString('utf-8');
      
      expect(content).toBe('');
    });

    it('should skip people without sourceIdentifier', async () => {
      const mockPeople = [
        createMockPerson('SRC001'),
        { ...createMockPerson('SRC002'), sourceIdentifier: undefined as any },
        createMockPerson('SRC003'),
        { ...createMockPerson('SRC004'), sourceIdentifier: null as any }
      ];
      
      mockListPeople.listSourceIdentifiers.mockResolvedValue(mockPeople);
      s3Mock.onAnyCommand().resolves({});

      const cache = new HuronPersonCache({ config: mockConfig });
      
      await cache.setS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      const putCalls = s3Mock.commandCalls(PutObjectCommand);
      const body = putCalls[0].args[0].input.Body as Buffer;
      const content = body.toString('utf-8');
      const lines = content.split('\n');
      
      // Should only have 2 valid sourceIdentifiers
      expect(lines).toHaveLength(2);
      expect(lines).toContain('SRC001');
      expect(lines).toContain('SRC003');
      expect(lines).not.toContain('SRC002');
      expect(lines).not.toContain('SRC004');
    });

    it('should handle large populations', async () => {
      // Create 1000 mock people
      const mockPeople = Array.from({ length: 1000 }, (_, i) => 
        createMockPerson(`SRC${String(i).padStart(6, '0')}`)
      );
      
      mockListPeople.listSourceIdentifiers.mockResolvedValue(mockPeople);
      s3Mock.onAnyCommand().resolves({});

      const cache = new HuronPersonCache({ config: mockConfig });
      
      await cache.setS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      const putCalls = s3Mock.commandCalls(PutObjectCommand);
      const body = putCalls[0].args[0].input.Body as Buffer;
      const content = body.toString('utf-8');
      const lines = content.split('\n');
      
      expect(lines).toHaveLength(1000);
      expect(putCalls[0].args[0].input.Metadata?.recordCount).toBe('1000');
    });

    it('should throw error when S3 write fails', async () => {
      const mockPeople = [createMockPerson('SRC001')];
      
      mockListPeople.listSourceIdentifiers.mockResolvedValue(mockPeople);
      s3Mock.onAnyCommand().rejects(new Error('Access Denied'));

      const cache = new HuronPersonCache({ config: mockConfig });
      
      await expect(
        cache.setS3PopulationCache({
          bucketName: 'test-bucket',
          key: 'test-path/_personCache.txt',
          region: 'us-east-2'
        })
      ).rejects.toThrow('Failed to write population cache to S3: Access Denied');
    });
  });

  describe('getS3PopulationCache', () => {
    const createMockS3Response = (sourceIdentifiers: string[]) => {
      const content = sourceIdentifiers.join('\n');
      const stream = sdkStreamMixin(Readable.from([content]));
      return {
        Body: stream,
        Metadata: {
          recordCount: sourceIdentifiers.length.toString(),
          createdAt: '2026-05-05T10:00:00.000Z'
        }
      };
    };

    it('should read population cache from S3', async () => {
      const sourceIds = ['SRC001', 'SRC002', 'SRC003'];
      s3Mock.onAnyCommand().resolves(createMockS3Response(sourceIds));

      const cache = new HuronPersonCache({ config: mockConfig });
      
      const result = await cache.getS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(3);
      expect(result.has('SRC001')).toBe(true);
      expect(result.has('SRC002')).toBe(true);
      expect(result.has('SRC003')).toBe(true);

      // Verify S3 getObject was called correctly
      const getCalls = s3Mock.commandCalls(GetObjectCommand);
      expect(getCalls.length).toBe(1);
      expect(getCalls[0].args[0].input).toMatchObject({
        Bucket: 'test-bucket',
        Key: 'test-path/_personCache.txt'
      });
    });

    it('should handle empty cache file', async () => {
      const stream = sdkStreamMixin(Readable.from(['']));
      s3Mock.onAnyCommand().resolves({ Body: stream, Metadata: {} });

      const cache = new HuronPersonCache({ config: mockConfig });
      
      const result = await cache.getS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(0);
    });

    it('should handle cache file with whitespace and empty lines', async () => {
      const content = 'SRC001\n\nSRC002\n  \nSRC003\n\n';
      const stream = sdkStreamMixin(Readable.from([content]));
      s3Mock.onAnyCommand().resolves({ Body: stream, Metadata: {} });

      const cache = new HuronPersonCache({ config: mockConfig });
      
      const result = await cache.getS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      expect(result.size).toBe(3);
      expect(result.has('SRC001')).toBe(true);
      expect(result.has('SRC002')).toBe(true);
      expect(result.has('SRC003')).toBe(true);
    });

    it('should handle large cache files', async () => {
      // Create 10000 sourceIdentifiers
      const sourceIds = Array.from({ length: 10000 }, (_, i) => 
        `SRC${String(i).padStart(6, '0')}`
      );
      s3Mock.onAnyCommand().resolves(createMockS3Response(sourceIds));

      const cache = new HuronPersonCache({ config: mockConfig });
      
      const result = await cache.getS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      expect(result.size).toBe(10000);
      expect(result.has('SRC000000')).toBe(true);
      expect(result.has('SRC009999')).toBe(true);
    });

    it('should handle missing cache file (NoSuchKey)', async () => {
      const error: any = new Error('The specified key does not exist');
      error.name = 'NoSuchKey';
      s3Mock.onAnyCommand().rejects(error);

      const cache = new HuronPersonCache({ config: mockConfig });
      
      const result = await cache.getS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      // Should return empty Set without throwing
      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(0);
    });

    it('should handle cache file without Body', async () => {
      s3Mock.onAnyCommand().resolves({ Metadata: {} });

      const cache = new HuronPersonCache({ config: mockConfig });
      
      const result = await cache.getS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(0);
    });

    it('should throw error for S3 access errors', async () => {
      s3Mock.onAnyCommand().rejects(new Error('Access Denied'));

      const cache = new HuronPersonCache({ config: mockConfig });
      
      await expect(
        cache.getS3PopulationCache({
          bucketName: 'test-bucket',
          key: 'test-path/_personCache.txt',
          region: 'us-east-2'
        })
      ).rejects.toThrow('Failed to read population cache from S3: Access Denied');
    });

    it('should handle duplicate sourceIdentifiers in cache file', async () => {
      const content = 'SRC001\nSRC002\nSRC001\nSRC003\nSRC002';
      const stream = sdkStreamMixin(Readable.from([content]));
      s3Mock.onAnyCommand().resolves({ Body: stream, Metadata: {} });

      const cache = new HuronPersonCache({ config: mockConfig });
      
      const result = await cache.getS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      // Set should automatically deduplicate
      expect(result.size).toBe(3);
      expect(result.has('SRC001')).toBe(true);
      expect(result.has('SRC002')).toBe(true);
      expect(result.has('SRC003')).toBe(true);
    });
  });

  describe('Integration workflow', () => {
    it('should write and read cache successfully', async () => {
      const mockPeople = [
        createMockPerson('SRC001'),
        createMockPerson('SRC002'),
        createMockPerson('SRC003')
      ];
      
      mockListPeople.listSourceIdentifiers.mockResolvedValue(mockPeople);
      
      let writtenContent: string = '';
      
      // Mock write
      s3Mock.on(PutObjectCommand).callsFake((input: any) => {
        writtenContent = input.Body.toString('utf-8');
        return Promise.resolve({});
      });
      
      // Mock read
      s3Mock.on(GetObjectCommand).callsFake(() => {
        const stream = sdkStreamMixin(Readable.from([writtenContent]));
        return Promise.resolve({
          Body: stream,
          Metadata: { recordCount: '3' }
        });
      });

      const cache = new HuronPersonCache({ config: mockConfig });
      
      // Write cache
      await cache.setS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      // Read cache
      const result = await cache.getS3PopulationCache({
        bucketName: 'test-bucket',
        key: 'test-path/_personCache.txt',
        region: 'us-east-2'
      });

      expect(result.size).toBe(3);
      expect(result.has('SRC001')).toBe(true);
      expect(result.has('SRC002')).toBe(true);
      expect(result.has('SRC003')).toBe(true);
    });
  });

  describe('Static properties', () => {
    it('should have CACHE_FILE_NAME constant', () => {
      expect(HuronPersonCache.CACHE_FILE_NAME).toBe('_personCache.txt');
    });
  });
});
