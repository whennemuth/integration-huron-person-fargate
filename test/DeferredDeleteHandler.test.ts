import { DeferredDeleteHandler, DeferredDeleteHandlerParams } from '../src/merging/DeferredDeleteHandler';
import { TargetPersonDeleteType, Config, HuronPersonDataTarget, ReadPerson } from 'integration-huron-person';
import { BatchStatus, FieldSet } from 'integration-core';
import { TrackingTargetApiErrorProcessor } from '../src/processing/ApiErrorTracking';
import { mockClient } from 'aws-sdk-client-mock';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { sdkStreamMixin } from '@smithy/util-stream';
import { Readable } from 'stream';

// Mock pushAll at the prototype level
const mockPushAll = jest.fn();
HuronPersonDataTarget.prototype.pushAll = mockPushAll;

// Mock ReadPerson for HRN lookups
const mockReadPersonBySourceIdentifier = jest.fn();
ReadPerson.prototype.readPersonBySourceIdentifier = mockReadPersonBySourceIdentifier;

const s3Mock = mockClient(S3);

// Helper functions for creating mock data (available to all tests)
const createMockFieldSet = (sourceIdentifier: string, firstName: string): FieldSet => ({
  hash: `hash-${sourceIdentifier}`,
  fieldValues: [
    { sourceIdentifier },
    { firstName },
    { hrn: `hrn:hrs:person:${sourceIdentifier}` }
  ]
});

const mockNdjsonResponse = (fieldSets: FieldSet[]) => {
  const ndjson = fieldSets.map(fs => JSON.stringify(fs)).join('\n');
  const stream = sdkStreamMixin(Readable.from([ndjson]));
  return { Body: stream };
};

describe('DeferredDeleteHandler', () => {
  let mockConfig: Config;
  let mockCache: any;
  let mockErrorTracker: jest.Mocked<TrackingTargetApiErrorProcessor>;
  let mockParams: DeferredDeleteHandlerParams;

  beforeEach(() => {
    s3Mock.reset();
    jest.clearAllMocks();
    mockPushAll.mockClear(); // Clear mockPushAll call history
    mockReadPersonBySourceIdentifier.mockClear(); // Clear readPersonBySourceIdentifier call history
    
    // Reset environment variable
    delete process.env.PERSON_DELETE_TYPE;

    mockConfig = {
      executionMode: 'people',
      dataSource: {
        people: {
          endpointConfig: {
            baseUrl: 'https://api.example.com',
            apiKey: 'test-key'
          },
          fetchPath: '/persons'
        },
        idpName: 'test-idp'
      },
      dataTarget: {
        endpointConfig: {
          baseUrl: 'https://target.example.com',
          authMethod: 'externalToken',
          loginSvcPath: '/auth/token',
          username: 'user',
          password: 'pass'
        },
        personsPath: '/api/v1/persons/batch',
        organizationsPath: '/api/v1/organizations',
        personDeleteType: TargetPersonDeleteType.SOFT
      },
      integration: {
        clientId: 'test-client',
        batchSize: 10,
        timeout: 5000
      },
      storage: {
        type: 'file',
        config: {}
      }
    } as any;

    mockCache = {
      get: jest.fn(),
      set: jest.fn()
    };

    mockErrorTracker = {
      processError: jest.fn(),
      writeStatistics: jest.fn(),
      getStatisticsSummary: jest.fn().mockReturnValue({
        totalErrors: 0,
        throttleCount: 0,
        errorsByStatus: {}
      })
    } as any;

    mockParams = {
      bucketName: 'test-bucket',
      region: 'us-east-2',
      mergedNdjsonPath: 'deltas/person-full/2026-05-02/previous-input.ndjson',
      baselineNdjsonPath: 'delta-storage/previous-input.ndjson',
      primaryKeyFieldNames: ['sourceIdentifier'],
      config: mockConfig,
      cache: mockCache,
      errorTracker: mockErrorTracker
    };
  });

  describe('getDeleteType', () => {
    it('should return SOFT when PERSON_DELETE_TYPE is "soft"', () => {
      process.env.PERSON_DELETE_TYPE = 'soft';
      expect(DeferredDeleteHandler.getDeleteType()).toBe(TargetPersonDeleteType.SOFT);
    });

    it('should return HARD when PERSON_DELETE_TYPE is "hard"', () => {
      process.env.PERSON_DELETE_TYPE = 'hard';
      expect(DeferredDeleteHandler.getDeleteType()).toBe(TargetPersonDeleteType.HARD);
    });

    it('should return NONE when PERSON_DELETE_TYPE is "none"', () => {
      process.env.PERSON_DELETE_TYPE = 'none';
      expect(DeferredDeleteHandler.getDeleteType()).toBe(TargetPersonDeleteType.NONE);
    });

    it('should default to SOFT when PERSON_DELETE_TYPE is not set', () => {
      delete process.env.PERSON_DELETE_TYPE;
      expect(DeferredDeleteHandler.getDeleteType()).toBe(TargetPersonDeleteType.SOFT);
    });

    it('should default to SOFT and warn when PERSON_DELETE_TYPE is invalid', () => {
      const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation();
      process.env.PERSON_DELETE_TYPE = 'invalid';
      
      expect(DeferredDeleteHandler.getDeleteType()).toBe(TargetPersonDeleteType.SOFT);
      expect(consoleWarnSpy).toHaveBeenCalledWith(
        'Invalid PERSON_DELETE_TYPE: invalid. Defaulting to SOFT.'
      );
      
      consoleWarnSpy.mockRestore();
    });

    it('should be case-insensitive for delete type', () => {
      process.env.PERSON_DELETE_TYPE = 'SOFT';
      expect(DeferredDeleteHandler.getDeleteType()).toBe(TargetPersonDeleteType.SOFT);
    });
  });

  describe('isConfiguredForDeletes', () => {
    it('should return true when delete type is SOFT', () => {
      process.env.PERSON_DELETE_TYPE = 'soft';
      expect(DeferredDeleteHandler.isConfiguredForDeletes()).toBe(true);
    });

    it('should return true when delete type is HARD', () => {
      process.env.PERSON_DELETE_TYPE = 'hard';
      expect(DeferredDeleteHandler.isConfiguredForDeletes()).toBe(true);
    });

    it('should return false when delete type is NONE', () => {
      process.env.PERSON_DELETE_TYPE = 'none';
      expect(DeferredDeleteHandler.isConfiguredForDeletes()).toBe(false);
    });
  });

  describe('processDeletes', () => {
    it('should return no-op result when delete type is NONE', async () => {
      process.env.PERSON_DELETE_TYPE = 'none';
      const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      expect(result).toEqual({
        deletedCount: 0,
        failedCount: 0,
        totalProcessed: 0,
        message: 'Deletion handling disabled'
      });
      
      expect(consoleLogSpy).toHaveBeenCalledWith('Deletion handling is disabled (TargetPersonDeleteType.NONE). No deletes will be processed.');
      consoleLogSpy.mockRestore();
    });

    it('should throw error when delete type is HARD', async () => {
      process.env.PERSON_DELETE_TYPE = 'hard';

      const handler = new DeferredDeleteHandler(mockParams);
      
      await expect(handler.processDeletes()).rejects.toThrow('Hard deletes are not implemented. Use soft deletes instead.');
    });

    it('should default to SOFT delete when delete type is unknown', async () => {
      // Force an unknown type by setting invalid value
      process.env.PERSON_DELETE_TYPE = 'unknown-type';
      const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation();

      // Set up S3 mocks since it will default to SOFT and try to read files
      const records = [
        createMockFieldSet('U00000001', 'Alice')
      ];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(records));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(records));

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Should have warned about invalid type and defaulted to SOFT
      expect(consoleWarnSpy).toHaveBeenCalledWith(
        'Invalid PERSON_DELETE_TYPE: unknown-type. Defaulting to SOFT.'
      );

      // Should complete successfully with no removals (same records in both files)
      expect(result).toEqual({
        deletedCount: 0,
        failedCount: 0,
        totalProcessed: 0,
        message: 'No removals detected'
      });

      consoleWarnSpy.mockRestore();
    });
  });

  describe('handleSoftDeletes', () => {
    it('should soft-delete records that exist in baseline but not in consolidated', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      // Baseline has 3 records: A, B, C
      const baseline = [
        createMockFieldSet('U00000001', 'Alice'),
        createMockFieldSet('U00000002', 'Bob'),
        createMockFieldSet('U00000003', 'Charlie')
      ];

      // Consolidated has only 2 records: B, C (A was removed from source)
      const consolidated = [
        createMockFieldSet('U00000002', 'Bob'),
        createMockFieldSet('U00000003', 'Charlie')
      ];

      // Mock S3 responses
      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      // Mock PersonDataTarget.pushAll
      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [{ status: 'success' }],
        failures: [],
        timestamp: new Date(),
        message: 'Success'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Verify pushAll was called with removed records
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [baseline[0]] // Only Alice should be in removed array
      });

      // Verify result
      expect(result).toEqual({
        deletedCount: 1,
        failedCount: 0,
        totalProcessed: 1,
        message: 'Soft-deleted 1 of 1 records'
      });
    });

    it('should handle multiple removed records correctly', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      // Baseline has 5 records
      const baseline = [
        createMockFieldSet('U00000001', 'Alice'),
        createMockFieldSet('U00000002', 'Bob'),
        createMockFieldSet('U00000003', 'Charlie'),
        createMockFieldSet('U00000004', 'David'),
        createMockFieldSet('U00000005', 'Eve')
      ];

      // Consolidated has only 2 records (3 removed)
      const consolidated = [
        createMockFieldSet('U00000002', 'Bob'),
        createMockFieldSet('U00000005', 'Eve')
      ];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [{}, {}, {}],
        failures: [],
        timestamp: new Date(),
        message: 'Success'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Verify 3 records were identified for removal
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [baseline[0], baseline[2], baseline[3]] // Alice, Charlie, David
      });

      expect(result).toEqual({
        deletedCount: 3,
        failedCount: 0,
        totalProcessed: 3,
        message: 'Soft-deleted 3 of 3 records'
      });
    });

    it('should return no-op result when no removals detected', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      // Both files have the same records
      const records = [
        createMockFieldSet('U00000001', 'Alice'),
        createMockFieldSet('U00000002', 'Bob')
      ];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(records));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(records));

      // mockPushAll will not be called when no removals

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // pushAll should not be called
      expect(mockPushAll).not.toHaveBeenCalled();

      expect(result).toEqual({
        deletedCount: 0,
        failedCount: 0,
        totalProcessed: 0,
        message: 'No removals detected'
      });
    });

    it('should handle partial failure in soft-delete batch', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      const baseline = [
        createMockFieldSet('U00000001', 'Alice'),
        createMockFieldSet('U00000002', 'Bob'),
        createMockFieldSet('U00000003', 'Charlie')
      ];

      const consolidated = [
        createMockFieldSet('U00000001', 'Alice')
      ];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      // Mock partial failure: 1 success, 1 failure
      mockPushAll.mockResolvedValue({
        status: BatchStatus.PARTIAL,
        successes: [{ status: 'success' }],
        failures: [{ status: 'failure', message: 'API error' }],
        timestamp: new Date(),
        message: 'Partial success'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      expect(result).toEqual({
        deletedCount: 1,
        failedCount: 1,
        totalProcessed: 2,
        message: 'Soft-deleted 1 of 2 records'
      });
    });

    it('should handle empty baseline file gracefully', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      const consolidated = [createMockFieldSet('U00000001', 'Alice')];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse([])); // Empty baseline

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      expect(result).toEqual({
        deletedCount: 0,
        failedCount: 0,
        totalProcessed: 0,
        message: 'No removals detected'
      });
    });

    it('should handle missing baseline file (NoSuchKey) gracefully', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      const consolidated = [createMockFieldSet('U00000001', 'Alice')];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      // Mock NoSuchKey error for baseline
      const noSuchKeyError: any = new Error('NoSuchKey');
      noSuchKeyError.name = 'NoSuchKey';
      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).rejects(noSuchKeyError);

      const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation();

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      expect(consoleWarnSpy).toHaveBeenCalledWith(
        expect.stringContaining('File not found')
      );
      
      expect(result).toEqual({
        deletedCount: 0,
        failedCount: 0,
        totalProcessed: 0,
        message: 'No removals detected'
      });

      consoleWarnSpy.mockRestore();
    });

    it('should handle composite primary keys correctly', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      // Use composite key: firstName + lastName
      const paramsWithCompositeKey = {
        ...mockParams,
        primaryKeyFieldNames: ['firstName', 'lastName']
      };

      const baseline = [
        {
          hash: 'hash-1',
          fieldValues: [
            { firstName: 'John' },
            { lastName: 'Doe' },
            { hrn: 'hrn:hrs:person:1' }
          ]
        },
        {
          hash: 'hash-2',
          fieldValues: [
            { firstName: 'Jane' },
            { lastName: 'Smith' },
            { hrn: 'hrn:hrs:person:2' }
          ]
        }
      ];

      const consolidated = [baseline[0]]; // Only John Doe remains

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [{}],
        failures: [],
        timestamp: new Date(),
        message: 'Success'
      });

      const handler = new DeferredDeleteHandler(paramsWithCompositeKey);
      const result = await handler.processDeletes();

      // Jane Smith should be identified for removal
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [baseline[1]]
      });

      expect(result.deletedCount).toBe(1);
    });

    it('should skip records with missing primary key fields', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      const baseline = [
        createMockFieldSet('U00000001', 'Alice'),
        {
          // Missing sourceIdentifier
          hash: 'hash-incomplete',
          fieldValues: [
            { firstName: 'Incomplete' },
            { hrn: 'hrn:hrs:person:incomplete' }
          ]
        }
      ];

      const consolidated: FieldSet[] = [];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [{}],
        failures: [],
        timestamp: new Date(),
        message: 'Success'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Only Alice should be identified (incomplete record skipped due to missing sourceIdentifier)
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [baseline[0]]
      });

      expect(result.deletedCount).toBe(1);
    });
  });

  describe('HRN enrichment', () => {
    it('should enrich records lacking HRN by looking up via sourceIdentifier', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      // Baseline with records that lack HRN (only sourceIdentifier)
      const baseline: FieldSet[] = [
        {
          hash: 'hash-U00001',
          fieldValues: [{ sourceIdentifier: 'U00001' }]
        },
        {
          hash: 'hash-U00002',
          fieldValues: [{ sourceIdentifier: 'U00002' }]
        }
      ];

      const consolidated: FieldSet[] = []; // Both removed

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      // Mock ReadPerson to return HRN for lookups
      mockReadPersonBySourceIdentifier
        .mockResolvedValueOnce([{ hrn: 'hrn:hrs:person:1234', sourceIdentifier: 'U00001' }])
        .mockResolvedValueOnce([{ hrn: 'hrn:hrs:person:5678', sourceIdentifier: 'U00002' }]);

      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [{}, {}],
        failures: [],
        timestamp: new Date(),
        message: 'Success'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Verify ReadPerson was called twice
      expect(mockReadPersonBySourceIdentifier).toHaveBeenCalledTimes(2);
      expect(mockReadPersonBySourceIdentifier).toHaveBeenCalledWith('U00001', ['hrn']);
      expect(mockReadPersonBySourceIdentifier).toHaveBeenCalledWith('U00002', ['hrn']);

      // Verify pushAll received enriched records with HRN
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [
          {
            hash: 'hash-U00001',
            fieldValues: [{ sourceIdentifier: 'U00001' }, { hrn: 'hrn:hrs:person:1234' }]
          },
          {
            hash: 'hash-U00002',
            fieldValues: [{ sourceIdentifier: 'U00002' }, { hrn: 'hrn:hrs:person:5678' }]
          }
        ]
      });

      expect(result.deletedCount).toBe(2);
    });

    it('should skip HRN lookup for records that already have HRN', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      // Baseline with one record that already has HRN
      const baseline: FieldSet[] = [
        {
          hash: 'hash-U00001',
          fieldValues: [
            { sourceIdentifier: 'U00001' },
            { hrn: 'hrn:hrs:person:existing' }
          ]
        }
      ];

      const consolidated: FieldSet[] = [];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [{}],
        failures: [],
        timestamp: new Date(),
        message: 'Success'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Verify ReadPerson was NOT called (record already has HRN)
      expect(mockReadPersonBySourceIdentifier).not.toHaveBeenCalled();

      // Verify pushAll received the original record unchanged
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [baseline[0]]
      });

      expect(result.deletedCount).toBe(1);
    });

    it('should handle records with no sourceIdentifier found in lookup', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      const baseline: FieldSet[] = [
        {
          hash: 'hash-U99999',
          fieldValues: [{ sourceIdentifier: 'U99999' }]
        }
      ];

      const consolidated: FieldSet[] = [];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      // Mock ReadPerson to return empty array (person not found)
      mockReadPersonBySourceIdentifier.mockResolvedValue([]);

      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [],
        failures: [{ error: 'No HRN available' }],
        timestamp: new Date(),
        message: 'Partial failure'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Verify lookup was attempted
      expect(mockReadPersonBySourceIdentifier).toHaveBeenCalledWith('U99999', ['hrn']);

      // Verify pushAll was still called with unenriched record
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [baseline[0]] // Record without HRN
      });

      // PersonDataTarget should handle the missing HRN error
      expect(result.failedCount).toBe(1);
    });

    it('should handle API errors during HRN lookup gracefully', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      const baseline: FieldSet[] = [
        {
          hash: 'hash-U00001',
          fieldValues: [{ sourceIdentifier: 'U00001' }]
        }
      ];

      const consolidated: FieldSet[] = [];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      // Mock ReadPerson to throw error
      mockReadPersonBySourceIdentifier.mockRejectedValue(new Error('API timeout'));

      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [],
        failures: [{ error: 'No HRN available' }],
        timestamp: new Date(),
        message: 'Failure'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Verify lookup was attempted
      expect(mockReadPersonBySourceIdentifier).toHaveBeenCalledWith('U00001', ['hrn']);

      // Verify pushAll was still called with unenriched record (error handled gracefully)
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [baseline[0]]
      });

      expect(result.failedCount).toBe(1);
    });

    it('should handle records missing both HRN and sourceIdentifier', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      const baseline: FieldSet[] = [
        {
          hash: 'hash-incomplete',
          fieldValues: [{ firstName: 'NoIdentifier' }] // Missing both HRN and sourceIdentifier
        }
      ];

      const consolidated: FieldSet[] = [];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Records without primary key cannot be identified as removed
      // They will be filtered out by findRemovedRecords
      expect(mockReadPersonBySourceIdentifier).not.toHaveBeenCalled();
      expect(mockPushAll).not.toHaveBeenCalled(); // No records to delete

      expect(result).toEqual({
        deletedCount: 0,
        failedCount: 0,
        totalProcessed: 0,
        message: 'No removals detected'
      });
    });

    it('should handle multiple persons returned for sourceIdentifier', async () => {
      process.env.PERSON_DELETE_TYPE = 'soft';

      const baseline: FieldSet[] = [
        {
          hash: 'hash-U00001',
          fieldValues: [{ sourceIdentifier: 'U00001' }]
        }
      ];

      const consolidated: FieldSet[] = [];

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'deltas/person-full/2026-05-02/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(consolidated));

      s3Mock.on(GetObjectCommand, {
        Bucket: 'test-bucket',
        Key: 'delta-storage/previous-input.ndjson'
      }).resolves(mockNdjsonResponse(baseline));

      // Mock ReadPerson to return multiple persons (should use first)
      mockReadPersonBySourceIdentifier.mockResolvedValue([
        { hrn: 'hrn:hrs:person:first', sourceIdentifier: 'U00001' },
        { hrn: 'hrn:hrs:person:second', sourceIdentifier: 'U00001' }
      ]);

      mockPushAll.mockResolvedValue({
        status: BatchStatus.SUCCESS,
        successes: [{}],
        failures: [],
        timestamp: new Date(),
        message: 'Success'
      });

      const handler = new DeferredDeleteHandler(mockParams);
      const result = await handler.processDeletes();

      // Verify pushAll received enriched record with FIRST HRN
      expect(mockPushAll).toHaveBeenCalledWith({
        added: [],
        updated: [],
        removed: [
          {
            hash: 'hash-U00001',
            fieldValues: [{ sourceIdentifier: 'U00001' }, { hrn: 'hrn:hrs:person:first' }]
          }
        ]
      });

      expect(result.deletedCount).toBe(1);
    });
  });
});
