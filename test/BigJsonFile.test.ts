import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { sdkStreamMixin } from '@smithy/util-stream';
import { mockClient } from 'aws-sdk-client-mock';
import 'aws-sdk-client-mock-jest';
import { Readable } from 'stream';
import { BigJsonFile, BigJsonFileConfig } from '../src/chunking/filedrop/BigJsonFile';
import { PersonArrayWrapper } from '../src/PersonArrayWrapper';
import { S3StorageAdapter } from '../src/storage';

// Create S3 client mock
const s3Mock = mockClient(S3Client);

describe('BigJsonFile', () => {
  const bucketName = 'test-bucket';
  const s3Storage = new S3StorageAdapter({ bucketName, s3Client: s3Mock as any });
  
  // Wrapper that forces flexible mode (no specific array path detection)
  const flexibleWrapper = {
    detectPersonArrayPath: async () => undefined
  };
  
  const defaultConfig: BigJsonFileConfig = {
    itemsPerChunk: 1000,
    bucketName,
    clientId: 'test-client', // Top-level folder prefix for chunks
    s3Client: s3Mock as any, // Use mocked S3 client for all tests
    personArrayWrapper: new PersonArrayWrapper(s3Storage, 'personid') // Auto-detect structure
  };

  beforeEach(() => {
    s3Mock.reset();
  });

  /**
   * Helper to create a mock S3 GetObject response with a JSON array
   */
  function createMockS3Response(records: any[]): any {
    const jsonContent = JSON.stringify(records);
    const stream = new Readable();
    stream.push(jsonContent);
    stream.push(null);
    // AWS SDK expects streams with SDK mixin
    return sdkStreamMixin(stream);
  }

  /**
   * Helper to create test records
   */
  function createTestRecords(count: number): any[] {
    return Array.from({ length: count }, (_, i) => ({
      personid: `P${String(i + 1).padStart(8, '0')}`, // Changed from 'id' to 'personid'
      name: `Person ${i + 1}`,
      email: `person${i + 1}@example.com`,
      metadata: {
        created: new Date().toISOString(),
        index: i
      }
    }));
  }

  describe('constructor', () => {
    it('should create instance with provided configuration', () => {
      const chunker = new BigJsonFile(defaultConfig);
      expect(chunker).toBeInstanceOf(BigJsonFile);
    });

    it('should accept custom S3 client', () => {
      const customS3 = new S3Client({ region: 'us-west-2' });
      const chunker = new BigJsonFile({
        ...defaultConfig,
        s3Client: customS3
      });
      expect(chunker).toBeInstanceOf(BigJsonFile);
    });
  });

  describe('breakup - chunking by record count', () => {
    it('should chunk file with exact multiple of itemsPerChunk', async () => {
      const s3Key = 'data/people.json';
      const records = createTestRecords(2000); // Exactly 2 chunks

      // Return fresh stream for each GetObjectCommand (needed for auto-detection + streaming)
      s3Mock.on(GetObjectCommand).callsFake(() => ({
        Body: createMockS3Response(records)
      }));

      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkCount).toBe(2);
      expect(result.totalRecords).toBe(2000);
      expect(result.chunkKeys).toHaveLength(2);
      expect(result.chunkKeys[0]).toBe('test-client/chunks/chunk-0000.ndjson');
      expect(result.chunkKeys[1]).toBe('test-client/chunks/chunk-0001.ndjson');

      // Verify S3 operations (2 GetObject calls: 1 for detection + 1 for streaming)
      expect(s3Mock).toHaveReceivedCommandTimes(GetObjectCommand, 2);
      expect(s3Mock).toHaveReceivedCommandTimes(PutObjectCommand, 2);
    });

    it('should handle partial last chunk correctly', async () => {
      const s3Key = 'data/people.json';
      const records = createTestRecords(2500); // 2 full chunks + 1 partial (500 records)

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkCount).toBe(3);
      expect(result.totalRecords).toBe(2500);
      expect(result.chunkKeys).toHaveLength(3);
      expect(result.chunkKeys[2]).toBe('test-client/chunks/chunk-0002.ndjson');
    });

    it('should handle single record files', async () => {
      const s3Key = 'data/single.json';
      const records = createTestRecords(1);

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkCount).toBe(1);
      expect(result.totalRecords).toBe(1);
      expect(result.chunkKeys[0]).toBe('test-client/chunks/chunk-0000.ndjson');
    });

    it('should handle files smaller than itemsPerChunk', async () => {
      const s3Key = 'data/small.json';
      const records = createTestRecords(50); // Less than 1000

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkCount).toBe(1);
      expect(result.totalRecords).toBe(50);
      expect(result.chunkKeys[0]).toBe('test-client/chunks/chunk-0000.ndjson');
    });

    it('should handle deeply nested directory structures', async () => {
      const s3Key = 'deep/nested/path/to/data/file.json';
      const records = createTestRecords(1000);

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkKeys[0]).toBe('test-client/chunks/chunk-0000.ndjson');
    });

    it('should handle files in root directory (no subdirectories)', async () => {
      const s3Key = 'rootfile.json';
      const records = createTestRecords(1000);

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkKeys[0]).toBe('test-client/chunks/chunk-0000.ndjson');
    });
  });

  describe('NDJSON format validation', () => {
    it('should write chunks in NDJSON format with correct content type', async () => {
      const s3Key = 'data/test.json';
      const records = createTestRecords(1500); // Will create 2 chunks

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      await chunker.breakup(s3Key);

      // Verify PutObjectCommand was called with correct parameters
      const putCalls = s3Mock.commandCalls(PutObjectCommand);
      expect(putCalls).toHaveLength(2);

      // Check first chunk
      const firstChunkCall = putCalls[0];
      expect(firstChunkCall.args[0].input.ContentType).toBe('application/x-ndjson');
      expect(firstChunkCall.args[0].input.Key).toBe('test-client/chunks/chunk-0000.ndjson');
      
      // Verify NDJSON format: each line should be valid JSON, ends with newline
      const firstChunkBody = firstChunkCall.args[0].input.Body as string;
      const lines = firstChunkBody.split('\n');
      expect(lines.length).toBe(1001); // 1000 records + 1 trailing newline = 1001 elements after split
      expect(lines[1000]).toBe(''); // Last element should be empty (trailing newline)
      
      // Verify first record
      const firstRecord = JSON.parse(lines[0]);
      expect(firstRecord).toEqual({
        personid: 'P00000001', // Changed from 'id: 1' to match updated createTestRecords
        name: 'Person 1',
        email: 'person1@example.com',
        metadata: expect.any(Object)
      });

      // Verify last record in first chunk
      const lastRecord = JSON.parse(lines[999]);
      expect(lastRecord.personid).toBe('P00001000'); // Changed from id to personid
    });
  });

  describe('chunk numbering', () => {
    it('should pad chunk numbers with leading zeros', async () => {
      const s3Key = 'data/huge.json';
      const records = createTestRecords(12345); // Will create 13 chunks (12x1000 + 1x345)

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkKeys[0]).toBe('test-client/chunks/chunk-0000.ndjson');
      expect(result.chunkKeys[9]).toBe('test-client/chunks/chunk-0009.ndjson');
      expect(result.chunkKeys[12]).toBe('test-client/chunks/chunk-0012.ndjson');
      
      // Verify lexicographic sorting works correctly with padding
      const sorted = [...result.chunkKeys].sort();
      expect(sorted).toEqual(result.chunkKeys);
    });
  });

  describe('custom chunk size', () => {
    it('should respect custom itemsPerChunk configuration', async () => {
      const s3Key = 'data/test.json';
      const records = createTestRecords(2500);

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile({
        ...defaultConfig,
        itemsPerChunk: 500 // Smaller chunks
      });
      
      const result = await chunker.breakup(s3Key);

      expect(result.chunkCount).toBe(5); // 2500 / 500 = 5 chunks
      expect(result.totalRecords).toBe(2500);
    });
  });

  describe('error handling', () => {
    it('should throw error when file has no body', async () => {
      const s3Key = 'data/empty.json';

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: undefined }));

      const chunker = new BigJsonFile(defaultConfig);

      await expect(chunker.breakup(s3Key)).rejects.toThrow(
        `No body returned for s3://${bucketName}/${s3Key}`
      );
    });

    it('should throw error when GetObject fails', async () => {
      const s3Key = 'data/forbidden.json';

      s3Mock.on(GetObjectCommand).rejects(new Error('AccessDenied'));

      const chunker = new BigJsonFile(defaultConfig);

      await expect(chunker.breakup(s3Key)).rejects.toThrow('AccessDenied');
    });
  });

  describe('edge cases', () => {
    it('should handle empty array', async () => {
      const s3Key = 'data/empty-array.json';
      const records: any[] = [];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));

      const chunker = new BigJsonFile(defaultConfig);
      
      // Empty arrays should throw an error since no person records are found
      await expect(chunker.breakup(s3Key)).rejects.toThrow(
        `No person records found in: ${s3Key}`
      );
    });

    it('should handle records with complex nested structures', async () => {
      const s3Key = 'data/complex.json';
      const complexRecords = [
        {
          personid: 'COMPLEX001', // Changed from 'id' to 'personid'
          person: {
            names: [
              { firstName: 'John', lastName: 'Doe' },
              { firstName: 'Jane', lastName: 'Smith' }
            ],
            affiliation: {
              department: { code: 'CS', name: 'Computer Science' },
              office: { building: 'ENG', room: '123' }
            }
          },
          metadata: {
            tags: ['active', 'faculty'],
            permissions: { read: true, write: false }
          }
        }
      ];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(complexRecords) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkCount).toBe(1);
      expect(result.totalRecords).toBe(1);

      // Verify the complex structure was preserved in NDJSON
      const putCall = s3Mock.commandCalls(PutObjectCommand)[0];
      const body = putCall.args[0].input.Body as string;
      const parsed = JSON.parse(body.trim());
      expect(parsed).toEqual(complexRecords[0]);
    });

    it('should handle files with special characters in name', async () => {
      const s3Key = 'data/file-with-special_chars.2024-03-17.json';
      const records = createTestRecords(1000);

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(records) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.chunkKeys[0]).toBe('test-client/chunks/chunk-0000.ndjson');
    });
  });

  describe('realistic person data scenario', () => {
    /**
     * Helper to create realistic person records similar to CDM person data structure
     */
    function createRealisticPersonRecords(count: number): any[] {
      return Array.from({ length: count }, (_, i) => ({
        personid: `U${(12345678 + i).toString().padStart(8, '0')}`,
        personBasic: {
          names: [
            {
              source: 'SAP',
              nameType: 'PRI',
              displayName: '',
              firstName: `TestFirst${i + 1}`,
              middleName: 'M',
              lastName: `TestLast${i + 1}`,
              fullName: `TestLast${i + 1}, TestFirst${i + 1}`,
              prefix: '',
              suffix: '',
              effectiveDate: '20251020'
            },
            {
              nameType: 'PRF',
              firstName: `TestFirst${i + 1}`,
              middleName: '',
              lastName: `TestLast${i + 1}`,
              suffix: '',
              prefix: '',
              effectiveDate: '20240104',
              source: 'Campus Solutions'
            }
          ],
          sex: [
            {
              value: i % 2 === 0 ? 'M' : 'F',
              source: 'SAP'
            }
          ],
          pronouns: [
            {
              list: '',
              source: 'Campus Solutions'
            }
          ],
          dataRestriction: [
            {
              group: {
                code: 'FERPA',
                isActive: 'N',
                subGroup: null
              }
            }
          ]
        },
        personDetails: {
          affiliation: ['employee', 'student'],
          account: [
            {
              name: `TEST${i + 1}`,
              source: 'SAP'
            }
          ],
          maritalStatus: [
            {
              code: 'S',
              description: 'Single',
              source: 'Campus Solutions'
            }
          ],
          ethnicity: [
            {
              isHispanicLatino: 'N',
              group: {
                code: 'R5',
                description: 'White'
              },
              source: 'SAP'
            }
          ],
          citizenship: [
            {
              country: {
                code: 'US'
              },
              source: 'Campus Solutions'
            }
          ],
          contact: {
            email: [
              {
                address: `test${i + 1}@example.edu`,
                type: 'BUSN',
                isPrimary: true,
                source: 'SAP'
              }
            ],
            phone: [
              {
                number: `555-010-${(1000 + i).toString().slice(-4)}`,
                type: 'BUSN',
                source: 'SAP'
              }
            ],
            address: [
              {
                street1: `${100 + i} Main Street`,
                city: 'Boston',
                state: 'MA',
                postalCode: '02215',
                country: 'US',
                type: 'BUSN',
                source: 'SAP'
              }
            ]
          }
        }
      }));
    }

    it('should stream and chunk 10 realistic person records from API response structure', async () => {
      const apiResponseKey = 'data/api-response.json';
      const personRecords = createRealisticPersonRecords(10);

      // Wrap in the API response structure: [{response_code: 200, response: [persons...]}]
      const apiResponseStructure = [
        {
          response_code: 200,
          response: personRecords
        }
      ];

      // Mock: BigJsonFile streams API response structure directly, finds persons, writes chunks
      s3Mock.on(GetObjectCommand, { Bucket: bucketName, Key: apiResponseKey })
        .callsFake(() => ({ Body: createMockS3Response(apiResponseStructure) } as any));
      
      // Mock chunk writes
      s3Mock.on(PutObjectCommand).resolves({});

      // Execute: BigJsonFile handles everything in one streaming pass
      const chunker = new BigJsonFile({
        ...defaultConfig,
        itemsPerChunk: 3
      });
      
      const chunkResult = await chunker.breakup(apiResponseKey);

      // Verify chunk metadata - should find and chunk the 10 person records
      expect(chunkResult.chunkCount).toBe(4);
      expect(chunkResult.totalRecords).toBe(10);
      expect(chunkResult.chunkKeys).toHaveLength(4);
      expect(chunkResult.chunkKeys[0]).toBe('test-client/chunks/chunk-0000.ndjson');
      expect(chunkResult.chunkKeys[1]).toBe('test-client/chunks/chunk-0001.ndjson');
      expect(chunkResult.chunkKeys[2]).toBe('test-client/chunks/chunk-0002.ndjson');
      expect(chunkResult.chunkKeys[3]).toBe('test-client/chunks/chunk-0003.ndjson');

      // Verify PutObjectCommand calls (4 chunks)
      const putCalls = s3Mock.commandCalls(PutObjectCommand);
      expect(putCalls.length).toBe(4);

      // Verify first chunk has 3 person records
      const chunk0Call = putCalls.find(c => c.args[0].input.Key?.includes('chunk-0000'));
      expect(chunk0Call).toBeDefined();
      const chunk0Body = chunk0Call!.args[0].input.Body as string;
      const chunk0Lines = chunk0Body.trim().split('\n');
      expect(chunk0Lines).toHaveLength(3);
      
      const chunk0Person1 = JSON.parse(chunk0Lines[0]);
      expect(chunk0Person1.personid).toBe('U12345678');
      expect(chunk0Person1.personBasic.names[0].firstName).toBe('TestFirst1');

      // Verify fourth chunk has 1 person record
      const chunk3Call = putCalls.find(c => c.args[0].input.Key?.includes('chunk-0003'));
      expect(chunk3Call).toBeDefined();
      const chunk3Body = chunk3Call!.args[0].input.Body as string;
      const chunk3Lines = chunk3Body.trim().split('\n');
      expect(chunk3Lines).toHaveLength(1);
      
      const chunk3Person1 = JSON.parse(chunk3Lines[0]);
      expect(chunk3Person1.personid).toBe('U12345687');

      // Verify all chunks have correct ContentType
      putCalls.forEach(call => {
        expect(call.args[0].input.ContentType).toBe('application/x-ndjson');
      });
    });
  });

  describe('extractPersons - nesting depth variations', () => {
    it('should find persons at top level of array', async () => {
      const s3Key = 'data/top-level.json';
      const persons = [
        { personid: 'TOP001', name: 'Person 1' },
        { personid: 'TOP002', name: 'Person 2' }
      ];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(persons) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.totalRecords).toBe(2);
      expect(result.chunkCount).toBe(1);
    });

    it('should find persons nested one level deep in objects', async () => {
      const s3Key = 'data/nested-one-level.json';
      const data = [
        {
          status: 'success',
          persons: [
            { personid: 'NEST001', name: 'Person 1' },
            { personid: 'NEST002', name: 'Person 2' }
          ]
        }
      ];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(data) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.totalRecords).toBe(2);
      expect(result.chunkCount).toBe(1);

      // Verify the persons were extracted correctly
      const putCall = s3Mock.commandCalls(PutObjectCommand)[0];
      const body = putCall.args[0].input.Body as string;
      const lines = body.trim().split('\n');
      expect(lines).toHaveLength(2);
      
      const person1 = JSON.parse(lines[0]);
      expect(person1.personid).toBe('NEST001');
    });

    it('should find persons nested multiple levels deep', async () => {
      const s3Key = 'data/deeply-nested.json';
      const data = [
        {
          api_response: {
            data: {
              results: {
                people: [
                  { personid: 'DEEP001', name: 'Person 1' },
                  { personid: 'DEEP002', name: 'Person 2' }
                ]
              }
            }
          }
        }
      ];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(data) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile({ ...defaultConfig, personArrayWrapper: flexibleWrapper });
      const result = await chunker.breakup(s3Key);

      expect(result.totalRecords).toBe(2);
      expect(result.chunkCount).toBe(1);
    });

    it('should find persons scattered across multiple nesting levels', async () => {
      const s3Key = 'data/scattered.json';
      const data = [
        {
          // Person at level 1
          person: { personid: 'SCAT001', name: 'Person 1' },
          // Persons at level 2
          data: {
            person: { personid: 'SCAT002', name: 'Person 2' },
            // Persons at level 3
            nested: {
              people: [
                { personid: 'SCAT003', name: 'Person 3' },
                { personid: 'SCAT004', name: 'Person 4' }
              ]
            }
          }
        }
      ];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(data) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.totalRecords).toBe(4); // Should find all 4 persons
      expect(result.chunkCount).toBe(1);

      // Verify all persons were found
      const putCall = s3Mock.commandCalls(PutObjectCommand)[0];
      const body = putCall.args[0].input.Body as string;
      const lines = body.trim().split('\n');
      expect(lines).toHaveLength(4);
      
      const personIds = lines.map(line => JSON.parse(line).personid);
      expect(personIds).toEqual(['SCAT001', 'SCAT002', 'SCAT003', 'SCAT004']);
    });

    it('should handle mixed arrays and objects with persons at various depths', async () => {
      const s3Key = 'data/mixed-structure.json';
      const data = [
        {
          batch: [
            {
              records: [
                { personid: 'MIX001', name: 'Person 1' },
                { personid: 'MIX002', name: 'Person 2' }
              ]
            },
            {
              records: [
                { personid: 'MIX003', name: 'Person 3' }
              ]
            }
          ]
        },
        {
          single: { personid: 'MIX004', name: 'Person 4' }
        }
      ];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(data) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile({ ...defaultConfig, personArrayWrapper: flexibleWrapper });
      const result = await chunker.breakup(s3Key);

      expect(result.totalRecords).toBe(4);
      expect(result.chunkCount).toBe(1);
    });

    it('should ignore objects without personid field regardless of depth', async () => {
      const s3Key = 'data/with-non-persons.json';
      const data = [
        {
          metadata: { id: 'META001', type: 'config' }, // Not a person (no personid)
          settings: { value: 123 },
          persons: [
            { personid: 'REAL001', name: 'Real Person 1' },
            { id: 'FAKE001', name: 'Fake Person' }, // Not a person (has 'id' not 'personid')
            { personid: 'REAL002', name: 'Real Person 2' }
          ]
        }
      ];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(data) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile(defaultConfig);
      const result = await chunker.breakup(s3Key);

      expect(result.totalRecords).toBe(2); // Only the 2 with personid
      
      const putCall = s3Mock.commandCalls(PutObjectCommand)[0];
      const body = putCall.args[0].input.Body as string;
      const lines = body.trim().split('\n');
      
      const personIds = lines.map(line => JSON.parse(line).personid);
      expect(personIds).toEqual(['REAL001', 'REAL002']);
    });

    it('should handle empty nested structures without errors', async () => {
      const s3Key = 'data/empty-nested.json';
      const data = [
        {
          data: {
            persons: [] // Empty array
          }
        },
        {
          persons: null // Null value
        },
        {
          nested: {
            deep: {
              persons: [
                { personid: 'EMPTY001', name: 'Only Person' }
              ]
            }
          }
        }
      ];

      s3Mock.on(GetObjectCommand).callsFake(() => ({ Body: createMockS3Response(data) }));
      s3Mock.on(PutObjectCommand).resolves({});

      const chunker = new BigJsonFile({ ...defaultConfig, personArrayWrapper: flexibleWrapper });
      const result = await chunker.breakup(s3Key);

      expect(result.totalRecords).toBe(1);
      expect(result.chunkCount).toBe(1);
    });
  });
});
