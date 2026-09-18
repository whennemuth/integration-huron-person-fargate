/**
 * Tests for ChunkPopulationReader
 *
 * Verifies that raw chunk-*.ndjson files for a sync run are read and converted into the
 * FieldSet[] shape (keyed by sourceIdentifier) that deferred-delete diffing expects.
 */

import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import { ChunkPopulationReader } from '../src/merging/ChunkPopulationReader';

const s3Mock = mockClient(S3Client);

describe('ChunkPopulationReader', () => {
  const bucketName = 'test-bucket';
  const chunkDirectory = 'chunks/person-full/2026-05-26T15:00:00.000Z';
  const region = 'us-east-2';

  beforeEach(() => {
    s3Mock.reset();
  });

  it('returns an empty population when there are no chunk files', async () => {
    s3Mock.on(ListObjectsV2Command).resolves({ Contents: [], IsTruncated: false });

    const reader = new ChunkPopulationReader({ bucketName, chunkDirectory, region });
    const population = await reader.getCurrentPopulation();

    expect(population).toEqual([]);
  });

  it('parses records from every chunk file into sourceIdentifier-keyed FieldSets', async () => {
    const chunkKeys = [
      `${chunkDirectory}/chunk-0000.ndjson`,
      `${chunkDirectory}/chunk-0001.ndjson`,
    ];
    s3Mock.on(ListObjectsV2Command).resolves({
      Contents: chunkKeys.map(Key => ({ Key })),
      IsTruncated: false,
    });

    let callCount = 0;
    s3Mock.on(GetObjectCommand).callsFake(async () => {
      const responses = [
        { Body: { transformToString: async () => '{"personid":"U1"}\n{"personid":"U2"}' } },
        { Body: { transformToString: async () => '{"personid":"U3"}' } },
      ];
      return responses[callCount++];
    });

    const reader = new ChunkPopulationReader({ bucketName, chunkDirectory, region });
    const population = await reader.getCurrentPopulation();

    expect(population).toEqual([
      { fieldValues: [{ sourceIdentifier: 'U1' }] },
      { fieldValues: [{ sourceIdentifier: 'U2' }] },
      { fieldValues: [{ sourceIdentifier: 'U3' }] },
    ]);
  });

  it('uses a custom personIdField when provided', async () => {
    s3Mock.on(ListObjectsV2Command).resolves({
      Contents: [{ Key: `${chunkDirectory}/chunk-0000.ndjson` }],
      IsTruncated: false,
    });
    s3Mock.on(GetObjectCommand).resolves({
      Body: { transformToString: async () => '{"buid":"U99"}' } as any
    });

    const reader = new ChunkPopulationReader({ bucketName, chunkDirectory, region, personIdField: 'buid' });
    const population = await reader.getCurrentPopulation();

    expect(population).toEqual([{ fieldValues: [{ sourceIdentifier: 'U99' }] }]);
  });

  it('skips unparseable lines and records missing the personIdField', async () => {
    s3Mock.on(ListObjectsV2Command).resolves({
      Contents: [{ Key: `${chunkDirectory}/chunk-0000.ndjson` }],
      IsTruncated: false,
    });
    s3Mock.on(GetObjectCommand).resolves({
      Body: { transformToString: async () => '{not valid json}\n{"personid":"U1"}\n{"firstname":"NoId"}' } as any
    });

    const reader = new ChunkPopulationReader({ bucketName, chunkDirectory, region });
    const population = await reader.getCurrentPopulation();

    expect(population).toEqual([{ fieldValues: [{ sourceIdentifier: 'U1' }] }]);
  });
});
