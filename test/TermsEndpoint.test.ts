/**
 * The terms endpoint override (Flags.termsEndpoint) travels from the Runner to the processor tasks:
 * Runner -> chunker SQS message -> chunker task parameters -> next chunker messages -> run Flags ->
 * processor config. It is set only for simulated-source runs, so that they get simulated terms.
 */
import { Message } from '@aws-sdk/client-sqs';
import { Config } from 'integration-huron-person';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { ChunkFromAPI } from '../src/chunking/fetch/ChunkFromAPI';
import { MetadataForS3 } from '../src/chunking/metadata/MetadataForS3';
import { applyTermsEndpoint } from '../src/processing/ProcessorCommon';
import { SyncPopulation } from '../docker/chunkTypes';

const mockSend = jest.fn().mockResolvedValue({});
jest.mock('@aws-sdk/client-sqs', () => ({
  ...jest.requireActual('@aws-sdk/client-sqs'),
  SQSClient: jest.fn().mockImplementation(() => ({ send: (...args: any[]) => mockSend(...args) })),
}));

const termsEndpoint = { baseUrl: 'https://sim.lambda-url.us-east-2.on.aws', fetchPath: '/terms' };

const config = {
  dataSource: {
    people: {
      endpointConfig: { baseUrl: 'https://real.example.com', apiKey: 'people-key' },
      fetchPath: '/people'
    },
    terms: {
      endpointConfig: { baseUrl: 'https://real.example.com', apiKey: 'terms-key', timeout: 1234 },
      fetchPath: '/real/terms'
    }
  }
} as unknown as Config;

describe('termsEndpoint propagation', () => {
  beforeEach(() => {
    mockSend.mockClear();
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  it('handleApiEvent puts it on the chunker queue message', async () => {
    await handleApiEvent({
      baseUrl: 'https://sim.lambda-url.us-east-2.on.aws', fetchPath: '/', populationType: SyncPopulation.PersonFull, termsEndpoint
    }, 'https://sqs.example.com/queue');

    const body = JSON.parse(mockSend.mock.calls[0][0].input.MessageBody);
    expect(body.termsEndpoint).toEqual(termsEndpoint);
  });

  it('handleApiEvent leaves it off the message when absent (hybrid and scheduled runs)', async () => {
    await handleApiEvent({
      baseUrl: 'https://real.example.com', fetchPath: '/people', populationType: SyncPopulation.PersonFull
    }, 'https://sqs.example.com/queue');

    const body = JSON.parse(mockSend.mock.calls[0][0].input.MessageBody);
    expect(body).not.toHaveProperty('termsEndpoint');
  });

  it('the chunker reads it from the queue message', () => {
    const chunker = new ChunkFromAPI(JSON.parse(JSON.stringify(config)));
    chunker.setTaskParametersFromQueueMessage({ Body: JSON.stringify({
      baseUrl: 'https://sim.lambda-url.us-east-2.on.aws', fetchPath: '/', populationType: SyncPopulation.PersonFull, termsEndpoint
    }) } as Message);

    expect(chunker.getTermsEndpoint()).toEqual(termsEndpoint);
  });

  it('S3-mode flags include it when present', async () => {
    const writeFile = jest.fn().mockResolvedValue(undefined);
    const metadata = new MetadataForS3({ config, storage: { writeFile } as any });

    await metadata.writeFlags({
      bucketName: 'bucket', chunkDirectory: 'chunks/person-full/2026-01-01T00:00:00.000Z',
      bulkReset: false, trustPreviousStorage: true, syncPopulation: SyncPopulation.PersonFull, termsEndpoint
    });

    expect(JSON.parse(writeFile.mock.calls[0][1]).termsEndpoint).toEqual(termsEndpoint);
  });
});

describe('applyTermsEndpoint (processor)', () => {
  beforeEach(() => jest.spyOn(console, 'log').mockImplementation(() => undefined));
  afterEach(() => jest.restoreAllMocks());

  it('points the terms data source at the override, keeping its apiKey and timeout', () => {
    const result = applyTermsEndpoint(config, termsEndpoint);

    expect(result.dataSource.terms).toEqual({
      endpointConfig: { baseUrl: termsEndpoint.baseUrl, apiKey: 'terms-key', timeout: 1234 },
      fetchPath: '/terms'
    });
    expect(result.dataSource.people).toBe(config.dataSource.people);
  });

  it('does not modify the config it was given', () => {
    applyTermsEndpoint(config, termsEndpoint);
    expect(config.dataSource.terms!.endpointConfig.baseUrl).toBe('https://real.example.com');
    expect(config.dataSource.terms!.fetchPath).toBe('/real/terms');
  });

  it('returns the config unchanged without an override', () => {
    expect(applyTermsEndpoint(config, undefined)).toBe(config);
  });

  it('returns the config unchanged when it has no terms data source', () => {
    const noTerms = { dataSource: { people: config.dataSource.people } } as unknown as Config;
    expect(applyTermsEndpoint(noTerms, termsEndpoint)).toBe(noTerms);
  });
});
