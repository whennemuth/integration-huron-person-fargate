/**
 * A single-request source (one person by BUID) runs as a population of one that depletes like the real
 * people source: the run's first fetch, by any chunker task, gets the person, and every later fetch gets
 * an empty batch. So the pipeline runs exactly as in a full sync (chaining, the fetch loop, ending on an
 * empty batch) and a chained task can't chunk the same person again (preview, 2026-11-12).
 */
import { BuCdmPeopleDataSource, Config, DataSourceConfig } from 'integration-huron-person';
import {
  DepletingSingleRequestDataSource, isSingleRequestSource, localSingleRequestFetchClaim
} from '../src/chunking/fetch/SingleRequestSource';
import { BigJsonFetch } from '../src/chunking/fetch/BigJsonFetch';
import { IStorageAdapter } from '../src/storage';

const peopleConfig = (baseUrl: string, fetchPath: string, executionMode: string = 'people'): Config => ({
  executionMode,
  dataSource: {
    people: { endpointConfig: { baseUrl, apiKey: 'people-api-key' }, fetchPath } as DataSourceConfig
  },
  integration: { timeout: 5000 }
} as unknown as Config);

const personConfig = () => peopleConfig('https://cdm.example.edu', '/v1/huronIRBPerson?buid=U15364542');

describe('isSingleRequestSource', () => {
  it('is false for the paged people endpoint', () => {
    expect(isSingleRequestSource(peopleConfig('https://cdm.example.edu', '/v1/huronIRBPeople'))).toBe(false);
  });

  it('is true for a fetch path that names one person by buid', () => {
    expect(isSingleRequestSource(personConfig())).toBe(true);
  });

  it('is true for person-mode config whatever the URL', () => {
    expect(isSingleRequestSource(peopleConfig('https://cdm.example.edu', '/v1/huronIRBPeople', 'person'))).toBe(true);
  });

  it('is true for a simulator person path (stateless /person?buid=)', () => {
    expect(isSingleRequestSource(peopleConfig('https://abc.lambda-url.us-east-2.on.aws', '/person?buid=U1&seed=7'))).toBe(true);
  });

  it('is false when another query parameter only mentions buid in its value', () => {
    expect(isSingleRequestSource(peopleConfig('https://cdm.example.edu', '/v1/huronIRBPeople?filter=buid'))).toBe(false);
  });

  it('is false without a people fetch path or config, and when the URL cannot be parsed', () => {
    expect(isSingleRequestSource(undefined)).toBe(false);
    expect(isSingleRequestSource({ dataSource: {} } as unknown as Config)).toBe(false);
    expect(isSingleRequestSource(peopleConfig('not a url', 'also not a url?buid=1'))).toBe(false);
  });
});

describe('localSingleRequestFetchClaim', () => {
  it('is won by the first call only', async () => {
    const claim = localSingleRequestFetchClaim();
    expect(await claim()).toBe(true);
    expect(await claim()).toBe(false);
    expect(await claim()).toBe(false);
  });
});

describe('A single-request source depletes after the run\'s one fetch', () => {
  let fetchedUrls: string[];
  let realFetch: jest.SpyInstance;
  let writeFile: jest.Mock;

  beforeEach(() => {
    fetchedUrls = [];
    // The real fetch: records the URL it would request and returns the one person
    realFetch = jest.spyOn(BuCdmPeopleDataSource.prototype, 'fetchRaw').mockImplementation(async function (this: BuCdmPeopleDataSource) {
      fetchedUrls.push(this.getFetchUrl());
      return [{ personid: 'U15364542' }];
    });
    writeFile = jest.fn().mockResolvedValue(undefined);
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  // A chunker task's fetch, as ChunkFromAPI configures it (the run's claim shared by every task)
  const chunkerTask = (config: Config, offset: number, claimSingleRequestFetch: () => Promise<boolean>) => new BigJsonFetch({
    itemsPerChunk: 200,
    config,
    outputStorage: { writeFile } as unknown as IStorageAdapter,
    clientId: 'chunks/person-full/2026-11-12T17:04:00.376Z',
    offset,
    iterationLimit: 10,
    claimSingleRequestFetch
  });

  it('the data source fetches only when it wins the claim, and never sends paging parameters', async () => {
    const claim = jest.fn().mockResolvedValueOnce(true).mockResolvedValueOnce(false);
    const dataSource = new DepletingSingleRequestDataSource({ config: personConfig() }, claim);
    dataSource.setQueryParam('recordCount', 200);
    dataSource.setQueryParam('offset', 0);

    expect(await dataSource.fetchRaw()).toEqual([{ personid: 'U15364542' }]);
    expect(await dataSource.fetchRaw()).toEqual([]);
    expect(realFetch).toHaveBeenCalledTimes(1);
    expect(fetchedUrls).toEqual(['https://cdm.example.edu/v1/huronIRBPerson?buid=U15364542']);
  });

  it('the first task chunks the person, then ends on an empty batch as a full sync does', async () => {
    const result = await chunkerTask(personConfig(), 0, localSingleRequestFetchClaim()).fetchAndChunk();

    expect(realFetch).toHaveBeenCalledTimes(1);
    expect(result.chunkCount).toBe(1);
    expect(result.totalRecords).toBe(1);
    expect(result.endOfRecordsDetected).toBe(true);
    expect(fetchedUrls).toEqual(['https://cdm.example.edu/v1/huronIRBPerson?buid=U15364542']);
  });

  it('a chained task finds the source depleted, whichever task fetches first', async () => {
    // Preview's 2026-11-12 order: the chained task (offset 10) fetched while the first built the cache
    const runClaim = localSingleRequestFetchClaim();
    const chained = await chunkerTask(personConfig(), 10, runClaim).fetchAndChunk();
    const first = await chunkerTask(personConfig(), 0, runClaim).fetchAndChunk();

    expect(realFetch).toHaveBeenCalledTimes(1);
    expect(chained.chunkCount + first.chunkCount).toBe(1);
    expect(writeFile).toHaveBeenCalledTimes(1);
    expect(chained.endOfRecordsDetected).toBe(true);
    expect(first.endOfRecordsDetected).toBe(true);
    expect(first.chunkCount).toBe(0);
  });

  it('a paged source is fetched as before, with paging parameters and no claim', async () => {
    const claim = jest.fn();
    realFetch.mockImplementation(async function (this: BuCdmPeopleDataSource) {
      fetchedUrls.push(this.getFetchUrl());
      return [];
    });

    const result = await chunkerTask(peopleConfig('https://cdm.example.edu', '/v1/huronIRBPeople'), 0, claim).fetchAndChunk();

    expect(claim).not.toHaveBeenCalled();
    expect(result.chunkCount).toBe(0);
    expect(fetchedUrls).toEqual(['https://cdm.example.edu/v1/huronIRBPeople?recordCount=200&offset=0']);
  });
});
