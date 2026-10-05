import { Config } from 'integration-huron-person';
import { BigJsonFetch, BigJsonFetchConfig } from '../src/chunking/fetch/BigJsonFetch';
import { IStorageAdapter } from '../src/storage';

const fetchRaw = jest.fn();

jest.mock('integration-huron-person', () => {
  const actual = jest.requireActual('integration-huron-person');
  return {
    ...actual,
    BuCdmPeopleDataSource: jest.fn().mockImplementation(() => ({
      setQueryParam: jest.fn(),
      fetchRaw: (...args: any[]) => fetchRaw(...args),
      getFetchUrl: () => 'https://api.example.com/people?recordCount=2&offset=0',
      apiClient: { recreateInstance: jest.fn() }
    }))
  };
});

const persons = (...ids: string[]) => ids.map(personid => ({ personid }));

describe('BigJsonFetch end-of-records detection and safety cutoff', () => {
  let writeFile: jest.Mock;

  const newFetcher = (overrides: Partial<BigJsonFetchConfig> = {}) => {
    writeFile = jest.fn().mockResolvedValue(undefined);
    return new BigJsonFetch({
      itemsPerChunk: 2,
      config: { executionMode: 'people' } as unknown as Config,
      outputStorage: { writeFile } as unknown as IStorageAdapter,
      clientId: 'chunks/person-full/2026-10-01T00:00:00.000Z',
      offset: 0,
      iterationLimit: 10,
      ...overrides
    });
  };

  beforeEach(() => {
    fetchRaw.mockReset();
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('continues past a partial batch by default and reports actual record/chunk totals', async () => {
    fetchRaw
      .mockResolvedValueOnce(persons('U1', 'U2'))
      .mockResolvedValueOnce(persons('U3'))
      .mockResolvedValueOnce(persons('U4', 'U5'))
      .mockResolvedValueOnce([]);

    const result = await newFetcher().fetchAndChunk();

    expect(fetchRaw).toHaveBeenCalledTimes(4);
    expect(result.totalRecords).toBe(5);
    expect(result.chunkCount).toBe(3);
    expect(writeFile).toHaveBeenCalledTimes(3);
    expect(result.endOfRecordsDetected).toBe(true);
    expect(result.terminalErrorEncountered).toBe(false);
  });

  it('stops at the first partial batch when stopAtFirstPartial is true', async () => {
    fetchRaw
      .mockResolvedValueOnce(persons('U1', 'U2'))
      .mockResolvedValueOnce(persons('U3'));

    const result = await newFetcher({ stopAtFirstPartial: true }).fetchAndChunk();

    expect(fetchRaw).toHaveBeenCalledTimes(2);
    expect(result.totalRecords).toBe(3);
    expect(result.chunkCount).toBe(2);
    expect(result.endOfRecordsDetected).toBe(true);
  });

  it('does not signal the end when the iteration limit is met on a partial batch by default', async () => {
    fetchRaw
      .mockResolvedValueOnce(persons('U1', 'U2'))
      .mockResolvedValueOnce(persons('U3'));

    const result = await newFetcher({ iterationLimit: 2 }).fetchAndChunk();

    expect(fetchRaw).toHaveBeenCalledTimes(2);
    expect(result.totalRecords).toBe(3);
    expect(result.endOfRecordsDetected).toBe(false);
  });

  it('signals the end without logging an error when the very first batch is empty', async () => {
    fetchRaw.mockResolvedValueOnce([]);

    const result = await newFetcher().fetchAndChunk();

    expect(result.totalRecords).toBe(0);
    expect(result.chunkCount).toBe(0);
    expect(result.endOfRecordsDetected).toBe(true);
    expect(console.error).not.toHaveBeenCalled();
  });

  it('terminates as a terminal error once the run-wide total would exceed maxTotalRecords', async () => {
    fetchRaw.mockResolvedValue(persons('U1', 'U2'));

    const result = await newFetcher({ iterationLimit: 0, maxTotalRecords: 5, runTotalRecordsAtStart: 1 }).fetchAndChunk();

    // 1 + 2 + 2 = 5 is allowed; the third batch would make 7.
    expect(fetchRaw).toHaveBeenCalledTimes(3);
    expect(result.totalRecords).toBe(4);
    expect(result.chunkCount).toBe(2);
    expect(result.terminalErrorEncountered).toBe(true);
    expect(result.terminalErrorMessage).toContain('Safety cutoff');
    expect(result.endOfRecordsDetected).toBe(true);
  });

  it('fails fast without calling the source when completed tasks already exceeded maxTotalRecords', async () => {
    const result = await newFetcher({ maxTotalRecords: 5, runTotalRecordsAtStart: 6 }).fetchAndChunk();

    expect(fetchRaw).not.toHaveBeenCalled();
    expect(result.terminalErrorEncountered).toBe(true);
    expect(result.terminalErrorMessage).toContain('Safety cutoff');
  });

  it('applies no cutoff when maxTotalRecords is 0', async () => {
    fetchRaw
      .mockResolvedValueOnce(persons('U1', 'U2'))
      .mockResolvedValueOnce([]);

    const result = await newFetcher({ maxTotalRecords: 0, runTotalRecordsAtStart: 10_000_000 }).fetchAndChunk();

    expect(result.terminalErrorEncountered).toBe(false);
    expect(result.totalRecords).toBe(2);
  });
});
