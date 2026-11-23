/**
 * PersonCacheLookup: a processor reading the run's person cache (bulk reset, or previous storage not
 * trusted). The cache is only a short cut: a person it doesn't hold is looked up in the target
 * directly by UpsertDeltaStrategy, which treats a lookup that THROWS as a new person and creates
 * them. So the lookup waits while the cache is being built, and when it can't have the cache it
 * proceeds with an empty one (direct lookups), never throwing.
 */

const mockCache = {
  cacheExists: jest.fn(),
  isLockActive: jest.fn(),
  getCache: jest.fn()
};

jest.mock('../src/person-cache/PersonCacheFactory', () => ({
  PersonCacheFactory: { create: () => mockCache }
}));

import { Config } from 'integration-huron-person';
import { PersonCacheLookup } from '../src/person-cache/PersonCacheLookup';
import { AbstractPersonCache } from '../src/person-cache/AbstractPersonCache';

const s3Key = 'chunks/person-full/2026-11-12T17:04:00.376Z/chunk-0000.ndjson';
const cacheKey = `chunks/person-full/2026-11-12T17:04:00.376Z/${AbstractPersonCache.CACHE_FILE_NAME}`;
const person = { fieldValues: [{ sourceIdentifier: 'U15364542' }] } as any;

/** A clock that only moves when the lookup sleeps */
const fakeClock = () => {
  let t = 0;
  const sleeps: number[] = [];
  return {
    now: () => t,
    sleep: async (ms: number) => { sleeps.push(ms); t += ms; },
    sleeps
  };
};

describe('PersonCacheLookup', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'warn').mockImplementation();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const lookupWith = (clock: ReturnType<typeof fakeClock>, maxWaitMs?: number) => new PersonCacheLookup({
    config: {} as Config, region: 'us-east-2', bucketName: 'chunks-bucket', maxWaitMs, sleep: clock.sleep, now: clock.now
  });

  it('reads the cache next to the chunk and finds a person in it', async () => {
    const clock = fakeClock();
    mockCache.cacheExists.mockResolvedValue(true);
    mockCache.getCache.mockResolvedValue(new Set(['U15364542']));

    const found = await lookupWith(clock).lookupPersonInTargetSystemCache({ person, s3Key });

    expect(found).toBe('U15364542');
    expect(mockCache.getCache).toHaveBeenCalledWith({ bucketName: 'chunks-bucket', key: cacheKey, region: 'us-east-2' });
    expect(clock.sleeps).toEqual([]);
  });

  it('waits while another task holds the lock, then uses the cache it writes', async () => {
    const clock = fakeClock();
    // Built after 3 minutes, as in preview's 2026-11-12 run (longer than the old ~3-minute limit's first waits)
    mockCache.cacheExists.mockImplementation(async () => clock.now() >= 180_000);
    mockCache.isLockActive.mockImplementation(async () => clock.now() < 180_000);
    mockCache.getCache.mockResolvedValue(new Set(['U15364542']));

    const found = await lookupWith(clock).lookupPersonInTargetSystemCache({ person, s3Key });

    expect(found).toBe('U15364542');
    expect(clock.sleeps.slice(0, 5)).toEqual([1000, 2000, 4000, 8000, 10000]);
    expect(Math.max(...clock.sleeps)).toBe(10000);
  });

  it('keeps waiting past the old ~3-minute limit while the lock holds', async () => {
    const clock = fakeClock();
    mockCache.cacheExists.mockImplementation(async () => clock.now() >= 8 * 60_000);
    mockCache.isLockActive.mockImplementation(async () => clock.now() < 8 * 60_000);
    mockCache.getCache.mockResolvedValue(new Set(['U15364542']));

    await expect(lookupWith(clock).lookupPersonInTargetSystemCache({ person, s3Key })).resolves.toBe('U15364542');
  });

  it('proceeds with direct lookups, not a throw, when the lock outlasts the wait', async () => {
    const clock = fakeClock();
    mockCache.cacheExists.mockResolvedValue(false);
    mockCache.isLockActive.mockResolvedValue(true);
    const lookup = lookupWith(clock, 60_000);

    await expect(lookup.lookupPersonInTargetSystemCache({ person, s3Key })).resolves.toBeUndefined();
    expect(lookup.getCachedSourceIdentifiers()).toEqual(new Set());
    expect(clock.now()).toBeGreaterThanOrEqual(60_000);

    // Loaded once per chunk: the next person doesn't wait again
    const sleepsSoFar = clock.sleeps.length;
    await lookup.lookupPersonInTargetSystemCache({ person: 'U99999999', s3Key });
    expect(clock.sleeps.length).toBe(sleepsSoFar);
  });

  it('by default waits as long as the lock TTL', async () => {
    const clock = fakeClock();
    mockCache.cacheExists.mockResolvedValue(false);
    mockCache.isLockActive.mockResolvedValue(true);

    await expect(lookupWith(clock).lookupPersonInTargetSystemCache({ person, s3Key })).resolves.toBeUndefined();
    expect(clock.now()).toBeGreaterThanOrEqual(AbstractPersonCache.CACHE_LOCK_TTL_MS);
    expect(clock.now()).toBeLessThan(AbstractPersonCache.CACHE_LOCK_TTL_MS + 10_000);
  });

  it('with no cache and no lock, waits once for creation to start, then uses direct lookups', async () => {
    const clock = fakeClock();
    mockCache.cacheExists.mockResolvedValue(false);
    mockCache.isLockActive.mockResolvedValue(false);

    await expect(lookupWith(clock).lookupPersonInTargetSystemCache({ person, s3Key })).resolves.toBeUndefined();
    expect(clock.sleeps).toEqual([2000]);
  });

  it('finds a cache written between its exists check and its lock check', async () => {
    const clock = fakeClock();
    // The builder writes the cache and then releases its lock between the two checks
    mockCache.cacheExists.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
    mockCache.isLockActive.mockResolvedValue(false);
    mockCache.getCache.mockResolvedValue(new Set(['U15364542']));

    await expect(lookupWith(clock).lookupPersonInTargetSystemCache({ person, s3Key })).resolves.toBe('U15364542');
    expect(clock.sleeps).toEqual([]);
  });

  it('proceeds with direct lookups, not a throw, when reading the cache fails', async () => {
    const clock = fakeClock();
    mockCache.cacheExists.mockRejectedValue(Object.assign(new Error('Access Denied'), { name: 'AccessDenied' }));

    await expect(lookupWith(clock).lookupPersonInTargetSystemCache({ person, s3Key })).resolves.toBeUndefined();
  });
});
