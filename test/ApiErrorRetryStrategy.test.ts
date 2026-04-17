/**
 * Unit tests for ApiErrorRetryStrategy module
 * 
 * Tests retry logic, exponential backoff, jitter, and retry conditions
 */

import {
  RetryStrategy,
  RetryStrategyOptions,
  RetryStrategyType,
  getRetryStrategy,
  DEFAULT_RETRY_STRATEGY,
  AGGRESSIVE_RETRY_STRATEGY,
  THROTTLING_ONLY_RETRY_STRATEGY,
} from '../src/processing/ApiErrorRetryStrategy';

describe('RetryStrategy', () => {
  describe('constructor and defaults', () => {
    it('should use default values when no options provided', () => {
      const strategy = new RetryStrategy();
      expect(strategy.maxRetries).toBe(3);
    });

    it('should use custom values when options provided', () => {
      const options: RetryStrategyOptions = {
        maxRetries: 5,
        baseDelayMs: 2000,
        exponentialBase: 3,
        maxDelayMs: 60000,
        retryableStatusCodes: [429],
      };
      const strategy = new RetryStrategy(options);
      expect(strategy.maxRetries).toBe(5);
    });

    it('should accept custom shouldRetryPredicate', () => {
      const customPredicate = jest.fn(() => true);
      const strategy = new RetryStrategy({ shouldRetryPredicate: customPredicate });
      
      const error = { response: { status: 400 } };
      strategy.shouldRetry(error, 0);
      expect(customPredicate).toHaveBeenCalledWith(error, 0);
    });
  });

  describe('shouldRetry', () => {
    let strategy: RetryStrategy;

    beforeEach(() => {
      strategy = new RetryStrategy({
        maxRetries: 3,
        retryableStatusCodes: [429, 500, 502, 503, 504],
      });
    });

    it('should return false when max retries exceeded', () => {
      const error = { response: { status: 429 } };
      expect(strategy.shouldRetry(error, 3)).toBe(false);
      expect(strategy.shouldRetry(error, 4)).toBe(false);
    });

    it('should return true for retryable status codes', () => {
      const retryableCodes = [429, 500, 502, 503, 504];
      retryableCodes.forEach(code => {
        const error = { response: { status: code } };
        expect(strategy.shouldRetry(error, 0)).toBe(true);
      });
    });

    it('should return false for non-retryable status codes', () => {
      const nonRetryableCodes = [400, 401, 403, 404];
      nonRetryableCodes.forEach(code => {
        const error = { response: { status: code } };
        expect(strategy.shouldRetry(error, 0)).toBe(false);
      });
    });

    it('should return true for network errors (ECONNREFUSED)', () => {
      const error = { code: 'ECONNREFUSED' };
      expect(strategy.shouldRetry(error, 0)).toBe(true);
    });

    it('should return true for timeout errors (ETIMEDOUT)', () => {
      const error = { code: 'ETIMEDOUT' };
      expect(strategy.shouldRetry(error, 0)).toBe(true);
    });

    it('should return true for DNS errors (ENOTFOUND)', () => {
      const error = { code: 'ENOTFOUND' };
      expect(strategy.shouldRetry(error, 0)).toBe(true);
    });

    it('should return true for axios timeout errors', () => {
      const error = { code: 'ECONNABORTED', message: 'timeout of 5000ms exceeded' };
      expect(strategy.shouldRetry(error, 0)).toBe(true);
    });

    it('should use custom predicate when provided', () => {
      const customPredicate = jest.fn((error, attempt) => {
        return error.customField === 'retry-me' && attempt < 2;
      });
      const customStrategy = new RetryStrategy({ shouldRetryPredicate: customPredicate });

      const error1 = { customField: 'retry-me' };
      expect(customStrategy.shouldRetry(error1, 0)).toBe(true);
      expect(customStrategy.shouldRetry(error1, 2)).toBe(false);

      const error2 = { customField: 'do-not-retry' };
      expect(customStrategy.shouldRetry(error2, 0)).toBe(false);
    });
  });

  describe('calculateDelay', () => {
    it('should calculate exponential backoff correctly', () => {
      const strategy = new RetryStrategy({
        baseDelayMs: 1000,
        exponentialBase: 2,
        maxDelayMs: 60000,
      });

      // Mock Math.random to return 1 for predictable testing
      const originalRandom = Math.random;
      Math.random = jest.fn(() => 1);

      // Attempt 0: 2^0 * 1000 = 1000ms max
      const delay0 = strategy.calculateDelay(0);
      expect(delay0).toBe(1000);

      // Attempt 1: 2^1 * 1000 = 2000ms max
      const delay1 = strategy.calculateDelay(1);
      expect(delay1).toBe(2000);

      // Attempt 2: 2^2 * 1000 = 4000ms max
      const delay2 = strategy.calculateDelay(2);
      expect(delay2).toBe(4000);

      // Attempt 3: 2^3 * 1000 = 8000ms max
      const delay3 = strategy.calculateDelay(3);
      expect(delay3).toBe(8000);

      Math.random = originalRandom;
    });

    it('should apply jitter (randomization)', () => {
      const strategy = new RetryStrategy({
        baseDelayMs: 1000,
        exponentialBase: 2,
        maxDelayMs: 60000,
      });

      // Generate multiple delays for the same attempt and verify they differ
      const delays = new Set<number>();
      for (let i = 0; i < 10; i++) {
        delays.add(strategy.calculateDelay(1));
      }

      // With jitter, we should get different values (very unlikely to get all same by chance)
      expect(delays.size).toBeGreaterThan(1);
    });

    it('should cap delay at maxDelayMs', () => {
      const strategy = new RetryStrategy({
        baseDelayMs: 1000,
        exponentialBase: 2,
        maxDelayMs: 5000,
      });

      const originalRandom = Math.random;
      Math.random = jest.fn(() => 1);

      // Attempt 5: 2^5 * 1000 = 32000ms, but should cap at 5000ms
      const delay5 = strategy.calculateDelay(5);
      expect(delay5).toBe(5000);

      Math.random = originalRandom;
    });

    it('should return value between 0 and calculated max', () => {
      const strategy = new RetryStrategy({
        baseDelayMs: 1000,
        exponentialBase: 2,
        maxDelayMs: 60000,
      });

      // Test multiple attempts
      for (let attempt = 0; attempt < 5; attempt++) {
        const delay = strategy.calculateDelay(attempt);
        const expectedMax = Math.min(
          Math.pow(2, attempt) * 1000,
          60000
        );
        expect(delay).toBeGreaterThanOrEqual(0);
        expect(delay).toBeLessThanOrEqual(expectedMax);
      }
    });
  });

  describe('delay', () => {
    beforeEach(() => {
      jest.useFakeTimers();
      jest.spyOn(console, 'log').mockImplementation();
    });

    afterEach(() => {
      jest.useRealTimers();
      jest.restoreAllMocks();
    });

    it('should delay for calculated duration', async () => {
      const strategy = new RetryStrategy({
        baseDelayMs: 1000,
        exponentialBase: 2,
        maxDelayMs: 60000,
      });

      const delayPromise = strategy.delay(0);
      
      // Fast-forward time
      jest.advanceTimersByTime(1000);
      
      await expect(delayPromise).resolves.toBeUndefined();
    });

    it('should log retry information', async () => {
      const strategy = new RetryStrategy({ maxRetries: 3 });
      
      strategy.delay(0);
      
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('[RetryStrategy] Waiting')
      );
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('retry attempt 1/3')
      );
    });
  });

  describe('executeWithRetry', () => {
    beforeEach(() => {
      jest.useFakeTimers();
      jest.spyOn(console, 'log').mockImplementation();
      jest.spyOn(console, 'error').mockImplementation();
    });

    afterEach(() => {
      jest.useRealTimers();
      jest.restoreAllMocks();
    });

    it('should return result on first success', async () => {
      const strategy = new RetryStrategy({ maxRetries: 3 });
      const mockFn = jest.fn().mockResolvedValue('success');

      const result = await strategy.executeWithRetry(mockFn, 'test operation');

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(1);
    });

    it('should retry on retryable errors and eventually succeed', async () => {
      const strategy = new RetryStrategy({ maxRetries: 3, baseDelayMs: 100 });
      const mockFn = jest.fn()
        .mockRejectedValueOnce({ response: { status: 429 }, message: 'Too Many Requests' })
        .mockRejectedValueOnce({ response: { status: 500 }, message: 'Server Error' })
        .mockResolvedValue('success');

      const resultPromise = strategy.executeWithRetry(mockFn, 'test operation');

      // Advance timers for first retry
      await jest.advanceTimersByTimeAsync(200);
      // Advance timers for second retry
      await jest.advanceTimersByTimeAsync(200);

      const result = await resultPromise;

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(3);
    });

    it('should throw error if not retryable', async () => {
      const strategy = new RetryStrategy({ maxRetries: 3 });
      const error = { response: { status: 404 }, message: 'Not Found' };
      const mockFn = jest.fn().mockRejectedValue(error);

      await expect(strategy.executeWithRetry(mockFn, 'test operation')).rejects.toEqual(error);
      expect(mockFn).toHaveBeenCalledTimes(1);
    });

    it('should throw error after max retries exhausted', async () => {
      const strategy = new RetryStrategy({ maxRetries: 2, baseDelayMs: 100 });
      const error = { response: { status: 429 }, message: 'Too Many Requests' };
      const mockFn = jest.fn().mockRejectedValue(error);

      // Create promise but don't await it yet
      const resultPromise = strategy.executeWithRetry(mockFn, 'test operation');

      // Run all timers and handle the rejection
      const result = await Promise.race([
        resultPromise.catch(err => ({ rejected: true, error: err })),
        jest.runAllTimersAsync().then(() => ({ timedOut: true }))
      ]);

      // Wait for any remaining async operations
      await new Promise(resolve => setImmediate(resolve));

      expect(mockFn).toHaveBeenCalledTimes(3); // 1 initial + 2 retries
      
      // Now check the rejection
      await expect(resultPromise).rejects.toEqual(error);
    });

    it('should log errors with context', async () => {
      const strategy = new RetryStrategy({ maxRetries: 1, baseDelayMs: 100 });
      const error = { response: { status: 429 }, message: 'Too Many Requests' };
      const mockFn = jest.fn().mockRejectedValue(error);

      const resultPromise = strategy.executeWithRetry(mockFn, 'API call');
      
      // Run all timers and handle the rejection
      await Promise.race([
        resultPromise.catch(err => err),
        jest.runAllTimersAsync()
      ]);

      // Wait for any remaining async operations
      await new Promise(resolve => setImmediate(resolve));

      await expect(resultPromise).rejects.toEqual(error);

      expect(console.error).toHaveBeenCalledWith(
        expect.stringContaining('[RetryStrategy] API call failed')
      );
      expect(console.error).toHaveBeenCalledWith(
        expect.stringContaining('429')
      );
    });
  });

  describe('predefined strategies', () => {
    it('should export DEFAULT_RETRY_STRATEGY with correct config', () => {
      expect(DEFAULT_RETRY_STRATEGY).toBeInstanceOf(RetryStrategy);
      expect(DEFAULT_RETRY_STRATEGY.maxRetries).toBe(3);
    });

    it('should export AGGRESSIVE_RETRY_STRATEGY with correct config', () => {
      expect(AGGRESSIVE_RETRY_STRATEGY).toBeInstanceOf(RetryStrategy);
      expect(AGGRESSIVE_RETRY_STRATEGY.maxRetries).toBe(5);
    });

    it('should export THROTTLING_ONLY_RETRY_STRATEGY with correct config', () => {
      expect(THROTTLING_ONLY_RETRY_STRATEGY).toBeInstanceOf(RetryStrategy);
      expect(THROTTLING_ONLY_RETRY_STRATEGY.maxRetries).toBe(5);
      
      // Should only retry on 429 errors
      const error429 = { response: { status: 429 } };
      const error500 = { response: { status: 500 } };
      expect(THROTTLING_ONLY_RETRY_STRATEGY.shouldRetry(error429, 0)).toBe(true);
      expect(THROTTLING_ONLY_RETRY_STRATEGY.shouldRetry(error500, 0)).toBe(false);
    });
  });

  describe('getRetryStrategy', () => {
    beforeEach(() => {
      jest.spyOn(console, 'error').mockImplementation();
    });

    afterEach(() => {
      jest.restoreAllMocks();
    });

    it('should return undefined when no JSON provided', () => {
      expect(getRetryStrategy(undefined)).toBeUndefined();
    });

    it('should create custom strategy from retryStrategyOptions', () => {
      const json = JSON.stringify({
        retryStrategyOptions: {
          maxRetries: 7,
          baseDelayMs: 500,
          exponentialBase: 3,
          maxDelayMs: 45000,
          retryableStatusCodes: [429, 503],
        },
      });

      const strategy = getRetryStrategy(json);
      expect(strategy).toBeInstanceOf(RetryStrategy);
      expect(strategy?.maxRetries).toBe(7);
    });

    it('should return DEFAULT_RETRY_STRATEGY when type is DEFAULT', () => {
      const json = JSON.stringify({
        retryStrategyType: RetryStrategyType.DEFAULT,
      });

      const strategy = getRetryStrategy(json);
      expect(strategy).toBe(DEFAULT_RETRY_STRATEGY);
    });

    it('should return AGGRESSIVE_RETRY_STRATEGY when type is AGGRESSIVE', () => {
      const json = JSON.stringify({
        retryStrategyType: RetryStrategyType.AGGRESSIVE,
      });

      const strategy = getRetryStrategy(json);
      expect(strategy).toBe(AGGRESSIVE_RETRY_STRATEGY);
    });

    it('should return THROTTLING_ONLY_RETRY_STRATEGY when type is THROTTLING_ONLY', () => {
      const json = JSON.stringify({
        retryStrategyType: RetryStrategyType.THROTTLING_ONLY,
      });

      const strategy = getRetryStrategy(json);
      expect(strategy).toBe(THROTTLING_ONLY_RETRY_STRATEGY);
    });

    it('should prioritize retryStrategyOptions over retryStrategyType', () => {
      const json = JSON.stringify({
        retryStrategyOptions: {
          maxRetries: 9,
        },
        retryStrategyType: RetryStrategyType.DEFAULT,
      });

      const strategy = getRetryStrategy(json);
      expect(strategy?.maxRetries).toBe(9);
      expect(strategy).not.toBe(DEFAULT_RETRY_STRATEGY);
    });

    it('should return THROTTLING_ONLY_RETRY_STRATEGY for unrecognized type', () => {
      const json = JSON.stringify({
        retryStrategyType: 'INVALID_TYPE',
      });

      const strategy = getRetryStrategy(json);
      expect(strategy).toBe(THROTTLING_ONLY_RETRY_STRATEGY);
    });

    it('should return THROTTLING_ONLY_RETRY_STRATEGY on parse error', () => {
      const invalidJson = 'invalid json{{{';

      const strategy = getRetryStrategy(invalidJson);
      expect(strategy).toBe(THROTTLING_ONLY_RETRY_STRATEGY);
      expect(console.error).toHaveBeenCalledWith(
        expect.stringContaining('[RetryStrategy] Failed to parse retry strategy JSON')
      );
    });

    it('should return THROTTLING_ONLY_RETRY_STRATEGY when JSON is empty object', () => {
      const json = JSON.stringify({});

      const strategy = getRetryStrategy(json);
      expect(strategy).toBe(THROTTLING_ONLY_RETRY_STRATEGY);
    });
  });
});
