/**
 * Retry strategy with exponential backoff and jitter for handling transient API failures.
 * 
 * Key Features:
 * - Exponential backoff: Delay increases exponentially with each retry (base^attempt)
 * - Full jitter: Randomizes delay to prevent thundering herd problem
 * - Max retries: Configurable limit to prevent infinite loops
 * - Max delay cap: Prevents excessive wait times
 * - Retry conditions: Configurable logic for when to retry (e.g., 429, 5xx errors)
 * 
 * Use Cases:
 * - 429 throttling: Retry with backoff until rate limit resets
 * - 5xx server errors: Retry transient failures
 * - Network timeouts: Retry dropped connections
 * 
 * Algorithm:
 * 1. Check if error is retryable (based on status code)
 * 2. Calculate exponential delay: base^attempt
 * 3. Apply jitter: randomize delay between 0 and calculated value
 * 4. Cap delay at maxDelay to prevent excessive waits
 * 5. Sleep for calculated duration
 * 
 * Example:
 * ```typescript
 * const strategy = new RetryStrategy({ maxRetries: 3, baseDelayMs: 1000 });
 * 
 * for (let attempt = 0; attempt < strategy.maxRetries; attempt++) {
 *   try {
 *     return await apiCall();
 *   } catch (error) {
 *     if (!strategy.shouldRetry(error, attempt)) throw error;
 *     await strategy.delay(attempt);
 *   }
 * }
 * ```
 * 
 * Delay examples (baseDelayMs=1000, exponentialBase=2):
 * - Attempt 0: 0-2000ms (2^0 * 1000 * 2 = 2000ms max)
 * - Attempt 1: 0-4000ms (2^1 * 1000 * 2 = 4000ms max)
 * - Attempt 2: 0-8000ms (2^2 * 1000 * 2 = 8000ms max)
 * - With maxDelayMs=5000, attempt 2+ capped at 0-5000ms
 */

export type RetryStrategyConfig = {
  retryStrategyOptions?: RetryStrategyOptions;
  retryStrategyType?: RetryStrategyType; // Optional string to specify which retry strategy to use (e.g., 'DEFAULT', 'AGGRESSIVE', 'THROTTLING_ONLY')
};

export interface RetryStrategyOptions {
  /** Maximum number of retry attempts (default: 3) */
  maxRetries?: number;
  
  /** Base delay in milliseconds before first retry (default: 1000) */
  baseDelayMs?: number;
  
  /** Exponential base for calculating delay (default: 2 for doubling) */
  exponentialBase?: number;
  
  /** Maximum delay in milliseconds to cap exponential growth (default: 30000 = 30s) */
  maxDelayMs?: number;
  
  /** HTTP status codes that should trigger a retry (default: [429, 500, 502, 503, 504]) */
  retryableStatusCodes?: number[];
  
  /** Custom predicate for determining if an error should be retried */
  shouldRetryPredicate?: (error: any, attemptNumber: number) => boolean;
}

export enum RetryStrategyType {
  DEFAULT = 'DEFAULT',
  AGGRESSIVE = 'AGGRESSIVE',
  THROTTLING_ONLY = 'THROTTLING_ONLY'
}

export class RetryStrategy {
  public readonly maxRetries: number;
  private readonly baseDelayMs: number;
  private readonly exponentialBase: number;
  private readonly maxDelayMs: number;
  private readonly retryableStatusCodes: Set<number>;
  private readonly shouldRetryPredicate?: (error: any, attemptNumber: number) => boolean;

  constructor(options: RetryStrategyOptions = {}) {
    this.maxRetries = options.maxRetries ?? 3;
    this.baseDelayMs = options.baseDelayMs ?? 1000;
    this.exponentialBase = options.exponentialBase ?? 2;
    this.maxDelayMs = options.maxDelayMs ?? 30000;
    this.retryableStatusCodes = new Set(options.retryableStatusCodes ?? [429, 500, 502, 503, 504]);
    this.shouldRetryPredicate = options.shouldRetryPredicate;
  }

  /**
   * Determine if an error should trigger a retry attempt
   * @param error - The error object from axios or other HTTP client
   * @param attemptNumber - Current attempt number (0-indexed)
   * @returns true if the error is retryable, false otherwise
   */
  public shouldRetry(error: any, attemptNumber: number): boolean {
    // Check attempt limit
    if (attemptNumber >= this.maxRetries) {
      return false;
    }

    // Use custom predicate if provided
    if (this.shouldRetryPredicate) {
      return this.shouldRetryPredicate(error, attemptNumber);
    }

    // Check for HTTP status code in response
    const statusCode = error?.response?.status;
    if (statusCode && this.retryableStatusCodes.has(statusCode)) {
      return true;
    }

    // Check for network errors (no response means network issue)
    if (error?.code === 'ECONNREFUSED' || error?.code === 'ETIMEDOUT' || error?.code === 'ENOTFOUND') {
      return true;
    }

    // Check for axios timeout errors
    if (error?.code === 'ECONNABORTED' && error?.message?.includes('timeout')) {
      return true;
    }

    return false;
  }

  /**
   * Calculate delay for the current attempt using exponential backoff with full jitter
   * @param attemptNumber - Current attempt number (0-indexed)
   * @returns Delay in milliseconds
   */
  public calculateDelay(attemptNumber: number): number {
    // Exponential backoff: base^attempt * baseDelay
    const exponentialDelay = Math.pow(this.exponentialBase, attemptNumber) * this.baseDelayMs;
    
    // Cap at maxDelay
    const cappedDelay = Math.min(exponentialDelay, this.maxDelayMs);
    
    // Apply full jitter: random value between 0 and cappedDelay
    // This spreads out retry attempts to prevent thundering herd
    const jitteredDelay = Math.random() * cappedDelay;
    
    return Math.floor(jitteredDelay);
  }

  /**
   * Sleep for the calculated delay duration
   * @param attemptNumber - Current attempt number (0-indexed)
   */
  public async delay(attemptNumber: number): Promise<void> {
    const delayMs = this.calculateDelay(attemptNumber);
    console.log(`[RetryStrategy] Waiting ${delayMs}ms before retry attempt ${attemptNumber + 1}/${this.maxRetries}`);
    return new Promise(resolve => setTimeout(resolve, delayMs));
  }

  /**
   * Execute a function with automatic retry logic
   * @param fn - Async function to execute
   * @param context - Optional context string for logging
   * @returns Result of the function
   */
  public async executeWithRetry<T>(fn: () => Promise<T>, context?: string): Promise<T> {
    let lastError: any;
    
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        return await fn();
      } catch (error: any) {
        lastError = error;
        
        const shouldRetry = this.shouldRetry(error, attempt);
        const statusCode = error?.response?.status || 'unknown';
        
        if (context) {
          console.error(`[RetryStrategy] ${context} failed (attempt ${attempt + 1}/${this.maxRetries + 1}): ${statusCode} - ${error.message}`);
        }
        
        if (!shouldRetry) {
          console.error(`[RetryStrategy] Error is not retryable or max retries exceeded`);
          throw error;
        }
        
        // Delay before next retry (don't delay after the last attempt)
        if (attempt < this.maxRetries) {
          await this.delay(attempt);
        }
      }
    }
    
    // All retries exhausted
    throw lastError;
  }
}

/**
 * Get an instance of RetryStrategy based on JSON configuration string (probably from environment 
 * variable). Technically, retryStrategyOptions and retryStrategyType should not BOTH be specified, but
 * if they are, we prioritize retryStrategyOptions since it is probably custom.
 * 
 * @param retriesJSON JSON string representing the retry strategy configuration
 * @returns Instance of RetryStrategy or undefined if not specified or failed to parse
 * 
 * Expected JSON structure:
 * {
 *   "retryStrategyOptions": {
 *     "maxRetries": 5,
 *     "baseDelayMs": 2000,
 *     "exponentialBase": 2,
 *     "maxDelayMs": 60000,
 *     "retryableStatusCodes": [429, 500, 502, 503, 504]
 *   },
 *   "retryStrategyType": "AGGRESSIVE" // Optional string to specify which retry strategy to use (e.g., 'DEFAULT', 'AGGRESSIVE', 'THROTTLING_ONLY')
 * }
 * 
 * If retryStrategyOptions is provided, it will be used to create a custom RetryStrategy instance.
 * If retryStrategyType is provided without retryStrategyOptions, it will return a predefined strategy.
 * If neither is provided or if parsing fails, it defaults to THROTTLING_ONLY_RETRY_STRATEGY.
 * 
 * Example usage:
 * const retryStrategy = getRetryStrategy(process.env.RETRY_STRATEGY);
 * await retryStrategy.executeWithRetry(() => apiCall(), 'API call');
 * 
 * Note: The retry strategy type strings ('DEFAULT', 'AGGRESSIVE', 'THROTTLING_ONLY') correspond 
 * to predefined strategies that can be used for common scenarios without needing custom options.
 */
export const getRetryStrategy = (retriesJSON: string | undefined): RetryStrategy | undefined => {
  if (!retriesJSON) {
    return undefined;
  }

  try {
    const retries = JSON.parse(retriesJSON) as RetryStrategyConfig;
    const { retryStrategyOptions, retryStrategyType } = retries;
    if (retryStrategyOptions && typeof retryStrategyOptions === 'object') {
      return new RetryStrategy(retryStrategyOptions);
    }
    else if ( retryStrategyType && typeof retryStrategyType === 'string') {
      const { AGGRESSIVE, DEFAULT, THROTTLING_ONLY } = RetryStrategyType
      switch (retryStrategyType) {
        case AGGRESSIVE:
          return AGGRESSIVE_RETRY_STRATEGY;
        case THROTTLING_ONLY:
          return THROTTLING_ONLY_RETRY_STRATEGY;
        case DEFAULT:
          return DEFAULT_RETRY_STRATEGY;
        default:
          return THROTTLING_ONLY_RETRY_STRATEGY; // Default to throttling-only if unrecognized string provided, since it's the safest option for preventing rate limit issues
      }
    }
  } catch (error) {
    console.error(`[RetryStrategy] Failed to parse retry strategy JSON: ${error}`);
  }

  return THROTTLING_ONLY_RETRY_STRATEGY;
};

/**
 * Default retry strategy for Huron API calls
 * - 3 retries max
 * - 1 second base delay
 * - Exponential backoff (2x)
 * - 30 second max delay
 * - Retry on 429, 5xx, network errors
 */
export const DEFAULT_RETRY_STRATEGY = new RetryStrategy({
  maxRetries: 3,
  baseDelayMs: 1000,
  exponentialBase: 2,
  maxDelayMs: 30000,
  retryableStatusCodes: [429, 500, 502, 503, 504],
});

/**
 * Aggressive retry strategy for critical operations
 * - 5 retries max
 * - 2 second base delay
 * - Higher delays to respect rate limits
 */
export const AGGRESSIVE_RETRY_STRATEGY = new RetryStrategy({
  maxRetries: 5,
  baseDelayMs: 2000,
  exponentialBase: 2,
  maxDelayMs: 60000,
  retryableStatusCodes: [429, 500, 502, 503, 504],
});

export const THROTTLING_ONLY_RETRY_STRATEGY = new RetryStrategy({
  maxRetries: 5,
  baseDelayMs: 2000,
  exponentialBase: 2,
  maxDelayMs: 60000,
  retryableStatusCodes: [429], // Only retry on throttling errors
});
