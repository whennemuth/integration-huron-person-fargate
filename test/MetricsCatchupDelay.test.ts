/**
 * Tests for MetricsCatchupDelay abort handling
 */

import { MetricsCatchupDelay } from '../src/runner/MetricsCatchupDelay';

describe('MetricsCatchupDelay.startDelay abort', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    jest.clearAllTimers();
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  // No cluster/service/region, so alarm state is always undefined (never OK) and the full delay would run.
  const makeDelay = () => new MetricsCatchupDelay({ alarmPeriodSeconds: 60, additionalDelaySeconds: 300, countdownStepSeconds: 5 });

  it('returns false promptly when aborted mid-sleep, without waiting out the full delay', async () => {
    const controller = new AbortController();
    const result = makeDelay().startDelay(controller.signal);

    await jest.advanceTimersByTimeAsync(12_000);
    controller.abort();

    await expect(result).resolves.toBe(false);
    expect(jest.getTimerCount()).toBe(0);
  });

  it('returns false immediately when the signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(makeDelay().startDelay(controller.signal)).resolves.toBe(false);
  });
});
