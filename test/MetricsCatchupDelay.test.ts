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

  it("ends within maxWaitSeconds, whatever the period and buffer add up to (a Lambda's remaining time)", async () => {
    const bounded = new MetricsCatchupDelay({ alarmPeriodSeconds: 60, additionalDelaySeconds: 300, countdownStepSeconds: 5, maxWaitSeconds: 10 });
    let settled = false;
    const result = bounded.startDelay().then(value => { settled = true; return value; });
    await jest.advanceTimersByTimeAsync(9_000);
    expect(settled).toBe(false);
    await jest.advanceTimersByTimeAsync(1_000);
    await expect(result).resolves.toBe(true);
    expect(console.log).toHaveBeenCalledWith(expect.stringContaining('limited to 10s'));
  });

  it('waits not at all when the limit is used up, and never a negative time', async () => {
    const none = new MetricsCatchupDelay({ alarmPeriodSeconds: 60, additionalDelaySeconds: 300, maxWaitSeconds: -5 });
    await expect(none.startDelay()).resolves.toBe(true);
    expect(jest.getTimerCount()).toBe(0);
  });

  it('returns false immediately when the signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(makeDelay().startDelay(controller.signal)).resolves.toBe(false);
  });
});
