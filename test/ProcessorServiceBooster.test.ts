/**
 * Tests for ProcessorServiceBooster
 */

import { GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { mockClient } from 'aws-sdk-client-mock';
import { DesiredCount } from '../src/DesiredCount';
import { MetricsCatchupDelay } from '../src/runner/MetricsCatchupDelay';
import { ProcessorServiceBooster } from '../src/chunking/fetch/ProcessorServiceBooster';

jest.mock('../src/DesiredCount');
jest.mock('../src/runner/MetricsCatchupDelay');

const sqsMock = mockClient(SQSClient);

const baseParams = {
  clusterName: 'test-cluster',
  serviceName: 'test-processor-service',
  queueUrl: 'https://sqs.us-east-2.amazonaws.com/123456789012/processor-queue',
  region: 'us-east-2',
};

const mockQueueDepth = (count: number) => {
  sqsMock.on(GetQueueAttributesCommand).resolves({
    Attributes: { ApproximateNumberOfMessages: String(count) }
  });
};

const makeMetadataBroker = (overrides: { claim?: boolean } = {}) => ({
  claimProcessorBoost: jest.fn().mockResolvedValue(overrides.claim ?? true),
  releaseProcessorBoostClaim: jest.fn().mockResolvedValue(undefined),
});

describe('ProcessorServiceBooster.checkAndBoostIfNeeded', () => {
  let getCurrent: jest.Mock;
  let getMax: jest.Mock;
  let setTo: jest.Mock;
  let startDelay: jest.Mock;

  beforeEach(() => {
    sqsMock.reset();
    getCurrent = jest.fn();
    getMax = jest.fn();
    setTo = jest.fn();
    startDelay = jest.fn().mockResolvedValue(true);
    (DesiredCount as jest.Mock).mockImplementation(() => ({ getCurrent, getMax, setTo }));
    (MetricsCatchupDelay as jest.Mock).mockImplementation(() => ({ startDelay }));
  });

  afterEach(() => jest.clearAllMocks());

  it('claims, waits for the alarm to clear, and boosts when the criterion is met', async () => {
    getCurrent.mockResolvedValue(1);
    getMax.mockResolvedValue(5);
    mockQueueDepth(11);
    const metadataBroker = makeMetadataBroker();

    await new ProcessorServiceBooster(metadataBroker as any, { ...baseParams, claimedByChunk: '840' }).checkAndBoostIfNeeded();

    expect(metadataBroker.claimProcessorBoost).toHaveBeenCalledWith('840');
    expect(startDelay).toHaveBeenCalledTimes(1);
    expect(setTo).toHaveBeenCalledWith(5);
    expect(metadataBroker.releaseProcessorBoostClaim).not.toHaveBeenCalled();
  });

  it('does not boost when desiredCount already equals max (no claim attempted)', async () => {
    getCurrent.mockResolvedValue(5);
    getMax.mockResolvedValue(5);
    mockQueueDepth(100);
    const metadataBroker = makeMetadataBroker();

    await new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded();

    expect(metadataBroker.claimProcessorBoost).not.toHaveBeenCalled();
    expect(setTo).not.toHaveBeenCalled();
  });

  it('does not boost when backlog is at or below 2x max (no claim attempted)', async () => {
    getCurrent.mockResolvedValue(1);
    getMax.mockResolvedValue(5);
    mockQueueDepth(10);
    const metadataBroker = makeMetadataBroker();

    await new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded();

    expect(metadataBroker.claimProcessorBoost).not.toHaveBeenCalled();
    expect(setTo).not.toHaveBeenCalled();
  });

  it('skips entirely when another task already holds the claim', async () => {
    getCurrent.mockResolvedValue(1);
    getMax.mockResolvedValue(5);
    mockQueueDepth(11);
    const metadataBroker = makeMetadataBroker({ claim: false });

    await new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded();

    expect(startDelay).not.toHaveBeenCalled();
    expect(setTo).not.toHaveBeenCalled();
    expect(metadataBroker.releaseProcessorBoostClaim).not.toHaveBeenCalled();
  });

  it('releases the claim and does not boost when the alarm never clears', async () => {
    getCurrent.mockResolvedValue(1);
    getMax.mockResolvedValue(5);
    mockQueueDepth(11);
    startDelay.mockResolvedValue(false);
    const metadataBroker = makeMetadataBroker();

    await new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded();

    expect(setTo).not.toHaveBeenCalled();
    expect(metadataBroker.releaseProcessorBoostClaim).toHaveBeenCalledTimes(1);
  });

  it('releases the claim when setTo() throws', async () => {
    getCurrent.mockResolvedValue(1);
    getMax.mockResolvedValue(5);
    mockQueueDepth(11);
    setTo.mockRejectedValue(new Error('ECS update failed'));
    const metadataBroker = makeMetadataBroker();

    await expect(
      new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded()
    ).resolves.toBeUndefined();

    expect(metadataBroker.releaseProcessorBoostClaim).toHaveBeenCalledTimes(1);
  });

  it('does not throw and does not boost when getMax() is unavailable', async () => {
    getCurrent.mockResolvedValue(1);
    getMax.mockResolvedValue(undefined);
    mockQueueDepth(100);
    const metadataBroker = makeMetadataBroker();

    await expect(
      new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded()
    ).resolves.toBeUndefined();
    expect(setTo).not.toHaveBeenCalled();
  });

  it('does not throw and does not boost when the queue depth lookup fails', async () => {
    getCurrent.mockResolvedValue(1);
    getMax.mockResolvedValue(5);
    sqsMock.on(GetQueueAttributesCommand).rejects(new Error('boom'));
    const metadataBroker = makeMetadataBroker();

    await expect(
      new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded()
    ).resolves.toBeUndefined();
    expect(setTo).not.toHaveBeenCalled();
  });

  it('does nothing (no DesiredCount constructed) when configuration is incomplete', async () => {
    const metadataBroker = makeMetadataBroker();

    await new ProcessorServiceBooster(metadataBroker as any, { clusterName: 'only-this' }).checkAndBoostIfNeeded();

    expect(DesiredCount).not.toHaveBeenCalled();
    expect(sqsMock.calls()).toHaveLength(0);
    expect(metadataBroker.claimProcessorBoost).not.toHaveBeenCalled();
  });
});

describe('ProcessorServiceBooster.startPeriodicCheck', () => {
  let getCurrent: jest.Mock;
  let metadataBroker: ReturnType<typeof makeMetadataBroker>;

  beforeEach(() => {
    jest.useFakeTimers();
    sqsMock.reset();
    mockQueueDepth(100);
    getCurrent = jest.fn().mockResolvedValue(1);
    metadataBroker = makeMetadataBroker();
    (DesiredCount as jest.Mock).mockImplementation(() => ({
      getCurrent,
      getMax: jest.fn().mockResolvedValue(5),
      setTo: jest.fn(),
    }));
    (MetricsCatchupDelay as jest.Mock).mockImplementation(() => ({ startDelay: jest.fn().mockResolvedValue(true) }));
  });

  afterEach(() => {
    jest.clearAllTimers();
    jest.useRealTimers();
  });

  it('stops checking once the returned stop function is called', async () => {
    const stop = ProcessorServiceBooster.startPeriodicCheck(metadataBroker as any, 30, baseParams);

    await jest.advanceTimersByTimeAsync(30_000);
    expect(getCurrent).toHaveBeenCalledTimes(1);

    stop();
    await jest.advanceTimersByTimeAsync(60_000);
    expect(getCurrent).toHaveBeenCalledTimes(1);
  });
});

describe('ProcessorServiceBooster BOOST_PROCESSOR feature flag', () => {
  let originalBoostProcessor: string | undefined;
  let getCurrent: jest.Mock;
  let metadataBroker: ReturnType<typeof makeMetadataBroker>;

  beforeEach(() => {
    jest.clearAllMocks();
    originalBoostProcessor = process.env.BOOST_PROCESSOR;
    sqsMock.reset();
    mockQueueDepth(11);
    getCurrent = jest.fn().mockResolvedValue(1);
    metadataBroker = makeMetadataBroker();
    (DesiredCount as jest.Mock).mockImplementation(() => ({
      getCurrent,
      getMax: jest.fn().mockResolvedValue(5),
      setTo: jest.fn(),
    }));
    (MetricsCatchupDelay as jest.Mock).mockImplementation(() => ({ startDelay: jest.fn().mockResolvedValue(true) }));
  });

  afterEach(() => {
    process.env.BOOST_PROCESSOR = originalBoostProcessor;
    jest.clearAllMocks();
  });

  it('does nothing when BOOST_PROCESSOR=false', async () => {
    process.env.BOOST_PROCESSOR = 'false';

    await new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded();

    expect(DesiredCount).not.toHaveBeenCalled();
    expect(metadataBroker.claimProcessorBoost).not.toHaveBeenCalled();
  });

  it('does not start an interval when BOOST_PROCESSOR=false', () => {
    process.env.BOOST_PROCESSOR = 'false';

    const stop = ProcessorServiceBooster.startPeriodicCheck(metadataBroker as any, 30, baseParams);

    expect(DesiredCount).not.toHaveBeenCalled();
    expect(() => stop()).not.toThrow();
  });

  it('still works when BOOST_PROCESSOR is unset (defaults to enabled)', async () => {
    delete process.env.BOOST_PROCESSOR;

    await new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded();

    expect(metadataBroker.claimProcessorBoost).toHaveBeenCalled();
  });

  it('still works when BOOST_PROCESSOR=true', async () => {
    process.env.BOOST_PROCESSOR = 'true';

    await new ProcessorServiceBooster(metadataBroker as any, baseParams).checkAndBoostIfNeeded();

    expect(metadataBroker.claimProcessorBoost).toHaveBeenCalled();
  });
});
