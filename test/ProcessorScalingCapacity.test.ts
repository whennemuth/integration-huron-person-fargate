/**
 * Tests a run's processor maximum (src/chunking/ProcessorScalingCapacity.ts): what an override may be, what a run start
 * sets the live maximum to (its override, else the landscape's maxScalingCapacity), the failure rules (a run with an
 * override fails its start; restoring the landscape's only warns), DRY_RUN, and DesiredCount.setMax()'s request.
 * AWS is replaced by injected functions, and by aws-sdk-client-mock for DesiredCount.
 */
import { ApplicationAutoScalingClient, RegisterScalableTargetCommand } from '@aws-sdk/client-application-auto-scaling';
import { mockClient } from 'aws-sdk-client-mock';
import type { ApiChunkerEvent } from '../src/chunking/ChunkerSubscriber';
import { ProcessorScalingCapacity, ProcessorScalingCapacityDeps } from '../src/chunking/ProcessorScalingCapacity';
import { DesiredCount } from '../src/DesiredCount';
import { SyncPopulation } from '../docker/chunkTypes';

const ENV = {
  REGION: 'us-east-2', CHUNKER_CLUSTER_NAME: 'huron-person-cluster-preview', PROCESSOR_SERVICE_NAME: 'Processor',
  PROCESSOR_MAX_SCALING_CAPACITY: '3'
};

const event = (overrides: Partial<ApiChunkerEvent> = {}): ApiChunkerEvent => ({
  baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', populationType: SyncPopulation.PersonFull,
  iterationLimit: 10, offset: 0, ...overrides
});

/** Injected AWS, recording each maximum set, or failing */
const fakeAws = (fail?: Error) => {
  const set: number[] = [];
  const deps: ProcessorScalingCapacityDeps = {
    setMax: jest.fn(async (maxTasks: number) => {
      if (fail) throw fail;
      set.push(maxTasks);
    })
  };
  return { deps, set };
};

describe('ProcessorScalingCapacity', () => {
  let warn: jest.SpyInstance;
  let log: jest.SpyInstance;
  beforeEach(() => {
    warn = jest.spyOn(console, 'warn').mockImplementation(() => {});
    log = jest.spyOn(console, 'log').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  describe('refusal()', () => {
    it('has nothing to refuse when the event carries no override', () => {
      expect(ProcessorScalingCapacity.refusal(event(), {})).toBeUndefined();
    });

    it('takes a whole number from 1 to 20 on a run start', () => {
      for (const value of [1, 3, 15, 20]) {
        expect(ProcessorScalingCapacity.refusal(event({ processorScalingCapacityOverride: value }), ENV)).toBeUndefined();
      }
    });

    it('refuses anything else, before the run starts', () => {
      for (const value of [0, -1, 21, 2.5, '5' as any, null as any]) {
        expect(ProcessorScalingCapacity.refusal(event({ processorScalingCapacityOverride: value }), ENV))
          .toMatch(/must be a whole number from 1 to 20/);
      }
    });

    it("refuses an override on an event that doesn't start a run", () => {
      expect(ProcessorScalingCapacity.refusal(event({ processorScalingCapacityOverride: 5, offset: 10 }), ENV))
        .toMatch(/only for a run's first event/);
      expect(ProcessorScalingCapacity.refusal(event({ processorScalingCapacityOverride: 5, chunkDirectory: 'chunks/x' }), ENV))
        .toMatch(/only for a run's first event/);
    });

    it("refuses an override the subscriber couldn't apply, naming what's missing", () => {
      expect(ProcessorScalingCapacity.refusal(event({ processorScalingCapacityOverride: 5 }), { REGION: 'us-east-2' }))
        .toBe("A processor scaling capacity override needs CHUNKER_CLUSTER_NAME, PROCESSOR_SERVICE_NAME in the chunker subscriber's environment");
    });
  });

  describe('deployedCapacity()', () => {
    it("reads the landscape's maxScalingCapacity", () => {
      expect(ProcessorScalingCapacity.deployedCapacity({ PROCESSOR_MAX_SCALING_CAPACITY: '15' })).toBe(15);
    });

    it("is unknown when unset, blank or not a whole number above 0", () => {
      for (const raw of [undefined, '', ' ', '0', '-2', '2.5', 'many']) {
        expect(ProcessorScalingCapacity.deployedCapacity({ PROCESSOR_MAX_SCALING_CAPACITY: raw })).toBeUndefined();
      }
    });
  });

  describe('apply()', () => {
    it("sets a run's override as the live maximum", async () => {
      const { deps, set } = fakeAws();
      await expect(new ProcessorScalingCapacity(deps, 3).apply(8))
        .resolves.toEqual({ capacity: 8, override: 8, applied: true });
      expect(set).toEqual([8]);
    });

    it("sets the landscape's maxScalingCapacity back on a run without one", async () => {
      const { deps, set } = fakeAws();
      await expect(new ProcessorScalingCapacity(deps, 3).apply())
        .resolves.toEqual({ capacity: 3, applied: true });
      expect(set).toEqual([3]);
    });

    it("fails a run's start when its override can't be set", async () => {
      const { deps } = fakeAws(new Error('AccessDeniedException'));
      await expect(new ProcessorScalingCapacity(deps, 3).apply(8))
        .rejects.toThrow("Couldn't set the processor's maximum tasks to 8 for this run (the landscape's is 3): AccessDeniedException");
    });

    it("only warns when the landscape's can't be set back, so the run still starts", async () => {
      const { deps } = fakeAws(new Error('Throttling'));
      const result = await new ProcessorScalingCapacity(deps, 3).apply();
      expect(result).toEqual({
        capacity: 3, applied: false,
        warning: "Couldn't set the processor's maximum tasks back to the landscape's 3: Throttling"
      });
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('The run starts with the processor'));
    });

    it("leaves the live maximum alone when there's no override and the landscape's isn't known", async () => {
      const { deps } = fakeAws();
      const result = await new ProcessorScalingCapacity(deps).apply();
      expect(result).toMatchObject({ applied: false, warning: expect.stringContaining("isn't known") });
      expect(deps.setMax).not.toHaveBeenCalled();
    });

    it('in DRY_RUN, logs what it would set and sets nothing', async () => {
      const { deps } = fakeAws();
      await expect(new ProcessorScalingCapacity(deps, 3, true).apply(8))
        .resolves.toEqual({ capacity: 8, override: 8, applied: false, dryRun: true });
      expect(deps.setMax).not.toHaveBeenCalled();
      expect(log).toHaveBeenCalledWith("[DRY RUN] Would set the processor's maximum tasks to 8 for this run (the landscape's is 3)");
    });
  });

  describe('fromEnvironment()', () => {
    it("sets the processor service's maximum in the landscape's cluster", async () => {
      const aas = mockClient(ApplicationAutoScalingClient);
      aas.on(RegisterScalableTargetCommand).resolves({});
      try {
        await expect(ProcessorScalingCapacity.fromEnvironment(ENV).apply()).resolves.toEqual({ capacity: 3, applied: true });
        expect(aas.commandCalls(RegisterScalableTargetCommand)[0].args[0].input).toEqual({
          ServiceNamespace: 'ecs', ResourceId: 'service/huron-person-cluster-preview/Processor',
          ScalableDimension: 'ecs:service:DesiredCount', MaxCapacity: 3
        });
      } finally {
        aas.restore();
      }
    });

    it('honors DRY_RUN from the environment', async () => {
      const aas = mockClient(ApplicationAutoScalingClient);
      try {
        await expect(ProcessorScalingCapacity.fromEnvironment({ ...ENV, DRY_RUN: 'TRUE' }).apply(5))
          .resolves.toMatchObject({ dryRun: true, applied: false });
        expect(aas.calls()).toHaveLength(0);
      } finally {
        aas.restore();
      }
    });
  });
});

describe('DesiredCount.setMax()', () => {
  const aas = mockClient(ApplicationAutoScalingClient);
  beforeEach(() => {
    aas.reset();
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());
  afterAll(() => aas.restore());

  const service = { clusterName: 'huron-person-cluster-mock', serviceName: 'Processor', region: 'us-east-2' };

  it('changes only MaxCapacity, and getMax() then reports it without asking again', async () => {
    aas.on(RegisterScalableTargetCommand).resolves({});
    const desiredCount = new DesiredCount(service);
    await desiredCount.setMax(5);
    expect(aas.commandCalls(RegisterScalableTargetCommand)[0].args[0].input).toEqual({
      ServiceNamespace: 'ecs', ResourceId: 'service/huron-person-cluster-mock/Processor',
      ScalableDimension: 'ecs:service:DesiredCount', MaxCapacity: 5
    });
    await expect(desiredCount.getMax()).resolves.toBe(5);
    expect(aas.calls()).toHaveLength(1);
  });

  it('throws what AWS refuses', async () => {
    aas.on(RegisterScalableTargetCommand).rejects(new Error('AccessDeniedException'));
    await expect(new DesiredCount(service).setMax(5)).rejects.toThrow('AccessDeniedException');
  });

  it('refuses a maximum below 1, or without a service, before asking AWS', async () => {
    await expect(new DesiredCount(service).setMax(0)).rejects.toThrow('at least 1');
    await expect(new DesiredCount({ ...service, serviceName: undefined }).setMax(5)).rejects.toThrow('without a cluster, service and region');
    expect(aas.calls()).toHaveLength(0);
  });
});
