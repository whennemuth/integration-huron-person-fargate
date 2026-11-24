/**
 * Tests the manual Runner setting the processor's live maximum at a run's start (ChunkingServiceRunner.start() and
 * setProcessorScalingCapacity() in src/runner/AbstractRunner.ts, src/chunking/ProcessorScalingCapacity.ts): its
 * PROCESSOR_SCALING_CAPACITY_OVERRIDE, or else the context's maxScalingCapacity, set before the counters are reset; an
 * unusable value stopping the run before anything; a continued run (SKIP_ATOMIC_COUNTER_RESET) leaving it alone; and
 * the record on the run's event, for its Flags. AWS is replaced by aws-sdk-client-mock and stubs.
 */
import { ApplicationAutoScalingClient, RegisterScalableTargetCommand } from '@aws-sdk/client-application-auto-scaling';
import { mockClient } from 'aws-sdk-client-mock';
import { Config } from 'integration-huron-person';
import { ChunkingServiceRunner } from '../src/runner/AbstractRunner';
import { Endpoint, NormalizedPopulationType, RunnerEnv, TargetConfig, extractEnvironment } from '../src/runner/RunnerTypes';
import { SingleMessageRunner } from '../src/runner/RunnerForSingleMessage';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { SyncPopulation } from '../docker/chunkTypes';

jest.mock('../src/chunking/fetch/ChunkerApiSubscriber', () => ({
  ...jest.requireActual('../src/chunking/fetch/ChunkerApiSubscriber'),
  handleApiEvent: jest.fn().mockResolvedValue({ statusCode: 200 })
}));

const ENV: RunnerEnv = {
  region: 'us-east-2', landscape: 'mock', stackId: 'huron-person-fargate-processor', messagesToPrepopulate: '0', desiredCount: 0,
  queueUrl: 'https://sqs.us-east-2.amazonaws.com/111111111111/chunker', previousStateSource: 'storage'
};

/** A runner whose every step is recorded, with the context's processor maximum given */
class RecordingRunner extends ChunkingServiceRunner {
  readonly calls: string[] = [];
  executedWith?: Endpoint;
  constructor(env: RunnerEnv, private readonly deployed?: number) {
    super(env);
  }
  protected async loadContext(): Promise<any> {
    return this.deployed === undefined ? {} : { ECS: { processorService: { maxScalingCapacity: this.deployed } } };
  }
  protected async loadConfiguration(): Promise<Config> {
    return {} as Config;
  }
  protected async resetAtomicCounters(): Promise<void> {
    this.calls.push('reset counters');
  }
  public async validatePrerequisites(): Promise<boolean> {
    this.calls.push('validate');
    return true;
  }
  public async resolveDataSource(): Promise<Endpoint> {
    return { baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people' };
  }
  public async resolveDataTarget(): Promise<TargetConfig> {
    return {};
  }
  public async execute(endpoint: Endpoint, _target: TargetConfig, _config: Config, _population: NormalizedPopulationType): Promise<void> {
    this.calls.push('execute');
    this.executedWith = endpoint;
  }
}

describe("the Runner's processor maximum", () => {
  const aas = mockClient(ApplicationAutoScalingClient);
  let calls: string[];

  beforeEach(() => {
    aas.reset();
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());
  afterAll(() => aas.restore());

  const run = async (env: Partial<RunnerEnv>, deployed?: number) => {
    const runner = new RecordingRunner({ ...ENV, ...env }, deployed);
    calls = runner.calls;
    aas.on(RegisterScalableTargetCommand).callsFake(input => { calls.push(`set max ${input.MaxCapacity}`); return {}; });
    await runner.start();
    return runner;
  };

  it('reads PROCESSOR_SCALING_CAPACITY_OVERRIDE, leaving it unset when absent', () => {
    const saved = { ...process.env };
    try {
      process.env.PROCESSOR_SCALING_CAPACITY_OVERRIDE = '8';
      expect(extractEnvironment().processorScalingCapacityOverride).toBe(8);
      delete process.env.PROCESSOR_SCALING_CAPACITY_OVERRIDE;
      expect(extractEnvironment().processorScalingCapacityOverride).toBeUndefined();
    } finally {
      process.env = saved;
    }
  });

  it("sets the run's override in the landscape's cluster before the counters are reset, and carries it to the run", async () => {
    const runner = await run({ processorScalingCapacityOverride: 8 }, 15);
    expect(calls).toEqual(['validate', 'set max 8', 'reset counters', 'execute']);
    expect(aas.commandCalls(RegisterScalableTargetCommand)[0].args[0].input).toEqual({
      ServiceNamespace: 'ecs', ResourceId: 'service/huron-person-cluster-mock/Processor',
      ScalableDimension: 'ecs:service:DesiredCount', MaxCapacity: 8
    });
    expect(runner.executedWith?.processorScalingCapacity).toEqual({ maxTasks: 8, overridden: true });
  });

  it("sets the context's maxScalingCapacity back without an override, so no run inherits another's", async () => {
    const runner = await run({}, 15);
    expect(calls).toEqual(['validate', 'set max 15', 'reset counters', 'execute']);
    expect(runner.executedWith?.processorScalingCapacity).toEqual({ maxTasks: 15, overridden: false });
  });

  it('stops the run before anything when the override is not 1 to 20', async () => {
    for (const value of [0, 21, 2.5]) {
      await run({ processorScalingCapacityOverride: value }, 15);
      expect(calls).toEqual([]);
    }
    expect(console.error).toHaveBeenCalledWith('PROCESSOR_SCALING_CAPACITY_OVERRIDE must be a whole number from 1 to 20 (got 21)');
  });

  it("stops the run, counters untouched, when the override can't be set", async () => {
    const runner = new RecordingRunner({ ...ENV, processorScalingCapacityOverride: 8 }, 15);
    aas.on(RegisterScalableTargetCommand).rejects(new Error('AccessDeniedException'));
    await expect(runner.start()).rejects.toThrow(/AccessDeniedException/);
    expect(runner.calls).toEqual(['validate']);
  });

  it("leaves the maximum alone when continuing a run (SKIP_ATOMIC_COUNTER_RESET)", async () => {
    const runner = await run({ skipAtomicCounterReset: true, processorScalingCapacityOverride: 8 }, 15);
    expect(aas.calls()).toHaveLength(0);
    expect(calls).toEqual(['validate', 'execute']);
    expect(runner.executedWith).not.toHaveProperty('processorScalingCapacity');
  });

  it('puts the record on the single message it sends', async () => {
    (handleApiEvent as jest.Mock).mockClear();
    const runner = new SingleMessageRunner({ ...ENV, iterationLimit: 10 });
    await runner.execute(
      { baseUrl: 'https://cdm.example.bu.edu', fetchPath: '/api/people', processorScalingCapacity: { maxTasks: 8, overridden: true } },
      {}, {} as Config, SyncPopulation.PersonFull
    );
    expect((handleApiEvent as jest.Mock).mock.calls[0][0].processorScalingCapacity).toEqual({ maxTasks: 8, overridden: true });
  });
});
