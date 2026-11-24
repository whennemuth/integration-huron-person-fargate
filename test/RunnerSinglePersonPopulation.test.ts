/**
 * Tests that a single-person run from the manual Runner is always person-delta (singlePersonPopulationRefusal() and
 * SinglePersonRunner in src/runner/RunnerForSinglePerson.ts, the check in src/runner/Runner.ts). A person-full run of
 * one person deactivates everyone else in the target: on 2026-11-23 one began to in preview. The Runner refuses any
 * POPULATION_TYPE but person-delta for a single person before building a runner, since some decorators change things
 * (a mock landscape's reset, service toggles) before the runner's own prerequisites are checked; unset means person-delta.
 */
import { Config } from 'integration-huron-person';
import { SyncPopulation } from '../docker/chunkTypes';
import { handleApiEvent } from '../src/chunking/fetch/ChunkerApiSubscriber';
import { ChunkingServiceRunner } from '../src/runner/AbstractRunner';
import { MockLandscapeRunnerDecorator } from '../src/runner/decorators/MockLandscapeRunnerDecorator';
import { RestoreToFullOperationRunnerDecorator } from '../src/runner/decorators/RestoreToFullOperationRunnerDecorator';
import { startChunkingService } from '../src/runner/Runner';
import { SinglePersonRunner, singlePersonPopulationRefusal } from '../src/runner/RunnerForSinglePerson';
import { RunnerEnv } from '../src/runner/RunnerTypes';

jest.mock('../src/chunking/fetch/ChunkerApiSubscriber', () => ({
  ...jest.requireActual('../src/chunking/fetch/ChunkerApiSubscriber'),
  handleApiEvent: jest.fn().mockResolvedValue({ statusCode: 200 })
}));

const ENV: RunnerEnv = {
  region: 'us-east-2', landscape: 'preview', stackId: 'huron-person-fargate-processor', messagesToPrepopulate: '0',
  desiredCount: 0, queueUrl: 'https://sqs.us-east-2.amazonaws.com/111111111111/chunker', buid: 'U15364542',
  previousStateSource: 'storage'
};

/** A single-person runner with its AWS and configuration steps stubbed, so start() runs through to execute() */
class StubbedSinglePersonRunner extends SinglePersonRunner {
  protected async loadConfiguration(): Promise<Config> {
    return {
      dataSource: { person: { endpointConfig: { baseUrl: 'https://cdm.example.bu.edu' }, fetchPath: '/person' } },
      dataTarget: { endpointConfig: { baseUrl: 'https://huron.example.com' }, personsPath: '/api/v2/persons' }
    } as unknown as Config;
  }
  protected async setProcessorScalingCapacity(): Promise<undefined> {
    return undefined;
  }
  protected async resetAtomicCounters(): Promise<void> { }
}

describe('a single-person run is always person-delta', () => {
  beforeEach(() => {
    (handleApiEvent as jest.Mock).mockClear();
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  describe('singlePersonPopulationRefusal()', () => {
    it.each([undefined, '', '  ', 'person-delta', ' PERSON-DELTA '])('accepts %p', populationType => {
      expect(singlePersonPopulationRefusal(populationType)).toBeUndefined();
    });

    it.each(['person-full', 'PERSON-FULL', 'person_delta', 'delta'])('refuses %p, naming it', populationType => {
      const refusal = singlePersonPopulationRefusal(populationType);
      expect(refusal).toContain(`POPULATION_TYPE=${populationType.trim()}`);
      expect(refusal).toContain('person-delta');
    });
  });

  describe('Runner.ts', () => {
    it('refuses a single-person person-full run before building a runner, so no decorator acts', async () => {
      const start = jest.spyOn(ChunkingServiceRunner.prototype, 'start');
      const mockLandscapeCheck = jest.spyOn(MockLandscapeRunnerDecorator.prototype, 'validatePrerequisites');
      const restoreCheck = jest.spyOn(RestoreToFullOperationRunnerDecorator.prototype, 'validatePrerequisites');

      await startChunkingService({ ...ENV, landscape: 'mock', mockTargetResetState: true, populationType: 'person-full' });

      expect(console.error).toHaveBeenCalledWith(expect.stringContaining('must be person-delta'));
      expect(start).not.toHaveBeenCalled();
      expect(mockLandscapeCheck).not.toHaveBeenCalled();
      expect(restoreCheck).not.toHaveBeenCalled();
      expect(handleApiEvent).not.toHaveBeenCalled();
    });

    it.each([
      ['a single person, person-delta', { populationType: 'person-delta' }],
      ['a single person, POPULATION_TYPE unset', { populationType: undefined }],
      ['a full sync, person-full', { buid: undefined, populationType: 'person-full' }]
    ])('lets %s start', async (_label, env) => {
      const start = jest.spyOn(ChunkingServiceRunner.prototype, 'start').mockResolvedValue(undefined);

      await startChunkingService({ ...ENV, ...env } as RunnerEnv);

      expect(start).toHaveBeenCalledTimes(1);
      expect(console.error).not.toHaveBeenCalled();
    });
  });

  describe("Runner.ts: where the run's previous state comes from (RUNNER_PREVIOUS_STATE_SOURCE)", () => {
    it.each([
      ['unset', undefined, 'No previousStateSource'],
      ['the old pair\'s value', 'true', 'Invalid previousStateSource']
    ])('refuses a run with it %s, before building a runner', async (_label, previousStateSource, reason) => {
      const start = jest.spyOn(ChunkingServiceRunner.prototype, 'start');

      await startChunkingService({ ...ENV, buid: undefined, populationType: 'person-full', previousStateSource });

      expect(console.error).toHaveBeenCalledWith(expect.stringContaining('RUNNER_PREVIOUS_STATE_SOURCE'));
      expect(console.error).toHaveBeenCalledWith(expect.stringContaining(reason));
      expect(start).not.toHaveBeenCalled();
    });

    it.each(['storage', 'target'])('sends previousStateSource %p and never the old pair', async previousStateSource => {
      await new StubbedSinglePersonRunner({ ...ENV, previousStateSource }).start();

      const event = (handleApiEvent as jest.Mock).mock.calls[0][0];
      expect(event.previousStateSource).toBe(previousStateSource);
      expect(event).not.toHaveProperty('bulkReset');
      expect(event).not.toHaveProperty('trustPreviousStorage');
    });
  });

  describe('SinglePersonRunner', () => {
    it('sends person-delta when POPULATION_TYPE is unset (which would otherwise be read as person-full)', async () => {
      await new StubbedSinglePersonRunner({ ...ENV, populationType: undefined }).start();

      expect(handleApiEvent).toHaveBeenCalledTimes(1);
      expect((handleApiEvent as jest.Mock).mock.calls[0][0]).toMatchObject({
        populationType: SyncPopulation.PersonDelta,
        fetchPath: '/person?buid=U15364542'
      });
    });

    it('refuses person-full in its own prerequisites too, when built directly', async () => {
      await new StubbedSinglePersonRunner({ ...ENV, populationType: 'person-full' }).start();

      expect(console.error).toHaveBeenCalledWith(expect.stringContaining('must be person-delta'));
      expect(handleApiEvent).not.toHaveBeenCalled();
    });
  });
});
