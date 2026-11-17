import { LambdaClient } from '@aws-sdk/client-lambda';
import { mockClient } from 'aws-sdk-client-mock';
import { getSimulatorCounter, SourceSimulatorFunctionURL } from '../src/chunking/fetch/SourceSimulator';
import { ChunkingServiceRunner } from '../src/runner/AbstractRunner';
import { SourceSimulatorRunnerDecorator } from '../src/runner/decorators/SourceSimulatorRunnerDecorator';
import { extractEnvironment, RunnerEnv } from '../src/runner/RunnerTypes';

jest.mock('../src/chunking/fetch/SourceSimulator', () => ({
  ...jest.requireActual('../src/chunking/fetch/SourceSimulator'),
  getSimulatorCounter: jest.fn(),
}));

describe('SourceSimulatorRunnerDecorator', () => {
  const functionUrl = 'https://abc123.lambda-url.us-east-2.on.aws/';

  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
    jest.spyOn(SourceSimulatorFunctionURL.prototype, 'getUrl').mockResolvedValue(functionUrl);
    jest.spyOn(SourceSimulatorFunctionURL.prototype, 'exists').mockResolvedValue(true);
    jest.spyOn(SourceSimulatorFunctionURL.prototype, 'health').mockResolvedValue({ totalPopulation: 1950, simulatedDelaySeconds: 3, errorRate: 0 });
    (getSimulatorCounter as jest.Mock).mockReset();
  });
  afterEach(() => jest.restoreAllMocks());

  const decoratorFor = (env: Partial<RunnerEnv>) => {
    const wrapped = {
      env: { messagesToPrepopulate: '0', desiredCount: 0, sourceSimulator: true, region: 'us-east-2', landscape: 'mock', stackId: 'stack', ...env },
      validatePrerequisites: jest.fn().mockResolvedValue(true),
    } as unknown as ChunkingServiceRunner;
    return { decorator: new SourceSimulatorRunnerDecorator(wrapped), wrapped };
  };

  it('points the source at the single person endpoint, without touching the depletion counter', async () => {
    const { decorator } = decoratorFor({ buid: 'U0000042' });
    const logPredictions = jest.spyOn(decorator as any, 'logSourceSimulatorPredictions');

    const endpoint = await decorator.resolveDataSource({ dataSource: {} } as any);

    expect(endpoint).toEqual({
      baseUrl: 'https://abc123.lambda-url.us-east-2.on.aws',
      fetchPath: '/person?buid=U0000042',
      termsEndpoint: { baseUrl: 'https://abc123.lambda-url.us-east-2.on.aws', fetchPath: '/terms' }
    });
    // The buid query parameter is what makes the chunker do a single, non-paged request
    expect(new URL(`${endpoint.baseUrl}${endpoint.fetchPath}`).searchParams.get('buid')).toBe('U0000042');
    expect(getSimulatorCounter).not.toHaveBeenCalled();
    expect(logPredictions).not.toHaveBeenCalled();
  });

  it('points the source at the people endpoint without a BUID', async () => {
    const reset = jest.fn().mockResolvedValue(undefined);
    (getSimulatorCounter as jest.Mock).mockReturnValue({ reset });
    const { decorator } = decoratorFor({});
    jest.spyOn(decorator as any, 'logSourceSimulatorPredictions').mockResolvedValue(undefined);

    const endpoint = await decorator.resolveDataSource({ dataSource: {} } as any);

    expect(endpoint.fetchPath).toBe('/');
    expect(reset).toHaveBeenCalledTimes(1);
  });

  it('sends simulated terms via the endpoint, not by changing the runner config (which never reaches the tasks)', async () => {
    (getSimulatorCounter as jest.Mock).mockReturnValue({ reset: jest.fn().mockResolvedValue(undefined) });
    const { decorator } = decoratorFor({});
    jest.spyOn(decorator as any, 'logSourceSimulatorPredictions').mockResolvedValue(undefined);
    const terms = { endpointConfig: { baseUrl: 'https://real.example.com', apiKey: 'k' }, fetchPath: '/real/terms' };
    const config = { dataSource: { terms: JSON.parse(JSON.stringify(terms)) } } as any;

    const endpoint = await decorator.resolveDataSource(config);

    expect(endpoint.termsEndpoint).toEqual({ baseUrl: 'https://abc123.lambda-url.us-east-2.on.aws', fetchPath: '/terms' });
    expect(config.dataSource.terms).toEqual(terms);
  });

  it("carries the run's simulator settings in the people fetchPath, 0 included", async () => {
    (getSimulatorCounter as jest.Mock).mockReturnValue({ reset: jest.fn().mockResolvedValue(undefined) });
    const { decorator } = decoratorFor({
      sourceSimulatorMockTotalPopulation: 40, sourceSimulatorMockSimulatedDelaySeconds: 0.5,
      sourceSimulatorMockErrorRate: 0, sourceSimulatorMockNonCurrentTermRate: 0.25
    });
    jest.spyOn(decorator as any, 'logSourceSimulatorPredictions').mockResolvedValue(undefined);

    const endpoint = await decorator.resolveDataSource({ dataSource: {} } as any);

    expect(endpoint.fetchPath).toBe('/?totalPopulation=40&simulatedDelaySeconds=0.5&errorRate=0&nonCurrentTermRate=0.25');
    expect(endpoint.termsEndpoint).toEqual({ baseUrl: 'https://abc123.lambda-url.us-east-2.on.aws', fetchPath: '/terms' });
  });

  it("carries them on the single person endpoint too, after the buid that stops the chunker paging", async () => {
    const { decorator } = decoratorFor({ buid: 'U0000042', sourceSimulatorMockErrorRate: 0 });

    const endpoint = await decorator.resolveDataSource({ dataSource: {} } as any);

    expect(endpoint.fetchPath).toBe('/person?buid=U0000042&errorRate=0');
  });

  it("never reads or changes the simulator Lambda's configuration", async () => {
    const lambda = mockClient(LambdaClient);
    try {
      (getSimulatorCounter as jest.Mock).mockReturnValue({ reset: jest.fn().mockResolvedValue(undefined) });
      const { decorator, wrapped } = decoratorFor({
        messagesToPrepopulate: '5', iterationLimit: 2,
        sourceSimulatorMockTotalPopulation: 40, sourceSimulatorMockSimulatedDelaySeconds: 1, sourceSimulatorMockErrorRate: 0.1
      });
      jest.spyOn(decorator as any, 'loadContext').mockResolvedValue({ ITEMS_PER_CHUNK: 10 });
      jest.spyOn(console, 'warn').mockImplementation(() => undefined);

      await expect(decorator.validatePrerequisites()).resolves.toBe(true);
      await decorator.resolveDataSource({ dataSource: {} } as any);

      expect(wrapped.validatePrerequisites).toHaveBeenCalled();
      // getUrl is stubbed, so any Lambda call would be a configuration read or update
      expect(lambda.calls()).toHaveLength(0);
    } finally {
      lambda.restore();
    }
  });

  it("predicts from the simulator's /health, asked with the run's settings", async () => {
    const { decorator } = decoratorFor({ sourceSimulatorMockTotalPopulation: 40, sourceSimulatorMockErrorRate: 0 });
    jest.spyOn(decorator as any, 'loadContext').mockResolvedValue({ ITEMS_PER_CHUNK: 15, LAMBDA: { sourceSimulator: { mockTotalPopulation: 1950 } } });
    const health = jest.spyOn(SourceSimulatorFunctionURL.prototype, 'health').mockResolvedValue({ totalPopulation: 40 });

    await expect((decorator as any).getSourceSimulatorPredictions())
      .resolves.toEqual({ totalPopulation: 40, totalFullChunks: 2, remainingItems: 10, itemsPerChunk: 15 });
    expect(health).toHaveBeenCalledWith({ totalPopulation: 40, errorRate: 0 });
  });

  it("predicts from the run's population, else the context's, when /health can't be read", async () => {
    jest.spyOn(SourceSimulatorFunctionURL.prototype, 'health').mockResolvedValue(undefined);
    const context = { ITEMS_PER_CHUNK: 100, LAMBDA: { sourceSimulator: { mockTotalPopulation: 1950 } } };
    const withTotal = decoratorFor({ sourceSimulatorMockTotalPopulation: 40 }).decorator;
    jest.spyOn(withTotal as any, 'loadContext').mockResolvedValue(context);
    await expect((withTotal as any).getSourceSimulatorPredictions()).resolves.toMatchObject({ totalPopulation: 40 });
    const without = decoratorFor({}).decorator;
    jest.spyOn(without as any, 'loadContext').mockResolvedValue(context);
    await expect((without as any).getSourceSimulatorPredictions()).resolves.toMatchObject({ totalPopulation: 1950, totalFullChunks: 19 });
  });

  it('fails prerequisites for a BUID the simulator does not generate', async () => {
    const { decorator, wrapped } = decoratorFor({ buid: 'X1234' });
    await expect(decorator.validatePrerequisites()).resolves.toBe(false);
    expect(wrapped.validatePrerequisites).not.toHaveBeenCalled();
  });

  it('warns, but proceeds, for a BUID beyond the simulated population', async () => {
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    const { decorator, wrapped } = decoratorFor({ buid: 'U0000011' });
    jest.spyOn(decorator as any, 'getSourceSimulatorPredictions').mockResolvedValue({ totalPopulation: 10 });

    await expect(decorator.validatePrerequisites()).resolves.toBe(true);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('beyond'));
    expect(wrapped.validatePrerequisites).toHaveBeenCalled();
  });
});

describe("The Runner's simulator settings from the environment", () => {
  const saved = { ...process.env };
  afterEach(() => { process.env = { ...saved }; jest.restoreAllMocks(); });

  it('takes 0 and fractions as given, and leaves unset, blank or non-numeric ones to the simulator', () => {
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    process.env = {
      ...saved,
      SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION: '1950',
      SOURCE_SIMULATOR_MOCK_SIMULATED_DELAY_SECONDS: '0.5',
      SOURCE_SIMULATOR_MOCK_ERROR_RATE: '0',
      SOURCE_SIMULATOR_MOCK_NON_CURRENT_TERM_RATE: ''
    };
    expect(extractEnvironment()).toMatchObject({
      sourceSimulatorMockTotalPopulation: 1950,
      sourceSimulatorMockSimulatedDelaySeconds: 0.5,
      sourceSimulatorMockErrorRate: 0,
      sourceSimulatorMockNonCurrentTermRate: undefined
    });
    process.env.SOURCE_SIMULATOR_MOCK_ERROR_RATE = 'some';
    delete process.env.SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION;
    expect(extractEnvironment()).toMatchObject({ sourceSimulatorMockErrorRate: undefined, sourceSimulatorMockTotalPopulation: undefined });
  });
});
