import { getSimulatorCounter, SourceSimulatorFunctionURL } from '../src/chunking/fetch/SourceSimulator';
import { ChunkingServiceRunner } from '../src/runner/AbstractRunner';
import { SourceSimulatorRunnerDecorator } from '../src/runner/decorators/SourceSimulatorRunnerDecorator';
import { RunnerEnv } from '../src/runner/RunnerTypes';

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
