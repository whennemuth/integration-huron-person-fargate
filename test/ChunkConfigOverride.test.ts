import { Config, DataSourceConfig } from 'integration-huron-person';
import { ChunkConfigOverride } from '../src/chunking/fetch/ChunkConfigOverride';
import { SyncPopulation } from '../docker/chunkTypes';

type TaskParametersLike = {
  baseUrl: string;
  fetchPath: string;
  populationType: SyncPopulation;
  bulkReset: boolean;
};

const buildTaskParameters = (overrides?: Partial<TaskParametersLike>): TaskParametersLike => ({
  baseUrl: 'https://api.example.com',
  fetchPath: '/people',
  populationType: SyncPopulation.PersonFull,
  bulkReset: false,
  ...overrides,
});

const buildConfig = (overrides?: Partial<Config>): Config => {
  const config = {
    dataSource: {
      people: {
        endpointConfig: {
          baseUrl: 'https://api.example.com',
          apiKey: 'people-api-key-1234',
        },
        fetchPath: '/people',
      } as DataSourceConfig,
      person: {
        endpointConfig: {
          baseUrl: 'https://api.example.com',
          apiKey: 'person-api-key-5678',
        },
        fetchPath: '/person',
      } as DataSourceConfig,
    },
  } as Config;

  return {
    ...config,
    ...overrides,
    dataSource: {
      ...config.dataSource,
      ...(overrides?.dataSource || {}),
    },
  } as Config;
};

describe('ChunkConfigOverride', () => {
  let consoleLogSpy: jest.SpyInstance;
  let consoleWarnSpy: jest.SpyInstance;
  let consoleErrorSpy: jest.SpyInstance;

  beforeEach(() => {
    consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
    consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation();
    consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();
  });

  afterEach(() => {
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
  });

  it('returns same config object when no override is needed', () => {
    const config = buildConfig();
    const taskParameters = buildTaskParameters({
      baseUrl: 'https://api.example.com',
      fetchPath: '/people',
    });

    const override = new ChunkConfigOverride(config, taskParameters as any);
    const result = override.getOverridenConfig();

    expect(result).toBe(config);
    expect((result.dataSource.people as DataSourceConfig).fetchPath).toBe('/people');
    expect((result.dataSource.people as DataSourceConfig).endpointConfig?.baseUrl).toBe('https://api.example.com');
    expect((result.dataSource.people as DataSourceConfig).endpointConfig?.apiKey).toBe('people-api-key-1234');
    expect(override.getMessages()).toHaveLength(0);
  });

  it('overrides baseUrl and fetchPath from task parameters when different', () => {
    const config = buildConfig();
    const taskParameters = buildTaskParameters({
      baseUrl: 'https://api.override.example.com',
      fetchPath: '/override/people',
    });

    const override = new ChunkConfigOverride(config, taskParameters as any);
    const result = override.getOverridenConfig();

    expect((result.dataSource.people as DataSourceConfig).endpointConfig?.baseUrl).toBe('https://api.override.example.com');
    expect((result.dataSource.people as DataSourceConfig).fetchPath).toBe('/override/people');

    const messages = override.getMessages().join('\n');
    expect(messages).toContain('Overriding config baseUrl');
    expect(messages).toContain('Overriding config fetchPath');
  });

  it('does not treat querystring differences as endpoint mismatch for apiKey selection', () => {
    const config = buildConfig();
    const taskParameters = buildTaskParameters({
      baseUrl: 'https://api.example.com',
      fetchPath: '/person?buid=123456',
    });

    const override = new ChunkConfigOverride(config, taskParameters as any);
    const result = override.getOverridenConfig();

    expect((result.dataSource.people as DataSourceConfig).endpointConfig?.apiKey).toBe('person-api-key-5678');
    expect(override.getMessages().some((m) => m.includes('Overriding config apiKey'))).toBe(true);
    expect(override.getMessages().some((m) => m.includes('no matching key'))).toBe(false);
  });

  it('ignores querystrings when both config and task fetchPath include different query params', () => {
    const config = buildConfig({
      dataSource: {
        people: {
          endpointConfig: {
            baseUrl: 'https://api.example.com',
            apiKey: 'people-api-key-1234',
          },
          fetchPath: '/people?source=default',
        } as DataSourceConfig,
        person: {
          endpointConfig: {
            baseUrl: 'https://api.example.com',
            apiKey: 'person-api-key-5678',
          },
          fetchPath: '/person?template=true',
        } as DataSourceConfig,
      },
    } as Partial<Config>);

    const taskParameters = buildTaskParameters({
      baseUrl: 'https://api.example.com',
      fetchPath: '/person?buid=ABC123',
    });

    const override = new ChunkConfigOverride(config, taskParameters as any);
    const result = override.getOverridenConfig();

    expect((result.dataSource.people as DataSourceConfig).endpointConfig?.apiKey).toBe('person-api-key-5678');
    expect(override.getMessages().some((m) => m.includes('Overriding config apiKey'))).toBe(true);
  });

  it('keeps existing apiKey when overridden endpoint does not match person endpoint', () => {
    const config = buildConfig();
    const taskParameters = buildTaskParameters({
      baseUrl: 'https://api.example.com',
      fetchPath: '/not-person',
    });

    const override = new ChunkConfigOverride(config, taskParameters as any);
    const result = override.getOverridenConfig();

    expect((result.dataSource.people as DataSourceConfig).endpointConfig?.apiKey).toBe('people-api-key-1234');
    expect(override.getMessages().some((m) => m.includes('no matching key'))).toBe(true);
  });

  it('returns unaltered config even when optional config sections are missing entirely', () => {
    const config = {} as Config;
    const taskParameters = buildTaskParameters();

    const override = new ChunkConfigOverride(config, taskParameters as any);

    expect(() => override.getOverridenConfig()).not.toThrow();
    const result = override.getOverridenConfig();
    expect(result).toBe(config);
  });

  it('returns unaltered config when dataSource.people is excluded', () => {
    const config = { dataSource: {} } as Config;
    const taskParameters = buildTaskParameters();

    const override = new ChunkConfigOverride(config, taskParameters as any);

    expect(() => override.getOverridenConfig()).not.toThrow();
    const result = override.getOverridenConfig();
    expect(result).toBe(config);
  });

  it('returns unaltered config when people.endpointConfig is excluded', () => {
    const config = {
      dataSource: {
        people: {
          fetchPath: '/people',
        } as DataSourceConfig,
      },
    } as Config;
    const taskParameters = buildTaskParameters();

    const override = new ChunkConfigOverride(config, taskParameters as any);

    expect(() => override.getOverridenConfig()).not.toThrow();
    const result = override.getOverridenConfig();
    expect(result).toBe(config);
  });

  it('returns unaltered config when person endpoint config is excluded', () => {
    const config = {
      dataSource: {
        people: {
          endpointConfig: {
            baseUrl: 'https://api.example.com',
            apiKey: 'people-api-key-1234',
          },
          fetchPath: '/people',
        } as DataSourceConfig,
        person: {
          fetchPath: '/person',
        } as DataSourceConfig,
      },
    } as Config;

    const taskParameters = buildTaskParameters({ fetchPath: '/person?buid=77' });

    const override = new ChunkConfigOverride(config, taskParameters as any);

    expect(() => override.getOverridenConfig()).not.toThrow();
    const result = override.getOverridenConfig();
    expect(result).toBe(config);
  });

  it('returns unaltered config when task parameters are missing optional values', () => {
    const config = buildConfig();
    const taskParameters = {
      populationType: SyncPopulation.PersonFull,
      bulkReset: false,
    } as any;

    const override = new ChunkConfigOverride(config, taskParameters);

    expect(() => override.getOverridenConfig()).not.toThrow();
    const result = override.getOverridenConfig();
    expect(result).toBe(config);
  });
});
