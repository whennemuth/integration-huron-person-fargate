import { App, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { IContext } from '../context/IContext';
import { DynamoDbTables } from '../lib/DynamoDB';
import { AppConstruct } from '../lib/AppConstruct';
import { BulkPurger, BulkPurgerMode } from '../src/dynamodb/BulkPurger';
import { isMockLandscape } from '../src/Utils';
import { ChunkingServiceRunner } from '../src/runner/AbstractRunner';
import { MockLandscapeRunnerDecorator } from '../src/runner/decorators/MockLandscapeRunnerDecorator';
import { RunnerEnv } from '../src/runner/RunnerTypes';

const STACK_ID = 'huron-person-fargate-processor';

const contextFor = (landscape: string, extra: Partial<IContext> = {}): IContext => ({
  STACK_ID,
  ACCOUNT: '123456789012',
  REGION: 'us-east-2',
  TAGS: { Landscape: landscape, Service: 'svc', Function: 'fn' },
  PREVIOUS_STORAGE_TYPE: 'dynamodb',
  ...extra
} as IContext);

const tableNamesFor = (landscape: string): string[] => {
  const stack = new Stack(new App(), 'TestStack');
  new DynamoDbTables({ scope: stack, id: 'DynamoDb', props: { context: contextFor(landscape) } });
  const tables = Template.fromStack(stack).findResources('AWS::DynamoDB::Table');
  return Object.values(tables).map((t: any) => t.Properties.TableName).sort();
};

describe('isMockLandscape', () => {
  it.each(['mock', 'mock1', 'mock2', 'mock22'])('"%s" is a mock landscape', (landscape) => {
    expect(isMockLandscape(landscape)).toBe(true);
  });

  it.each(['preview', 'staging', 'migration', 'mocks', 'mock-1', 'mock1a', 'xmock', 'Mock1', '', undefined])(
    '"%s" is not a mock landscape', (landscape) => {
      expect(isMockLandscape(landscape)).toBe(false);
    }
  );
});

describe('DynamoDbTables (CDK)', () => {
  const standardTables = (landscape: string) => [
    `${STACK_ID}-atomic-counter-${landscape}`,
    `${STACK_ID}-person-current-state-${landscape}`,
    `${STACK_ID}-person-history-${landscape}`,
    `${STACK_ID}-person-record-processor-log-${landscape}`,
    `${STACK_ID}-statistics-${landscape}`,
  ];

  it('creates only the standard tables (no mock tables at all) in a non-mock landscape', () => {
    const names = tableNamesFor('preview');
    expect(names.filter(n => n.includes('-mock-'))).toEqual([]);
    expect(names).toEqual(expect.arrayContaining(standardTables('preview')));
  });

  it('creates the standard tables plus exactly one extra - the mock target table - in a mock landscape', () => {
    const names = tableNamesFor('mock1');
    const nonMock = tableNamesFor('preview').length;
    expect(names).toHaveLength(nonMock + 1);
    expect(names).toEqual(expect.arrayContaining([
      ...standardTables('mock1'),
      `${STACK_ID}-mock-target-person-mock1`
    ]));
    expect(names.filter(n => n.includes('-mock-statistics-') || n.includes('-mock-person-'))).toEqual([]);
  });
});

describe('AppConstruct source simulator guard (CDK)', () => {
  const sourceSimulator = { enabled: true, timeoutSeconds: 60, memorySizeMb: 256, mockTotalPopulation: 10 };

  it('refuses to synthesize a source simulator outside a mock landscape', () => {
    const stack = new Stack(new App(), 'TestStack');
    const context = contextFor('preview', { LAMBDA: { sourceSimulator } as any, S3: { chunksBucket: 'chunks' } as any });
    expect(() => new AppConstruct(stack, 'App', { context, config: {} as any }))
      .toThrow(/not a mock landscape/);
  });
});

describe('BulkPurger landscape guard', () => {
  beforeEach(() => jest.spyOn(console, 'log').mockImplementation(() => undefined));
  afterEach(() => jest.restoreAllMocks());

  it('refuses to purge the tables of a non-mock landscape', async () => {
    const purger = new BulkPurger({ mode: BulkPurgerMode.TRUNCATE, context: contextFor('preview'), dryRun: true });
    await expect(purger.purge()).rejects.toThrow(/not a mock landscape/);
  });

  it('purges a non-mock landscape only when forced', async () => {
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    const purger = new BulkPurger({ mode: BulkPurgerMode.TRUNCATE, context: contextFor('preview'), dryRun: true, force: true });
    await expect(purger.purge()).resolves.toBeUndefined();
  });

  it('purges a mock landscape (dry run)', async () => {
    const purger = new BulkPurger({ mode: BulkPurgerMode.TRUNCATE, context: contextFor('mock'), dryRun: true });
    await expect(purger.purge()).resolves.toBeUndefined();
  });
});

describe('MockLandscapeRunnerDecorator', () => {
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  const wrappedRunner = (env: Partial<RunnerEnv>) => ({
    env: { messagesToPrepopulate: '0', desiredCount: 0, ...env },
    start: jest.fn().mockResolvedValue(undefined),
    validatePrerequisites: jest.fn().mockResolvedValue(true),
    resolveDataTarget: jest.fn().mockResolvedValue({ endpoint: { baseUrl: 'b', fetchPath: 'f' } }),
  }) as unknown as ChunkingServiceRunner;

  it('runs its OWN validatePrerequisites (mock checks + reset) when started - start() must not be delegated', async () => {
    // Regression: start() used to be delegated to wrappedRunner.start(), which ran the template
    // method against the wrapped runner and silently skipped this decorator (so the
    // MOCK_TARGET_RESET_STATE truncation never happened).
    const wrapped = wrappedRunner({ landscape: 'preview' }); // non-mock: fails fast, no AWS calls
    const decorator = new MockLandscapeRunnerDecorator(wrapped);
    const ownValidate = jest.spyOn(decorator, 'validatePrerequisites');

    await decorator.start();

    expect(ownValidate).toHaveBeenCalledTimes(1);
    expect(wrapped.start).not.toHaveBeenCalled();
  });

  it('fails prerequisites (without touching AWS) when the landscape is not a mock landscape', async () => {
    const wrapped = wrappedRunner({ landscape: 'preview' });
    const decorator = new MockLandscapeRunnerDecorator(wrapped);
    await expect(decorator.validatePrerequisites()).resolves.toBe(false);
    expect(wrapped.validatePrerequisites).not.toHaveBeenCalled();
  });
});
