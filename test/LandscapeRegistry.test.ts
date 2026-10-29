/**
 * Tests the landscape picker's registry (dashboard/src/services/LandscapeRegistry.ts): reading the
 * SSM path, defaults for hand-made entries, per-instance caching, and the synth-time check that at
 * most one landscape is the default (dashboard/lib/LandscapeRegistration.ts).
 */
import { GetParametersByPathCommand, SSMClient } from '@aws-sdk/client-ssm';
import { assertSingleDefaultLandscape } from '../dashboard/lib/LandscapeRegistration';
import { LandscapeRegistry, LANDSCAPES_PARAMETER_PATH, readLandscapeEntries } from '../dashboard/src/services/LandscapeRegistry';

const parameter = (landscape: string, value: any) => ({
  Name: `${LANDSCAPES_PARAMETER_PATH}/${landscape}`,
  Value: typeof value === 'string' ? value : JSON.stringify(value)
});

/** An SSM client whose GetParametersByPath returns the given pages in turn */
const ssmClient = (...pages: any[][]) => {
  const send = jest.fn();
  pages.forEach((Parameters, index) => send.mockResolvedValueOnce({
    Parameters, NextToken: index < pages.length - 1 ? `page-${index + 1}` : undefined
  }));
  return { send } as unknown as SSMClient & { send: jest.Mock };
};

describe('readLandscapeEntries', () => {
  beforeEach(() => jest.spyOn(console, 'warn').mockImplementation(() => {}));
  afterEach(() => jest.restoreAllMocks());

  it('reads every page of the registry path', async () => {
    const client = ssmClient(
      [parameter('mock', { landscape: 'mock', label: 'Mock', url: 'https://mock.example.com', isDefault: false })],
      [parameter('staging', { landscape: 'staging', isDefault: true })]
    );
    expect(await readLandscapeEntries({ client })).toEqual([
      { landscape: 'mock', label: 'Mock', url: 'https://mock.example.com', isDefault: false },
      { landscape: 'staging', isDefault: true },
    ]);
    const commands = client.send.mock.calls.map(([command]) => command as GetParametersByPathCommand);
    expect(commands.map(c => c.input)).toEqual([
      { Path: LANDSCAPES_PARAMETER_PATH, Recursive: false, NextToken: undefined },
      { Path: LANDSCAPES_PARAMETER_PATH, Recursive: false, NextToken: 'page-1' },
    ]);
  });

  it('takes the landscape from the parameter name when the value omits it', async () => {
    const client = ssmClient([parameter('other-account', { url: 'https://prod.example.org' })]);
    expect(await readLandscapeEntries({ client })).toEqual([
      { landscape: 'other-account', url: 'https://prod.example.org', isDefault: false }
    ]);
  });

  it('skips values that are not JSON objects, with a warning', async () => {
    const client = ssmClient([
      parameter('bad', 'not json'),
      parameter('list', '["dev","staging"]'),
      parameter('good', { landscape: 'good' }),
    ]);
    expect(await readLandscapeEntries({ client })).toEqual([{ landscape: 'good', isDefault: false }]);
    expect(console.warn).toHaveBeenCalledTimes(2);
  });

  it('counts only a literal true as the default', async () => {
    const client = ssmClient([parameter('mock', { isDefault: 'true' })]);
    expect((await readLandscapeEntries({ client }))[0].isDefault).toBe(false);
  });
});

describe('LandscapeRegistry', () => {
  const entries = [
    parameter('staging', { landscape: 'staging', label: 'Staging', url: 'https://staging.cssnprd.warhen.work', isDefault: true }),
    parameter('mock', { landscape: 'mock' }),
  ];

  it('fills in label and url defaults, flags the current landscape and sorts by landscape', async () => {
    const registry = new LandscapeRegistry({ currentLandscape: 'mock', hostedZone: 'cssnprd.warhen.work', client: ssmClient(entries) });
    expect(await registry.getLandscapes()).toEqual({
      current: 'mock',
      landscapes: [
        { landscape: 'mock', label: 'mock', url: 'https://mock.cssnprd.warhen.work', isDefault: false, current: true },
        { landscape: 'staging', label: 'Staging', url: 'https://staging.cssnprd.warhen.work', isDefault: true, current: false },
      ]
    });
  });

  it('leaves the url out when an entry has none and there is no hosted zone', async () => {
    const registry = new LandscapeRegistry({ currentLandscape: 'local', client: ssmClient([parameter('mock', {})]) });
    const { landscapes: [mock] } = await registry.getLandscapes();
    expect(mock.url).toBeUndefined();
    expect(mock.current).toBe(false);
  });

  it('reads SSM once per instance', async () => {
    const client = ssmClient(entries);
    const registry = new LandscapeRegistry({ currentLandscape: 'mock', client });
    await registry.getLandscapes();
    await registry.getLandscapes();
    expect(client.send).toHaveBeenCalledTimes(1);
  });

  it('retries after a failed read', async () => {
    const client = ssmClient(entries);
    client.send.mockReset()
      .mockRejectedValueOnce(new Error('AccessDenied'))
      .mockResolvedValueOnce({ Parameters: entries });
    const registry = new LandscapeRegistry({ currentLandscape: 'mock', client });
    await expect(registry.getLandscapes()).rejects.toThrow('AccessDenied');
    expect((await registry.getLandscapes()).landscapes).toHaveLength(2);
  });

  it('is configured from the Lambda environment', async () => {
    const registry = LandscapeRegistry.fromEnvironment({ LANDSCAPE: 'staging', HOSTED_ZONE: 'example.com' });
    expect((registry as any).params).toEqual({
      currentLandscape: 'staging', path: undefined, hostedZone: 'example.com', region: undefined
    });
    expect((LandscapeRegistry.fromEnvironment({}) as any).params.currentLandscape).toBe('local');
  });
});

describe('assertSingleDefaultLandscape', () => {
  const readEntries = jest.fn();
  afterEach(() => readEntries.mockReset());

  it('does not read SSM unless the landscape claims the default', async () => {
    await assertSingleDefaultLandscape({ landscape: 'mock', isDefault: false, readEntries });
    await assertSingleDefaultLandscape({ landscape: 'mock', readEntries });
    expect(readEntries).not.toHaveBeenCalled();
  });

  it('passes when no other landscape is the default, including a redeploy of the default', async () => {
    readEntries.mockResolvedValue([{ landscape: 'staging', isDefault: true }, { landscape: 'mock', isDefault: false }]);
    await expect(assertSingleDefaultLandscape({ landscape: 'staging', isDefault: true, readEntries })).resolves.toBeUndefined();
  });

  it('fails when another landscape is already the default, naming it', async () => {
    readEntries.mockResolvedValue([{ landscape: 'staging', isDefault: true }]);
    await expect(assertSingleDefaultLandscape({ landscape: 'mock', isDefault: true, readEntries }))
      .rejects.toThrow(/staging is already the default \(\/hp-dashboard\/landscapes\/staging\)/);
  });
});
