/**
 * LaunchFlag (dashboard/src/services/LaunchFlag.ts): a landscape's SSM parameter saying a run was just launched, until
 * its run appears or the flag expires. Driven through SSM (aws-sdk-client-mock) and in memory.
 */
import { DeleteParameterCommand, GetParameterCommand, PutParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import { mockClient } from 'aws-sdk-client-mock';
import { Launch, LaunchFlag, MemoryLaunchFlagStore, SsmLaunchFlagStore } from '../dashboard/src/services/LaunchFlag';

const LAUNCHED_AT = '2026-11-16T14:00:00.000Z';
const AT = (minutes: number) => Date.parse(LAUNCHED_AT) + minutes * 60_000;
const CHOICES = {
  runType: 'full', population: 'person-full', bulkReset: false, trustPreviousStorage: true,
  iterationLimit: 10, parallelStart: 0, overrideStalled: false
} as const;
const launch = (choices: Record<string, unknown> = {}): Launch =>
  LaunchFlag.launch(LAUNCHED_AT, { ...CHOICES, ...choices } as any, { by: 'dashboard', principal: 'wrh' });

const awsError = (name: string) => Object.assign(new Error(name), { name });

describe('LaunchFlag', () => {
  const ssm = mockClient(SSMClient);
  let now = AT(1);
  const flag = () => new LaunchFlag({ landscape: 'mock', store: new SsmLaunchFlagStore(new SSMClient({ region: 'us-east-2' })), now: () => now });

  beforeEach(() => {
    ssm.reset();
    now = AT(1);
  });

  it("names one parameter per landscape, apart from the landscape registry's", () => {
    expect(LaunchFlag.parameterName('mock')).toBe('/hp-dashboard/launch-flags/mock');
    expect(flag().parameterName).toBe('/hp-dashboard/launch-flags/mock');
    expect(LaunchFlag.parameterName('mock').startsWith('/hp-dashboard/landscapes')).toBe(false);
  });

  it('gives a launch 5 minutes, 7 more for a parallel start, 10 more for a start from scratch', () => {
    expect(launch().expiresAt).toBe(new Date(AT(5)).toISOString());
    expect(launch({ parallelStart: 8 }).expiresAt).toBe(new Date(AT(12)).toISOString());
    expect(launch({ resetState: true }).expiresAt).toBe(new Date(AT(15)).toISOString());
    expect(LaunchFlag.lifetimeMs({ parallelStart: 8, resetState: true })).toBe(22 * 60_000);
    expect(launch()).toMatchObject({ launchedAt: LAUNCHED_AT, startedBy: { by: 'dashboard', principal: 'wrh' }, choices: { runType: 'full' } });
  });

  it('raises a flag only when none is up: a put that never overwrites', async () => {
    ssm.on(PutParameterCommand).resolvesOnce({ Version: 1 }).rejectsOnce(awsError('ParameterAlreadyExists'));
    expect(await flag().raise(launch())).toBe(true);
    expect(await flag().raise(launch())).toBe(false);
    const [put] = ssm.commandCalls(PutParameterCommand);
    expect(put.args[0].input).toMatchObject({ Name: '/hp-dashboard/launch-flags/mock', Type: 'String', Overwrite: false });
    expect(JSON.parse(put.args[0].input.Value!)).toEqual(launch());
  });

  it("passes on SSM's other failures", async () => {
    ssm.on(PutParameterCommand).rejects(awsError('AccessDeniedException'));
    await expect(flag().raise(launch())).rejects.toThrow('AccessDeniedException');
    ssm.on(GetParameterCommand).rejects(awsError('ThrottlingException'));
    await expect(flag().read()).rejects.toThrow('ThrottlingException');
  });

  it('reads none when the parameter is missing, and a launch when it is up', async () => {
    ssm.on(GetParameterCommand).rejectsOnce(awsError('ParameterNotFound')).resolves({ Parameter: { Value: JSON.stringify(launch()) } });
    expect(await flag().read()).toBeUndefined();
    expect(await flag().read()).toEqual(launch());
    expect(ssm.commandCalls(GetParameterCommand)[0].args[0].input).toEqual({ Name: '/hp-dashboard/launch-flags/mock' });
  });

  it('is stale at its expiry and after, not before', () => {
    expect(LaunchFlag.isStale(launch(), AT(4.99))).toBe(false);
    expect(LaunchFlag.isStale(launch(), AT(5))).toBe(true);
    expect(LaunchFlag.isStale(launch(), AT(30))).toBe(true);
  });

  it('is resolved once the newest run is no earlier than the launch, less 30 seconds of clock slack', () => {
    expect(LaunchFlag.isResolved(launch(), undefined)).toBe(false);
    expect(LaunchFlag.isResolved(launch(), '2026-11-15T02:00:00.000Z')).toBe(false);
    expect(LaunchFlag.isResolved(launch(), '2026-11-16T13:59:29.999Z')).toBe(false);
    expect(LaunchFlag.isResolved(launch(), '2026-11-16T13:59:30.000Z')).toBe(true);
    expect(LaunchFlag.isResolved(launch(), '2026-11-16T14:03:12.345Z')).toBe(true);
    expect(LaunchFlag.isResolved(launch(), 'not a time')).toBe(false);
  });

  it('lowers a flag, and one already gone is fine', async () => {
    ssm.on(DeleteParameterCommand).resolvesOnce({}).rejectsOnce(awsError('ParameterNotFound'));
    await flag().lower();
    await flag().lower();
    expect(ssm.commandCalls(DeleteParameterCommand).map(call => call.args[0].input)).toEqual([
      { Name: '/hp-dashboard/launch-flags/mock' }, { Name: '/hp-dashboard/launch-flags/mock' }
    ]);
  });

  describe('assess', () => {
    const up = (value: string) => ssm.on(GetParameterCommand).resolves({ Parameter: { Value: value } });

    it('finds nothing to do when no flag is up', async () => {
      ssm.on(GetParameterCommand).rejects(awsError('ParameterNotFound'));
      expect(await flag().assess('2026-11-16T14:03:00.000Z')).toBeUndefined();
      expect(ssm.commandCalls(DeleteParameterCommand)).toHaveLength(0);
    });

    it('lowers the flag once its run has appeared', async () => {
      up(JSON.stringify(launch()));
      expect(await flag().assess('2026-11-16T14:00:40.000Z')).toBeUndefined();
      expect(ssm.commandCalls(DeleteParameterCommand)).toHaveLength(1);
    });

    it('lowers the flag once its time is up, its run never seen', async () => {
      up(JSON.stringify(launch()));
      now = AT(6);
      expect(await flag().assess('2026-11-15T02:00:00.000Z')).toBeUndefined();
      expect(ssm.commandCalls(DeleteParameterCommand)).toHaveLength(1);
    });

    it('returns the launch while its run is still to appear and its time is not up', async () => {
      up(JSON.stringify(launch({ resetState: true })));
      now = AT(12);
      expect(await flag().assess('2026-11-15T02:00:00.000Z')).toEqual(launch({ resetState: true }));
      expect(await flag().assess(undefined)).toEqual(launch({ resetState: true }));
      expect(ssm.commandCalls(DeleteParameterCommand)).toHaveLength(0);
    });

    it("lowers something kept that isn't a launch, rather than let it block every start", async () => {
      up('{"not":"a launch"}');
      expect(await flag().assess(undefined)).toBeUndefined();
      expect(ssm.commandCalls(DeleteParameterCommand)).toHaveLength(1);
    });
  });

  it('works the same kept in memory (offline)', async () => {
    const memory = LaunchFlag.inMemory('mock', () => now);
    expect(await memory.raise(launch())).toBe(true);
    expect(await memory.raise(launch())).toBe(false);
    expect(await memory.assess(undefined)).toEqual(launch());
    now = AT(5);
    expect(await memory.assess(undefined)).toBeUndefined();
    expect(await memory.read()).toBeUndefined();
    expect(await new MemoryLaunchFlagStore().get('x')).toBeUndefined();
  });
});
