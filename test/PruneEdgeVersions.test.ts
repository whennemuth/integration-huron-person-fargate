/**
 * Tests the post-deploy Lambda@Edge version pruning (dashboard/scripts/PruneEdgeVersions.ts): which
 * dashboards have edge functions, and the names they are pruned by.
 */
import { pruneEdgeLambdaVersions } from 'shibboleth-sp-at-edge';
import { IContext as LandscapeContext } from '../context/IContext';
import { edgePruneTarget, pruneDashboardEdgeVersions } from '../dashboard/scripts/PruneEdgeVersions';

jest.mock('shibboleth-sp-at-edge', () => ({ pruneEdgeLambdaVersions: jest.fn() }));
const prune = pruneEdgeLambdaVersions as jest.MockedFunction<typeof pruneEdgeLambdaVersions>;

const landscapeContext = (DASHBOARD?: Record<string, any>) => ({
  STACK_ID: 'huron-person-fargate-processor',
  ACCOUNT: '123456789012',
  REGION: 'us-east-2',
  TAGS: { Landscape: 'preview', Service: 'test', Function: 'test' },
  ...(DASHBOARD ? { DASHBOARD } : {})
}) as unknown as LandscapeContext;

describe('edgePruneTarget', () => {
  it('uses the dashboard STACK_ID and the landscape the edge functions were named with', () => {
    expect(edgePruneTarget(landscapeContext({ STACK_ID: 'hp-dashboard', AUTHENTICATION_MODE: 'authenticated' })))
      .toEqual({ stackId: 'hp-dashboard', landscape: 'preview' });
  });

  it('returns undefined for a public dashboard or a context without one', () => {
    expect(edgePruneTarget(landscapeContext({ STACK_ID: 'hp-dashboard', AUTHENTICATION_MODE: 'public' }))).toBeUndefined();
    expect(edgePruneTarget(landscapeContext())).toBeUndefined();
  });
});

describe('pruneDashboardEdgeVersions', () => {
  beforeEach(() => {
    prune.mockReset().mockResolvedValue({ distributionId: 'E1', deleted: [], kept: [], deferred: [], failed: [] });
    jest.spyOn(console, 'log').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  it('prunes an authenticated dashboard, passing dryrun through', async () => {
    await pruneDashboardEdgeVersions(landscapeContext({ STACK_ID: 'hp-dashboard', AUTHENTICATION_MODE: 'authenticated' }), true);
    expect(prune).toHaveBeenCalledWith({ stackId: 'hp-dashboard', landscape: 'preview', dryrun: true });
  });

  it('does not call the package for a dashboard without edge functions', async () => {
    await pruneDashboardEdgeVersions(landscapeContext({ STACK_ID: 'hp-dashboard', AUTHENTICATION_MODE: 'public' }), false);
    expect(prune).not.toHaveBeenCalled();
  });
});
