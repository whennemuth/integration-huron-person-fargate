/**
 * Tests the teardown step that deletes the dashboard's Lambda@Edge function versions
 * (dashboard/scripts/DeleteEdgeVersions.ts): it must never run while the distribution exists.
 */
import { IContext as LandscapeContext } from '../context/IContext';
import { deleteDashboardEdgeVersions, EdgeVersionDeletion } from '../dashboard/scripts/DeleteEdgeVersions';

const landscapeContext = (DASHBOARD?: Record<string, any>) => ({
  STACK_ID: 'huron-person-fargate-processor',
  ACCOUNT: '123456789012',
  REGION: 'us-east-2',
  TAGS: { Landscape: 'preview', Service: 'test', Function: 'test' },
  ...(DASHBOARD ? { DASHBOARD } : {})
}) as unknown as LandscapeContext;

const authenticated = landscapeContext({ STACK_ID: 'hp-dashboard', AUTHENTICATION_MODE: 'authenticated' });

const deletion = (distributionId?: string) => ({
  findDistribution: jest.fn().mockResolvedValue(distributionId),
  deleteVersions: jest.fn().mockResolvedValue(undefined)
}) satisfies EdgeVersionDeletion;

describe('deleteDashboardEdgeVersions', () => {
  beforeEach(() => jest.spyOn(console, 'log').mockImplementation(() => {}));
  afterEach(() => jest.restoreAllMocks());

  it('deletes the versions of all three edge functions once the distribution is gone', async () => {
    const d = deletion(undefined);
    await deleteDashboardEdgeVersions(authenticated, d);
    expect(d.findDistribution).toHaveBeenCalledWith({ stackId: 'hp-dashboard', landscape: 'preview' });
    expect(d.deleteVersions).toHaveBeenCalledWith(
      'hp-dashboard-preview-SPFunctionViewerRequest,hp-dashboard-preview-SPFunctionOriginRequest,hp-dashboard-preview-SPFunctionViewerResponse',
      'preview'
    );
  });

  it('refuses while the distribution exists, since removing its edge functions would open the dashboard', async () => {
    const d = deletion('EC4HMKSX3H4CA');
    await expect(deleteDashboardEdgeVersions(authenticated, d)).rejects.toThrow(/Distribution EC4HMKSX3H4CA .* still exists/);
    expect(d.deleteVersions).not.toHaveBeenCalled();
  });

  it('does nothing for a dashboard without edge functions', async () => {
    const d = deletion(undefined);
    await deleteDashboardEdgeVersions(landscapeContext({ STACK_ID: 'hp-dashboard', AUTHENTICATION_MODE: 'public' }), d);
    await deleteDashboardEdgeVersions(landscapeContext(), d);
    expect(d.findDistribution).not.toHaveBeenCalled();
    expect(d.deleteVersions).not.toHaveBeenCalled();
  });
});
