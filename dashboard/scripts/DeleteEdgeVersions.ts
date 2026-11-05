/**
 * Teardown step: deletes every numbered version of the landscape's Lambda@Edge functions (context/context.json),
 * waiting for CloudFront to remove its replicas, so the edge-lambda stack can then be destroyed in one go.
 * bin/teardown.sh runs it between destroying the dashboard stack and destroying the edge-lambda stack:
 *
 *   npm run -s dashboard:delete-edge-versions
 *
 * Why: destroying the dashboard stack deletes its CloudFront distribution, but CloudFront takes hours to remove
 * its replicas of the edge functions, and until then Lambda refuses to delete them, so destroying the
 * edge-lambda stack right away fails (DELETE_FAILED). shibboleth-sp-at-edge's deleteEdgeLambdaVersions retries
 * each version's deletion until its replicas are gone (up to about two hours per version).
 *
 * Only after the distribution is gone: deleteEdgeLambdaVersions first removes the edge functions from the
 * landscape's distribution, and the dashboard trusts every request that reaches it to have been signed in by
 * them, so on a live distribution it would open the dashboard to anyone. This refuses while the distribution
 * exists.
 *
 * Takes no arguments on purpose: deleteEdgeLambdaVersions treats a first argument of "dryrun" as a dry run of
 * its deletions only, not of removing the edge functions from the distribution.
 */
import { deleteEdgeLambdaVersions, EDGE_FUNCTION_BASENAMES, edgeFunctionName, pruneEdgeLambdaVersions } from 'shibboleth-sp-at-edge';
import { IContext as LandscapeContext } from '../../context/IContext';
import { edgePruneTarget } from './PruneEdgeVersions';

export type EdgeVersionDeletion = {
  /** Read-only probe: resolves with the landscape's distribution id, or undefined if it has none */
  findDistribution: (target: { stackId: string; landscape: string }) => Promise<string | undefined>;
  deleteVersions: (functionList: string, landscape: string) => Promise<void>;
};

const defaultDeletion: EdgeVersionDeletion = {
  // A dry run deletes nothing, and reports the distribution it found
  findDistribution: async target => (await pruneEdgeLambdaVersions({ ...target, dryrun: true })).distributionId,
  deleteVersions: deleteEdgeLambdaVersions
};

/**
 * Delete every version of the landscape's edge functions, once its distribution is gone. Does nothing for a
 * dashboard without edge functions; throws while the distribution still exists.
 */
export const deleteDashboardEdgeVersions = async (landscapeContext: LandscapeContext, deletion: EdgeVersionDeletion = defaultDeletion): Promise<void> => {
  const target = edgePruneTarget(landscapeContext);
  if ( ! target) {
    console.log('The dashboard has no Lambda@Edge functions (no DASHBOARD section, or not authenticated): nothing to delete.');
    return;
  }
  const distributionId = await deletion.findDistribution(target);
  if (distributionId) {
    throw new Error(`Distribution ${distributionId} of ${target.stackId}-${target.landscape} still exists. Destroy the dashboard ` +
      `stack first: deleting the edge function versions now would remove sign-in from the live dashboard.`);
  }
  const functionNames = EDGE_FUNCTION_BASENAMES.map(basename => edgeFunctionName(target.stackId, target.landscape, basename));
  console.log(`Deleting every version of ${functionNames.join(', ')}. This waits for CloudFront to remove its replicas, ` +
    `which can take a few hours after the distribution was deleted.`);
  await deletion.deleteVersions(functionNames.join(','), target.landscape);
}

if (require.main === module) {
  // Lambda@Edge functions live in us-east-1: look there first, rather than in the profile's default region
  process.env.AWS_REGION = 'us-east-1';
  const landscapeContext = require('../../context/context.json') as LandscapeContext;
  deleteDashboardEdgeVersions(landscapeContext).catch(e => {
    console.error(`${e?.name ?? 'Error'}: ${e?.message ?? e}`);
    process.exitCode = 1;
  });
}
