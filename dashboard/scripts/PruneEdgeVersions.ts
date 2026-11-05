/**
 * Deletes the Lambda@Edge function versions that earlier dashboard deploys left behind, for the landscape in
 * context/context.json. bin/deploy.sh runs it after every deploy that includes the dashboard stack:
 *
 *   npm run -s dashboard:prune-edge-versions [-- --dryrun]
 *
 * Why: a deploy that changes an edge function (its code, or a SHIBBOLETH setting baked into the bundle's
 * edge-context.json) publishes a new version. CloudFormation fails to delete the version it replaced, since
 * Lambda refuses until CloudFront has removed its replicas (hours later), so the deploy logs
 * "DELETE_FAILED (skipped)" and leaves the version behind. shibboleth-sp-at-edge's pruneEdgeLambdaVersions
 * deletes versions older than the ones the distribution uses, without touching the distribution. A version
 * still replicated is left for the next deploy's run, so each run deletes what earlier deploys left.
 *
 * Only an authenticated dashboard has edge functions. The construct names them from the dashboard context
 * (DASHBOARD.STACK_ID and TAGS.Landscape, see dashboard/lib/DashboardStacks.ts), so they are found the same way.
 *
 * Never fails the deploy: errors are reported and the exit code stays 0.
 */
import { pruneEdgeLambdaVersions } from 'shibboleth-sp-at-edge';
import { IContext as LandscapeContext } from '../../context/IContext';
import { AuthenticationMode, DashboardSettings, toDashboardContext } from '../context/IContext';

export type EdgePruneTarget = { stackId: string; landscape: string };

/**
 * The stackId and landscape the landscape's edge functions were named with, or undefined if its dashboard
 * has no edge functions (no DASHBOARD section, or a public dashboard).
 */
export const edgePruneTarget = (landscapeContext: LandscapeContext): EdgePruneTarget | undefined => {
  if ( ! landscapeContext.DASHBOARD) {
    return undefined;
  }
  const context = toDashboardContext(landscapeContext, landscapeContext.DASHBOARD as unknown as DashboardSettings);
  if (context.AUTHENTICATION_MODE !== AuthenticationMode.AUTHENTICATED) {
    return undefined;
  }
  return { stackId: context.STACK_ID, landscape: context.TAGS.Landscape };
}

export const pruneDashboardEdgeVersions = async (landscapeContext: LandscapeContext, dryrun: boolean): Promise<void> => {
  const target = edgePruneTarget(landscapeContext);
  if ( ! target) {
    console.log('The dashboard has no Lambda@Edge functions (no DASHBOARD section, or not authenticated): nothing to prune.');
    return;
  }
  console.log(`Pruning old Lambda@Edge versions of ${target.stackId}-${target.landscape}${dryrun ? ' (dry run)' : ''}...`);
  const { deleted, kept, deferred, failed } = await pruneEdgeLambdaVersions({ ...target, dryrun });
  console.log(`${dryrun ? 'Would delete' : 'Deleted'} ${deleted.length}, kept ${kept.length} in use, ` +
    `deferred ${deferred.length} still replicated (a later deploy deletes them), failed ${failed.length}.`);
}

if (require.main === module) {
  const dryrun = process.argv.slice(2).includes('--dryrun');
  const landscapeContext = require('../../context/context.json') as LandscapeContext;
  pruneDashboardEdgeVersions(landscapeContext, dryrun).catch(e => {
    console.warn(`Pruning old Lambda@Edge versions failed (the deploy is unaffected): ${e?.name ?? 'Error'}: ${e?.message ?? e}`);
  });
}
