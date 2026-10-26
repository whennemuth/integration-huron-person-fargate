# Plan: Per-Landscape Dashboards (dashboard merged into the fargate app)

Status: **Planned - not yet implemented**

> Supersedes the earlier "pan-landscape data exchange" plan (cross-account dashboard-api Lambda,
> landscape registry, versioned contracts). That approach was dropped: with one dashboard per
> landscape there is no contract between independently deployed apps, no cross-account access,
> and no second copy of each landscape's integration config.
>
> Moved here from the dashboard repo's `docs/` in Phase 2.

## Goal

Every fargate landscape gets its own dashboard, deployed from the same repository and CDK app as
that landscape's pipeline, reachable at `<landscape>.huron1.cssnprd.warhen.work`. The dashboard
reads its own landscape's run data and performs single-person lookup/sync in-process, using that
landscape's config secret and code version. "Pan-landscape" becomes navigation between these
dashboards. **No UI** is in scope - only the data access ("wiring"), plus the data a future
landscape picker needs.

## Decisions

| Topic | Decision |
|---|---|
| Code home | Dashboard source moves into the fargate repo (one repo, one CDK app); the dashboard repo is retired after migration |
| Stack arrangement | Separate dashboard stack per landscape in the same CDK app (`fargate-<landscape>` and `dashboard-<landscape>`), so CloudFront / Lambda@Edge deploy and deletion times never block pipeline deploys |
| Mock landscapes | Always get a dashboard stack |
| Data access | In-process: the dashboard Lambda uses fargate's `StatisticsTable`, `PersonHistoryTable` etc. directly. No API contract |
| Integration config | The dashboard reads its landscape's existing fargate config secret (`${STACK_ID}/integration/_config/${landscape}`); the dashboard's own `Secrets.ts` copy is removed |
| Cross-stack references | Deterministic resource names (table names, secret name), not CloudFormation exports, so the fargate stack is never locked by imports |
| DNS / certificates | A records are CDK resources, as today: the `shibboleth-sp-at-edge` construct creates each landscape's A record in `cssnprd.warhen.work`, plus its SSM ownership parameter (`/route53/arecords/<record name>`), when no record for that name exists yet; an existing record it doesn't own is left alone. Certificates stay imported by ARN in context (a single wildcard `*.huron1.cssnprd.warhen.work` certificate can serve every landscape) |
| Bare hostname `huron1.cssnprd.warhen.work` | Redirects to the default landscape's subdomain (the URL always names the landscape) |
| Cross-landscape sign-in | Undecided - investigate `shibboleth-sp-at-edge` first (Phase 6) |
| Run summary | Post-processor writes a `RUN_SUMMARY` item per run; both storage modes |
| Reporting depth | First the distinct list of syncRunIds; after one is selected, drill down as deep as the storage mode allows |
| Landscape picker data | A repo-level list of landscapes and hostnames, baked in at deploy time and served by `GET /api/landscapes` |
| Run-start (bulk pipeline) command | Out of scope |
| UI | Out of scope |

## Phases and Steps

### Phase 0 - Prerequisite

0. **Distinct-run index.** *Done - committed to fargate `master` as `5b58b2a` (applied from the
   `statistics` stash).* After deploying, wait for the index to be `ACTIVE`, then run the
   `backfill-runs` harness task once per landscape.

### Phase 1 - fargate run data (depends on Phase 0)

*Done - committed to fargate `master` as `db1482e`.*

1. **Persist per-chunk IntegrationResult counts.** Extend `SyncStatistics` in
   `src/ApiErrorTracking.ts` with optional `successCount`, `failureCount`, `skippedCount`,
   `addedCount`, `updatedCount`, `removedCount` (`totalProcessed` is already stored as
   `totalRecords`). Give `writeTrackerStatistics()`
   (`src/processing/ProcessorCommon.ts`) an optional `IntegrationResult` param; pass `result` from
   `ProcessorForS3.ts` and `ProcessorForDynamoDb.ts`.
2. **Return deletion results.** `AbstractPostProcessor.runDeferredDeletes()` returns a
   `DeletionOutcome` (defined in `StatisticsTable.ts`): `COMPLETED` (the `DeferredDeleteResult`
   counts), `SKIPPED` with a reason (`TERMINAL_ERROR` / `PERSON_DELTA` / `DELETES_DISABLED` /
   `NO_MERGE_OUTPUT` / `MISSING_TABLES`), or `FAILED`. Updated `PostProcessorForS3` and
   `PostProcessorForDynamoDB`; `main()` records the two skips it decides itself.
3. **RUN_SUMMARY in StatisticsTable.** `buildRunSummary()`, `writeRunSummary()`,
   `readRunSummary()` (SK = `RUN_SUMMARY`). `buildRunSummary()` sums `getAllChunkStatistics()`
   (results, errors, throttles), merges `errorsByStatus`, and folds in METADATA (chunkCount,
   totalRecords, createdAt, syncPopulation, bulkReset), the dryRun flag, storage mode and the
   deletion outcome. Removed the dead `getStatistics()`; the harness `statistics` task now prints
   the run summary.
4. **Write the summary in the post-processor.** In the `AbstractPostProcessor.main()` `finally`
   block, write `RUN_SUMMARY` with status `SUCCEEDED`, `FAILED` (deletes skipped because of a
   terminal error) or `POST_PROCESSING_FAILED`; a write failure only logs a warning.
   `DYNAMODB_STATISTICS_TABLE_NAME` is confirmed in the post-processor environment in both modes.
5. **Paged queries.** *Parallel with 1-4.*
   - `DynamoDBTable.queryPage()` / `queryGSIPage()`: `Limit` + `ExclusiveStartKey`, returning
     `{ items, lastEvaluatedKey }` (existing fetch-all methods unchanged).
   - `StatisticsTable.getRunErrorsPage(syncRunId, { statusCode?, limit, startKey })` (SK prefix
     `ERROR:` or `ERROR:<code>:`).
   - `PersonHistoryTable.getChangesInSyncRunPage(syncRunId, changeType: 'NEW' | 'UPDATED' | 'DELETED', page)`
     on the `syncRunId-changeType` GSI.

6. **Run reporting module** - `src/reporting/RunReporting.ts` (plain class over the tables, no
   transport layer). *Depends on 3 and 5.*
   - `listRuns` - `getUniqueIntegrationTimestamps()`
   - `getRunReport` - stored `RUN_SUMMARY`, else assembled from METADATA + FLAGS + TERMINAL_ERROR
     with status `FAILED` / `INCOMPLETE` (DynamoDB mode reads the statistics table; S3 mode reads
     the S3 files under METADATA's `chunkDirectory` - a run with no METADATA can't be located there)
   - `getRunChunkStatistics`
   - `getRunErrors` - paged (default 100, max 500), optional status-code filter
   - `getRunChanges` - paged in DynamoDB mode; `UNSUPPORTED_IN_STORAGE_MODE` in S3 mode (S3 mode
     keeps no per-person change record per run)
   - `getCapabilities` - landscape, storage mode, whether change lists are supported

   Page cursors are opaque base64url strings, rejected unless their key attributes match the
   requested run (and change type). Errors are `ReportingError`s with a `code`
   (`INVALID_CURSOR`, `INVALID_LIMIT`, `UNSUPPORTED_IN_STORAGE_MODE`) for the routes to map.

### Phase 2 - Move the dashboard source into the fargate repo (no behavior change; parallel with Phase 1)

*Done - see the warning in step 9.*

7. **Relocate source** into a `dashboard/` directory of the fargate repo, keeping the dashboard's
   internal layout (`dashboard/{bin,lib,src,context,templates,public,scripts}`, plus
   `dashboard/cdk.json` and `dashboard/CLAUDE.md`). Keeping the layout means no relative import or
   `__dirname` path changes (a pure copy), and the dashboard stays deployable as its own CDK app
   until Phase 3. The dashboard repo is left intact until Phase 5; secrets that git ignores there
   (`.env`, `context/*.key|*.pem|*.xml`) are not copied. This plan moved to fargate `docs/`.
8. **Dependencies** - added to fargate `package.json`: `mustache`, `express`,
   `@aws-sdk/client-secrets-manager`, `shibboleth-sp-at-edge` (same `file:` path - the repos sit at
   the same depth); dev deps `nodemon`, `html-minifier-terser`, `@types/express`,
   `@types/mustache`, `@types/html-minifier-terser` (`esbuild`, `@types/aws-lambda` already
   present). Scripts (run from the fargate root): `dashboard:build:templates` (`ts-node
   --transpile-only`, since fargate's `module: Node16` rejects `require` of the ESM
   `html-minifier-terser` types - the script was never type-checked in the dashboard repo either),
   `dashboard:dev`, `dashboard:dev:watch`, `dashboard:synth`, `dashboard:deploy`,
   `dashboard:teardown`, `dashboard:clearcache`, `dashboard:publish-dev`. `tsconfig.json` includes
   `dashboard/**/*.ts` (excluding `dashboard/scripts`); `.gitignore` covers the generated
   `TemplateConstants.ts`, the browser scripts and `dashboard/cdk.out/`. The dashboard compiles
   against fargate's `integration-huron-person` build with no changes.
9. **aws-cdk-lib alignment** - `shibboleth-sp-at-edge` dedupes onto fargate's `aws-cdk-lib@2.260.0`.
   The newer `shibboleth-sp-at-edge-1.0.0.tgz` build (Feb 12; the dashboard repo still has the Jan 25
   build installed under the same version) is used. It requires `@aws-sdk/client-acm` without
   declaring it, so fargate now depends on it directly. Compared with the old build's synth, the new
   one is **not a drop-in**: it renames the CloudFront construct (new logical IDs, so the
   distribution and its logs bucket would be replaced), and adds a context-config bucket that
   uploads the whole dashboard context JSON to S3. Both builds create the A record as a CDK
   resource with an SSM ownership parameter, and skip it when a record for the name already exists
   and the parameter doesn't name this stack. Synthesized from here, the new build doesn't
   recognize the existing `huron1` record's parameter as its own, so it leaves the record out of the
   template (deploying over the old stack would delete that record). `aws-cdk-lib` 2.260 also moves
   `NODEJS_LATEST` Lambdas from `nodejs22.x` to `nodejs24.x`. **Do not run `dashboard:deploy`
   against the existing dashboard stack**; the first dashboard deploy from this repo is Phase 3's
   new per-landscape stacks, whose subdomains have no records yet, so each stack creates its own.
10. **Gate:** `npx tsc --noEmit`, full `npx jest` (714 pass) and `cdk synth` of the fargate stack
    pass; the dashboard synthesizes from `dashboard/` (`AWS_PROFILE=infnprd`). Its integration
    secret differs from the old repo's only in credentials the old repo read from its git-ignored
    `.env`, which was not copied (Phase 3 removes that secret).

### Phase 3 - Dashboard stack per landscape (depends on Phase 2)

11. **Context** - add a `DASHBOARD` section to fargate's `IContext` and every
    `context/context.*.json`, absorbing the dashboard's current context fields:
    `AUTHENTICATION_MODE`, `ORIGIN` (`subdomain`, e.g. `preview.huron1.cssnprd.warhen.work`,
    `appAuthorization`), `DNS` (`hostedZone` - required, since the construct only manages the A
    record when both `ORIGIN.subdomain` and `DNS.hostedZone` are set - and `certificateARN`),
    Shibboleth/SAML settings, `APP_LOGIN_HEADER` /
    `APP_LOGOUT_HEADER`, `LAMBDA_TIMEOUT_SECONDS` / `LAMBDA_MEMORY_SIZE_MB`. `INTEGRATION_CONFIG`
    is dropped. `STACK_ID`, `ACCOUNT`, `REGION`, `TAGS` are shared with the fargate stack.
12. **bin/App.ts** - create `dashboard-<landscape>` (stack name derived from `STACK_ID` +
    landscape) next to the fargate stack, Public or Authenticated per `AUTHENTICATION_MODE` as the
    dashboard's `bin/App.ts` does today (including the async
    `ShibbolethAtEdgeConstruct.getInstance()`). Always created, including mock landscapes.
13. **Config from the fargate secret** - delete `lib/dashboard/Secrets.ts`. `FunctionUrl.ts`
    references the landscape's integration secret by name
    (`Secret.fromSecretNameV2(..., '${STACK_ID}/integration/_config/${landscape}')`), grants read,
    and sets the secret reference for `DashboardCache`. `LocalServer.ts`'s
    `OVERRIDE_CACHE_WITH_LOCAL_CONTEXT` stops depending on `INTEGRATION_CONFIG` (local runs load
    the integration config file / secret like other fargate harnesses).
14. **Grants for in-process data access** - by deterministic name: statistics table + its indexes
    (read), person history table + indexes (read, DynamoDB mode only), person current state table
    (read, if lookups need it), chunks bucket (read, for S3-mode flags / terminal-error files).
    Environment variables for table names, bucket, storage mode and landscape. Raise the default
    Lambda timeout from 10s to 30s (person sync).

### Phase 4 - In-process routes and services (depends on Phases 1 and 3)

15. **RunReportingService** - new interface in `ServiceTypes.ts`, real implementation over
    `src/reporting/RunReporting.ts`, mock in `DashboardMocks.ts`, registered in `ServiceProvider.ts`.
16. **Routes** in `DashboardHandler.ts` (and `LocalServer.ts`):
    - `GET /api/runs`
    - `GET /api/runs/{syncRunId}/summary`
    - `GET /api/runs/{syncRunId}/chunks`
    - `GET /api/runs/{syncRunId}/errors?cursor&limit&statusCode`
    - `GET /api/runs/{syncRunId}/changes?changeType&cursor&limit`
    - `GET /api/capabilities`

    `syncRunId` validated against the ISO-timestamp pattern; `limit` capped (e.g. 500); errors
    returned without stack traces. No `landscape` parameter - the landscape is the deployment.
17. **Landscape picker data** - a repo-level `context/landscapes.json`
    (`[{ landscape, label, url, isDefault }]`), baked into the Lambda environment at synth, served
    by `GET /api/landscapes` with the current landscape flagged (`local` when run locally).
18. **Person lookup/sync** - unchanged logic, now running against the landscape's own config.
    `History` / `SystemStatus` stubs drop their unused `Config` constructor parameter.

### Phase 5 - DNS, redirect and migration (depends on Phase 3; auth decision from Phase 6)

19. **Redirect stack** - a small `dashboard-redirect` stack (deployed once, enabled by a context
    flag on the default landscape): CloudFront distribution with a CloudFront Function returning
    `302` to `https://<default>.huron1.cssnprd.warhen.work` + original path/query; imported
    certificate covering `huron1.cssnprd.warhen.work`. It creates the `huron1` A record the same
    way the construct does (CDK `ARecord` plus the `/route53/arecords/<record name>` ownership
    parameter naming this stack), only when no record for the name exists.
20. **Migration sequence**:
    1. Deploy `dashboard-staging` at `staging.huron1.cssnprd.warhen.work` (the stack creates its A
       record); verify.
    2. Deploy the other landscapes' dashboards.
    3. Destroy the old standalone dashboard stack. That deletes its `huron1` A record and
       ownership parameter, and releases the `huron1.cssnprd.warhen.work` alternate domain name,
       which a distribution must release before another can claim it.
    4. Deploy `dashboard-redirect`; with no `huron1` record left, it creates one pointing at the
       redirect distribution.
    5. Archive the dashboard repo.

### Phase 6 - Authentication investigation (parallel with everything; decide before Phase 5)

21. Read `shibboleth-sp-at-edge` (`wp_to_cloud/bu-lambda-shibboleth`) and answer:
    - Cookie `Domain` / `Path` / `SameSite` - host-only, or configurable to the parent domain?
    - JWT signing key source (`jwtPrivateKeySecretFld` / `jwtPublicKeySecretFld` in the
      configured secret), and whether landscapes can share one.
    - How the SAML ACS URL / entityId derive from the hostname, and whether each new subdomain
      needs IdP registration (the current entityId is a wildcard:
      `https://*.kualitest.research.bu.edu/shibboleth`).
22. Decide: per-landscape sign-in (relying on the IdP session for silent re-login) vs a shared
    parent-domain token; record the decision here and adjust the Phase 3 context.

## Relevant Files

**integration-huron-person-fargate**
- `src/dynamodb/StatisticsTable.ts`, `src/dynamodb/DynamoDBTable.ts`, `src/dynamodb/PersonHistoryTable.ts`
- `src/ApiErrorTracking.ts`, `src/processing/ProcessorCommon.ts`, `src/processing/ProcessorForS3.ts`, `src/processing/ProcessorForDynamoDb.ts`
- `src/post-processing/AbstractPostProcessor.ts`, `src/post-processing/PostProcessorForS3.ts`, `src/post-processing/PostProcessorForDynamoDB.ts`
- `context/IContext.ts`, `context/context.*.json`, `bin/App.ts`, `package.json`, `lib/services/post-processor/PostProcessorTaskDefinition.ts`
- New: `src/reporting/RunReporting.ts`, `src/dashboard/**`, `lib/dashboard/**`, `dashboard/{templates,public,scripts}`, `context/landscapes.json`, redirect stack

**integration-huron-person-dashboard** (source of the move; retired afterwards)
- `bin/App.ts`, `lib/*`, `src/**`, `templates/`, `public/`, `scripts/BuildTemplates.ts`, `context/IContext.ts`, `context/*.json`

## Verification

1. **Phase 1 unit tests** - summary building (chunk sums, combined `errorsByStatus`, deletion
   result and skip reasons); post-processor writes the summary on success and failure; paged
   queries; cursor-tampering rejection; `RunReporting` per storage mode (S3 returns
   `UNSUPPORTED_IN_STORAGE_MODE` for changes).
2. **Phase 2 gate** - `npx tsc --noEmit`, full `npx jest`, `cdk synth` unchanged for the fargate
   stack; `dashboard:dev` with `LOCALHOST_WITH_MOCKS=true` serves the dashboard as before.
3. **Phase 3/4 tests** - CDK template assertions for the dashboard stack (secret/table/bucket
   grants by name, no `Fn::ImportValue` against the fargate stack, timeout, an A record and
   ownership parameter for the landscape's subdomain); route tests for
   `/api/runs*`, `/api/capabilities`, `/api/landscapes` (validation, limit cap, mocks).
4. **Manual, mock landscape first** - deploy `dashboard-mock` and confirm its A record and
   ownership parameter were created, run a
   source-simulator pipeline; `/api/runs` lists the run; `summary`, `chunks`, `errors`, `changes`
   return data; person lookup/sync reaches the target simulator. Repeat on staging; then the
   migration sequence and redirect check (`huron1...` -> `staging.huron1...`, path/query kept).

## Out of Scope

- Any UI (reporting views, landscape picker widget)
- Starting a full pipeline run (Bulk Operations)
- System status checks
- Cross-landscape views (e.g. one person across landscapes)
- Certificate management (certificates stay imported by ARN)
- README updates (CLAUDE.md files will be updated)

## Open Considerations

1. **Bundled data files** - the data mapper's state/country lookups come from CSV files; esbuild
   does not copy non-JS assets. Verify the dashboard Lambda bundle still includes them after the
   move.
2. **Per-landscape cost** - each landscape, including mock ones, runs a CloudFront distribution
   and Lambda@Edge functions, and may need an IdP registration (Phase 6).
3. **Picker staleness** - `context/landscapes.json` is baked in at deploy time; adding a landscape
   requires redeploying the others for it to appear in their pickers. A runtime source (e.g. a
   small JSON object fetched by the browser) can replace it later.
