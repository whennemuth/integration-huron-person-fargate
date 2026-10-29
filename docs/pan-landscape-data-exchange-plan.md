# Plan: Per-Landscape Dashboards (dashboard merged into the fargate app)

Status: **In progress - Phases 0-4 done**

> Supersedes the earlier "pan-landscape data exchange" plan (cross-account dashboard-api Lambda,
> landscape registry, versioned contracts). That approach was dropped: with one dashboard per
> landscape there is no contract between independently deployed apps, no cross-account access,
> and no second copy of each landscape's integration config.
>
> Moved here from the dashboard repo's `docs/` in Phase 2.

## Goal

Every fargate landscape gets its own dashboard, deployed from the same repository and CDK app as
that landscape's pipeline, reachable at `<landscape>.cssnprd.warhen.work`. The dashboard
reads its own landscape's run data and performs single-person lookup/sync in-process, using that
landscape's config secret and code version. "Pan-landscape" becomes navigation between these
dashboards. **No UI** is in scope - only the data access ("wiring"), plus the data a future
landscape picker needs.

## Decisions

| Topic | Decision |
|---|---|
| Code home | Dashboard source moves into the fargate repo (one repo, one CDK app); the dashboard repo is retired after migration |
| Stack arrangement | Separate dashboard stack per landscape in the same CDK app (`huron-person-fargate-processor-<landscape>` and `hp-dashboard-<landscape>`), so CloudFront / Lambda@Edge deploy and deletion times never block pipeline deploys. The dashboard stack name must stay within 25 characters: the Shibboleth construct names a bucket `<stack name>-context-config-<account>-<region>` (S3 limit 63) |
| Deploys | `npm run deploy` deploys every stack (`cdk deploy --all`); `deploy-fargate` / `deploy-dashboard` deploy one (a named dashboard stack brings its us-east-1 edge-lambda stack, a dependency). All three run `bin/deploy.sh`, which synthesizes once and writes each deployed stack's template to `cdk.out/synth.output.fargate.yaml` / `synth.output.dashboard.yaml` (dumped from the cloud assembly, so `deploy` gets both files without a second synth). `teardown` destroys every stack; `teardown-fargate` / `teardown-dashboard` one |
| Mock landscapes | Always get a dashboard stack |
| Data access | In-process: the dashboard Lambda uses fargate's `StatisticsTable`, `PersonHistoryTable` etc. directly. No API contract |
| Integration config | The dashboard reads its landscape's existing fargate config secret (`${STACK_ID}/integration/_config/${landscape}`); the dashboard's own `Secrets.ts` copy is removed |
| Cross-stack references | Deterministic resource names (table names, secret name), not CloudFormation exports, so the fargate stack is never locked by imports |
| DNS / certificates | A records are CDK resources, as today: the `shibboleth-sp-at-edge` construct creates each landscape's A record in `cssnprd.warhen.work`, plus its SSM ownership parameter (`/route53/arecords/<record name>`), when no record for that name exists yet; an existing record it doesn't own is left alone. Certificates stay imported by ARN in context. Hostnames are `<landscape>.cssnprd.warhen.work` (no `huron1` level), so the existing `*.cssnprd.warhen.work` certificate, which covers one level only, serves every landscape; none of those names had a record in the zone |
| Bare hostname `huron1.cssnprd.warhen.work` | Redirects to the default landscape's hostname (the URL always names the landscape) |
| Cross-landscape sign-in | Undecided - investigate `shibboleth-sp-at-edge` first (Phase 6) |
| Run summary | Post-processor writes a `RUN_SUMMARY` item per run; both storage modes |
| Reporting depth | First the distinct list of syncRunIds; after one is selected, drill down as deep as the storage mode allows |
| Landscape picker data | A runtime registry, no repo-level list: each dashboard stack owns one SSM parameter (`/hp-dashboard/landscapes/<landscape>`, JSON) that it creates on deploy and CloudFormation deletes on teardown; the Lambda reads the path on cold start and serves it by `GET /api/landscapes` |
| Default landscape | `DASHBOARD.isDefault` in the landscape's context: written to its registry parameter and enabling the redirect stack. Synth fails if another landscape's parameter already claims the default |
| Run-start (bulk pipeline) command | Out of scope |
| UI | Out of scope |

## Phases and Steps

### Phase 0 - Prerequisite

0. **Distinct-run index.** *Done - committed to fargate `master` as `5b58b2a` (applied from the
   `statistics` stash).* After deploying, wait for the index to be `ACTIVE`, then run the
   `backfill-runs` harness task once per landscape.

### Phase 1 - fargate run data (depends on Phase 0)

*Done - committed to fargate `master` as `f24cb0a`.*

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
   the same depth; in Phase 3 `mustache`, `express`, `@aws-sdk/client-acm` and
   `shibboleth-sp-at-edge` moved to `optionalDependencies` - see step 14a); dev deps `nodemon`, `html-minifier-terser`, `@types/express`,
   `@types/mustache`, `@types/html-minifier-terser` (`esbuild`, `@types/aws-lambda` already
   present). Scripts (run from the fargate root): `dashboard:build:templates` (`ts-node
   --transpile-only`, since fargate's `module: Node16` rejects `require` of the ESM
   `html-minifier-terser` types - the script was never type-checked in the dashboard repo either;
   in Phase 3 fargate moved to `NodeNext`, as the dashboard repo used, so the script is now
   type-checked and `--transpile-only` is gone),
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

*Done. `hp-dashboard-mock` deployed and Shibboleth sign-in tested (step 14c).*

11. **Context** - fargate's `IContext` has an optional `DASHBOARD: DashboardSettings` (the
    dashboard's `IContext` minus `ACCOUNT` / `REGION` / `TAGS`, which come from the landscape;
    `toDashboardContext()` merges them). Every `context/context*.json` has one: `STACK_ID`
    `hp-dashboard`, `AUTHENTICATION_MODE` `authenticated`, `ORIGIN` (`subdomain`
    `<landscape>.cssnprd.warhen.work`, `appAuthorization`), `DNS` (`hostedZone` - required,
    since the construct only manages the A record when both `ORIGIN.subdomain` and `DNS.hostedZone`
    are set - and `certificateARN`), the Shibboleth/SAML settings and `APP_LOGIN_HEADER` /
    `APP_LOGOUT_HEADER` copied from the old dashboard context, and its `LAMBDA_TIMEOUT_SECONDS`
    (180) / `LAMBDA_MEMORY_SIZE_MB` (1024). `INTEGRATION_CONFIG` is dropped. The dashboard's
    `context/*.json` files are deleted. All landscapes share the old dashboard's Shibboleth secret
    (`shibsp/wp/devl1`) until Phase 6 decides otherwise.
12. **bin/App.ts** - calls `createDashboardStack()` (`dashboard/lib/DashboardStacks.ts`), which
    creates `${DASHBOARD.STACK_ID}-${landscape}` next to the fargate stack, Authenticated (with
    the async `ShibbolethAtEdgeConstruct.getInstance()`) or Public per `AUTHENTICATION_MODE`. No
    `DASHBOARD` section, no stack. `dashboard/bin/App.ts`, `dashboard/cdk.json` and
    `dashboard/bin/clear-cache.sh` are deleted (the root `clearcache` covers the one `cdk.out`).
    Scripts: `build` and `synth` build the templates first (bundling the dashboard Lambda needs
    the generated `TemplateConstants.ts`); `dashboard:get-stack-name` is new; `dashboard:synth`
    names the dashboard stack; `deploy` (all stacks), `deploy-fargate` and `deploy-dashboard` run
    `bin/deploy.sh <all|fargate|dashboard>`; `teardown` (all), `teardown-fargate` and
    `teardown-dashboard` (with its edge-lambda stack) destroy; `redeploy` is unchanged (all). `dashboard:deploy`,
    `dashboard:teardown` and `dashboard:clearcache` are removed.
13. **Config from the fargate secret** - `dashboard/lib/Secrets.ts` is deleted.
    `lib/Secrets.ts` exports `integrationSecretName()`, used by both stacks. `FunctionUrl.ts`
    references the secret by name (`Secret.fromSecretNameV2`), grants read, and passes the name as
    `DashboardCache`'s secret reference (`GetSecretValue` accepts a name). `LocalServer.ts`'s
    switch is renamed `OVERRIDE_CACHE_WITH_LOCAL_CONFIG` and loads the config with fargate's
    `getConfig()`, as the `PersonLookup` harness now does too.
14. **Grants for in-process data access** - `dashboard/lib/LandscapeResources.ts` derives the
    names the pipeline stack uses (storage mode via `resolveSecretStorage()`): read on the
    statistics table and its indexes, the person history table and its indexes (DynamoDB mode
    only) and the chunks bucket. The person current-state table is not granted (no lookup needs it
    yet). Environment: `LANDSCAPE`, `PREVIOUS_STORAGE_TYPE`, `DYNAMODB_STATISTICS_TABLE_NAME`,
    `DYNAMODB_PERSON_HISTORY_TABLE_NAME` (DynamoDB mode), `CHUNKS_BUCKET`. Default Lambda timeout
    raised from 10s to 30s (the contexts set 180s). The origin-verify SSM parameter is now per
    landscape (`/cloudfront/<STACK_ID>/<landscape>/origin-verify-secret`).

14a. **Keep the dashboard out of the task image** - Phase 2 broke `docker-build`: the image's
    `npm ci` could not reach the `file:` Shibboleth tarball outside the build context. The tasks
    never use the dashboard, so it is excluded rather than made installable: dashboard/CDK-only
    packages (`shibboleth-sp-at-edge`, `express`, `mustache`, `@aws-sdk/client-acm`) are
    `optionalDependencies` and the Dockerfile runs `npm ci --omit=optional` (both stages); the
    builder compiles with `npx tsc` instead of `npm run build`; and `context/IContext.ts` types
    `DASHBOARD` loosely (`{ STACK_ID, [key]: unknown }`), with `DashboardStacks.ts` applying
    `DashboardSettings`, so no task code compiles dashboard or Shibboleth types.
14b. **NodeNext** - fargate's `tsconfig.json` moved from `module`/`moduleResolution` `Node16` to
    `NodeNext` (the dashboard repo's setting), which lets `dashboard/scripts/BuildTemplates.ts`
    import the ESM `html-minifier-terser` types. `dashboard/scripts` is no longer excluded from
    the project, and `dashboard:build:templates` no longer needs `--transpile-only`.
14c. **shibboleth-sp-at-edge 2.0.1 / shibboleth-sp 1.2.0** - the first `hp-dashboard-mock` deploy returned CloudFront
    `503`: the installed 1.0.0 tarball (Feb 12) predated the source fix granting its
    viewer-request function read access to the Shibboleth secret, and its prebuilt edge bundles
    carried `bu-lambda-shibboleth`'s own (WordPress) context, ignoring `DASHBOARD.SHIBBOLETH`.
    Version 2.0.0 (`bu-lambda-shibboleth` `MASTER.PLAN.md`, Part 2) writes the consumer's context
    (`edge-context.json`) next to each prebuilt edge bundle at synth, grants `GetSecretValue` on
    the configured secret only, declares `@aws-sdk/client-acm`, and makes `aws-cdk-lib` /
    `constructs` peer dependencies. Fargate now installs it (and `shibboleth-sp` 1.1.0) from the
    `simple-npm-package-bucket` registry URL, still in `optionalDependencies`, and no longer
    declares `@aws-sdk/client-acm` itself. No fargate code changed: the construct's API and
    `IContext` are unchanged.

    The next deploy returned `502`: the dashboard Lambda crashed at load. Its `Authentication.ts`
    imports `shibboleth-sp`, whose 1.1.0 ES module build (picked by esbuild for an `import`)
    calls `createRequire(import.meta.url)`, and `import.meta.url` is undefined once
    `NodejsFunction` bundles it to CommonJS. `shibboleth-sp` 1.2.0 ships CommonJS only
    (`shibboleth-sp/PLAN-commonjs-only.md`), and `shibboleth-sp-at-edge` 2.0.1 depends on it;
    fargate installs 2.0.1. With both, the mock dashboard deploys and signs in.

    **Gate:** `npx tsc --noEmit`; full `npx jest` (722 pass, including `test/DashboardStack.test.ts`);
    `cdk synth hp-dashboard-mock` (`AWS_PROFILE=infnprd`) produces the A record and ownership
    parameter for `mock.cssnprd.warhen.work`, the edge-lambda stack, and no `Fn::ImportValue`.
    `bin/deploy.sh`'s synth and template dump steps (run without the deploy) write both
    `cdk.out/synth.output.*.yaml` files. `npm run docker-build` succeeds, and the image contains
    the three task entry points and none of the optional packages.

### Phase 4 - In-process routes and services (depends on Phases 1 and 3)

*Done.*

15. **RunReportingService** - new interface in `ServiceTypes.ts`, real implementation over
    `src/reporting/RunReporting.ts`, mock in `DashboardMocks.ts`, registered in `ServiceProvider.ts`.
    `RunReporting` satisfies the interface as it is, so the real implementation is that class,
    built from the Lambda environment by `runReportingFromEnvironment()`
    (`dashboard/src/services/RunReports.ts`; `LANDSCAPE`, `PREVIOUS_STORAGE_TYPE` and
    `DYNAMODB_STATISTICS_TABLE_NAME` are required). `ServiceProvider` keeps one instance per
    Lambda instance.
16. **Routes** in `DashboardHandler.ts` (and `LocalServer.ts`):
    - `GET /api/runs`
    - `GET /api/runs/{syncRunId}/summary`
    - `GET /api/runs/{syncRunId}/chunks`
    - `GET /api/runs/{syncRunId}/errors?cursor&limit&statusCode`
    - `GET /api/runs/{syncRunId}/changes?changeType&cursor&limit`
    - `GET /api/capabilities`

    `syncRunId` validated against the ISO-timestamp pattern; `limit` capped (e.g. 500); errors
    returned without stack traces. No `landscape` parameter - the landscape is the deployment.

    As built: GET only (other methods get `405` with `Allow: GET`); the `syncRunId` path segment
    is URL-decoded first, so `%3A` works. Responses: `/api/runs` `{ runs }` (oldest first),
    `summary` the `RunReport` (`404` when nothing is recorded for the run), `chunks` `{ items }`,
    `errors` / `changes` a page `{ items, cursor? }`, `/api/capabilities` the `Capabilities`.
    `limit` must be digits and `statusCode` three digits (else `400`); a `limit` over 500 is
    rejected, not clamped, by `RunReporting` (`INVALID_LIMIT`). `changeType` is required for
    `changes` (`NEW` | `UPDATED` | `DELETED`). A `ReportingError` (`INVALID_CURSOR`,
    `INVALID_LIMIT`, `UNSUPPORTED_IN_STORAGE_MODE`) is a `400` `{ error, code }`; anything else a
    `500` `{ error, message }`. `LocalServer.ts` already forwards every path to the handler, so it
    only logs the new endpoints.
17. **Landscape picker data** - a registry of SSM parameters, one per landscape, under the
    constant path `/hp-dashboard/landscapes/`. No repo-level list: adding a landscape never
    requires editing or redeploying the others.
    - **Registration** - the dashboard stack creates a CDK `StringParameter`
      `/hp-dashboard/landscapes/<landscape>`, so deploying registers the landscape and tearing
      it down deregisters it (CloudFormation deletes the parameter). No register script, and no
      parameter is written by more than one stack, so there are no read-modify-write races. The
      value is JSON, `{ landscape, label, url, isDefault }`, all derived at synth: `url` from
      `ORIGIN.subdomain` (`https://<landscape>.cssnprd.warhen.work`), `label` from an optional
      `DASHBOARD.LABEL` (default: the landscape name), `isDefault` from an optional
      `DASHBOARD.isDefault` (default `false`) - both new optional `DashboardSettings` fields in
      `dashboard/context/IContext.ts`. The parameter's description (up to 1024 characters)
      says what reads it, that the stack owns it, and how to add a landscape from another account.
    - **Default check** - at synth, `createDashboardStack()` reads the path and throws when
      `DASHBOARD.isDefault` is `true` and another landscape's parameter already has
      `isDefault: true`, naming that landscape. (Synth already needs AWS credentials for the
      Shibboleth construct's lookups.) To move the default: set `isDefault` to `false` on the old
      landscape and deploy it, then set `true` on the new one and deploy it. The context stays the
      only source of truth; nobody edits SSM by hand.
    - **Landscapes in other accounts** - SSM is per account/region, so such a landscape can't
      register itself here. Create an unmanaged parameter under the same path by hand, with an
      explicit `url`; the Lambda and the default check treat it like any other entry.
    - **Reading** - the Lambda gets `ssm:GetParametersByPath` on the path (read only) and
      `LANDSCAPES_PARAMETER_PATH` in its environment. A `LandscapeRegistry` reads and parses
      the entries (`label` defaulting to `landscape`; `url` to `https://<landscape>.<this
      dashboard's hosted zone>` when absent; an entry that doesn't parse is skipped with a warning)
      and caches them for the life of the Lambda instance, like `DashboardCache`. A new landscape
      therefore appears in other dashboards after their next cold start.
    - **Route** - `GET /api/landscapes` returns the entries sorted by landscape, with the current
      landscape (`LANDSCAPE`) flagged. Locally (`local` as the current landscape) it reads the path
      with the developer's credentials, or a fixed list in `DashboardMocks.ts` with
      `LOCALHOST_WITH_MOCKS=true`.
    - **Cleanup** - none needed: each stack removes its own parameter. Hand-made entries for other
      accounts are removed by hand.
    - **As built** - `dashboard/lib/LandscapeRegistration.ts` (`registerLandscape()`, called from
      `FunctionUrl.ts`; `assertSingleDefaultLandscape()`, called from `createDashboardStack()`,
      which reads SSM only when the landscape claims the default) and
      `dashboard/src/services/LandscapeRegistry.ts` (the path constant, `readLandscapeEntries()`
      shared with the synth check, and the `LandscapeRegistry` service). An entry's landscape is
      its value's `landscape`, else the last part of the parameter name; only a literal `true` is
      the default. The Lambda gets `HOSTED_ZONE` (from `DNS.hostedZone`) for the default url. The
      route returns `{ current, landscapes: [{ landscape, label, url?, isDefault, current }] }`. A
      failed SSM read is not cached (the next request retries). No context sets `LABEL` or
      `isDefault` yet: choosing the default landscape belongs with the redirect stack (Phase 5).
18. **Person lookup/sync** - unchanged logic, now running against the landscape's own config.
    `History` / `SystemStatus` stubs drop their unused `Config` constructor parameter (and their
    routes no longer load the config).

    **Gate:** `npx tsc --noEmit`; full `npx jest` (758 pass, including the new
    `test/DashboardRunRoutes.test.ts` and `test/LandscapeRegistry.test.ts`, and registry
    assertions in `test/DashboardStack.test.ts`). The handler bundled with esbuild (as
    `NodejsFunction` does) serves every new route from the mocks, and, pointed at the deployed
    mock landscape's tables (read only), returns its run, summary, chunk statistics and a `NEW`
    change page, and rejects `limit=501`; `/api/landscapes` returns an empty list there until a
    stack creates its registry parameter. `cdk synth hp-dashboard-mock` (`AWS_PROFILE=infnprd`)
    produces `/hp-dashboard/landscapes/mock` with the derived url, the read-only
    `ssm:GetParametersByPath` grant on the path, and no `Fn::ImportValue`. `npm run docker-build`
    succeeds (the image copies no `dashboard/` code).

### Phase 5 - DNS, redirect and migration (depends on Phase 3; auth decision from Phase 6)

19. **Redirect stack** - a small `dashboard-redirect` stack (deployed once, enabled by
    `DASHBOARD.isDefault` on the default landscape - the flag step 17 writes to the registry):
    CloudFront distribution with a CloudFront Function returning
    `302` to `https://<default>.cssnprd.warhen.work` + original path/query; imported
    certificate covering `huron1.cssnprd.warhen.work`. It creates the `huron1` A record the same
    way the construct does (CDK `ARecord` plus the `/route53/arecords/<record name>` ownership
    parameter naming this stack), only when no record for the name exists.
20. **Migration sequence**:
    1. Deploy `hp-dashboard-staging` at `staging.cssnprd.warhen.work` (the stack creates its A
       record); verify.
    2. Deploy the other landscapes' dashboards.
    3. Destroy the old standalone dashboard stack (`hp-dashboard-dev`). That deletes its `huron1` A record and
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

    Since `shibboleth-sp-at-edge` 2.0.0 (step 14c), the edge functions use each landscape's
    `DASHBOARD.SHIBBOLETH` (until then they ran with that package's own compiled-in context), so
    per-landscape SP keys / secrets are now possible.

## Relevant Files

**integration-huron-person-fargate**
- `src/dynamodb/StatisticsTable.ts`, `src/dynamodb/DynamoDBTable.ts`, `src/dynamodb/PersonHistoryTable.ts`
- `src/ApiErrorTracking.ts`, `src/processing/ProcessorCommon.ts`, `src/processing/ProcessorForS3.ts`, `src/processing/ProcessorForDynamoDb.ts`
- `src/post-processing/AbstractPostProcessor.ts`, `src/post-processing/PostProcessorForS3.ts`, `src/post-processing/PostProcessorForDynamoDB.ts`
- `context/IContext.ts`, `context/context.*.json`, `bin/App.ts`, `package.json`, `lib/services/post-processor/PostProcessorTaskDefinition.ts`
- New: `src/reporting/RunReporting.ts`, `dashboard/**` (incl. `dashboard/lib/DashboardStacks.ts`, `dashboard/lib/LandscapeResources.ts`), `test/DashboardStack.test.ts`, `LandscapeRegistry` (dashboard `src/services/`), redirect stack

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
   ownership parameter for the landscape's subdomain, the registry parameter with the derived
   `url`/`label`/`isDefault`, read-only `GetParametersByPath` on the registry path; synth throws on
   a second default); route tests for `/api/runs*`, `/api/capabilities`, `/api/landscapes`
   (validation, limit cap, mocks; registry defaults for a hand-made entry without `label`/`url`,
   unparseable entries skipped, current landscape flagged).
4. **Manual, mock landscape first** - deploy `hp-dashboard-mock` and confirm its A record and
   ownership parameter were created, run a
   source-simulator pipeline; `/api/runs` lists the run; `summary`, `chunks`, `errors`, `changes`
   return data; person lookup/sync reaches the target simulator. Repeat on staging; then the
   migration sequence and redirect check (`huron1...` -> `staging...`, path/query kept).

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
3. **Picker staleness** - resolved by the SSM registry (step 17): no landscape is redeployed
   for another to appear. The remaining lag is the per-instance cache, until each dashboard's
   next cold start.
4. **IdP registration of the new hostnames** - the SAML entityId is the wildcard
   `https://*.kualitest.research.bu.edu/shibboleth`, while the hostnames are under
   `cssnprd.warhen.work`; whether `<landscape>` sign-in works without registering each ACS
   URL is for Phase 6 to confirm before the first deploy.
5. **Context uploaded to S3** - the Shibboleth construct uploads the dashboard's context to a
   context-config bucket. That context no longer carries the integration config, only the
   `DASHBOARD` settings plus account, region and tags (the IdP certificate is public).
