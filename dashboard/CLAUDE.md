# Dashboard: Serverless Management Dashboard

A landscape's web dashboard for the BU CDM → Huron person sync: person lookup and sync, Bulk sync (starting a run as
the schedule does), Run history, Overview, Status and Administration (access and audit). One Lambda (behind a
Function URL and CloudFront, with Shibboleth sign-in at the edge) serves both the Mustache-rendered pages, with inline
CSS/JS, and the `/api` routes.

**Where it lives**: the `dashboard/` directory of `integration-huron-person-fargate` (moved from the retired
`integration-huron-person-dashboard` repository), keeping its own layout (`lib/`, `src/`, `context/`, `templates/`,
`public/`, `scripts/`). It shares fargate's `package.json`, `node_modules`, `tsconfig.json` and CDK app: fargate's
`bin/App.ts` adds the landscape's dashboard stack (`lib/DashboardStacks.ts`) next to the pipeline stack. Paths below
are relative to `dashboard/`; npm scripts run from the fargate root. Workspace setup, the pipeline, and **keeping the
dashboard out of the task image** are in fargate's `CLAUDE.md`. Reference docs are in [docs/](docs/).

**Verify before you rely on an abstraction** (skill `verify-abstractions-before-implementation`; fargate's `CLAUDE.md`
has the steps). High-risk here: the `ServiceProvider` registry and service lifecycle, request routing and response
formatting in `DashboardHandler`, Mustache rendering and inline assets, origin verification, offline vs deployed
services, CloudFront / Function URL / edge-lambda stack wiring, and sign-in (SAML via Shibboleth, JWT claims).

## Architecture

- **CloudFront** in front of a **Lambda Function URL**; CloudFront injects a secret header that the Lambda checks
  (`src/handlers/OriginVerification.ts`, `X-Origin-Verify`, 403 otherwise; bypassed when `NODE_ENV=development`). The
  secret rotates at every dashboard deploy, so requests may briefly get 403 after one.
- **Sign-in**: shibboleth-sp-at-edge's Lambda@Edge functions (a separate us-east-1 edge-lambda stack) sign the user in
  and set a JWT cookie; the Lambda believes its claims only after verifying them against `JWT_PUBLIC_KEY`. A dashboard
  can also be public (no sign-in).
- **Handler**: `src/handlers/DashboardHandler.ts` (gate, then routes) over `ServiceProvider`
  (`src/services/ServiceProvider.ts`): PersonLookup, PersonSync, BulkSync, SystemStatus, RunReporting (`RunReports.ts`:
  fargate's `src/reporting/RunReporting.ts` built from the Lambda environment), LandscapeListing
  (`LandscapeRegistry.ts`), Template, the role catalog and stores, the audit store and the launch flag. Run reporting
  and the landscape listing live for the life of the Lambda instance (the registry caches what it reads from SSM).
- **Templates**: `src/services/TemplateService.ts` renders `templates/` (compiled and cached) with minified, inlined
  CSS/JS, generated into `src/handlers/TemplateConstants.ts` by `scripts/BuildTemplates.ts`
  (`npm run dashboard:build:templates`). Inline assets mean the page makes no external resource requests of its own,
  which keeps it Content-Security-Policy friendly.
- **Offline services** (`src/handlers/OfflineServices.ts`, `LOCAL_OFFLINE=true`, ignored in Lambda): stand-ins
  answering from built-in data with no AWS access, including a 40-run offline Run history
  (`src/handlers/OfflineRunReporting.ts`). They ignore the integration config, so no config secret is needed.
- **"Offline" and "mock" are kept apart**: *offline* is a local run with stand-in services; *mock* means only a mock
  landscape (`isMockLandscape()`), a deployed pipeline whose target is the target simulator. An offline run can play a
  mock landscape (`--landscape mock`).

## Routes

Every route, with request and response shapes and refusal codes: [docs/API.md](docs/API.md). In brief:

| Route | Action | What |
|---|---|---|
| `GET /dashboard` | sign-in | the page |
| `GET /api/me` | sign-in | who is asking and what they may do |
| `POST /api/person-lookup` | `person:lookup` | one person, by BUID or HRN |
| `POST /api/person-sync/preview`, `/preview-experiment` | `person:preview` | what a sync would send |
| `POST /api/person-sync` | `person:sync` | sync one person |
| `GET /api/bulk-sync` | `runs:read` | what starting a run would do, and whether it may start now |
| `POST /api/bulk-sync/plan`, `POST /api/bulk-sync` | `bulk:start` | review a run; start one |
| `GET` / `POST /api/mock-landscape/reset` | `runs:read` / `mock:reset` | a mock landscape's last reset; start one |
| `GET /api/runs[?include=report&limit&before]`, `/api/runs/{id}/summary\|chunks\|errors\|changes`, `/api/capabilities` | `runs:read` | run reporting |
| `GET /api/landscapes`, `GET /api/status` | `runs:read` | the landscape registry; health checks |
| `GET /api/audit` | `audit:read` | the audit trail |
| `/api/access/roles`, `/api/access/assignments[/{person}]` | `access:admin` | roles and assignments |
| `POST` / `DELETE /api/impersonation` | own `access:impersonate` / sign-in | start or stop impersonating |

`RoutePermissions` (`src/authorization/RoutePermissions.ts`) is the authority for which action each route needs.
Errors are `{ error, code }`: `ReportingError` → 400 (`RUN_LIST_UNAVAILABLE` → 503), `BulkSyncError` →
400/403/409/503, anything else `500 { error, message }`.

## Local Development

```bash
npm install
npm run dashboard:build:templates    # Build Mustache templates with inline assets
npm run dashboard:dev                # Local server at http://localhost:3000/dashboard, as a landscape's Lambda
npm run dashboard:dev -- --offline   # The same with the offline services (no AWS access), for one run
npm run dashboard:dev:offline:mock   # Offline, playing the mock landscape (--offline --landscape mock)
```

- `scripts/LocalLaunch.ts` starts `LocalServer.ts` with settings from `scripts/LocalLaunch.env` (git-ignored; copy
  `scripts/LocalLaunch.env.example`); the repository's `.env` is never read. Online (`OFFLINE=false`) it looks up the
  deployed Lambda's environment (`LANDSCAPE`, `REGION`, `STACK_ID`) at every launch, leaving out its secrets, and runs
  with `AWS_PROFILE`'s credentials. Any other variable in `LocalLaunch.env` overrides the Lambda's.
- **Switches**: `OFFLINE`, `AUTHENTICATION` (`none` | `local`), `WATCH`, `OFFLINE_STORAGE_MODE`, `PORT`. **Flags**
  after `--`: `--offline`, `--online`, `--auth <none|local>`, `--watch`, `--no-watch`, `--storage-mode`,
  `--landscape`, `--allow-production`, `--roles <Role,...>`. Pre-2026-11-15 names (`MOCKED`, `--mocks`,
  `AUTHENTICATION=mock`, ...) still work with a "Renamed:" warning.
- **Production guard**: online, the launcher refuses a landscape whose `LANDSCAPE_TONE` is `production` (local code
  would write to its real target) unless `ALLOW_PRODUCTION=true` or `--allow-production`.
- **Local sign-in** (`AUTHENTICATION=local`, `scripts/LocalSignIn.ts`): every request arrives signed in as
  `USER_PRINCIPAL`, with a JWT made and verified as the edge functions' would be. Offline, the local user's roles come
  from `USER_ROLES` or `--roles` (`none` for none); online, from the landscape's access table.
- Don't set `NODE_ENV` or `LOCAL_OFFLINE` by hand; the launcher sets them. `LocalLaunchOptions`
  (`scripts/LocalLaunchOptions.ts`) decides everything without side effects; tests `test/DashboardLocalLaunch.test.ts`.
  The launcher is TypeScript because npm runs scripts in `cmd.exe` on Windows.

The launcher in full, the local sign-in's mechanics and the offline services' data:
[docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md).

## Test Harnesses

Dashboard harnesses follow the fargate harness pattern (`TestEnvironment` from `integration-core`, an explicit variable
list in the `require.main === module` block, a group in `../.env` and `../example-env.md`), with one rule of their
own: **every dashboard harness prefix begins with `DASHBOARD_`**. Run with
`<PREFIX>_TASK=<task> npx ts-node -r dotenv/config dashboard/<file>`.

| File | Prefix | `TASK` |
|---|---|---|
| `src/services/ServiceProvider.ts` | `DASHBOARD_SERVICE_PROVIDER` | `capabilities`, `runs`, `summary`, `chunks`, `errors`, `changes`, `landscapes`, `lookup`, `preview`, `status` (read only; the Lambda environment given with the prefix; `LOCAL_OFFLINE=true` for the offline services) |
| `src/ShibbolethSecret.ts` | `DASHBOARD_SHIBBOLETH_SECRET` | `read`, `create` (refuses to replace an existing secret; [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md#the-shibboleth-secret)) |
| `src/services/LaunchFlag.ts` | `DASHBOARD_LAUNCH_FLAG` | `read`, `assess` (`NEWEST_SYNC_RUN_ID`), `raise` (`LIFETIME_MINUTES`), `lower` |
| `scripts/LambdaFunction.ts` | `DASHBOARD_LAMBDA_FUNCTION` | `show` (the deployed Lambda's environment, secrets masked: what the launcher looks up; `LambdaFunction.SECRET_VARIABLES`) |
| `src/authorization/RoleAssignmentStore.ts` | `DASHBOARD_ROLE_ASSIGNMENTS` | `list`, `get`, `set` (`PRINCIPAL`, `ROLES`), `grant`, `revoke` |
| `src/audit/AuditStore.ts` | `DASHBOARD_AUDIT_TRAIL` | `list` (`PRINCIPAL`, `ACTION`, `LIMIT`, `CURSOR`) |

Their tests: `test/DashboardLambdaFunction.test.ts`, `DashboardLaunchFlag`, `DashboardRoleAssignments`,
`DashboardAuditStore`. `LambdaFunction` finds the Lambda by `dashboardFunctionName()` (`lib/DashboardFunctionName.ts`,
which `lib/FunctionUrl.ts` names the function with); `LANDSCAPE`, `REGION` and `STACK_ID` default to the active
context's, and `withoutSecrets()` / `masked()` drop or mask `LambdaFunction.SECRET_VARIABLES` (`ORIGIN_VERIFY_SECRET`,
read only in Lambda).

## Authorization

Who may do what, by role, per landscape. The model, stores, audit, Administration pages and impersonation in full:
[docs/AUTHORIZATION.md](docs/AUTHORIZATION.md).

- **Deny by default** (`RequestGate` in `DashboardHandler`, before routing): pages need only sign-in, `/api/me` only
  sign-in, every other `/api/*` route its action from `RoutePermissions`; an unmapped `/api` route is `403`, a known
  route's unsupported method `405`. A new `/api` route must be added to `RoutePermissions` (and to
  `test/DashboardAccessGate.test.ts`). Public mode allows everything but impersonation.
- **Identity** comes only from verified sign-in claims (`ClaimsReader` / `VerifiedClaimsReader` against
  `JWT_PUBLIC_KEY`); never from shibboleth-sp's `IMPERSONATE_*` variables, and never from the request body
  (`startedBy` included).
- **Roles** (`src/authorization/`, `StaticRoleCatalog`, least to most): RunViewer, PersonViewer, Viewer,
  PersonOperator, PipelineOperator, Operator, Admin. Held in the access table (`RoleAssignmentStore`); covered roles
  are pruned on save (`pruneCoveredRoles()`). No change may leave a landscape without an Admin (`409 LAST_ADMIN`). A
  bulk grant only gives roles to people who hold none.
- **The first Admin** is seeded by hand after each deploy that creates the tables (the `DASHBOARD_ROLE_ASSIGNMENTS`
  harness or `aws dynamodb put-item`; fargate `README.md`, "Dashboard access"). A teardown deletes the tables.
- **Audit** (`src/audit/`): every denial, and every allowed change and person read, is recorded and never deleted;
  `AuditTrail` never fails a request.
- **Impersonation** is viewing only: an Admin's `hp-dashboard-impersonate` cookie is re-checked on every request, and
  any request that changes something is refused `403 IMPERSONATING`.
- **A refused load is redacted**: a section's failed load goes through `PageState.failed()` / `failedElement()`,
  which shows a `403` as the redacted state.

## CDK Deployment

One stack per landscape, `${DASHBOARD.STACK_ID}-${landscape}` (e.g. `hp-dashboard-mock`), from the `DASHBOARD` section
of the landscape's fargate context (`../context/context.json`; types in `context/IContext.ts`, `DashboardSettings`).
It takes nothing from the pipeline stack's outputs: it reaches the landscape's config secret, tables, chunks bucket,
chunker subscriber and services by deterministic name (`lib/LandscapeResources.ts`), with read-only grants except its
own access and audit tables (`lib/AccessTables.ts`) and its launch flag. Resources, grants and the reasoning:
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md). DNS, certificates and context for a new landscape: fargate `README.md`,
"Dashboard DNS and Certificates".

```bash
npm run deploy-dashboard    # Dashboard and edge-lambda stacks; template in cdk.out/synth.output.dashboard.yaml
npm run dashboard:synth     # Generate CloudFormation template
npm run teardown-dashboard  # Destroy the dashboard and edge-lambda stacks (can take hours; safe to rerun)
npm run -s dashboard:prune-edge-versions [-- --dryrun]  # Delete edge function versions earlier deploys left behind
```

Rules:
- **Keep the stack name within 25 characters** (the Shibboleth construct names a bucket after it).
- **`DELETE_FAILED (skipped)` for an old edge function version is expected** after a deploy that changes an edge
  function; `bin/deploy.sh` prunes old versions afterwards with `pruneEdgeLambdaVersions`. Never use the package's
  `deleteEdgeLambdaVersions` to prune: it removes the edge functions from the distribution first.
- **Never delete edge versions while the distribution exists** (teardown's step 2,
  `scripts/DeleteEdgeVersions.ts`): without the edge functions, the Lambda would trust every request as signed in.
  `bin/teardown.sh` destroys the dashboard stack, then the edge versions, then the edge-lambda stack.
- **Landscape registry** (`/hp-dashboard/landscapes/<landscape>`, owned by each stack): never edit a stack-owned
  parameter, change the context and redeploy. Only one landscape may be `isDefault` (synth fails otherwise); to move
  the default, unset it on the old landscape and deploy, then set it on the new one. A landscape in another account is
  listed by a parameter created by hand.
- **Hostnames**: `ORIGIN.subdomain` must be `<landscape>.<dashboard domain>` (synth checks it); the default
  landscape's stack also redirects the dashboard domain to it.
- **Shared sign-in** (`sharedSignIn`): every sharing landscape uses the same JWT key pair; the Lambda reads the cookie
  named by `AUTH_COOKIE_NAME`, **never `JwtTools.COOKIE_NAME` directly**; an isolated landscape's cookie name must
  differ from the shared one.

## Pages

The page frame (sidebar, `#<page>` fragments, top bar, landscape navigator and tone, sign-out), the page patterns, Run
history and Overview: [docs/PAGES.md](docs/PAGES.md). Bulk sync and the launch flag:
[docs/BULK_SYNC.md](docs/BULK_SYNC.md).

- **Reuse the page patterns** (`templates/partials/*-tab.mustache`, the "Page patterns" section of
  `public/css/dashboard.css`) rather than adding page-specific styles: `.page` / `header.page-header`,
  `section.page-section` over a `.surface`, `.stat-grid`, `table.data-table` in `.table-responsive`, `.status-badge`,
  `PageState` for loading / empty / error / redacted, `Html.escape()` for every value in template-literal HTML,
  `RefreshStamp`, `PagedTable` for cursor-paged lists. Change the look in the `--hp-*` theme tokens, not per component.
- **Pages never poll**: they load when shown and on Refresh (an incomplete run may never finish).
- **Data loads** go through `fetchSectionData()` / `postSectionData()` (`public/js/dashboard.js`), which carry the
  server's message, `status` and `code`.
- **A page with details** (`PAGES_WITH_DETAILS`, e.g. `#runs/<syncRunId>`) must ignore a repeat call when its fragment
  changes (`popstate` and `hashchange` both fire).
- **Run history** (`public/js/run-history.js`): an unfinished run shows its phase (Fetching, Processing,
  Post-processing, Stalled), figures marked "so far", and percent done counted in **chunks** (`chunksReported` ÷
  `chunkCount`), never in records, which count only changed people.
- **Bulk sync** (`src/services/BulkSync.ts`, `public/js/bulk-sync.js`): what is started is what was reviewed (Review
  run locks the fields). Bulk reset always sends both previous-state flags. The run's `startedBy` comes from the
  signed-in user. A started run is guarded by the launch flag (`/hp-dashboard/launch-flags/<landscape>`) until it is
  listed. A mock landscape adds Source (real or simulator) and Start from scratch.
- **Overview** (`public/js/overview.js`): the latest run and the service checks, loaded independently; in a mock
  landscape, its last reset and Reset landscape.

## Adding Things

**A service**: create the class in `src/services/`, define its interface in `ServiceTypes.ts`, register it in
`ServiceProvider.ts`, add an offline implementation to `OfflineServices.ts`, add a handler method in
`DashboardHandler.ts`, and route it (with its action in `RoutePermissions`).

**A route**: add the handler in `src/handlers/DashboardHandler.ts`, update `src/LocalServer.ts` for local development,
add a Mustache template if needed (`templates/`), and run `npm run dashboard:build:templates`.

**Inline assets**:
1. Modify CSS/JS in `public/`, then run `npm run dashboard:build:templates` to minify and embed them into
   `src/handlers/TemplateConstants.ts`.
2. Templates use `{{{cssAssets.dashboard}}}` and `{{{jsAssets.<name>}}}`. A new JS file must also be added to
   `TemplateService.getJsAssets()` (both branches) and given a `<script>` in `templates/dashboard.mustache`, in
   dependency order (`page-state.js`, `run-history.js`, `system-status.js`, `overview.js`, `bulk-sync.js`,
   `administration.js`, then `dashboard.js`). A new partial must be added to `TemplateService.getPartials()` and to
   `partialFiles` in `scripts/BuildTemplates.ts`.
3. **The minifier is regex-based**: it deletes everything after `//` on a line (including inside strings and
   regexes) and joins lines, so end every statement with a semicolon and never write `//` in a string or regex. The
   Lambda serves the minified assets and the local server the files as written, so a local run can't catch a minifier
   break; `test/DashboardPageState.test.ts` checks that every script still parses once minified.

## Dependencies

`integration-core` (utilities, TestEnvironment) and `integration-huron-person` (person sync, mapping, source and target);
AWS Lambda, CloudFront, SSM Parameter Store, DynamoDB, IAM; CDK; Mustache; Express and nodemon locally. Dashboard-only
packages are fargate `optionalDependencies` (`npm install --save-optional`).

## Common Issues

- **Origin verification failures (403)**: check the `X-Origin-Verify` header is in CloudFront's custom headers, the
  Parameter Store secret matches CloudFront's, the Lambda can read Parameter Store, and CloudWatch logs; right after a
  deploy, wait for CloudFront to roll the new header out.
- **Template rendering**: run `npm run dashboard:build:templates` after changing templates or assets, use triple braces
  `{{{ }}}` for HTML, check `TemplateConstants.ts` was regenerated, and read the build output for minification errors.
- **Local runs**:
  1. No AWS access, or none wanted: `OFFLINE=true` in `scripts/LocalLaunch.env`, or `-- --offline` for one run.
  2. The launch stops before the server starts: an expired session for `AWS_PROFILE`, or no dashboard Lambda by that
     name (check `LANDSCAPE`, `REGION`, `STACK_ID`); the message says which.
  3. The port is already in use: stopping `npm run dashboard:dev` (Ctrl+C, or closing its terminal) can leave its
     `node` process listening on Windows. Stop the process listening on `PORT` (default 3000), or set another `PORT`.
  4. PowerShell refuses `npm` (execution policy `Restricted` blocks `npm.ps1`): run `npm.cmd run dashboard:dev`, or use
     `cmd.exe` or Git Bash.
  5. Works locally, fails deployed (or the reverse): a local run uses your profile's AWS permissions, not the Lambda
     role's, so a missing grant only shows when deployed.
  6. The top bar names the user differently: deployed it shows the IdP's `eduPersonPrincipalName`; the local sign-in
     shows `USER_PRINCIPAL` unless `USER_EPPN` is set.
  7. Route errors are in the server's own output.
