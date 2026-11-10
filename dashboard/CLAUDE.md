# integration-huron-person-dashboard: Serverless Management Dashboard

## Project Purpose
A serverless dashboard for managing integration operations between BU CDM and Huron systems. Provides web-based UI for individual person sync, bulk operations, run history, and system status monitoring. Deployed via AWS CDK with Lambda function serving both as HTML dashboard server and data synchronization engine.

## Repository Relationship Model

The dashboard source lives in the `dashboard/` directory of the `integration-huron-person-fargate`
repository (moved from the retired `integration-huron-person-dashboard` repository). It keeps its
original internal layout (`lib/`, `src/`, `context/`, `templates/`, `public/`, `scripts/`) and
shares fargate's `package.json`,
`node_modules`, `tsconfig.json` and CDK app: fargate's `bin/App.ts` adds the landscape's dashboard
stack (`dashboard/lib/DashboardStacks.ts`) next to the pipeline stack. All paths below are relative
to `dashboard/`; npm scripts run from the fargate root.

## Shared Skills Repository

Cross-repository agent skills are maintained in a separate repository (https://github.com/whennemuth/integration-skills) that is cloned as the `.claude` directory of the workspace root, so skills live at `<workspace root>/.claude/skills/`. This is a native skill location that both VS Code (GitHub Copilot) and Claude Code discover without any custom settings.

- **Install**: from the workspace root, run `git clone https://github.com/whennemuth/integration-skills.git .claude`.
- **VS Code**: include the workspace root itself (`"path": "."`) as a root folder in your `.code-workspace` file. VS Code discovers `.claude/skills/` in every workspace root folder and makes those skills available across the whole workspace.
- **Claude Code**: start Claude Code in the workspace root. When starting in a package repository instead, add the workspace root with `--add-dir` (e.g. `claude --add-dir ..` from the repository root, or `/add-dir` in a running session).

See `<workspace root>/.claude/README.md` for full setup details.

Core-only and core+person+fargate workspace examples are documented in this repository's `README.md`.

## Workspace Instructions

Workspace-wide agent instructions live in `<workspace root>/.claude/CLAUDE.md`, versioned in the integration-skills repository alongside the shared skills. That file holds the workspace architecture overview, pointers to the shared-pattern skills and project CLAUDE.md files, change and commit preferences, and the task verification protocol (build, test, and completion reporting requirements for all code implementation tasks) that apply to all integration projects (core, huron-person, fargate, dashboard, file-drop, etc.).

**Discovery**: Claude Code and GitHub Copilot CLI load it automatically when started in the workspace root. VS Code Copilot Chat loads it when the workspace root (`"path": "."`) is a root folder of the `.code-workspace` file and the `chat.useClaudeMdFile` setting is enabled. When starting Claude Code in a package repository with `--add-dir` pointing at the workspace root, also set `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` so the workspace instructions are loaded.

## Implementation Verification Protocol

**CRITICAL**: When implementing code that depends on unfamiliar abstractions, control flow directives, or domain-specific patterns, you MUST verify their actual behavior before proceeding.

### High-Risk Abstractions Requiring Verification

- **Service provider patterns**: ServiceProvider registry, service lifecycle
- **Lambda handler abstractions**: Request routing, response formatting
- **Template service patterns**: Mustache rendering, inline asset injection
- **Origin verification**: Security header validation, request filtering
- **Mock service implementations**: DashboardMocks behavior, local vs production switching
- **CDK infrastructure patterns**: Stack dependencies, CloudFront configurations, Lambda Function URL setup
- **Authentication abstractions**: SAML/OIDC integration (when configured)

### Mandatory Verification Steps

Before implementing code that uses an unfamiliar abstraction:

1. **Search for definition**: Use `grep_search` to find where it's defined
2. **Find consumers**: Search for where it's processed/interpreted
3. **Read usage examples**: Look at tests and similar patterns
4. **State your understanding**: Explicitly describe what you think it does
5. **Think through interactions**: Consider edge cases and combinations
6. **Only then implement**: Proceed with verified understanding

### When You're Uncertain

If you cannot fully verify an abstraction's behavior:

- **State explicitly what you don't know**
- **Ask whether to search for implementation first**
- **Do NOT proceed on "educated guesses"**

### User Override

You can skip verification by saying:
- "Skip verification and proceed"
- "Use inference for this"

**See Also**: `verify-abstractions-before-implementation` skill in workspace skills repository

## Architecture

### Dual-Purpose Lambda Function

The Lambda function serves two primary roles:

**1. HTML Dashboard Server:**
- Serves Mustache-rendered HTML with inline CSS/JS assets
- Handles static dashboard routes
- Provides a responsive Bootstrap UI: a sidebar of grouped pages and a top bar

**2. Data Synchronization Engine:**
- Executes person sync operations (individual and bulk)
- Interfaces with BU CDM data sources and Huron data targets
- Processes API requests for person lookup, sync, run reporting, and status

### Infrastructure Components

**CloudFront Distribution:**
- Global CDN with edge caching
- Origin verification for secure Lambda access
- Blocks direct Lambda Function URL access

**Lambda Function URL:**
- Serverless backend with Express-style routing
- Built-in origin verification security
- Multiple route handlers for dashboard and API operations

**Origin Verification Security:**
- Cryptographically secure secret header
- Stored in AWS Systems Manager Parameter Store
- CloudFront injects secret in all origin requests
- Lambda validates and blocks unauthorized access (403)

**Template Build Pipeline:**
- Automated CSS/JS minification
- Inline asset embedding for optimal performance
- Mustache template generation

## Key Features

### Dashboard Operations
- **Individual Person Sync**: Lookup and sync single person records by BUID or HRN
- **Bulk Operations**: Large-scale batch synchronization jobs
- **Overview** (the landing page): the latest run and the service health checks
- **Run History**: The landscape's sync runs, newest first, and each run's figures, chunks, errors and changes
- **System Status**: Health monitoring of BU CDM and Huron services

### Security Features
- Origin verification prevents direct Lambda URL access
- Inline assets eliminate external resource requests
- Content Security Policy friendly architecture
- SAML/OIDC integration support (when configured)

### Development Features
- Express-based local development server
- Mock service implementations for standalone testing
- Hot reload with nodemon watch mode
- Template build automation

## API Endpoints

**Dashboard:**
- `GET /dashboard` - Main dashboard interface with inline assets

**API Operations:**
- `POST /api/person-lookup` - Look up individual person by BUID or HRN
- `POST /api/person-sync` - Sync individual person record
- `POST /api/bulk-sync` - Start bulk synchronization operation
- `GET /api/status` - Get system health status and metrics

**Run reporting** (this landscape's sync runs, read in-process from its tables and chunks bucket
through fargate's `src/reporting/RunReporting.ts`; GET only, no `landscape` parameter):
- `GET /api/runs` - `{ runs }`, the syncRunIds, oldest first
- `GET /api/runs?include=report&limit&before` - a page of runs with their reports, newest first:
  `{ items, before? }`. Up to `limit` runs (1-50, default 20) older than `before` (a syncRunId);
  the response's `before` continues the list and is present only while older runs remain. A run
  with nothing recorded to report on is listed as `{ syncRunId, status: 'UNKNOWN' }`. Built by
  `RunList` (`src/handlers/RunList.ts`): `listRunPage({ limit, before })` (one Query of the statistics
  table's `run-list-index`, or a scan when the table hasn't got it yet), then that page's reports in
  parallel. So Overview (`limit=1`) reads one run's key. A bad `limit` is `400` `INVALID_LIMIT`, a bad
  `before` or `include` is `400`.
- `GET /api/runs/{syncRunId}/summary` - the run's report (`404` if nothing is recorded)
- `GET /api/runs/{syncRunId}/chunks` - `{ items }`, per-chunk statistics
- `GET /api/runs/{syncRunId}/errors?cursor&limit&statusCode` - a page `{ items, cursor? }`
- `GET /api/runs/{syncRunId}/changes?changeType&cursor&limit` - a page of person changes
  (`NEW` | `UPDATED` | `DELETED`; DynamoDB storage mode only)
- `GET /api/capabilities` - landscape, storage mode, whether change lists are supported

`syncRunId` must be an ISO timestamp (URL-encoded or not); `limit` 1-500 (default 100). A
`ReportingError` maps to `400 { error, code }` (`INVALID_CURSOR`, `INVALID_LIMIT`,
`UNSUPPORTED_IN_STORAGE_MODE`), anything else to `500 { error, message }`.

**Landscape picker data:**
- `GET /api/landscapes` - `{ current, landscapes: [{ landscape, label, url?, isDefault, current }] }`
  from the landscape registry (see "Landscape registry" below). The dashboard page's own
  navigator doesn't call it (see "Page frame: sidebar and top bar" below)

## Key Patterns

### Service Provider Pattern
**Location**: `src/services/ServiceProvider.ts`

Centralized registry for managing service instances:
- PersonLookup service
- PersonSync service
- BulkSync service
- SystemStatus service
- RunReporting service (`RunReports.ts`: fargate's `RunReporting` built from the Lambda environment)
- LandscapeListing service (`LandscapeRegistry.ts`)
- Template service

The run reporting and landscape listing instances are kept for the life of the Lambda instance
(the registry caches what it reads from SSM).

**Usage:**
```typescript
const provider = new ServiceProvider(config);
const personSync = provider.getPersonSyncService();
```

### Mock Services for Local Development
**Location**: `src/handlers/DashboardMocks.ts`

Provides mock implementations when `LOCALHOST_WITH_MOCKS=true` (ignored in Lambda):
- Mock BU CDM data source responses
- Mock Huron data target operations
- Simulated sync execution
- Fake status data
- A fake landscape registry ("local", plus `mock` and `Staging` at example.com urls). This `mock`
  is unrelated to the deployed mock landscape.
- Run reporting (`src/handlers/MockRunReporting.ts`): a deterministic history of 40 runs ending
  with the nightly run of 2026-11-04. `MockRunCatalog` defines them: nightly delta syncs, a full
  sync every seventh night, and at fixed positions from the newest the special cases (an incomplete
  run, a terminal error, a failed and a post-processing-failed run, a dry run, a bulk reset, two
  single-person runs). The newest run has 260 errors across 400/404/429/500 and 130/240/12
  NEW/UPDATED/DELETED changes, enough for several pages. Chunk statistics, errors and changes agree
  with each run's summary. `MockRunReportingService` pages them with offset cursors (`OffsetPager`)
  under RunReporting's limits and `ReportingError` codes. `MOCK_STORAGE_MODE` (`dynamodb`, the
  default, or `s3`) sets the storage mode it reports; in S3 mode change lists are refused with
  `UNSUPPORTED_IN_STORAGE_MODE`. Tests: `test/DashboardMockRunReporting.test.ts`.

**Benefit**: Enables full local testing without AWS credentials or external service access. The
mocks ignore the integration config, so the person and bulk sync routes don't read it when they
are in use (`serviceConfiguration()` in `DashboardHandler.ts`): no config secret is needed.

### Template Service Pattern
**Location**: `src/services/TemplateService.ts`

Mustache-based HTML rendering with inline assets:
- CSS minification and embedding
- JavaScript minification and embedding
- Template compilation and caching
- Build-time asset processing

**Build Script**: `scripts/BuildTemplates.ts`

### Origin Verification Pattern
**Location**: `src/handlers/OriginVerification.ts`

Request validation security layer:
- Checks for `X-Origin-Verify` header
- Compares against stored secret from Parameter Store
- Returns 403 Forbidden if validation fails
- Logs security events for monitoring

**Bypassed**: When `NODE_ENV=development` for local testing

## Local Development

**Prerequisites:**
- Node.js 18+
- npm or yarn

**Setup** (from the fargate root):
```bash
npm install
npm run dashboard:build:templates  # Build Mustache templates with inline assets
npm run dashboard:dev              # Start the local server at http://localhost:3000/dashboard
npm run dashboard:dev -- --mocks   # The same with the mock services for one run (flags: see below)
```

`dashboard:dev` builds the templates, then runs `scripts/LocalLaunch.ts`, which starts
`LocalServer.ts` as a landscape's dashboard Lambda would run. Its settings are in
`scripts/LocalLaunch.env` (git-ignored; copy the committed `scripts/LocalLaunch.env.example`). The
repository's `.env` is never read.
- **The landscape**: `LANDSCAPE`, `REGION` and optional `STACK_ID` name the deployed dashboard Lambda to
  run as; `AWS_PROFILE` gives the credentials the server uses in place of the Lambda's role.
- **Without mocks** (`MOCKED=false`) the launcher looks up that Lambda's environment at every launch
  (`LambdaFunction`, see "Test Harnesses" below), leaves out its secrets (`ORIGIN_VERIFY_SECRET`), and runs
  the server with it: the server reads the landscape's tables, config secret and registry. A redeploy
  that changes the environment is picked up at the next launch. An expired AWS session, or a landscape
  without a deployed dashboard, stops the launch with a message. The message hints at the likely cause:
  `LANDSCAPE`/`REGION` when AWS answered that the function doesn't exist (`DashboardFunctionNotFoundError`),
  otherwise the AWS session.
- **Overrides**: any other variable in `LocalLaunch.env` wins over the Lambda's value (the launcher marks
  each one in what it prints). Without a deployed dashboard Lambda, the whole environment can be given there
  by hand.
- **Switches**: `MOCKED` (`LOCALHOST_WITH_MOCKS`; no lookup and no AWS access), `AUTHENTICATION` (`none`:
  public mode, the app login/logout headers removed; `mock`: signed in as `USER_PRINCIPAL`, see below),
  `WATCH` (nodemon restarts the server on a change under `dashboard/`; the environment is looked up once and
  handed to each restart through `LOCAL_LAUNCH_RESOLVED`), `MOCK_STORAGE_MODE` (`dynamodb` | `s3`, the
  storage mode the mock run reporting reports), `PORT` (default 3000).
- **Flags** override a switch for one run, after `--`: `--mocks`, `--no-mocks`, `--auth <none|mock>`,
  `--watch`, `--no-watch`, `--storage-mode <dynamodb|s3>` (mocks only), `--allow-production`.
- **Production guard**: without mocks the launcher refuses a landscape whose `LANDSCAPE_TONE` (the Lambda's,
  or an override) is `production`, since Sync and Bulk sync would write to its real target from local code,
  unless `ALLOW_PRODUCTION=true` or `--allow-production`. A local run also uses your profile's permissions,
  not the Lambda role's.
- **Mock sign-in** (`AUTHENTICATION=mock`, `MockSignIn` in `scripts/MockSignIn.ts`): stands in for the sign-in
  edge functions, so every request arrives signed in as `USER_PRINCIPAL` without an IdP. The Lambda's own
  sign-in settings are kept (`APP_AUTHORIZATION`, the login/logout header names, `AUTH_COOKIE_NAME`), so
  `Authenticator` runs as deployed. Before the handler sees a request, `MockSignIn` adds the `authenticated:
  true` header and presents the JWT cookie, made as the edge functions make it: `JwtTools` signs
  `{ sub, user: <claims> }` (RS256) with a key pair generated when the server starts (`publicKey` keeps the
  public half), the claims array-valued as the IdP sends them (`buPrincipal`, and `eduPersonPrincipalName`,
  `mail`, `givenName`, `sn` from `USER_EPPN`, `USER_MAIL`, `USER_GIVEN_NAME`, `USER_SURNAME`). The top bar shows
  the user. `/logout` is answered locally with a page saying the mock user can't sign out. Under mocks, with no
  Lambda to take them from, placeholder login/logout header names are set. The launcher passes
  `LOCAL_MOCK_SIGN_IN=true` in the environment, so each watch restart puts it back in front of the handler.
  `LocalServer.ts` takes it through `startLocalServer({ requestDecorator })` (`LocalRequestDecorator`:
  `decorate(event)` and an optional `respond(path)`), and imports nothing from `scripts/`. Tests:
  `test/DashboardMockSignIn.test.ts`.

`LocalLaunchOptions` (`scripts/LocalLaunchOptions.ts`) decides all of this from the file and the flags
without side effects; `LocalLaunch.ts` reads, looks up, prints the environment (secrets masked) and starts.
Tests: `test/DashboardLocalLaunch.test.ts`. The launcher is TypeScript, not a shell script, because npm
runs scripts in `cmd.exe` on Windows, where neither `VAR=value` prefixes nor `sh` work.

**Environment Variables** (set by the launcher, or in `LocalLaunch.env`):
- `NODE_ENV=development` - Enables local mode, bypasses origin verification
- `PORT=3000` - Local server port (default)
- `LOCALHOST_WITH_MOCKS=true` - Enables mock services
- `OVERRIDE_CACHE_WITH_LOCAL_CONFIG=true` - Loads the integration config with fargate's
  `getConfig()` (same sources as the fargate harnesses) instead of reading `INTEGRATION_SECRETS_ARN`

### Test Harnesses

Dashboard harnesses follow the fargate harness pattern (`TestEnvironment` from `integration-core`,
an explicit variable list in the `require.main === module` block, a group in `../.env` and
`../example-env.md`), with one rule of their own: **every dashboard harness prefix begins with
`DASHBOARD_`** (e.g. `DASHBOARD_SERVICE_PROVIDER`), so its variables can't be mistaken for a
pipeline harness's.

- `src/services/ServiceProvider.ts` (`DASHBOARD_SERVICE_PROVIDER`) - gets a service from the
  provider and calls it as `DashboardHandler` does, read only. `TASK`: `capabilities`, `runs`,
  `summary`, `chunks`, `errors`, `changes`, `landscapes`, `lookup`, `preview`,
  `status`. The landscape's Lambda environment (`LANDSCAPE`, `REGION`, table names, ...) is
  given with the prefix; `LOCALHOST_WITH_MOCKS=true` uses the mocks without loading any config.

```bash
DASHBOARD_SERVICE_PROVIDER_TASK=summary npx ts-node -r dotenv/config dashboard/src/services/ServiceProvider.ts
```

- `src/ShibbolethSecret.ts` (`DASHBOARD_SHIBBOLETH_SECRET`) - the active landscape's `SHIBBOLETH.secret`, through
  shibboleth-sp-at-edge's `SecretsManagerSecret`. `TASK`: `read` (which values exist; prints only the
  public ones), `create` (SAML key and certificate copied from `SOURCE_SECRET_ARN` under that secret's
  own field names, read from PEM files, or, with neither, generated with shibboleth-sp's `Keys` - the
  IdP must then be given the printed certificate before sign-in works; a new JWT key pair; refuses to
  replace an existing secret, since a new key pair ends every sign-in that shares it). The
  `ShibbolethSecret` class is not part of the Lambda: only the edge functions read this secret at
  runtime.
  - **Secret names**: a name ending with `/<landscape>` (e.g. `shibsp/hp-dashboard/staging`) is that
    landscape's own secret; any other (e.g. `shibsp/hp-dashboard`) is shared by the landscapes that
    share a sign-in (`secretLandscapeOf()`). `create` tags the secret from the context's `TAGS`, with
    `Landscape` only on a landscape's own secret, describes it accordingly, and creates it in the
    context's `REGION` (default `us-east-1`). An existing secret is read in the region its ARN names,
    as the edge functions read it.

```bash
DASHBOARD_SHIBBOLETH_SECRET_TASK=read npx ts-node -r dotenv/config dashboard/src/ShibbolethSecret.ts
```

- `scripts/LambdaFunction.ts` (`DASHBOARD_LAMBDA_FUNCTION`) - a landscape's deployed dashboard Lambda
  (`LambdaFunction`, found by `dashboardFunctionName()` in `lib/DashboardFunctionName.ts`, which
  `lib/FunctionUrl.ts` names the function with). Read only. `TASK`: `show` (its environment, with its
  secret variables masked: what the local launcher looks up). `LANDSCAPE`, `REGION` and `STACK_ID`
  default to the active context's. `LambdaFunction.SECRET_VARIABLES` (`ORIGIN_VERIFY_SECRET`, read only in
  Lambda) are what a local run leaves out (`withoutSecrets()`) and any display masks (`masked()`). Tests:
  `test/DashboardLambdaFunction.test.ts`. The launcher's own settings are in `scripts/LocalLaunch.env`
  (git-ignored), documented by the committed `scripts/LocalLaunch.env.example`.

```bash
DASHBOARD_LAMBDA_FUNCTION_TASK=show npx ts-node -r dotenv/config dashboard/scripts/LambdaFunction.ts
```

## CDK Deployment

One dashboard stack per landscape, `${DASHBOARD.STACK_ID}-${landscape}` (e.g. `hp-dashboard-mock`),
created by `createDashboardStack()` when the landscape's fargate context (`../context/context.json`)
has a `DASHBOARD` section. That section is this directory's `IContext` minus `ACCOUNT` / `REGION` /
`TAGS`, which come from the landscape (`toDashboardContext()`). Keep the stack name within 25
characters: the Shibboleth construct names a bucket `<stack name>-context-config-<account>-<region>`.

**Stacks:**
- Distribution (CloudFront) and Shibboleth Lambda@Edge (separate us-east-1 edge-lambda stack)
- A record `<landscape>.<dashboard domain>` (e.g. `staging.person.huron.cssnprd.warhen.work`) plus
  its `/route53/arecords/<name>` ownership parameter, created by the Shibboleth construct when no
  record for the name exists
- Default landscape only (`isDefault`): the dashboard domain's redirect (see "Dashboard domain and
  default landscape redirect" below)
- Lambda Function URL with origin verification (SSM `/cloudfront/<STACK_ID>/<landscape>/origin-verify-secret`)
- The landscape's registry parameter `/hp-dashboard/landscapes/<landscape>` (see below), and
  `ssm:GetParametersByPath` on that path for the Lambda
- No secret of its own: the Lambda reads the landscape's integration config secret
  (`integrationSecretName()` in fargate `lib/Secrets.ts`), and has read-only grants on the
  statistics table, the person history table (DynamoDB mode) and the chunks bucket
  (`lib/LandscapeResources.ts`). All by deterministic name - no CloudFormation imports from the
  pipeline stack.

**Commands** (from the fargate root; `npm run deploy` deploys the pipeline and dashboard stacks):
```bash
npm run deploy-dashboard    # Deploy the dashboard stack (and its edge-lambda stack); template in cdk.out/synth.output.dashboard.yaml
npm run dashboard:synth     # Generate CloudFormation template
npm run teardown-dashboard  # Destroy the dashboard and edge-lambda stacks (see "Teardown" below; can take hours)
npm run -s dashboard:prune-edge-versions [-- --dryrun]  # Delete edge function versions earlier deploys left behind
```

**Old Lambda@Edge versions:** a deploy that changes an edge function (its code, or a `SHIBBOLETH`
setting the construct bakes into the bundle's `edge-context.json`, such as `logoutUrl`) publishes a
new version, and logs `DELETE_FAILED (skipped)` for the version it replaced: Lambda refuses to
delete it until CloudFront has removed its replicas, hours later. This is expected; the deploy
succeeds. `bin/deploy.sh` then runs `scripts/PruneEdgeVersions.ts` after every deploy that includes
the dashboard stack. It calls shibboleth-sp-at-edge's (2.2.0+) `pruneEdgeLambdaVersions`, which
deletes versions older than the ones the distribution uses and never changes the distribution; a
version still replicated is left for the next deploy's run. It skips a dashboard without edge
functions (public mode), and never fails the deploy. Don't use the package's
`deleteEdgeLambdaVersions` for this: it is a teardown tool that removes the edge functions from the
distribution first.

**Teardown:** `teardown` / `teardown-dashboard` run `bin/teardown.sh`, which destroys the dashboard
in three steps, because Lambda won't delete an edge function while CloudFront still holds replicas
of it, and CloudFront removes them only hours after the distribution is deleted:
1. Destroy the dashboard stack, which deletes the distribution.
2. `scripts/DeleteEdgeVersions.ts` (`npm run -s dashboard:delete-edge-versions`) calls the package's
   `deleteEdgeLambdaVersions`, which deletes every version of the three edge functions, retrying
   each until its replicas are gone (up to about two hours per version).
3. Destroy the edge-lambda stack.

Steps 2 and 3 run only for an authenticated dashboard (the only kind with an edge-lambda stack).
Step 2 must never run while the distribution exists: `deleteEdgeLambdaVersions` first removes the
edge functions from the distribution, and the Lambda trusts every request that reaches it to have
been signed in by them (`Authenticator.isAuthenticated`), so a live dashboard would be open to
anyone. The script refuses if it finds the landscape's distribution, and the shell script runs it
only after step 1 succeeds. A failed step stops the teardown; rerunning it is safe, since
destroying a stack that's already gone does nothing.

**Context Configuration:**
- `../context/context*.json` `DASHBOARD` section - Infrastructure parameters
- `context/IContext.ts` - Dashboard context types (`DashboardSettings`), including the optional
  `LABEL` (picker label, default: the landscape), `isDefault` (the default landscape),
  `LANDSCAPE_TONE` (the top bar's tint, see "Landscape tone" below) and `sharedSignIn` (see
  "Shared sign-in" below)
- Setting up the hosted zone and certificate, and which context keys take them: the fargate
  `README.md`, "Dashboard DNS and Certificates"

### Landscape registry

The landscape picker's data is a registry of SSM parameters, one per landscape, under
`/hp-dashboard/landscapes` (`LANDSCAPES_PARAMETER_PATH` in `src/services/LandscapeRegistry.ts`).
- **Registration** (`lib/LandscapeRegistration.ts`, `registerLandscape()`): each dashboard stack
  owns `/hp-dashboard/landscapes/<landscape>`, a String parameter holding JSON
  `{ landscape, label, url, isDefault }` derived at synth from `ORIGIN.subdomain`, `LABEL` and
  `isDefault`. Deploying registers the landscape; tearing the stack down deregisters it. Never
  edit a stack-owned parameter: change the context and redeploy.
- **One default**: `assertSingleDefaultLandscape()` fails synth when `isDefault` is true and
  another landscape's parameter already has `isDefault: true`. To move the default, set
  `isDefault` false on the old landscape and deploy it, then set it on the new one.
- **Reading** (`LandscapeRegistry`): `GetParametersByPath` once per Lambda instance; a new
  landscape appears in other dashboards after their next cold start. Missing `label` defaults to
  the landscape, missing `url` to `https://<landscape>.<DASHBOARD_DOMAIN>`; unparseable entries are
  skipped with a warning.
- **Other accounts**: SSM is per account and region, so list a landscape deployed elsewhere by
  creating a parameter under the path by hand, with an explicit `url` (and remove it by hand).

### Dashboard domain and default landscape redirect

The **dashboard domain** (`person.huron.cssnprd.warhen.work`) is the parent of every landscape's
hostname, `ORIGIN.subdomain` = `<landscape>.<dashboard domain>`, inside the account's hosted zone
(`DNS.hostedZone`, `cssnprd.warhen.work`). It has no setting of its own: `lib/DashboardDomain.ts`
derives it from `ORIGIN.subdomain` (minus the first label), and `assertLandscapeHostname()` fails
synth unless that first label is the landscape. The Lambda gets it as `DASHBOARD_DOMAIN`.
- **Redirect** (`lib/DefaultLandscapeRedirect.ts`, added by `createDashboardStack()` when
  `isDefault`): a CloudFront distribution for the dashboard domain whose viewer-request CloudFront
  Function returns `302` to `https://<ORIGIN.subdomain>` with the path and query string (its origin
  is never reached, so no sign-in), plus the domain's alias A record and
  `/route53/arecords/<dashboard domain>` ownership parameter.
- **Synth checks** (`planDefaultLandscapeRedirect()`, AWS reads injectable as `RedirectLookups`):
  the certificate (`DNS.certificateARN`, us-east-1) must cover the dashboard domain, and the A
  record is created only when none exists or its ownership parameter names this stack, the rule
  the Shibboleth construct applies; otherwise synth warns and leaves the record alone.
- **Moving the default** removes the redirect from the old default's stack first (see "One
  default" above); the dashboard domain doesn't resolve until the new default deploys.

### Shared sign-in

`DASHBOARD.sharedSignIn` (authenticated mode only, default false) lets landscapes share one
sign-in. `createDashboardStack()` passes the context through `applySharedSignIn()`
(`lib/SharedSignIn.ts`), which sets `SHIBBOLETH.cookieDomain` to the dashboard domain and
`SHIBBOLETH.cookieName` to `SHARED_SIGN_IN_COOKIE_NAME` (`hp-dashboard-auth`), before both the
dashboard stack and the Shibboleth construct see it. shibboleth-sp-at-edge (2.1.0+) passes them to
the edge functions, which set, clear and read the JWT cookie with them.
- **The Lambda reads the same cookie**: `lib/FunctionUrl.ts` sets `AUTH_COOKIE_NAME` to
  `SHIBBOLETH.cookieName` (default `JwtTools.COOKIE_NAME`), and `Authenticator.user`
  (`src/services/Authentication.ts`) reads that cookie. Never read `JwtTools.COOKIE_NAME`
  directly: a `sharedSignIn` landscape's browser may also hold an old host-only cookie under it.
- **Without `sharedSignIn`** the cookie stays host-only under the default name, which must differ
  from the shared name: the browser sends the shared cookie to every landscape under the dashboard
  domain, and `parseCookieValue` takes whichever match comes first.
- **Same key pair**: every `sharedSignIn` landscape must use the same JWT key pair
  (`SHIBBOLETH.secret`). An isolated landscape needs its own pair as well as its own cookie.
- **Synth fails** for `sharedSignIn` without `ORIGIN.subdomain`, with a context that also sets
  `cookieName`/`cookieDomain`, or in public mode. Tests: `test/DashboardSharedSignIn.test.ts`.
- Setting up a landscape: the fargate `README.md`, "Dashboard DNS and Certificates", step 4.

### Page frame: sidebar and top bar

The dashboard page (`templates/dashboard.mustache`) is a left sidebar of grouped page links, and a
main column with a top bar above the pages. The login page has the same top bar and no sidebar. Colors, type and
shape come from the theme tokens at the top of `public/css/dashboard.css` (`--hp-*`), which
Bootstrap's own `--bs-*` variables point at. Change the look there, not per component.
- **Sidebar** (`PageNavigation`, `src/handlers/PageNavigation.ts`): the page groups and their
  items (id, label, Font Awesome icon). A group without a label is listed with no heading: the
  Overview, at the top, is the default page, so a bare URL opens it. `renderDashboard()` passes its `templateData(?tab)`: the
  `navigation` groups plus a `<page>Active` flag per page, which the page partials use. An unknown
  `?tab` falls back to the default page rather than showing none. Each item is a Bootstrap pill
  (`data-bs-toggle="pill"`) for the tab pane of the same id. At `lg` and up the sidebar is a static
  column. Below, it's an off-canvas menu (`offcanvas-lg`) opened from the top bar, which
  `dashboard.js` closes once a page is chosen. Tests: `test/DashboardPageNavigation.test.ts` and
  the sidebar render tests in `test/DashboardNavigation.test.ts`.
- **Page fragments** (`initializePageNavigation()` in `public/js/dashboard.js`): choosing a page
  sets the URL fragment `#<page>` (a history entry, so back and forward move between pages), and a
  fragment, which the server never sees, takes precedence over the server-rendered page on load.
  Only the fragment's first segment names the page (`#<page>/<detail>`), leaving room for detail
  views. A page shown by following the URL (load, back, forward) adds no history entry. Pages that
  fetch data when shown are loaded from `onPageShown()`. A page with details (`PAGES_WITH_DETAILS`,
  e.g. `#runs/<syncRunId>`) is called again when the fragment changes while it is shown, and must
  ignore a repeat call (a fragment change fires both `popstate` and `hashchange`). Choosing such a
  page in the sidebar while it shows a detail returns to the page itself. The panes have
  `scroll-margin-top`, so the browser's jump to the pane a fragment names stays clear of the sticky
  top bar.
- **Top bar**: built by `buildNavigationBar()` (`src/handlers/NavigationBar.ts`). Tests:
  `test/DashboardNavigation.test.ts`. Its parts are described below.
- **Landscape navigator**: a select over the landscape registry, rendered server-side from the
  `LandscapeRegistry`'s per-instance cache (no SSM read per page, and no `/api/landscapes` call
  from the page), with this landscape selected. Choosing one sends the browser to its `url`
  (`public/js/dashboard.js`). An entry without a `url` is listed but disabled. When the registry
  doesn't list this landscape (e.g. run locally), it's added, selected. If the registry can't be
  read or lists no landscape, a badge naming this landscape (`LANDSCAPE`, else `local`) takes the
  navigator's place.
- **Landscape tone** (`LandscapeTone`, `src/handlers/LandscapeTone.ts`): with no colored banner, the
  navigator (or badge) is tinted so it's obvious which landscape you're in: red for `production`,
  amber for `test`, gray for `mock`. The tone comes from the optional context setting
  `LANDSCAPE_TONE`, which `lib/FunctionUrl.ts` validates at synth (any other value fails it) and
  passes to the Lambda as the `LANDSCAPE_TONE` environment variable. When it's unset, a mock landscape
  (`isMockLandscape()`) is marked `mock` and any other landscape gets no tint.
  `buildNavigationBar()` resolves it at render time, ignoring an invalid value with a warning. A
  local run reads `LANDSCAPE_TONE` from the environment (or `.env`). The tone is also given as text
  for screen readers ("Production landscape"). Tests: `test/DashboardLandscapeTone.test.ts`,
  the navigation bar tests in `test/DashboardNavigation.test.ts`, and the synth test in
  `test/DashboardStack.test.ts`.
- **Sign-out**: a link to `SIGN_OUT_PATH` (`/logout`), shown unless the dashboard is public. The
  edge functions answer `/logout` themselves, so the Lambda never sees it: they expire the JWT
  cookie (with its Domain, so with `sharedSignIn` every landscape is signed out), then redirect to
  `/logout?target=idp`, which sends the browser to `SHIBBOLETH.logoutUrl` to end the IdP session.
  The browser ends on the IdP's page. The top bar also shows the JWT's `eduPersonPrincipalName`
  (else `buPrincipal`).

### Page patterns

Every page (`templates/partials/*-tab.mustache`) is built from the same parts, styled in the
"Page patterns" section of `public/css/dashboard.css`. Reuse them for new pages rather than adding
page-specific styles:
- **Page header**: `.page` > `header.page-header` with an `h1.page-title`, a one-line
  `.page-description`, and `.page-actions` (e.g. Refresh) on the right.
- **Sections**: `section.page-section`, with an optional `.section-heading` (`h2.section-title`)
  over a white `.surface`. Use `.surface-flush` when a table runs to its edges, and `.page-columns`
  for a main section beside a narrower one (stacked below `lg`). Cards are only for a bounded
  object shown as one unit (a person record, a mapping result), not for every section.
- **Stat tiles**: `.stat-grid` of `.stat-tile` (`.stat-label` over `.stat-value`).
- **Data tables**: `table.table.data-table` inside `.table-responsive`. Cells stay on one line and
  the table scrolls sideways on a narrow screen; mark prose cells `.wrap` and figures `.num`.
  Outcomes use `.status-badge` (`-success`, `-warning`, `-danger`), from `Html.statusBadge()`
  (or `Html.badge(label, tone)`). No fixed-height inner scroll boxes: pages scroll as a whole.
- **States** (`PageState`, `public/js/page-state.js`): the one pattern for a section with nothing to
  show: `loading`, `empty`, `error` and `redacted` (the viewer's role doesn't allow the content).
  `PageState.loading(target, title)` and friends replace a container's content;
  `PageState.element(kind, { title, message, compact })` builds one to place yourself. Text is set
  as text, so a message may carry server output. `dashboard.js`, `person-card.js` and
  `mapping-experiment-card.js` all render their states through it, so it loads before them. A
  section's data load goes through `fetchSectionData()` (`dashboard.js`), which throws an `Error`
  carrying the server's message and `status`. Tests: `test/DashboardPageState.test.ts` (in a vm
  with a stand-in DOM, since jest runs without one).
- **Markup** (`Html`, `public/js/page-state.js`): HTML built from template literals escapes every
  value with `Html.escape()`, and draws badges and tags with `Html`. `dashboard.js`'s `escapeHtml()`
  and `statusBadge()` delegate to it.
- **Refresh stamp** (`RefreshStamp`, `public/js/page-state.js`): a `.refresh-stamp` in
  `.page-actions`, beside Refresh, set to "Refreshed HH:MM" once a page's data loads. Pages refresh
  only when asked: there is no polling (an incomplete run may never finish).
- **Paged lists** (`PagedTable` and `CursorPager`, `public/js/run-history.js`): a data table filled a
  page at a time from a cursor-paged route, with a `.table-footer` saying how many rows are shown and
  offering the next page, or explaining a failed page and offering to try again. `.filter-chip`
  buttons (`aria-pressed`, with a count) over a list choose what it lists.

### Run history page

The `runs` page (`templates/partials/runs-tab.mustache`, `public/js/run-history.js`) shows the
landscape's runs from the run reporting routes. Its classes each do one thing:
- `RunsApi` calls the routes (`/api/runs?include=report`, `/api/runs/{id}/summary|chunks|errors|changes`,
  `/api/capabilities`) through `fetchSectionData()`, and `RunView` reduces a run's report to what the
  page shows: the summary's figures when the post-processor wrote one, else what the metadata, flags
  and terminal error say, with the report's `progress` (the chunk statistics summed so far) as the
  figures (`soFar`). An unfinished run's duration runs from its start to its last chunk
  (`lastChunkAt`), not to now. Deactivations are the processors' `removed` plus the deletion step's
  `deletedCount`.
- **An unfinished run's figures** (`soFar`) are marked wherever they show. In the list its Added,
  Updated, Deactivated, Errors and Duration are muted (`td.so-far`), with "so far" beside its status
  (Records come from the metadata and aren't marked). Its tiles (`.stat-tile-so-far`, on the run's
  page and Overview) are labeled "so far" and followed by `RunView.progressText()` ("6 of 15 chunks
  processed · last chunk finished 9 min ago"); an incomplete full sync's Deactivated tile notes that
  post-processing adds deletions. A single-person run is named by its BUID only when the report has metadata (the
  source URL's `buid`): a finished run's summary doesn't record it.
- `RunHistoryPage` shows the list (`#runs`, 20 runs a page with "Load older runs") or one run
  (`#runs/<syncRunId>`). The list keeps what it has loaded while a run is open; Refresh reloads it.
- `RunDetailView` is one run: the header (status, population, storage mode, duration, dry run, bulk
  reset), stat tiles, a note when the run failed, failed post-processing or has no summary yet (for
  a run with `progress`, saying its figures are summed from the chunks processed so far), and
  four tabs, each loaded when first opened: **Overview** (summary, deletion outcome, flags),
  **Chunks** (per-chunk statistics with totals), **Errors** (filtered by status code, with counts
  from the summary or else the chunk statistics) and **Changes** (by type; in S3 mode, where
  `changesSupported` is false, it says why there are none).
Tests: `test/DashboardRunHistory.test.ts` runs the script in a vm (as written and minified) and drives
`RunsApi` through the handler with the mock run history.

### Overview page

The `overview` page (`templates/partials/overview-tab.mustache`, `public/js/overview.js`) is the
landing page. `OverviewPage` loads, each time the page is shown and on Refresh, the latest run
(`/api/runs?include=report&limit=1`: its status, facts, how long ago it finished or started, the
run page's note and stat tiles, figures so far for a run without a summary, and a link to its page) and the service checks (`/api/status`, in
the `ServiceStatusTable` of `public/js/system-status.js`, which the Status page uses too). The two
load independently, so one failing (the real `SystemStatus` service isn't implemented yet) leaves
the other shown. Scripts load in dependency order: `page-state.js`, `run-history.js`,
`system-status.js`, `overview.js`, then `dashboard.js`, which creates the pages. Tests:
`test/DashboardOverview.test.ts`.

## Dependencies

- `integration-core`: Base utilities, delta processing, TestEnvironment
- `integration-huron-person`: Person sync logic, data mapping, source/target abstractions
- AWS services: Lambda, CloudFront, Systems Manager Parameter Store, IAM
- CDK libraries: cdk-lib, constructs
- Template engine: Mustache
- Local development: Express, Nodemon

## Key Patterns to Follow

### Adding a New Service
1. Create service class in `src/services/`
2. Define service interface in `ServiceTypes.ts`
3. Register in `ServiceProvider.ts`
4. Add mock implementation to `DashboardMocks.ts`
5. Create handler method in `DashboardHandler.ts`
6. Add route in Lambda handler

### Adding a New Dashboard Route
1. Add route handler in `src/handlers/DashboardHandler.ts`
2. Update `src/LocalServer.ts` for local development
3. Add Mustache template if needed (in `templates/`)
4. Run `npm run dashboard:build:templates` to regenerate template constants

### Updating Inline Assets
1. Modify CSS/JS in `public/` directory
2. Run `npm run dashboard:build:templates` to minify and embed
3. Generated constants appear in `src/handlers/TemplateConstants.ts`
4. Templates use `{{{cssAssets.dashboard}}}` and `{{{jsAssets.<name>}}}`. A new JS file must also be
   added to `TemplateService.getJsAssets()` (both branches) and given a `<script>` in
   `templates/dashboard.mustache`. A new partial must be added to `TemplateService.getPartials()` and
   to `partialFiles` in `scripts/BuildTemplates.ts`.
5. The Lambda serves the minified assets, the local server the files as written, so a local run can't
   catch a minifier break. The minifier is regex-based: it deletes everything after `//` on a line
   (including inside strings and regexes) and joins lines, so end every statement with a semicolon.
   `test/DashboardPageState.test.ts` checks that every script still parses once minified.

## Common Issues & Debugging

### Origin Verification Failures
1. Check `X-Origin-Verify` header is present in CloudFront custom headers
2. Verify secret in Parameter Store matches CloudFront configuration
3. Check Lambda function has permission to read Parameter Store
4. Review CloudWatch logs for validation errors

### Template Rendering Issues
1. Run `npm run dashboard:build:templates` after modifying templates or assets
2. Verify Mustache syntax (use triple braces `{{{ }}}` for HTML)
3. Check `TemplateConstants.ts` was regenerated
4. Review minification errors in build output

### Local Development Issues
1. Set `NODE_ENV=development` to bypass origin verification
2. Set `LOCALHOST_WITH_MOCKS=true` to use mock services
3. Check Express server logs for route errors
4. Verify port 3000 is available
