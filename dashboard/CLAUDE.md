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
- **Offline service implementations**: OfflineServices behavior, local vs production switching
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
- Cryptographically secure secret header: `cf-origin-verify-` plus 32 bytes from `crypto.randomBytes`,
  base64url encoded (`lib/FunctionUrl.ts`). A new value is made at every synth, so each dashboard deploy
  rotates it in the SSM parameter, the Lambda's `ORIGIN_VERIFY_SECRET` and CloudFront's header together (the
  deploy's diff shows all three changing). Until CloudFront has rolled the new header out, requests may briefly
  get 403.
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
- **Bulk sync**: start a sync run in the landscape, as its schedule does (full or delta, bulk reset, or a one-person
  test), after a review of what will run and the event the chunker will be sent
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
- Offline service implementations for standalone testing (no AWS access)
- Hot reload with nodemon watch mode
- Template build automation

## API Endpoints

**Dashboard:**
- `GET /dashboard` - Main dashboard interface with inline assets
- `GET /api/me` - who the request is taken to be and what they may do here (see "Authorization"); needs only sign-in.
- `GET /api/audit` - the landscape's audit trail, a page at a time (see "Authorization"; `audit:read`)
- `GET /api/access/roles`, `GET|POST /api/access/assignments`, `GET|PUT|DELETE /api/access/assignments/{person}` -
  who holds which roles, and giving roles to several people at once (see "Authorization"; `access:admin`)
- `POST|DELETE /api/impersonation` - start or stop seeing the dashboard through another person's roles (see
  "Authorization"; starting needs `access:impersonate`)
  Every other `/api` route needs an action of the signed-in user's roles, and answers `403` without it

**API Operations:**
- `POST /api/person-lookup` - Look up individual person by BUID or HRN
- `POST /api/person-sync` - Sync individual person record
- `GET /api/bulk-sync` - what starting a run would do and whether it's allowed now (`BulkSyncState`: the form's
  defaults, the latest run, `launch` while a run launched moments ago isn't listed yet (see "Launch flag" below), and
  `blocked` with the refusal a start would get, `NOT_CONFIGURED` included, so the page shows even where starting
  isn't configured)
- `POST /api/bulk-sync/plan` - review a run: the same checks as a start, then what it would send (`BulkSyncPlan`:
  the choices, the `ApiChunkerEvent`, and every setting of the run, named as the manual Runner's `RunnerEnv`, each
  with its source: `form`, `landscape`, `fixed` or `unused`; the page shows the choices and the event, not the
  settings); `200`, nothing is started
- `POST /api/bulk-sync` - start a sync run, as the EventBridge schedule does (`BulkSync`,
  `src/services/BulkSync.ts`): `{ runType: 'full' | 'full-no-deactivations' | 'test', buid? (a test's,
  and only a test's), bulkReset?, iterationLimit? (0-50, 0 a single task; default 10; always 0 for a test), parallelStart? (0 to the chunker service's most tasks, at most 20; refused for a test or a single task),
  processorScalingCapacityOverride? (1-20, the processor service's maximum for this run; refused for a test), overrideStalled?, and in a mock landscape only: source? ('real' | 'simulator'), simulator? ({ totalPopulation?,
  simulatedDelaySeconds?, errorRate?, nonCurrentTermRate? }, with the simulator), resetState? }`; `202` with what was
  started. A refused review
  or start is a `BulkSyncError` as `{ error, code }` (400 invalid, 403 production, 409 a run underway or stalled, a
  run launched moments ago and not yet listed (`RUN_LAUNCHING`), a mock landscape's reset running (`RESET_UNDERWAY`)
  or a subscriber in `DRY_RUN`, 503 not configured)
- `GET /api/mock-landscape/reset` - a mock landscape's last reset of its run state and whether one can be started now
  (`MockLandscapeResetState`; `{ mockLandscape: false }` in any other landscape). `POST` starts one: the chunker
  subscriber, invoked asynchronously with a reset-only event, empties the target simulator, Run history and the
  fingerprints; `202` once it has accepted it. Refusals as Bulk sync's (`BulkSyncError`): 400 not a mock landscape,
  409 a run underway (`RUN_UNDERWAY`) or launching (`RUN_LAUNCHING`), a reset still running (`RESET_UNDERWAY`) or the
  subscriber in `DRY_RUN`, 503 not configured
- `GET /api/status` - Get system health status and metrics

**Run reporting** (this landscape's sync runs, read in-process from its tables and chunks bucket
through fargate's `src/reporting/RunReporting.ts`; GET only, no `landscape` parameter):
- `GET /api/runs` - `{ runs }`, the syncRunIds, oldest first
- `GET /api/runs?include=report&limit&before` - a page of runs with their reports, newest first:
  `{ items, before?, launch? }` (`launch` on the first page only, while a run launched moments ago isn't listed yet:
  "Launch flag" below). Up to `limit` runs (1-50, default 20) older than `before` (a syncRunId);
  the response's `before` continues the list and is present only while older runs remain. A run
  with nothing recorded to report on is listed as `{ syncRunId, status: 'UNKNOWN' }`. Built by
  `RunList` (`src/handlers/RunList.ts`): `listRunPage({ limit, before })` (one Query of the statistics
  table's `run-list-index`), then that page's reports in parallel. So Overview (`limit=1`) reads one run's
  key. A bad `limit` is `400` `INVALID_LIMIT`, a bad `before` or `include` is `400`. A table that can't serve
  `run-list-index` yet (its pipeline stack not deployed since the index was added, or the index still being
  built) is `503` `RUN_LIST_UNAVAILABLE`, on this route and `GET /api/runs`, with a message saying what to do,
  which the run list's error state shows.
- `GET /api/runs/{syncRunId}/summary` - the run's report (`404` if nothing is recorded)
- `GET /api/runs/{syncRunId}/chunks` - `{ items }`, per-chunk statistics
- `GET /api/runs/{syncRunId}/errors?cursor&limit&statusCode` - a page `{ items, cursor? }`
- `GET /api/runs/{syncRunId}/changes?changeType&cursor&limit` - a page of person changes
  (`NEW` | `UPDATED` | `DELETED`; DynamoDB storage mode only)
- `GET /api/capabilities` - landscape, storage mode, whether change lists are supported

`syncRunId` must be an ISO timestamp (URL-encoded or not); `limit` 1-500 (default 100). A
`ReportingError` maps to `400 { error, code }` (`INVALID_CURSOR`, `INVALID_LIMIT`,
`UNSUPPORTED_IN_STORAGE_MODE`), except `RUN_LIST_UNAVAILABLE` (the landscape's state, not the request's
fault), which maps to `503 { error, code }`; anything else to `500 { error, message }`.

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

### Offline Services for Local Development
**Location**: `src/handlers/OfflineServices.ts`

**"Offline" and "mock" are kept apart** (renamed 2026-11-15): *offline* is a local run whose dashboard services are
stand-ins answering from built-in data, with no AWS access; *mock* means only a mock landscape (`isMockLandscape()`),
a deployed pipeline whose target is the target simulator. The local stand-in for the sign-in edge functions is the
*local sign-in*. An offline run can play a mock landscape (`--landscape mock`).

Provides the offline implementations when `LOCAL_OFFLINE=true` (ignored in Lambda):
- Sample BU CDM data source responses
- Sample Huron data target operations
- Simulated sync execution
- Sample status data
- A sample landscape registry (`mock` and `Staging` at example.com urls), whose current landscape is the one the
  offline services play: `LANDSCAPE`, which the launcher sets to `local`, or to `--landscape`'s
  (`npm run dashboard:dev:offline:mock` plays `mock`, so the top bar shows mock selected, with the mock tint). The
  entries' urls lead nowhere: a local run plays one landscape, and switching means restarting with the other command.
- **The landscape the offline services play** (`LANDSCAPE`, else `local`) is the one Bulk sync
  (`OfflineBulkSyncService`) and run reporting's capabilities report. A mock landscape's name (`mock`) shows Bulk
  sync's mock landscape options.
- Run reporting (`src/handlers/OfflineRunReporting.ts`): a deterministic history of 40 runs ending
  with the nightly run of 2026-11-04. `OfflineRunCatalog` defines them: nightly delta syncs, a full
  sync every seventh night, and at fixed positions from the newest the special cases (an incomplete
  run, a terminal error, a failed and a post-processing-failed run, a dry run, a bulk reset, two
  single-person runs). The newest run has 260 errors across 400/404/429/500 and 130/240/12
  NEW/UPDATED/DELETED changes, enough for several pages. Chunk statistics, errors and changes agree
  with each run's summary. `OfflineRunReportingService` pages them with offset cursors (`OffsetPager`)
  under RunReporting's limits and `ReportingError` codes. `OFFLINE_STORAGE_MODE` (`dynamodb`, the
  default, or `s3`) sets the storage mode it reports; in S3 mode change lists are refused with
  `UNSUPPORTED_IN_STORAGE_MODE`. Tests: `test/DashboardOfflineRunReporting.test.ts`.

**Benefit**: Enables full local testing without AWS credentials or external service access. The
offline services ignore the integration config, so the person and bulk sync routes don't read it when they
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
npm run dashboard:dev -- --offline  # The same offline (the offline services, no AWS access) for one run (flags: see below)
npm run dashboard:dev:offline:mock # Offline, playing the mock landscape (--offline --landscape mock)
```

`dashboard:dev` builds the templates, then runs `scripts/LocalLaunch.ts`, which starts
`LocalServer.ts` as a landscape's dashboard Lambda would run. Its settings are in
`scripts/LocalLaunch.env` (git-ignored; copy the committed `scripts/LocalLaunch.env.example`). The
repository's `.env` is never read.
- **The landscape**: `LANDSCAPE`, `REGION` and optional `STACK_ID` name the deployed dashboard Lambda to
  run as; `AWS_PROFILE` gives the credentials the server uses in place of the Lambda's role.
- **Online** (`OFFLINE=false`) the launcher looks up that Lambda's environment at every launch
  (`LambdaFunction`, see "Test Harnesses" below), leaves out its secrets (`ORIGIN_VERIFY_SECRET`), and runs
  the server with it: the server reads the landscape's tables, config secret and registry. A redeploy
  that changes the environment is picked up at the next launch. An expired AWS session, or a landscape
  without a deployed dashboard, stops the launch with a message. The message hints at the likely cause:
  `LANDSCAPE`/`REGION` when AWS answered that the function doesn't exist (`DashboardFunctionNotFoundError`),
  otherwise the AWS session.
- **Overrides**: any other variable in `LocalLaunch.env` wins over the Lambda's value (the launcher marks
  each one in what it prints). Without a deployed dashboard Lambda, the whole environment can be given there
  by hand.
- **Switches**: `OFFLINE` (`LOCAL_OFFLINE`; no lookup and no AWS access), `AUTHENTICATION` (`none`:
  public mode, the app login/logout headers removed; `local`: signed in as `USER_PRINCIPAL`, see below),
  `WATCH` (nodemon restarts the server on a change under `dashboard/`; the environment is looked up once and
  handed to each restart through `LOCAL_LAUNCH_RESOLVED`), `OFFLINE_STORAGE_MODE` (`dynamodb` | `s3`, the
  storage mode the offline run reporting reports), `PORT` (default 3000).
- **Flags** override a switch for one run, after `--`: `--offline`, `--online`, `--auth <none|local>`,
  `--watch`, `--no-watch`, `--storage-mode <dynamodb|s3>` (offline only), `--landscape <name>` (offline only: the
  landscape the offline services play, default `local`; `LocalLaunch.env`'s `LANDSCAPE` names a deployed landscape
  and isn't used offline), `--allow-production`, `--roles <Role,...>` (offline with the local sign-in: the local
  user's roles, see below).
- **Names before 2026-11-15** still work, each with a "Renamed:" warning saying what it is now
  (`LocalLaunchOptions.RENAMED`): `MOCKED`, `MOCK_STORAGE_MODE` and `AUTHENTICATION=mock` in `LocalLaunch.env`,
  `--mocks`, `--no-mocks` and `--auth mock`; and on the server `LOCALHOST_WITH_MOCKS` (`ServiceProvider.isOffline()`)
  and `MOCK_STORAGE_MODE`. A new name set alongside an old one wins.
- **Production guard**: online, the launcher refuses a landscape whose `LANDSCAPE_TONE` (the Lambda's,
  or an override) is `production`, since Sync and Bulk sync would write to its real target from local code,
  unless `ALLOW_PRODUCTION=true` or `--allow-production`. A local run also uses your profile's permissions,
  not the Lambda role's.
- **Local sign-in** (`AUTHENTICATION=local`, `LocalSignIn` in `scripts/LocalSignIn.ts`): stands in for the sign-in
  edge functions, so every request arrives signed in as `USER_PRINCIPAL` without an IdP. The Lambda's own
  sign-in settings are kept (`APP_AUTHORIZATION`, the login/logout header names, `AUTH_COOKIE_NAME`), so
  `Authenticator` runs as deployed. Before the handler sees a request, `LocalSignIn` adds the `authenticated:
  true` header and presents the JWT cookie, made as the edge functions make it: `JwtTools` signs
  `{ sub, user: <claims> }` (RS256) with a key pair generated when the server starts (`publicKey` keeps the
  public half), the claims array-valued as the IdP sends them (`buPrincipal`, and `eduPersonPrincipalName`,
  `mail`, `givenName`, `sn` from `USER_EPPN`, `USER_MAIL`, `USER_GIVEN_NAME`, `USER_SURNAME`). The top bar shows
  the user. `/logout` is answered locally with a page saying the local user can't sign out. Offline, with no
  Lambda to take them from, placeholder login/logout header names are set. The launcher passes
  `LOCAL_SIGN_IN=true` in the environment, so each watch restart puts it back in front of the handler.
  `LocalServer.ts` takes it through `startLocalServer({ requestDecorator })` (`LocalRequestDecorator`:
  `decorate(event)` and an optional `respond(path)`), and imports nothing from `scripts/`. The launcher sets
  `JWT_PUBLIC_KEY` to the local sign-in's public key (in place of a deployed Lambda's), so the handler verifies the
  local tokens as it verifies the landscape's. Tests: `test/DashboardLocalSignIn.test.ts`.
- **The local user's roles**: offline, `USER_ROLES` (comma-delimited role names, e.g. `Viewer` or
  `PersonOperator,Admin`; `none` for no role), or `--roles <Role,...>`, seeds the offline role store (`LocalLaunchOptions.roles`, checked against
  `ROLE_NAMES`, which a test keeps equal to the catalog's). Without it the local user has no role. Online the
  landscape's access table holds the roles (seed them with `DASHBOARD_ROLE_ASSIGNMENTS`), and `USER_ROLES` is
  ignored with a warning, as it is in public mode.

`LocalLaunchOptions` (`scripts/LocalLaunchOptions.ts`) decides all of this from the file and the flags
without side effects; `LocalLaunch.ts` reads, looks up, prints the environment (secrets masked) and starts.
Tests: `test/DashboardLocalLaunch.test.ts`. The launcher is TypeScript, not a shell script, because npm
runs scripts in `cmd.exe` on Windows, where neither `VAR=value` prefixes nor `sh` work.

**Environment Variables** (set by the launcher, or in `LocalLaunch.env`):
- `NODE_ENV=development` - Enables local mode, bypasses origin verification
- `PORT=3000` - Local server port (default)
- `LOCAL_OFFLINE=true` - Enables the offline services
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
  given with the prefix; `LOCAL_OFFLINE=true` uses the offline services without loading any config.

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

- `src/services/LaunchFlag.ts` (`DASHBOARD_LAUNCH_FLAG`) - a landscape's launch flag in SSM (see "Launch flag"
  below). `TASK`: `read` (as kept), `assess` (lowers it if `NEWEST_SYNC_RUN_ID` resolves it or it has expired),
  `raise` (a made-up launch lasting `LIFETIME_MINUTES`; refused if one is up), `lower`. Tests:
  `test/DashboardLaunchFlag.test.ts`.

```bash
DASHBOARD_LAUNCH_FLAG_TASK=read npx ts-node -r dotenv/config dashboard/src/services/LaunchFlag.ts
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

- `src/authorization/RoleAssignmentStore.ts` (`DASHBOARD_ROLE_ASSIGNMENTS`) - a landscape's role assignments, in its
  access table. `TASK`: `list`, `get`, `set` (replace `PRINCIPAL`'s roles with `ROLES`), `grant`, `revoke`. How the
  first Admin is seeded (fargate `README.md`, "Dashboard access"). Tests: `test/DashboardRoleAssignments.test.ts`.
- `src/audit/AuditStore.ts` (`DASHBOARD_AUDIT_TRAIL`) - a landscape's audit trail, read only. `TASK`: `list` (newest
  first, optionally by `PRINCIPAL` or `ACTION`, `LIMIT` and `CURSOR`). Tests: `test/DashboardAuditStore.test.ts`.

```bash
DASHBOARD_ROLE_ASSIGNMENTS_TASK=list npx ts-node -r dotenv/config dashboard/src/authorization/RoleAssignmentStore.ts
```

### Authorization

Who may do what in a landscape's dashboard, by role, per landscape (`authorization-plan.md`, delivered in seven steps,
2026-11-17).
- **The gate** (`RequestGate`, in `DashboardHandler` before routing; decision 7, deny by default): pages and the
  sign-in routes need only authentication; `GET /api/me` only sign-in; every other `/api/*` route its action
  (`RoutePermissions`); an unmapped `/api` route is `403`, and a known route's unsupported method `405` (`Allow`),
  before any role is asked. A denial is `403 { error: "FORBIDDEN", action, roles, message }` ("None of your roles
  (Viewer) has a policy that grants bulk:start."; a user with no role is told to ask an Admin). A signed-in user with
  no role gets every page, and `403` from every `/api` route but `/api/me`. Public mode (no sign-in) allows
  everything but impersonation (`PermitAllAuthorizer`). Tests: `test/DashboardAccessGate.test.ts` (each role against
  each route, through the handler offline).
- **Who is asking**: the sign-in token's claims, believed only once its signature and expiry check out
  (`ClaimsReader`, `VerifiedClaimsReader`: shibboleth-sp's `JwtTools`, RS256) against `JWT_PUBLIC_KEY`, which synth
  reads from the landscape's Shibboleth secret (`lib/JwtPublicKey.ts`; synth fails if the secret has none) and gives
  the Lambda (authenticated mode only; the Lambda gets no access to the secret). Without the key no one is signed
  in. It never takes a token from shibboleth-sp's `IMPERSONATE_*` variables. `Authenticator.user` (`Request.user`:
  the top bar, `startedBy`) reads claims the same way. Tests: `test/DashboardClaimsReader.test.ts`.
- **`GET /api/me`**: `{ signedIn, user?: { buPrincipal, eppn, mail, displayName }, buPrincipal, roles: [{ name,
  description, policies: [{ name, description, actions }] }], actions, unknownRoles, publicMode }`. It and a page load
  record the user's profile (`recordProfile()`; never fails the request).
- **Bulk sync's choices** (`BulkSync.choiceActions()`, checked in `plan()` and `start()` with the request's
  authorizer, after the request is parsed and before anything else): Start from scratch needs `mock:reset`, and
  Processor tasks above the landscape's maximum (any override when the maximum isn't known) `bulk:processors`. A
  refusal is a `ForbiddenError`, answered with the same `403`; nothing is started or reviewed.
- **The services**: `ServiceProvider.getRoleCatalog()` and `getRoleAssignmentStore()` (the access table,
  `ACCESS_TABLE_NAME`; offline, in memory, seeded with the local user's `USER_ROLES`; with neither, no one holds a
  role).
- **The model** (`src/authorization/`): actions (`Actions.ts`), policies and roles (`Policy`, `Role`,
  `StaticRoleCatalog`, from least to most: RunViewer (RunReporting), PersonViewer (PersonDataRead), Viewer (both),
  PersonOperator (Viewer's and PersonDataWrite), PipelineOperator (Viewer's and PipelineControl), Operator (all four),
  Admin (every policy); the finer roles since 2026-11-18, `authorization-plan.md` step 8), the person (`Principal.fromClaims()`, keyed by `buPrincipal`, compared
  lower-cased; `PrincipalResolver` takes a `buPrincipal` or `<id>@bu.edu`), `Authorizer` (deny by default; a 403's
  message; `effectivePermissions()` for `/api/me`) and `PermitAllAuthorizer` (public mode), `RoutePermissions` (what
  each `/api/*` route needs, whether it changes anything; an unmapped `/api` route is denied, a known route's
  unsupported method stays a `405`), and `Impersonation` (an Admin seeing the dashboard through another person's
  roles: the `hp-dashboard-impersonate` cookie, re-checked on every request, viewing only). Tests:
  `DashboardAuthorizer`, `DashboardRoutePermissions`, `DashboardImpersonation`.
- **Role assignments** (`RoleAssignmentStore.ts`): `DynamoDbRoleAssignmentStore` over the access table (partition key
  `buPrincipal`, sort key `sk`: one item per role held, with `grantedBy`/`grantedAt`, and one `#PROFILE` item per
  person seen, with what their sign-in token last said); `setRoles()` replaces a person's roles in one transaction,
  keeping the original grant of a role they keep. `InMemoryRoleAssignmentStore` for offline runs and tests.
- **Audit trail** (`src/audit/AuditStore.ts`): `DynamoDbAuditStore` over the audit table (every record in partition
  `pk` = `AUDIT`, sort key `sk` = `<ISO time>#<id>`, so a Query lists newest first; `principal-index` for one
  person's), paged with cursors (`AuditError` `INVALID_CURSOR` / `INVALID_LIMIT` / `INVALID_FILTER`); a public
  dashboard's records name `anonymous`. An id starts with a sequence number of the process, so records written in the
  same millisecond by one instance keep their order. `InMemoryAuditStore` for offline runs and tests
  (`ServiceProvider.getAuditStore()`). Never deleted from.
- **What is recorded** (`src/audit/AuditTrail.ts`, decision 9; `AuditTrail` is bound to one request's user and
  landscape, and never fails the request): every denial of an action, at the gate or of a Bulk sync choice (its reason
  as the detail); and, once the route has answered, every allowed `person:lookup`, `person:preview`, `person:sync`,
  `bulk:start` (a start, not a review), `mock:reset` and `access:admin` request (`auditsAllowed()`), `SUCCEEDED` for a
  2xx and else `FAILED` with the status (`access:admin` only for changes, since 2026-11-17: its reads were two
  records per Access page load; a role change's detail is the roles saved and what was dropped, `AuditTrail.note()`). `describeRequest()` takes the target (a BUID, a person, a test's BUID) and a
  short detail (a start's choices, Processor tasks among them) from the route and body. `runs:read` and `audit:read`
  aren't recorded, nor are a `405`, an unmapped route or `/api/me`. Tests: `test/DashboardAuditTrail.test.ts`,
  `test/DashboardAuditStore.test.ts`.
- **`GET /api/audit?cursor&limit&principal&action`** (`audit:read`): `{ items, cursor? }`, newest first, at most 100 a
  page (default 50), optionally one person's (a `buPrincipal`, `<id>@bu.edu` or `anonymous`) or one action's; a bad
  query is `400 { error, code }`.
- **The Access routes** (`src/handlers/AccessRoutes.ts`, `access:admin`): `GET /api/access/roles` (the catalog's roles,
  policies and actions), `GET /api/access/assignments` (everyone with a role, with profiles), `GET`, `PUT { roles }`
  and `DELETE /api/access/assignments/{person}` (a `buPrincipal` or `<id>@bu.edu`; `PUT` replaces the roles, granted
  by the Admin who saved them, less any role another of them covers, answering those as `pruned`). Refusals are `{ error, code }`: `400 INVALID_PRINCIPAL | INVALID_REQUEST |
  INVALID_ROLE`, and `409 LAST_ADMIN` for a change that would leave no Admin (decision 11; checked against the store
  as it is, so two Admins removing each other at the same moment could both pass). Tests:
  `test/DashboardAccessRoutes.test.ts`.
- **Giving roles to several people** (since 2026-11-19): `POST /api/access/assignments { principals, roles }` gives
  the roles, less the covered ones, to each person listed (each once, at most `MAX_BULK_PRINCIPALS` = 100, a read and
  a write each within the Lambda's 30 seconds) **who holds no role**. Anyone who already holds a role keeps exactly what
  they hold, so a bulk grant never overwrites a role given one person at a time; the answer
  `{ roles, pruned, granted, unchanged: [{ buPrincipal, roles }] }` lists them, and the page says so gently ("left as
  they are ... look each one up above"). Nothing is saved unless every principal is valid (`400 INVALID_PRINCIPAL`
  with `invalid`); also `400 TOO_MANY_PRINCIPALS`. It only adds, so the last-Admin safeguard has nothing to guard.
  Audited as a record per person given the roles (`roles=Viewer (bulk)`) and the request's own record (target
  `N people`, detail the roles, how many were granted and who was left as they were). On the Access page, below the
  table of role holders, everyone's `buPrincipal` is listed comma-delimited in small print (one click selects it all;
  Copy writes it to the clipboard, or selects it where the clipboard can't be written), to paste into the form
  "Give roles to several people" in another landscape. That form has its own role checkboxes, with the same coverage
  as one person's (`AccessPage.choose()` / `applyCoverage()` take the form's container and chosen set).
- **Covered roles are pruned** (decision 15; `pruneCoveredRoles()` in `RoleCatalog.ts`): of the roles chosen for a
  person, one is dropped when every action it grants is also granted by another chosen role (actions compared, not a
  ranking; of two granting the same actions, the later in the catalog is kept). The `PUT` route and the
  `DASHBOARD_ROLE_ASSIGNMENTS` harness's `set` and `grant` save the pruned set; the last-Admin safeguard judges it.
  The Access page applies the same rule as roles are checked (`AccessPage.coverage()` / `includedIn()`): a covered role
  shows checked, disabled and "included in Admin", and unchecking its coverer frees it. Assignments saved before are
  pruned the next time they are saved.
- **The Administration pages** (sidebar group Administration, shown to everyone; `public/js/administration.js`,
  `templates/partials/access-tab.mustache` and `audit-tab.mustache`): **Access** (`AccessPage`: two ways of changing
  roles as tabs, one shown at a time (`showMode()`, `AccessPage.MODES`; underlined like a run's sections): **A person's
  roles**, the default (look a person up, their profile and a checkbox per role, Save, asking first when an Admin
  removes their own Admin role), and **Give roles to several people** (above); below them, everyone with a role, a row
  opening that person in the first tab) and **Audit trail** (`AuditTrailPage`: a `PagedTable` over `/api/audit`,
  filtered by person and action; the action list comes from `ACTIONS`, as the template's `auditActions`). Tests:
  `test/DashboardAdministrationPages.test.ts`.
- **Who am I** (`WhoAmI`, decision 10): the top bar's user is a button opening a dialog with the user's identity,
  roles, policies and actions, read from `/api/me` when it opens; a signed-in user with no role also gets a notice
  above the pages (`data-no-role-notice`). Neither exists in public mode.
- **A refused load is redacted** (decision 7): every section that loads content to display reports a failure through
  `PageState.failed()` / `failedElement()` (`page-state.js`), which shows a `403` as the redacted state naming the
  action the roles lack (`fetchSectionData()` carries the 403's `action`), and anything else as the error state.
  Overview's Mock landscape section stays hidden when its read is refused. An action the user triggers (lookup, sync,
  review, start, save) shows the server's message in the error pattern.
- **Impersonation** (decision 14): an Admin sees the dashboard through another person's roles, for viewing only.
  - **The top bar**: an Impersonate button, the one control hidden by role (only for a user whose own roles grant
    `access:impersonate`), opens a dialog: a person (typed, or picked from everyone with a role), their roles, then
    Impersonate. While impersonating, the button is **Stop impersonating <person>** (their name if they have signed in
    here, else their `buPrincipal`), and the navigator's tint is the `impersonating` tone ("Impersonating amy: viewing
    only", `IMPERSONATING_TONE` in `NavigationBar.ts`). The who-am-I dialog shows both people. `ImpersonateControl`
    (`administration.js`) starts and stops it, then reloads the page.
  - **The routes**: `POST /api/impersonation { principal }` (the user's own `access:impersonate`; not yourself) sets the
    `hp-dashboard-impersonate` cookie (host-only, `HttpOnly`, `Secure`, `SameSite=Strict`, a session cookie);
    starting another replaces it. `DELETE /api/impersonation` (sign-in only) clears it. Both are audited.
  - **The gate** (`RequestGate`, through `Impersonation.resolve()`): on every request, a cookie naming someone is
    honored only while the signed-in user's own roles grant `access:impersonate`; otherwise it is ignored, cleared
    (`Set-Cookie`) and audited as a denial. Honored, the person's roles decide (`RequestAccess.authorizer`; the user's
    own are `userAuthorizer`), and a request that changes something is refused `403 { code: "IMPERSONATING", message:
    "Stop impersonating to do this." }` even when the person's roles allow it, so nothing is done in their name. A
    run's review changes nothing, so it is allowed.
  - **Who did it**: audit records name the user as `buPrincipal` and the person as `viewedAs`; `startedBy` and profiles
    are always the user's. `/api/me` gives the roles in force, `viewedAs` and `mayImpersonate`.
  - Tests: `test/DashboardImpersonation.test.ts` (the model), `test/DashboardImpersonationGate.test.ts` (through the
    handler: valid, revoked and forged cookies, each changing route, the audit, the top bar by role).
- **The first Admin** is seeded by hand after a deploy (the tables start empty, and a teardown deletes them): the
  `DASHBOARD_ROLE_ASSIGNMENTS` harness or `aws dynamodb put-item` (fargate `README.md`, "Dashboard access").

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
- Its own two tables (`lib/AccessTables.ts`, see "Authorization" above), in public mode too: the access table
  `<STACK_ID>-access-<landscape>` and the audit table `<STACK_ID>-audit-<landscape>`, both destroyed with the stack,
  read and written by the Lambda (`ACCESS_TABLE_NAME`, `AUDIT_TABLE_NAME`). The only DynamoDB tables it writes
- No secret of its own: the Lambda reads the landscape's integration config secret
  (`integrationSecretName()` in fargate `lib/Secrets.ts`), and has read-only grants on the
  statistics table, the person history table (DynamoDB mode) and the chunks bucket
  (`lib/LandscapeResources.ts`). To start a run (Bulk sync) it may also invoke the chunker subscriber
  (`chunker-subscriber-<landscape>`, which resets the chunker's counters for a run's start) and read its and
  the processor subscriber's configuration (their `DRY_RUN`), and read the chunker service's most tasks (its scalable target, for Parallel start). It is told the context's processor maximum (`PROCESSOR_MAX_SCALING_CAPACITY`, for Processor tasks) and changes nothing on the service; in a mock landscape only, it may also look up the source simulator's Function URL (`lambda:GetFunctionUrlConfig` on `source-simulator-<landscape>`, named in its `SOURCE_SIMULATOR_FUNCTION_NAME`), and read its last reset's record (`dynamodb:GetItem` on the atomic counter table). The one thing it writes is its landscape's launch flag (`ssm:PutParameter`, `GetParameter`, `DeleteParameter` on `/hp-dashboard/launch-flags/<landscape>`, "Launch flag" below). All by deterministic name -
  no CloudFormation imports from the pipeline stack.

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
  (`src/services/Authentication.ts`, through `ClaimsReader`, which verifies it) reads that cookie. Never read `JwtTools.COOKIE_NAME`
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
- **An unfinished run's status is its phase** (`RunView.phase(now)`, drawn by `RunView.statusBadgeHtml(now)` in
  the list's Status, the run page's header, Overview and Bulk sync's status panel), in place of "Incomplete":
  **Fetching** (the chunker hasn't found the end of the source; no percent), **Processing**, **Post-processing**
  (every chunk processed; deactivations and the summary come next; no percent) or **Stalled**
  (`RunView.stalledFor(now)`: the stall rule below, applied to a run waiting on post-processing too, so one whose
  post-processor never ran doesn't read "Post-processing" forever; the rule `RunActivity` applies for Bulk sync's
  guard; with its percent unless it stalled while fetching). All amber; green is kept for a finished run. A run
  without chunk figures keeps "Incomplete", a failed one "Failed".
- **An unfinished run's figures** (`soFar`) are marked wherever they show. In the list its Added,
  Updated, Deactivated, Errors and Duration are muted (`td.so-far`), with its percent done beside its
  phase (none while fetching or post-processing, or "so far" without chunk figures; Records come from the metadata
  and aren't marked). Its
  tiles (`.stat-tile-so-far`, on the run's page and Overview) are labeled "so far", the Processed tile
  also shows the percent done ("of 735 chunks") and a bar (`RunDetailView.completionHtml()`), and they are followed
  by the progress line (`RunDetailView.progressHtml()` over `RunView.progressParts()`: "412 of 735 chunks
  processed · last chunk finished 1 min ago · processing should finish around 7:45 PM (in about 5 h 06
  min)"; while fetching, "412 chunks processed", with no "of"); an incomplete full sync's Deactivated tile notes
  that post-processing adds deletions.
- **Percent done** (`RunView.completion`): chunks processed ÷ chunks written (`chunksReported` ÷ METADATA
  `chunkCount`), rounded down to one decimal and capped at 100, so it reads 100.0 only when every chunk is
  processed. Counted in chunks because the processors' own figure (`results.processed`, each chunk's
  `totalRecords`) counts only the people sent to Huron, those whose record changed: by it, a run where few people
  changed (a nightly full sync trusting stored fingerprints, or a Test of an unchanged person) never looked done,
  and never reached Post-processing (found in preview 2026-11-12; phase 5d step 5). Chunks hold the same number of
  people bar the last, so it is the share of people processed too, at chunk granularity. It measures processing,
  not chunking. Its state (`.completion-<state>`): `done` (100.0, and chunking has finished, METADATA
  `partialOrEmptyChunkEncountered`), `underway`, `fetching` (no percent: the total isn't known, and `chunkCount` is
  added as each chunker task finishes, so it lags the chunks processed and could read 100% mid-fetch), all amber,
  since the run isn't finished until its summary (`done` was green until 2026-11-11, which read as contradicting
  "Incomplete" beside it); `failed` (gray).
- **Estimated finish** (`RunView.estimate(now)`, underway runs only): the chunk rate so far (chunks processed ÷
  start to last chunk) applied to the chunks left, from the last chunk's end; it leaves out post-processing. A run is
  **stalled** (in place of the last-chunk time, amber) when no chunk has finished for more than
  `RunView.STALL_FACTOR` (10) times its average gap between chunks and at least `RunView.MIN_STALL_MS` (15
  minutes); a still-fetching run can stall but gets no estimate. A run waiting on post-processing stalls by the
  same rule ("Stalled: post-processing hasn't finished 3 days after the last chunk"). All from the browser's clock
  at each load.
- **The processor maximum** (`RunView.processorTasksLabel()`): among a run's facts, "Up to 8 processor tasks
  (override)" (the summary's `processorScalingCapacity`, else the flags'), and "Processor maximum" on the Flags card;
  nothing for a run from before it was recorded. The offline catalog's incomplete run has an override of 8.
- **Started by** (`RunView.startedByLabel()`): a column after Started, and the first of a run's facts ("Started by
  jdoe"): the signed-in user's `buPrincipal` for a start from Bulk sync ("Dashboard" without one), "Schedule" or
  "Runner", from the summary's `startedBy` or else the flags' (fargate `src/chunking/StartedBy.ts`); a dash for a run
  from before it was recorded. The offline catalog's newest 30 runs have one (`OfflineRunCatalog.STARTED_BY_RECORDED_FOR`):
  the schedule's nightly runs, two single-person runs and the incomplete run from Bulk sync, the bulk reset from the
  Runner.
- A single-person run is named by its BUID only when the report has metadata (the source URL's `buid`): a
  finished run's summary doesn't record it.
- `RunHistoryPage` shows the list (`#runs`, 20 runs a page with "Load older runs") or one run
  (`#runs/<syncRunId>`). The list keeps what it has loaded while a run is open; Refresh reloads it.
- `RunDetailView` is one run: the header (status, population, storage mode, duration, dry run, bulk
  reset), stat tiles, a note when the run failed, failed post-processing or has no summary yet (for
  a run with `progress`, saying its figures are summed from the chunks processed so far), and
  four tabs, each loaded when first opened: **Overview** (summary; the deletion outcome, whose counts are Removed
  from source, the deletion step's `totalProcessed`, then Deactivated and Failed, which add up to it; and the flags,
  each shown as its value by `RunDetailView.flagValue()`, never as JSON: Started by as `startedByLabel()`, the terms
  endpoint as one URL, any other object as `name: value` pairs, and a value longer than `WIDE_FLAG_LENGTH` under its
  name across the card),
  **Chunks** (per-chunk statistics with totals), **Errors** (filtered by status code, with counts
  from the summary or else the chunk statistics) and **Changes** (by type; in S3 mode, where
  `changesSupported` is false, it says why there are none).
Tests: `test/DashboardRunHistory.test.ts` runs the script in a vm (as written and minified) and drives
`RunsApi` through the handler with the offline run history.

### Launch flag

A run started from Bulk sync isn't listed until its first chunker task writes its flags and metadata (the RUN marker
in the statistics table), which can be minutes after Start: the chunker subscriber, invoked asynchronously, may first
reset a mock landscape (up to 10 minutes) or seed a parallel start (its alarm wait up to about 6), and the chunker task
chooses the run's syncRunId when it starts. A start from scratch also empties Run history. So until then the
dashboard keeps a flag of its own, `LaunchFlag` (`src/services/LaunchFlag.ts`, since 2026-11-16,
`mock-run-feedback-plan.md` issue 1); the pipeline knows nothing of it.
- **One SSM String parameter per landscape**, `/hp-dashboard/launch-flags/<landscape>` (apart from the landscape
  registry's path), holding a `Launch` as JSON: `launchedAt` (the start's `processedAt`, not the run's syncRunId),
  `expiresAt`, `startedBy` and the choices. Its lifetime is 5 minutes, plus 7 for a parallel start, plus 10 for a start
  from scratch (`LaunchFlag.lifetimeMs()`).
- **Raised** by `BulkSync.start()` once the start's checks pass, just before it invokes the chunker subscriber, with
  `PutParameter` `Overwrite: false`: of two starts that pass the checks together, only one put succeeds, and the other
  is refused `RUN_LAUNCHING` without invoking anything. Lowered again if the invoke fails. A review never raises it.
- **Assessed** (`LaunchFlag.assess(newestSyncRunId)`) by `BulkSync`'s state, start, review and reset checks, and by
  the runs route's first page (`RunList`): lowered once the newest listed run is no earlier than `launchedAt` less
  `CLOCK_SLACK_MS` (30 seconds: the chunker task's clock against the Lambda's), or once it has expired; otherwise
  active. While active, a start or a mock landscape's reset is refused (`RUN_LAUNCHING`), and the pages show the launch
  note (`RunFormat.launchNoteHtml()`, in `run-history.js`): what, by whom, how long ago, what it waits for (the reset,
  the parallel start's tasks), and the time after which it didn't start. Overview shows it in place of "No runs yet"
  or above the latest run, Run history above the list (`RunHistoryPage.showLaunch()`, `data-run-launch`), Bulk sync
  as its status panel.
- **Known trade-offs** (accepted 2026-11-16): a start that dies after the invoke (a failed reset, a dropped event, a
  chunker task that never starts) only shows in CloudWatch; its flag simply expires. A page load that reads an expired
  flag could, in the milliseconds before it deletes it, delete a new start's flag instead, leaving that start
  unguarded.
- **Also refused while a mock landscape's reset runs** (`RESET_UNDERWAY`, from its record): during a start from
  scratch's reset the statistics table is empty, so the newest-run check alone would let a second start through.
- **Offline** the flag is kept in memory (`MemoryLaunchFlagStore`), one per landscape played, shared by the offline
  Bulk sync and the runs route (`ServiceProvider.getLaunchFlag()`). No offline run ever appears, so a launch shows until
  it expires; restarting the server clears it. Tests clear it with `ServiceProvider.resetOfflineState()`.
- A dashboard teardown leaves a flag that happens to be up; there is at most one, and it lives for minutes.
- Tests: `test/DashboardLaunchFlag.test.ts`, the launch cases in `test/DashboardBulkSync.test.ts`, the pages' in
  `DashboardBulkSyncPage`, `DashboardOverview` and `DashboardRunHistory`, and the grant in `DashboardStack`.

### Overview page

The `overview` page (`templates/partials/overview-tab.mustache`, `public/js/overview.js`) is the
landing page. `OverviewPage` loads, each time the page is shown and on Refresh, the latest run
(`/api/runs?include=report&limit=1`: its status, facts, how long ago it finished or started, the
run page's note and stat tiles, figures so far for a run without a summary, and a link to its page) and the service checks (`/api/status`, in
the `ServiceStatusTable` of `public/js/system-status.js`, which the Status page uses too). The two
load independently, so one failing (the real `SystemStatus` service isn't implemented yet) leaves
the other shown. While a run launched moments ago isn't listed yet (the runs route's `launch`), its launch note
comes first, in place of "No runs yet" or above the latest run. Scripts load in dependency order: `page-state.js`, `run-history.js`,
`system-status.js`, `overview.js`, `bulk-sync.js`, `administration.js`, then `dashboard.js`, which creates the pages. Tests:
`test/DashboardOverview.test.ts`.
- **A mock landscape's own section** ("Mock landscape", hidden in any other landscape; since 2026-11-15,
  `mock-landscape-options-plan.md` step 3a): loaded with the rest from `GET /api/mock-landscape/reset`. It explains in
  plain words what a reset empties, says what the last reset did (`BulkSync.resetState()`, from the record the chunker
  subscriber keeps in the atomic counter table: running, for how long and since when, up to 10 minutes; finished,
  with each table's count and time to the second, `OverviewPage.resetDuration()`; failed, with why; or never finished,
  most likely out of time), and offers Reset landscape. It asks first (`OverviewPage.RESET_CONFIRMATION`), then posts
  (`BulkSync.requestReset()`), and says the reset runs in the background for up to 10 minutes: Refresh shows whether
  it has finished (the page never polls). Disabled when one can't be started now, with the reason as a note, except
  while a reset is running, which its own line already says (the reason is then only the button's title).
- **Offline**, the stand-in service keeps the last reset in memory (one per landscape played,
  `ServiceProvider.getBulkSyncService()`): a past one that emptied an old hybrid run's tables, replaced by a reset
  requested there, which finds the tables empty. The sample Run history isn't emptied.

### Bulk sync page

The `bulk` page (`templates/partials/bulk-tab.mustache`, `public/js/bulk-sync.js`) starts a sync run in this
landscape the way its schedule does (`GET`/`POST /api/bulk-sync`, `POST /api/bulk-sync/plan`, `BulkSync`; see the
routes above).
- **The options** (`BulkSyncRequest`): a run type (Full sync (with deactivations), `person-full`, the default; Full
  sync (no deactivations), `person-delta`; a disabled Delta sync, since a source of only the people who changed
  doesn't exist yet; Test, one person by BUID through the person endpoint, `person-delta`), Bulk reset, and Custom
  settings: chunker tasks ("Chunks per task", 1-50, default 10, `BulkSyncRequest.CHUNKS_PER_TASK_DEFAULT`; or
  "Single task", sent as `iterationLimit: 0`; a test is always a single task, whatever the form sends, since a chained chunker task would fetch the same person again: with bulk reset it got ahead of the first while that built the person cache, and the person was chunked twice, preview 2026-11-12; `BulkSyncRequest.parse()`; on the page, choosing Test checks Single task and locks both chunk modes, and choosing another run type unlocks them and restores the mode chosen before, kept on the fieldset as `data-chosen-mode`: `BulkSyncPage.lockChunkMode()`) and Parallel start (below). The
  defaults are the scheduled run's (`ScheduledRun`, fargate `src/chunking/ScheduledRun.ts`).
- **A mock landscape's own options** (`BulkSyncState.mock`, a "Mock landscape" section after Custom settings, only where
  `isMockLandscape()`; `mock-landscape-options-plan.md`, since 2026-11-15):
  - **Source**: the real source API (the default, a hybrid run) or the source simulator. With the simulator, the
    event's source is its Function URL (looked up by `AwsSourceSimulatorService`), its people endpoint or
    `/person?buid=` for a test, with the four simulator settings as query parameters of `fetchPath`
    (`simulatorFetchPath()`, which the manual Runner shares, and `simulationSettings()`, both in
    `src/chunking/fetch/SourceSimulator.ts`), and its `/terms` as the `termsEndpoint`.
    Nothing about the simulator Lambda is changed. Its settings start at its deployed ones, read from its
    unauthenticated `/health`; refused out of range (`BulkSyncRequest.SIMULATOR_LIMITS`: at most 500,000 people, the
    chunker's safety cutoff, and 30 seconds a response). When the simulator isn't deployed, its choice is disabled
    with the reason.
  - **A test with the simulator** reads one person, so the page doesn't send People and the simulator keeps its own
    population, which only says which BUIDs it has. While Test and the simulator are both chosen, the form carries
    `BulkSyncView.SIMULATOR_TEST_CLASS` (`BulkSyncPage.markSimulatorTest()`; `dashboard.css` does the rest): People is
    hidden, the other settings' hints speak of one person (`SIMULATOR_FIELDS`' `testHint`), and the BUID box says the
    simulator's range (`simulatorBuidHint()`: U0000001 to its population, from `/health`) with `U0000042` as its
    placeholder. A BUID only has to be `U` and digits from 1: one past the simulator's people is allowed (since
    2026-11-16, `mock-run-feedback-plan.md`), since the simulator then answers with no one and the test sees someone
    who isn't in the source; the review says so (`simulatorTestLine()`, given the page's population).
  - **Start from scratch** (`resetState`): the event carries it, and the chunker subscriber empties the target
    simulator, the statistics (Run history) and the delta baseline before the run starts
    (`src/chunking/MockLandscapeReset.ts`). The page says Run history starts again from this run.
  - **Refused elsewhere** (`BulkSyncRequest.parse(body, { mockLandscape })`), and only a mock landscape's dashboard
    has the simulator's name and lookup grant. The review's `sourceSimulator`, `sourceSimulatorMock*` and
    `mockTargetResetState` settings come from the form in a mock landscape, and are fixed at `false` elsewhere.
  - **The review** names the target simulator as the target, says which source, and notes a Parallel start seeded
    past the simulator's people (from the review's `itemsPerChunk`).
  - **Offline**: `npm run dashboard:dev:offline:mock` plays the mock landscape, with a stand-in simulator
    (`OfflineBulkSyncService.STAND_IN_SIMULATOR`, mock's deployed settings) at an example URL. It is always found, so a
    simulator that isn't deployed is seen only in the tests.
- **Parallel start** (`parallelStart`): how many chunker tasks start the run at once, standing for the
  Runner's `MESSAGES_TO_PREPOPULATE` and `DESIRED_COUNT` together. 0 sends the chunker subscriber one message, and
  auto-scaling adds tasks. N above 0 goes on the event, and the subscriber seeds the queue with N messages (offsets
  a task's chunks apart) and sets the chunker service's desired count to N once its scale-in alarm has settled
  (fargate `src/chunking/ParallelStart.ts`, shared with the Runner's queue seeding), so the start takes up to a few
  minutes, which the review and the status panel say.
  - **Its default** is 15 (`BulkSyncRequest.PARALLEL_START_DEFAULT`, since 2026-11-16; it was 0), lowered to its
    most: `describe()`'s `defaults.parallelStart`, which the form starts from, so a run starts at full speed rather
    than ramping up from one task. A request without `parallelStart` (the API's, not the form's) still gets 0, so the
    form carries the default (`data-default-parallel`) and `readRequest()` sends it for a full sync even with Custom
    settings closed (until 2026-11-17 it didn't, and a run started that way began with one task).
  - **Its most** is the chunker service's most tasks (`DesiredCount.getMax()`, its scalable target's
    `MaxCapacity`), capped at 20 (`parallelStartMax()`): `describe()` offers 0 to it, and `plan()`/`start()` refuse
    more (`INVALID_REQUEST`). The Lambda has `application-autoscaling:DescribeScalableTargets` and the chunker
    cluster and service names (`CHUNKER_CLUSTER_NAME`, `CHUNKER_SERVICE_NAME`, `lib/LandscapeResources.ts`) for
    this; it changes nothing on the service itself. If the maximum can't be read, 20 is offered and the subscriber
    refuses what the service can't run.
  - **Refused** for a test (one person, one task) and with a single task (`iterationLimit` 0: seeded offsets are a
    task's chunks apart). On the page it's held at 0 and locked while Test or Single task is chosen, and the value
    chosen before is restored afterward (kept on its wrapper as `data-chosen-parallel`:
    `BulkSyncPage.lockParallelStart()`); `readRequest()` sends 0 then whatever the select says.
- **Processor tasks** (`processorScalingCapacityOverride`, since 2026-11-17, `processor-max-tasks-plan.md`; fargate
  "A run may have its own processor maximum"): the processor service's maximum for this run only, 1 to 20
  (`BulkSyncRequest.PROCESSOR_TASKS_MIN`/`MAX`, the chunker subscriber's), after Parallel start under Custom settings.
  - **It starts at the landscape's** (`defaults.processorScalingCapacity`, from the Lambda's
    `PROCESSOR_MAX_SCALING_CAPACITY`, the context's `ECS.processorService.maxScalingCapacity`;
    `LandscapeResources.processorMaxScalingCapacity`). A dashboard that doesn't know it offers "The landscape's own".
  - **Sent only when it differs** from the landscape's (`data-default-processors`, `readRequest()`), never for a test
    (refused by `parse()`; on the page locked at the landscape's while Test is chosen, the choice restored after:
    `BulkSyncPage.lockProcessorTasks()`). The event carries it, and the chunker subscriber sets the service's live
    maximum to it at the run's start.
  - **The review** says "It uses up to 8 processor tasks at once, for this run only", and above the landscape's warns
    "Processors may be throttled" (`BulkSyncView.throttlingWarning()`, from the plan's `processorMaxScalingCapacity`
    setting) above Start run and Back: starting is allowed, to find out whether Huron takes more now. The review's
    settings list both.
  - The Custom settings hint lists the three defaults (`BulkSyncView.defaultsText()`).
- **Bulk reset is one choice for both previous-state flags** (`ScheduledRun.previousStateFlags()`): checked sends
  `bulkReset: true, trustPreviousStorage: false`, unchecked `false, true`. The pipeline only acts on their
  combination, and a missing `trustPreviousStorage` reads as `false` (a reset), so both are always sent.
- **Not offered** (command line only, or not yet): addons, `CHUNKING_ONLY` / `MESSAGING_ONLY` (they switch the
  subscribers' `DRY_RUN`, which stays switched), skipping the counter reset (the chunker subscriber always resets),
  and `ITEMS_PER_CHUNK` (the chunker task's environment, set at deploy). The plan route's settings list them with
  their fixed values; the page doesn't show that list.
- **The page's words are for people who don't know the pipeline**: bulk reset is explained as a fingerprint of
  each person's record rather than as hashes, and the page doesn't mention the schedule (a later page will
  describe what the schedule does).
- `BulkSyncView` is the words and markup. `formHtml(state, values)` starts at the defaults and keeps what was
  entered; choosing Test shows the BUID box and what a test is, checking Custom settings shows those fields, and
  "Chunks per task" shows its dropdown. A start
  the server would refuse now (`state.blocked`, not configured included) is said above Review run, which is
  disabled then; a stalled newest run instead offers the "Start despite the stalled run" checkbox
  (`overrideStalled`). Warnings (a mock landscape's real source) show first. `requestError()` catches an invalid
  request before it is sent, `summaryLines()` spells out what will run, and `confirmationHtml(plan)` shows the
  review: the summary, then a table of the event the chunker subscriber will receive.
- `BulkSyncPage` loads the state each time the page is shown and on Refresh. Review run locks the fields and asks
  the server for the review (`POST /api/bulk-sync/plan`), which a refusal answers under the fields (unlocked); Back
  unlocks them. So what is started is what was reviewed. Start posts the same request (`postSectionData()` in
  `dashboard.js`, which also carries the server's error `code`); a refusal is shown under the review.
- **The status panel is the server's state at each load** (`BulkSyncView.statusHtml()`): while a run launched
  moments ago isn't listed yet (`state.launch`), the launch note; else the newest run, as it is now, which
  Refresh keeps current. It used to keep the started run as first found, so it said "Processing" after the run had
  finished, until the page was reloaded (fixed 2026-11-16, `mock-run-feedback-plan.md` issue 3). After a start the
  page simply reloads: the server raised the launch flag before answering, so the note shows at once, and Review run
  is disabled with the `RUN_LAUNCHING` reason.
- The Lambda's environment carries what the review shows: `PIPELINE_STACK_ID`, `ITEMS_PER_CHUNK`, and the config
  secret's name (`lib/FunctionUrl.ts`).
- **Who starts it**: the handler passes the request's signed-in user (`Request.user`, the sign-in token's claims) to
  `plan()` and `start()`, and `BulkSync.startedBy(user)` makes `{ by: 'dashboard', principal: <buPrincipal> }` of it
  (no principal in a public dashboard). It goes on the event, so the review shows it and the run records it (Run
  history's Started by). Never taken from the request body.
Tests: `test/DashboardBulkSyncPage.test.ts` (the script in a vm, as written and minified, and once over the
offline routes); the service: `test/DashboardBulkSync.test.ts`.

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
4. Add an offline implementation to `OfflineServices.ts`
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
The launcher (`npm run dashboard:dev`, see "Local Development") sets `NODE_ENV=development`, which bypasses
origin verification, and `LOCAL_OFFLINE` from `OFFLINE`; don't set them by hand.
1. **No AWS access, or none wanted**: `OFFLINE=true` in `scripts/LocalLaunch.env`, or `-- --offline` for one run.
2. **The launch stops before the server starts**: an expired session for `AWS_PROFILE` (refresh it), or no
   dashboard Lambda by that name (check `LANDSCAPE`, `REGION` and `STACK_ID`); the message says which.
3. **The port is already in use**: stopping `npm run dashboard:dev` (Ctrl+C, or closing its terminal) can leave
   its `node` process listening on Windows. Find the process listening on `PORT` (default 3000) and stop it, or
   set another `PORT` in `LocalLaunch.env`.
4. **PowerShell refuses `npm`**: with the execution policy `Restricted`, npm's `npm.ps1` shim is blocked; run
   `npm.cmd run dashboard:dev`, or use `cmd.exe` or Git Bash.
5. **Works locally, fails deployed** (or the reverse): a local run uses your profile's AWS permissions, not the
   Lambda role's, so a missing grant only shows when deployed.
6. **The top bar names the user differently**: deployed it shows the IdP's `eduPersonPrincipalName`; the local
   sign-in shows `USER_PRINCIPAL` unless `USER_EPPN` is set.
7. Route errors are in the server's own output.
