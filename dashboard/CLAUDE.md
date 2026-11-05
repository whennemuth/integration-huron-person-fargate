# integration-huron-person-dashboard: Serverless Management Dashboard

## Project Purpose
A serverless dashboard for managing integration operations between BU CDM and Huron systems. Provides web-based UI for individual person sync, bulk operations, activity history, and system status monitoring. Deployed via AWS CDK with Lambda function serving both as HTML dashboard server and data synchronization engine.

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
- Provides responsive Bootstrap UI with tabbed interface

**2. Data Synchronization Engine:**
- Executes person sync operations (individual and bulk)
- Interfaces with BU CDM data sources and Huron data targets
- Processes API requests for person lookup, sync, history, and status

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
- **Activity History**: Logs and audit trail of integration activities
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
- `GET /api/history` - Get activity history and logs
- `GET /api/status` - Get system health status and metrics

**Run reporting** (this landscape's sync runs, read in-process from its tables and chunks bucket
through fargate's `src/reporting/RunReporting.ts`; GET only, no `landscape` parameter):
- `GET /api/runs` - `{ runs }`, the syncRunIds, oldest first
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
  navigator doesn't call it (see "Navigation bar" below)

## Key Patterns

### Service Provider Pattern
**Location**: `src/services/ServiceProvider.ts`

Centralized registry for managing service instances:
- PersonLookup service
- PersonSync service
- BulkSync service
- History service
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

Provides mock implementations when `LOCALHOST_WITH_MOCKS=true`:
- Mock BU CDM data source responses
- Mock Huron data target operations
- Simulated sync execution
- Fake history and status data

**Benefit**: Enables full local testing without AWS credentials or external service access

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
npm run dashboard:dev              # Start local server at http://localhost:3000/dashboard
npm run dashboard:dev:watch        # Watch mode with hot reload
```

**Environment Variables:**
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
  `summary`, `chunks`, `errors`, `changes`, `landscapes`, `lookup`, `preview`, `history`,
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
npm run teardown-dashboard  # Destroy the dashboard and edge-lambda stacks (rerun later if Lambda@Edge replicas block the edge stack)
```

**Context Configuration:**
- `../context/context*.json` `DASHBOARD` section - Infrastructure parameters
- `context/IContext.ts` - Dashboard context types (`DashboardSettings`), including the optional
  `LABEL` (picker label, default: the landscape), `isDefault` (the default landscape) and
  `sharedSignIn` (see "Shared sign-in" below)
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

### Navigation bar

`renderDashboard()` builds the navbar's data with `buildNavigationBar()` (`src/handlers/NavigationBar.ts`).
Tests: `test/DashboardNavigation.test.ts`.
- **Landscape navigator**: a select over the landscape registry, rendered server-side from the
  `LandscapeRegistry`'s per-instance cache (no SSM read per page, and no `/api/landscapes` call
  from the page), with this landscape selected. Choosing one sends the browser to its `url`
  (`public/js/dashboard.js`). An entry without a `url` is listed but disabled. When the registry
  doesn't list this landscape (e.g. run locally), it's added, selected. If the registry can't be
  read, the page renders without the navigator.
- **Sign-out**: a link to `SIGN_OUT_PATH` (`/logout`), shown unless the dashboard is public. The
  edge functions answer `/logout` themselves, so the Lambda never sees it: they expire the JWT
  cookie (with its Domain, so with `sharedSignIn` every landscape is signed out), then redirect to
  `/logout?target=idp`, which sends the browser to `SHIBBOLETH.logoutUrl` to end the IdP session.
  The browser ends on the IdP's page. The navbar also shows the JWT's `eduPersonPrincipalName`
  (else `buPrincipal`).

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
4. Templates in `templates/` use `{{{ inlineCss }}}` and `{{{ inlineJs }}}` placeholders

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
