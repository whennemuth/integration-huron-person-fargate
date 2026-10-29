# integration-huron-person-dashboard: Serverless Management Dashboard

## Project Purpose
A serverless dashboard for managing integration operations between BU CDM and Huron systems. Provides web-based UI for individual person sync, bulk operations, activity history, and system status monitoring. Deployed via AWS CDK with Lambda function serving both as HTML dashboard server and data synchronization engine.

## Repository Relationship Model

The dashboard source lives in the `dashboard/` directory of the `integration-huron-person-fargate`
repository (moved from the retired `integration-huron-person-dashboard` repository; see
`docs/pan-landscape-data-exchange-plan.md`). It keeps its original internal layout (`lib/`,
`src/`, `context/`, `templates/`, `public/`, `scripts/`) and shares fargate's `package.json`,
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

## Key Patterns

### Service Provider Pattern
**Location**: `src/services/ServiceProvider.ts`

Centralized registry for managing service instances:
- PersonLookup service
- PersonSync service
- BulkSync service
- History service
- SystemStatus service
- Template service

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

## CDK Deployment

One dashboard stack per landscape, `${DASHBOARD.STACK_ID}-${landscape}` (e.g. `hp-dashboard-mock`),
created by `createDashboardStack()` when the landscape's fargate context (`../context/context.json`)
has a `DASHBOARD` section. That section is this directory's `IContext` minus `ACCOUNT` / `REGION` /
`TAGS`, which come from the landscape (`toDashboardContext()`). Keep the stack name within 25
characters: the Shibboleth construct names a bucket `<stack name>-context-config-<account>-<region>`.

**Stacks:**
- Distribution (CloudFront) and Shibboleth Lambda@Edge (separate us-east-1 edge-lambda stack)
- A record `<landscape>.cssnprd.warhen.work` plus its `/route53/arecords/<name>` ownership
  parameter, created by the Shibboleth construct when no record for the name exists
- Lambda Function URL with origin verification (SSM `/cloudfront/<STACK_ID>/<landscape>/origin-verify-secret`)
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
- `context/IContext.ts` - Dashboard context types (`DashboardSettings`)

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
