# Running the Dashboard Locally

How the local launcher runs the dashboard as a landscape's Lambda would, online or offline, with or without a sign-in,
and what the offline services answer with. The commands and the rules are in `dashboard/CLAUDE.md` ("Local
Development"); read this when changing the launcher, the local sign-in or the offline services, or when a local run
doesn't behave like the deployed one.

## The launcher

`npm run dashboard:dev` (from the fargate root) builds the templates, then runs `scripts/LocalLaunch.ts`, which starts
`LocalServer.ts` as a landscape's dashboard Lambda would run. Its settings are in `scripts/LocalLaunch.env`
(git-ignored; copy the committed `scripts/LocalLaunch.env.example`). The repository's `.env` is never read.

- **The landscape**: `LANDSCAPE`, `REGION` and optional `STACK_ID` name the deployed dashboard Lambda to run as;
  `AWS_PROFILE` gives the credentials the server uses in place of the Lambda's role.
- **Online** (`OFFLINE=false`) the launcher looks up that Lambda's environment at every launch (`LambdaFunction`, the
  `DASHBOARD_LAMBDA_FUNCTION` harness), leaves out its secrets (`ORIGIN_VERIFY_SECRET`), and runs the server with it:
  the server reads the landscape's tables, config secret and registry. A redeploy that changes the environment is
  picked up at the next launch. An expired AWS session, or a landscape without a deployed dashboard, stops the launch
  with a message. The message hints at the likely cause: `LANDSCAPE`/`REGION` when AWS answered that the function
  doesn't exist (`DashboardFunctionNotFoundError`), otherwise the AWS session.
- **Overrides**: any other variable in `LocalLaunch.env` wins over the Lambda's value (the launcher marks each one in
  what it prints). Without a deployed dashboard Lambda, the whole environment can be given there by hand.
- **Switches**: `OFFLINE` (`LOCAL_OFFLINE`; no lookup and no AWS access), `AUTHENTICATION` (`none`: public mode, the app
  login/logout headers removed; `local`: signed in as `USER_PRINCIPAL`, see below), `WATCH` (nodemon restarts the
  server on a change under `dashboard/`; the environment is looked up once and handed to each restart through
  `LOCAL_LAUNCH_RESOLVED`), `OFFLINE_STORAGE_MODE` (`dynamodb` | `s3`, the storage mode the offline run reporting
  reports), `PORT` (default 3000).
- **Flags** override a switch for one run, after `--`: `--offline`, `--online`, `--auth <none|local>`, `--watch`,
  `--no-watch`, `--storage-mode <dynamodb|s3>` (offline only), `--landscape <name>` (offline only: the landscape the
  offline services play, default `local`; `LocalLaunch.env`'s `LANDSCAPE` names a deployed landscape and isn't used
  offline), `--allow-production`, `--roles <Role,...>` (offline with the local sign-in: the local user's roles, see
  below).
- **Names before 2026-11-15** still work, each with a "Renamed:" warning saying what it is now
  (`LocalLaunchOptions.RENAMED`): `MOCKED`, `MOCK_STORAGE_MODE` and `AUTHENTICATION=mock` in `LocalLaunch.env`,
  `--mocks`, `--no-mocks` and `--auth mock`; and on the server `LOCALHOST_WITH_MOCKS` (`ServiceProvider.isOffline()`)
  and `MOCK_STORAGE_MODE`. A new name set alongside an old one wins.
- **Production guard**: online, the launcher refuses a landscape whose `LANDSCAPE_TONE` (the Lambda's, or an override)
  is `production`, since Sync and Bulk sync would write to its real target from local code, unless
  `ALLOW_PRODUCTION=true` or `--allow-production`. A local run also uses your profile's permissions, not the Lambda
  role's.

`LocalLaunchOptions` (`scripts/LocalLaunchOptions.ts`) decides all of this from the file and the flags without side
effects; `LocalLaunch.ts` reads, looks up, prints the environment (secrets masked) and starts. Tests:
`test/DashboardLocalLaunch.test.ts`. The launcher is TypeScript, not a shell script, because npm runs scripts in
`cmd.exe` on Windows, where neither `VAR=value` prefixes nor `sh` work.

**Environment variables the launcher sets** (or `LocalLaunch.env` gives):

- `NODE_ENV=development` - enables local mode, bypasses origin verification.
- `PORT=3000` - local server port (default).
- `LOCAL_OFFLINE=true` - enables the offline services.
- `OVERRIDE_CACHE_WITH_LOCAL_CONFIG=true` - loads the integration config with fargate's `getConfig()` (same sources
  as the fargate harnesses) instead of reading `INTEGRATION_SECRETS_ARN`.

## The local sign-in

`AUTHENTICATION=local` (`LocalSignIn` in `scripts/LocalSignIn.ts`) stands in for the sign-in edge functions, so every
request arrives signed in as `USER_PRINCIPAL` without an IdP.

- The Lambda's own sign-in settings are kept (`APP_AUTHORIZATION`, the login/logout header names, `AUTH_COOKIE_NAME`),
  so `Authenticator` runs as deployed.
- Before the handler sees a request, `LocalSignIn` adds the `authenticated: true` header and presents the JWT cookie,
  made as the edge functions make it: `JwtTools` signs `{ sub, user: <claims> }` (RS256) with a key pair generated when
  the server starts (`publicKey` keeps the public half), the claims array-valued as the IdP sends them (`buPrincipal`,
  and `eduPersonPrincipalName`, `mail`, `givenName`, `sn` from `USER_EPPN`, `USER_MAIL`, `USER_GIVEN_NAME`,
  `USER_SURNAME`). The top bar shows the user.
- `/logout` is answered locally with a page saying the local user can't sign out.
- Offline, with no Lambda to take them from, placeholder login/logout header names are set.
- The launcher passes `LOCAL_SIGN_IN=true` in the environment, so each watch restart puts it back in front of the
  handler. `LocalServer.ts` takes it through `startLocalServer({ requestDecorator })` (`LocalRequestDecorator`:
  `decorate(event)` and an optional `respond(path)`), and imports nothing from `scripts/`.
- The launcher sets `JWT_PUBLIC_KEY` to the local sign-in's public key (in place of a deployed Lambda's), so the
  handler verifies the local tokens as it verifies the landscape's.
- Tests: `test/DashboardLocalSignIn.test.ts`.

**The local user's roles**: offline, `USER_ROLES` (comma-delimited role names, e.g. `Viewer` or
`PersonOperator,Admin`; `none` for no role), or `--roles <Role,...>`, seeds the offline role store
(`LocalLaunchOptions.roles`, checked against `ROLE_NAMES`, which a test keeps equal to the catalog's). Without it the
local user has no role. Online the landscape's access table holds the roles (seed them with
`DASHBOARD_ROLE_ASSIGNMENTS`), and `USER_ROLES` is ignored with a warning, as it is in public mode.

## The offline services

`src/handlers/OfflineServices.ts` provides the offline implementations when `LOCAL_OFFLINE=true` (ignored in Lambda):

- Sample BU CDM data source responses, sample Huron data target operations, simulated sync execution, sample status
  data.
- A sample landscape registry (`mock` and `Staging` at example.com urls), whose current landscape is the one the
  offline services play: `LANDSCAPE`, which the launcher sets to `local`, or to `--landscape`'s
  (`npm run dashboard:dev:offline:mock` plays `mock`, so the top bar shows mock selected, with the mock tint). The
  entries' urls lead nowhere: a local run plays one landscape, and switching means restarting with the other command.
- **The landscape the offline services play** (`LANDSCAPE`, else `local`) is the one Bulk sync
  (`OfflineBulkSyncService`) and run reporting's capabilities report. A mock landscape's name (`mock`) shows Bulk
  sync's mock landscape options.
- **Run reporting** (`src/handlers/OfflineRunReporting.ts`): a deterministic history of 40 runs ending with the nightly
  run of 2026-11-04. `OfflineRunCatalog` defines them: nightly delta syncs, a full sync every seventh night, and at
  fixed positions from the newest the special cases (an incomplete run, a terminal error, a failed and a
  post-processing-failed run, a dry run, a bulk reset, two single-person runs). The newest run has 260 errors across
  400/404/429/500 and 130/240/12 NEW/UPDATED/DELETED changes, enough for several pages. Chunk statistics, errors and
  changes agree with each run's summary. `OfflineRunReportingService` pages them with offset cursors (`OffsetPager`)
  under RunReporting's limits and `ReportingError` codes. `OFFLINE_STORAGE_MODE` (`dynamodb`, the default, or `s3`)
  sets the storage mode it reports; in S3 mode change lists are refused with `UNSUPPORTED_IN_STORAGE_MODE`. The
  catalog's incomplete run has a processor-maximum override of 8, and its newest 30 runs record who started them
  (`OfflineRunCatalog.STARTED_BY_RECORDED_FOR`). Tests: `test/DashboardOfflineRunReporting.test.ts`.
- **The launch flag** is kept in memory ([BULK_SYNC.md](BULK_SYNC.md#the-launch-flag)), and so is a mock landscape's
  last reset ([PAGES.md](PAGES.md#overview-page)); the role store and audit trail are in memory too
  ([AUTHORIZATION.md](AUTHORIZATION.md)).

The offline services ignore the integration config, so the person and bulk sync routes don't read it when they are in
use (`serviceConfiguration()` in `DashboardHandler.ts`): no config secret is needed, and the whole dashboard can be
tested without AWS credentials.

**"Offline" and "mock" are kept apart** (renamed 2026-11-15): *offline* is a local run whose dashboard services are
stand-ins answering from built-in data, with no AWS access; *mock* means only a mock landscape (`isMockLandscape()`), a
deployed pipeline whose target is the target simulator. The local stand-in for the sign-in edge functions is the
*local sign-in*. An offline run can play a mock landscape (`--landscape mock`).
