# Dashboard Deployment

What a landscape's dashboard stack creates and may touch, how the origin is protected, how Lambda@Edge versions are
cleaned up and why teardown takes three steps, the landscape registry, the dashboard domain and its default-landscape
redirect, and shared sign-in. The rules and commands are in `dashboard/CLAUDE.md` ("CDK Deployment"); DNS, certificate
and context setup for a new landscape are in the fargate `README.md` ("Dashboard DNS and Certificates"). Read this
before changing anything under `dashboard/lib/`, `bin/deploy.sh`, `bin/teardown.sh` or the edge-version scripts.

## The stack

One dashboard stack per landscape, `${DASHBOARD.STACK_ID}-${landscape}` (e.g. `hp-dashboard-mock`), created by
`createDashboardStack()` when the landscape's fargate context (`../context/context.json`) has a `DASHBOARD` section.
That section is this directory's `IContext` minus `ACCOUNT` / `REGION` / `TAGS`, which come from the landscape
(`toDashboardContext()`). Keep the stack name within 25 characters: the Shibboleth construct names a bucket
`<stack name>-context-config-<account>-<region>`.

What it creates:

- Distribution (CloudFront) and Shibboleth Lambda@Edge (separate us-east-1 edge-lambda stack).
- A record `<landscape>.<dashboard domain>` (e.g. `staging.person.huron.cssnprd.warhen.work`) plus its
  `/route53/arecords/<name>` ownership parameter, created by the Shibboleth construct when no record for the name
  exists.
- Default landscape only (`isDefault`): the dashboard domain's redirect (below).
- Lambda Function URL with origin verification (SSM `/cloudfront/<STACK_ID>/<landscape>/origin-verify-secret`).
- The landscape's registry parameter `/hp-dashboard/landscapes/<landscape>` (below), and `ssm:GetParametersByPath` on
  that path for the Lambda.
- Its own two tables (`lib/AccessTables.ts`, [AUTHORIZATION.md](AUTHORIZATION.md)), in public mode too: the access
  table `<STACK_ID>-access-<landscape>` and the audit table `<STACK_ID>-audit-<landscape>`, both destroyed with the
  stack, read and written by the Lambda (`ACCESS_TABLE_NAME`, `AUDIT_TABLE_NAME`). The only DynamoDB tables it writes.

What the Lambda may reach in the landscape, all by deterministic name - no CloudFormation imports from the pipeline
stack:

- No secret of its own: it reads the landscape's integration config secret (`integrationSecretName()` in fargate
  `lib/Secrets.ts`), and has read-only grants on the statistics table, the person history table (DynamoDB mode) and
  the chunks bucket (`lib/LandscapeResources.ts`).
- To start a run (Bulk sync) it may invoke the chunker subscriber (`chunker-subscriber-<landscape>`, which resets the
  chunker's counters for a run's start) and read its and the processor subscriber's configuration (their `DRY_RUN`),
  and read the chunker service's most tasks (its scalable target, for Parallel start:
  `application-autoscaling:DescribeScalableTargets`, with `CHUNKER_CLUSTER_NAME` and `CHUNKER_SERVICE_NAME`). It is
  told the context's processor maximum (`PROCESSOR_MAX_SCALING_CAPACITY`, for Processor tasks) and changes nothing on
  either service.
- In a mock landscape only, it may also look up the source simulator's Function URL (`lambda:GetFunctionUrlConfig` on
  `source-simulator-<landscape>`, named in its `SOURCE_SIMULATOR_FUNCTION_NAME`), and read its last reset's record
  (`dynamodb:GetItem` on the atomic counter table).
- Outside its own tables it writes its landscape's launch flag (`ssm:PutParameter`, `GetParameter`,
  `DeleteParameter` on `/hp-dashboard/launch-flags/<landscape>`; [BULK_SYNC.md](BULK_SYNC.md#the-launch-flag)), and its
  landscape's bulk sync schedule.
- **The bulk sync schedule**, which no stack deploys (fargate `src/schedule/`), is created, changed and deleted from
  the dashboard, so it may `scheduler:CreateSchedule`, `UpdateSchedule` and `DeleteSchedule` on
  `schedule/default/chunker-api-schedule-<landscape>` only. To keep schedules an hour apart (the landscapes share the
  source) it may `scheduler:GetSchedule` on every landscape's (`chunker-api-schedule-*`) and `ListSchedules` (which
  can't be narrowed to a resource). Creating or changing one passes the schedule's role, so it may `iam:PassRole` on
  `chunker-schedule-<landscape>` (which the pipeline stack creates) only to `scheduler.amazonaws.com`. Its environment
  names that role (`SCHEDULE_ROLE_ARN`), the chunker subscriber as the target (`SCHEDULE_TARGET_ARN`) and the context's
  `SCHEDULE` cron (`SCHEDULE_DEFAULT_CRON`). These are the only writes on pipeline resources besides invoking the
  chunker subscriber. It reads the last scheduled start the chunker subscriber skipped (`ssm:GetParameter` on
  `/hp-dashboard/schedule-refusals/<landscape>`, which the subscriber writes).
- Its environment also carries what Bulk sync's review shows: `PIPELINE_STACK_ID`, `ITEMS_PER_CHUNK`, and the config
  secret's name (`lib/FunctionUrl.ts`).

**Context configuration**: the `DASHBOARD` section of `../context/context*.json`; `context/IContext.ts` holds the
dashboard context types (`DashboardSettings`), including the optional `LABEL` (picker label, default: the landscape),
`isDefault` (the default landscape), `LANDSCAPE_TONE` (the top bar's tint, [PAGES.md](PAGES.md)) and `sharedSignIn`
(below).

## Origin verification

CloudFront injects a secret header in every origin request, and the Lambda (`src/handlers/OriginVerification.ts`)
checks it (`X-Origin-Verify`) against the stored secret, answers 403 when it doesn't match, and logs the event. It is
bypassed when `NODE_ENV=development` (local runs).

The secret is `cf-origin-verify-` plus 32 bytes from `crypto.randomBytes`, base64url encoded (`lib/FunctionUrl.ts`),
stored in SSM Parameter Store. A new value is made at every synth, so each dashboard deploy rotates it in the SSM
parameter, the Lambda's `ORIGIN_VERIFY_SECRET` and CloudFront's header together (the deploy's diff shows all three
changing). Until CloudFront has rolled the new header out, requests may briefly get 403.

## Old Lambda@Edge versions

A deploy that changes an edge function (its code, or a `SHIBBOLETH` setting the construct bakes into the bundle's
`edge-context.json`, such as `logoutUrl`) publishes a new version, and logs `DELETE_FAILED (skipped)` for the version
it replaced: Lambda refuses to delete it until CloudFront has removed its replicas, hours later. This is expected; the
deploy succeeds.

`bin/deploy.sh` then runs `scripts/PruneEdgeVersions.ts` after every deploy that includes the dashboard stack. It calls
shibboleth-sp-at-edge's (2.2.0+) `pruneEdgeLambdaVersions`, which deletes versions older than the ones the
distribution uses and never changes the distribution; a version still replicated is left for the next deploy's run.
It skips a dashboard without edge functions (public mode), and never fails the deploy. Don't use the package's
`deleteEdgeLambdaVersions` for this: it is a teardown tool that removes the edge functions from the distribution first.

## Teardown

`teardown` / `teardown-dashboard` run `bin/teardown.sh`, which destroys the dashboard in three steps, because Lambda
won't delete an edge function while CloudFront still holds replicas of it, and CloudFront removes them only hours after
the distribution is deleted:

1. Destroy the dashboard stack, which deletes the distribution.
2. `scripts/DeleteEdgeVersions.ts` (`npm run -s dashboard:delete-edge-versions`) calls the package's
   `deleteEdgeLambdaVersions`, which deletes every version of the three edge functions, retrying each until its
   replicas are gone (up to about two hours per version).
3. Destroy the edge-lambda stack.

Steps 2 and 3 run only for an authenticated dashboard (the only kind with an edge-lambda stack). Step 2 must never run
while the distribution exists: `deleteEdgeLambdaVersions` first removes the edge functions from the distribution, and
the Lambda trusts every request that reaches it to have been signed in by them (`Authenticator.isAuthenticated`), so a
live dashboard would be open to anyone. The script refuses if it finds the landscape's distribution, and the shell
script runs it only after step 1 succeeds. A failed step stops the teardown; rerunning it is safe, since destroying a
stack that's already gone does nothing.

A teardown also deletes the access and audit tables (so the first Admin must be seeded again after the next deploy),
and leaves a launch flag that happens to be up ([BULK_SYNC.md](BULK_SYNC.md#the-launch-flag)).

## Landscape registry

The landscape picker's data is a registry of SSM parameters, one per landscape, under `/hp-dashboard/landscapes`
(`LANDSCAPES_PARAMETER_PATH` in `src/services/LandscapeRegistry.ts`).

- **Registration** (`lib/LandscapeRegistration.ts`, `registerLandscape()`): each dashboard stack owns
  `/hp-dashboard/landscapes/<landscape>`, a String parameter holding JSON `{ landscape, label, url, isDefault }`
  derived at synth from `ORIGIN.subdomain`, `LABEL` and `isDefault`. Deploying registers the landscape; tearing the
  stack down deregisters it. Never edit a stack-owned parameter: change the context and redeploy.
- **One default**: `assertSingleDefaultLandscape()` fails synth when `isDefault` is true and another landscape's
  parameter already has `isDefault: true`. To move the default, set `isDefault` false on the old landscape and deploy
  it, then set it on the new one.
- **Reading** (`LandscapeRegistry`): `GetParametersByPath` once per Lambda instance; a new landscape appears in other
  dashboards after their next cold start. Missing `label` defaults to the landscape, missing `url` to
  `https://<landscape>.<DASHBOARD_DOMAIN>`; unparseable entries are skipped with a warning.
- **Other accounts**: SSM is per account and region, so list a landscape deployed elsewhere by creating a parameter
  under the path by hand, with an explicit `url` (and remove it by hand).

## Dashboard domain and default landscape redirect

The **dashboard domain** (`person.huron.cssnprd.warhen.work`) is the parent of every landscape's hostname,
`ORIGIN.subdomain` = `<landscape>.<dashboard domain>`, inside the account's hosted zone (`DNS.hostedZone`,
`cssnprd.warhen.work`). It has no setting of its own: `lib/DashboardDomain.ts` derives it from `ORIGIN.subdomain`
(minus the first label), and `assertLandscapeHostname()` fails synth unless that first label is the landscape. The
Lambda gets it as `DASHBOARD_DOMAIN`.

- **Redirect** (`lib/DefaultLandscapeRedirect.ts`, added by `createDashboardStack()` when `isDefault`): a CloudFront
  distribution for the dashboard domain whose viewer-request CloudFront Function returns `302` to
  `https://<ORIGIN.subdomain>` with the path and query string (its origin is never reached, so no sign-in), plus the
  domain's alias A record and `/route53/arecords/<dashboard domain>` ownership parameter.
- **Synth checks** (`planDefaultLandscapeRedirect()`, AWS reads injectable as `RedirectLookups`): the certificate
  (`DNS.certificateARN`, us-east-1) must cover the dashboard domain, and the A record is created only when none exists
  or its ownership parameter names this stack, the rule the Shibboleth construct applies; otherwise synth warns and
  leaves the record alone.
- **Moving the default** removes the redirect from the old default's stack first (see "One default" above); the
  dashboard domain doesn't resolve until the new default deploys.

## Shared sign-in

`DASHBOARD.sharedSignIn` (authenticated mode only, default false) lets landscapes share one sign-in.
`createDashboardStack()` passes the context through `applySharedSignIn()` (`lib/SharedSignIn.ts`), which sets
`SHIBBOLETH.cookieDomain` to the dashboard domain and `SHIBBOLETH.cookieName` to `SHARED_SIGN_IN_COOKIE_NAME`
(`hp-dashboard-auth`), before both the dashboard stack and the Shibboleth construct see it. shibboleth-sp-at-edge
(2.1.0+) passes them to the edge functions, which set, clear and read the JWT cookie with them.

- **The Lambda reads the same cookie**: `lib/FunctionUrl.ts` sets `AUTH_COOKIE_NAME` to `SHIBBOLETH.cookieName`
  (default `JwtTools.COOKIE_NAME`), and `Authenticator.user` (`src/services/Authentication.ts`, through `ClaimsReader`,
  which verifies it) reads that cookie. Never read `JwtTools.COOKIE_NAME` directly: a `sharedSignIn` landscape's
  browser may also hold an old host-only cookie under it.
- **Without `sharedSignIn`** the cookie stays host-only under the default name, which must differ from the shared
  name: the browser sends the shared cookie to every landscape under the dashboard domain, and `parseCookieValue` takes
  whichever match comes first.
- **Same key pair**: every `sharedSignIn` landscape must use the same JWT key pair (`SHIBBOLETH.secret`). An isolated
  landscape needs its own pair as well as its own cookie.
- **Synth fails** for `sharedSignIn` without `ORIGIN.subdomain`, with a context that also sets
  `cookieName`/`cookieDomain`, or in public mode. Tests: `test/DashboardSharedSignIn.test.ts`.
- Setting up a landscape: the fargate `README.md`, "Dashboard DNS and Certificates", step 4.

**Sign-out**: the edge functions answer `/logout` themselves, so the Lambda never sees it: they expire the JWT cookie
(with its Domain, so with `sharedSignIn` every landscape is signed out), then redirect to `/logout?target=idp`, which
sends the browser to `SHIBBOLETH.logoutUrl` to end the IdP session. The browser ends on the IdP's page.

## The Shibboleth secret

The edge functions read the landscape's `SHIBBOLETH.secret` (SAML key and certificate, and the JWT key pair) at
runtime; the Lambda doesn't (synth reads only the JWT public key from it, `lib/JwtPublicKey.ts`). The
`DASHBOARD_SHIBBOLETH_SECRET` harness (`src/ShibbolethSecret.ts`, through shibboleth-sp-at-edge's
`SecretsManagerSecret`) reads and creates it:

- `TASK=read`: which values exist; prints only the public ones.
- `TASK=create`: SAML key and certificate copied from `SOURCE_SECRET_ARN` under that secret's own field names, read
  from PEM files, or, with neither, generated with shibboleth-sp's `Keys` - the IdP must then be given the printed
  certificate before sign-in works; a new JWT key pair. It refuses to replace an existing secret, since a new key pair
  ends every sign-in that shares it.
- **Secret names**: a name ending with `/<landscape>` (e.g. `shibsp/hp-dashboard/staging`) is that landscape's own
  secret; any other (e.g. `shibsp/hp-dashboard`) is shared by the landscapes that share a sign-in
  (`secretLandscapeOf()`). `create` tags the secret from the context's `TAGS`, with `Landscape` only on a landscape's
  own secret, describes it accordingly, and creates it in the context's `REGION` (default `us-east-1`). An existing
  secret is read in the region its ARN names, as the edge functions read it.
- The `ShibbolethSecret` class is not part of the Lambda.
