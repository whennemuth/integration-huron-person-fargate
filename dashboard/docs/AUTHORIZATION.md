# Dashboard Authorization

Who may do what in a landscape's dashboard, by role, per landscape (`authorization-plan.md`, delivered in seven steps,
2026-11-17; the finer roles followed on 2026-11-18 as its step 8). This doc covers the gate, how the user is
identified, the role model, role assignments and the audit trail, the Administration pages, and impersonation. The
"decision N" references are to `authorization-plan.md`. The rules an agent must follow are in `dashboard/CLAUDE.md`
("Authorization"); read this before changing anything under `src/authorization/` or `src/audit/`, the gate in
`DashboardHandler`, a route's permissions, or the Administration pages.

## The gate

`RequestGate`, in `DashboardHandler` before routing (decision 7, deny by default):

- Pages and the sign-in routes need only authentication; `GET /api/me` only sign-in; every other `/api/*` route its
  action (`RoutePermissions`).
- An unmapped `/api` route is `403`, and a known route's unsupported method `405` (`Allow`), before any role is asked.
- A denial is `403 { error: "FORBIDDEN", action, roles, message }` ("None of your roles (Viewer) has a policy that
  grants bulk:start."; a user with no role is told to ask an Admin).
- A signed-in user with no role gets every page, and `403` from every `/api` route but `/api/me`.
- Public mode (no sign-in) allows everything but impersonation (`PermitAllAuthorizer`).
- Tests: `test/DashboardAccessGate.test.ts` (each role against each route, through the handler offline).

## Who is asking

The sign-in token's claims, believed only once its signature and expiry check out (`ClaimsReader`,
`VerifiedClaimsReader`: shibboleth-sp's `JwtTools`, RS256) against `JWT_PUBLIC_KEY`, which synth reads from the
landscape's Shibboleth secret (`lib/JwtPublicKey.ts`; synth fails if the secret has none) and gives the Lambda
(authenticated mode only; the Lambda gets no access to the secret). Without the key no one is signed in. It never
takes a token from shibboleth-sp's `IMPERSONATE_*` variables. `Authenticator.user` (`Request.user`: the top bar,
`startedBy`) reads claims the same way. Tests: `test/DashboardClaimsReader.test.ts`.

## GET /api/me

`{ signedIn, user?: { buPrincipal, eppn, mail, displayName }, buPrincipal, roles: [{ name, description, policies:
[{ name, description, actions }] }], actions, unknownRoles, publicMode }`, plus while impersonating the roles in force,
`viewedAs` and `mayImpersonate`. It and a page load record the user's profile (`recordProfile()`; never fails the
request).

## Bulk sync's choices

`BulkSync.choiceActions()`, checked in `plan()` and `start()` with the request's authorizer, after the request is
parsed and before anything else: Start from scratch needs `mock:reset`, and Processor tasks above the landscape's
maximum (any override when the maximum isn't known) `bulk:processors`. A refusal is a `ForbiddenError`, answered with
the same `403`; nothing is started or reviewed. The bulk sync schedule's run is checked the same way
(`BulkSync.scheduleEvent()`), on top of the schedule routes' own `schedule:edit`, which `PipelineControl` grants
beside `bulk:start` (so PipelineOperator, Operator and Admin).

## The model

`src/authorization/`:

- **Actions** (`Actions.ts`).
- **Policies and roles** (`Policy`, `Role`, `StaticRoleCatalog`), from least to most: RunViewer (RunReporting),
  PersonViewer (PersonDataRead), Viewer (both), PersonOperator (Viewer's and PersonDataWrite), PipelineOperator
  (Viewer's and PipelineControl), Operator (all four), Admin (every policy). The finer roles since 2026-11-18
  (`authorization-plan.md` step 8).
- **The person** (`Principal.fromClaims()`, keyed by `buPrincipal`, compared lower-cased; `PrincipalResolver` takes a
  `buPrincipal` or `<id>@bu.edu`).
- **`Authorizer`** (deny by default; a 403's message; `effectivePermissions()` for `/api/me`) and
  **`PermitAllAuthorizer`** (public mode).
- **`RoutePermissions`**: what each `/api/*` route needs, and whether it changes anything; an unmapped `/api` route is
  denied, a known route's unsupported method stays a `405`.
- **`Impersonation`**: an Admin seeing the dashboard through another person's roles (below).
- Tests: `DashboardAuthorizer`, `DashboardRoutePermissions`, `DashboardImpersonation`.

**The services**: `ServiceProvider.getRoleCatalog()` and `getRoleAssignmentStore()` (the access table,
`ACCESS_TABLE_NAME`; offline, in memory, seeded with the local user's `USER_ROLES`; with neither, no one holds a role).

## Role assignments

`RoleAssignmentStore.ts`: `DynamoDbRoleAssignmentStore` over the access table (partition key `buPrincipal`, sort key
`sk`: one item per role held, with `grantedBy`/`grantedAt`, and one `#PROFILE` item per person seen, with what their
sign-in token last said); `setRoles()` replaces a person's roles in one transaction, keeping the original grant of a
role they keep. `InMemoryRoleAssignmentStore` for offline runs and tests. The `DASHBOARD_ROLE_ASSIGNMENTS` harness
reads and changes them from the command line.

**The first Admin** is seeded by hand after a deploy (the tables start empty, and a teardown deletes them): the
`DASHBOARD_ROLE_ASSIGNMENTS` harness or `aws dynamodb put-item` (fargate `README.md`, "Dashboard access").

**Covered roles are pruned** (decision 15; `pruneCoveredRoles()` in `RoleCatalog.ts`): of the roles chosen for a
person, one is dropped when every action it grants is also granted by another chosen role (actions compared, not a
ranking; of two granting the same actions, the later in the catalog is kept). The `PUT` route and the
`DASHBOARD_ROLE_ASSIGNMENTS` harness's `set` and `grant` save the pruned set; the last-Admin safeguard judges it. The
Access page applies the same rule as roles are checked (`AccessPage.coverage()` / `includedIn()`): a covered role shows
checked, disabled and "included in Admin", and unchecking its coverer frees it. Assignments saved before are pruned the
next time they are saved.

## The audit trail

**The store** (`src/audit/AuditStore.ts`): `DynamoDbAuditStore` over the audit table (every record in partition `pk` =
`AUDIT`, sort key `sk` = `<ISO time>#<id>`, so a Query lists newest first; `principal-index` for one person's), paged
with cursors (`AuditError` `INVALID_CURSOR` / `INVALID_LIMIT` / `INVALID_FILTER`); a public dashboard's records name
`anonymous`. An id starts with a sequence number of the process, so records written in the same millisecond by one
instance keep their order. `InMemoryAuditStore` for offline runs and tests (`ServiceProvider.getAuditStore()`). Never
deleted from.

**What is recorded** (`src/audit/AuditTrail.ts`, decision 9; `AuditTrail` is bound to one request's user and
landscape, and never fails the request):

- every denial of an action, at the gate or of a Bulk sync choice (its reason as the detail);
- once the route has answered, every allowed `person:lookup`, `person:preview`, `person:sync`, `bulk:start` (a start,
  not a review), `mock:reset` and `access:admin` request (`auditsAllowed()`), `SUCCEEDED` for a 2xx and else `FAILED`
  with the status. `access:admin` only for changes, since 2026-11-17: its reads were two records per Access page load.
  A role change's detail is the roles saved and what was dropped (`AuditTrail.note()`).
- `describeRequest()` takes the target (a BUID, a person, a test's BUID) and a short detail (a start's choices,
  Processor tasks among them) from the route and body.
- `runs:read` and `audit:read` aren't recorded, nor are a `405`, an unmapped route or `/api/me`.
- Tests: `test/DashboardAuditTrail.test.ts`, `test/DashboardAuditStore.test.ts`.

**`GET /api/audit?cursor&limit&principal&action`** (`audit:read`): `{ items, cursor? }`, newest first, at most 100 a
page (default 50), optionally one person's (a `buPrincipal`, `<id>@bu.edu` or `anonymous`) or one action's; a bad query
is `400 { error, code }`.

## The Access routes

`src/handlers/AccessRoutes.ts`, `access:admin`:

- `GET /api/access/roles` (the catalog's roles, policies and actions), `GET /api/access/assignments` (everyone with a
  role, with profiles), `GET`, `PUT { roles }` and `DELETE /api/access/assignments/{person}` (a `buPrincipal` or
  `<id>@bu.edu`; `PUT` replaces the roles, granted by the Admin who saved them, less any role another of them covers,
  answering those as `pruned`).
- Refusals are `{ error, code }`: `400 INVALID_PRINCIPAL | INVALID_REQUEST | INVALID_ROLE`, and `409 LAST_ADMIN` for a
  change that would leave no Admin (decision 11; checked against the store as it is, so two Admins removing each other
  at the same moment could both pass).
- Tests: `test/DashboardAccessRoutes.test.ts`.

**Giving roles to several people** (since 2026-11-19): `POST /api/access/assignments { principals, roles }` gives the
roles, less the covered ones, to each person listed (each once, at most `MAX_BULK_PRINCIPALS` = 100, a read and a
write each within the Lambda's 30 seconds) **who holds no role**.

- Anyone who already holds a role keeps exactly what they hold, so a bulk grant never overwrites a role given one
  person at a time; the answer `{ roles, pruned, granted, unchanged: [{ buPrincipal, roles }] }` lists them, and the
  page says so gently ("left as they are ... look each one up above").
- Nothing is saved unless every principal is valid (`400 INVALID_PRINCIPAL` with `invalid`); also
  `400 TOO_MANY_PRINCIPALS`. It only adds, so the last-Admin safeguard has nothing to guard.
- Audited as a record per person given the roles (`roles=Viewer (bulk)`) and the request's own record (target
  `N people`, detail the roles, how many were granted and who was left as they were).
- On the Access page, below the table of role holders, everyone's `buPrincipal` is listed comma-delimited in small
  print (one click selects it all; Copy writes it to the clipboard, or selects it where the clipboard can't be
  written), to paste into the form "Give roles to several people" in another landscape. That form has its own role
  checkboxes, with the same coverage as one person's (`AccessPage.choose()` / `applyCoverage()` take the form's
  container and chosen set).

## The Administration pages

Sidebar group Administration, shown to everyone; `public/js/administration.js`, `templates/partials/access-tab.mustache`
and `audit-tab.mustache`:

- **Access** (`AccessPage`): two ways of changing roles as tabs, one shown at a time (`showMode()`,
  `AccessPage.MODES`; underlined like a run's sections): **A person's roles**, the default (look a person up, their
  profile and a checkbox per role, Save, asking first when an Admin removes their own Admin role), and **Give roles to
  several people** (above); below them, everyone with a role, a row opening that person in the first tab.
- **Audit trail** (`AuditTrailPage`): a `PagedTable` over `/api/audit`, filtered by person and action; the action list
  comes from `ACTIONS`, as the template's `auditActions`.
- Tests: `test/DashboardAdministrationPages.test.ts`.

**Who am I** (`WhoAmI`, decision 10): the top bar's user is a button opening a dialog with the user's identity, roles,
policies and actions, read from `/api/me` when it opens; a signed-in user with no role also gets a notice above the
pages (`data-no-role-notice`). Neither exists in public mode.

**A refused load is redacted** (decision 7): every section that loads content to display reports a failure through
`PageState.failed()` / `failedElement()` (`page-state.js`), which shows a `403` as the redacted state naming the action
the roles lack (`fetchSectionData()` carries the 403's `action`), and anything else as the error state. Overview's Mock
landscape section stays hidden when its read is refused. An action the user triggers (lookup, sync, review, start,
save) shows the server's message in the error pattern.

## Impersonation

Decision 14: an Admin sees the dashboard through another person's roles, for viewing only.

- **The top bar**: an Impersonate button, the one control hidden by role (only for a user whose own roles grant
  `access:impersonate`), opens a dialog: a person (typed, or picked from everyone with a role), their roles, then
  Impersonate. While impersonating, the button is **Stop impersonating <person>** (their name if they have signed in
  here, else their `buPrincipal`), and the navigator's tint is the `impersonating` tone ("Impersonating amy: viewing
  only", `IMPERSONATING_TONE` in `NavigationBar.ts`). The who-am-I dialog shows both people. `ImpersonateControl`
  (`administration.js`) starts and stops it, then reloads the page.
- **The routes**: `POST /api/impersonation { principal }` (the user's own `access:impersonate`; not yourself) sets the
  `hp-dashboard-impersonate` cookie (host-only, `HttpOnly`, `Secure`, `SameSite=Strict`, a session cookie); starting
  another replaces it. `DELETE /api/impersonation` (sign-in only) clears it. Both are audited.
- **The gate** (`RequestGate`, through `Impersonation.resolve()`): on every request, a cookie naming someone is honored
  only while the signed-in user's own roles grant `access:impersonate`; otherwise it is ignored, cleared (`Set-Cookie`)
  and audited as a denial. Honored, the person's roles decide (`RequestAccess.authorizer`; the user's own are
  `userAuthorizer`), and a request that changes something is refused `403 { code: "IMPERSONATING", message: "Stop
  impersonating to do this." }` even when the person's roles allow it, so nothing is done in their name. A run's
  review changes nothing, so it is allowed.
- **Who did it**: audit records name the user as `buPrincipal` and the person as `viewedAs`; `startedBy` and profiles
  are always the user's. `/api/me` gives the roles in force, `viewedAs` and `mayImpersonate`.
- Tests: `test/DashboardImpersonation.test.ts` (the model), `test/DashboardImpersonationGate.test.ts` (through the
  handler: valid, revoked and forged cookies, each changing route, the audit, the top bar by role).
