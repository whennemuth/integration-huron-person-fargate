# Mock Landscape Integration Testing: Hash Storage State

This runbook verifies, end to end in the **mock landscape**, that person syncs leave the expected
state behind in hash storage. It covers both kinds of sync that write that state:

- **Manual syncs** - `SyncPerson.ts` and `SyncPersonBatch.ts` in integration-huron-person, run
  locally against the source and target simulators.
- **Pipeline syncs** - this project's chunker -> processor -> post-processor pipeline, started
  with `src/runner/Runner.ts`.

It is written for both storage modes:

| | DynamoDB mode | S3 mode |
|---|---|---|
| Baseline (one hash per person) | PersonCurrentState table | `s3://<chunks bucket>/delta-storage/previous-input.ndjson` |
| Audit trail | PersonHistory table | none |
| Soft-delete marker | `DELETED:` hash prefix + `deletedAt` in PersonCurrentState, `DELETED` history record | `DELETED:` hash prefix in the baseline file |
| Per-record classification | NEW / UPDATED / UNCHANGED (UNCHANGED not written) | none - the baseline file is rewritten only if something changed |

Run Part 1 (DynamoDB) and/or Part 2 (S3). Part 0 applies to both.

---

## Part 0: Prerequisites and conventions

### What must already be deployed

- The mock stack (`./context/swap.sh mock`, then `cdk deploy`), with the target simulator and the
  source simulator (`LAMBDA.sourceSimulator.enabled`). See "Mock Landscapes" and "Source
  Simulator" in this project's `CLAUDE.md`.
- integration-huron-person built with an integration-core that includes
  `DynamoDBDeltaStorage.markDeleted()` and the `DELETED:` hash helpers.

### Starting state

The expected results below assume empty state at the start of a part: no persons in the mock
target table (`huron-person-fargate-processor-mock-target-person-mock`), an empty statistics table,
and an empty baseline - empty PersonCurrentState and PersonHistory tables in DynamoDB mode, or no
`delta-storage/previous-input.ndjson` in the mock chunks bucket in S3 mode.

A Runner run with `RUNNER_MOCK_TARGET_RESET_STATE=true` resets exactly that, for the storage mode
the stack is deployed in (`MockLandscapeRunnerDecorator`), before starting its own run. To start a
part without a pipeline run, truncate the tables in the console and delete the baseline file.

Each part also uses its own BUID range, so that its "first sync" steps really are creates even if
the other part ran since the last reset:

| Part | BUIDs |
|---|---|
| Part 1 (DynamoDB) | `U0000042` - `U0000049`, plus `U0999999` |
| Part 2 (S3) | `U0000142` - `U0000149`, plus `U0999998` |

Part 2's BUIDs are Part 1's plus 100, so each S3 step uses the same person number as its DynamoDB
counterpart. `U0000143` is unused: 1.3's "no stored hash" case has no S3 equivalent.

The source simulator returns the same person for a BUID on every request (`/person?buid=`), as long
as its index is below the simulator Lambda's `MOCK_TOTAL_POPULATION` (`U0000042` is index 41).

### Configure integration-huron-person for the mock landscape

From `integration-huron-person/config`, make the mock landscape active (copies
`config.mock.json` to `config.json` and `mock.env` to `../.env`):

```bash
./swap.sh mock
```

Then set the simulator endpoints in integration-huron-person's `.env`. They are the same for every
step, so the steps' tables don't repeat them:

| Variable | Value | Why |
|---|---|---|
| `DATATARGET_ENDPOINTCONFIG_BASE_URL` | the target simulator URL (stack output `TargetSimulatorUrl`) | environment values override the config file - this must not point at a real Huron API |
| `DATATARGET_ENDPOINTCONFIG_EXTERNAL_TOKEN` | the target simulator token (below) | the simulator only issues JWTs for its own token |
| `DATASOURCE_ENDPOINTCONFIG_PERSON_BASE_URL` | the source simulator Function URL | |
| `DATASOURCE_ENDPOINTCONFIG_PERSON_PATH` | `/person` | any other path is routed to the depleting people endpoint, which ignores `buid` |
| `DATASOURCE_ENDPOINTCONFIG_PERSON_API_KEY` | the mock config secret's `dataSource.person.endpointConfig.apiKey` | the simulator accepts any of the secret's three source keys (person, people, terms) and nothing else; `config.mock.json`'s key is not one of them |
| `DATASOURCE_ENDPOINTCONFIG_CURRENT_TERMS_BASE_URL` | the source simulator Function URL | pipeline runs against the simulated source use simulated terms; hashes only match if manual runs do too |
| `DATASOURCE_ENDPOINTCONFIG_CURRENT_TERMS_PATH` | `/terms` | |
| `DATASOURCE_ENDPOINTCONFIG_CURRENT_TERMS_API_KEY` | the mock config secret's `dataSource.terms.endpointConfig.apiKey` | |

The target simulator token is a 48-character value in its own secret. Do not copy the
`externalToken` from `mock.env`: that is a real Huron token and the simulator rejects it with `401`.

```bash
aws secretsmanager get-secret-value --region us-east-2 --secret-id huron-person-fargate-processor/target-simulator/token/mock --query SecretString --output text
```

```bash
aws secretsmanager get-secret-value --region us-east-2 --secret-id huron-person-fargate-processor/integration/_config/mock --query SecretString --output text | jq '{personKey: .dataSource.person.endpointConfig.apiKey, termsKey: .dataSource.terms.endpointConfig.apiKey}'
```

### Settings tables

Every step that runs something has a settings table. It lists the variables the step's code reads
that drive a decision - for the program run (`SyncPerson.ts`, `SyncPersonBatch.ts` or `Runner.ts`),
the source simulator, and the deployed stack - with the value to use. Variables a step's code
doesn't read are left out: for example `INTEGRATED_DELTA_CLIENT_ID` and `DELTA_STORAGE_BUCKET` are
only read in S3 mode, and `RUNNER_MESSAGES_TO_PREPOPULATE` is ignored when
`RUNNER_SINGLE_PERSON_BUID` is set.

- <span style="color:gray">Grayed rows</span> have the same value as in the previous table for the
  same program (a program's first table shows every row bold).
- **Bold rows** are new or changed - these are the ones to set.
- Renderers that don't show colored text (e.g. GitHub) show grayed rows as plain text; the bolding
  still marks what changed.

These are read too, but are the same for every step that runs the program, so they are set once
here instead of being repeated in the tables:

| Variable (integration-huron-person `.env`) | Value |
|---|---|
| `DATASOURCE_ENDPOINTCONFIG_*`, `DATATARGET_ENDPOINTCONFIG_*` | as above |
| `SYNC_PERSON_SYNC_CRUD` | unset - the operation comes from the target lookup |
| `SYNC_PERSON_DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME`, `SYNC_PERSON_DYNAMODB_PERSON_HISTORY_TABLE_NAME`, and their `SYNC_PERSON_BATCH_` equivalents | unset - in DynamoDB mode `config.json` names the mock tables; in S3 mode they aren't used |
| `SYNC_PERSON_REGION`, `SYNC_PERSON_BATCH_REGION` | `us-east-2` (or unprefixed `REGION=us-east-2`) |

| Variable (fargate `.env`, for `Runner.ts`) | Value |
|---|---|
| `RUNNER_REGION` | `us-east-2` |
| `RUNNER_STACK_ID` | `huron-person-fargate-processor` |
| `RUNNER_CHUNKER_QUEUE_URL` | `<mock chunker queue URL>` |
| `RUNNER_SECRET_ARN` | `<mock config secret ARN>` |
| `RUNNER_DATASOURCE_ENDPOINTCONFIG_ITERATION_LIMIT` | `10` |
| `RUNNER_PERSON_RECORD_PROCESSOR_ADDONS` | unset |

**"unset" means unset everywhere.** The harnesses read prefixed variables through `TestEnvironment`,
which uses the prefixed variable only if it is non-empty and otherwise falls back to the
**unprefixed** one - so an empty `SYNC_PERSON_SYNC_CRUD` still picks up an unprefixed `SYNC_CRUD`
(`mock.env` has unprefixed `SYNC_BUID`, `SYNC_CRUD`, `SYNC_PREVIEW`, `SYNC_UPDATE_HASH`, `DRY_RUN`
and `REGION`). For "unset", remove or comment out both; for `true`/`false`, setting the prefixed
variable is enough. A few variables are only read unprefixed - the tables name them as such.

### How to run things

Manual syncs run from `integration-huron-person`, configured through `.env`:

```bash
npx ts-node src/SyncPerson.ts
```

```bash
npx ts-node src/SyncPersonBatch.ts
```

Pipeline syncs run from this project, configured through its `.env`. With a mock landscape the
Runner verifies the mock stack is deployed before sending anything:

```bash
npx ts-node src/runner/Runner.ts
```

### What the manual sync settings do

| Variable | Effect |
|---|---|
| `SYNC_UPDATE_HASH` | `true`: compute hashes, check whether a person is already in sync, and record their hash (and deactivations) in hash storage. `false`: push without an in-sync check and leave hash storage alone |
| `SYNC_TRUST_PREVIOUS_STORAGE` | `true`: decide whether a person is in sync from their stored hash, falling back to comparing with the target. DynamoDB mode only; requires `SYNC_UPDATE_HASH=true` (a warning is logged otherwise) |
| `SYNC_PREVIEW` | `true`: no pushes or deactivations, and no hash storage writes |
| `DRY_RUN` | `true`: log pushes and hash storage writes instead of performing them |
| `SYNC_CRUD` | supplies the CRUD operation instead of deriving it from the target lookup; leave unset |
| `PREVIOUS_STORAGE_TYPE`, `DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME`, `DYNAMODB_PERSON_HISTORY_TABLE_NAME`, `REGION` | storage selection; take precedence over the config file |
| `INTEGRATED_DELTA_CLIENT_ID` | S3/file mode only: the baseline path. `delta-storage` is the pipeline's baseline |
| `DELTA_STORAGE_BUCKET` | S3 mode only: overrides the configured bucket |

---

## Part 1: DynamoDB mode

The mock stack's default (`PREVIOUS_STORAGE_TYPE: "dynamodb"` in `context/context.mock.json`).
`config.mock.json` names the mock tables, so manual syncs need no storage variables.

### Inspecting state

```bash
aws dynamodb get-item --region us-east-2 --table-name huron-person-fargate-processor-person-current-state-mock --key '{"personId":{"S":"U0000042"}}'
```

```bash
aws dynamodb query --region us-east-2 --table-name huron-person-fargate-processor-person-history-mock --key-condition-expression "personId = :p" --expression-attribute-values '{":p":{"S":"U0000042"}}'
```

Every manual run logs the `syncRunId` it stamps on what it writes:
`Using DynamoDB delta storage: {..."syncRunId":"<ISO timestamp>"}`.

### 1.1 Smoke test (no writes)

Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| **huron-person `.env`** | **`HURON_PERSON_CONFIG_PATH`** | **unset (uses `config/config.json`)** |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000042`** |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_PREVIEW`** | **`true`** |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_UPDATE_HASH`** | **`false`** |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`** | **`false`** |
| **huron-person `.env`** | **`SYNC_PERSON_DRY_RUN`** | **`false`** |
| **huron-person `.env`** | **`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`** | **unset** |
| **simulator Lambda env** | **`MOCK_TOTAL_POPULATION`** | **deployed value, at least `150` (default `10000`)** |

Expect:
- `Found U0000042 in source`, then a target lookup with no `401`.
- No change to any table.

### 1.2 First sync, then an unchanged re-run

Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_BUID`</span> | <span style="color:gray">`U0000042`</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_PREVIEW`** | **`false`** |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_UPDATE_HASH`** | **`true`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect:
- A create in the target simulator (the person appears in the mock target table).
- `DynamoDBDeltaStorage: 1 NEW, 0 UPDATED, 0 UNCHANGED (not written) of 1 record(s)`.
- One PersonCurrentState item (`hash`, the logged `syncRunId`) and one `NEW` PersonHistory item.

Run it again, with the same settings. Expect:
- `Source and target are already in sync for BUID: U0000042.` (the target comparison) and
  `No push for U0000042: source and target already in sync`.
- `DynamoDBDeltaStorage: 0 NEW, 0 UPDATED, 1 UNCHANGED (not written) of 1 record(s)`: after a
  target comparison, the hash storage step still runs, to create or repair a missing or stale
  stored record.
- No change to any table (UNCHANGED records are not written, so the first run's `syncRunId` stays).

"No push", here and below, means no request that changes the target - no `POST` or `PATCH`. The
target is still read (looked up by `sourceIdentifier`) to resolve the person's HRN and the CRUD
operation.

### 1.3 Stored-hash in-sync check

The stored hash is only consulted when the run uses hash storage (`SYNC_UPDATE_HASH=true`).

**Stored hash present.** Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_BUID`</span> | <span style="color:gray">`U0000042`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`** | **`true`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect:
- `Source matches the hash in storage for BUID: U0000042 - already in sync, hash storage left as is.`
- `No push for U0000042: source and target already in sync`.
- No `DynamoDBDeltaStorage: ...` line: the stored hash already matches, so the hash storage step is
  skipped.
- No change to any table.

The two ways of deciding "in sync" are told apart by their first line:

| Path | Line | Then |
|---|---|---|
| Stored hash (trust `true`, DynamoDB mode, a usable stored hash) | `Source matches the hash in storage for BUID: ...` | no hash storage step |
| Target comparison (anything else) | `Source and target are already in sync for BUID: ...` | `DynamoDBDeltaStorage: ... 1 UNCHANGED ...` |

`Checking if source and target are already in sync...` precedes both, so it doesn't distinguish
them. If you see the target comparison line with trust set, check that
`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE` really is `true` in `.env`.

**No stored hash.**

1. Create `U0000043` in the target without writing hash storage. Run `SyncPerson.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000043`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_UPDATE_HASH`** | **`false`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`true`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
   | <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

   Expect the warning
   `SYNC_TRUST_PREVIOUS_STORAGE=true has no effect without SYNC_UPDATE_HASH=true...`, a create in
   the target, and no items for `U0000043`.
2. Run `SyncPerson.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_BUID`</span> | <span style="color:gray">`U0000043`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_UPDATE_HASH`** | **`true`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`true`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
   | <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

   Expect `No hash in storage for BUID: U0000043 - comparing with target instead.`, the target
   comparison reporting in sync, and `1 NEW` (the person's first state and history records).

### 1.4 Hash parity with the pipeline

1. Run `Runner.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | **fargate `context/context.json` (deployed)** | **`PREVIOUS_STORAGE_TYPE`** | **`dynamodb`** |
   | **fargate `.env`** | **`LANDSCAPE` (unprefixed: the Runner has no prefixed version)** | **`mock`** |
   | **fargate `.env`** | **`RUNNER_HURON_PERSON_CONFIG_PATH`** | **unset** |
   | **fargate `.env`** | **`RUNNER_SINGLE_PERSON_BUID`** | **`U0000044`** |
   | **fargate `.env`** | **`RUNNER_POPULATION_TYPE`** | **`person-delta`** |
   | **fargate `.env`** | **`RUNNER_SOURCE_SIMULATOR`** | **`true`** |
   | **fargate `.env`** | **`RUNNER_SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`** | **unset (a value would be written to the simulator Lambda)** |
   | **fargate `.env`** | **`RUNNER_SOURCE_SIMULATOR_MOCK_SIMULATED_DELAY_SECONDS`** | **unset** |
   | **fargate `.env`** | **`RUNNER_SOURCE_SIMULATOR_MOCK_ERROR_RATE`** | **unset** |
   | **fargate `.env`** | **`RUNNER_MOCK_TARGET_RESET_STATE`** | **`false`** |
   | **fargate `.env`** | **`RUNNER_MESSAGING_ONLY`** | **`false`** |
   | **fargate `.env`** | **`RUNNER_CHUNKING_ONLY`** | **`false`** |
   | **fargate `.env`** | **`RUNNER_BULK_RESET`** | **`false`** |
   | **fargate `.env`** | **`RUNNER_TRUST_PREVIOUS_STORAGE`** | **`false`** |
   | **fargate `.env`** | **`SKIP_ATOMIC_COUNTER_RESET` (unprefixed)** | **`false`** |
   | **simulator Lambda env** | **`MOCK_TOTAL_POPULATION`** | **deployed value, at least `150` (default `10000`)** |

   `RUNNER_POPULATION_TYPE` matters: unset, it defaults to `person-full`, and the post-processor
   then treats every other person in PersonCurrentState as removed and deactivates them. With
   `RUNNER_DATASOURCE_ENDPOINTCONFIG_ITERATION_LIMIT` above `0`, the first chunker task sends one
   more chunking message before fetching (`Sending message to chunker queue`); the task it starts
   finds the end of records already reached and ends with `⊘ Cancelling.` - one extra task, not a
   chain. (`0` would avoid chaining altogether.)
2. Wait for the post-processor to finish. Expect a PersonCurrentState item and a `NEW` history
   item for `U0000044` carrying the pipeline run's `syncRunId`, and a `METADATA` record for that
   `syncRunId` in the statistics table.
3. Run `SyncPerson.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000044`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`** | **`false`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
   | <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

   Expect `1 UNCHANGED` and no new history item: the manual sync computed exactly the hash the
   pipeline stored. An `UPDATED` here means the two hash differently - check, in order, that terms
   come from the simulator (Part 0), the integration config's `preLoadedMaps`, and the chunker's
   `dataSource.people.fieldsOfInterest` trimming.

Note: `syncRunId`s written by manual syncs have no `METADATA`/`FLAGS` record in the statistics
table; only pipeline runs write those. (Error event records use their own timestamps, so look for
`METADATA`, not just any record.)

### 1.5 Deactivation and reactivation

1. Lower the source simulator Lambda's `MOCK_TOTAL_POPULATION` in the console. Change only that
   variable - `aws lambda update-function-configuration` replaces the whole environment. Then run
   `SyncPerson.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000042`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
   | **simulator Lambda env** | **`MOCK_TOTAL_POPULATION`** | **`40` (below `U0000042`'s index, 41)** |

   Expect:
   - `Did not find U0000042 in source`, then `Deactivating U0000042 in target system...`.
   - `DynamoDBDeltaStorage: marking 1 of 1 person(s) as DELETED` and
     `Marked U0000042 as deleted in hash storage`.
   - PersonCurrentState: `hash` = `DELETED:<previous hash>`, a `deletedAt`, this run's `syncRunId`.
   - PersonHistory: a `DELETED` item with `hash` and `previousHash` both the unmarked previous hash
     (history never carries the marker in `hash`; this matches what the post-processor writes).
2. Run it again, with the same settings. Expect
   `No-op: need to deactivate U0000042 in target system, but already inactive.` and
   `No-op: U0000042 has no hash storage record to mark as deleted, or is already marked`; no table
   changes.
3. Restore `MOCK_TOTAL_POPULATION`, then run `SyncPerson.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_BUID`</span> | <span style="color:gray">`U0000042`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
   | **simulator Lambda env** | **`MOCK_TOTAL_POPULATION`** | **deployed value, at least `150` (default `10000`)** |

   Expect the person to be reactivated in the target, PersonCurrentState to hold the unmarked hash
   again, and an `UPDATED` history item whose `previousHash` is `DELETED:<hash>`.

**Storage-only variant** (no simulator change): put a PersonCurrentState item for a BUID beyond the
population, then sync it. The person is absent from both source and target, so only the storage
marking is exercised.

```bash
aws dynamodb put-item --region us-east-2 --table-name huron-person-fargate-processor-person-current-state-mock --item '{"personId":{"S":"U0999999"},"hash":{"S":"abc"},"syncRunId":{"S":"seed"}}'
```

Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0999999`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect the item's hash to become `DELETED:abc` with a `deletedAt`, and a `DELETED` history item with
`hash` and `previousHash` both `abc`.

### 1.6 Batch: per-person writes

Run `SyncPersonBatch.ts` with:

| Where | Variable | Value |
|---|---|---|
| **huron-person `.env`** | **`HURON_PERSON_CONFIG_PATH`** | **unset (uses `config/config.json`)** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_SYNC_BUIDS`** | **`U0000045,U0000046,U0000047`** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_SYNC_BUIDS_FILE_PATH`** | **unset (it would take precedence over `SYNC_BUIDS`)** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_SYNC_PREVIEW`** | **`false`** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_SYNC_UPDATE_HASH`** | **`true`** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_SYNC_TRUST_PREVIOUS_STORAGE`** | **`false`** |
| **huron-person `.env`** | **`DRY_RUN` (unprefixed: the batch harness has no prefixed version)** | **`false`** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_PREVIOUS_STORAGE_TYPE`** | **unset** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_OUTPUT_FILE_PATH`** | **unset (logs to `data/sync_person_batch_output.json`)** |
| **simulator Lambda env** | **`MOCK_TOTAL_POPULATION`** | **deployed value, at least `150` (default `10000`)** |

1. Interrupt it (Ctrl+C) after the first `Hash storage checked for person U0000045` line. Expect
   `U0000045` to already have its PersonCurrentState and history items: in DynamoDB mode each
   person's state is written right after their push, not at the end of the batch.
2. Run it again to completion, with the same settings. Expect `U0000045` to be `UNCHANGED` (keeping
   its first `syncRunId`), and `U0000046` and `U0000047` to be `NEW` with this run's shared
   `syncRunId`.

### 1.7 Dry run

Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000048`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
| **huron-person `.env`** | **`SYNC_PERSON_DRY_RUN`** | **`true`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">unset</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect `[DRY RUN]` log lines, no create in the target and no items for `U0000048`.

---

## Part 2: S3 mode

In S3 mode the baseline is the single file
`s3://huron-person-chunks-mock/delta-storage/previous-input.ndjson` - one line per person with
their key and hash. The bucket is the landscape's chunks bucket (`${S3.chunksBucket}-<landscape>`),
and the directory is `delta-storage` unless the integration config's S3 storage sets a `keyPrefix`
(which CDK passes to the tasks as `SHARED_DELTA_STORAGE_DIR`). There is no history and no
NEW/UPDATED/UNCHANGED classification; the file is rewritten only if something changed. Run metadata
and flags live as `_metadata.json` / `_flags.json` in each run's chunk directory.

### Switching to S3 mode

**Manual syncs** need no redeployment. In integration-huron-person, add
`"bucketName": "huron-person-chunks-mock"` to `storage.config` in the active `config/config.json`
(not `config.mock.json`, which `swap.sh` overwrites `config.json` from). It is ignored in DynamoDB
mode. The rest is in the steps' tables: `SYNC_PERSON_PREVIOUS_STORAGE_TYPE=s3` overrides
`storage.type` (which stays `dynamodb` in the file, so switching back is just unsetting it -
environment variables can set the storage type, table names and region, but not `bucketName`), and
`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID=delta-storage` makes manual syncs read and write the same
baseline file as the pipeline. (A scratch path such as `delta-storage/sync-person-regression` tests
manual syncs in isolation, but step 2.4 needs `delta-storage`.)

**The pipeline** takes its storage mode from the deployment, so step 2.4 needs the mock stack in
S3 mode: with the mock context active (`./context/swap.sh mock`), set
`"PREVIOUS_STORAGE_TYPE": "s3"` in `context/context.json` and update the stack (`cdk deploy`). That
value alone switches the ECS tasks and the config secret's storage block (`resolveSecretStorage()`
in `lib/Secrets.ts` derives it, with the chunks bucket as `bucketName`); the integration config
files need no change.

> **Warning:** the PersonCurrentState and PersonHistory tables only exist in DynamoDB mode, with
> `RemovalPolicy.DESTROY`. Deploying in S3 mode deletes them and their contents; switching back
> recreates them empty. Switching never touches an S3 baseline left by an earlier S3-mode period;
> the Runner's mock reset deletes it.

Do not run manual syncs against `delta-storage` while a pipeline run is in progress: both rewrite
the whole baseline file, and the last writer wins.

### Inspecting state

```bash
aws s3 cp s3://huron-person-chunks-mock/delta-storage/previous-input.ndjson - | grep U0000142
```

```bash
aws s3 ls s3://huron-person-chunks-mock/delta-storage/
```

### 2.1 Smoke test (no writes)

Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000142`** |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_PREVIEW`** | **`true`** |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_UPDATE_HASH`** | **`false`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
| **huron-person `.env`** | **`SYNC_PERSON_DRY_RUN`** | **`false`** |
| **huron-person `.env`** | **`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`** | **`s3`** |
| **huron-person `.env`** | **`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID`** | **`delta-storage`** |
| **huron-person `.env`** | **`SYNC_PERSON_DELTA_STORAGE_BUCKET`** | **unset** |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect `Found U0000142 in source`, a target lookup with no `401`, and the baseline file unchanged
(or still absent).

### 2.2 First sync, then an unchanged re-run

Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_BUID`</span> | <span style="color:gray">`U0000142`</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_PREVIEW`** | **`false`** |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_UPDATE_HASH`** | **`true`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">`s3`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID`</span> | <span style="color:gray">`delta-storage`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DELTA_STORAGE_BUCKET`</span> | <span style="color:gray">unset</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect:
- A create in the target, then `Hash storage updated successfully for person U0000142`.
- The baseline file gains a line for `U0000142` with its hash (the file is created if absent);
  other lines are unchanged.
- No writes to the statistics table, and none to PersonCurrentState or PersonHistory (S3 mode has
  neither table). The only DynamoDB write is the target simulator storing the created person in the
  mock target table (`huron-person-fargate-processor-mock-target-person-mock`).

Run it again, with the same settings. Expect
`Source and target are already in sync for BUID: U0000142.`,
`No push for U0000142: source and target already in sync`, then
`Hash storage unchanged for person U0000142 - baseline not rewritten`: the hash storage step reads
the baseline, finds the stored hash current, and leaves the file untouched (its `LastModified` time
doesn't change).

### 2.3 SyncPerson ignores the stored hash in S3 mode

Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_BUID`</span> | <span style="color:gray">`U0000142`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`** | **`true`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">`s3`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID`</span> | <span style="color:gray">`delta-storage`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DELTA_STORAGE_BUCKET`</span> | <span style="color:gray">unset</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect the target comparison (`Source and target are already in sync...`), not `Source matches the
hash in storage`, then `Hash storage unchanged for person U0000142 - baseline not rewritten`: in S3
mode manual syncs don't consult `previous-input.ndjson` to decide whether a person is in sync,
because reading one person's hash means reading the whole baseline. (The hash storage step does
read it afterwards, to decide whether to rewrite it.)

Pipeline runs are different: with `trustPreviousStorage` on (and bulk reset off), S3-mode
processors do compare against the hashes in `previous-input.ndjson`, reading it once per chunk
rather than once per person.

### 2.4 Hash parity with the pipeline

**Before this step**, switch the mock stack to S3 mode if you haven't: with the mock context active
(`./context/swap.sh mock`), set `"PREVIOUS_STORAGE_TYPE": "s3"` in `context/context.json` and
`cdk deploy` (with an image that includes the S3-mode `_metadata.json` totals fix). This deletes
the PersonCurrentState and PersonHistory tables - see "Switching to S3 mode".

1. Run `Runner.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | **fargate `context/context.json` (deployed)** | **`PREVIOUS_STORAGE_TYPE`** | **`s3`** |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`LANDSCAPE` (unprefixed: the Runner has no prefixed version)</span> | <span style="color:gray">`mock`</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset</span> |
   | **fargate `.env`** | **`RUNNER_SINGLE_PERSON_BUID`** | **`U0000144`** |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_POPULATION_TYPE`</span> | <span style="color:gray">`person-delta`</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_SOURCE_SIMULATOR`</span> | <span style="color:gray">`true`</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">unset (a value would be written to the simulator Lambda)</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_SOURCE_SIMULATOR_MOCK_SIMULATED_DELAY_SECONDS`</span> | <span style="color:gray">unset</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_SOURCE_SIMULATOR_MOCK_ERROR_RATE`</span> | <span style="color:gray">unset</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_MOCK_TARGET_RESET_STATE`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_MESSAGING_ONLY`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_CHUNKING_ONLY`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_BULK_RESET`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`RUNNER_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">fargate `.env`</span> | <span style="color:gray">`SKIP_ATOMIC_COUNTER_RESET` (unprefixed)</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

   As in 1.4, the first chunker task chains one more message, and the task it starts ends with
   `⊘ Cancelling.` - in S3 mode this relies on `_metadata.json` carrying the end-of-records flag
   (`partialOrEmptyChunkEncountered: true`) after the first task finishes. A chunker that keeps
   chaining tasks for the same person means the deployed image predates that fix.
2. After the post-processor finishes, expect a `U0000144` line in
   `delta-storage/previous-input.ndjson`. Note its hash.
3. Run `SyncPerson.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000144`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`** | **`false`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">`s3`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID`</span> | <span style="color:gray">`delta-storage`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DELTA_STORAGE_BUCKET`</span> | <span style="color:gray">unset</span> |
   | <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

   Expect the target comparison to report in sync, no push, and
   `Hash storage unchanged for person U0000144 - baseline not rewritten`: the manual sync computed
   exactly the hash the pipeline stored, so the file is not rewritten.

### 2.5 Deactivation and reactivation

1. Lower the source simulator Lambda's `MOCK_TOTAL_POPULATION` in the console (only that variable),
   then run `SyncPerson.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
   | **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000142`** |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">`s3`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID`</span> | <span style="color:gray">`delta-storage`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DELTA_STORAGE_BUCKET`</span> | <span style="color:gray">unset</span> |
   | **simulator Lambda env** | **`MOCK_TOTAL_POPULATION`** | **`140` (below `U0000142`'s index, 141)** |

   Expect the deactivation in the target, then
   `Marking 1 of 1 person(s) as deleted in hash storage`; the `U0000142` line's hash becomes
   `DELETED:<hash>`, and other lines are unchanged.
2. Run it again, with the same settings. Expect `already inactive`,
   `Marking 0 of 1 person(s) as deleted in hash storage`, and no rewrite of the file.
3. Restore `MOCK_TOTAL_POPULATION`, then run `SyncPerson.ts` with:

   | Where | Variable | Value |
   |---|---|---|
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_BUID`</span> | <span style="color:gray">`U0000142`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">`s3`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID`</span> | <span style="color:gray">`delta-storage`</span> |
   | <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DELTA_STORAGE_BUCKET`</span> | <span style="color:gray">unset</span> |
   | **simulator Lambda env** | **`MOCK_TOTAL_POPULATION`** | **deployed value, at least `150` (default `10000`)** |

   Expect the person to be reactivated in the target, and the line's hash unmarked again.

**Storage-only variant**: add a line for a BUID with no source or target person to the baseline
file - e.g. `{"fieldValues":[{"sourceIdentifier":"U0999998"}],"hash":"abc"}` - then run
`SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0999998`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DRY_RUN`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">`s3`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID`</span> | <span style="color:gray">`delta-storage`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DELTA_STORAGE_BUCKET`</span> | <span style="color:gray">unset</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect its hash to become `DELETED:abc`.

### 2.6 Batch: one write at the end

Run `SyncPersonBatch.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_SYNC_BUIDS`** | **`U0000145,U0000146,U0000147`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_BATCH_SYNC_BUIDS_FILE_PATH`</span> | <span style="color:gray">unset (it would take precedence over `SYNC_BUIDS`)</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_BATCH_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_BATCH_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_BATCH_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`DRY_RUN` (unprefixed: the batch harness has no prefixed version)</span> | <span style="color:gray">`false`</span> |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_PREVIOUS_STORAGE_TYPE`** | **`s3`** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_INTEGRATED_DELTA_CLIENT_ID`** | **`delta-storage`** |
| **huron-person `.env`** | **`SYNC_PERSON_BATCH_DELTA_STORAGE_BUCKET`** | **unset** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_BATCH_OUTPUT_FILE_PATH`</span> | <span style="color:gray">unset (logs to `data/sync_person_batch_output.json`)</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

1. Interrupt it after the first person's push. Expect **no** change to the baseline file: in S3
   mode the batch writes the baseline once, at the end, for all persons.
2. Run it to completion, with the same settings. Expect
   `Successfully updated hash storage with 3 person record(s)` and lines for all three persons,
   written in a single rewrite. A person deactivated during a batch is marked `DELETED:` in that
   same write.

### 2.7 Dry run

Run `SyncPerson.ts` with:

| Where | Variable | Value |
|---|---|---|
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`HURON_PERSON_CONFIG_PATH`</span> | <span style="color:gray">unset (uses `config/config.json`)</span> |
| **huron-person `.env`** | **`SYNC_PERSON_SYNC_BUID`** | **`U0000148`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_PREVIEW`</span> | <span style="color:gray">`false`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_UPDATE_HASH`</span> | <span style="color:gray">`true`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE`</span> | <span style="color:gray">`false`</span> |
| **huron-person `.env`** | **`SYNC_PERSON_DRY_RUN`** | **`true`** |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_PREVIOUS_STORAGE_TYPE`</span> | <span style="color:gray">`s3`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID`</span> | <span style="color:gray">`delta-storage`</span> |
| <span style="color:gray">huron-person `.env`</span> | <span style="color:gray">`SYNC_PERSON_DELTA_STORAGE_BUCKET`</span> | <span style="color:gray">unset</span> |
| <span style="color:gray">simulator Lambda env</span> | <span style="color:gray">`MOCK_TOTAL_POPULATION`</span> | <span style="color:gray">deployed value, at least `150` (default `10000`)</span> |

Expect `[DRY RUN] - updatePreviousData: ...` in place of the write, no create in the target, and no
`U0000148` line.

### Switching back

- Unset `SYNC_PERSON_PREVIOUS_STORAGE_TYPE` / `SYNC_PERSON_BATCH_PREVIOUS_STORAGE_TYPE` (the added
  `bucketName` can stay).
- If the stack was switched to S3 mode, set `"PREVIOUS_STORAGE_TYPE": "dynamodb"` in
  `context/context.json` again and update the stack; the person tables come back empty.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `[ApiClientForJWT] Received 401 Unauthorized`, then `Error during token refresh after 401` | `DATATARGET_ENDPOINTCONFIG_EXTERNAL_TOKEN` is not the target simulator's token (a real Huron token is about 1750 characters; the simulator's is 48) |
| `Did not find ... in target (indicates a create)` followed by a `PATCH` and `Cannot determine HRN for UPDATE operation` | An older integration-huron-person with `SYNC_CRUD=update` set (prefixed or unprefixed). Current versions correct a supplied UPDATE to CREATE when the person is not in the target; leave `SYNC_CRUD` unset either way |
| A setting seems ignored, or a step behaves as if a different value were set | An unprefixed variable (e.g. `SYNC_UPDATE_HASH`, `DRY_RUN`) is filling in for an empty prefixed one - see "Settings tables" |
| A different person, or a used-up depletion counter, from the source | `DATASOURCE_ENDPOINTCONFIG_PERSON_PATH` is not `/person` |
| Pushes reach `bu.hrs-preview.com` (or another real target) | `.env`'s `DATATARGET_ENDPOINTCONFIG_BASE_URL` still names a real target; it overrides the config file |
| `Configuration validation passed` is missing; `S3 storage requires bucketName configuration` | S3 mode selected without a `bucketName` in the active config |
| `UPDATED` where `UNCHANGED` was expected in a parity step | Terms, `preLoadedMaps` or `fieldsOfInterest` differ between the manual sync and the pipeline (see 1.4) |
| In S3 mode, the chunker keeps starting tasks for the same run | The deployed image predates the fix that keeps S3-mode run totals and the end-of-records flag in `_metadata.json` |
