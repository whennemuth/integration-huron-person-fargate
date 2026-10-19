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
| Per-record classification | NEW / UPDATED / UNCHANGED (UNCHANGED not written) | none - the baseline file is rewritten |

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

The source simulator returns the same person for a BUID on every request (`/person?buid=`), as long
as its index is below the simulator Lambda's `MOCK_TOTAL_POPULATION` (`U0000042` is index 41).

### Configure integration-huron-person for the mock landscape

From `integration-huron-person/config`, make the mock landscape active (copies
`config.mock.json` to `config.json` and `mock.env` to `../.env`):

```bash
./swap.sh mock
```

Then, in integration-huron-person's `.env`:

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
| `SYNC_PERSON_SYNC_CRUD` | unset | the operation must come from the target lookup |
| `SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE` | unset (false) | except where a step says otherwise |

The target simulator token is a 48-character value in its own secret. Do not copy the
`externalToken` from `mock.env`: that is a real Huron token and the simulator rejects it with `401`.

```bash
aws secretsmanager get-secret-value --region us-east-2 --secret-id huron-person-fargate-processor/target-simulator/token/mock --query SecretString --output text
```

```bash
aws secretsmanager get-secret-value --region us-east-2 --secret-id huron-person-fargate-processor/integration/_config/mock --query SecretString --output text | jq '{personKey: .dataSource.person.endpointConfig.apiKey, termsKey: .dataSource.terms.endpointConfig.apiKey}'
```

### How to run things

Manual syncs run from `integration-huron-person`, configured through `.env` (`SYNC_PERSON_*` for
`SyncPerson.ts`, `SYNC_PERSON_BATCH_*` for `SyncPersonBatch.ts`):

```bash
npx ts-node src/SyncPerson.ts
```

```bash
npx ts-node src/SyncPersonBatch.ts
```

`SyncPersonBatch.ts` logs to `data/sync_person_batch_output.json` (`SYNC_PERSON_BATCH_OUTPUT_FILE_PATH`).

Pipeline syncs run from this project, configured through its `.env` (`RUNNER_*`):

```bash
npx ts-node src/runner/Runner.ts
```

Besides the per-step settings, the Runner needs `RUNNER_LANDSCAPE=mock`, `RUNNER_REGION`,
`RUNNER_STACK_ID` and `RUNNER_CHUNKER_QUEUE_URL` (see `src/runner/README.md`). With a mock
landscape it verifies the mock stack is deployed before sending anything.

### Manual sync settings that matter here

| Variable | Effect |
|---|---|
| `SYNC_UPDATE_HASH` | `true`: compute hashes, check whether a person is already in sync, and record their hash (and deactivations) in hash storage. `false`: push without an in-sync check and leave hash storage alone |
| `SYNC_TRUST_PREVIOUS_STORAGE` | `true`: decide whether a person is in sync from their stored hash, falling back to comparing with the target. DynamoDB mode only; requires `SYNC_UPDATE_HASH=true` (a warning is logged otherwise) |
| `SYNC_PREVIEW` | `true`: no pushes or deactivations, and no hash storage writes |
| `DRY_RUN` | `true`: log pushes and hash storage writes instead of performing them |
| `PREVIOUS_STORAGE_TYPE`, `DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME`, `DYNAMODB_PERSON_HISTORY_TABLE_NAME`, `REGION` | storage selection; take precedence over the config file |
| `INTEGRATED_DELTA_CLIENT_ID` | S3/file mode only: the baseline path. `delta-storage` is the pipeline's baseline |

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

Set `SYNC_PERSON_SYNC_BUID=U0000042`, `SYNC_PERSON_SYNC_PREVIEW=true`; run `SyncPerson.ts`.

Expect:
- `Found U0000042 in source`, then a target lookup with no `401`.
- No change to any table.

### 1.2 First sync, then an unchanged re-run

Set `SYNC_PERSON_SYNC_PREVIEW=false`, `SYNC_PERSON_SYNC_UPDATE_HASH=true`; run `SyncPerson.ts`.

Expect:
- A create in the target simulator (the person appears in the mock target table).
- `DynamoDBDeltaStorage: 1 NEW, 0 UPDATED, 0 UNCHANGED (not written) of 1 record(s)`.
- One PersonCurrentState item (`hash`, the logged `syncRunId`) and one `NEW` PersonHistory item.

Run it again. Expect:
- `Source and target are already in sync for BUID: U0000042.` and the push result
  `Sync skipped - source and target already in sync`.
- `DynamoDBDeltaStorage: 0 NEW, 0 UPDATED, 1 UNCHANGED (not written) of 1 record(s)`.
- No change to any table (UNCHANGED records are not written, so the first run's `syncRunId` stays).

### 1.3 Stored-hash in-sync check

**Stored hash present.** Add `SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE=true`; run `SyncPerson.ts`
for `U0000042`. Expect:
- `Source matches the hash in storage for BUID: U0000042 - already in sync.` - and no
  `Source and target are already in sync` line (the target comparison was skipped).
- No push; `1 UNCHANGED`.

**No stored hash.**
1. Set `SYNC_PERSON_SYNC_BUID=U0000043`, `SYNC_PERSON_SYNC_UPDATE_HASH=false` (trust still
   `true`); run. Expect the warning
   `SYNC_TRUST_PREVIOUS_STORAGE=true has no effect without SYNC_UPDATE_HASH=true...`, a create in
   the target, and no rows for `U0000043`.
2. Set `SYNC_PERSON_SYNC_UPDATE_HASH=true`; run. Expect
   `No hash in storage for BUID: U0000043 - comparing with target instead.`, the target comparison
   reporting in sync, and `1 NEW` (the person's first state and history records).

Unset `SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE` afterwards.

### 1.4 Hash parity with the pipeline

1. In this project's `.env`, set `RUNNER_SINGLE_PERSON_BUID=U0000044`,
   `RUNNER_SOURCE_SIMULATOR=true` and **`RUNNER_POPULATION_TYPE=person-delta`**; run `Runner.ts`.
   The population type matters: unset, it defaults to `person-full`, and the post-processor then
   treats every other person in PersonCurrentState as removed and deactivates them.
2. Wait for the post-processor to finish. Expect a PersonCurrentState item and a `NEW` history
   item for `U0000044` carrying the pipeline run's `syncRunId`, and a `METADATA` record for that
   `syncRunId` in the statistics table.
3. Run `SyncPerson.ts` for `U0000044` with `SYNC_PERSON_SYNC_UPDATE_HASH=true`.

Expect `1 UNCHANGED` and no new history item: the manual sync computed exactly the hash the
pipeline stored. An `UPDATED` here means the two hash differently - check, in order, that terms
come from the simulator (Part 0), the integration config's `preLoadedMaps`, and the chunker's
`dataSource.people.fieldsOfInterest` trimming.

Note: `syncRunId`s written by manual syncs have no `METADATA`/`FLAGS` record in the statistics
table; only pipeline runs write those. (Error event records use their own timestamps, so look for
`METADATA`, not just any record.)

### 1.5 Deactivation and reactivation

1. Lower the source simulator Lambda's `MOCK_TOTAL_POPULATION` below `U0000042`'s index, e.g. to
   `40`. Change only that variable in the console - `aws lambda update-function-configuration`
   replaces the whole environment. (A Runner run with `RUNNER_SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION`
   also sets it, but starts a pipeline run.)
2. Run `SyncPerson.ts` for `U0000042` (`SYNC_PERSON_SYNC_UPDATE_HASH=true`). Expect:
   - `Did not find U0000042 in source`, then `Deactivating U0000042 in target system...`.
   - `DynamoDBDeltaStorage: marking 1 of 1 person(s) as DELETED` and
     `Marked U0000042 as deleted in hash storage`.
   - PersonCurrentState: `hash` = `DELETED:<previous hash>`, a `deletedAt`, this run's `syncRunId`.
   - PersonHistory: a `DELETED` item with `hash` and `previousHash` both the unmarked previous hash
     (history never carries the marker in `hash`; this matches what the post-processor writes).
3. Run again. Expect `No-op: need to deactivate U0000042 in target system, but already inactive.`
   and `No-op: U0000042 has no hash storage record to mark as deleted, or is already marked`; no
   table changes.
4. Restore `MOCK_TOTAL_POPULATION`; run again. Expect the person to be reactivated in the target,
   PersonCurrentState to hold the unmarked hash again, and an `UPDATED` history item whose
   `previousHash` is `DELETED:<hash>`.

**Storage-only variant** (no simulator change): put a PersonCurrentState item for a BUID beyond the
population, then sync it. The person is absent from both source and target, so only the storage
marking is exercised.

```bash
aws dynamodb put-item --region us-east-2 --table-name huron-person-fargate-processor-person-current-state-mock --item '{"personId":{"S":"U0999999"},"hash":{"S":"abc"},"syncRunId":{"S":"seed"}}'
```

Run `SyncPerson.ts` for `U0999999`. Expect the item's hash to become `DELETED:abc` with a
`deletedAt`, and a `DELETED` history item with `hash` and `previousHash` both `abc`.

### 1.6 Batch: per-person writes

Set `SYNC_PERSON_BATCH_SYNC_BUIDS=U0000045,U0000046,U0000047` and
`SYNC_PERSON_BATCH_SYNC_UPDATE_HASH=true`; run `SyncPersonBatch.ts`.

1. Interrupt it (Ctrl+C) after the first `Hash storage updated successfully` line. Expect
   `U0000045` to already have its PersonCurrentState and history items: in DynamoDB mode each
   person's state is written right after their push, not at the end of the batch.
2. Run it again to completion. Expect `U0000045` to be `UNCHANGED` (keeping its first
   `syncRunId`), and `U0000046` and `U0000047` to be `NEW` with this run's shared `syncRunId`.

### 1.7 Dry run

Set `SYNC_PERSON_DRY_RUN=true`; run `SyncPerson.ts` for `U0000048`. Expect `[DRY RUN]` log lines,
no create in the target and no items for `U0000048`. Unset `SYNC_PERSON_DRY_RUN` afterwards.

---

## Part 2: S3 mode

In S3 mode the baseline is the single file
`s3://huron-person-chunks-mock/delta-storage/previous-input.ndjson` - one line per person with
their key and hash. The bucket is the landscape's chunks bucket (`${S3.chunksBucket}-<landscape>`),
and the directory is `delta-storage` unless the integration config's S3 storage sets a `keyPrefix`
(which CDK passes to the tasks as `SHARED_DELTA_STORAGE_DIR`). There is no history and no
NEW/UPDATED/UNCHANGED classification; every write rewrites the whole file. Run metadata and flags
live as `_metadata.json` / `_flags.json` in each run's chunk directory.

### Switching to S3 mode

**Manual syncs** need no redeployment. In integration-huron-person:
- Add `"bucketName": "huron-person-chunks-mock"` to `storage.config` in the active
  `config/config.json` (not `config.mock.json`, which `swap.sh` overwrites `config.json` from).
  It is ignored in DynamoDB mode.
- Set `SYNC_PERSON_PREVIOUS_STORAGE_TYPE=s3` (and `SYNC_PERSON_BATCH_PREVIOUS_STORAGE_TYPE=s3`).
- Set `SYNC_PERSON_INTEGRATED_DELTA_CLIENT_ID=delta-storage` (and
  `SYNC_PERSON_BATCH_INTEGRATED_DELTA_CLIENT_ID=delta-storage`) so manual syncs read and write the
  same baseline file as the pipeline. Use a scratch path instead (e.g.
  `delta-storage/sync-person-regression`) to test manual syncs in isolation; the pipeline-parity
  step needs `delta-storage`.

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

As 1.1, for `U0000142`. Expect the baseline file to be unchanged (or still absent).

### 2.2 First sync, then an unchanged re-run

`SYNC_PERSON_SYNC_UPDATE_HASH=true`, BUID `U0000142`; run `SyncPerson.ts`. Expect:
- A create in the target, then `Hash storage updated successfully for person U0000142`.
- The baseline file gains a line for `U0000142` with its hash (the file is created if absent);
  other lines are unchanged.
- No DynamoDB writes.

Run it again. Expect `Source and target are already in sync for BUID: U0000142.`, no push, and
the file rewritten with the same line and hash.

### 2.3 Stored-hash check is DynamoDB-only

Set `SYNC_PERSON_SYNC_TRUST_PREVIOUS_STORAGE=true`; run for `U0000142`. Expect the target
comparison (`Source and target are already in sync...`), not `Source matches the hash in
storage`: in S3 mode the stored-hash fast path is not used, because reading one person's hash would
mean reading the whole baseline. Unset the variable afterwards.

### 2.4 Hash parity with the pipeline

Requires the stack deployed in S3 mode and the manual integrated client ID `delta-storage`.

1. Run `Runner.ts` with `RUNNER_SINGLE_PERSON_BUID=U0000144`, `RUNNER_SOURCE_SIMULATOR=true`,
   **`RUNNER_POPULATION_TYPE=person-delta`**.
2. After the post-processor finishes, expect a `U0000144` line in
   `delta-storage/previous-input.ndjson`. Note its hash.
3. Run `SyncPerson.ts` for `U0000144` (`SYNC_PERSON_SYNC_UPDATE_HASH=true`). Expect the target
   comparison to report in sync, no push, and the `U0000144` line's hash unchanged.

### 2.5 Deactivation and reactivation

As 1.5, for `U0000142` (lower `MOCK_TOTAL_POPULATION` below 141). Expect:
- The deactivation in the target, then `Marking 1 of 1 person(s) as deleted in hash storage`.
- The `U0000142` line's hash becomes `DELETED:<hash>`; other lines are unchanged.
- A re-run: `already inactive`, `Marking 0 of 1 person(s)...`, and no rewrite of the file.
- After restoring the population: reactivation in the target, and the line's hash unmarked again.

Storage-only variant: add a line for a BUID with no source or target person (e.g.
`{"fieldValues":[{"sourceIdentifier":"U0999998"}],"hash":"abc"}`) to the baseline file, then sync
`U0999998`. Expect its hash to become `DELETED:abc`.

### 2.6 Batch: one write at the end

Set `SYNC_PERSON_BATCH_SYNC_BUIDS=U0000145,U0000146,U0000147`,
`SYNC_PERSON_BATCH_SYNC_UPDATE_HASH=true`; run `SyncPersonBatch.ts`.

1. Interrupt it after the first person's push. Expect **no** change to the baseline file: in S3
   mode the batch writes the baseline once, at the end, for all persons.
2. Run it to completion. Expect `Successfully updated hash storage with 3 person record(s)` and
   lines for all three persons, written in a single rewrite. A person deactivated during a batch is
   marked `DELETED:` in that same write.

### 2.7 Dry run

Set `SYNC_PERSON_DRY_RUN=true`; run `SyncPerson.ts` for `U0000148`. Expect
`[DRY RUN] - updatePreviousData: ...` in place of the write, no create in the target, and no
`U0000148` line.

### Switching back

- Remove `SYNC_PERSON_PREVIOUS_STORAGE_TYPE` / `SYNC_PERSON_BATCH_PREVIOUS_STORAGE_TYPE` (the
  added `bucketName` can stay).
- If the stack was switched to S3 mode, set `"PREVIOUS_STORAGE_TYPE": "dynamodb"` in
  `context/context.json` again and update the stack; the person tables come back empty.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `[ApiClientForJWT] Received 401 Unauthorized`, then `Error during token refresh after 401` | `DATATARGET_ENDPOINTCONFIG_EXTERNAL_TOKEN` is not the target simulator's token (a real Huron token is about 1750 characters; the simulator's is 48) |
| `Did not find ... in target (indicates a create)` followed by a `PATCH` and `Cannot determine HRN for UPDATE operation` | An older integration-huron-person with `SYNC_CRUD=update` set. Current versions correct a supplied UPDATE to CREATE when the person is not in the target; clear `SYNC_PERSON_SYNC_CRUD` either way |
| A different person, or a used-up depletion counter, from the source | `DATASOURCE_ENDPOINTCONFIG_PERSON_PATH` is not `/person` |
| Pushes reach `bu.hrs-preview.com` (or another real target) | `.env`'s `DATATARGET_ENDPOINTCONFIG_BASE_URL` still names a real target; it overrides the config file |
| `Configuration validation passed` is missing; `S3 storage requires bucketName configuration` | S3 mode selected without a `bucketName` in the active config |
| `UPDATED` where `UNCHANGED` was expected in a parity step | Terms, `preLoadedMaps` or `fieldsOfInterest` differ between the manual sync and the pipeline (see 1.4) |
