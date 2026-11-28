# Storage Modes: DynamoDB vs S3

The pipeline keeps its delta state (each person's last-synced hash) and each run's metadata in one of two storage
modes, chosen per landscape at deploy time. This doc describes what each mode stores and where, the abstractions that
let the code serve both, how the mode is switched, when to choose which, and how soft-deleted people are marked in the
baseline. Read it before changing anything that reads or writes delta state, run metadata or the person cache, or
before switching a landscape's mode.

## Selecting the mode

The mode is selected at deploy time by `context.PREVIOUS_STORAGE_TYPE` (CDK bakes it into every task definition as the
`PREVIOUS_STORAGE_TYPE` env var, which the Docker routers branch on; the runtime factories below then switch on the
resulting `config.storage.type`):

- `'s3'` | `'file'` | `'database'` → **S3 Mode** (traditional file-based storage)
- `'dynamodb'` → **DynamoDB Mode** (database-backed state tracking)

S3 chunk NDJSON files (`chunk-0000.ndjson`, `chunk-0001.ndjson`, etc.) remain in S3 regardless of storage mode. Only
state/metadata files move to DynamoDB.

## S3 mode

What's stored in S3:

1. **Chunk files**: `s3://bucket/chunks/{population}/{timestamp}/chunk-XXXX.ndjson`
2. **Metadata file**: `s3://bucket/chunks/{population}/{timestamp}/_metadata.json`
3. **Flags file**: `s3://bucket/chunks/{population}/{timestamp}/_flags.json`
4. **Terminal error marker**: `s3://bucket/chunks/{population}/{timestamp}/_terminal_error.json`
5. **Delta storage**: `s3://bucket/delta-storage/{population}/previous-input.ndjson`
6. **Hash storage**: `s3://bucket/delta-storage/{population}/hashes.ndjson`

Characteristics:

- Simple file-based architecture
- Easy to inspect with AWS Console or CLI
- Lower cost for infrequent access patterns
- Sequential file I/O (streaming, line-by-line reading)

## DynamoDB mode (the default)

What's stored in DynamoDB:

1. **PersonCurrentStateTable** - current person sync state
   - PK: `personId` (BUID)
   - Attributes: `hash`, `sourceIdentifier`, `lastSyncTime`, `syncRunId`
   - GSI: `syncRunId-personId` (for querying all persons in a sync run)
2. **PersonHistoryTable** - historical change audit trail
   - PK: `personId`, SK: `syncRunId` (composite key for versioning)
   - Attributes: `changeType` (NEW | UPDATED | REACTIVATED | DELETED, integration-core's `PersonHistoryChangeType`),
     `hash`, `previousHash` (never `DELETED:`-marked since 2026-11-25)
   - GSI1: `syncRunId-changeType` (query all changes in a run by type)
   - GSI2: `changeType-syncRunId` (query changes across runs by type)
3. **StatisticsTable** - metadata, flags and error events, replacing `_metadata.json` and `_flags.json`; see
   [STATISTICS_TABLE.md](STATISTICS_TABLE.md).

What remains in S3 (even in DynamoDB mode):

- Chunk NDJSON files (parallel processing dependency)
- Person cache file (10K-100K BUIDs, better in S3 than DynamoDB item batches)

Characteristics:

- Atomic updates with conditional expressions
- Point-in-time recovery and backups
- Query-based access patterns (GSI flexibility)
- Better for high-frequency state lookups
- Supports resumption after failures (via PersonCurrentStateTable)

## Abstractions serving both modes

DynamoDB mode does NOT remove S3 features; both modes coexist through template-pattern abstractions:

1. **HashStorage** (integration-huron-person `src/delta-storage/`)
   - `AbstractHashStorage` base class
   - `HashStorageResetForS3` (S3 implementation)
   - `HashStorageResetForDynamoDb` (DynamoDB implementation)
   - `HashStorageResetFactory` switches on `config.storage.type`
2. **PersonCache** (`src/person-cache/`)
   - `AbstractPersonCache` base class
   - `PersonCacheForS3` (direct S3 implementation)
   - `PersonCacheForDynamoDb` (facade delegating to S3 - optimal for bulk data)
   - `PersonCacheFactory` switches on `config.storage.type`
   - **Person target** (Strategy Pattern): `AbstractPersonTarget` abstracts the target system the cache is built
     from; `PersonTargetReal` fetches from the target API (`config.dataTarget`) via ListPeople - the real Huron API,
     or in a mock landscape the target simulator (see [MOCK_LANDSCAPES.md](MOCK_LANDSCAPES.md)). Dependency
     injection enables testing without environment coupling.
3. **Metadata** (`src/chunking/metadata/`)
   - `AbstractMetadata` with 19 methods (5 static, 14 abstract)
   - `MetadataForS3` (file-based implementation)
   - `MetadataForDynamoDb` (StatisticsTable with `eventType` field)
   - `MetadataFactory` switches on `config.storage.type`

**IAM permissions** (configured in the task definitions): S3 mode gets S3 bucket read/write only; DynamoDB mode gets
the S3 bucket plus DynamoDB table read/write (conditionally granted).

**CDK infrastructure** (`lib/DynamoDB.ts`): tables are created when `context.PREVIOUS_STORAGE_TYPE === 'dynamodb'` or
when `context.PREVIOUS_STORAGE_TYPE` is undefined (`DEFAULT_STORAGE_TYPE`, 'dynamodb'). Task definitions check
`dynamoDbTables.personCurrentStateTable` before granting permissions. S3 mode has zero DynamoDB infrastructure impact.

## Switching between modes

Set `PREVIOUS_STORAGE_TYPE` (`"s3"` or `"dynamodb"`) in the active `context/context.json` and update the stack. That
one value is the switch: the integration config files (`integration-huron-person/config/config.*.json`) need no
change, because the config secret's storage block is derived from it (`resolveSecretStorage()` in `lib/Secrets.ts`):

- **Type**: `context.PREVIOUS_STORAGE_TYPE`, else the integration config's `storage.type`, else `'dynamodb'`
  (`DEFAULT_STORAGE_TYPE`, the last resort). So the secret always agrees with the mode the tasks run in.
- **Settings**: the integration config's `storage.config` is kept only if it belongs to the resolved type (e.g. an S3
  `keyPrefix`); settings for another type are dropped, not mixed in. Settings the stack determines come from the
  context: the landscape's chunks bucket (`bucketName`, S3 mode) or the person current-state/history table names and
  region (DynamoDB mode).
- Environment variables (`.env`, loaded by `bin/App.ts`) don't set storage in the secret.

Keep `PREVIOUS_STORAGE_TYPE` set in every `context.*.json`. When it is missing, every CDK fallback ends at
`DEFAULT_STORAGE_TYPE` (`'dynamodb'`, in `lib/Secrets.ts`), but they don't all look at the integration config first:
`lib/DynamoDB.ts` and the chunker and post-processor subscriber Lambdas go straight to the default, while
`TaskDefinitions` takes the locally loaded integration config's `storage.type` and the secret the context's
`HURON_PERSON_CONFIG` storage. So an integration config that names `s3` would split the stack. Switching a stack to S3 mode deletes its PersonCurrentState and PersonHistory tables (removal policy
DESTROY); switching back creates them empty. Switching does not touch an S3 baseline (`delta-storage/` in the chunks
bucket) left by an earlier S3-mode period; in a mock landscape the runner's reset deletes it.

## When to use each mode

**Use S3 mode when**:

- Simple deployment with minimal infrastructure
- Infrequent sync operations (daily/weekly)
- File-based introspection preferred (AWS Console browsing)
- Lower cost priority for small-scale operations

**Use DynamoDB mode when**:

- Frequent sync operations requiring fast state lookups
- Audit trail and history tracking critical
- Resumption and retry logic needed (pipeline interruptions)
- Query-based analytics desired (change type filtering, time-series analysis)
- Point-in-time recovery and backups required

## Soft-deleted persons in the baseline

**Soft-deleted persons are marked in the baseline** (`src/post-processing/DeletedHashMarker.ts`): both baselines
(PersonCurrentStateTable, `previous-input.ndjson`) retain every person ever seen, so a person absent from the source
would otherwise be selected for deletion on every full sync. After a successful soft-delete, `onSoftDeleteSuccess`
rewrites that person's baseline hash with a `DELETED:` prefix (DynamoDB mode also sets `deletedAt`). Deletion
detection skips marked entries, and a marked hash never matches a fresh source hash, so a person who reappears is
classified UPDATED and reactivated (via `__active`); the processor's write then clears the marker.

### History: DynamoDB-mode matching before 2026-11-17

In DynamoDB mode this only works since 2026-11-17 (`dynamodb-delta-match-plan.md`; integration-core `31a0b49`, taken
in by `bb28a74`'s lockfile). Until then, a run trusting the stored hashes looked a chunk's stored records up by whole
records, and they came back named after the person's first mapped field (`id`), not `sourceIdentifier`, so none
matched. Every changed person, and every returning one, was classified as added and sent as a create, which the target
refused as a duplicate (409). Failed people keep their old hash, so they failed again on every later trusted run, and
returning people stayed deactivated.

Mock runs `2026-11-17T07:13:57.528Z` and `07:42:14.206Z` failed 1,500 returning people this way; with the fix, run
`18:58:47.312Z` updated and reactivated all 1,500, and a repeat run sent nothing. Bulk-reset runs take another path and
weren't affected; preview and staging had no one left in that state (their only trusted runs sent nothing). See
integration-core's `CLAUDE.md` ("Delta Engines").

### History: taking an integration-core build while core listed itself as a dependency

Until 2026-11-17, while integration-core listed itself as a dependency, `npm install <its URL>` broke this project's
lockfile. The procedure that replaced it (update the `integrity` and run a plain `npm install`) is in the project
`CLAUDE.md`.
