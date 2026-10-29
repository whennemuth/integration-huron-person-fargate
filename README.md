# Huron Person Integration - Fargate Infrastructure

This CDK TypeScript project deploys the Huron Person integration as a serverless ECS Fargate task that processes batches of people synchronization in parallel.

## Repository Boundaries

This project is an independently versioned npm package with its own Git repository.

It depends on other packages (notably `integration-core`, `integration-huron-person` and integration pipeline packages) through dependency-level composition. The workspace is a development convenience, not a workspace-level source-control unit.

## Shared Skills Setup

Shared agent skills are maintained in a separate repository (https://github.com/whennemuth/integration-skills) that is cloned as the `.claude` directory of the workspace root, so skills live at `<workspace root>/.claude/skills/`. This is a native skill location that both VS Code (GitHub Copilot) and Claude Code discover without any custom settings. See `<workspace root>/.claude/README.md` for full setup details.

```bash
cd <workspace root>
git clone https://github.com/whennemuth/integration-skills.git .claude
```

### Claude Code

Start Claude Code in the workspace root. It loads `.claude/skills/` from its start directory with no extra configuration. When starting in this repository instead, add the workspace root with `claude --add-dir ..` (or `/add-dir` in a running session).

### VS Code

VS Code discovers `.claude/skills/` only at the top of each root folder of a [**multi-root**](https://code.visualstudio.com/docs/editing/workspaces/workspaces#_multiroot-workspaces) workspace. Include the workspace root itself (`"path": "."`) as a root folder in your [**`.code-workspace`** file](https://code.visualstudio.com/docs/editing/workspaces/workspaces#_workspace-settings), saved in the workspace root, as in the scenarios below. To avoid listing each repository twice, hide the package folders inside that root with `files.exclude` in `<workspace root>/.vscode/settings.json` (see the skills README).

### Scenario 1: Working only on integration-core

```json
{
  "folders": [
    {
      "path": ".",
      "name": "workspace"
    },
    {
      "path": "integration-core",
      "name": "core"
    }
  ],
  "settings": {
    "chat.tools.terminal.autoApprove": {
      "npm test": true
    }
  }
}
```

### Scenario 2: Working on core + person + fargate

```json
{
  "folders": [
    {
      "path": ".",
      "name": "workspace"
    },
    {
      "path": "integration-core",
      "name": "core"
    },
    {
      "path": "integration-huron-person",
      "name": "huron-person"
    },
    {
      "path": "integration-huron-person-fargate",
      "name": "huron-person-fargate"
    }
  ],
  "settings": {
    "chat.tools.terminal.autoApprove": {
      "npm test": true
    }
  }
}
```

### Workspace Instructions

Workspace-wide agent instructions live in `<workspace root>/.claude/CLAUDE.md`, versioned in the [integration-skills](https://github.com/whennemuth/integration-skills) repository that is cloned as the workspace's `.claude` directory.

**Purpose**: Contains the workspace architecture overview, coding preferences, the task verification protocol (build verification, test execution, and completion reporting on all code implementation tasks), and workflow requirements that should be consistently applied across all integration projects.

**Discovery**: Claude Code and GitHub Copilot CLI load it automatically when started in the workspace root. VS Code Copilot Chat loads it when the workspace root (`"path": "."`) is a root folder, as in the scenarios above, and the `chat.useClaudeMdFile` setting is enabled. When starting Claude Code in this repository with `--add-dir ..`, also set `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` to load it.

**Scope**: These instructions apply workspace-wide and are distinct from project-specific conventions documented in individual project `CLAUDE.md` files.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Architecture

- **ECS Cluster**: Manages Fargate task instances
- **Fargate Task Definition**: Configures processor container with environment variables
- **EventBridge Scheduled Rule**: Optional trigger for recurring sync operations
- **S3 Integration**: Reads source data from S3 for parallel chunk processing
- **Secrets Manager**: Stores API credentials and configuration securely

## Configuration

The Fargate task is configured via the `HURON_PERSON_CONFIG` environment variable, which contains a JSON configuration object. The configuration structure matches the `integration-huron-person` package.

### Dry Run Mode

Dry run mode allows testing the synchronization logic without making changes to the target Huron API or delta storage. This is particularly useful when:
- Testing new configurations in production-like environments
- Validating data mappings before actual synchronization
- Debugging issues without affecting live data
- Auditing planned synchronization operations

#### Enabling Dry Run in Fargate

Set `dryRun: true` in the `dataTarget` section of your configuration:

```json
{
  "dataTarget": {
    "endpointConfig": { ... },
    "personsPath": "/api/v2/persons",
    "organizationsPath": "/api/v2/organizations",
    "dryRun": true
  }
}
```

When dry run is enabled:
- The Fargate task sets the `DRY_RUN=true` environment variable for processor containers
- All write operations (POST/PATCH to Huron API) are logged but not executed
- Delta storage updates are skipped (previous data is not persisted)
- Read operations continue normally (source fetching, delta computation, HRN lookups)
- All parallel chunks process without writing any data
- Console logs show what operations would have been performed

**Important**: Dry run mode affects all parallel processing chunks, so the entire batch runs in test mode.

### Source End-of-Records Detection

Configured in `context/context.json` under `ECS.chunkerTaskDefinition` and passed to the chunker task as environment variables:

| Context field | Task env var | Default | Purpose |
|---|---|---|---|
| `stopAtFirstPartial` | `STOP_AT_FIRST_PARTIAL` | `false` | If `true`, the first source batch smaller than the requested `recordCount` (a "partial") ends chunking. If `false`, only an empty batch does, because the source API can return partials before it runs out of records. |
| `maxTotalRecords` | `MAX_TOTAL_RECORDS` | `500000` | Safety cutoff. If a run's total source records would exceed this, the source is assumed to be glitched (never returning an empty batch), and chunking stops as a failed run. A value `<= 0` disables the cutoff. |

For local harness runs, use the prefixed equivalents (e.g. `DOCKER_CHUNKER_STOP_AT_FIRST_PARTIAL`, `CHUNK_FROM_API_MAX_TOTAL_RECORDS`); see `example-env.md`.

## Test Harnesses

Test harnesses are executable modules that verify individual components using environment-based configuration via the `TestEnvironment` utility from `integration-core`. Each harness loads its own prefixed environment variables and validates component behavior in isolation.

Harness configuration is documented in [example-env.md](./example-env.md). The file contains grouped environment variables for test harnesses covering the major processing pipelines:

- **Orchestration**: `Runner` (Manual chunking service invocation, queue seeding)
- **Chunking Pipeline**: `ChunkFromAPI`, `ChunkFromS3`, `BigJsonFetch`, `BigJsonFile`, `PersonArrayWrapper`
- **Post-processing Pipeline**: `DeferredDeleteHandler`, `PersonCache`, `StatisticsTable`
- **Docker Entry Points**: `chunker.ts`, `processor.ts`, `post-processor.ts`
- **Storage & Error Handling**: `S3StorageAdapter`, `ApiErrorTracking`, `EcrChecker`

### Running Test Harnesses

**Option 1: Using VS Code Launch Configuration (Recommended)**

1. Open the harness file in the editor (e.g., `src/chunking/fetch/ChunkFromAPI.ts`)
2. Press `F5` or go to **Run > Start Debugging**
3. Select "Debug current file" from the launch configuration dropdown
4. The harness will execute with your `.env` file automatically loaded

**Option 2: Command Line with npx**

```bash
# Example: Run the Runner harness (manual chunking orchestration)
npx ts-node src/Runner.ts

# Example: Run the ChunkFromAPI harness
npx ts-node src/chunking/fetch/ChunkFromAPI.ts

# Example: Run the PersonCache harness
npx ts-node src/PersonCache.ts

# Example: Run the docker chunker harness
npx ts-node docker/chunker.ts
```

For detailed documentation on the Runner harness, see [src/CLAUDE.md](./src/CLAUDE.md).

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `npm run deploy`  deploy every stack (pipeline, and the landscape's dashboard if the context has a `DASHBOARD` section)
* `npm run deploy-fargate` / `npm run deploy-dashboard`  deploy one stack (pipeline first: the dashboard reads its resources)
* `npm run teardown` / `teardown-fargate` / `teardown-dashboard`  destroy every stack / one stack
* `npx cdk diff --all`    compare deployed stacks with current state
* `npm run synth`   emits the pipeline stack's CloudFormation template

The app has more than one stack, so a bare `cdk deploy` / `cdk destroy` fails; see
[docs/CDK_DEPLOYMENT.md](./docs/CDK_DEPLOYMENT.md#deployment-steps).
