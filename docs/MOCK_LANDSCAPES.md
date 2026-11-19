# Mock Landscapes

A mock landscape is a pipeline stack dedicated to mocked runs, whose target is always the target simulator and whose
source may be the source simulator. This doc covers why mocking is a landscape rather than a per-run switch, what
differs in a mock stack, how its run state is reset, and the dead-code lesson that led here. The rules an agent must
follow are in the project `CLAUDE.md` ("Mock Landscapes"); the end-to-end test runbook is
[MOCK_INTEGRATION_TESTING.md](MOCK_INTEGRATION_TESTING.md).

## The signal

Any landscape whose name matches `^mock\d*$` (`mock`, `mock1`, `mock2`, ...) is a mock landscape. `isMockLandscape()`
(`src/Utils.ts`) is the single test for it, used only by CDK (with the lower-cased `TAGS.Landscape`), the Runner and
`BulkPurger`. There is no per-run "mock" flag, and the pipeline's runtime code (chunker, processor, post-processor) has
no mock awareness at all: mock-ness is a deploy-time property of the stack. (The dashboard and the chunker subscriber
also test it, for a mock landscape's own options and reset.)

## Why a landscape, not isolation tables

Nearly every resource name already carries the landscape (`${STACK_ID}-<kind>-${landscape}` tables, queues, cluster,
Lambdas, config secret, chunks bucket, the stack itself), so a dedicated mock stack isolates everything by
construction - including the S3 delta storage and the chunker's baseline scan, which the old per-run isolation tables
never covered. A mock stack uses the **standard** tables; they simply never hold real data.

## What differs in a mock stack

- Exactly one extra table: `${STACK_ID}-mock-target-person-${landscape}` (MockTargetPersonTable), backing the target
  simulator. Non-mock stacks have no `-mock-` tables at all.
- The target simulator ([TARGET_SIMULATOR.md](TARGET_SIMULATOR.md)), always, and the source simulator
  ([SOURCE_SIMULATOR.md](SOURCE_SIMULATOR.md)) if enabled - synthesis fails if it is enabled in a non-mock landscape.
  Neither exists in any other landscape.
- Its integration config (`integration-huron-person/config/config.mock.json`) sets
  `dataTarget.endpointConfig.baseUrl` to an unroutable placeholder, which CDK replaces with the simulator's URL in the
  deployed secret (so a local harness pointed at the file still fails loudly instead of reaching a real target), and
  `preLoadedMaps` like preview (`orgMap` false; state/country maps come from CSV files, not Huron).
- Its chunker subscriber may reset the landscape's run state and has the grants for it, and 1,024 MB of memory
  ([RUN_START.md](RUN_START.md#a-mock-landscapes-run-from-scratch-resetstate)).

## The Runner in a mock landscape

When `LANDSCAPE` is a mock landscape, `Runner.ts` wraps the runner in `MockLandscapeRunnerDecorator`, which:

- verifies the mock stack is deployed (the mock target table exists);
- optionally resets its run state (`RUNNER_MOCK_TARGET_RESET_STATE=true`, below);
- warns about "hybrid" runs (real source API -> mock target), which remain supported (`RUNNER_SOURCE_SIMULATOR=false`
  against a mock landscape), but consume from the real, depleting source API like any other landscape.

## Resetting a mock landscape's run state

The reset (`src/chunking/MockLandscapeReset.ts`, shared by the Runner and the chunker subscriber's `resetState` and
reset-only events) truncates the mock target and statistics tables via `BulkPurger`, then clears the delta baseline of
the stack's storage mode: the person current-state and history tables in DynamoDB mode, or the `delta-storage/` files
of the chunks bucket in S3 mode. The mode is detected from whether the person current-state table exists. Emptying the
statistics table empties Run history too.

`BulkPurger` refuses to purge a non-mock landscape unless `force: true`.

How the subscriber runs a reset (refusals, grants, how long it takes, the reset record): see
[RUN_START.md](RUN_START.md#a-mock-landscapes-run-from-scratch-resetstate).

## Deploying one

`context/context.mock.json` (+ git-ignored `context/mock.env`) -> `./context/swap.sh mock`, then `cdk deploy`. Add
`context.mock1.json` etc. for more mock stacks.

## History: the `config.preLoadedMaps` dead code

Before mock landscapes, mock mode was a per-run flag. A runner decorator (`MockTargetRunnerDecorator`, since replaced
by `MockLandscapeRunnerDecorator`) mutated `config.preLoadedMaps = {orgMap:false, ...}` believing this would prevent
organization/state/country lookups from calling the real Huron API in mock target mode:

- **Assumption**: setting this field on the Runner's `Config` object would reach the running processor task.
- **Reality**: `config.preLoadedMaps` is only ever read by CDK at deploy time (`ProcessorTaskDefinition.ts`) to bake
  `STATIC_MAP_USAGE` into the ECS task's environment variables - from an entirely different `Config` object loaded
  from disk, not the one the Runner mutated at invocation time.
- **Result**: the override compiled and ran with zero errors, logged nothing wrong, and had absolutely no effect -
  mock-mode runs could still trigger real organization API calls.

It was fixed by forcing `StaticMapUsage` at the point it's actually consumed - inside the processor, immediately after
`flags.useMockTarget` was read from chunk metadata (the same channel already used to select the mock data target) -
rather than trying to inject it upstream through a Config object that never reaches the running task. The lesson: a
runtime override that "looks right" and produces no errors can still be a no-op if it mutates the wrong instance of a
config object that exists in two different lifecycles (deploy-time vs invocation-time).

Since superseded: mock mode is now a deploy-time property of a mock landscape, whose target simulator answers the same
calls the real Huron API would, so no static-map override is needed at all; the runtime override
`resolveStaticMapUsage()` was removed. The same lesson shaped how simulated terms reach the processor
([SOURCE_SIMULATOR.md](SOURCE_SIMULATOR.md#how-simulated-terms-reach-the-processor)).
