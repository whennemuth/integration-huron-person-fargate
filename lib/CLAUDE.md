# lib/: CDK Infrastructure

The pipeline stack and the shared network stack (the dashboard's constructs are in `dashboard/lib/`). `bin/App.ts`
builds the stacks for the active landscape's context; `npm run deploy-fargate` / `teardown-fargate` (through
`bin/deploy.sh` / `bin/teardown.sh`) deploy and destroy them. The rules that span this and other folders (the shared
network, keeping the dashboard out of the task image, one statistics-table GSI per deploy, mock landscapes) are in the
project [CLAUDE.md](../CLAUDE.md); deployment steps in [docs/CDK_DEPLOYMENT.md](../docs/CDK_DEPLOYMENT.md).

## Files

- **`Stack.ts`** (`IntegrationHuronPersonLambdaStack`): the pipeline stack; builds `AppConstruct` and outputs the ECR
  repository URI.
- **`AppConstruct.ts`**: everything in the pipeline stack, in order: the ECR repository (`EcrRepository.ts`), the
  DynamoDB tables (`DynamoDB.ts`), in a mock landscape the simulators (`Simulator.ts`), the integration config secret
  (`Secrets.ts`), ECS (`EcsInfrastructure.ts`), the queues (`QueueInfrastructure.ts`), the chunks bucket, and the
  subscriber Lambdas (`SubscribingLambdas.ts`). It refuses to synthesize a source simulator outside a mock landscape.
- **`NetworkStack.ts`**: the shared network stack (`huron-integration-network`), `NetworkLayout` and `NetworkParameters`
  ([docs/SHARED_NETWORK.md](../docs/SHARED_NETWORK.md)).
- **`EcsInfrastructure.ts`**: the cluster, the task definitions (`TaskDefinitions.ts`) and the three
  `QueueProcessingFargateService`s, in the shared network's private subnets (`NetworkParameters.vpc()`).
- **`QueueInfrastructure.ts`**: a queue and dead-letter queue for each service (chunker, processor, post-processor).
- **`DynamoDB.ts`** (`DynamoDbTables`): the statistics, atomic counter and addon log tables, the person current-state and
  history tables (DynamoDB storage mode), and the mock target table (mock landscapes).
- **`Secrets.ts`**: the integration config secret (`integrationSecretName()`, `HuronPersonSecrets`), its storage block
  derived from `PREVIOUS_STORAGE_TYPE` (`resolveSecretStorage()`), and a mock landscape's target override
  (`DataTargetOverride`).
- **`Simulator.ts`**: a mock landscape's target simulator (always) and source simulator (if enabled), built in two
  steps because the source simulator reads the config secret, which embeds the target simulator's endpoint.
- **`SubscribingLambdas.ts`**: the Lambdas triggered by events: the chunker subscriber (a file in the input bucket, the
  EventBridge schedule, or an event from the dashboard), the processor subscriber (chunk files) and the post-processor
  subscriber (delta chunk files).
- **`Tagging.ts`**: tags applied to the stack and its resources.
- **`services/`**: per service, its Fargate service, task definition and subscriber Lambda:
  - `chunker/`: `ChunkerService.ts` (also the EventBridge schedule, `createApiChunkingSchedule()`),
    `ChunkerTaskDefinition.ts`, `ChunkerSubscribingLambda.ts`, `SourceSimulator.ts` (the source simulator's Function
    URL); [services/chunker/README.md](services/chunker/README.md).
  - `processor/`: `ProcessorService.ts`, `ProcessorTaskDefinition.ts` (bakes `STATIC_MAP_USAGE` from the config's
    `preLoadedMaps`), `ProcessorSubscribingLambda.ts`, `TargetSimulator.ts` (the target simulator's HTTP API).
  - `post-processor/`: `PostProcessorService.ts`, `PostProcessorTaskDefinition.ts`, `PostProcessorSubscribingLambda.ts`.
  - `AbstractService.ts`, the services' base. How the phases merge delta storage: [services/README.md](services/README.md).

## Rules and pitfalls

- **Context**: each landscape has `context/context.<landscape>.json` (`IContext` in `context/IContext.ts`); `context.json`
  is the active one, switched with `./context/swap.sh <landscape>` (with the landscape's git-ignored `.env`). Keep
  `PREVIOUS_STORAGE_TYPE` set in every context. No context names a VPC.
- **Values read at deploy time stay there**: what CDK reads from the integration config (e.g. `preLoadedMaps`) is baked
  into the task definitions; nothing the Runner sets at run time reaches it.
- **Names are deterministic** (`${STACK_ID}-<kind>-${landscape}` and the like), so the dashboard and the Runner find
  resources by name. Use no CloudFormation exports between stacks.
- **`src/` must not import from `lib/`**: the task image compiles `src/` without `lib/`, so names both need (the cluster
  and processor service names) live in `src/Utils.ts`, which `lib/` takes.
- **Every synth synthesizes the dashboard stack too**, so it needs AWS credentials and the generated
  `TemplateConstants.ts` (`npm run synth` / `build` make it).

## Testing

`npm test` runs the CDK template tests with the rest (e.g. `test/NetworkStack.test.ts`,
`test/EcsInfrastructureNetwork.test.ts`, `test/RunCounters.test.ts` for the subscriber's grants). Review a change with
`cdk diff` before `npm run deploy-fargate`; the deploy writes the synthesized template to
`cdk.out/synth.output.fargate.yaml`. Validate a run end to end with the Runner (`src/runner/CLAUDE.md`).
