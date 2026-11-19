# The Shared Network Stack

Every landscape's pipeline runs in one network, owned by a stack of its own, `huron-integration-network`
(`lib/NetworkStack.ts`), one per account and region. This doc covers what it owns, how pipelines reach it, why it
exists (the 2026-11-12 incident), and how a network was imported into it. The rule that follows from it - never let a
landscape create or look up a VPC - is in the project `CLAUDE.md`. Read this before changing `lib/NetworkStack.ts`,
`NetworkParameters`, `EcsInfrastructure`'s VPC, or before any teardown that could touch the network.

## Why one shared network

The region's quotas of 5 VPCs and 5 internet gateways (both full in us-east-2) are why the landscapes share one
network rather than each having its own.

## What it owns

The VPC (10.0.0.0/16), public and private `/24` subnets in the region's `a` and `b` zones, the internet gateway, a
route table per subnet, the elastic IP and one NAT gateway (in the first public subnet).

The layout is in code (`NetworkLayout`), not in any context, since every landscape's CDK app synthesizes the same
network stack and a deploy from any of them must leave it as it is. Termination protection is on, and every resource is
retained (`RETAIN`), so removing the network takes deliberate steps.

## No landscape owns it

`bin/App.ts` makes it the pipeline stack's dependency. `cdk deploy` of a pipeline stack deploys it first (creating it
in a brand-new account, a no-op otherwise); `cdk destroy` of a pipeline stack never selects it (`cdk destroy` adds a
stack's dependents, `cdk deploy`/`diff` its dependencies). So `npm run deploy-fargate` and `teardown-fargate` need
nothing extra.

## How a pipeline gets it

The network stack publishes its IDs as SSM parameters (`/huron-integration-network/vpc-id`,
`.../public-subnet-ids/<az>`, `.../private-subnet-ids/<az>`; `NetworkParameters`), and `EcsInfrastructure` builds its
VPC with `NetworkParameters.vpc()` (`Vpc.fromVpcAttributes` over parameters CloudFormation resolves at each deploy).
The ECS services run in the private subnets, their security groups in the VPC.

Nothing is created or looked up at synth: no `cdk.context.json` cache of subnets, and no CloudFormation exports, whose
imports would stop the network stack from ever changing a subnet. The subscriber Lambdas aren't in the VPC.

## History: the 2026-11-12 incident

Until 2026-11-12 `EcsInfrastructure` created or looked up a VPC itself: with `ECS.vpcId` in a context it looked one up
(`Vpc.fromLookup`, whose cache kept subnet IDs after the subnets were gone); without it, it created one, retaining only
the VPC. Staging's context had no `vpcId`, so staging's July stack created the VPC preview, mock and migration looked
up, and tearing staging down deleted the subnets, gateways and NAT gateway they all ran in. The network was rebuilt by
hand, then imported into the network stack (`cdk import`), so its IDs didn't change.

Every landscape is now on the parameters: migration was the last, its July pipeline stack (on the subnets deleted on
2026-11-12) recreated on 2026-11-19, with its first dashboard stack.

## Importing a network

As done once in this account:

- The network stack has no custom resource (`restrictDefaultSecurityGroup: false`) and no CDK metadata resource
  (`analyticsReporting: false`), since an import creates nothing besides what it imports.
- Import with a resource-mapping file (`cdk import huron-integration-network --resource-mapping <file>`); the
  parameters are skipped by the import and created by an ordinary deploy after it.
- A gateway attachment's identifier is `AttachmentType: IGW` (not `igw`) plus the VPC ID, and a route's is its route
  table ID plus `0.0.0.0/0`.
- The layout has a route table per public subnet; resources imported with other tags show as drift until their tags
  are set to the stack's.

## Tests

`test/NetworkStack.test.ts` (the layout, retained resources, protection, parameters, tags, the dependency),
`test/EcsInfrastructureNetwork.test.ts` (the pipeline's services from the parameters).
