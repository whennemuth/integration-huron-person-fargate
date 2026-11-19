# The Target Simulator

The target simulator stands in for the Huron person API in a mock landscape (see
[MOCK_LANDSCAPES.md](MOCK_LANDSCAPES.md)). Read this before changing the simulator, its front door (API Gateway), or
the Huron client calls it must answer; and after any change to its front door, probe the deployed endpoint as
described at the end.

## What it is

`src/processing/TargetSimulator.ts` (CDK: `lib/services/processor/TargetSimulator.ts`): a Lambda behind an API Gateway
HTTP API that impersonates the subset of the Huron person API the pipeline uses - token request,
`POST`/`PATCH`/`GET /api/v2/persons[/{hrn}]` with filters/paging - storing persons in the mock target table
(`${STACK_ID}-mock-target-person-${landscape}`).

CDK injects its endpoint (and a generated external token, from a separate `.../target-simulator/token/<landscape>`
secret, via a CloudFormation dynamic reference) into the mock stack's integration config secret as
`dataTarget.endpointConfig.baseUrl`/`externalToken`. So the ECS tasks run the ordinary
`HuronPersonDataTarget`/`ReadPerson`/`ListPeople` code against it, unaware it is a fake.

- The simulator reads its token from its own secret, never the config secret, keeping the CloudFormation dependency
  one-way (config secret -> simulator).
- `MockPersonDataTarget`, `DataTargetFlags.useMockTarget` and Upsert's mock branch have been removed from
  integration-huron-person; the target simulator is the only mock target.

## Contract quirks it must honor

See the file header and `test/TargetSimulator.test.ts`, which drives the real client classes against it:

- "not found" is `200 {data: []}`;
- `pagination[offset]` is a page index, and a page past the end is `200 {data: []}`;
- `filter[...includeInactive...]` is a flag;
- hrns (`hrn:hrs:persons:<sourceIdentifier>`) are derived, so `PATCH /{hrn}` needs no index.

DELETE (`PATCH {active:false}`) is a soft delete (`deactivated`/`deactivatedAt` + `data.active=false`); records are
never removed, matching Huron's soft-delete-only behavior.

## Why an HTTP API, not a Lambda Function URL

The Huron client deliberately sends UNENCODED `[ ]` in query strings (`pagination[offset]=0`,
`filter[0!sourceIdentifier!and]=...` - see integration-huron-person `UrlSerializer.ts`; the real Huron API rejects the
encoded form). A Function URL rejects such requests with `400 {"message":null}` before ever invoking the function (the
first mock run failed this way).

## Probing the front door after a change

The local contract test cannot catch front-door strictness like this (Node's HTTP server accepts anything), so after
any change to the simulator's front door, probe the deployed endpoint:

```bash
curl -g "<endpoint>/api/v2/persons?pagination[offset]=0"
```

It must return the simulator's own `401 {"errors":[...]}`, not an AWS-generated 400.
