# Observed contracts on handler opens

## Phase

- [x] Ground retained connectors, assignment observations, and application replacement.
- [x] Compare client-side selection with gateway-owned forwarding.
- [x] Select client-side selection after bounded gpt-5.5 review.
- [ ] Implement and verify the open-time contract precondition.
- [ ] Check the result against the design and record remaining selection work.

## Usage

Framework selection inspects a candidate, evaluates compatibility, and opens the
same contract it inspected:

```rust,ignore
let observed = candidate.describe(request.clone()).await?;
// Directional compatibility and assignment/readiness checks remain separate.
let (initial, connection) = candidate.connect_described(request, observed).await?;
```

Applications keep their existing `follow_*_reactive` calls. A future routing-aware
connector repeats selection on each initial open and reconnect. This unit adds
the precondition that such a connector needs, not that connector itself.

## Grounding

`NodeHandlerSubscription` retains its connector and request across reconnects.
`IrohHandlerConnector` currently dials one fixed peer. Queries already separate
event-origin filtering from serving-node selection. Reports and views derive
their source/scope selectors using the connector's target identity.

`ExecutionAssignmentCoordinator::observe` obtains a fresh control observation,
not a reusable execution permit. `DescribeHandler` returns activated generated
schemas after admission and release authorization. Until this change,
`FollowHandler` cannot express which descriptor the caller inspected.
`FederatedSession::set_application` can replace the application between calls.

## Shape

The canonical `FollowHandler` request gains an optional `observed_contract`.
`HandlerOpenRequest` keeps the handler request and its precondition together;
the JSON fields remain flat inside the existing request envelope. Wire version
16 rejects version 15 rather than silently discarding the new precondition.
Direct, uninspected opens use `None`. An inspected open carries its complete
descriptor and requires an exact match before handler setup runs. This is an
optimistic precondition on one observation, not a schema compatibility rule.
Missing schema evidence is an error, never an empty matching contract.

Core checks the active application and opens the handler under one synchronous
read guard. Replacement cannot occur between those two operations. No guard is
held across asynchronous authorization or network waits. Core retains that
application instance for the stream. An application
replacement ends the inspected stream instead of letting it continue under
metadata or authorization from another instance. Application changes wake live
requests through the existing session revision channel, subscribed before
capturing the application so a replacement cannot be missed. Authorization still
precedes release of precondition errors and handler frames.

Wire owns the portable precondition, core enforces it, and Iroh exposes the
described-open operation. Prepared request interpretation must retain the
precondition. No storage role, gateway, assignment, or readiness marker is added.

## Synthesis decision

The main agent and existing gpt-5.5 reviewer compared two whole routing shapes.
The selected shape puts candidate selection in the retained connector and keeps
open-time enforcement on the executing application node. Gateway-owned routing
would add a gateway dependency and forwarding authority requirements, and risks
treating storage nodes as implicit typed-query gateways. It is not selected.

The first implementation step is the open precondition. Choosing a candidate
from a descriptor without checking it at open leaves an application-replacement
race. Echoing the existing descriptor avoids a new digest format and collision
contract. It costs request bytes; existing transport limits still apply.

## Remaining work

This precondition does not compare a client's generated accepted/emitted types
with a candidate's types. Directional compatibility, current assignment checks
at use, per-scope readiness, automatic candidate selection, and typed retry
classification still need implementation. Exact descriptor equality must not
be presented as rolling-update compatibility or completion of AB08/AB19.

## Verification

The wire regression first failed because decoding discarded the observed
contract. The native regressions then failed because a mismatched contract still
opened its handler and a stream remained open after application replacement.
After enforcement, all three native tests pass, including continuation denial
taking precedence over a mismatch. The matching-contract control opens the
handler and observes its setup counter. The mismatch test changes an observed
schema; it does not simulate independently versioned binaries.

Wire tests pass, ten unit tests and four contract tests. Strict Clippy passes for
core, wire, Iroh, local, and node libraries and tests with `myko-node/schema`.
The full core run completed with 263 passes and one failure in
`generated_undirected_neighbors_are_routed_live_and_deduplicated`, which observed
zero rows instead of three. The old focused-rerun and baseline handles were no
longer available when work resumed. A fresh full schema core run passed all 264
tests, including that graph test; this does not establish the original failure's
cause. The old expanded baseline has no recovered terminal result. An earlier baseline
run stopped while the request-type refactor was temporarily missing its re-export;
that run is not a passing gate. A consumer check with only `myko-iroh/schema`
also failed on `RevocationKind: JsonSchema` in authority; enabling the composing
node's schema feature is the verified configuration.
