# Handler contract exchange

## Phase

- [x] Ground the retained connector, activated schema registry, and authorization.
- [x] Compare descriptor requests with subscription-time negotiation.
- [x] Select descriptor requests after bounded gpt-5.5 review.
- [x] Implement and verify canonical transport exchange.
- [x] Check the result against the design and record remaining failover work.

## Usage

Framework routing inspects a candidate application executor before deciding
whether it can serve a logical subscription:

```rust,ignore
let contract = connector.describe(request.clone()).await?;
// Compare the generated argument and result contracts directionally.
// Assignment and scope readiness require independent, use-time checks.
```

The application author registers typed handlers as before. Schema-enabled builds
provide the metadata automatically. A build without schema evidence rejects the
descriptor request rather than returning an empty contract.

## Problem and existing flow

`NodeHandlerSubscription` retains its connector and request during reconnect.
`IrohHandlerConnector` currently dials a fixed endpoint. Reconnecting scalar and
keyed handles already retain coherent values while a peer is resynchronizing.
Selecting a different executor still needs assignment, compatibility, and scope
readiness evidence. `ExecutionAssignmentCoordinator::observe` supplies a fresh
observation point, not a reusable execution permit.

`MykoApplication::service_contract` collects schemas from activated registrations.
The new descriptor request gives those schemas a node transport path. Advertised
service names and storage custody cannot substitute for generated application
contracts.

## Shape

`DescribeHandler { request: HandlerRequest }` is a finite canonical node request.
Its `HandlerContract` response binds the serving node, service owner, handler kind,
and handler identifier to generated argument and result schemas. Result metadata
distinguishes one value from keyed rows. Each type retains separate emitted and
accepted schemas. Wire schema documents admit JSON Schema object and boolean
roots; this does not validate all schema keywords or prove compatibility.

`FederatedSession` uses the same typed parameters, claims, capabilities, and
authorization preparation as `FollowHandler`. The activated registry supplies
the schema provider. Descriptor lookup does not run a handler or open a source.
The client rejects response identity or result-shape mismatches as protocol
errors. The existing authenticated transport binds the response to the endpoint.

Admission authorization happens before dispatch. The release-time continuation
check guards both the descriptor and schema-construction errors. Missing schema
evidence must not expose a construction error after access has been revoked.

Storage-only nodes have no application registry and cannot describe typed
handlers. An unknown owner, inactive service, missing handler, or absent schema
fails explicitly. A successful response does not grant permission to open a
subscription later. Application replacement and authorization changes still need
validation at that later use.

This addition changes the canonical wire protocol from version 14 to 15. Peers
using the old version fail explicitly; this unit adds no compatibility adapter.

## Synthesis decision

The main agent and the existing gpt-5.5 reviewer compared two shapes. A finite
descriptor request separates collecting evidence from deciding execution
eligibility. Subscription-time schema negotiation would require compatibility,
readiness, and assignment fencing inside the stream-open path before those
contracts are implemented. Use the finite descriptor request now; do not turn
schema equality into a replacement for directional compatibility.

The wire owns portable descriptor data, not Schemars generators or application
types. Core converts generated schemas at the application boundary. Native
transport exposes one descriptor operation and validates its response.

## Verification and remaining work

The wire suite passes all ten unit tests and three descriptor tests. Native
descriptor tests pass with schema enabled, five tests, and disabled, four tests.
They cover authenticated identity and typed claims, query/report/view metadata,
inactive and storage-only rejection, malformed requests, and missing schemas.
The report and view fixtures verify that description does not execute handlers.

The release-order regression first failed because a missing-schema error escaped
instead of the continuation denial. Keeping the construction result until after
the release check makes the same native test pass. This narrows the original
gpt-5.5 review finding: admission already ran before dispatch; the missing check
was on release of construction errors, not initial authorization.

Strict Clippy passes for core, wire, Iroh, and local libraries and tests with
schema enabled. The native Iroh library suite passes all 31 tests. Targeted
formatting, baseline shell syntax, and whitespace checks pass.

The expanded application-builder baseline passes. Its descriptor stages ran
before the final release-order correction; the focused native suites above
verify that correction in both feature configurations. The existing gpt-5.5
reviewer accepted the bounded implementation and recorded the remaining limits.

The wider server/Swift consumer check is not clean. Swift native FFI has a report
subscription generic mismatch at `native_ffi/federation.rs:183` and an unmatched
`AdministerExecution` operation at `native_ffi/authority.rs:611`. Those API
migrations remain separate work; this descriptor checkpoint does not establish
workspace or Apple-platform compatibility.

Wire descriptor evidence into assignment-aware executor selection, directional
compatibility, and per-scope readiness next. This unit alone does not close AB08,
AB09, AB19, or the first milestone.
