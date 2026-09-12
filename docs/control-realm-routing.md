# Control-realm routing

## Work phases

- Ground: complete. Local and Iroh clients send control frames to the shared
  `FederatedSession`, which currently holds one endpoint. Concrete endpoints
  authenticate callers and validate history before durable voting.
- Sketch: complete. Alternatives and usage are below.
- Agree: proceed without a human checkpoint or new agents.
- Implement: complete for realm-addressed dispatch and caller migration.
- Scrap review: retained the explicit-target design after the isolation tests.

## Usage

```rust
sessions.set_control_endpoint(execution_realm.clone(), Some(execution_endpoint))?;
sessions.set_control_endpoint(authority_realm.clone(), Some(authority_endpoint))?;
let target = ControlTarget { realm: execution_realm, head };
let promise = client.prepare_control(target, ballot).await?;
```

## Shape and rationale

`ControlTarget` carries the realm and predecessor head. The wire requires it for
prepare, propose, and accept. `FederatedSession` indexes endpoints by realm,
selects one endpoint, and forwards the authenticated presentation unchanged.
The concrete endpoint rejects a target outside its own independently configured
anchor before any durable vote. The generic endpoint API is named
`ControlEndpoint`; no authority-specific aliases remain.

Epoch and electorate still come from verified retained history. A client-supplied
target is an address, not a trust anchor or freshness proof. In particular, two
realms may have the same configured genesis head. Head-only dispatch is ambiguous.
Unknown realms fail without falling back to another endpoint. Removing one realm
does not remove other controllers from the shared session. Already-dispatched
calls keep their selected endpoint; removal is not cancellation of an in-flight
vote or revocation of previously persisted evidence.

This keeps the routing boundary small, per boundary discipline. The target is
required by types instead of an optional connection setting. Application handlers,
typed projections, storage placement, and default controller enrollment do not
change.

## Alternatives and synthesis

The selected design carries an explicit target on each call. A realm-bound
control-client object would hide repetition, but would add parallel local and
Iroh wrapper APIs around the same three calls. Per-connection realm selection
would prevent one shared session from addressing both realms. Trying installed
controllers in order would risk side effects and ambiguous head selection.

We accept explicit addressing on low-level coordinator calls in exchange for
one request shape across transports and in-process endpoints. The coordinator
already owns the realm and can construct the target for its callers.

## Verification and remaining work

`authority/tests/control_realms.rs` exercises both application-authority and
execution controllers on one session, colliding genesis heads, unknown realm
refusal, cross-realm proposal rejection without writes, and removal of one realm
without affecting another. A new authority promise also succeeds after the
journal retains a chosen execution assignment. Alias-registration tests prove
that both concrete endpoint types check their independently configured realm.

Both tests passed. The existing 54 coordinator tests and three controller-endpoint
tests passed, including native Iroh callers. The expanded baseline passed with
31 local transport tests, the new realm tests, and native retained-history
recovery. Wire schema 13 requires the target; a serialization test rejects a
missing realm and caller-supplied anchor fields. All Myko Rust callers migrated,
and a source search found no old generic endpoint names or head-only wire forms.
No compatibility routing is retained.

Strict Clippy passed for core, federation, wire, local transport, Iroh, authority,
and node libraries and tests. No lint exemptions were added. The post-migration
local assignment test also rechecks the projected assignment after journal reopen.

Current assignment evidence, coordinator recovery, operator-intent authorization,
and ready-executor failover remain separate requirements. This routing change
does not establish them.
