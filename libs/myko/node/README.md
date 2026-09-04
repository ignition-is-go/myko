# myko-node

`myko-node` is the restartable native-node composition for Myko 7. It combines:

- a transport-neutral `myko-federation::Node`;
- an immutable `myko-redb::RedbJournal` and source-aware peer checkpoints;
- one persistent-identity `myko-iroh::IrohReplicator`; and
- framework-owned `Peer` items with a retained reactive Iroh reconciler.

Opening the same data directory restores the stable Myko node ID, private Iroh
identity, journal, replication cursors, and configured peers. On Unix the Iroh
secret is created with mode `0600`.

```rust,no_run
use std::time::Duration;

use myko_node::Node;

# async fn example() -> Result<(), myko_node::NodeError> {
let node = Node::open("myko-data", Duration::from_secs(1)).await?;
println!("{}", serde_json::to_string_pretty(&node.descriptor())?);
node.shutdown().await?;
# Ok(())
# }
```

`NativeNodeDescriptor` binds the authenticated Iroh endpoint to the stable
Myko `NodeId` expected behind it. Peer relationships are ordinary durable Myko
state in the built-in `FederationService`: `AddPeer`, `RememberPeer`,
`SetPeerReplication`, and `RemovePeer` are typed commands, while `PeersView` and
`PeerReport` are retained live projections. The runtime subscribes to those
items and reconciles Iroh replication tasks; it does not maintain a second
peer-state authority. Existing `peers.json` files are imported once into the
event log, and no new peer configuration is written there.

The rest of the federation control plane uses the same application surface:

- `ConfigureLanDiscovery` changes durable discovery configuration;
  `DiscoverySettingsReport` and `NearbyNodesView` expose it reactively.
- `IssuePairingInvitation`, `RedeemPairingInvitation`, and `ConfirmPairing`
  drive pairing; `PairingRedemptionReport` and `PairingReceiptsView` expose its
  long-lived state.
- `AdvertisedServicesView` and `ServiceCapabilityReport` expose which typed
  services each node can execute so command routing does not depend on
  application-owned transport tables.
- `NodeStatusView` combines local identity with configured peer replication
  status for operator surfaces.

Pinned peers refuse to ingest if an endpoint later advertises another Myko
history. Legacy endpoint-only peers remain supported as explicitly unpinned
bindings. The descriptor is versioned JSON. `IssuePairingInvitation` and
`RedeemPairingInvitation` provide an expiring one-use exchange on a separate
bounded Iroh ALPN, with a mutually identity-bound receipt and six-digit
comparison code. `ConfirmPairing` remembers the opposite descriptor only after
that code is confirmed, with replication paused. `SetPeerReplication` is the
independent, directional decision to ingest that peer's history. Pairing establishes
infrastructure identity knowledge; it never installs an application's
authorization grant. UX may wrap the invitation in a file, ticket, QR, or
discovery encoding without changing the protocol's bound identities.

The crate is deliberately application-neutral. Applications compose their
typed services into `MykoApplication`; `application()` exposes the resulting
command, query, report, and view runtime. `open_with_policy` and its loopback
variant resolve a transport-neutral `AccessPolicy` from restored state before
the Iroh router begins serving, so an application does not expose a permissive
startup window while rebuilding durable authority. Filesystem roots, model
providers, and other application secrets do not belong in this runtime or its
framework peer items.

WebSocket is not part of this composition. An application can expose the same
state through retained `myko-server` when it needs a short-lived edge client;
older wire formats are not preserved.
