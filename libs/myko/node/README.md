# myko-node

`myko-node` is the restartable native-node composition for Myko 7. It combines:

- a transport-neutral `myko-federation::Node`;
- an immutable `myko-redb::RedbJournal` and source-aware peer checkpoints;
- one persistent-identity `myko-iroh::IrohReplicator`; and
- durable peer configuration with automatically restored followers.

Opening the same data directory restores the stable Myko node ID, private Iroh
identity, journal, replication cursors, and configured peers. On Unix the Iroh
secret is created with mode `0600`.

```rust,no_run
use std::time::Duration;

use myko_node::DurableIrohNode;

# async fn example() -> Result<(), myko_node::DurableNodeError> {
let node = DurableIrohNode::open("myko-data", Duration::from_secs(1)).await?;
println!("{}", serde_json::to_string_pretty(&node.descriptor())?);
node.shutdown().await?;
# Ok(())
# }
```

`NativeNodeDescriptor` binds the authenticated Iroh endpoint to the stable
Myko `NodeId` expected behind it. `upsert_peer_descriptor` persists that pair
and refuses to ingest if the endpoint later advertises another Myko history.
Legacy endpoint-only peers remain supported as explicitly unpinned bindings.
The descriptor is versioned JSON. `issue_pairing_invitation` and
`redeem_pairing` provide an expiring one-use exchange on a separate bounded
Iroh ALPN, with a mutually identity-bound receipt and six-digit comparison
code. `confirm_pairing` persists the opposite descriptor only after that code
is confirmed. Pairing establishes infrastructure identity knowledge; it never
installs an application's authorization grant. UX may wrap the invitation in a
file, ticket, QR, or discovery encoding without changing the protocol's bound
identities.

The crate is deliberately application-neutral. Applications project their own
commands, grants, services, and node-local configuration over `node()`, and may
install a transport-neutral `AccessPolicy`. `open_with_policy` and its loopback
variant resolve that policy from the restored Myko node before the Iroh router
begins serving, so an application does not expose a permissive startup window
while rebuilding durable authority. Filesystem roots, model providers, and
other application secrets do not belong in this runtime or its peer file.

WebSocket is not part of this composition. A node may independently wrap the
same Myko node in `myko-websocket-gateway` when it needs a short-lived edge
client interface.
