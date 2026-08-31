# myko-redb

`myko-redb` is the opt-in embedded durability adapter for the transport-neutral
Myko 7 federation foundation.

It stores a stable node identity and the complete node-local immutable event
log in Redb using immediate-durability transactions. It also stores monotonic,
transport-scoped peer cursors as node-local operational metadata.
`myko-federation` rebuilds its command and graph projections from history before
exposing the node. A disk transaction completes before an event becomes visible
to local subscribers or receives a `CommittedLocally` lifecycle state.

```rust
let node = myko_redb::RedbJournal::open_node("myko.redb")?;
```

Applications that also supervise durable replication retain the journal handle:

```rust
let (node, journal) =
    myko_redb::RedbJournal::open_node_with_journal("myko.redb")?;
let follower = iroh.follow_persisted(peer, journal, retry_interval)?;
```

The adapter has no WebSocket, HTTP, Tokio, or Iroh dependency. Replication and
short-lived client gateways remain separate optional adapters.
