//! Peer discovery and federation manager.
//!
//! The [`PeerManager`] actor handles:
//! - Discovering peer servers via `GetPeerServers` query
//! - Connecting to peers via WebSocket
//! - Verifying peer identity via `GetConnectedServer` query
//! - Forwarding queries/commands/reports to peers
//! - Tracking peer health status
//!
//! # Architecture
//!
//! ```text
//! PeerManager
//!    ├── Subscribes to GetPeerServers (discovers new peers)
//!    ├── For each peer:
//!    │      ├── MykoClient connection
//!    │      ├── GetConnectedServer verification
//!    │      └── Health tracking (last_seen, latency)
//!    └── Federation: ForwardQuery, ForwardCommand, ForwardReport
//! ```
//!
//! Note: Event replication is handled by Kafka, not direct peer subscription.

mod peer_manager;

pub use peer_manager::{PeerManager, PeerManagerArgs, PeerManagerMsg, PeerStatus};
