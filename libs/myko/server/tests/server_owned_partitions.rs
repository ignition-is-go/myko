//! Claiming orphaned `#[server_owned]` rows of a partitioned entity type.
//!
//! Startup runs `ServerOwnershipManager::claim_orphaned` on the root context.
//! A partitioned type's rows live only in its partitions, so reading it from
//! the root store panics; the claim must read and write each row in the
//! partition that holds it.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use myko::{
    entities::server::{Server, ServerId},
    item::AnyItem,
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, PartitionKey, PartitionSchema,
        RelationshipManager, persister::PersisterRouter,
    },
    store::StoreRegistry,
};
use myko_server::ServerOwnershipManager;
use uuid::Uuid;

pub mod session {
    use myko::prelude::*;

    /// Shared. Setting one opens the partition named by its id.
    #[myko_item]
    pub struct SoSession {
        pub label: String,
    }
}

pub mod active {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Partitioned by session, and owned by a server.
    #[myko_item]
    pub struct SoActive {
        #[myko_partition_key]
        pub session: Arc<str>,
        #[server_owned]
        pub server_id: Arc<str>,
    }
}

use active::SoActive;
use session::SoSession;

fn root_context(host: Uuid) -> MykoServerContext {
    MykoServerContext::new(
        host,
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    )
    .with_partitions(PartitionSchema::new().lifecycle::<SoSession>())
}

#[test]
fn an_orphaned_row_in_a_partition_is_claimed_without_a_root_read() {
    let host = Uuid::new_v4();
    let root = root_context(host);
    let live: Arc<str> = Arc::from(host.to_string());
    root.set(&Server {
        id: ServerId::from(Arc::clone(&live)),
        version: "1".into(),
        address: "127.0.0.1".into(),
        port: 0,
        started_at: "2026-11-25T00:00:00Z".into(),
    })
    .expect("live server");
    root.set(&SoSession {
        id: "s1".into(),
        label: "s1".into(),
    })
    .expect("session");
    let s1 = root
        .partition(&PartitionKey::new("s1"))
        .expect("partition s1");
    s1.set(&SoActive {
        id: "row".into(),
        session: "s1".into(),
        server_id: "gone".into(),
    })
    .expect("orphaned row");
    root.settle();

    assert!(root.is_partitioned("SoActive"));
    ServerOwnershipManager::claim_orphaned(&root).expect("claim");
    root.settle();

    let rows = root.partitioned_snapshot("SoActive");
    let owners: Vec<Option<String>> = rows
        .iter()
        .flat_map(|(_, items)| items.iter())
        .map(|item| AnyItem::server_owner(item.as_ref()).map(str::to_owned))
        .collect();
    assert_eq!(owners, vec![Some(live.to_string())]);
}
