use std::sync::Arc;
use uuid::Uuid;

use crate::store::StoreRegistry;
use crate::sync_client::SyncClient;

// Re-export CellServer as MykoServer for backwards compatibility
pub use crate::cell_server::{
    CellServer, CellServerBuilder, CellServerConfig, CellServerCtx, KafkaConfig,
    PeerRegistryConfig,
};

/// Type alias for backwards compatibility.
pub type MykoServer = CellServer;

/// Type alias for backwards compatibility.
pub type MykoServerBuilder = CellServerBuilder;

/// Server context shared across handlers.
///
/// Contains server identity and shared resources.
pub struct MykoServerCtx {
    /// Unique identifier for this server instance
    pub host_id: Uuid,
    /// Store registry for entity access
    pub registry: Arc<StoreRegistry>,
    /// Sync client for distributed timing (optional)
    pub sync_client: std::sync::OnceLock<Arc<SyncClient>>,
}

impl std::fmt::Debug for MykoServerCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MykoServerCtx")
            .field("host_id", &self.host_id)
            .field("sync_client", &self.sync_client.get().map(|_| "SyncClient"))
            .finish()
    }
}

impl MykoServerCtx {
    /// Create a new server context.
    pub fn new(host_id: Uuid, registry: Arc<StoreRegistry>) -> Self {
        Self {
            host_id,
            registry,
            sync_client: std::sync::OnceLock::new(),
        }
    }

    /// Search for entities matching a query string.
    ///
    /// Returns matching entity IDs (up to `limit` results).
    /// TODO: Implement search using tantivy directly
    pub fn search(&self, _entity_type: &str, _query: &str, _limit: usize) -> Vec<Arc<str>> {
        // Search not yet implemented in cell-based server
        vec![]
    }
}
