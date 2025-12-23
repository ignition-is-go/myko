//! Minimal server context for query handlers.

use std::sync::Arc;
use uuid::Uuid;

use crate::store::StoreRegistry;

/// Minimal server context provided to query handlers.
///
/// This is a lightweight context that provides queries access to:
/// - Server identity (`host_id`)
/// - Entity stores (`registry`)
///
/// For more capabilities (publishing, relationships), use `CellServerCtx`.
#[derive(Clone)]
pub struct MykoServerCtx {
    pub host_id: Uuid,
    pub registry: Arc<StoreRegistry>,
}

impl MykoServerCtx {
    pub fn new(host_id: Uuid, registry: Arc<StoreRegistry>) -> Self {
        Self { host_id, registry }
    }
}

impl std::fmt::Debug for MykoServerCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MykoServerCtx")
            .field("host_id", &self.host_id)
            .finish()
    }
}
