//! Minimal server context for query handlers.

use std::sync::Arc;

use crate::request::RequestContext;

/// Minimal server context provided to query handlers.
///
/// This is a lightweight context that provides queries access to:
/// - Server identity (`host_id`)
/// - Entity stores (`registry`)
///
/// For more capabilities (publishing, relationships), use `CellServerCtx`.
#[derive(Clone, Debug)]
pub struct QueryContext {
    pub req: Arc<RequestContext>,
}
