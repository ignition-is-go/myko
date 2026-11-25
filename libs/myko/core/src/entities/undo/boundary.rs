use crate::{command::CommandContext, prelude::*};
use std::sync::Arc;
/// Durable start of an undo epoch. Only internal server provenance establishes it.
#[myko_item]
pub struct UndoHistoryBoundary {}

/// Establish an application-selected undo boundary after a startup migration.
/// # Errors
/// Returns persistence errors; callers must finish migration before calling this.
pub fn begin_undo_history(
    server: &Arc<crate::server::MykoServerContext>,
    id: &str,
) -> Result<bool, String> {
    server.registry.transactions.coordinate(true, || {
        server.flush_history_writes()?;
        let provider = server
            .history_replay()
            .ok_or("durable undo history is unavailable")?;
        if provider.undo_boundary_exists(id)? {
            return Ok(false);
        }
        server.require_undo_backend(UndoHistoryBoundary::ENTITY_NAME_STATIC)?;
        let request = Arc::new(crate::request::RequestContext::internal(
            Uuid::new_v4().to_string().into(),
            server.host_id,
            "undo-boundary",
        ));
        let ctx = CommandContext::new("myko:undo-history-boundary".into(), request, server.clone());
        ctx.emit_set(&UndoHistoryBoundary { id: id.into() })
            .map_err(|error| error.message)?;
        drop(ctx);
        server.flush_history_writes()?;
        Ok(true)
    })
}
