//! GPUI bridge for Myko's live client cells.
//!
//! Scalar Hyphae notifications use the general `hyphae-gpui` foreground
//! bridge. Query and view maps use a Myko-owned projection so response
//! readiness, reconnect generations, and stable row entities cross into GPUI
//! through one response-generation boundary.

mod client;
mod command;
mod components;
mod crud;
mod event;
mod remote;
mod render;

pub use client::{Myko, disconnect_myko, myko, provide_myko};
pub use command::{
    Command, CommandBoundary, CommandEvent, CommandHooks, CommandSlot, CommandState,
    CommandTracker, command, command_boundary, observe_command, observe_command_in,
    on_command_change, on_command_lifecycle,
};
pub use components::{
    FineQueryList, RemoteBoundary, fine_query_list, fine_query_list_from_store_with_key,
    query_boundary, remote_boundary, report_boundary, view_boundary,
};
pub use crud::{CrudCommands, CrudController, CrudRowActions};
pub use event::{send_delete_event, send_set_event};
// Consumers that own application startup can use the same native/web platform
// facade pinned with this crate instead of introducing a second Zed revision.
pub use gpui_platform;
pub use myko::client::{CommandLifecycle, ConnectionStatus};
pub use remote::{
    ClientReportSpec, LoadState, MapEntry, QueryStore, Remote, connection_status,
    connection_status_in, live_client_report_in, live_query, live_query_in, live_query_store,
    live_report, live_report_in, live_view, live_view_store, observe_crud_store,
    observe_query_store, observe_remote, ping_ms, send_command,
};
pub use render::{RemoteRender, render_remote, render_remote_list};

/// Runs this crate's tests one at a time.
///
/// A subscriber callback runs on whichever thread is delivering writes, and
/// GPUI's test scheduler rejects a wake-up from any thread but its own. Two
/// tests in one process could otherwise wake each other's scheduler.
#[cfg(test)]
pub(crate) fn test_serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
