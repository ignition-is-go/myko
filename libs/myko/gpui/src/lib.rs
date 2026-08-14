//! GPUI bridge for Myko's live client cells.
//!
//! Hyphae notifications are bridged by `hyphae-gpui` onto GPUI's foreground
//! executor. This crate layers Myko-specific loading, connection, and error
//! semantics over those event-driven entities.

mod client;
mod command;
mod components;
mod crud;
mod remote;
mod render;

pub use client::{Myko, disconnect_myko, myko, provide_myko};
pub use command::{
    Command, CommandBoundary, CommandHooks, CommandSlot, CommandState, command, command_boundary,
    observe_command, observe_command_in, on_command_change,
};
pub use components::{
    FineQueryList, RemoteBoundary, fine_query_list, fine_query_list_from_store_with_key,
    query_boundary, remote_boundary, report_boundary, view_boundary,
};
pub use crud::{CrudCommands, CrudController, CrudRowActions};
// Consumers that own application startup can use the same native/web platform
// facade pinned with this crate instead of introducing a second Zed revision.
pub use gpui_platform;
pub use hyphae_gpui::MapEntry;
pub use myko::client::ConnectionStatus;
pub use remote::{
    LoadState, QueryStore, Remote, connection_status, live_query, live_query_store, live_report,
    live_view, live_view_store, observe_crud_store, observe_query_store, observe_remote, ping_ms,
    send_command,
};
pub use render::{RemoteRender, render_remote, render_remote_list};
