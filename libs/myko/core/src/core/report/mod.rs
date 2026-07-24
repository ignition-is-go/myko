//! Report types and registration.

// The export request/output types are plain serde types and compile on wasm so
// the client can request `ExportEntityTree`; the report handler + BFS traversal
// (which pull in hyphae + the store registry) stay gated inside the module.
pub mod export_tree;
mod handler;
#[cfg(not(target_arch = "wasm32"))]
mod registration;
mod request;
mod traits;

use std::pin::Pin;

use futures::Stream;

/// Type alias for boxed report streams, reducing verbosity in handler signatures.
pub type ReportStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

// Re-export handler types (available on all platforms for entity impls)
// Re-export entity tree export request/output types (wasm-safe serde types; the
// handler impl lives in export_tree.rs behind a wasm gate).
pub use export_tree::{EntityTreeExport, ExportEntityTree, ExportedEntity};
pub use handler::{ReportContext, ReportHandler};
// Re-export registration types (server-only)
#[cfg(not(target_arch = "wasm32"))]
pub use registration::{
    AnyOutput, ReportCellFactory, ReportFactory, ReportParseFn, ReportRegistration,
};
// Re-export request type
pub use request::ReportRequest;
// Re-export traits
pub use traits::{
    AnyReport, CountResult, MykoReport, Report, ReportId, ReportIdStatic, ReportOutput,
    ReportOutputType, ReportParams,
};

// Re-export wire types for backwards compatibility
pub use crate::wire::{ReportError, ReportResponse, WrappedReport, wrap_report};
