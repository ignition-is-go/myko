//! Report types and registration.

#[cfg(not(target_arch = "wasm32"))]
pub mod cell;
#[cfg(not(target_arch = "wasm32"))]
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

// Re-export entity tree export types
#[cfg(not(target_arch = "wasm32"))]
pub use export_tree::{EntityTreeExport, ExportEntityTree, ExportedEntity};

// Re-export wire types for backwards compatibility
pub use crate::wire::{ReportError, ReportResponse, WrappedReport, wrap_report};
