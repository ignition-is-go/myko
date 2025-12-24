//! Report types and registration.

pub mod cell;
mod handler;
mod registration;
mod request;
mod traits;

use std::pin::Pin;

use futures::Stream;

/// Type alias for boxed report streams, reducing verbosity in handler signatures.
pub type ReportStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

// Re-export handler types
pub use handler::{ReportContext, ReportHandler};
// Re-export registration types
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

/// Convenience macro for creating report streams.
///
/// This macro wraps `Box::pin(async_stream::stream! { ... })` to reduce boilerplate
/// in `ReportHandler::compute` implementations.
///
/// # Example
///
/// ```ignore
/// impl ReportHandler for MyReport {
///     type Output = MyOutput;
///
///     fn compute(ctx: ReportContext) -> Pin<Box<dyn Stream<Item = Self::Output> + Send>> {
///         report_stream! {
///             let data_stream = ctx.query(MyQuery::default());
///             futures::pin_mut!(data_stream);
///             while let Some(items) = data_stream.next().await {
///                 yield MyOutput { count: items.len() };
///             }
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! report_stream {
    ($($body:tt)*) => {
        Box::pin(async_stream::stream! { $($body)* })
    };
}
