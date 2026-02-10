//! Query types and registration.

#[cfg(not(target_arch = "wasm32"))]
pub mod cell;
mod context;
#[cfg(not(target_arch = "wasm32"))]
mod registration;
mod request;
mod traits;

// Re-export all public types
pub use context::QueryContext;
#[cfg(not(target_arch = "wasm32"))]
pub use registration::{QueryCellFactory, QueryFactory, QueryParseFn, QueryRegistration};
pub use request::QueryRequest;
pub use traits::{
    AnyQuery, Query, QueryHandler, QueryId, QueryIdStatic, QueryItemType, QueryParams, QueryTestCtx,
};
