//! Query types and registration.

pub mod cell;
mod context;
mod registration;
mod request;
mod traits;

// Re-export all public types
pub use context::MykoServerCtx;
pub use registration::{QueryCellFactory, QueryFactory, QueryParseFn, QueryRegistration};
pub use request::QueryRequest;
pub use traits::{
    AnyQuery, Query, QueryHandler, QueryHandlerCtx, QueryHandlerCtxAny, QueryId, QueryIdStatic,
    QueryItemType, QueryParams,
};
