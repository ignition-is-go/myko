//! View registration and metadata.
//!
//! Views are query-like subscriptions with their own registration path.

mod cell;
mod context;
#[cfg(not(target_arch = "wasm32"))]
mod registration;
mod request;
mod traits;

pub use cell::{FilteredViewCellMap, TypedViewCellMap};
pub use context::{ViewBuildContext, ViewContext};
#[cfg(not(target_arch = "wasm32"))]
pub use registration::{ViewCellFactory, ViewFactory, ViewParseFn, ViewRegistration};
pub use request::ViewRequest;
pub use traits::{
    AnyView, ViewBuildArgs, ViewHandler, ViewId, ViewIdStatic, ViewItemType, ViewParams,
};
