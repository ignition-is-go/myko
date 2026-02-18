//! View registration and metadata.
//!
//! Views are query-like subscriptions with their own registration path.

mod cell;
mod context;
mod registration;
mod request;
mod traits;

pub use cell::{FilteredViewCellMap, TypedViewCellMap};
pub use context::{ViewCellContext, ViewContext};
pub use registration::{ViewCellFactory, ViewFactory, ViewParseFn, ViewRegistration};
pub use request::ViewRequest;
pub use traits::{
    AnyView, ViewBuildCellCtx, ViewHandler, ViewId, ViewIdStatic, ViewItemType, ViewParams,
};
