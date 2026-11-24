//! View registration and metadata.
//!
//! Views are query-like subscriptions with their own registration path.

mod cell;
mod context;
#[cfg(test)]
mod erase_tests;
mod merged;
mod registration;
mod request;
#[cfg(test)]
mod switched_tests;
mod traits;

pub use cell::{ErasedViewDiff, ErasedViewMap, FilteredViewCellMap, TypedViewCellMap};
pub use context::{ViewBuildContext, ViewContext};
pub use registration::{
    ViewCellFactory, ViewFactory, ViewParseFn, ViewRegistration, ViewSubscribeFactory,
    ViewSubscribeRegistration,
};
pub use request::ViewRequest;
pub use traits::{
    AnyView, ViewBuildArgs, ViewHandler, ViewId, ViewIdStatic, ViewItemType, ViewParams,
};
