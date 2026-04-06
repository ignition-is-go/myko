//! Item types, traits, and registration.

#[cfg(not(target_arch = "wasm32"))]
mod cell_map_cast;
mod traits;

#[cfg(not(target_arch = "wasm32"))]
pub use cell_map_cast::{
    apply_map_diff, downcast_any_item_arc, downcast_any_item_map_diff, typed_map_arc_from_any_item,
    typed_map_from_any_item, typed_map_from_any_item_with_typed_id,
};
pub use traits::{
    AnyItem, Eventable, IngestBufferPolicy, IngestBufferRegistration, ItemParseFn, ItemRegistration,
};
