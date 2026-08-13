//! Generated-language backends.

// Operation index parsing is runtime- and language-neutral. Keep its public
// compatibility re-exports outside the TypeScript renderer.
pub use crate::operation_index::{OperationArg, OperationSchema, build_operation_index};

mod typescript;

pub use typescript::{
    export_registered_ts_types, generate_docs_json_from_bindings, generate_item_types,
};
