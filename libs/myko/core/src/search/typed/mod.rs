//! Per-type search index — replacement for the tantivy-backed `SearchIndex`
//! in the parent module. See `../SPEC.md` for the full design.

mod hit;
mod index;
mod interner;
mod registry;
mod score;
mod searchable;

pub use hit::Hit;
pub use index::{SearchIndex, SearchOptions};
pub use interner::IdInterner;
pub use registry::{DynSearchIndex, SearchRegistry, TypedShim};
pub use score::Score;
pub use searchable::{Searchable, SearchableExtractor};
