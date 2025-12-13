//! Full-text search actor for Myko entities.
//!
//! The SearchManager actor provides full-text search capabilities using tantivy.
//! It subscribes to EventBus to keep indices updated as entities change.

mod search_manager;

pub use search_manager::{SearchManager, SearchManagerArgs, SearchManagerMsg, SearchManagerState};
