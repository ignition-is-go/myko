//! Store module - Reactive entity storage using hyphae cells
//!
//! This module provides the core storage layer for the Myko framework,
//! replacing the actor-based `EventHandler` system with hyphae's reactive cells.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │                     StoreRegistry                        │
//! │  ┌─────────────────┐  ┌─────────────────┐               │
//! │  │ EntityStore     │  │ EntityStore     │  ...          │
//! │  │ (Target)        │  │ (Scene)         │               │
//! │  │                 │  │                 │               │
//! │  │ CellMap<id,item>│  │ CellMap<id,item>│               │
//! │  │ .diffs() cell   │  │ .diffs() cell   │               │
//! │  │ .entries() cell │  │ .entries() cell │               │
//! │  └─────────────────┘  └─────────────────┘               │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Types
//!
//! - [`EntityStore`]: Type alias for `CellMap<Arc<str>, Arc<dyn AnyItem>>`
//! - [`StoreRegistry`]: Central registry managing all entity stores

mod entity_store;
mod registry;

pub use entity_store::{EntityDiffCell, EntityEntriesCell, EntityStore};
pub use registry::StoreRegistry;
