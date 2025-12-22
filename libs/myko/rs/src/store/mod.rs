//! Store module - Reactive entity storage using hypha cells
//!
//! This module provides the core storage layer for the Myko framework,
//! replacing the actor-based EventHandler system with hypha's reactive cells.
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
//! - [`EntityStore`]: Reactive storage for a single entity type
//! - [`StoreRegistry`]: Central registry managing all entity stores

mod entity_store;
mod registry;

pub use entity_store::EntityStore;
pub use registry::StoreRegistry;
