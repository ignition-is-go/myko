//! # Myko RS - Event-Sourcing CQRS Framework
//!
//! `myko-rs` is an actor-based event-sourcing framework for building real-time,
//! distributed systems with strong consistency guarantees.
//!
//! ## Core Concepts
//!
//! | Concept | Description |
//! |---------|-------------|
//! | **Item** | Base entity with typed `id` |
//! | **Event** | Immutable record of state change: `SET` (create/update) or `DEL` (delete) |
//! | **Query** | Request for live data stream, returns reactive `Observable<T[]>` |
//! | **Report** | Computed/derived data request, returns `Observable<T>` |
//! | **Command** | Intent to mutate state, returns result of operation |
//! | **Saga** | Stateful stream processor that reacts to events and emits commands |
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                             CellServer                                   │
//! │                                                                          │
//! │  WebSocket ──► WsHandler ──► CellServerCtx ──► StoreRegistry             │
//! │      │              │             │                   │                  │
//! │      │              │             │             CellMap<id, item>        │
//! │      │              ▼             │                   │                  │
//! │      │        KafkaProducer       │                   ▼                  │
//! │      │              │             │         Query/Report cells           │
//! │      │              ▼             │                   │                  │
//! │      │           Kafka ◄─────────────── KafkaConsumer                    │
//! │      │                                                                   │
//! │      ◄────────────────────── (subscription updates)                      │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! Define an entity using the `#[myko_item]` attribute macro:
//!
//! ```text
//! use myko_rs::prelude::*;
//!
//! #[myko_item]
//! pub struct Target {
//!     pub name: String,
//!     pub category: Option<String>,
//!     // id is added automatically
//! }
//! ```
//!
//! The macro auto-generates:
//! - `GetAllTargets`, `GetTargetsByIds`, `GetTargetsByQuery` queries
//! - `CountAllTargets`, `CountTargets`, `GetTargetById` reports
//! - `DeleteTarget`, `DeleteTargets` commands
//! - `PartialTarget` struct for partial matching
//! - Registration with the [`inventory`] system
//!
//! ## Module Guide
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`client`] | WebSocket client for connecting to Myko servers |
//! | [`core`] | Core types: command, query, report, saga, item, relationship |
//! | [`wire`] | Wire protocol types: MykoMessage, MEvent, responses, errors |
//! | [`server`] | CellServer and server context |
//! | [`store`] | Entity store and registry |
//!
//! ## Performance
//!
//! Myko-rs is optimized for high-throughput, low-latency scenarios:
//!
//! - **Hyphae cells**: Reactive queries and reports using the hyphae cell library
//! - **Lock-free stores**: CellMap for concurrent entity access
//! - **MessagePack serialization**: Binary format for efficient WebSocket communication
//! - **Optional Kafka**: Run in-memory for development, add Kafka for production persistence
//!
//! See `libs/myko/rs/OPTIMIZATION.md` for detailed performance guidelines.

// Main module structure
pub mod cache;
pub mod client;
#[cfg(not(target_arch = "wasm32"))]
pub mod codegen;
pub mod codegen_types;
pub mod core;
pub mod entities;
#[cfg(not(target_arch = "wasm32"))]
pub mod search;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub mod store;
pub mod utils;
pub mod wire;

pub mod prelude;

#[cfg(feature = "bench")]
pub mod bench_entities;

/// Shared websocket sizing used by Myko client/server on native platforms.
pub const WS_MAX_MESSAGE_SIZE_BYTES: usize = 64 * 1024 * 1024;
pub const WS_MAX_FRAME_SIZE_BYTES: usize = 64 * 1024 * 1024;

// Re-export core modules at top level for backwards compatibility
#[cfg(not(target_arch = "wasm32"))]
pub use core::saga;
pub use core::{command, common, item, query, relationship, report, request, view};

// Re-export crates for use in macros
pub use futures; // For proc macro generated stream adapters in typed sagas
pub use hyphae; // For cell-based queries/reports in #[myko_item]
pub use inventory;
pub use inventory::submit; // For myko_rs::submit! macro
pub use partially; // For #[derive(partially::Partial)] in #[myko_item]
pub use serde; // For #[derive(serde::Serialize, serde::Deserialize)] in #[myko_item]
pub use serde_json; // For proc macro generated serde_json::from_value in typed sagas
pub use ts_rs::{self, TS};
// Re-export wire types at top level for backwards compatibility
pub use wire::event; // For #[derive(myko_rs::TS)]

/// Register a type for ts-rs export.
#[macro_export]
macro_rules! register_ts_export {
    ($ty:ty) => {
        $crate::inventory::submit! {
            $crate::codegen_types::TsExportRegistration {
                type_name: stringify!($ty),
                export_fn: || <$ty as $crate::ts_rs::TS>::export(),
            }
        }
    };
    ($($ty:ty),+ $(,)?) => {
        $(
            $crate::register_ts_export!($ty);
        )+
    };
}

/// Define a Rust constant and register it for TypeScript export.
#[macro_export]
macro_rules! ts_const {
    (pub $name:ident : &str = $value:expr) => {
        pub const $name: &str = $value;
        $crate::inventory::submit! {
            $crate::codegen_types::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen_types::TsConstValue::Str($value),
                crate_name: module_path!(),
            }
        }
    };
    ($name:ident : &str = $value:expr) => {
        const $name: &str = $value;
        $crate::inventory::submit! {
            $crate::codegen_types::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen_types::TsConstValue::Str($value),
                crate_name: module_path!(),
            }
        }
    };
    (pub $name:ident : i64 = $value:expr) => {
        pub const $name: i64 = $value;
        $crate::inventory::submit! {
            $crate::codegen_types::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen_types::TsConstValue::Int($value),
                crate_name: module_path!(),
            }
        }
    };
    ($name:ident : i64 = $value:expr) => {
        const $name: i64 = $value;
        $crate::inventory::submit! {
            $crate::codegen_types::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen_types::TsConstValue::Int($value),
                crate_name: module_path!(),
            }
        }
    };
    (pub $name:ident : bool = $value:expr) => {
        pub const $name: bool = $value;
        $crate::inventory::submit! {
            $crate::codegen_types::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen_types::TsConstValue::Bool($value),
                crate_name: module_path!(),
            }
        }
    };
    ($name:ident : bool = $value:expr) => {
        const $name: bool = $value;
        $crate::inventory::submit! {
            $crate::codegen_types::TsConstRegistration {
                name: stringify!($name),
                value: $crate::codegen_types::TsConstValue::Bool($value),
                crate_name: module_path!(),
            }
        }
    };
}

/// Helper macro for submitting message event registrations
#[macro_export]
macro_rules! submit_message_event {
    ($variant:ident, $event:expr) => {
        inventory::submit!($crate::wire::MessageEventRegistration {
            variant_name: stringify!($variant),
            event_value: $event,
        });
    };
}
