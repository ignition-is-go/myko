//! # Myko RS - Event-Sourcing CQRS Framework
//!
//! `myko` is an actor-based event-sourcing framework for building real-time,
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
//! │      │         Persister          │                   ▼                  │
//! │      │              │             │         Query/Report cells           │
//! │      │              ▼             │                   │                  │
//! │      │       Durable Backend ◄────────── Consumer                        │
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
//! use myko::prelude::*;
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
//! - **Pluggable persistence**: Run in-memory for development, add Postgres for production persistence
//!
//! See `libs/myko/rs/OPTIMIZATION.md` for detailed performance guidelines.

// Main module structure
pub mod cache;
pub mod client;
#[cfg(all(not(target_arch = "wasm32"), feature = "codegen"))]
pub mod codegen;
pub mod codegen_types;
pub mod core;
pub mod entities;
#[cfg(not(target_arch = "wasm32"))]
pub mod search;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;
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

pub use erased_serde; // For AnyItem::erased_serialize in generated code
// Re-export crates for use in macros
pub use futures; // For proc macro generated stream adapters in typed sagas
pub use hyphae; // For cell-based queries/reports in #[myko_item]
pub use inventory;
pub use inventory::submit; // For myko::submit! macro
// Re-export all attribute/derive macros so downstream crates can consume them
// as `myko::myko_item`, `myko::myko_subtype`, etc. without adding a separate
// `myko-macros` dependency.
pub use myko_macros::*;
pub use partially; // For #[derive(partially::Partial)] in #[myko_item]
pub use serde; // For #[derive(serde::Serialize, serde::Deserialize)] in #[myko_item]
pub use serde_json; // For proc macro generated serde_json::from_value in typed sagas
pub use ts_rs;
// `myko::TS` resolves to the real `ts_rs::TS` derive+trait when the
// consuming crate has `ts-export` on, and to a noop derive that emits
// nothing (but still claims the `#[ts(...)]` helper attrs so they don't
// become orphan attributes) when off. Routing everything through
// `myko::TS` lets entity crates opt out of the expensive derive without
// touching their source — hand-written `#[derive(myko::TS)]` becomes
// a no-op, and macro-emitted `#[cfg_attr(feature = "ts-export", derive(myko::TS))]`
// doesn't run the derive at all.
#[cfg(feature = "ts-export")]
pub use ts_rs::TS;
#[cfg(not(feature = "ts-export"))]
pub use myko_macros::TsNoop as TS;
// Re-export wire types at top level for backwards compatibility
pub use wire::event; // For #[derive(myko::TS)]

/// Register a type for ts-rs export.
///
/// When the consuming crate has `ts-export` off, this expands to nothing —
/// the registered fn would require `$ty: ts_rs::TS` but we don't emit
/// that impl in that configuration. Types that should be exported during
/// typegen must pick up `ts-export` via their crate's feature forwarding.
#[macro_export]
macro_rules! register_ts_export {
    ($ty:ty) => {
        #[cfg(feature = "ts-export")]
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
