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
//! │                             MykoServer                                   │
//! │                                                                          │
//! │  WebSocket ──► WsHandler ──► MykoServerContext ──► StoreRegistry             │
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
//! - `TargetQuery` struct for query filter parameters
//! - Registration with the [`inventory`] system
//!
//! ## Module Guide
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`client`] | WebSocket client for connecting to Myko servers |
//! | [`core`] | Core types: command, query, report, saga, item, relationship |
//! | [`wire`] | Wire protocol types: `MykoMessage`, `MEvent`, responses, errors |
//! | [`server`] | `MykoServer` and server context |
//! | [`store`] | Entity store and registry |
//!
//! ## Performance
//!
//! Myko-rs is optimized for high-throughput, low-latency scenarios:
//!
//! - **Hyphae cells**: Reactive queries and reports using the hyphae cell library
//! - **Lock-free stores**: `CellMap` for concurrent entity access
//! - **`MessagePack` serialization**: Binary format for efficient WebSocket communication
//! - **Pluggable persistence**: Run in-memory for development, add Postgres for production persistence
//!
//! See `libs/myko/rs/OPTIMIZATION.md` for detailed performance guidelines.

extern crate self as myko;

// Main module structure
pub mod cache;
pub mod client;
#[cfg(all(not(target_arch = "wasm32"), feature = "codegen-ts"))]
pub mod codegen;
pub mod codegen_types;
pub mod core;
pub mod entities;
#[cfg(not(target_arch = "wasm32"))]
pub mod operation_index;
pub mod search;
pub mod server;
pub mod store;
pub mod typegen_module;
#[cfg(feature = "codegen-ts")]
pub mod typegen_typescript;
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
pub use core::{command, common, item, query, reflection, relationship, report, request, view};

pub use erased_serde; // For AnyItem::erased_serialize in generated code
// Re-export crates for use in macros
pub use futures; // For proc macro generated stream adapters in typed sagas
pub use hyphae; // For cell-based queries/reports in #[myko_item]
pub use inventory;
pub use inventory::submit; // For myko::submit! macro
// `myko::TS` resolves to the TypeScript adapter's derive+trait when
// `codegen-ts` is enabled on Myko, and to a no-op helper derive otherwise.
// Macro expansion can therefore keep a stable path without requiring entity
// crates to depend directly on ts-rs.
#[cfg(not(feature = "codegen-ts"))]
pub use myko_macros::TsNoop as TS;
// Re-export all attribute/derive macros so downstream crates can consume them
// as `myko::myko_item`, `myko::myko_subtype`, etc. without adding a separate
// `myko-macros` dependency.
pub use myko_macros::*;
pub use serde; // For #[derive(serde::Serialize, serde::Deserialize)] in #[myko_item]
pub use serde_json; // For proc macro generated serde_json::from_value in typed sagas
pub use tracing; // For proc macro generated tracing::debug!/warn! in typed sagas
#[cfg(feature = "codegen-ts")]
pub use ts_rs::{self, TS};
// Re-export wire types at top level for backwards compatibility
pub use wire::event; // For #[derive(myko::TS)]

/// Register a Rust type for generated-language export.
///
/// Registration is emitted only for native typegen builds. Runtime builds,
/// especially WebAssembly clients, must not pay for inventory constructors.
/// When the TypeScript backend is enabled, a separate adapter carries the
/// `ts-rs` export callback.
#[cfg(all(
    feature = "codegen-ts",
    not(target_arch = "wasm32")
))]
#[macro_export]
macro_rules! register_typegen_type {
    ($ty:ty) => {
        $crate::inventory::submit! {
            $crate::codegen_types::TypegenTypeRegistration {
                id: concat!(module_path!(), "::", stringify!($ty)),
                type_name: stringify!($ty),
                crate_path: module_path!(),
            }
        }
        $crate::inventory::submit! {
            $crate::typegen_typescript::TypeExportRegistration {
                type_id: concat!(module_path!(), "::", stringify!($ty)),
                type_name: stringify!($ty),
                rust_type_id: || ::std::any::TypeId::of::<$ty>(),
                generated_name: |config| <$ty as $crate::ts_rs::TS>::ident(config),
                output_path: || <$ty as $crate::ts_rs::TS>::output_path(),
                export_fn: || {
                    <$ty as $crate::ts_rs::TS>::export_all(&$crate::ts_rs::Config::from_env())
                },
            }
        }
    };
    ($($ty:ty),+ $(,)?) => {
        $(
            $crate::register_typegen_type!($ty);
        )+
    };
}

#[cfg(all(
    feature = "typegen",
    not(feature = "codegen-ts"),
    not(target_arch = "wasm32")
))]
#[macro_export]
macro_rules! register_typegen_type {
    ($ty:ty) => {
        $crate::inventory::submit! {
            $crate::codegen_types::TypegenTypeRegistration {
                id: concat!(module_path!(), "::", stringify!($ty)),
                type_name: stringify!($ty),
                crate_path: module_path!(),
            }
        }
    };
    ($($ty:ty),+ $(,)?) => {
        $(
            $crate::register_typegen_type!($ty);
        )+
    };
}

#[cfg(any(not(feature = "typegen"), target_arch = "wasm32"))]
#[macro_export]
macro_rules! register_typegen_type {
    ($($ty:ty),+ $(,)?) => {};
}

/// Give a Rust type an opaque/custom TypeScript representation.
///
/// This is emitted by Myko-owned derives such as
/// `#[myko_subtype(ts("unknown"))]`; downstream crates should not
/// implement the underlying TypeScript trait directly.
#[cfg(all(feature = "codegen-ts", not(target_arch = "wasm32")))]
#[macro_export]
macro_rules! impl_ts_as {
    ($ty:ty, $typescript:literal) => {
        impl $crate::TS for $ty {
            type WithoutGenerics = Self;
            type OptionInnerType = Self;

            fn name(_: &$crate::ts_rs::Config) -> String {
                $typescript.to_owned()
            }

            fn inline(_: &$crate::ts_rs::Config) -> String {
                $typescript.to_owned()
            }

            fn inline_flattened(_: &$crate::ts_rs::Config) -> String {
                $typescript.to_owned()
            }
        }
    };
}

#[cfg(any(not(feature = "codegen-ts"), target_arch = "wasm32"))]
#[macro_export]
macro_rules! impl_ts_as {
    ($ty:ty, $typescript:literal) => {};
}

/// Register a constant in the native language-neutral typegen catalog.
#[doc(hidden)]
#[cfg(all(feature = "typegen", not(target_arch = "wasm32")))]
#[macro_export]
macro_rules! register_typegen_const {
    ($name:ident, $variant:ident, $value:expr) => {
        $crate::inventory::submit! {
            $crate::codegen_types::TypegenConstRegistration {
                name: stringify!($name),
                value: $crate::codegen_types::TypegenConstValue::$variant($value),
                crate_path: module_path!(),
            }
        }
    };
}

#[doc(hidden)]
#[cfg(any(not(feature = "typegen"), target_arch = "wasm32"))]
#[macro_export]
macro_rules! register_typegen_const {
    ($name:ident, $variant:ident, $value:expr) => {};
}

/// Define a Rust constant and register it for generated-language export in
/// native typegen builds.
#[macro_export]
macro_rules! shared_const {
    (pub $name:ident : &str = $value:expr) => {
        pub const $name: &str = $value;
        $crate::register_typegen_const!($name, Str, $value);
    };
    ($name:ident : &str = $value:expr) => {
        const $name: &str = $value;
        $crate::register_typegen_const!($name, Str, $value);
    };
    (pub $name:ident : i64 = $value:expr) => {
        pub const $name: i64 = $value;
        $crate::register_typegen_const!($name, Int, $value);
    };
    ($name:ident : i64 = $value:expr) => {
        const $name: i64 = $value;
        $crate::register_typegen_const!($name, Int, $value);
    };
    (pub $name:ident : bool = $value:expr) => {
        pub const $name: bool = $value;
        $crate::register_typegen_const!($name, Bool, $value);
    };
    ($name:ident : bool = $value:expr) => {
        const $name: bool = $value;
        $crate::register_typegen_const!($name, Bool, $value);
    };
}

/// Define a typed marker for an explicit cross-crate typegen group.
#[macro_export]
macro_rules! typegen_group {
    ($vis:vis $name:ident) => {
        $vis struct $name;
        impl $crate::codegen_types::TypegenGroup for $name {}
    };
}

/// Enroll all registrations owned by this crate in a typed typegen group.
#[cfg(all(feature = "typegen", not(target_arch = "wasm32")))]
#[macro_export]
macro_rules! register_typegen_group_member {
    ($group:ty) => {
        $crate::inventory::submit! {
            $crate::codegen_types::TypegenGroupMemberRegistration {
                group_type_id: || ::std::any::TypeId::of::<$group>(),
                crate_path: module_path!(),
            }
        }
    };
}

#[cfg(any(not(feature = "typegen"), target_arch = "wasm32"))]
#[macro_export]
macro_rules! register_typegen_group_member {
    ($group:ty) => {};
}

/// Mark already-registered types as framework dependencies needed by downstream typegen.
#[cfg(all(feature = "typegen", not(target_arch = "wasm32")))]
#[macro_export]
macro_rules! mark_framework_typegen_type {
    ($($ty:ty),+ $(,)?) => {
        $(
            $crate::inventory::submit! {
                $crate::codegen_types::FrameworkTypegenRegistration {
                    type_id: concat!(module_path!(), "::", stringify!($ty)),
                }
            }
        )+
    };
}

#[cfg(any(not(feature = "typegen"), target_arch = "wasm32"))]
#[macro_export]
macro_rules! mark_framework_typegen_type {
    ($($ty:ty),+ $(,)?) => {};
}

/// Register a language-neutral generated typegen module.
///
/// `$build` is a function returning [`typegen_module::TypegenModule`]. TypeScript (or
/// any other target-language source) belongs in Myko's renderer, not in that
/// callback.
#[cfg(all(feature = "typegen", not(target_arch = "wasm32")))]
#[macro_export]
macro_rules! register_typegen_module {
    ($id:ident, $build:path) => {
        $crate::inventory::submit! {
            $crate::codegen_types::TypegenModuleRegistration {
                id: stringify!($id),
                crate_path: module_path!(),
                build: $build,
            }
        }
    };
}

#[cfg(any(not(feature = "typegen"), target_arch = "wasm32"))]
#[macro_export]
macro_rules! register_typegen_module {
    ($id:ident, $build:path) => {};
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

#[cfg(test)]
pub(crate) mod test_util {
    //! Shared by unit tests that exercise hyphae's reactive scheduler
    //! (subscribe/cascade/diff-count assertions at a specific instant).
    //!
    //! hyphae's scheduler tick queue is process-wide by design (a per-thread
    //! queue can't coordinate two threads' batches converging on a shared
    //! cell), and `cargo test` runs a crate's unit tests concurrently within
    //! one process by default — so two such tests can perturb each other's
    //! cache/diff/message counts mid-flight. No entries are ever stranded;
    //! the assertion just samples too early. Serialize with this guard as
    //! the first line of any test asserting on reactive state at an instant
    //! (same fix as `tests/query_cache_leak_test.rs`, which is a separate
    //! process and doesn't need to share this lock).
    pub fn scheduler_test_serial() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
