//! Handler capabilities.
//!
//! Each handler context (report, view, query, command, saga) exposes a
//! *scoped* slice of the server's functionality. Rather than each context
//! hand-forwarding a curated set of methods to an internally-held
//! `Arc<MykoServerContext>`, that functionality lives here as a set of sealed
//! capability traits with default methods. A context opts into exactly the
//! capabilities its handler kind is allowed by implementing those traits
//! (the impls are empty — the bodies are the trait defaults), and the scope
//! is then a compile-time property: a `ReportContext` that does not
//! `impl EventPublishing` simply has no `emit_set`/`emit_del` in scope, so a
//! report cannot mutate state — that's an `E0599`, not a runtime check or a
//! review-time convention. (Dispatching nested commands is a second,
//! independent capability, `CommandSending`.)
//!
//! The accessor traits are sealed via a crate-private supertrait, so nothing
//! downstream can implement them, forge a capability, or reach the raw
//! `MykoServerContext` to escape its scope.
//!
//! ## wasm
//!
//! Handlers are authored once and compile for both native and wasm even
//! though they only *run* server-side. The capability methods whose
//! signatures are wasm-compatible (`query_map`, `report`, `search`, …) keep a
//! wasm default that `unreachable!()`s — the single home for what used to be
//! a per-context stub. Methods whose types are inherently server-only
//! (`view`, `peer_client`, `replay_store`, …) are native-only; handlers that
//! call them are themselves native-gated.

use std::sync::Arc;

use uuid::Uuid;

use crate::{request::RequestContext, store::StoreRegistry};

pub(crate) mod sealed {
    /// Sealing supertrait: crate-private, so the accessor traits below can
    /// only be implemented inside this crate. Capability traits inherit the
    /// seal transitively, so downstream code can call capability methods but
    /// never implement one to grant itself a capability.
    pub trait Sealed {}
}

// ─────────────────────────────────────────────────────────────────────────
// Accessors — the ONLY path from a context to its underlying state. Hidden
// (`__`-prefixed, doc(hidden)); capability default methods read through them.
// ─────────────────────────────────────────────────────────────────────────

/// A context carrying the originating [`RequestContext`] (tx / client / host
/// / lineage).
pub trait RequestScoped: sealed::Sealed {
    #[doc(hidden)]
    fn __request(&self) -> &Arc<RequestContext>;

    /// The transaction ID.
    fn tx(&self) -> &str {
        &self.__request().tx
    }
    /// The client ID, if the request originated from a client.
    fn client_id(&self) -> Option<&str> {
        self.__request().client_id.as_deref()
    }
    /// The host ID of the server handling the request.
    fn host_id(&self) -> Uuid {
        self.__request().host_id
    }
    /// The call chain (lineage) that led to this handler.
    fn lineage(&self) -> &[Arc<str>] {
        &self.__request().lineage
    }
}

/// A context with access to the store registry for type-erased entity lookups.
pub trait RegistryScoped: sealed::Sealed {
    #[doc(hidden)]
    fn __registry(&self) -> &Arc<StoreRegistry>;

    /// The store registry, for traversing entities by runtime-determined type
    /// name (e.g. relationship-graph walks).
    fn registry(&self) -> Arc<StoreRegistry> {
        self.__registry().clone()
    }
}

/// A context backed by the live server. The sealed accessor every server
/// capability reads through — native-only, since there is no server on wasm.
pub trait ServerScoped: RequestScoped {
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    fn __server_ctx(&self) -> &Arc<crate::server::MykoServerContext>;
}

use hyphae::{Cell, CellImmutable, CellMap, CellValue};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    cache::CacheKey,
    command::{CommandContext, CommandError, CommandHandler},
    common::with_id::{WithId, WithTypedId},
    core::item::Eventable,
    query::{LiveFilterQuery, QueryParams},
    report::{ReportHandler, ReportId},
    wire::MEvent,
};

/// Body helper for a capability method that is real on native and a stub on
/// wasm. Expands to the `#[cfg]`-split every wasm-compatible server capability
/// shares: `$body` runs natively (reading through `__server_ctx()`), while on
/// wasm32 it consumes its arguments (silencing unused-variable lints) and
/// `unreachable!()`s — handlers only ever *run* server-side (see the module
/// docs). Native-only capabilities (`view`, `peer_client`, …) name server-only
/// types, so they are `#[cfg(not(wasm))]` on the trait and do not use this.
macro_rules! wasm_native {
    ($name:literal, ($($arg:ident),* $(,)?), $body:block) => {{
        #[cfg(not(target_arch = "wasm32"))]
        $body
        #[cfg(target_arch = "wasm32")]
        {
            $(let _ = $arg;)*
            unreachable!(concat!($name, " is not available on wasm32"))
        }
    }};
}

/// Subscribe to reactive query dependencies.
pub trait Querying: ServerScoped {
    /// Subscribe to a query and get a typed reactive `CellMap` keyed by the
    /// item's typed id.
    fn query_map<Q>(
        &self,
        query: Q,
    ) -> CellMap<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: Eventable
            + WithId
            + WithTypedId
            + DeserializeOwned
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + CellValue
            + 'static,
    {
        wasm_native!("query_map", (query), {
            self.__server_ctx().query_map(query, self.__request().clone())
        })
    }

    /// Subscribe to a query keyed by canonical `Arc<str>` ids.
    fn query_map_by_str<Q>(&self, query: Q) -> CellMap<Arc<str>, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: Eventable
            + WithId
            + WithTypedId
            + DeserializeOwned
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + CellValue
            + 'static,
    {
        wasm_native!("query_map_by_str", (query), {
            self.__server_ctx()
                .query_map_by_str(query, self.__request().clone())
        })
    }

    /// Subscribe to a query and get an untyped (erased `AnyItem`) reactive map.
    #[cfg(not(target_arch = "wasm32"))]
    fn query_map_untyped<Q>(&self, query: Q) -> crate::query::FilteredCellMap
    where
        Q: crate::query::QueryFactory
            + crate::query::QueryHandler
            + QueryParams
            + Clone
            + Send
            + Sync
            + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.__server_ctx()
            .query_map_untyped(query, self.__request().clone())
    }

    /// Subscribe to a query and get its incremental `MapDiff` stream.
    #[cfg(not(target_arch = "wasm32"))]
    fn query_diff<Q>(
        &self,
        query: Q,
    ) -> Cell<hyphae::MapDiff<Arc<str>, Q::Item>, CellImmutable>
    where
        Q: crate::query::QueryFactory
            + crate::query::QueryHandler
            + QueryParams
            + Clone
            + Send
            + Sync
            + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        use hyphae::{MapExt, MaterializeDefinite};
        self.query_map_untyped(query)
            .diffs()
            .map(|diff| {
                crate::item::downcast_any_item_map_diff::<Q::Item>(diff, "query_diff")
            })
            .materialize()
    }

    /// Reactive filter parameters: a live `Cell` filter instead of a value.
    fn query_live<F>(
        &self,
        filter_cell: impl hyphae::Watchable<F>,
    ) -> CellMap<<F::Item as WithTypedId>::Id, Arc<F::Item>, CellImmutable>
    where
        F: LiveFilterQuery,
        F::Item: WithTypedId,
    {
        wasm_native!("query_live", (filter_cell), {
            self.__server_ctx().query_live(filter_cell)
        })
    }
}

/// Full-text search over `#[searchable]` fields.
pub trait Searching: ServerScoped {
    /// Matching entity ids (up to `limit`), backed by the per-type search index.
    fn search(&self, entity_type: &str, query: &str, limit: usize) -> Vec<Arc<str>> {
        wasm_native!("search", (entity_type, query, limit), {
            self.__server_ctx()
                .search_index()
                .search(entity_type, query, limit)
        })
    }
}

/// Compose sub-reports (reports depending on reports).
pub trait Reporting: ServerScoped {
    /// Subscribe to a sub-report; the framework memoizes the compute by cache
    /// key so concurrent requests share one computation.
    fn report<R>(&self, report: R) -> Cell<Arc<R::Output>, CellImmutable>
    where
        R: ReportHandler + ReportId + CacheKey + Clone + Serialize + 'static,
    {
        wasm_native!("report", (report), {
            self.__server_ctx().report(report, self.__request().clone())
        })
    }
}

/// Emit typed events — the write capability. Every method emits a SET/DEL of a
/// typed entity, applied immediately: command emits never route through the
/// WS-ingest time-window buffer (that buffer is for wire ingest only). A
/// report/view/query handler does not `impl EventPublishing`, so it has no
/// `emit_*` in scope and physically cannot mutate state — an `E0599`, not a
/// convention.
pub trait EventPublishing: ServerScoped {
    #[doc(hidden)]
    fn __command_id(&self) -> &Arc<str>;

    #[doc(hidden)]
    fn __emit_err(&self, message: impl std::fmt::Display) -> CommandError {
        CommandError::new(
            self.__request().tx.to_string(),
            self.__command_id().to_string(),
            message.to_string(),
        )
    }

    /// Emit a SET event for an item.
    fn emit_set<T>(&self, item: impl std::ops::Deref<Target = T>) -> Result<(), CommandError>
    where
        T: Eventable + Serialize + Clone + 'static,
    {
        wasm_native!("emit_set", (item), {
            self.__server_ctx()
                .set(&*item)
                .map_err(|e| self.__emit_err(e))
        })
    }

    /// Emit a batch of typed SET events (applied immediately, in one bulk pass).
    fn emit_set_batch<T: Eventable + Serialize + Clone + 'static>(
        &self,
        items: &[T],
    ) -> Result<(), CommandError> {
        wasm_native!("emit_set_batch", (items), {
            let anys = items
                .iter()
                .map(|item| Arc::new(item.clone()) as Arc<dyn crate::item::AnyItem>);
            self.__server_ctx()
                .set_batch_any(anys)
                .map_err(|e| self.__emit_err(e))
        })
    }

    /// Emit a mixed batch of type-erased SET events (applied immediately).
    fn emit_set_any_batch<I>(&self, items: I) -> Result<(), CommandError>
    where
        I: IntoIterator<Item = Arc<dyn crate::item::AnyItem>>,
    {
        wasm_native!("emit_set_any_batch", (items), {
            self.__server_ctx()
                .set_batch_any(items)
                .map_err(|e| self.__emit_err(e))
        })
    }

    /// Emit a DEL event for an item.
    fn emit_del<T>(&self, item: impl std::ops::Deref<Target = T>) -> Result<(), CommandError>
    where
        T: Eventable + Serialize + Clone + 'static,
    {
        wasm_native!("emit_del", (item), {
            self.__server_ctx()
                .del(&*item)
                .map_err(|e| self.__emit_err(e))
        })
    }

    /// Emit a batch of typed DEL events (applied immediately, in one bulk pass).
    fn emit_del_batch<'a, T, I>(&self, items: I) -> Result<(), CommandError>
    where
        T: Eventable + Serialize + Clone + 'static,
        I: IntoIterator<Item = &'a T>,
        T: 'a,
    {
        wasm_native!("emit_del_batch", (items), {
            let anys = items
                .into_iter()
                .map(|item| Arc::new(item.clone()) as Arc<dyn crate::item::AnyItem>);
            self.__server_ctx()
                .del_batch_any(anys)
                .map_err(|e| self.__emit_err(e))
        })
    }

    /// Apply a batch of pre-built raw events (SET or DEL), applied immediately.
    ///
    /// The one raw-`MEvent` path — for type-erased imports where the caller
    /// already holds erased JSON + entity-type strings rather than typed
    /// entities. Returns the number of events applied.
    fn emit_event_batch(&self, events: Vec<MEvent>) -> Result<usize, CommandError> {
        wasm_native!("emit_event_batch", (events), {
            self.__server_ctx()
                .apply_events_immediate(events)
                .map_err(|e| self.__emit_err(e))
        })
    }
}

/// Dispatch nested commands — compose handlers by executing another command in
/// the same transaction. Independent of [`EventPublishing`]: a context may be
/// allowed to emit events without being allowed to send commands, or vice
/// versa, since each is its own trait.
pub trait CommandSending: ServerScoped {
    #[doc(hidden)]
    fn __command_ctx(&self) -> CommandContext;

    /// Execute another command within this context. The nested command shares
    /// the same transaction and is consumed by execution.
    fn execute_command<C: CommandHandler>(&self, cmd: C) -> Result<C::Result, CommandError> {
        // Bounded-cardinality span (per command id, never per-invocation) so a
        // profiler shows which commands fire hot.
        let _span = tracing::trace_span!("myko.command", cmd = C::command_id_static()).entered();
        // "internal": composed in-process by another handler/saga, not a fresh
        // wire arrival — see `dispatch_metrics::record_command`.
        #[cfg(not(target_arch = "wasm32"))]
        crate::server::dispatch_metrics::record_command(C::command_id_static(), "internal");
        cmd.execute(self.__command_ctx())
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Native-only capabilities: their signatures name server-only types, so
// handlers that use them are themselves native-gated.
// ─────────────────────────────────────────────────────────────────────────

/// Subscribe to view dependencies (reuse incremental map-native view logic).
#[cfg(not(target_arch = "wasm32"))]
pub trait Viewing: ServerScoped {
    /// Subscribe to a view and get a typed reactive `CellMap`.
    fn view<V>(&self, view: V) -> crate::core::view::TypedViewCellMap<V::Item>
    where
        V: crate::core::view::ViewFactory + Clone,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug,
    {
        self.__server_ctx().view(view, self.__request().clone())
    }

    /// Subscribe to a view and get an untyped (erased) reactive map.
    fn view_map_untyped<V>(&self, view: V) -> crate::core::view::FilteredViewCellMap
    where
        V: crate::core::view::ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.__server_ctx()
            .view_map_untyped(view, self.__request().clone())
    }
}

/// Reach live peer servers (federation).
#[cfg(not(target_arch = "wasm32"))]
pub trait PeerAccess: ServerScoped {
    /// The live peer client for a peer server id, if present.
    fn peer_client(&self, peer_id: &str) -> Option<Arc<crate::client::MykoClient>> {
        self.__server_ctx().peer_client(peer_id)
    }
    /// A reactive tick that updates when peer-client membership changes.
    fn peer_clients_tick(&self) -> Cell<u64, CellImmutable> {
        self.__server_ctx().peer_clients_tick()
    }
}

/// Point-in-time history replay and persistence health.
#[cfg(not(target_arch = "wasm32"))]
pub trait Replaying: ServerScoped {
    /// Live persist-health counters (queued, errors, throughput).
    fn persist_health(&self) -> Arc<crate::server::PersistHealth> {
        self.__server_ctx().persist_health()
    }
    /// Replay historical events into a temporary `StoreRegistry`. Errs if no
    /// history-replay provider is configured.
    fn replay_store(&self, until: &str) -> Result<Arc<StoreRegistry>, String> {
        let ctx = self.__server_ctx();
        let provider = ctx
            .history_replay()
            .ok_or_else(|| "No history replay provider configured".to_string())?;
        provider.replay_to_store(until, &ctx.handler_registry)
    }
}
