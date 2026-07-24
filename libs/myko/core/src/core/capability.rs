//! Handler capabilities.
//!
//! Each handler context (report, view, query, command, saga) exposes a
//! *scoped* slice of the server's functionality. Rather than each context
//! hand-forwarding a curated set of methods to an internally-held
//! `Arc<MykoServerCtx>`, that functionality lives here as a set of sealed
//! capability traits with default methods. A context opts into exactly the
//! capabilities its handler kind is allowed by implementing those traits
//! (the impls are empty — the bodies are the trait defaults), and the scope
//! is then a compile-time property: a `ReportContext` that does not
//! `impl Emitting` simply has no `emit_set`/`emit_del`/`send_command` in
//! scope, so a report cannot mutate state — that's an `E0599`, not a runtime
//! check or a review-time convention.
//!
//! The accessor traits are sealed via a crate-private supertrait, so nothing
//! downstream can implement them, forge a capability, or reach the raw
//! `MykoServerCtx` to escape its scope.
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
    fn __server_ctx(&self) -> &Arc<crate::server::MykoServerCtx>;
}

use hyphae::{Cell, CellImmutable, CellMap, CellValue};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    cache::CacheKey,
    common::with_id::{WithId, WithTypedId},
    core::item::Eventable,
    query::{LiveFilterQuery, QueryParams},
    report::{ReportHandler, ReportId},
};

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
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.__server_ctx().query_map(query, self.__request().clone())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = query;
            unreachable!("query_map is not available on wasm32")
        }
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
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.__server_ctx()
                .query_map_by_str(query, self.__request().clone())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = query;
            unreachable!("query_map_by_str is not available on wasm32")
        }
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
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.__server_ctx().query_live(filter_cell)
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = filter_cell;
            unreachable!("query_live is not available on wasm32")
        }
    }
}

/// Full-text search over `#[searchable]` fields.
pub trait Searching: ServerScoped {
    /// Matching entity ids (up to `limit`), backed by the per-type search index.
    fn search(&self, entity_type: &str, query: &str, limit: usize) -> Vec<Arc<str>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.__server_ctx()
                .search_index()
                .search(entity_type, query, limit)
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (entity_type, query, limit);
            unreachable!("search is not available on wasm32")
        }
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
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.__server_ctx().report(report, self.__request().clone())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = report;
            unreachable!("report is not available on wasm32")
        }
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
    /// The connection status for a peer server id, if a peer client exists.
    fn peer_connection_status(
        &self,
        peer_id: &str,
    ) -> Option<crate::client::ConnectionStatus> {
        self.__server_ctx().peer_connection_status(peer_id)
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
