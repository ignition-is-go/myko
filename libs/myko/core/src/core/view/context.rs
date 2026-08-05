use std::sync::Arc;

use crate::{
    core::capability::{
        Querying, RegistryScoped, Reporting, RequestScoped, Searching, ServerScoped, Viewing,
    },
    request::RequestContext,
    server::MykoServerContext,
    store::StoreRegistry,
};

/// Per-request context for a view handler. Its scope: reactive queries,
/// sub-reports, and other views — but not command emission (a view can't
/// mutate state; see `core::capability`).
#[derive(Clone)]
pub struct ViewContext {
    pub req: Arc<RequestContext>,
    pub(crate) registry: Arc<StoreRegistry>,
    pub(crate) server_ctx: Arc<MykoServerContext>,
}

impl ViewContext {
    #[must_use]
    pub const fn new(
        req: Arc<RequestContext>,
        registry: Arc<StoreRegistry>,
        server_ctx: Arc<MykoServerContext>,
    ) -> Self {
        Self {
            req,
            registry,
            server_ctx,
        }
    }
}

impl crate::core::capability::sealed::Sealed for ViewContext {}
impl RequestScoped for ViewContext {
    fn __request(&self) -> &Arc<RequestContext> {
        &self.req
    }
}
impl RegistryScoped for ViewContext {
    fn __registry(&self) -> &Arc<StoreRegistry> {
        &self.registry
    }
}
impl ServerScoped for ViewContext {
    fn __server_ctx(&self) -> &Arc<MykoServerContext> {
        &self.server_ctx
    }
}
// The view handler's scope: reactive queries, search, sub-reports, and other
// views — but NOT command emission. Every capability compiles on both targets;
// see `core::capability` for why none of these may be `#[cfg]`-gated.
impl Querying for ViewContext {}
impl Searching for ViewContext {}
impl Reporting for ViewContext {}
impl Viewing for ViewContext {}

/// Context for a view handler's `build_cell` step — where the reactive map is
///
/// constructed. Wraps the per-request [`ViewContext`]; it used to also carry
/// its own duplicate `registry`/`server_ctx` and a separate `request_ctx`
/// (which was always `view_context.req`), all now read straight through the
/// wrapped context.
///
/// Renamed from `ViewCellContext`: the "Cell" there meant "the cell being
/// built", which collided with two unrelated "Cell" meanings elsewhere.
#[derive(Clone)]
pub struct ViewBuildContext {
    pub view_context: Arc<ViewContext>,
}

impl ViewBuildContext {
    #[must_use]
    pub const fn new(view_context: Arc<ViewContext>) -> Self {
        Self { view_context }
    }
}

impl crate::core::capability::sealed::Sealed for ViewBuildContext {}
impl RequestScoped for ViewBuildContext {
    fn __request(&self) -> &Arc<RequestContext> {
        &self.view_context.req
    }
}
impl RegistryScoped for ViewBuildContext {
    fn __registry(&self) -> &Arc<StoreRegistry> {
        &self.view_context.registry
    }
}
impl ServerScoped for ViewBuildContext {
    fn __server_ctx(&self) -> &Arc<MykoServerContext> {
        &self.view_context.server_ctx
    }
}
// NOTE(ts): this is the context `ViewHandler::build_cell` receives, and
// hand-written `build_cell` bodies live in consumer entity crates that DO
// compile to wasm32 (the leptos UI cdylibs pull them in). Gating any of these
// breaks every one of those handlers — that is the whole reason the capability
// layer is target-independent; `_capability_matrix` holds the line.
//
// `Searching` is here because view builders can seed a map from a full-text
// lookup — rship's target-search view does exactly this. Search is a
// point-in-time read (the index is not reactive), same as the
// `server_ctx().search_index()` call it replaces, so the built cell tracks the
// query results, not the index.
impl Querying for ViewBuildContext {}
impl Reporting for ViewBuildContext {}
impl Searching for ViewBuildContext {}
impl Viewing for ViewBuildContext {}
