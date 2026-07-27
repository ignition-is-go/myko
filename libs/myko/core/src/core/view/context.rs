use std::sync::Arc;

use crate::core::capability::{Querying, Reporting, RequestScoped, Searching, ServerScoped};
#[cfg(not(target_arch = "wasm32"))]
use crate::core::capability::{RegistryScoped, Viewing};
use crate::request::RequestContext;
#[cfg(not(target_arch = "wasm32"))]
use crate::server::MykoServerContext;
#[cfg(not(target_arch = "wasm32"))]
use crate::store::StoreRegistry;

/// Per-request context for a view handler. Its scope: reactive queries,
/// sub-reports, and other views — but not command emission (a view can't
/// mutate state; see `core::capability`).
#[derive(Clone)]
pub struct ViewContext {
    pub req: Arc<RequestContext>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) registry: Arc<StoreRegistry>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) server_ctx: Arc<MykoServerContext>,
}

impl ViewContext {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(
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
#[cfg(not(target_arch = "wasm32"))]
impl RegistryScoped for ViewContext {
    fn __registry(&self) -> &Arc<StoreRegistry> {
        &self.registry
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl ServerScoped for ViewContext {
    fn __server_ctx(&self) -> &Arc<MykoServerContext> {
        &self.server_ctx
    }
}
#[cfg(target_arch = "wasm32")]
impl ServerScoped for ViewContext {}

// Cross-platform: authored once, compiled for wasm too (where the bodies are
// `unreachable!` — views only build server-side). Must stay un-gated: see
// `core::capability::_capability_matrix`.
impl Querying for ViewContext {}
impl Reporting for ViewContext {}

// Native-only (server-only return types).
#[cfg(not(target_arch = "wasm32"))]
impl Viewing for ViewContext {}

/// Context for a view handler's `build_cell` step — where the reactive map is
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
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(view_context: Arc<ViewContext>) -> Self {
        Self { view_context }
    }
}

impl crate::core::capability::sealed::Sealed for ViewBuildContext {}
impl RequestScoped for ViewBuildContext {
    fn __request(&self) -> &Arc<RequestContext> {
        &self.view_context.req
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl RegistryScoped for ViewBuildContext {
    fn __registry(&self) -> &Arc<StoreRegistry> {
        &self.view_context.registry
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl ServerScoped for ViewBuildContext {
    fn __server_ctx(&self) -> &Arc<MykoServerContext> {
        &self.view_context.server_ctx
    }
}
#[cfg(target_arch = "wasm32")]
impl ServerScoped for ViewBuildContext {}

// Cross-platform: authored once, compiled for wasm too (where the bodies are
// `unreachable!` — views only build server-side). NOTE(ts): this is the context
// `ViewHandler::build_cell` receives, and hand-written `build_cell` bodies live
// in consumer entity crates that DO compile to wasm32 (the leptos UI cdylibs
// pull them in), so gating these off wasm breaks every one of them. myko 5.0.0
// kept an explicit wasm `query_map` stub here for exactly that reason; the
// guard in `core::capability::_capability_matrix` now holds the line.
//
// `Searching` is here because view builders can seed a map from a full-text
// lookup — rship's target-search view does exactly this. Search is a
// point-in-time read (the index is not reactive), same as the
// `server_ctx().search_index()` call it replaces, so the built cell tracks the
// query results, not the index.
impl Querying for ViewBuildContext {}
impl Reporting for ViewBuildContext {}
impl Searching for ViewBuildContext {}

// Native-only (server-only return types).
#[cfg(not(target_arch = "wasm32"))]
impl Viewing for ViewBuildContext {}
