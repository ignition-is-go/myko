use std::sync::Arc;

use crate::{request::RequestContext, server::CellServerCtx, store::StoreRegistry};

#[derive(Clone, Debug)]
pub struct ViewContext {
    pub req: Arc<RequestContext>,
}

#[derive(Clone)]
pub struct ViewCellContext {
    pub request_ctx: Arc<RequestContext>,
    pub view_context: Arc<ViewContext>,
    registry: Arc<StoreRegistry>,
    server_ctx: Option<Arc<CellServerCtx>>,
}

impl ViewCellContext {
    pub fn new(
        request_ctx: Arc<RequestContext>,
        view_context: Arc<ViewContext>,
        registry: Arc<StoreRegistry>,
        server_ctx: Option<Arc<CellServerCtx>>,
    ) -> Self {
        Self {
            request_ctx,
            view_context,
            registry,
            server_ctx,
        }
    }

    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }

    pub fn server_ctx(&self) -> Option<Arc<CellServerCtx>> {
        self.server_ctx.clone()
    }
}
