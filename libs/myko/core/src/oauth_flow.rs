//! Framework authentication commands, bound to a host's installed OAuth service.
pub use myko_auth::oauth::{AuthorizationInfo, OAuthFlowOperation, OAuthFlowReply};

crate::register_typegen_type!(AuthorizationInfo, OAuthFlowOperation, OAuthFlowReply);

#[crate::myko_command(OAuthFlowReply)]
pub struct MykoOAuthRequest {
    pub operation: OAuthFlowOperation,
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::{MykoOAuthRequest, OAuthFlowOperation, OAuthFlowReply};
    use crate::{
        command::{CommandContext, CommandError, CommandHandler},
        core::capability::ServerScoped,
        server::MykoServerContext,
        store::StoreRegistry,
    };
    use std::sync::{Arc, Mutex, OnceLock, Weak};
    pub trait OAuthFlowHandler: Send + Sync + 'static {
        fn authority_id(&self) -> &str;
        /// # Errors
        /// Returns a safe protocol error for an invalid or unavailable handoff.
        fn handle(&self, operation: OAuthFlowOperation) -> Result<OAuthFlowReply, String>;
    }
    type Entry = (Weak<StoreRegistry>, Weak<dyn OAuthFlowHandler>);
    fn services() -> &'static Mutex<Vec<Entry>> {
        static SERVICES: OnceLock<Mutex<Vec<Entry>>> = OnceLock::new();
        SERVICES.get_or_init(|| Mutex::new(Vec::new()))
    }
    /// Install a connection-owned service handle without retaining the server registry.
    /// # Errors
    /// Rejects conflicting authorities for one registry or unavailable registration state.
    pub fn install(
        context: &MykoServerContext,
        handler: Arc<dyn OAuthFlowHandler>,
    ) -> Result<Arc<dyn OAuthFlowHandler>, String> {
        let mut services = services()
            .lock()
            .map_err(|_| "OAuth service registry unavailable")?;
        services.retain(|(registry, handler)| {
            registry.strong_count() > 0 && handler.strong_count() > 0
        });
        if let Some(existing) = services.iter().find_map(|(registry, handler)| {
            registry
                .upgrade()
                .filter(|registry| Arc::ptr_eq(registry, &context.registry))
                .and_then(|_| handler.upgrade())
        }) {
            if existing.authority_id() != handler.authority_id() {
                return Err("Conflicting OAuth authorities for one registry".into());
            }
            return Ok(existing);
        }
        services.push((Arc::downgrade(&context.registry), Arc::downgrade(&handler)));
        drop(services);
        Ok(handler)
    }
    impl CommandHandler for MykoOAuthRequest {
        fn execute(self, ctx: CommandContext) -> Result<OAuthFlowReply, CommandError> {
            let failure = |message: &str| {
                CommandError::new(
                    ctx.req.tx.to_string(),
                    ctx.command_id.to_string(),
                    message.to_owned(),
                )
            };
            let handler = {
                let services = services()
                    .lock()
                    .map_err(|_| failure("OAuth service registry unavailable"))?;
                services
                    .iter()
                    .find_map(|(registry, handler)| {
                        registry
                            .upgrade()
                            .filter(|registry| Arc::ptr_eq(registry, &ctx.__server_ctx().registry))
                            .and_then(|_| handler.upgrade())
                    })
                    .ok_or_else(|| failure("OAuth authorization is not configured"))?
            };
            handler
                .handle(self.operation)
                .map_err(|error| failure(&error))
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
pub use native::{OAuthFlowHandler, install};
