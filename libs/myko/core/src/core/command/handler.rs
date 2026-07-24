use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

#[cfg(not(target_arch = "wasm32"))]
use crate::server::MykoServerCtx;
use crate::{
    command::CommandError, core::capability::RequestScoped, query::QueryParams,
    request::RequestContext,
};

/// Context provided to command handlers for accessing dependencies.
///
/// CommandContext allows handlers to:
/// - Emit SET/DEL events
/// - Execute queries against the store
/// - Access request context (tx, client_id, lineage, host_id)
#[derive(Clone)]
pub struct CommandContext {
    /// Request context with tracing information (tx, client_id, lineage, host_id).
    pub req: Arc<RequestContext>,

    /// The command ID being executed (for error reporting).
    pub command_id: Arc<str>,

    #[cfg(not(target_arch = "wasm32"))]
    server_ctx: Arc<MykoServerCtx>,
}

impl CommandContext {
    /// Create a new CommandContext.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(
        command_id: Arc<str>,
        req: Arc<RequestContext>,
        server_ctx: Arc<MykoServerCtx>,
    ) -> Self {
        Self {
            req,
            command_id,
            server_ctx,
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience accessors
    // ─────────────────────────────────────────────────────────────────────────

    /// Get the request creation timestamp.
    pub fn created_at(&self) -> &str {
        &self.req.created_at
    }

    /// Execute a query and return the first result.
    ///
    /// This performs a one-shot query against the store.
    pub fn exec_query_first<Q>(&self, query: Q) -> Result<Option<Arc<Q::Item>>, CommandError>
    where
        Q: QueryParams,
        Q::Item: DeserializeOwned + std::fmt::Debug + Send + Sync + Clone + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Ok(self
                .server_ctx
                .query_snapshot(query, self.req.clone())
                .into_iter()
                .next())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = query;
            unreachable!();
        }
    }

    /// Execute a query and return all results.
    ///
    /// This performs a one-shot query against the store.
    pub fn exec_query<Q>(&self, query: Q) -> Result<Vec<Arc<Q::Item>>, CommandError>
    where
        Q: QueryParams,
        Q::Item: DeserializeOwned + std::fmt::Debug + Send + Sync + Clone + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Ok(self
                .server_ctx
                .query_snapshot(query, self.req.clone())
                .into_iter()
                .collect())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = query;
            unreachable!();
        }
    }

    /// Execute a report and return the current value.
    ///
    /// This allows command handlers to query reports for decision making.
    pub fn exec_report<R>(
        &self,
        report: R,
    ) -> Result<<R as crate::report::ReportHandler>::Output, CommandError>
    where
        R: crate::report::ReportParams + Clone,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            use hyphae::Gettable;
            Ok(self
                .server_ctx
                .report(report, self.req.clone())
                .get()
                .as_ref()
                .clone())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = report;
            unreachable!();
        }
    }
}

// Capability impls. The command context is the only one that carries
// `EventPublishing` (emit typed SET/DEL events) and `CommandSending` (dispatch
// nested commands) — which is exactly why a report/view/query/saga handler
// cannot emit or dispatch: they don't implement these traits. The bodies live
// in `core::capability`; these impls just wire the accessors.
impl crate::core::capability::sealed::Sealed for CommandContext {}
impl crate::core::capability::RequestScoped for CommandContext {
    fn __request(&self) -> &Arc<RequestContext> {
        &self.req
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl crate::core::capability::ServerScoped for CommandContext {
    fn __server_ctx(&self) -> &Arc<MykoServerCtx> {
        &self.server_ctx
    }
}
#[cfg(target_arch = "wasm32")]
impl crate::core::capability::ServerScoped for CommandContext {}
impl crate::core::capability::EventPublishing for CommandContext {
    fn __command_id(&self) -> &Arc<str> {
        &self.command_id
    }
}
impl crate::core::capability::CommandSending for CommandContext {
    fn __command_ctx(&self) -> CommandContext {
        self.clone()
    }
}

/// Trait for command handlers.
///
/// Implement this directly on your command params struct.
/// The command is already deserialized when `execute` is called.
///
/// # Example
///
/// ```text
/// // Define params with #[myko_command(...)]:
/// #[myko_command(result = "()")]
/// pub struct DeleteMachine {
///   pub id: Arc<str>,
/// }
///
/// // Implement the business logic:
/// impl CommandHandler for DeleteMachine {
///   fn execute(self, ctx: CommandContext) -> Result<(), CommandError> {
///     // validate input, query current state, emit SET/DEL events
///     Ok(())
///   }
/// }
///
/// // Register the handler (usually near the command definition):
/// register_command_handler!(DeleteMachine);
/// ```
pub trait CommandHandler: crate::command::CommandParams {
    /// Execute the command synchronously.
    ///
    /// `self` is the deserialized command parameters (owned, consumed by execution).
    #[cfg(not(target_arch = "wasm32"))]
    fn execute(self, ctx: CommandContext) -> Result<Self::Result, CommandError>;

    /// Execute the command synchronously (wasm no-op).
    ///
    /// Command handling only runs server-side; on wasm32 the hand-written
    /// native body is gated out and this no-op default applies. Commands are
    /// dispatched to the server over the wire, so this is never invoked on
    /// wasm — it returns an "unsupported on wasm" error to type-check.
    #[cfg(target_arch = "wasm32")]
    fn execute(self, ctx: CommandContext) -> Result<Self::Result, CommandError> {
        Err(CommandError {
            tx: ctx.tx().to_string(),
            command_id: ctx.command_id.to_string(),
            message: "CommandHandler::execute is not supported on wasm32".to_string(),
        })
    }
}

/// Type-erased command executor for dynamic dispatch.
///
/// This is used internally by the registry to execute commands
/// without knowing their concrete types at compile time.
pub trait DynCommandExecutor: Send + Sync + 'static {
    /// The command ID this executor handles
    fn command_id(&self) -> &'static str;

    /// Execute the command from a JSON value
    fn execute_from_value(
        &self,
        command: Value,
        ctx: CommandContext,
    ) -> Result<Value, CommandError>;
}

/// Adapter that wraps a CommandHandler to provide DynCommandExecutor
pub struct CommandExecutorAdapter<C: CommandHandler> {
    _phantom: std::marker::PhantomData<C>,
}

impl<C: CommandHandler> CommandExecutorAdapter<C> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<C: CommandHandler> Default for CommandExecutorAdapter<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: CommandHandler> DynCommandExecutor for CommandExecutorAdapter<C> {
    fn command_id(&self) -> &'static str {
        C::command_id_static()
    }

    fn execute_from_value(
        &self,
        command: Value,
        ctx: CommandContext,
    ) -> Result<Value, CommandError> {
        // Deserialize the command
        let cmd: C = serde_json::from_value(command).map_err(|e| CommandError {
            tx: ctx.tx().to_string(),
            command_id: C::command_id_static().to_string(),
            message: format!("Failed to deserialize command: {}", e),
        })?;

        // Execute the handler. Same "myko.command" span as
        // CommandContext::execute_command, for the inbound (ws/wire)
        // dispatch path — this is the DynCommandExecutor adapter used when
        // a command arrives already-serialized rather than called directly.
        let _span = tracing::trace_span!("myko.command", cmd = C::command_id_static()).entered();
        // "external": arrived pre-serialized — the funnel for both native-WS
        // command dispatch and MCP HTTP/WS in-process tool calls — see
        // `dispatch_metrics::record_command`.
        #[cfg(not(target_arch = "wasm32"))]
        crate::server::dispatch_metrics::record_command(C::command_id_static(), "external");
        let result = cmd.execute(ctx)?;

        // Serialize the result
        serde_json::to_value(result).map_err(|e| CommandError {
            tx: String::new(),
            command_id: C::command_id_static().to_string(),
            message: format!("Failed to serialize result: {}", e),
        })
    }
}

/// Type-erased command executor factory for inventory registration
pub type CommandExecutorFactory = fn() -> Box<dyn DynCommandExecutor>;

/// Registration entry for command handlers
pub struct CommandHandlerRegistration {
    pub command_id: &'static str,
    pub factory: CommandExecutorFactory,
}

inventory::collect!(CommandHandlerRegistration);

/// Macro to register a command handler with the inventory.
///
/// # Example
///
/// ```text
/// register_command_handler!(DeleteMachine);
/// ```
#[macro_export]
macro_rules! register_command_handler {
    ($cmd:ty) => {
        $crate::inventory::submit! {
            $crate::command::CommandHandlerRegistration {
                command_id: <$cmd as $crate::command::CommandIdStatic>::COMMAND_ID,
                factory: || Box::new($crate::command::CommandExecutorAdapter::<$cmd>::new()),
            }
        }
    };
}
