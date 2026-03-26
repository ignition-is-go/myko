use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
use crate::server::CellServerCtx;
use crate::{
    command::CommandError,
    entities::client::ClientId,
    event::EventOptions,
    item::Eventable,
    query::QueryParams,
    request::RequestContext,
    wire::{MEvent, MEventType},
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
    server_ctx: Arc<CellServerCtx>,
}

impl CommandContext {
    /// Create a new CommandContext.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(
        command_id: Arc<str>,
        req: Arc<RequestContext>,
        server_ctx: Arc<CellServerCtx>,
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

    /// Get the transaction ID.
    pub fn tx(&self) -> Arc<str> {
        self.req.tx.clone()
    }

    /// Get the client ID if present.
    pub fn client_id(&self) -> Option<ClientId> {
        self.req.client_id.clone()
    }

    /// Get the host ID.
    pub fn host_id(&self) -> Uuid {
        self.req.host_id
    }

    /// Get the lineage (call chain).
    pub fn lineage(&self) -> &[Arc<str>] {
        &self.req.lineage
    }

    /// Get the request creation timestamp.
    pub fn created_at(&self) -> &str {
        &self.req.created_at
    }

    /// Emit a SET event for an item.
    pub fn emit_set<T>(&self, item: impl std::ops::Deref<Target = T>) -> Result<(), CommandError>
    where
        T: Eventable + Serialize + Clone + 'static,
    {
        self.emit_set_with_options(item, EventOptions::default())
    }

    /// Emit a SET event for an item with custom options.
    pub fn emit_set_with_options<T>(
        &self,
        item: impl std::ops::Deref<Target = T>,
        options: EventOptions,
    ) -> Result<(), CommandError>
    where
        T: Eventable + Serialize + Clone + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.server_ctx
                .set_with_options(&*item, Some(options))
                .map_err(|e| CommandError {
                    tx: self.req.tx.to_string(),
                    command_id: self.command_id.to_string(),
                    message: e.to_string(),
                })
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (item, options);
            unreachable!();
        }
    }

    /// Emit a batch of SET events for items.
    ///
    /// This is more efficient than repeated `emit_set` calls because the server can
    /// apply the events in one bulk pass.
    pub fn emit_set_batch<T: Eventable + Serialize + Clone + 'static>(
        &self,
        items: &[T],
    ) -> Result<(), CommandError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if items.is_empty() {
                return Ok(());
            }

            let source_id = Some(self.req.host_id.to_string());
            let mut events = Vec::with_capacity(items.len());
            for item in items {
                let item_json = serde_json::to_value(item).map_err(|err| CommandError {
                    tx: self.req.tx.to_string(),
                    command_id: self.command_id.to_string(),
                    message: format!("Failed to serialize item for batch set: {}", err),
                })?;
                events.push(MEvent {
                    item: item_json,
                    change_type: MEventType::SET,
                    item_type: item.entity_type().to_string(),
                    created_at: self.req.created_at.to_string(),
                    tx: self.req.tx.to_string(),
                    source_id: source_id.clone(),
                    options: None,
                });
            }
            self.server_ctx
                .apply_event_batch(events)
                .map_err(|e| CommandError {
                    tx: self.req.tx.to_string(),
                    command_id: self.command_id.to_string(),
                    message: e.to_string(),
                })?;
            Ok(())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = items;
            unreachable!();
        }
    }

    /// Emit a mixed batch of SET events for type-erased items.
    ///
    /// This is useful when a command needs to publish multiple entity types
    /// together in one server batch.
    pub fn emit_set_any_batch<I>(&self, items: I) -> Result<(), CommandError>
    where
        I: IntoIterator<Item = Arc<dyn crate::item::AnyItem>>,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let items: Vec<_> = items.into_iter().collect();
            if items.is_empty() {
                return Ok(());
            }

            let source_id = Some(self.req.host_id.to_string());
            let mut events = Vec::with_capacity(items.len());
            for item in items {
                events.push(MEvent {
                    item: item.to_value(),
                    change_type: MEventType::SET,
                    item_type: item.entity_type().to_string(),
                    created_at: self.req.created_at.to_string(),
                    tx: self.req.tx.to_string(),
                    source_id: source_id.clone(),
                    options: None,
                });
            }
            self.server_ctx
                .apply_event_batch(events)
                .map_err(|e| CommandError {
                    tx: self.req.tx.to_string(),
                    command_id: self.command_id.to_string(),
                    message: e.to_string(),
                })?;
            Ok(())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = items;
            unreachable!();
        }
    }

    /// Emit a DEL event for an item.
    pub fn emit_del<T>(&self, item: impl std::ops::Deref<Target = T>) -> Result<(), CommandError>
    where
        T: Eventable + Serialize + Clone + 'static,
    {
        self.emit_del_with_options(item, EventOptions::default())
    }

    /// Emit a batch of DEL events for items.
    ///
    /// This is more efficient than repeated `emit_del` calls because the server can
    /// apply the events in one bulk pass.
    pub fn emit_del_batch<'a, T, I>(&self, items: I) -> Result<(), CommandError>
    where
        T: Eventable + Serialize + Clone + 'static,
        I: IntoIterator<Item = &'a T>,
        T: 'a,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let items: Vec<&T> = items.into_iter().collect();
            if items.is_empty() {
                return Ok(());
            }

            let source_id = Some(self.req.host_id.to_string());
            let mut events = Vec::with_capacity(items.len());
            for item in items {
                let item_json = serde_json::to_value(item).map_err(|err| CommandError {
                    tx: self.req.tx.to_string(),
                    command_id: self.command_id.to_string(),
                    message: format!("Failed to serialize item for batch del: {}", err),
                })?;
                events.push(MEvent {
                    item: item_json,
                    change_type: MEventType::DEL,
                    item_type: item.entity_type().to_string(),
                    created_at: self.req.created_at.to_string(),
                    tx: self.req.tx.to_string(),
                    source_id: source_id.clone(),
                    options: None,
                });
            }
            self.server_ctx
                .apply_event_batch(events)
                .map_err(|e| CommandError {
                    tx: self.req.tx.to_string(),
                    command_id: self.command_id.to_string(),
                    message: e.to_string(),
                })?;
            Ok(())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = items;
            unreachable!();
        }
    }

    /// Emit a DEL event for an item with custom options.
    pub fn emit_del_with_options<T>(
        &self,
        item: impl std::ops::Deref<Target = T>,
        options: EventOptions,
    ) -> Result<(), CommandError>
    where
        T: Eventable + Serialize + Clone + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.server_ctx
                .del_with_options(&*item, Some(options))
                .map_err(|e| CommandError {
                    tx: self.req.tx.to_string(),
                    command_id: self.command_id.to_string(),
                    message: e.to_string(),
                })
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (item, options);
            unreachable!();
        }
    }

    /// Emit a batch of pre-built MEvents (SET or DEL).
    ///
    /// This is useful for type-erased imports where the caller already has
    /// the raw JSON and entity type strings.
    pub fn emit_event_batch(&self, events: Vec<MEvent>) -> Result<usize, CommandError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.server_ctx
                .apply_event_batch(events)
                .map_err(|e| CommandError {
                    tx: self.req.tx.to_string(),
                    command_id: self.command_id.to_string(),
                    message: e.to_string(),
                })
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = events;
            unreachable!();
        }
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

    /// Execute another command within this context.
    ///
    /// This allows command handlers to compose by calling other commands.
    /// The nested command shares the same transaction context.
    /// The command is consumed by execution, but the context is borrowed.
    pub fn execute_command<C: CommandHandler>(&self, cmd: C) -> Result<C::Result, CommandError> {
        cmd.execute(self.clone())
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
    fn execute(self, ctx: CommandContext) -> Result<Self::Result, CommandError>;
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

        // Execute the handler
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
