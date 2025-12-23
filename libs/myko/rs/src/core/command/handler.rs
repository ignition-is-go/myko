use std::{future::Future, pin::Pin, sync::Arc};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    command::CommandError,
    context::RequestContext,
    event::{MEvent, MEventType, EventOptions},
    item::Eventable,
    query::QueryParams,
    store::StoreRegistry,
};

/// Context provided to command handlers for accessing dependencies.
///
/// CommandContext allows handlers to:
/// - Emit SET/DEL events
/// - Execute queries against the store
/// - Access request context (tx, client_id, lineage, host_id)
pub struct CommandContext {
    /// Request context with tracing information (tx, client_id, lineage, host_id).
    pub req: RequestContext,

    /// The command ID being executed (for error reporting).
    pub command_id: Arc<str>,

    /// Store registry for queries
    pub(crate) registry: Arc<StoreRegistry>,

    /// Event sink for publishing events (optional, for persistence)
    pub(crate) event_sink: Option<flume::Sender<MEvent>>,
}

impl CommandContext {
    /// Create a new CommandContext.
    pub fn new(
        req: RequestContext,
        command_id: Arc<str>,
        registry: Arc<StoreRegistry>,
    ) -> Self {
        Self {
            req,
            command_id,
            registry,
            event_sink: None,
        }
    }

    /// Create a new CommandContext with an event sink.
    pub fn with_event_sink(
        req: RequestContext,
        command_id: Arc<str>,
        registry: Arc<StoreRegistry>,
        event_sink: flume::Sender<MEvent>,
    ) -> Self {
        Self {
            req,
            command_id,
            registry,
            event_sink: Some(event_sink),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience accessors
    // ─────────────────────────────────────────────────────────────────────────

    /// Get the transaction ID.
    pub fn tx(&self) -> &str {
        &self.req.tx
    }

    /// Get the client ID if present.
    pub fn client_id(&self) -> Option<&str> {
        self.req.client_id.as_deref()
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
    pub fn emit_set<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
    ) -> Result<(), CommandError> {
        self.emit_set_with_options(item, EventOptions::default())
    }

    /// Emit a SET event for an item with custom options.
    pub fn emit_set_with_options<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
        options: EventOptions,
    ) -> Result<(), CommandError> {
        let event = MEvent {
            item: serde_json::to_value(item).map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to serialize item: {}", e),
            })?,
            change_type: MEventType::SET,
            item_type: item.entity_type().to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            tx: self.tx().to_string(),
            source_id: self.req.client_id.as_ref().map(|s| s.to_string()),
            options: Some(options),
        };

        // Update the store directly
        let store = self.registry.get_or_create(item.entity_type());
        let id: Arc<str> = item.id();
        store.set(id, Arc::new(item.clone()));

        // Optionally send to event sink for persistence
        if let Some(ref sink) = self.event_sink {
            let _ = sink.send(event);
        }

        Ok(())
    }

    /// Emit a DEL event for an item.
    pub fn emit_del<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
    ) -> Result<(), CommandError> {
        self.emit_del_with_options(item, EventOptions::default())
    }

    /// Emit a DEL event for an item with custom options.
    pub fn emit_del_with_options<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
        options: EventOptions,
    ) -> Result<(), CommandError> {
        let event = MEvent {
            item: serde_json::to_value(item).map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to serialize item: {}", e),
            })?,
            change_type: MEventType::DEL,
            item_type: item.entity_type().to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            tx: self.tx().to_string(),
            source_id: self.req.client_id.as_ref().map(|s| s.to_string()),
            options: Some(options),
        };

        // Update the store directly
        let store = self.registry.get_or_create(item.entity_type());
        let id: Arc<str> = item.id();
        store.remove(&id);

        // Optionally send to event sink for persistence
        if let Some(ref sink) = self.event_sink {
            let _ = sink.send(event);
        }

        Ok(())
    }

    /// Execute a query and return the first result.
    ///
    /// This performs a one-shot query against the store.
    pub async fn query_one<Q>(&self, _query: &Q) -> Result<Option<Q::Item>, CommandError>
    where
        Q: QueryParams,
        Q::Item: DeserializeOwned + Send + Sync + Clone + 'static,
    {
        let store = self.registry.get_or_create(Q::query_item_type_static().as_ref());

        // Get all items and find the first match
        // In a real implementation, this would use the query's filter logic
        let items = store.snapshot();
        for (_, item) in items {
            if let Some(typed) = item.as_any().downcast_ref::<Q::Item>() {
                return Ok(Some(typed.clone()));
            }
        }
        Ok(None)
    }

    /// Execute a query and return all results.
    ///
    /// This performs a one-shot query against the store.
    pub async fn query_all<Q>(&self, _query: &Q) -> Result<Vec<Q::Item>, CommandError>
    where
        Q: QueryParams,
        Q::Item: DeserializeOwned + Send + Sync + Clone + 'static,
    {
        let store = self.registry.get_or_create(Q::query_item_type_static().as_ref());

        let items = store.snapshot();
        let mut results = Vec::new();
        for (_, item) in items {
            if let Some(typed) = item.as_any().downcast_ref::<Q::Item>() {
                results.push(typed.clone());
            }
        }
        Ok(results)
    }

    /// Get the store registry for direct access.
    pub fn registry(&self) -> &Arc<StoreRegistry> {
        &self.registry
    }

    /// Execute another command within this context.
    ///
    /// This allows command handlers to compose by calling other commands.
    /// The nested command shares the same transaction context.
    pub async fn execute_command<C>(&self, cmd: C) -> Result<C::Result, CommandError>
    where
        C: crate::command::CommandParams + Clone,
        C::Result: DeserializeOwned,
    {
        use crate::command::CommandRequest;

        // Create a request with the current tx
        let request = CommandRequest::with_tx(cmd.clone(), self.req.tx.clone());

        // Find and execute the handler
        // For now, we use a simplified approach - look up in inventory
        let command_id = C::command_id_static();

        for registration in inventory::iter::<crate::command::CommandHandlerRegistration> {
            if registration.command_id == command_id {
                let handler = (registration.factory)();
                let cmd_value = serde_json::to_value(&request).map_err(|e| CommandError {
                    tx: self.tx().to_string(),
                    command_id: command_id.to_string(),
                    message: format!("Failed to serialize command: {}", e),
                })?;

                // Create a new context for the nested command
                let nested_ctx = CommandContext {
                    req: self.req.clone(),
                    command_id: command_id.into(),
                    registry: self.registry.clone(),
                    event_sink: self.event_sink.clone(),
                };

                let result = handler.execute(cmd_value, nested_ctx).await?;
                return serde_json::from_value(result).map_err(|e| CommandError {
                    tx: self.tx().to_string(),
                    command_id: command_id.to_string(),
                    message: format!("Failed to deserialize result: {}", e),
                });
            }
        }

        Err(CommandError {
            tx: self.tx().to_string(),
            command_id: command_id.to_string(),
            message: format!("Command handler not found: {}", command_id),
        })
    }

    /// Execute a report and return the current value.
    ///
    /// This allows command handlers to query reports for decision making.
    pub fn report_one<R>(&self, request: &crate::report::ReportRequest<R>) -> Result<<R as crate::report::ReportHandler>::Output, CommandError>
    where
        R: crate::report::ReportParams + Clone,
    {
        use hypha::Gettable;

        // Create a report context
        let report_ctx = crate::report::ReportContext::new(
            self.req.clone(),
            self.registry.clone(),
            serde_json::to_value(&request.report).unwrap_or(Value::Null),
        );

        // Compute the report cell and get current value
        let cell = request.report.compute(report_ctx);
        Ok(cell.get())
    }
}

/// A boxed future for object-safe async trait methods
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Trait for command handlers.
///
/// Command handlers process mutations and can:
/// - Emit events (SET/DEL)
/// - Execute queries
///
/// # Example
///
/// ```ignore
/// impl CommandHandler for DeleteMachineHandler {
///     fn command_id(&self) -> &'static str {
///         "DeleteMachine"
///     }
///
///     fn execute(&self, command: Value, ctx: CommandContext) -> BoxFuture<'_, Result<Value, CommandError>> {
///         Box::pin(async move {
///             let cmd: DeleteMachine = serde_json::from_value(command)?;
///             ctx.emit_del(&machine)?;
///             Ok(Value::Null)
///         })
///     }
/// }
/// ```
pub trait CommandHandler: Send + Sync + 'static {
    /// The command ID this handler processes
    fn command_id(&self) -> &'static str;

    /// Execute the command
    fn execute(&self, command: Value, ctx: CommandContext) -> BoxFuture<'_, Result<Value, CommandError>>;
}

/// Type-erased command handler factory for inventory registration
pub type CommandHandlerFactory = fn() -> Box<dyn CommandHandler>;

/// Registration entry for command handlers
pub struct CommandHandlerRegistration {
    pub command_id: &'static str,
    pub factory: CommandHandlerFactory,
}

inventory::collect!(CommandHandlerRegistration);
