use std::{future::Future, pin::Pin, sync::Arc};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    actors::{
        command::CommandManagerMsg,
        event::{event_manager::EventManagerMsg, EventPublisher},
        query::query_manager::QueryManagerMsg,
        report::report_manager::ReportManagerMsg,
    },
    runtime::ActorRef,
    api::query::WrappedQuery,
    command::{CommandError, CommandId},
    context::RequestContext,
    event::EventOptions,
    item::Eventable,
    query::{QueryParams, QueryRequest},
    report::{Report, ReportIdStatic, ReportOutput, ReportOutputType, WrappedReport},
    server::MykoServerCtx,
};

/// Context provided to command handlers for accessing dependencies.
///
/// CommandContext allows handlers to:
/// - Emit SET/DEL events
/// - Execute nested commands
/// - Execute reports (one-shot)
/// - Access server context
/// - Access request context (tx, client_id, lineage, host_id)
///
/// The context carries request tracing information via [`RequestContext`],
/// which propagates through nested operations for correlation and debugging.
pub struct CommandContext {
    /// Request context with tracing information (tx, client_id, lineage, host_id).
    pub req: RequestContext,

    /// The command ID being executed (for error reporting).
    pub command_id: Arc<str>,

    pub(crate) server_ctx: Arc<MykoServerCtx>,
    pub(crate) event_manager: ActorRef<EventManagerMsg>,
    pub(crate) command_manager: ActorRef<CommandManagerMsg>,
    pub(crate) query_manager: ActorRef<QueryManagerMsg>,
    pub(crate) report_manager: ActorRef<ReportManagerMsg>,
}

impl CommandContext {
    /// Create a new CommandContext from a RequestContext.
    pub fn new(
        req: RequestContext,
        command_id: Arc<str>,
        server_ctx: Arc<MykoServerCtx>,
        event_manager: ActorRef<EventManagerMsg>,
        command_manager: ActorRef<CommandManagerMsg>,
        query_manager: ActorRef<QueryManagerMsg>,
        report_manager: ActorRef<ReportManagerMsg>,
    ) -> Self {
        Self {
            req,
            command_id,
            server_ctx,
            event_manager,
            command_manager,
            query_manager,
            report_manager,
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience accessors for backward compatibility and ergonomics
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

    /// Get an EventPublisher for emitting events.
    fn publisher(&self) -> EventPublisher {
        EventPublisher::new(self.event_manager.clone(), self.req.host_id)
    }

    /// Emit a SET event for an item.
    pub fn emit_set<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
    ) -> Result<(), CommandError> {
        self.publisher()
            .publish_set(item, self.tx(), self.req.client_id.clone(), None)
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to send event: {}", e),
            })
    }

    /// Emit a SET event for an item with custom options.
    pub fn emit_set_with_options<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
        options: EventOptions,
    ) -> Result<(), CommandError> {
        self.publisher()
            .publish_set(item, self.tx(), self.req.client_id.clone(), Some(options))
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to send event: {}", e),
            })
    }

    /// Emit a DEL event for an item.
    pub fn emit_del<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
    ) -> Result<(), CommandError> {
        self.publisher()
            .publish_del(item, self.tx(), self.req.client_id.clone(), None)
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to send event: {}", e),
            })
    }

    /// Emit a DEL event for an item with custom options.
    pub fn emit_del_with_options<T: Eventable + Serialize + Clone + 'static>(
        &self,
        item: &T,
        options: EventOptions,
    ) -> Result<(), CommandError> {
        self.publisher()
            .publish_del(item, self.tx(), self.req.client_id.clone(), Some(options))
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to send event: {}", e),
            })
    }

    /// Execute a nested command, updating lineage.
    ///
    /// The child command receives a context with extended lineage tracking
    /// the call chain (e.g., `["client", "CreateScene", "CreateBinding"]`).
    /// The parent's tx is preserved for the nested command.
    pub async fn execute_command<C: CommandId + Serialize + Clone>(
        &self,
        command: C,
    ) -> Result<Value, CommandError> {
        use crate::command::{CommandRequest, wrap_command_request};

        // Wrap command with parent's tx for transaction correlation
        let request = CommandRequest::with_tx(command.clone(), self.req.tx.clone());
        let wrapped = wrap_command_request(&request).map_err(|e| {
            CommandError {
                tx: self.tx().to_string(),
                command_id: command.command_id().to_string(),
                message: format!("Failed to wrap command: {}", e),
            }
        })?;

        // Create child context with extended lineage
        let child_req = self.req.child(&command.command_id());

        // Execute via command manager (sync call - blocks briefly on channel)
        self.command_manager
            .call(|r| CommandManagerMsg::ExecuteNested(wrapped, child_req, r))
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: command.command_id().to_string(),
                message: format!("Failed to call command manager: {}", e),
            })?
    }

    /// Get the server context
    pub fn server_ctx(&self) -> &Arc<MykoServerCtx> {
        &self.server_ctx
    }

    /// Execute a query and return the first result.
    ///
    /// This performs a one-shot query that returns current state without
    /// creating a subscription. Returns the first matching item if any exist.
    ///
    /// Accepts bare query params (e.g., `GetServersByIds { ids: vec![...] }`)
    /// and automatically wraps them with transaction metadata.
    pub async fn query_one<Q>(&self, query: &Q) -> Result<Option<Q::Item>, CommandError>
    where
        Q: QueryParams,
        Q::Item: DeserializeOwned + Send + Sync,
    {
        // Wrap with QueryRequest to add tx and created_at
        let wrapped_request = QueryRequest::new(query.clone());
        let query_value = serde_json::to_value(&wrapped_request).map_err(|e| CommandError {
            tx: self.tx().to_string(),
            command_id: self.command_id.to_string(),
            message: format!("Failed to serialize query: {}", e),
        })?;

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: Q::query_id_static(),
            query_item_type: Q::query_item_type_static(),
        };

        // Use WrappedQuerySnapshot for one-shot query (no subscription)
        // Sync call - blocks briefly on channel
        let snapshot = self
            .query_manager
            .call(|r| QueryManagerMsg::WrappedQuerySnapshot(wrapped, r))
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to query snapshot: {}", e),
            })?;

        // Return the first item if any exist
        if let Some((_, item)) = snapshot.into_iter().next() {
            let value = item.to_value();
            let parsed: Q::Item = serde_json::from_value(value).map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to parse query result: {}", e),
            })?;
            Ok(Some(parsed))
        } else {
            Ok(None)
        }
    }

    /// Execute a query and return all results.
    ///
    /// This performs a one-shot query that returns current state without
    /// creating a subscription. Returns all matching items.
    ///
    /// Accepts bare query params (e.g., `GetServersByQuery { ... }`)
    /// and automatically wraps them with transaction metadata.
    pub async fn query_all<Q>(&self, query: &Q) -> Result<Vec<Q::Item>, CommandError>
    where
        Q: QueryParams,
        Q::Item: DeserializeOwned + Send + Sync,
    {
        // Wrap with QueryRequest to add tx and created_at
        let wrapped_request = QueryRequest::new(query.clone());
        let query_value = serde_json::to_value(&wrapped_request).map_err(|e| CommandError {
            tx: self.tx().to_string(),
            command_id: self.command_id.to_string(),
            message: format!("Failed to serialize query: {}", e),
        })?;

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: Q::query_id_static(),
            query_item_type: Q::query_item_type_static(),
        };

        // Use WrappedQuerySnapshot for one-shot query (no subscription)
        let snapshot = self
            .query_manager
            .call(|r| QueryManagerMsg::WrappedQuerySnapshot(wrapped, r))
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to query snapshot: {}", e),
            })?;

        // Parse and return all items
        let mut results = Vec::with_capacity(snapshot.len());
        for (_, item) in snapshot {
            let value = item.to_value();
            let parsed: Q::Item = serde_json::from_value(value).map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to parse query result: {}", e),
            })?;
            results.push(parsed);
        }
        Ok(results)
    }

    /// Execute a report and return the first emitted value.
    ///
    /// This starts a report and waits for the first value to be emitted,
    /// then returns it.
    pub async fn report_one<R>(&self, report: &R) -> Result<<R as ReportOutputType>::Output, CommandError>
    where
        R: Report + ReportIdStatic + ReportOutputType + Serialize,
        <R as ReportOutputType>::Output: DeserializeOwned,
    {
        let report_value = serde_json::to_value(report).map_err(|e| CommandError {
            tx: self.tx().to_string(),
            command_id: self.command_id.to_string(),
            message: format!("Failed to serialize report: {}", e),
        })?;

        let wrapped = WrappedReport {
            report: report_value,
            report_id: R::report_id_static().to_string(),
        };

        // Create child context with extended lineage for the report
        let child_req = self.req.child(R::report_id_static());

        // Create a channel to receive report output
        let (output_tx, mut output_rx) = mpsc::channel::<ReportOutput>(1);

        // Start the report (from command context, no parent token)
        self.report_manager
            .send_message(ReportManagerMsg::StartReport(wrapped, child_req, output_tx, None))
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to start report: {}", e),
            })?;

        // Wait for the first output
        let first_output = output_rx.recv().await.ok_or_else(|| CommandError {
            tx: self.tx().to_string(),
            command_id: self.command_id.to_string(),
            message: "Report completed without emitting a value".to_string(),
        })?;

        // Handle value or error
        let first_value = match first_output {
            ReportOutput::Value(v) => v,
            ReportOutput::Error(err_msg) => {
                return Err(CommandError {
                    tx: self.tx().to_string(),
                    command_id: self.command_id.to_string(),
                    message: format!("Report error: {}", err_msg),
                });
            }
        };

        // Parse the result
        let result: <R as ReportOutputType>::Output =
            serde_json::from_value(first_value).map_err(|e| CommandError {
                tx: self.tx().to_string(),
                command_id: self.command_id.to_string(),
                message: format!("Failed to parse report result: {}", e),
            })?;

        Ok(result)
    }
}

/// A boxed future for object-safe async trait methods
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Trait for command handlers.
///
/// Command handlers process mutations and can:
/// - Emit events (SET/DEL)
/// - Execute nested commands
/// - Execute reports for one-shot queries
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
