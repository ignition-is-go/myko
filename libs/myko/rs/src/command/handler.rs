use std::{future::Future, pin::Pin, sync::Arc};

use ractor::ActorRef;
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
    api::query::WrappedQuery,
    command::{CommandError, CommandId},
    context::RequestContext,
    event::EventOptions,
    item::Eventable,
    query::{Query, QueryIdStatic, QueryItemType},
    report::{Report, ReportIdStatic, ReportOutputType, WrappedReport},
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
        server_ctx: Arc<MykoServerCtx>,
        event_manager: ActorRef<EventManagerMsg>,
        command_manager: ActorRef<CommandManagerMsg>,
        query_manager: ActorRef<QueryManagerMsg>,
        report_manager: ActorRef<ReportManagerMsg>,
    ) -> Self {
        Self {
            req,
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
                message: format!("Failed to send event: {}", e),
            })
    }

    /// Execute a nested command, updating lineage.
    ///
    /// The child command receives a context with extended lineage tracking
    /// the call chain (e.g., `["client", "CreateScene", "CreateBinding"]`).
    pub async fn execute_command<C: CommandId + Serialize + Clone>(
        &self,
        command: C,
    ) -> Result<Value, CommandError> {
        let wrapped = crate::command::wrap_command(self.tx().to_string(), &command).map_err(|e| {
            CommandError {
                tx: self.tx().to_string(),
                message: format!("Failed to wrap command: {}", e),
            }
        })?;

        // Create child context with extended lineage
        let child_req = self.req.child(&command.command_id());

        // Execute via command manager
        ractor::call!(
            self.command_manager,
            CommandManagerMsg::ExecuteNested,
            wrapped,
            child_req
        )
        .map_err(|e| CommandError {
            tx: self.tx().to_string(),
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
    pub async fn query_one<Q>(&self, query: &Q) -> Result<Option<Q::Item>, CommandError>
    where
        Q: Query + QueryIdStatic + QueryItemType + Serialize,
        Q::Item: DeserializeOwned + Send + Sync,
    {
        let query_value = serde_json::to_value(query).map_err(|e| CommandError {
            tx: self.tx().to_string(),
            message: format!("Failed to serialize query: {}", e),
        })?;

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: Q::query_id_static(),
            query_item_type: Q::query_item_type_static(),
        };

        // Use WrappedQuerySnapshot for one-shot query (no subscription)
        let snapshot =
            ractor::call!(self.query_manager, QueryManagerMsg::WrappedQuerySnapshot, wrapped)
                .map_err(|e| CommandError {
                    tx: self.tx().to_string(),
                    message: format!("Failed to query snapshot: {}", e),
                })?;

        // Return the first item if any exist
        if let Some((_, item)) = snapshot.into_iter().next() {
            let value = item.to_value();
            let parsed: Q::Item = serde_json::from_value(value).map_err(|e| CommandError {
                tx: self.tx().to_string(),
                message: format!("Failed to parse query result: {}", e),
            })?;
            Ok(Some(parsed))
        } else {
            Ok(None)
        }
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
            message: format!("Failed to serialize report: {}", e),
        })?;

        let wrapped = WrappedReport {
            report: report_value,
            report_id: R::report_id_static().to_string(),
        };

        // Create child context with extended lineage for the report
        let child_req = self.req.child(R::report_id_static());

        // Create a channel to receive report output
        let (output_tx, mut output_rx) = mpsc::channel::<Value>(1);

        // Start the report
        self.report_manager
            .send_message(ReportManagerMsg::StartReport(wrapped, child_req, output_tx))
            .map_err(|e| CommandError {
                tx: self.tx().to_string(),
                message: format!("Failed to start report: {}", e),
            })?;

        // Wait for the first value
        let first_value = output_rx.recv().await.ok_or_else(|| CommandError {
            tx: self.tx().to_string(),
            message: "Report completed without emitting a value".to_string(),
        })?;

        // Parse the result
        let result: <R as ReportOutputType>::Output =
            serde_json::from_value(first_value).map_err(|e| CommandError {
                tx: self.tx().to_string(),
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
