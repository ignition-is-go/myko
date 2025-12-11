use std::{future::Future, pin::Pin, sync::Arc, task::Poll};

use futures::Stream;
use futures_signals::signal_map::{MapDiff, SignalMap};
use ractor::ActorRef;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    actors::{
        command::CommandManagerMsg,
        event::event_manager::EventManagerMsg,
        query::query_manager::QueryManagerMsg,
        report::report_manager::ReportManagerMsg,
    },
    api::query::WrappedQuery,
    command::{CommandError, CommandId},
    event::MEvent,
    item::Eventable,
    query::{Query, QueryIdStatic, QueryItemType},
    report::{Report, ReportIdStatic, ReportOutputType, WrappedReport},
    server::MykoServerCtx,
};

/// Wrapper to convert a SignalMap into a Stream of MapDiff
struct SignalMapStream<S> {
    signal: S,
}

impl<S: SignalMap + Unpin> Stream for SignalMapStream<S> {
    type Item = MapDiff<S::Key, S::Value>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.signal).poll_map_change(cx)
    }
}

/// Context provided to command handlers for accessing dependencies.
///
/// CommandContext allows handlers to:
/// - Emit SET/DEL events
/// - Execute nested commands
/// - Execute reports (one-shot)
/// - Access server context
pub struct CommandContext {
    pub client_id: Arc<str>,
    pub tx: Arc<str>,
    pub lineage: Vec<String>,
    pub created_at: String,

    pub(crate) server_ctx: Arc<MykoServerCtx>,
    pub(crate) event_manager: ActorRef<EventManagerMsg>,
    pub(crate) command_manager: ActorRef<CommandManagerMsg>,
    pub(crate) query_manager: ActorRef<QueryManagerMsg>,
    pub(crate) report_manager: ActorRef<ReportManagerMsg>,
}

impl CommandContext {
    /// Create a new CommandContext
    pub fn new(
        client_id: Arc<str>,
        tx: Arc<str>,
        lineage: Vec<String>,
        created_at: String,
        server_ctx: Arc<MykoServerCtx>,
        event_manager: ActorRef<EventManagerMsg>,
        command_manager: ActorRef<CommandManagerMsg>,
        query_manager: ActorRef<QueryManagerMsg>,
        report_manager: ActorRef<ReportManagerMsg>,
    ) -> Self {
        Self {
            client_id,
            tx,
            lineage,
            created_at,
            server_ctx,
            event_manager,
            command_manager,
            query_manager,
            report_manager,
        }
    }

    /// Emit a SET event for an item
    pub fn emit_set<T: Eventable + Serialize>(&self, item: &T) -> Result<(), CommandError> {
        let event = MEvent::from_item(item, crate::event::MEventType::SET, self.tx.to_string());

        self.event_manager
            .send_message(crate::actors::event::event_manager::EventManagerMsg::ProcessEvent(
                crate::actors::event::common::ProcessEventData {
                    event,
                    persist: crate::actors::event::common::PersistEvent::Persist,
                },
            ))
            .map_err(|e| CommandError {
                tx: self.tx.to_string(),
                message: format!("Failed to send event: {}", e),
            })?;

        Ok(())
    }

    /// Emit a DEL event for an item
    pub fn emit_del<T: Eventable + Serialize>(&self, item: &T) -> Result<(), CommandError> {
        let event = MEvent::from_item(item, crate::event::MEventType::DEL, self.tx.to_string());

        self.event_manager
            .send_message(crate::actors::event::event_manager::EventManagerMsg::ProcessEvent(
                crate::actors::event::common::ProcessEventData {
                    event,
                    persist: crate::actors::event::common::PersistEvent::Persist,
                },
            ))
            .map_err(|e| CommandError {
                tx: self.tx.to_string(),
                message: format!("Failed to send event: {}", e),
            })?;

        Ok(())
    }

    /// Execute a nested command, updating lineage
    pub async fn execute_command<C: CommandId + Serialize + Clone>(
        &self,
        command: C,
    ) -> Result<Value, CommandError> {
        let wrapped = crate::command::wrap_command(self.tx.to_string(), &command).map_err(|e| {
            CommandError {
                tx: self.tx.to_string(),
                message: format!("Failed to wrap command: {}", e),
            }
        })?;

        // Build child lineage
        let mut child_lineage = self.lineage.clone();
        child_lineage.push(command.command_id());

        // Execute via command manager
        let result = ractor::call!(
            self.command_manager,
            CommandManagerMsg::ExecuteNested,
            wrapped,
            self.client_id.clone(),
            child_lineage,
            self.created_at.clone()
        )
        .map_err(|e| CommandError {
            tx: self.tx.to_string(),
            message: format!("Failed to call command manager: {}", e),
        })?;

        result
    }

    /// Get the server context
    pub fn server_ctx(&self) -> &Arc<MykoServerCtx> {
        &self.server_ctx
    }

    /// Execute a query and return the first result.
    ///
    /// This starts a query subscription and waits for the first batch of results,
    /// then returns the first item if any exist.
    pub async fn query_one<Q>(&self, query: &Q) -> Result<Option<Q::Item>, CommandError>
    where
        Q: Query + QueryIdStatic + QueryItemType + Serialize,
        Q::Item: DeserializeOwned + Send + Sync,
    {
        let query_value = serde_json::to_value(query).map_err(|e| CommandError {
            tx: self.tx.to_string(),
            message: format!("Failed to serialize query: {}", e),
        })?;

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: Q::query_id_static(),
            query_item_type: Q::query_item_type_static(),
        };

        // Start the query and get the signal map
        let signal_map = ractor::call!(self.query_manager, QueryManagerMsg::StartQuery, wrapped)
            .map_err(|e| CommandError {
                tx: self.tx.to_string(),
                message: format!("Failed to start query: {}", e),
            })?;

        // Get the first diff from the signal map using SignalMapStream
        let mut stream = SignalMapStream { signal: signal_map };

        // Poll for the first Replace event which contains initial state
        let first_diff = futures::future::poll_fn(|cx| {
            use futures::Stream;
            Pin::new(&mut stream).poll_next(cx)
        })
        .await;

        match first_diff {
            Some(MapDiff::Replace { entries }) => {
                if let Some((_, item)) = entries.into_iter().next() {
                    let value = item.to_value();
                    let parsed: Q::Item = serde_json::from_value(value).map_err(|e| CommandError {
                        tx: self.tx.to_string(),
                        message: format!("Failed to parse query result: {}", e),
                    })?;
                    Ok(Some(parsed))
                } else {
                    Ok(None)
                }
            }
            Some(MapDiff::Insert { value, .. }) => {
                let parsed: Q::Item =
                    serde_json::from_value(value.to_value()).map_err(|e| CommandError {
                        tx: self.tx.to_string(),
                        message: format!("Failed to parse query result: {}", e),
                    })?;
                Ok(Some(parsed))
            }
            _ => Ok(None),
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
            tx: self.tx.to_string(),
            message: format!("Failed to serialize report: {}", e),
        })?;

        let wrapped = WrappedReport {
            report: report_value,
            report_id: R::report_id_static().to_string(),
        };

        // Create a channel to receive report output
        let (output_tx, mut output_rx) = mpsc::channel::<Value>(1);

        // Start the report
        self.report_manager
            .send_message(ReportManagerMsg::StartReport(wrapped, output_tx))
            .map_err(|e| CommandError {
                tx: self.tx.to_string(),
                message: format!("Failed to start report: {}", e),
            })?;

        // Wait for the first value
        let first_value = output_rx.recv().await.ok_or_else(|| CommandError {
            tx: self.tx.to_string(),
            message: "Report completed without emitting a value".to_string(),
        })?;

        // Parse the result
        let result: <R as ReportOutputType>::Output =
            serde_json::from_value(first_value).map_err(|e| CommandError {
                tx: self.tx.to_string(),
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
