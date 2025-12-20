use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use log::{error, warn};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::actors::query::query_manager::QueryManagerMsg;
use crate::actors::query::subscription::create_subscription_stream;
use crate::actors::report::report_manager::ReportManagerMsg;
use crate::api::query::WrappedQuery;
use crate::context::RequestContext;
use crate::event::MEvent;
use crate::query::QueryParams;
use crate::report::{ReportOutput, ReportOutputType, ReportParams, ReportStream, WrappedReport};
use crate::runtime::ActorRef;
use crate::server::MykoServerCtx;

/// Guard that stops a sub-report when dropped.
/// This provides RAII-based cleanup - no explicit cancellation needed.
struct SubReportGuard {
    tx: Arc<str>,
    report_manager: ActorRef<ReportManagerMsg>,
}

impl Drop for SubReportGuard {
    fn drop(&mut self) {
        log::debug!("SubReportGuard dropped, stopping tx={}", self.tx);
        let _ = self
            .report_manager
            .send_message(ReportManagerMsg::StopReport(self.tx.clone()));
    }
}

/// A stream wrapper that owns a guard and cleans up when dropped.
/// This ensures the guard is dropped even when async_stream generators don't drop locals.
struct GuardedSubReportStream<S> {
    inner: S,
    _guard: SubReportGuard,
}

impl<S: Stream + Unpin> Stream for GuardedSubReportStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Context provided to report handlers for accessing dependencies.
///
/// ReportContext allows handlers to:
/// - Subscribe to queries and get reactive streams
/// - Subscribe to other reports (including self for recursion)
/// - Access server context for additional data
/// - Access the report arguments via `report_args`
/// - Access request context (tx, client_id, lineage, host_id)
#[derive(Clone)]
pub struct ReportContext {
    /// Request context with tracing information (tx, client_id, lineage, host_id).
    pub req: RequestContext,
    pub server_ctx: Arc<MykoServerCtx>,
    /// The report arguments as a JSON Value - handlers should parse this to their Args type
    pub report_args: Value,
    /// Cancellation token for this report - fires when the report should stop.
    /// All subscriptions created via ctx.query() will be cancelled when this fires.
    pub cancel_token: CancellationToken,
}

impl ReportContext {
    // ─────────────────────────────────────────────────────────────────────────
    // Convenience accessors for request context
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

    // ─────────────────────────────────────────────────────────────────────────
    // Report-specific methods
    // ─────────────────────────────────────────────────────────────────────────

    /// Parse the report arguments to the expected Args type.
    ///
    /// This is a convenience method to deserialize the report_args Value
    /// to the specific Args struct for the report.
    pub fn args<A: serde::de::DeserializeOwned>(&self) -> Result<A, serde_json::Error> {
        serde_json::from_value(self.report_args.clone())
    }

    /// Subscribe to a query dependency.
    ///
    /// Returns a stream that emits whenever the query results change.
    /// The subscription is automatically managed - when the stream is dropped,
    /// the query subscription is cleaned up via RAII.
    ///
    /// Accepts bare query params (e.g., `GetServersByIds { ids: vec![...] }`)
    /// and automatically wraps them with transaction metadata.
    pub fn query<Q>(&self, query: Q) -> Pin<Box<dyn Stream<Item = Vec<Q::Item>> + Send>>
    where
        Q: QueryParams + 'static,
        Q::Item: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        use crate::query::{QueryItemType, QueryRequest};

        // Check if the token is already cancelled - if so, return empty stream immediately.
        // This prevents creating orphaned subscriptions when the report is already stopped.
        if self.cancel_token.is_cancelled() {
            log::debug!("Token already cancelled, skipping query {}", query.query_id());
            return Box::pin(futures::stream::empty());
        }

        let query_id = query.query_id();
        let query_item_type = QueryItemType::query_item_type(&query);

        // Each subscription gets its own unique tx for independent lifecycle management.
        // Cleanup is handled via CancellationToken - when the parent report is stopped,
        // all child query subscriptions are automatically cancelled.
        let wrapped = QueryRequest::new(query);
        let tx: Arc<str> = wrapped.tx.clone();
        let query_value = serde_json::to_value(&wrapped).expect("Query should serialize");

        // Get QueryManager from server context
        let Some(query_manager) = self.server_ctx.query_manager.get() else {
            warn!("QueryManager not initialized when subscribing to query");
            return Box::pin(futures::stream::empty());
        };

        // Create a WrappedQuery and subscribe directly
        let query_id_for_log = query_id.clone();
        let wrapped_query = WrappedQuery {
            query: query_value,
            query_id,
            query_item_type,
        };

        // Get flume receiver for query updates
        let lineage: Arc<str> = Arc::from(self.req.lineage_string());
        let receiver = match query_manager.call(|r| QueryManagerMsg::WatchWrappedQuery(wrapped_query, r, Some(lineage))) {
            Ok(rx) => rx,
            Err(e) => {
                error!("Failed to start query subscription: {}", e);
                return Box::pin(futures::stream::empty());
            }
        };

        // Use create_subscription_stream with CancellationToken for cleanup.
        // When the parent report is stopped, the token is cancelled and all
        // child subscriptions are automatically cleaned up.
        log::debug!(
            "ReportContext::query creating subscription tx={} for {} | lineage: {}",
            tx,
            query_id_for_log,
            self.req.lineage_string()
        );
        create_subscription_stream::<Q::Item>(
            tx,
            receiver,
            query_manager.clone(),
            self.cancel_token.clone(),
            self.server_ctx.tokio_handle.clone(),
        )
    }

    /// Subscribe to another report dependency.
    ///
    /// Returns a stream that emits whenever the report results change.
    /// Supports recursive calls (report depending on itself with different args).
    ///
    /// Cycle detection is performed at runtime - if the same report with
    /// the same arguments is already in the dependency chain, an error is logged
    /// and an empty stream is returned.
    ///
    /// Accepts bare report params and automatically wraps them with transaction metadata.
    pub fn report<R>(&self, report: R) -> Pin<Box<dyn Stream<Item = <R as ReportOutputType>::Output> + Send>>
    where
        R: ReportParams + 'static,
        <R as ReportOutputType>::Output: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        use crate::report::ReportRequest;

        let report_id: Arc<str> = R::report_id_static().into();

        // Wrap with ReportRequest to add tx to the serialized value
        let wrapped = ReportRequest::new(report);
        let report_value = serde_json::to_value(&wrapped).expect("Report should serialize");

        // Create child context with extended lineage
        let child_req = self.req.child(&report_id);
        let child_tx: Arc<str> = child_req.tx.clone();

        // Check if the parent token is already cancelled - if so, don't start the sub-report.
        // This prevents creating orphaned sub-reports when the parent report is already stopped.
        if self.cancel_token.is_cancelled() {
            log::debug!("Parent token already cancelled, skipping sub-report {}", report_id);
            return Box::pin(futures::stream::empty());
        }

        // Get ReportManager from server context
        let Some(report_manager) = self.server_ctx.report_manager.get() else {
            warn!("ReportManager not initialized when subscribing to report");
            return Box::pin(futures::stream::empty());
        };

        // Create wrapped report for StartReport
        let wrapped_report = WrappedReport {
            report: report_value,
            report_id: report_id.to_string(),
        };

        // Create channel for report outputs
        let (sender, mut receiver) = mpsc::channel::<ReportOutput>(16);

        // Start the sub-report with parent's cancel_token.
        // ReportManager will create a child_token from it, so when parent is cancelled,
        // cancellation automatically propagates to all children without needing background tasks.
        if let Err(e) = report_manager.send_message(ReportManagerMsg::StartReport(
            wrapped_report,
            child_req,
            sender,
            Some(self.cancel_token.clone()),
        )) {
            error!("Failed to start sub-report: {}", e);
            return Box::pin(futures::stream::empty());
        }

        // Create the guard that will stop the sub-report on drop (backup for when Drop works)
        let guard = SubReportGuard {
            tx: child_tx,
            report_manager: report_manager.clone(),
        };

        // Create the inner stream that processes report outputs
        let inner_stream = async_stream::stream! {
            while let Some(output) = receiver.recv().await {
                match output {
                    ReportOutput::Value(value) => {
                        if let Ok(parsed) = serde_json::from_value::<<R as ReportOutputType>::Output>(value) {
                            yield parsed;
                        }
                    }
                    ReportOutput::Error(err) => {
                        error!("Sub-report error: {}", err);
                        // Don't yield on error, just log and continue
                    }
                }
            }
        };

        // Wrap in GuardedSubReportStream so the guard is dropped when the stream is dropped
        Box::pin(GuardedSubReportStream {
            inner: Box::pin(inner_stream),
            _guard: guard,
        })
    }

    /// Subscribe to the event stream.
    ///
    /// Returns a stream of all events published to the EventBus.
    /// This is useful for reports that need to react to events directly,
    /// such as `ServerEventLog` which streams events originating from this server.
    ///
    /// # Example
    ///
    /// ```ignore
    /// impl ReportHandler for ServerEventLog {
    ///     type Output = MEvent;
    ///
    ///     fn compute(ctx: ReportContext) -> Pin<Box<dyn Stream<Item = Self::Output> + Send>> {
    ///         let host_id = ctx.server_ctx.host_id.to_string();
    ///         Box::pin(ctx.events().filter(move |e| {
    ///             futures::future::ready(e.source_id.as_deref() == Some(&host_id))
    ///         }))
    ///     }
    /// }
    /// ```
    pub fn events(&self) -> Pin<Box<dyn Stream<Item = MEvent> + Send>> {
        if let Some(event_bus) = self.server_ctx.event_bus.get() {
            Box::pin(event_bus.subscribe().into_stream())
        } else {
            // EventBus not yet initialized - return empty stream
            warn!("ReportContext::events() called but EventBus not initialized");
            Box::pin(futures::stream::empty())
        }
    }
}

/// Trait for report handlers - defines how a report computes its output.
///
/// Unlike queries which filter existing items, reports can:
/// - Aggregate data from multiple queries
/// - Transform and combine data
/// - Recursively depend on themselves (e.g., parent traversal)
/// - Depend on other reports
///
/// # Reactivity
///
/// The `compute` method returns a Stream, not a single value. This stream
/// should emit whenever any dependency changes. Use combinators like
/// `flat_map`, `switch_map`, etc. to dynamically change subscriptions
/// based on upstream values.
///
/// # Argument Parsing
///
/// Report arguments are parsed by the framework before `compute` is called,
/// and passed as `&self`. Fields are directly accessible (e.g., `self.target_id`).
/// If argument parsing fails, an error is sent to the client and `compute` is
/// never invoked.
///
/// # Example
///
/// ```ignore
/// impl ReportHandler for GetParentTargets {
///     type Output = Vec<Target>;
///
///     fn compute(&self, ctx: ReportContext) -> ReportStream<Self::Output> {
///         let target_id = self.target_id.clone();
///         let depth = self.depth;
///         ctx.query(GetTargetsByIds { ids: vec![target_id.clone()] })
///             .flat_map(move |targets| {
///                 match targets.first().and_then(|t| t.parent_id.as_ref()) {
///                     Some(parent_id) => ctx.report(GetParentTargets {
///                         target_id: parent_id.clone(),
///                         depth: depth - 1,
///                     }).boxed(),
///                     None => stream::once(async { vec![] }).boxed()
///                 }
///             })
///     }
/// }
/// ```
pub trait ReportHandler: Sized + Send + Sync + 'static {
    type Output: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    /// Compute the report output as a reactive stream.
    ///
    /// This method is called once when the report is first subscribed to.
    /// The returned stream should emit whenever dependencies change.
    ///
    /// Report arguments are parsed by the framework and passed as `&self`,
    /// so fields are directly accessible (e.g., `self.target_id`).
    fn compute(&self, ctx: ReportContext) -> ReportStream<Self::Output>;
}
