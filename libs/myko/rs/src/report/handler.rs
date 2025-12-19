use std::pin::Pin;
use std::sync::Arc;

use crossbeam::channel as crossbeam_channel;
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::context::RequestContext;
use crate::event::MEvent;
use crate::query::QueryParams;
use crate::report::{ReportOutput, ReportOutputType, ReportParams, ReportStream};
use crate::server::MykoServerCtx;

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
    pub(crate) runner: Arc<ReportRunnerHandle>,
}

/// Handle to the report runner for creating subscriptions
pub struct ReportRunnerHandle {
    // Channel to request new query/report subscriptions (crossbeam for sync)
    pub(crate) subscription_tx: crossbeam_channel::Sender<SubscriptionRequest>,
}

#[derive(Debug)]
pub enum SubscriptionRequest {
    Query {
        query: Value,
        query_id: Arc<str>,
        query_item_type: Arc<str>,
        response_tx: crossbeam_channel::Sender<Vec<Value>>,
    },
    Report {
        report: Value,
        report_id: Arc<str>,
        /// Request context to propagate to the sub-report
        req: RequestContext,
        response_tx: mpsc::Sender<ReportOutput>,
    },
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
    /// The subscription is automatically managed - when the report is dropped,
    /// the query subscription is cleaned up.
    ///
    /// Accepts bare query params (e.g., `GetServersByIds { ids: vec![...] }`)
    /// and automatically wraps them with transaction metadata.
    pub fn query<Q>(&self, query: Q) -> Pin<Box<dyn Stream<Item = Vec<Q::Item>> + Send>>
    where
        Q: QueryParams + 'static,
        Q::Item: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        use crate::query::{QueryItemType, QueryRequest};
        let runner = self.runner.clone();
        let query_id = query.query_id();
        let query_item_type = QueryItemType::query_item_type(&query);
        // Wrap with QueryRequest to add tx and created_at to the serialized value
        let wrapped = QueryRequest::new(query);
        let query_value = serde_json::to_value(&wrapped).expect("Query should serialize");

        // Create bounded channel for query responses with backpressure
        // 64 items should handle normal query update bursts
        let (sender, receiver) = crossbeam_channel::bounded::<Vec<Value>>(64);

        // Send subscription request to runner (sync send)
        let _ = runner.subscription_tx.send(SubscriptionRequest::Query {
            query: query_value,
            query_id,
            query_item_type,
            response_tx: sender,
        });

        // Convert crossbeam channel to async stream
        Box::pin(async_stream::stream! {
            loop {
                // Use spawn_blocking to avoid blocking the async runtime
                let rx = receiver.clone();
                let result = tokio::task::spawn_blocking(move || rx.recv()).await;

                match result {
                    Ok(Ok(items)) => {
                        let parsed: Vec<Q::Item> = items
                            .into_iter()
                            .filter_map(|v| serde_json::from_value(v).ok())
                            .collect();
                        yield parsed;
                    }
                    _ => break, // Channel closed or error
                }
            }
        })
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
        let runner = self.runner.clone();
        let report_id: Arc<str> = R::report_id_static().into();
        // Wrap with ReportRequest to add tx to the serialized value
        let wrapped = ReportRequest::new(report);
        let report_value = serde_json::to_value(&wrapped).expect("Report should serialize");
        let req = self.req.clone();

        let (sender, mut receiver) = mpsc::channel::<ReportOutput>(16);

        // Send subscription request to runner (sync send)
        let _ = runner.subscription_tx.send(SubscriptionRequest::Report {
            report: report_value,
            report_id,
            req,
            response_tx: sender,
        });

        // Convert channel to stream, deserializing output
        Box::pin(async_stream::stream! {
            while let Some(output) = receiver.recv().await {
                match output {
                    ReportOutput::Value(value) => {
                        if let Ok(parsed) = serde_json::from_value::<<R as ReportOutputType>::Output>(value) {
                            yield parsed;
                        }
                    }
                    ReportOutput::Error(err) => {
                        log::error!("Sub-report error: {}", err);
                        // Don't yield on error, just log and continue
                    }
                }
            }
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
            log::warn!("ReportContext::events() called but EventBus not initialized");
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
