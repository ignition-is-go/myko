use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::context::RequestContext;
use crate::event::MEvent;
use crate::query::Query;
use crate::report::ReportIdStatic;
use crate::server::MykoServerCtx;

/// Context provided to report handlers for accessing dependencies.
///
/// ReportContext allows handlers to:
/// - Subscribe to queries and get reactive streams
/// - Subscribe to other reports (including self for recursion)
/// - Access server context for additional data
/// - Access the report arguments via `report_args`
/// - Access request context (tx, client_id, lineage, host_id)
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
    // Channel to request new query/report subscriptions
    pub(crate) subscription_tx: mpsc::Sender<SubscriptionRequest>,
}

#[derive(Debug)]
pub enum SubscriptionRequest {
    Query {
        query: Value,
        query_id: Arc<str>,
        query_item_type: Arc<str>,
        response_tx: mpsc::Sender<Vec<Value>>,
    },
    Report {
        report: Value,
        report_id: Arc<str>,
        /// Request context to propagate to the sub-report
        req: RequestContext,
        response_tx: mpsc::Sender<Value>,
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
    pub fn query<Q>(&self, query: Q) -> Pin<Box<dyn Stream<Item = Vec<Q::Item>> + Send>>
    where
        Q: Query + Serialize + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        use crate::query::QueryItemType;
        let runner = self.runner.clone();
        let query_id = query.query_id();
        let query_item_type = QueryItemType::query_item_type(&query);
        let query_value = serde_json::to_value(&query).expect("Query should serialize");

        let (sender, mut receiver) = mpsc::channel::<Vec<Value>>(16);

        // Send subscription request to runner
        let subscription_tx = runner.subscription_tx.clone();
        tokio::spawn(async move {
            let _ = subscription_tx
                .send(SubscriptionRequest::Query {
                    query: query_value,
                    query_id,
                    query_item_type,
                    response_tx: sender,
                })
                .await;
        });

        // Convert channel to stream, deserializing items
        Box::pin(async_stream::stream! {
            while let Some(items) = receiver.recv().await {
                let parsed: Vec<Q::Item> = items
                    .into_iter()
                    .filter_map(|v| serde_json::from_value(v).ok())
                    .collect();
                yield parsed;
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
    pub fn report<R>(&self, report: R) -> Pin<Box<dyn Stream<Item = R::Output> + Send>>
    where
        R: ReportHandler + ReportIdStatic + Serialize + Send + Sync + 'static,
        R::Output: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let runner = self.runner.clone();
        let report_id: Arc<str> = R::report_id_static().into();
        let report_value = serde_json::to_value(&report).expect("Report should serialize");
        let req = self.req.clone();

        let (sender, mut receiver) = mpsc::channel::<Value>(16);

        // Send subscription request to runner
        let subscription_tx = runner.subscription_tx.clone();
        tokio::spawn(async move {
            let _ = subscription_tx
                .send(SubscriptionRequest::Report {
                    report: report_value,
                    report_id,
                    req,
                    response_tx: sender,
                })
                .await;
        });

        // Convert channel to stream, deserializing output
        Box::pin(async_stream::stream! {
            while let Some(value) = receiver.recv().await {
                if let Ok(parsed) = serde_json::from_value::<R::Output>(value) {
                    yield parsed;
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
/// # Example
///
/// ```ignore
/// impl ReportHandler for GetParentTargets {
///     type Output = Vec<Target>;
///
///     fn compute(ctx: ReportContext) -> impl Stream<Item = Self::Output> {
///         let target_id = ctx.report.target_id.clone();
///         ctx.query(GetTargetsByIds { ids: vec![target_id] })
///             .flat_map(move |targets| {
///                 match targets.first().and_then(|t| t.parent_id.as_ref()) {
///                     Some(parent_id) => ctx.report(GetParentTargets {
///                         target_id: parent_id.clone(),
///                         depth: ctx.report.depth - 1,
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
    fn compute(ctx: ReportContext) -> Pin<Box<dyn Stream<Item = Self::Output> + Send>>;
}
