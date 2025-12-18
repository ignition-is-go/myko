use std::{
    collections::HashMap,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::Arc,
};

use crossbeam::channel as crossbeam_channel;
use futures::{Stream, StreamExt};
use log::{error, trace};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    actors::query::{common::QueryStreamUpdate, query_manager::QueryManagerMsg},
    api::query::WrappedQuery,
    context::RequestContext,
    prelude::AnyItem,
    report::{ReportContext, ReportOutput, ReportRunnerHandle, WrappedReport},
    runtime::{Actor, ActorRef},
    server::MykoServerCtx,
};

use super::report_runner::{ReportRunner, ReportRunnerArgs, ReportRunnerMsg};

/// Type alias for report compute functions
pub type ReportComputeFn = Arc<
    dyn Fn(ReportContext, Value) -> Pin<Box<dyn Stream<Item = Value> + Send>> + Send + Sync,
>;

/// Type alias for report parser functions
pub type ReportParserFn = Arc<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

pub struct ReportManagerArgs {
    pub ctx: Arc<MykoServerCtx>,
    pub query_manager: ActorRef<QueryManagerMsg>,
}

struct RegisteredReport {
    compute_fn: ReportComputeFn,
}

pub struct RegisterReportData {
    pub report_id: Arc<str>,
    pub compute_fn: ReportComputeFn,
}

pub struct ReportManager {
    ctx: Arc<MykoServerCtx>,
    query_manager: ActorRef<QueryManagerMsg>,
    myself: ActorRef<ReportManagerMsg>,
    /// Registered report handlers by report_id
    handlers: HashMap<Arc<str>, RegisteredReport>,
    /// Active report runners by tx
    runners: HashMap<Arc<str>, ActorRef<ReportRunnerMsg>>,
}

pub enum ReportManagerMsg {
    /// Register a new report handler
    RegisterReport(RegisterReportData),
    /// Start a new report subscription with request context
    StartReport(WrappedReport, RequestContext, mpsc::Sender<ReportOutput>),
    /// Subscribe to a query from within a report (uses crossbeam channel)
    SubscribeQuery {
        query: Value,
        query_id: Arc<str>,
        query_item_type: Arc<str>,
        response_tx: crossbeam_channel::Sender<Vec<Value>>,
    },
    /// Subscribe to another report from within a report
    SubscribeReport {
        report: Value,
        report_id: Arc<str>,
        /// Request context to propagate to the sub-report
        req: RequestContext,
        response_tx: mpsc::Sender<ReportOutput>,
    },
    /// Stop a report by tx
    StopReport(Arc<str>),
}

impl std::fmt::Debug for ReportManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReportManagerMsg::RegisterReport(data) => {
                write!(f, "RegisterReport({})", data.report_id)
            }
            ReportManagerMsg::StartReport(wrapped, req, _) => {
                write!(f, "StartReport({}, tx={})", wrapped.report_id, req.tx())
            }
            ReportManagerMsg::SubscribeQuery { query_id, .. } => {
                write!(f, "SubscribeQuery({})", query_id)
            }
            ReportManagerMsg::SubscribeReport { report_id, .. } => {
                write!(f, "SubscribeReport({})", report_id)
            }
            ReportManagerMsg::StopReport(tx) => write!(f, "StopReport({})", tx),
        }
    }
}

impl ReportManager {
    /// Create a new ReportManager with the given arguments.
    /// Collects all report registrations from inventory and registers them.
    fn create(args: ReportManagerArgs, myself: ActorRef<ReportManagerMsg>) -> Self {
        use crate::report::ReportRegistration;
        use log::{debug, trace};

        // Collect all registered reports from inventory
        let mut handlers = HashMap::new();
        for registration in inventory::iter::<ReportRegistration> {
            trace!("Registering report: {}", registration.report_id);
            let data = (registration.factory)();
            handlers.insert(
                data.report_id,
                RegisteredReport {
                    compute_fn: data.compute_fn,
                },
            );
        }

        debug!("ReportManager: {} reports registered", handlers.len());

        Self {
            ctx: args.ctx,
            query_manager: args.query_manager,
            myself,
            handlers,
            runners: HashMap::new(),
        }
    }

    fn handle_register_report(&mut self, data: RegisterReportData) {
        trace!("Registering report handler: {}", data.report_id);
        self.handlers.insert(
            data.report_id,
            RegisteredReport {
                compute_fn: data.compute_fn,
            },
        );
    }

    fn handle_start_report(
        &mut self,
        wrapped_report: WrappedReport,
        req: RequestContext,
        output_tx: mpsc::Sender<ReportOutput>,
    ) {
        let report_id: Arc<str> = wrapped_report.report_id.clone().into();

        trace!("Starting report {} with tx {}", report_id, req.tx());

        let handler = match self.handlers.get(&report_id) {
            Some(h) => h,
            None => {
                let err_msg = format!(
                    "No handler registered for report {}: {:?}",
                    report_id,
                    self.handlers.keys().collect::<Vec<_>>()
                );
                error!("{}", err_msg);
                let _ = output_tx.blocking_send(ReportOutput::Error(err_msg));
                return;
            }
        };

        // Create bounded channel for subscription requests with backpressure
        // 64 should be plenty for typical report dependency graphs
        let (subscription_tx, subscription_rx) = crossbeam_channel::bounded(64);

        // Create ReportContext with report args and request context
        let runner_handle = Arc::new(ReportRunnerHandle { subscription_tx });
        let report_ctx = ReportContext {
            req: req.clone(),
            server_ctx: self.ctx.clone(),
            report_args: wrapped_report.report.clone(),
            runner: runner_handle,
        };

        let tx: Arc<str> = req.tx.clone();

        // Create the report stream with panic catching
        let compute_fn = handler.compute_fn.clone();
        let report_args = wrapped_report.report.clone();
        let report_stream = catch_unwind(AssertUnwindSafe(|| {
            compute_fn(report_ctx, report_args)
        }));

        let report_stream = match report_stream {
            Ok(stream) => stream,
            Err(panic_payload) => {
                let panic_msg = extract_panic_message(panic_payload);
                error!(
                    "Report {} panicked during creation: {} (tx={})",
                    report_id, panic_msg, tx
                );
                let _ = output_tx.blocking_send(ReportOutput::Error(format!(
                    "Report handler panicked: {}",
                    panic_msg
                )));
                return;
            }
        };

        // Get myself reference
        let myself = self.myself.clone();

        // Spawn the runner actor
        let runner_args = ReportRunnerArgs {
            tx: tx.clone(),
            output_tx: output_tx.clone(),
            subscription_rx,
            report_manager: myself,
        };

        let runner_handle = ReportRunner::spawn(runner_args);
        let runner = runner_handle.actor_ref();
        self.runners.insert(tx.clone(), runner.clone());

        // Spawn async task to drive the report stream and send values to runner
        // Note: Panics during stream polling will crash this tokio task but not the server.
        // The panic hook will log them, and the client won't receive further updates.
        let runner_ref = runner.clone();
        self.ctx.tokio_handle.spawn(async move {
            let mut stream = report_stream;
            while let Some(value) = stream.next().await {
                let json_value = match serde_json::to_value(&value) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to serialize report output: {}", e);
                        continue;
                    }
                };
                if let Err(e) = runner_ref.send_message(ReportRunnerMsg::EmitValue(json_value)) {
                    trace!("Runner stopped, ending stream: {}", e);
                    break;
                }
            }
            let _ = runner_ref.send_message(ReportRunnerMsg::Complete);
        });
    }

    fn handle_subscribe_query(
        &self,
        query: Value,
        query_id: Arc<str>,
        query_item_type: Arc<str>,
        response_tx: crossbeam_channel::Sender<Vec<Value>>,
    ) {
        trace!("Report subscribing to query: {}", query_id);

        // Create a WrappedQuery and forward to QueryManager
        let wrapped_query = WrappedQuery {
            query,
            query_id: query_id.clone(),
            query_item_type,
        };

        // Get crossbeam receiver for query updates
        let query_manager = self.query_manager.clone();
        let receiver = match query_manager.call(|r| QueryManagerMsg::WatchWrappedQuery(wrapped_query, r)) {
            Ok(rx) => rx,
            Err(e) => {
                error!("Failed to start query subscription: {}", e);
                return;
            }
        };

        // Use shared tokio runtime with spawn_blocking for crossbeam iteration
        // This uses the tokio blocking thread pool instead of spawning unbounded OS threads
        self.ctx.tokio_handle.spawn(async move {
            let mut accumulated: std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem>> =
                std::collections::BTreeMap::new();

            loop {
                // Use spawn_blocking to poll the crossbeam channel from the blocking pool
                let rx = receiver.clone();
                let update = tokio::task::spawn_blocking(move || rx.recv()).await;

                let update = match update {
                    Ok(Ok(u)) => u,
                    _ => break, // Channel closed or task panicked
                };

                match update {
                    QueryStreamUpdate::Initial(entries) => {
                        accumulated = entries;
                    }
                    QueryStreamUpdate::Upsert(key, value) => {
                        accumulated.insert(key, value);
                    }
                    QueryStreamUpdate::Remove(key) => {
                        accumulated.remove(&key);
                    }
                    QueryStreamUpdate::Error(msg) => {
                        error!("Query error in report subscription [{}]: {}", query_id, msg);
                        break;
                    }
                }

                // Send current state as Vec<Value>
                let values: Vec<Value> = accumulated.values().map(|item| item.to_value()).collect();
                trace!(
                    "Report query [{}] sending {} items to report",
                    query_id,
                    values.len()
                );
                if response_tx.send(values).is_err() {
                    trace!("Report query response channel closed");
                    break;
                }
            }
        });
    }

    fn handle_subscribe_report(
        &self,
        report: Value,
        report_id: Arc<str>,
        req: RequestContext,
        response_tx: mpsc::Sender<ReportOutput>,
    ) {
        trace!(
            "Report subscribing to sub-report: {} (lineage: {})",
            report_id,
            req.lineage_string()
        );

        // Create a WrappedReport and start it
        let wrapped_report = WrappedReport {
            report,
            report_id: report_id.to_string(),
        };

        // Create child context with extended lineage
        let child_req = req.child(&report_id);

        // Recursively start the sub-report with propagated context
        let myself = self.myself.clone();
        if let Err(e) =
            myself.send_message(ReportManagerMsg::StartReport(wrapped_report, child_req, response_tx))
        {
            error!("Failed to start sub-report: {}", e);
        }
    }

    fn handle_stop_report(&mut self, tx: Arc<str>) {
        if self.runners.remove(&tx).is_some() {
            trace!("Stopping report runner for tx: {}", tx);
            // The runner actor will stop when all references to its ActorRef are dropped
            // and the channel is closed
        }
    }
}

impl Actor for ReportManager {
    type Msg = ReportManagerMsg;
    type Args = ReportManagerArgs;

    fn new(args: Self::Args, myself: ActorRef<Self::Msg>) -> Self {
        Self::create(args, myself)
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            ReportManagerMsg::RegisterReport(data) => {
                self.handle_register_report(data);
            }
            ReportManagerMsg::StartReport(wrapped_report, req, output_tx) => {
                self.handle_start_report(wrapped_report, req, output_tx);
            }
            ReportManagerMsg::SubscribeQuery {
                query,
                query_id,
                query_item_type,
                response_tx,
            } => {
                self.handle_subscribe_query(query, query_id, query_item_type, response_tx);
            }
            ReportManagerMsg::SubscribeReport {
                report,
                report_id,
                req,
                response_tx,
            } => {
                self.handle_subscribe_report(report, report_id, req, response_tx);
            }
            ReportManagerMsg::StopReport(tx) => {
                self.handle_stop_report(tx);
            }
        }
    }
}

/// Extract a human-readable message from a panic payload
fn extract_panic_message(panic_payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic_payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}
