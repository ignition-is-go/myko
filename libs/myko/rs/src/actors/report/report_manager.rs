use std::{collections::HashMap, pin::Pin, sync::Arc};

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
    report::{ReportContext, ReportRunnerHandle, WrappedReport},
    runtime::{Actor, ActorHandle, ActorRef},
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
    myself: Option<ActorRef<ReportManagerMsg>>,
    /// Registered report handlers by report_id
    handlers: HashMap<Arc<str>, RegisteredReport>,
    /// Active report runners by tx
    runners: HashMap<Arc<str>, ActorRef<ReportRunnerMsg>>,
}

pub enum ReportManagerMsg {
    /// Register a new report handler
    RegisterReport(RegisterReportData),
    /// Start a new report subscription with request context
    StartReport(WrappedReport, RequestContext, mpsc::Sender<Value>),
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
        response_tx: mpsc::Sender<Value>,
    },
    /// Stop a report by tx
    StopReport(Arc<str>),
    /// Set self-reference (called immediately after spawn)
    SetMyself(ActorRef<ReportManagerMsg>),
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
            ReportManagerMsg::SetMyself(_) => write!(f, "SetMyself"),
        }
    }
}

impl ReportManager {
    pub fn new(args: ReportManagerArgs) -> Self {
        Self {
            ctx: args.ctx,
            query_manager: args.query_manager,
            myself: None,
            handlers: HashMap::new(),
            runners: HashMap::new(),
        }
    }

    pub fn spawn(args: ReportManagerArgs) -> ActorHandle<ReportManagerMsg> {
        let actor = Self::new(args);
        let handle = crate::runtime::spawn::spawn(actor);

        // Set self-referenc
        let actor_ref = handle.actor_ref();
        let _ = actor_ref.send_message(ReportManagerMsg::SetMyself(actor_ref.clone()));

        handle
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
        output_tx: mpsc::Sender<Value>,
    ) {
        let report_id: Arc<str> = wrapped_report.report_id.clone().into();

        trace!("Starting report {} with tx {}", report_id, req.tx());

        let handler = match self.handlers.get(&report_id) {
            Some(h) => h,
            None => {
                error!(
                    "No handler registered for report {}: {:?}",
                    report_id,
                    self.handlers.keys().collect::<Vec<_>>()
                );
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

        // Create the report stream
        let compute_fn = handler.compute_fn.clone();
        let report_stream = compute_fn(report_ctx, wrapped_report.report.clone());

        // Get myself reference
        let myself = match &self.myself {
            Some(m) => m.clone(),
            None => {
                error!("ReportManager myself reference not set");
                return;
            }
        };

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
        response_tx: mpsc::Sender<Value>,
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

        // Get myself reference
        let myself = match &self.myself {
            Some(m) => m.clone(),
            None => {
                error!("ReportManager myself reference not set");
                return;
            }
        };

        // Recursively start the sub-report with propagated context
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
            ReportManagerMsg::SetMyself(myself) => {
                self.myself = Some(myself);
            }
        }
    }
}
