use std::{collections::HashMap, pin::Pin, sync::Arc};

use futures::{Stream, StreamExt};
use futures_signals::signal_map::MapDiff;
use log::{debug, error, trace};
use ractor::{Actor, ActorRef};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    actors::query::query_manager::QueryManagerMsg,
    api::query::WrappedQuery,
    prelude::AnyItem,
    report::{ReportContext, ReportRunnerHandle, WrappedReport},
    server::MykoServerCtx,
    utils::signal_stream::SignalMapStream,
};

use super::report_runner::{ReportRunner, ReportRunnerArgs, ReportRunnerMsg};

pub struct ReportManager;

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

pub struct ReportManagerState {
    ctx: Arc<MykoServerCtx>,
    query_manager: ActorRef<QueryManagerMsg>,
    /// Registered report handlers by report_id
    handlers: HashMap<Arc<str>, RegisteredReport>,
    /// Active report runners by tx
    runners: HashMap<Arc<str>, ActorRef<ReportRunnerMsg>>,
}

struct RegisteredReport {
    compute_fn: ReportComputeFn,
}

pub struct RegisterReportData {
    pub report_id: Arc<str>,
    pub compute_fn: ReportComputeFn,
}

pub enum ReportManagerMsg {
    /// Register a new report handler
    RegisterReport(RegisterReportData),
    /// Start a new report subscription
    StartReport(WrappedReport, mpsc::Sender<Value>),
    /// Subscribe to a query from within a report
    SubscribeQuery {
        query: Value,
        query_id: Arc<str>,
        query_item_type: Arc<str>,
        response_tx: mpsc::Sender<Vec<Value>>,
    },
    /// Subscribe to another report from within a report
    SubscribeReport {
        report: Value,
        report_id: Arc<str>,
        response_tx: mpsc::Sender<Value>,
    },
    /// Stop a report by tx
    StopReport(Arc<str>),
}

impl Actor for ReportManager {
    type State = ReportManagerState;
    type Msg = ReportManagerMsg;
    type Arguments = ReportManagerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("ReportManager starting");
        Ok(ReportManagerState {
            ctx: args.ctx,
            query_manager: args.query_manager,
            handlers: HashMap::new(),
            runners: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            ReportManagerMsg::RegisterReport(data) => {
                debug!("Registering report handler: {}", data.report_id);
                state.handlers.insert(
                    data.report_id,
                    RegisteredReport {
                        compute_fn: data.compute_fn,
                    },
                );
            }
            ReportManagerMsg::StartReport(wrapped_report, output_tx) => {
                let report_id: Arc<str> = wrapped_report.report_id.clone().into();
                let tx: Arc<str> = wrapped_report
                    .report
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .map(|s| s.into())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string().into());

                trace!("Starting report {} with tx {}", report_id, tx);

                let handler = match state.handlers.get(&report_id) {
                    Some(h) => h,
                    None => {
                        error!(
                            "No handler registered for report {}: {:?}",
                            report_id,
                            state.handlers.keys().collect::<Vec<_>>()
                        );
                        return Ok(());
                    }
                };

                // Create channels for the runner
                let (subscription_tx, subscription_rx) = mpsc::channel(16);

                // Create ReportContext with report args
                let runner_handle = Arc::new(ReportRunnerHandle { subscription_tx });
                let report_ctx = ReportContext {
                    server_ctx: state.ctx.clone(),
                    report_args: wrapped_report.report.clone(),
                    runner: runner_handle,
                };

                // Create the report stream
                let compute_fn = handler.compute_fn.clone();
                let report_stream = compute_fn(report_ctx, wrapped_report.report.clone());

                // Spawn the runner actor
                let runner_args = ReportRunnerArgs {
                    tx: tx.clone(),
                    output_tx: output_tx.clone(),
                    subscription_rx,
                    report_manager: myself.clone(),
                };

                match Actor::spawn(None, ReportRunner, runner_args).await {
                    Ok((runner, _handle)) => {
                        state.runners.insert(tx.clone(), runner.clone());

                        // Spawn a task to drive the report stream and send values to runner
                        let runner_ref = runner.clone();
                        tokio::spawn(async move {
                            let mut stream = report_stream;
                            while let Some(value) = stream.next().await {
                                let json_value = match serde_json::to_value(&value) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        error!("Failed to serialize report output: {}", e);
                                        continue;
                                    }
                                };
                                if let Err(e) =
                                    runner_ref.send_message(ReportRunnerMsg::EmitValue(json_value))
                                {
                                    trace!("Runner stopped, ending stream: {}", e);
                                    break;
                                }
                            }
                            let _ = runner_ref.send_message(ReportRunnerMsg::Complete);
                        });
                    }
                    Err(e) => {
                        error!("Failed to spawn report runner: {}", e);
                    }
                }
            }
            ReportManagerMsg::SubscribeQuery {
                query,
                query_id,
                query_item_type,
                response_tx,
            } => {
                trace!("Report subscribing to query: {}", query_id);

                // Clone query_id for use in async block (original moves into wrapped_query)
                let query_id_for_trace = query_id.clone();

                // Create a WrappedQuery and forward to QueryManager
                let wrapped_query = WrappedQuery {
                    query,
                    query_id,
                    query_item_type,
                };

                // Get the signal from QueryManager and forward updates to response_tx
                let query_manager = state.query_manager.clone();
                tokio::spawn(async move {
                    let query_id = query_id_for_trace;
                    use std::collections::BTreeMap;

                    // First get the initial snapshot - futures-signals doesn't emit
                    // Replace for empty initial state, so we need to send it explicitly
                    let initial_snapshot = ractor::call!(
                        query_manager,
                        QueryManagerMsg::QuerySnapshot,
                        wrapped_query.clone()
                    );

                    let mut accumulated: BTreeMap<Arc<str>, Arc<dyn AnyItem>> = match initial_snapshot
                    {
                        Ok(snapshot) => snapshot,
                        Err(e) => {
                            error!("Failed to get initial snapshot for query: {}", e);
                            return;
                        }
                    };

                    // Send initial state immediately (even if empty)
                    let initial_values: Vec<Value> =
                        accumulated.values().map(|item| item.to_value()).collect();
                    trace!(
                        "Report query [{}] sending initial {} items to report",
                        query_id,
                        initial_values.len()
                    );
                    if response_tx.send(initial_values).await.is_err() {
                        return;
                    }

                    // Now subscribe for updates
                    match ractor::call!(query_manager, QueryManagerMsg::StartQuery, wrapped_query) {
                        Ok(signal_map) => {
                            trace!("Report query [{}] subscribed for updates", query_id);

                            let mut stream = SignalMapStream::new(signal_map);
                            let mut seen_initial_replace = false;

                            while let Some(diff) = stream.next().await {
                                // Skip the first Replace - we already sent the initial snapshot
                                if matches!(&diff, MapDiff::Replace { .. }) && !seen_initial_replace {
                                    seen_initial_replace = true;
                                    trace!(
                                        "Report query [{}] skipping initial Replace (already sent snapshot)",
                                        query_id
                                    );
                                    continue;
                                }

                                trace!(
                                    "Report query [{}] received diff: {:?}",
                                    query_id,
                                    match &diff {
                                        MapDiff::Replace { entries } => {
                                            format!("Replace({} entries)", entries.len())
                                        }
                                        MapDiff::Insert { key, .. } => format!("Insert({})", key),
                                        MapDiff::Update { key, .. } => format!("Update({})", key),
                                        MapDiff::Remove { key } => format!("Remove({})", key),
                                        MapDiff::Clear {} => "Clear".to_string(),
                                    }
                                );

                                match diff {
                                    MapDiff::Replace { entries } => {
                                        // Subsequent Replace (e.g., on reconnect) - process normally
                                        accumulated.clear();
                                        for (k, v) in entries {
                                            accumulated.insert(k, v);
                                        }
                                    }
                                    MapDiff::Insert { key, value } => {
                                        accumulated.insert(key, value);
                                    }
                                    MapDiff::Update { key, value } => {
                                        accumulated.insert(key, value);
                                    }
                                    MapDiff::Remove { key } => {
                                        accumulated.remove(&key);
                                    }
                                    MapDiff::Clear {} => {
                                        accumulated.clear();
                                    }
                                }

                                // Send current state as Vec<Value>
                                let values: Vec<Value> =
                                    accumulated.values().map(|item| item.to_value()).collect();
                                trace!(
                                    "Report query [{}] sending {} items to report",
                                    query_id,
                                    values.len()
                                );
                                if response_tx.send(values).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to start query subscription: {}", e);
                        }
                    }
                });
            }
            ReportManagerMsg::SubscribeReport {
                report,
                report_id,
                response_tx,
            } => {
                trace!("Report subscribing to sub-report: {}", report_id);

                // Create a WrappedReport and start it
                let wrapped_report = WrappedReport {
                    report,
                    report_id: report_id.to_string(),
                };

                // Recursively start the sub-report
                if let Err(e) =
                    myself.send_message(ReportManagerMsg::StartReport(wrapped_report, response_tx))
                {
                    error!("Failed to start sub-report: {}", e);
                }
            }
            ReportManagerMsg::StopReport(tx) => {
                if let Some(runner) = state.runners.remove(&tx) {
                    debug!("Stopping report runner for tx: {}", tx);
                    runner.stop(Some("stopped by manager".to_string()));
                }
            }
        }
        Ok(())
    }
}
