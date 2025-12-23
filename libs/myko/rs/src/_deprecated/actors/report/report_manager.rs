use std::{
    collections::HashMap,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::Arc,
};

use futures::{Stream, StreamExt};
use log::{error, trace};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    context::RequestContext,
    report::{ReportContext, ReportOutput, WrappedReport},
    runtime::{Actor, ActorRef},
    server::MykoServerCtx,
};

/// Type alias for report compute functions.
/// Returns Result to allow framework-level arg validation before computing.
pub type ReportComputeFn = Arc<
    dyn Fn(ReportContext, Value) -> Result<Pin<Box<dyn Stream<Item = Value> + Send>>, String> + Send + Sync,
>;

/// Type alias for report parser functions
pub type ReportParserFn = Arc<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

pub struct ReportManagerArgs {
    pub ctx: Arc<MykoServerCtx>,
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
    /// Registered report handlers by report_id
    handlers: HashMap<Arc<str>, RegisteredReport>,
    /// Active report cancellation tokens by tx
    runners: HashMap<Arc<str>, CancellationToken>,
    /// Pending cancellations - tx values that received StopReport before StartReport.
    /// If StartReport arrives for a tx in this set, we skip starting the report.
    pending_stops: std::collections::HashSet<Arc<str>>,
    /// Message counter for throughput tracking
    msg_count: u64,
    /// Last time we logged stats
    last_stats_log: std::time::Instant,
}

pub enum ReportManagerMsg {
    /// Register a new report handler
    RegisterReport(RegisterReportData),
    /// Start a new report subscription with request context.
    /// The optional CancellationToken is the parent's token - if provided, we create a child token
    /// so cancellation automatically propagates from parent to child.
    StartReport(
        WrappedReport,
        RequestContext,
        mpsc::Sender<ReportOutput>,
        Option<CancellationToken>,
    ),
    /// Stop a report by tx
    StopReport(Arc<str>),
}

impl std::fmt::Debug for ReportManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReportManagerMsg::RegisterReport(data) => {
                write!(f, "RegisterReport({})", data.report_id)
            }
            ReportManagerMsg::StartReport(wrapped, req, _, parent_token) => {
                write!(
                    f,
                    "StartReport({}, tx={}, has_parent={})",
                    wrapped.report_id,
                    req.tx(),
                    parent_token.is_some()
                )
            }
            ReportManagerMsg::StopReport(tx) => write!(f, "StopReport({})", tx),
        }
    }
}

impl ReportManager {
    /// Create a new ReportManager with the given arguments.
    /// Collects all report registrations from inventory and registers them.
    fn create(args: ReportManagerArgs, _myself: ActorRef<ReportManagerMsg>) -> Self {
        use crate::report::ReportRegistration;
        use log::trace;

        // Collect all registered reports from inventory
        let mut handlers = HashMap::new();
        let mut report_ids: Vec<&str> = Vec::new();
        for registration in inventory::iter::<ReportRegistration> {
            trace!("Registering report: {}", registration.report_id);
            report_ids.push(registration.report_id);
            let data = (registration.factory)();
            handlers.insert(
                data.report_id,
                RegisteredReport {
                    compute_fn: data.compute_fn,
                },
            );
        }

        log::debug!(
            "ReportManager: {} reports registered: {:?}",
            handlers.len(),
            report_ids
        );

        Self {
            ctx: args.ctx,
            handlers,
            runners: HashMap::new(),
            pending_stops: std::collections::HashSet::new(),
            msg_count: 0,
            last_stats_log: std::time::Instant::now(),
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
        parent_token: Option<CancellationToken>,
    ) {
        let report_id: Arc<str> = wrapped_report.report_id.clone().into();
        let tx: Arc<str> = req.tx.clone();

        // Check if this report was already stopped before it started (race condition).
        // This happens when StopReport arrives before StartReport is processed.
        if self.pending_stops.remove(&tx) {
            log::debug!(
                "Report {} tx={} was stopped before it started, skipping",
                report_id,
                tx
            );
            return;
        }

        // If parent token is already cancelled, skip starting (parent was stopped)
        if let Some(ref pt) = parent_token
            && pt.is_cancelled()
        {
            log::debug!(
                "Report {} tx={} parent already cancelled, skipping",
                report_id,
                tx
            );
            return;
        }

        trace!(
            "Starting report {} with tx {} (args: {})",
            report_id,
            req.tx(),
            serde_json::to_string(&wrapped_report.report).unwrap_or_default()
        );

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

        // Create cancellation token for stopping the report.
        // If parent_token is provided, create a child token so cancellation propagates automatically.
        // Otherwise create a new root token (for top-level reports).
        let cancel_token = match parent_token {
            Some(pt) => pt.child_token(),
            None => CancellationToken::new(),
        };

        // Create ReportContext with report args and request context.
        // The cancel_token is passed so that all subscriptions created via ctx.query()
        // will be cancelled when the report is stopped (handles async_stream drop issues).
        let report_ctx = ReportContext {
            req: req.clone(),
            server_ctx: self.ctx.clone(),
            report_args: wrapped_report.report.clone(),
            cancel_token: cancel_token.clone(),
        };

        let tx: Arc<str> = req.tx.clone();

        // Create the report stream with panic catching
        let compute_fn = handler.compute_fn.clone();
        let report_args = wrapped_report.report.clone();
        let report_stream = catch_unwind(AssertUnwindSafe(|| {
            compute_fn(report_ctx, report_args)
        }));

        // Handle both panics and arg parse errors
        let report_stream = match report_stream {
            Ok(Ok(stream)) => stream,
            Ok(Err(arg_error)) => {
                // Arg parsing failed (framework-level validation)
                error!(
                    "Report {} arg parse error: {} (tx={})",
                    report_id, arg_error, tx
                );
                let _ = output_tx.blocking_send(ReportOutput::Error(arg_error));
                return;
            }
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

        // Store the cancellation token
        self.runners.insert(tx.clone(), cancel_token.clone());
        log::debug!(
            "[ReportManager] Started report {} tx={}, active_runners={}",
            report_id,
            tx,
            self.runners.len()
        );

        // Spawn async task to drive the report stream and send values directly to output
        let report_id_for_task = report_id.clone();
        let tx_for_task = tx.clone();
        self.ctx.tokio_handle.spawn(async move {
            trace!(
                "[Report {}] Stream task started for tx={}",
                report_id_for_task,
                tx_for_task
            );
            let mut stream = report_stream;

            loop {
                // Use biased select to always check cancellation first
                tokio::select! {
                    biased;

                    // Check for cancellation - MUST be first with biased
                    _ = cancel_token.cancelled() => {
                        log::debug!(
                            "[Report {}] Stream cancelled for tx={}",
                            report_id_for_task,
                            tx_for_task
                        );
                        break;
                    }
                    // Poll the stream for next value
                    maybe_value = stream.next() => {
                        match maybe_value {
                            Some(value) => {
                                // Value is already a serde_json::Value from the stream
                                let value_preview = value.to_string();
                                let preview = if value_preview.len() > 100 {
                                    format!("{}...", &value_preview[..100])
                                } else {
                                    value_preview
                                };
                                trace!("[Report {}] Emitting value: {}", report_id_for_task, preview);

                                // Send directly to output channel
                                if output_tx.send(ReportOutput::Value(value)).await.is_err() {
                                    trace!(
                                        "[Report {}] Output channel closed for tx={}",
                                        report_id_for_task,
                                        tx_for_task
                                    );
                                    break;
                                }
                            }
                            None => {
                                // Stream completed naturally
                                break;
                            }
                        }
                    }
                }
            }

            trace!(
                "[Report {}] Stream completed for tx={}",
                report_id_for_task,
                tx_for_task
            );
        });
    }

    fn handle_stop_report(&mut self, tx: Arc<str>) {
        if let Some(cancel_token) = self.runners.remove(&tx) {
            log::debug!(
                "[ReportManager] Stopping report tx={}, remaining_runners={}",
                tx,
                self.runners.len()
            );
            // Cancel the stream task - this will:
            // 1. Break out of the tokio::select! loop
            // 2. Drop the report stream
            // 3. Drop all query/report subscription streams held by the report
            // 4. Trigger cleanup via RAII guards (SubscriptionGuard sends CancelQuery)
            cancel_token.cancel();
        } else {
            // No runner yet - StopReport arrived before StartReport was processed.
            // Add to pending_stops so when StartReport arrives, it's skipped.
            log::debug!(
                "[ReportManager] StopReport for tx={} before StartReport, adding to pending_stops (pending={})",
                tx,
                self.pending_stops.len() + 1
            );
            self.pending_stops.insert(tx);
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
        // Track throughput and log stats periodically
        self.msg_count += 1;
        let since_last_log = self.last_stats_log.elapsed();
        if since_last_log.as_secs() >= 5 {
            let active_runners = self.runners.len();
            let pending_stops = self.pending_stops.len();
            if active_runners > 0 || pending_stops > 0 || self.msg_count > 10 {
                log::warn!(
                    "[RM] Stats: active_runners={}, pending_stops={}, msgs_in_period={}",
                    active_runners,
                    pending_stops,
                    self.msg_count
                );
            }
            self.msg_count = 0;
            self.last_stats_log = std::time::Instant::now();
        }

        match msg {
            ReportManagerMsg::RegisterReport(data) => {
                self.handle_register_report(data);
            }
            ReportManagerMsg::StartReport(wrapped_report, req, output_tx, parent_token) => {
                self.handle_start_report(wrapped_report, req, output_tx, parent_token);
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
