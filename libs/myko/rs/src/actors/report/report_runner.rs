use std::sync::Arc;

use crossbeam::channel as crossbeam_channel;
use log::{error, trace};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    report::{ReportOutput, SubscriptionRequest},
    runtime::{Actor, ActorRef},
};

use super::report_manager::ReportManagerMsg;

pub struct ReportRunnerArgs {
    /// The transaction ID for this report subscription
    pub tx: Arc<str>,
    /// Channel to send report outputs back to the subscriber
    pub output_tx: mpsc::Sender<ReportOutput>,
    /// Channel to receive subscription requests from ReportContext (crossbeam)
    pub subscription_rx: crossbeam_channel::Receiver<SubscriptionRequest>,
    /// Reference to report manager for fulfilling sub-report requests
    pub report_manager: ActorRef<ReportManagerMsg>,
    /// Tokio runtime handle for spawning subscription reader task
    pub tokio_handle: tokio::runtime::Handle,
}

pub struct ReportRunner {
    tx: Arc<str>,
    output_tx: mpsc::Sender<ReportOutput>,
    report_manager: ActorRef<ReportManagerMsg>,
}

#[derive(Debug)]
pub enum ReportRunnerMsg {
    /// Report computation produced a new value
    EmitValue(Value),
    /// Report computation completed or errored
    Complete,
    /// Handle a subscription request from the report's compute function
    HandleSubscription(SubscriptionRequest),
}

impl ReportRunner {
    fn handle_subscription(&self, request: SubscriptionRequest) {
        match request {
            SubscriptionRequest::Query {
                query,
                query_id,
                query_item_type,
                response_tx,
            } => {
                trace!(
                    "ReportRunner [{}] subscribing to query: {}",
                    self.tx,
                    query_id
                );
                // Forward to report manager which will coordinate with query manager
                if let Err(e) = self
                    .report_manager
                    .send_message(ReportManagerMsg::SubscribeQuery {
                        query,
                        query_id,
                        query_item_type,
                        response_tx,
                    })
                {
                    error!("Failed to forward query subscription: {}", e);
                }
            }
            SubscriptionRequest::Report {
                report,
                report_id,
                req,
                response_tx,
            } => {
                trace!(
                    "ReportRunner [{}] subscribing to report: {}",
                    self.tx,
                    report_id
                );
                // Forward to report manager to start the sub-report
                if let Err(e) = self
                    .report_manager
                    .send_message(ReportManagerMsg::SubscribeReport {
                        report,
                        report_id,
                        req,
                        response_tx,
                    })
                {
                    error!("Failed to forward report subscription: {}", e);
                }
            }
        }
    }
}

impl Actor for ReportRunner {
    type Msg = ReportRunnerMsg;
    type Args = ReportRunnerArgs;

    fn new(args: Self::Args, myself: ActorRef<Self::Msg>) -> Self {
        // Spawn a task to read subscription requests and forward them to self
        let subscription_rx = args.subscription_rx;
        let myself_ref = myself.clone();
        let tx_for_task = args.tx.clone();

        args.tokio_handle.spawn(async move {
            loop {
                let rx = subscription_rx.clone();
                let result = tokio::task::spawn_blocking(move || rx.recv()).await;

                match result {
                    Ok(Ok(request)) => {
                        trace!(
                            "[ReportRunner {}] Received subscription request: {:?}",
                            tx_for_task,
                            match &request {
                                SubscriptionRequest::Query { query_id, .. } =>
                                    format!("Query({})", query_id),
                                SubscriptionRequest::Report { report_id, .. } =>
                                    format!("Report({})", report_id),
                            }
                        );
                        if let Err(e) =
                            myself_ref.send_message(ReportRunnerMsg::HandleSubscription(request))
                        {
                            trace!("ReportRunner channel closed: {}", e);
                            break;
                        }
                    }
                    Ok(Err(_)) => {
                        // Channel closed
                        trace!("[ReportRunner {}] Subscription channel closed", tx_for_task);
                        break;
                    }
                    Err(e) => {
                        error!("[ReportRunner {}] spawn_blocking error: {}", tx_for_task, e);
                        break;
                    }
                }
            }
        });

        Self {
            tx: args.tx,
            output_tx: args.output_tx,
            report_manager: args.report_manager,
        }
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            ReportRunnerMsg::EmitValue(value) => {
                let value_preview = value.to_string();
                let preview = if value_preview.len() > 100 {
                    format!("{}...", &value_preview[..100])
                } else {
                    value_preview
                };
                trace!("ReportRunner [{}] emitting value: {}", self.tx, preview);

                // Use blocking_send for sync context
                if let Err(e) = self.output_tx.blocking_send(ReportOutput::Value(value)) {
                    error!("Failed to send report output: {}", e);
                }
            }
            ReportRunnerMsg::Complete => {
                trace!("ReportRunner [{}] completed", self.tx);
            }
            ReportRunnerMsg::HandleSubscription(request) => {
                self.handle_subscription(request);
            }
        }
    }
}
