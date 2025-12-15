use std::sync::Arc;

use crossbeam::channel as crossbeam_channel;
use log::{error, trace};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    report::SubscriptionRequest,
    runtime::{Actor, ActorHandle, ActorRef},
};

use super::report_manager::ReportManagerMsg;

pub struct ReportRunnerArgs {
    /// The transaction ID for this report subscription
    pub tx: Arc<str>,
    /// Channel to send report outputs back to the subscriber
    pub output_tx: mpsc::Sender<Value>,
    /// Channel to receive subscription requests from ReportContext (crossbeam)
    pub subscription_rx: crossbeam_channel::Receiver<SubscriptionRequest>,
    /// Reference to report manager for fulfilling sub-report requests
    pub report_manager: ActorRef<ReportManagerMsg>,
}

pub struct ReportRunner {
    tx: Arc<str>,
    output_tx: mpsc::Sender<Value>,
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
    pub fn spawn(args: ReportRunnerArgs) -> ActorHandle<ReportRunnerMsg> {
        trace!("Starting ReportRunner for tx: {}", args.tx);

        let tx_for_log = args.tx.clone();
        let subscription_rx = args.subscription_rx;

        let actor = Self {
            tx: args.tx,
            output_tx: args.output_tx,
            report_manager: args.report_manager,
        };

        let handle = crate::runtime::spawn::spawn(actor);

        // Spawn thread to forward subscription requests to the actor
        let actor_ref = handle.actor_ref();
        std::thread::spawn(move || {
            for request in subscription_rx.iter() {
                if let Err(e) =
                    actor_ref.send_message(ReportRunnerMsg::HandleSubscription(request))
                {
                    error!("Failed to forward subscription request: {}", e);
                    break;
                }
            }
            trace!(
                "ReportRunner [{}] subscription forwarder exiting",
                tx_for_log
            );
        });

        handle
    }

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
                if let Err(e) = self.output_tx.blocking_send(value) {
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
