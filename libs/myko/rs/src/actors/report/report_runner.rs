use std::sync::Arc;

use log::{debug, error, trace};
use ractor::{Actor, ActorRef};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::report::SubscriptionRequest;

pub struct ReportRunner;

pub struct ReportRunnerArgs {
    /// The transaction ID for this report subscription
    pub tx: Arc<str>,
    /// Channel to send report outputs back to the subscriber
    pub output_tx: mpsc::Sender<Value>,
    /// Channel to receive subscription requests from ReportContext
    pub subscription_rx: mpsc::Receiver<SubscriptionRequest>,
    /// Reference to report manager for fulfilling sub-report requests
    pub report_manager: ActorRef<super::report_manager::ReportManagerMsg>,
}

pub struct ReportRunnerState {
    tx: Arc<str>,
    output_tx: mpsc::Sender<Value>,
    report_manager: ActorRef<super::report_manager::ReportManagerMsg>,
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

impl Actor for ReportRunner {
    type State = ReportRunnerState;
    type Msg = ReportRunnerMsg;
    type Arguments = ReportRunnerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        mut args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        trace!("Starting ReportRunner for tx: {}", args.tx);

        // Spawn a task to forward subscription requests to the actor
        let myself_clone = myself.clone();
        tokio::spawn(async move {
            while let Some(request) = args.subscription_rx.recv().await {
                if let Err(e) = myself_clone.send_message(ReportRunnerMsg::HandleSubscription(request)) {
                    error!("Failed to forward subscription request: {}", e);
                    break;
                }
            }
        });

        Ok(ReportRunnerState {
            tx: args.tx,
            output_tx: args.output_tx,
            report_manager: args.report_manager,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            ReportRunnerMsg::EmitValue(value) => {
                let value_preview = value.to_string();
                let preview = if value_preview.len() > 100 {
                    format!("{}...", &value_preview[..100])
                } else {
                    value_preview
                };
                trace!("ReportRunner [{}] emitting value: {}", state.tx, preview);
                if let Err(e) = state.output_tx.send(value).await {
                    error!("Failed to send report output: {}", e);
                    // Output channel closed, stop the runner
                    myself.stop(Some("output channel closed".to_string()));
                }
            }
            ReportRunnerMsg::Complete => {
                debug!("ReportRunner [{}] completed", state.tx);
                myself.stop(Some("report completed".to_string()));
            }
            ReportRunnerMsg::HandleSubscription(request) => {
                match request {
                    SubscriptionRequest::Query {
                        query,
                        query_id,
                        query_item_type,
                        response_tx,
                    } => {
                        trace!(
                            "ReportRunner [{}] subscribing to query: {}",
                            state.tx,
                            query_id
                        );
                        // Forward to report manager which will coordinate with query manager
                        if let Err(e) = state.report_manager.send_message(
                            super::report_manager::ReportManagerMsg::SubscribeQuery {
                                query,
                                query_id,
                                query_item_type,
                                response_tx,
                            },
                        ) {
                            error!("Failed to forward query subscription: {}", e);
                        }
                    }
                    SubscriptionRequest::Report {
                        report,
                        report_id,
                        response_tx,
                    } => {
                        trace!(
                            "ReportRunner [{}] subscribing to report: {}",
                            state.tx,
                            report_id
                        );
                        // Forward to report manager to start the sub-report
                        if let Err(e) = state.report_manager.send_message(
                            super::report_manager::ReportManagerMsg::SubscribeReport {
                                report,
                                report_id,
                                response_tx,
                            },
                        ) {
                            error!("Failed to forward report subscription: {}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        debug!("ReportRunner [{}] stopped", state.tx);
        Ok(())
    }
}
