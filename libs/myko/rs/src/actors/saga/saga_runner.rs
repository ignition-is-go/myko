//! Saga runner actor that processes events through a saga's pipeline.
//!
//! Each saga gets its own SagaRunner actor that:
//! - Subscribes to the EventBus for high-throughput event reception
//! - Processes events through the saga's stream pipeline
//! - Forwards resulting commands to CommandManager

use std::sync::Arc;

use futures::StreamExt;
use log::{debug, error, trace};

use uuid::Uuid;

use crate::{
    actors::command::command_manager::CommandManagerMsg,
    context::RequestContext,
    runtime::{Actor, ActorHandle, ActorRef},
    saga::{AnySaga, SagaContext},
};

/// Actor that runs a single saga's event processing pipeline.
pub struct SagaRunner {
    saga_name: &'static str,
    stopped: bool,
}

/// Messages for SagaRunner actor.
pub enum SagaRunnerMsg {
    /// Stop the saga runner (events come via EventBus subscription, not messages)
    Stop,
}

impl std::fmt::Debug for SagaRunnerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SagaRunnerMsg::Stop => write!(f, "Stop"),
        }
    }
}

/// Arguments for spawning a SagaRunner.
pub struct SagaRunnerArgs {
    pub saga: Arc<dyn AnySaga>,
    pub ctx: Arc<SagaContext>,
    pub command_manager: ActorRef<CommandManagerMsg>,
}

impl SagaRunner {
    pub fn new(saga_name: &'static str) -> Self {
        Self {
            saga_name,
            stopped: false,
        }
    }

    pub fn spawn(args: SagaRunnerArgs) -> ActorHandle<SagaRunnerMsg> {
        let saga_name = args.saga.name();
        trace!("Starting saga runner: {}", saga_name);

        let actor = Self::new(saga_name);
        let handle = crate::runtime::spawn::spawn(actor);

        // Spawn task to process the saga pipeline
        let event_bus = args.ctx.event_bus.clone();
        let saga = args.saga.clone();
        let ctx = args.ctx.clone();
        let command_manager = args.command_manager.clone();
        let host_id = args.ctx.server_ctx.host_id;
        let tokio_handle = args.ctx.server_ctx.tokio_handle.clone();

        tokio_handle.spawn(async move {
            // Subscribe to EventBus and convert directly to a stream
            let event_stream = event_bus.subscribe().into_stream();

            // Build the saga's event processing pipeline
            let command_stream = saga.build_boxed(Box::pin(event_stream), ctx);

            // Process the entire pipeline
            futures::pin_mut!(command_stream);

            while let Some(command) = command_stream.next().await {
                trace!("Saga {} emitting command: {}", saga_name, command.command_id);

                // Create a RequestContext for saga-originated commands
                let req = RequestContext::internal(
                    Arc::from(Uuid::new_v4().to_string()),
                    host_id,
                    &format!("saga-{}", saga_name),
                );

                // Execute the command synchronously via spawn_blocking
                let cmd_mgr = command_manager.clone();
                match tokio::task::spawn_blocking(move || {
                    cmd_mgr.call(|r| CommandManagerMsg::Execute(command, req, r))
                })
                .await
                {
                    Ok(Ok(Ok(_value))) => {
                        trace!("Saga {} command executed successfully", saga_name);
                    }
                    Ok(Ok(Err(cmd_err))) => {
                        error!(
                            "Saga {} command failed: {}",
                            saga_name, cmd_err.message
                        );
                    }
                    Ok(Err(e)) => {
                        error!(
                            "Saga {} failed to call CommandManager: {}",
                            saga_name, e
                        );
                    }
                    Err(e) => {
                        error!(
                            "Saga {} spawn_blocking failed: {}",
                            saga_name, e
                        );
                    }
                }
            }

            debug!("Saga {} pipeline ended", saga_name);
        });

        handle
    }
}

impl Actor for SagaRunner {
    type Msg = SagaRunnerMsg;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            SagaRunnerMsg::Stop => {
                trace!("Stopping saga runner: {}", self.saga_name);
                self.stopped = true;
                // The pipeline thread will stop when the event bus closes
            }
        }
    }

    fn on_shutdown(&mut self) {
        trace!("Saga runner stopped: {}", self.saga_name);
    }
}
