//! Saga runner actor that processes events through a saga's pipeline.
//!
//! Each saga gets its own SagaRunner actor that:
//! - Receives events from SagaManager
//! - Processes them through the saga's stream pipeline
//! - Forwards resulting commands to CommandManager

use std::sync::Arc;

use futures::StreamExt;
use log::{debug, error, info};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::mpsc;

use crate::{
    actors::command::command_manager::CommandManagerMsg,
    event::MEvent,
    saga::{AnySaga, SagaContext},
};

/// Actor that runs a single saga's event processing pipeline.
pub struct SagaRunner;

/// Messages for SagaRunner actor.
pub enum SagaRunnerMsg {
    /// Process an event through the saga pipeline
    Event(MEvent),
    /// Stop the saga runner
    Stop,
}

/// State for SagaRunner actor.
pub struct SagaRunnerState {
    saga_name: &'static str,
    event_tx: mpsc::UnboundedSender<MEvent>,
    // The stream processing task handle
    _task_handle: tokio::task::JoinHandle<()>,
}

/// Arguments for spawning a SagaRunner.
pub struct SagaRunnerArgs {
    pub saga: Arc<dyn AnySaga>,
    pub ctx: Arc<SagaContext>,
    pub command_manager: ActorRef<CommandManagerMsg>,
}

impl Actor for SagaRunner {
    type Msg = SagaRunnerMsg;
    type State = SagaRunnerState;
    type Arguments = SagaRunnerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let saga_name = args.saga.name();
        info!("Starting saga runner: {}", saga_name);

        // Create a channel for events
        let (event_tx, event_rx) = mpsc::unbounded_channel::<MEvent>();

        // Convert receiver to a stream
        let event_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(event_rx);

        // Build the saga's event processing pipeline
        let command_stream = args.saga.build_boxed(Box::pin(event_stream), args.ctx.clone());

        // Spawn a task to process commands from the pipeline
        let command_manager = args.command_manager.clone();
        let saga_name_for_task = saga_name;
        let task_handle = tokio::spawn(async move {
            futures::pin_mut!(command_stream);

            while let Some(command) = command_stream.next().await {
                debug!("Saga {} emitting command: {}", saga_name_for_task, command.command_id);

                // Create a synthetic client ID for saga-originated commands
                let client_id: Arc<str> = format!("saga-{}", saga_name_for_task).into();

                // Use ractor::call! to execute the command and wait for response
                match ractor::call!(
                    command_manager,
                    CommandManagerMsg::Execute,
                    command,
                    client_id
                ) {
                    Ok(Ok(_value)) => {
                        debug!("Saga {} command executed successfully", saga_name_for_task);
                    }
                    Ok(Err(cmd_err)) => {
                        error!(
                            "Saga {} command failed: {}",
                            saga_name_for_task, cmd_err.message
                        );
                    }
                    Err(e) => {
                        error!(
                            "Saga {} failed to call CommandManager: {}",
                            saga_name_for_task, e
                        );
                    }
                }
            }

            info!("Saga {} command stream ended", saga_name_for_task);
        });

        Ok(SagaRunnerState {
            saga_name,
            event_tx,
            _task_handle: task_handle,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SagaRunnerMsg::Event(event) => {
                // Forward event to the saga's processing stream
                if let Err(e) = state.event_tx.send(event) {
                    error!("Saga {} failed to forward event: {}", state.saga_name, e);
                }
                Ok(())
            }
            SagaRunnerMsg::Stop => {
                info!("Stopping saga runner: {}", state.saga_name);
                myself.stop(Some("Stop requested".to_string()));
                Ok(())
            }
        }
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("Saga runner stopped: {}", state.saga_name);
        // Task will be aborted when the sender is dropped
        Ok(())
    }
}
