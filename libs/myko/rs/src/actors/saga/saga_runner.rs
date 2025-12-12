//! Saga runner actor that processes events through a saga's pipeline.
//!
//! Each saga gets its own SagaRunner actor that:
//! - Subscribes to the EventBus for high-throughput event reception
//! - Processes events through the saga's stream pipeline
//! - Forwards resulting commands to CommandManager

use std::sync::Arc;

use futures::StreamExt;
use log::{debug, error, trace};
use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::{
    actors::command::command_manager::CommandManagerMsg,
    saga::{AnySaga, SagaContext},
};

/// Actor that runs a single saga's event processing pipeline.
pub struct SagaRunner;

/// Messages for SagaRunner actor.
pub enum SagaRunnerMsg {
    /// Stop the saga runner (events come via EventBus subscription, not messages)
    Stop,
}

/// State for SagaRunner actor.
pub struct SagaRunnerState {
    saga_name: &'static str,
    // Task handle for the combined event+command pipeline
    _pipeline_task_handle: tokio::task::JoinHandle<()>,
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
        trace!("Starting saga runner: {}", saga_name);

        // Subscribe to EventBus and convert directly to a stream (no intermediate channel)
        let event_stream = args.ctx.event_bus.subscribe().into_stream();

        // Build the saga's event processing pipeline directly from the EventBus stream
        let command_stream = args.saga.build_boxed(Box::pin(event_stream), args.ctx.clone());

        // Spawn a single task to process the entire pipeline
        let command_manager = args.command_manager.clone();
        let saga_name_for_task = saga_name;
        let pipeline_task_handle = tokio::spawn(async move {
            futures::pin_mut!(command_stream);

            while let Some(command) = command_stream.next().await {
                trace!("Saga {} emitting command: {}", saga_name_for_task, command.command_id);

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
                        trace!("Saga {} command executed successfully", saga_name_for_task);
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

            debug!("Saga {} pipeline ended", saga_name_for_task);
        });

        Ok(SagaRunnerState {
            saga_name,
            _pipeline_task_handle: pipeline_task_handle,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SagaRunnerMsg::Stop => {
                trace!("Stopping saga runner: {}", state.saga_name);
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
        trace!("Saga runner stopped: {}", state.saga_name);
        // Task will be aborted when the sender is dropped
        Ok(())
    }
}
