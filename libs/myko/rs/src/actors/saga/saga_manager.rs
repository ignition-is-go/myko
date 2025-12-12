//! Saga manager actor that spawns and coordinates saga runners.
//!
//! The SagaManager:
//! - Discovers registered sagas via inventory
//! - Spawns a SagaRunner for each saga (runners subscribe to EventBus directly)
//! - Manages saga lifecycle (start/stop)
//!
//! Note: Event broadcasting is handled by EventBus for high throughput.

use std::sync::Arc;

use log::{debug, error, trace, warn};
use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::{
    actors::command::command_manager::CommandManagerMsg,
    saga::{SagaContext, SagaRegistration},
};

use super::saga_runner::{SagaRunner, SagaRunnerArgs, SagaRunnerMsg};

/// Actor that manages all saga runners.
pub struct SagaManager;

/// Messages for SagaManager actor.
/// Note: Events are broadcast via EventBus, not through this actor.
pub enum SagaManagerMsg {
    /// Start all registered sagas (called after AllInitComplete)
    StartAll,
    /// Stop all sagas
    StopAll,
}

/// State for SagaManager actor.
pub struct SagaManagerState {
    ctx: Arc<SagaContext>,
    command_manager: ActorRef<CommandManagerMsg>,
    saga_runners: Vec<ActorRef<SagaRunnerMsg>>,
    started: bool,
}

/// Arguments for spawning SagaManager.
pub struct SagaManagerArgs {
    pub ctx: Arc<SagaContext>,
    pub command_manager: ActorRef<CommandManagerMsg>,
}

impl Actor for SagaManager {
    type Msg = SagaManagerMsg;
    type State = SagaManagerState;
    type Arguments = SagaManagerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(SagaManagerState {
            ctx: args.ctx,
            command_manager: args.command_manager,
            saga_runners: Vec::new(),
            started: false,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SagaManagerMsg::StartAll => {
                if state.started {
                    warn!("SagaManager already started, ignoring StartAll");
                    return Ok(());
                }

                // Discover and spawn all registered sagas
                let registrations: Vec<_> = inventory::iter::<SagaRegistration>.into_iter().collect();

                for registration in registrations {
                    let saga = (registration.create)();
                    let saga_name = saga.name();

                    trace!("Spawning saga runner for: {}", saga_name);

                    match Actor::spawn(
                        Some(format!("saga-{}", saga_name)),
                        SagaRunner,
                        SagaRunnerArgs {
                            saga,
                            ctx: state.ctx.clone(),
                            command_manager: state.command_manager.clone(),
                        },
                    )
                    .await
                    {
                        Ok((actor_ref, _handle)) => {
                            state.saga_runners.push(actor_ref);
                        }
                        Err(e) => {
                            error!("Failed to spawn saga runner for {}: {}", saga_name, e);
                        }
                    }
                }

                state.started = true;
                debug!("SagaManager: {} sagas", state.saga_runners.len());

                Ok(())
            }

            SagaManagerMsg::StopAll => {
                trace!("Stopping all saga runners");

                for runner in &state.saga_runners {
                    if let Err(e) = runner.send_message(SagaRunnerMsg::Stop) {
                        error!("Failed to stop saga runner: {}", e);
                    }
                }

                state.saga_runners.clear();
                state.started = false;

                Ok(())
            }
        }
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        trace!("SagaManager stopping, cleaning up {} saga runners", state.saga_runners.len());

        // Stop all saga runners
        for runner in &state.saga_runners {
            let _ = runner.send_message(SagaRunnerMsg::Stop);
        }

        Ok(())
    }
}
