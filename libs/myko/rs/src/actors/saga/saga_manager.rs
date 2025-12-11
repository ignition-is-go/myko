//! Saga manager actor that spawns and coordinates saga runners.
//!
//! The SagaManager:
//! - Discovers registered sagas via inventory
//! - Spawns a SagaRunner for each saga
//! - Broadcasts events to all saga runners
//! - Manages saga lifecycle

use std::sync::Arc;

use log::{debug, error, info, warn};
use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::{
    actors::command::command_manager::CommandManagerMsg,
    event::MEvent,
    saga::{SagaContext, SagaRegistration},
};

use super::saga_runner::{SagaRunner, SagaRunnerArgs, SagaRunnerMsg};

/// Actor that manages all saga runners.
pub struct SagaManager;

/// Messages for SagaManager actor.
pub enum SagaManagerMsg {
    /// Start all registered sagas (called after AllInitComplete)
    StartAll,
    /// Stop all sagas
    StopAll,
    /// Broadcast an event to all saga runners
    BroadcastEvent(MEvent),
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
        info!("SagaManager starting");

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

                info!("Starting all registered sagas");

                // Discover and spawn all registered sagas
                let registrations: Vec<_> = inventory::iter::<SagaRegistration>.into_iter().collect();
                info!("Found {} registered sagas", registrations.len());

                for registration in registrations {
                    let saga = (registration.create)();
                    let saga_name = saga.name();

                    info!("Spawning saga runner for: {}", saga_name);

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
                            info!("Started saga runner: {}", saga_name);
                        }
                        Err(e) => {
                            error!("Failed to spawn saga runner for {}: {}", saga_name, e);
                        }
                    }
                }

                state.started = true;
                info!(
                    "Started {} saga runners",
                    state.saga_runners.len()
                );

                Ok(())
            }

            SagaManagerMsg::StopAll => {
                info!("Stopping all saga runners");

                for runner in &state.saga_runners {
                    if let Err(e) = runner.send_message(SagaRunnerMsg::Stop) {
                        error!("Failed to stop saga runner: {}", e);
                    }
                }

                state.saga_runners.clear();
                state.started = false;

                Ok(())
            }

            SagaManagerMsg::BroadcastEvent(event) => {
                if !state.started {
                    // Sagas not started yet, ignore events
                    return Ok(());
                }

                debug!(
                    "Broadcasting event to {} saga runners: {} {:?}",
                    state.saga_runners.len(),
                    event.item_type(),
                    event.change_type()
                );

                // Broadcast event to all saga runners
                for runner in &state.saga_runners {
                    if let Err(e) = runner.send_message(SagaRunnerMsg::Event(event.clone())) {
                        error!("Failed to send event to saga runner: {}", e);
                    }
                }

                Ok(())
            }
        }
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("SagaManager stopping, cleaning up {} saga runners", state.saga_runners.len());

        // Stop all saga runners
        for runner in &state.saga_runners {
            let _ = runner.send_message(SagaRunnerMsg::Stop);
        }

        Ok(())
    }
}
