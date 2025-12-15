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

use crate::{
    actors::command::command_manager::CommandManagerMsg,
    runtime::{Actor, ActorHandle, ActorRef},
    saga::{SagaContext, SagaRegistration},
};

use super::saga_runner::{SagaRunner, SagaRunnerArgs, SagaRunnerMsg};

/// Actor that manages all saga runners.
pub struct SagaManager {
    ctx: Arc<SagaContext>,
    command_manager: ActorRef<CommandManagerMsg>,
    saga_runners: Vec<ActorRef<SagaRunnerMsg>>,
    started: bool,
}

/// Messages for SagaManager actor.
#[derive(Debug)]
pub enum SagaManagerMsg {
    /// Start all registered sagas (called after AllInitComplete)
    StartAll,
    /// Stop all sagas
    StopAll,
}

/// Arguments for spawning SagaManager.
pub struct SagaManagerArgs {
    pub ctx: Arc<SagaContext>,
    pub command_manager: ActorRef<CommandManagerMsg>,
}

impl SagaManager {
    pub fn new(args: SagaManagerArgs) -> Self {
        Self {
            ctx: args.ctx,
            command_manager: args.command_manager,
            saga_runners: Vec::new(),
            started: false,
        }
    }

    pub fn spawn(args: SagaManagerArgs) -> ActorHandle<SagaManagerMsg> {
        let actor = Self::new(args);
        crate::runtime::spawn::spawn(actor)
    }
}

impl Actor for SagaManager {
    type Msg = SagaManagerMsg;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            SagaManagerMsg::StartAll => {
                if self.started {
                    warn!("SagaManager already started, ignoring StartAll");
                    return;
                }

                // Discover and spawn all registered sagas
                let registrations: Vec<_> = inventory::iter::<SagaRegistration>.into_iter().collect();

                for registration in registrations {
                    let saga = (registration.create)();
                    let saga_name = saga.name();

                    trace!("Spawning saga runner for: {}", saga_name);

                    let handle = SagaRunner::spawn(SagaRunnerArgs {
                        saga,
                        ctx: self.ctx.clone(),
                        command_manager: self.command_manager.clone(),
                    });

                    self.saga_runners.push(handle.actor_ref());
                }

                self.started = true;
                debug!("SagaManager: {} sagas", self.saga_runners.len());
            }

            SagaManagerMsg::StopAll => {
                trace!("Stopping all saga runners");

                for runner in &self.saga_runners {
                    if let Err(e) = runner.send_message(SagaRunnerMsg::Stop) {
                        error!("Failed to stop saga runner: {}", e);
                    }
                }

                self.saga_runners.clear();
                self.started = false;
            }
        }
    }

    fn on_shutdown(&mut self) {
        trace!("SagaManager stopping, cleaning up {} saga runners", self.saga_runners.len());

        // Stop all saga runners
        for runner in &self.saga_runners {
            let _ = runner.send_message(SagaRunnerMsg::Stop);
        }
    }
}
