//! Saga runner actor that processes events through a saga's pipeline.
//!
//! Each saga gets its own SagaRunner actor that:
//! - Subscribes to the EventBus for high-throughput event reception
//! - Processes events through the saga's stream pipeline
//! - Forwards resulting commands to CommandManager

use std::sync::Arc;

use log::trace;

use crate::{
    actors::command::command_manager::CommandManagerMsg,
    runtime::{Actor, ActorRef},
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
    pub fn create(saga_name: &'static str) -> Self {
        Self {
            saga_name,
            stopped: false,
        }
    }
}

impl Actor for SagaRunner {
    type Msg = SagaRunnerMsg;
    type Args = SagaRunnerArgs;

    fn new(args: Self::Args, _myself: ActorRef<Self::Msg>) -> Self {
        Self::create(args.saga.name())
    }

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
