use std::{
    collections::HashMap,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

use log::{debug, error, trace};
use serde_json::Value;

use crate::{
    actors::{
        event::event_manager::EventManagerMsg,
        query::query_manager::QueryManagerMsg,
        report::report_manager::ReportManagerMsg,
    },
    command::{
        CommandContext, CommandError, CommandHandler, CommandHandlerRegistration, WrappedCommand,
    },
    context::RequestContext,
    runtime::{Actor, ActorRef, RpcReplyPort},
    server::MykoServerCtx,
};

pub struct CommandManagerArgs {
    pub ctx: Arc<MykoServerCtx>,
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub report_manager: ActorRef<ReportManagerMsg>,
}

pub struct CommandManager {
    ctx: Arc<MykoServerCtx>,
    event_manager: ActorRef<EventManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    /// Registered command handlers by command_id
    handlers: HashMap<&'static str, Box<dyn CommandHandler>>,
    /// Self-reference for nested command execution
    myself: ActorRef<CommandManagerMsg>,
}

pub enum CommandManagerMsg {
    /// Execute a command from a client.
    /// The RequestContext is created by the caller (MessageHandler) from the WebSocket message.
    Execute(
        WrappedCommand,
        RequestContext,
        RpcReplyPort<Result<Value, CommandError>>,
    ),
    /// Execute a nested command (from within a handler).
    /// The RequestContext has extended lineage from the parent command.
    ExecuteNested(
        WrappedCommand,
        RequestContext,
        RpcReplyPort<Result<Value, CommandError>>,
    ),
}

impl std::fmt::Debug for CommandManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandManagerMsg::Execute(cmd, req, _) => {
                write!(f, "Execute({}, tx={})", cmd.command_id, req.tx())
            }
            CommandManagerMsg::ExecuteNested(cmd, req, _) => {
                write!(f, "ExecuteNested({}, tx={})", cmd.command_id, req.tx())
            }
        }
    }
}

impl CommandManager {

    fn execute_command(
        &self,
        command: &WrappedCommand,
        req: RequestContext,
    ) -> Result<Value, CommandError> {
        let command_id = command.command_id.as_str();

        // Look up handler
        let handler = self.handlers.get(command_id).ok_or_else(|| {
            error!(
                "No handler registered for command {}: {:?}",
                command_id,
                self.handlers.keys().collect::<Vec<_>>()
            );
            CommandError {
                tx: req.tx().to_string(),
                message: format!("No handler registered for command: {}", command_id),
            }
        })?;

        // Build context from RequestContext
        let ctx = CommandContext::new(
            req,
            self.ctx.clone(),
            self.event_manager.clone(),
            self.myself.clone(),
            self.query_manager.clone(),
            self.report_manager.clone(),
        );

        // Execute handler using shared tokio runtime
        self.ctx
            .tokio_handle
            .block_on(handler.execute(command.command.clone(), ctx))
    }

    /// Execute a command with panic catching.
    /// Converts panics to CommandError to prevent actor crashes.
    fn execute_command_safe(
        &self,
        command: &WrappedCommand,
        req: RequestContext,
        tx: &str,
    ) -> Result<Value, CommandError> {
        let command_id = command.command_id.clone();

        // Wrap in catch_unwind to prevent panics from crashing the actor
        let result = catch_unwind(AssertUnwindSafe(|| self.execute_command(command, req)));

        match result {
            Ok(inner_result) => inner_result,
            Err(panic_payload) => {
                // Extract panic message
                let panic_msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };

                error!(
                    "Command handler {} panicked: {} (tx={})",
                    command_id, panic_msg, tx
                );

                Err(CommandError {
                    tx: tx.to_string(),
                    message: format!("Command handler panicked: {}", panic_msg),
                })
            }
        }
    }
}

impl Actor for CommandManager {
    type Msg = CommandManagerMsg;
    type Args = CommandManagerArgs;

    fn new(args: Self::Args, myself: ActorRef<Self::Msg>) -> Self {
        // Collect all registered handlers from inventory
        let mut handlers: HashMap<&'static str, Box<dyn CommandHandler>> = HashMap::new();
        for registration in inventory::iter::<CommandHandlerRegistration> {
            trace!("Registering command handler: {}", registration.command_id);
            let handler = (registration.factory)();
            handlers.insert(registration.command_id, handler);
        }

        debug!("CommandManager: {} handlers", handlers.len());

        Self {
            ctx: args.ctx,
            event_manager: args.event_manager,
            query_manager: args.query_manager,
            report_manager: args.report_manager,
            handlers,
            myself,
        }
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            CommandManagerMsg::Execute(command, req, reply) => {
                let command_id = command.command_id.as_str();
                let tx = req.tx().to_string();
                trace!(
                    "Executing command {} with tx {} (lineage: {})",
                    command_id,
                    tx,
                    req.lineage_string()
                );

                let result = self.execute_command_safe(&command, req, &tx);
                let _ = reply.send(result);
            }
            CommandManagerMsg::ExecuteNested(command, req, reply) => {
                let command_id = command.command_id.as_str();
                let tx = req.tx().to_string();
                trace!(
                    "Executing nested command {} (lineage: {})",
                    command_id,
                    req.lineage_string()
                );

                let result = self.execute_command_safe(&command, req, &tx);
                let _ = reply.send(result);
            }
        }
    }
}
