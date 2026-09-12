use std::{error::Error, sync::Arc};

use myko::{
    ApplicationHost, CommandContext, CommandError, CommandHandler, MykoApplication,
    command::CommandHandlerRegistration,
};
use myko_federation::{AllowAllAccessPolicy, CommandSubmission, Node, NodeId, PrincipalId};
use myko_items::{
    MykoCommandContract,
    schema::{HandlerPayloadSchema, HandlerResultSchema, TypeSchema},
};
use serde_json::{Value, json};

use super::{FacadeRecord, FacadeRecordId, FacadeService, FacadeValue, TestResult};

#[myko::myko_command(FacadeValue, service = FacadeService, scope = FacadeRecord)]
struct EchoValue {
    record_id: FacadeRecordId,
    #[serde(rename = "payload")]
    value: FacadeValue,
    #[serde(default)]
    hint: Option<String>,
}

impl CommandHandler for EchoValue {
    fn scope(&self, _node_id: NodeId) -> FacadeRecordId {
        self.record_id.clone()
    }

    fn execute(self, _context: CommandContext) -> Result<FacadeValue, CommandError> {
        Ok(self.value)
    }
}

#[myko_items::myko_command((), item = FacadeRecord)]
struct PingRecord {
    record_id: FacadeRecordId,
}

impl CommandHandler for PingRecord {
    fn scope(&self, _node_id: NodeId) -> FacadeRecordId {
        self.record_id.clone()
    }

    fn execute(self, _context: CommandContext) -> Result<(), CommandError> {
        Ok(())
    }
}

fn registration<C: MykoCommandContract>(
    application: &MykoApplication,
) -> Result<&'static CommandHandlerRegistration, Box<dyn Error>> {
    application
        .handlers()
        .commands()
        .find(|registration| {
            registration.service_id == Some(C::SERVICE_ID)
                && registration.command_id == C::OPERATION_ID
        })
        .ok_or_else(|| "application did not retain its typed command registration".into())
}

fn contract(
    registration: &CommandHandlerRegistration,
) -> Result<HandlerPayloadSchema, Box<dyn Error>> {
    Ok(registration
        .payload_schema
        .ok_or("command omitted its payload schema")?())
}

#[test]
fn both_command_macros_register_inputs_and_scalar_outputs() -> TestResult {
    let app = MykoApplication::builder()
        .service::<FacadeService>()
        .build();
    let echo = contract(registration::<EchoValue>(&app)?)?;
    if echo.arguments != TypeSchema::of::<EchoValue>()
        || echo.result != HandlerResultSchema::Value(TypeSchema::of::<FacadeValue>())
    {
        return Err("runtime command schema does not describe its typed payloads".into());
    }
    let ping = contract(registration::<PingRecord>(&app)?)?;
    if ping.arguments != TypeSchema::of::<PingRecord>()
        || ping.result != HandlerResultSchema::Value(TypeSchema::of::<()>())
    {
        return Err("item command schema lost its arguments or unit result".into());
    }
    Ok(())
}

#[test]
fn command_schema_matches_durable_admission_renames_defaults_and_invalid_types() -> TestResult {
    let app = MykoApplication::builder()
        .service::<FacadeService>()
        .build();
    let handler = registration::<EchoValue>(&app)?;
    let schema = contract(handler)?;
    let input =
        jsonschema::validator_for(&serde_json::to_value(schema.arguments.deserialization)?)?;
    let executor = handler
        .durable_factory
        .ok_or("missing durable command executor")?();
    let command = EchoValue::new(EchoValueArgs {
        record_id: FacadeRecordId::from("a"),
        value: FacadeValue::Count(7),
        hint: None,
    });
    let node_id = NodeId::new();
    for (value, valid) in [
        (json!({"recordId": "a", "payload": {"Count": 7}}), true),
        (json!({"recordId": "a", "value": {"Count": 7}}), false),
        (json!({"recordId": 1, "payload": {"Count": 7}}), false),
        (json!({"recordId": "a", "payload": {"Count": "7"}}), false),
    ] {
        let mut submission = CommandSubmission::for_command(&command)?;
        submission.payload = serde_json::to_vec(&value)?;
        if input.is_valid(&value) != valid
            || executor
                .authenticate(node_id, PrincipalId::for_node(node_id), submission)
                .is_ok()
                != valid
        {
            return Err(format!("schema and durable decoder disagree about {value}").into());
        }
    }
    Ok(())
}

#[test]
fn registered_result_schemas_validate_executed_command_results() -> TestResult {
    let app = MykoApplication::builder()
        .service::<FacadeService>()
        .build();
    let echo = contract(registration::<EchoValue>(&app)?)?;
    let ping = contract(registration::<PingRecord>(&app)?)?;
    let host = ApplicationHost::new(Node::in_memory(), app)?
        .with_access_policy(Arc::new(AllowAllAccessPolicy))?;
    let result = host.exec_command(EchoValue {
        record_id: FacadeRecordId::from("a"),
        value: FacadeValue::Count(7),
        hint: None,
    })?;
    if result != FacadeValue::Count(7) {
        return Err("registered command did not execute its typed handler".into());
    }
    host.exec_command(PingRecord {
        record_id: FacadeRecordId::from("a"),
    })?;
    for (contract, result) in [(echo, serde_json::to_value(result)?), (ping, Value::Null)] {
        let HandlerResultSchema::Value(schema) = contract.result else {
            return Err("command result was incorrectly registered as rows".into());
        };
        for schema in [schema.serialization, schema.deserialization] {
            if !jsonschema::validator_for(&serde_json::to_value(schema)?)?.is_valid(&result) {
                return Err("registered schema rejected the actual durable result".into());
            }
        }
    }
    Ok(())
}

#[test]
fn inactive_commands_are_absent_from_both_metadata_and_execution() -> TestResult {
    let app = MykoApplication::builder().build();
    if registration::<EchoValue>(&app).is_ok() || registration::<PingRecord>(&app).is_ok() {
        return Err("inactive command contributed metadata".into());
    }
    let global = app
        .handlers()
        .commands()
        .find(|registration| registration.service_id.is_none())
        .ok_or("expected a legacy global registration")?;
    if global.payload_schema.is_some() || global.durable_factory.is_some() {
        return Err("legacy global command gained a durable service contract".into());
    }
    let node = Node::in_memory();
    let host = ApplicationHost::new(node.clone(), app)?
        .with_access_policy(Arc::new(AllowAllAccessPolicy))?;
    let command = EchoValue {
        record_id: FacadeRecordId::from("a"),
        value: FacadeValue::Count(7),
        hint: None,
    };
    if host.handles_submission(&CommandSubmission::for_command(&command)?)
        || !matches!(
            host.exec_command(command),
            Err(myko::AppError::UnregisteredHandler {
                kind: "command",
                ..
            })
        )
        || !node.events_after(None)?.is_empty()
    {
        return Err("inactive command executed or changed retained history".into());
    }
    Ok(())
}

#[test]
fn every_activated_native_durable_command_has_resolvable_payload_schemas() -> TestResult {
    let app = MykoApplication::builder()
        .service::<FacadeService>()
        .service::<myko_node::FederationService>()
        .service::<myko_authority::AuthorityService>()
        .build();
    let mut checked = 0;
    for handler in app
        .handlers()
        .commands()
        .filter(|handler| handler.durable_factory.is_some())
    {
        if handler.service_id.is_none() {
            return Err("durable command has no service owner".into());
        }
        let contract = contract(handler)?;
        let HandlerResultSchema::Value(result) = contract.result else {
            return Err("command contract registered rows instead of one result".into());
        };
        for ty in [contract.arguments, result] {
            jsonschema::validator_for(&serde_json::to_value(ty.serialization)?)?;
            jsonschema::validator_for(&serde_json::to_value(ty.deserialization)?)?;
        }
        checked += 1;
    }
    if checked < 2 {
        return Err("no durable command contracts were examined".into());
    }
    Ok(())
}
