use myko::application::ServiceHandlerKind;
use myko::{ApplicationHost, CommandContext, CommandError, CommandHandler, MykoApplication};
use myko_federation::{AllowAllAccessPolicy, Node, NodeId};
use myko_items::{
    MykoService,
    schema::{HandlerResultSchema, TypeSchema},
};
use std::sync::Arc;

use super::TestResult;

mod left {
    use super::*;

    #[myko_items::myko_service(Root)]
    pub struct Service;
    #[myko_items::myko_item(service = Service, scope_root)]
    pub struct Root {}

    #[myko_items::myko_command(String, item = Root)]
    pub struct Echo {
        pub value: String,
    }
    impl CommandHandler for Echo {
        fn scope(&self, _node_id: NodeId) -> RootId {
            RootId::from("root")
        }
        fn execute(self, _context: CommandContext) -> Result<String, CommandError> {
            Ok(self.value)
        }
    }
}

mod right {
    use super::*;

    #[myko_items::myko_service(Root)]
    pub struct Service;
    #[myko_items::myko_item(service = Service, scope_root)]
    pub struct Root {}

    #[myko_items::myko_command(u16, item = Root)]
    pub struct Echo {
        pub value: u16,
    }
    impl CommandHandler for Echo {
        fn scope(&self, _node_id: NodeId) -> RootId {
            RootId::from("root")
        }
        fn execute(self, _context: CommandContext) -> Result<u16, CommandError> {
            Ok(self.value)
        }
    }
}

#[test]
fn same_named_commands_keep_distinct_service_contracts_and_executors() -> TestResult {
    let app = MykoApplication::builder()
        .service::<left::Service>()
        .service::<right::Service>()
        .build();
    for (service, expected) in [
        (left::Service::SERVICE_ID, TypeSchema::of::<String>()),
        (right::Service::SERVICE_ID, TypeSchema::of::<u16>()),
    ] {
        let registration = app
            .handlers()
            .commands()
            .find(|handler| handler.command_id == "Echo" && handler.service_id == Some(service))
            .ok_or("missing service-qualified command")?;
        if registration
            .payload_schema
            .ok_or("command lost its schema")?()
        .result
            != HandlerResultSchema::Value(expected)
        {
            return Err("one service's command schema shadowed another's".into());
        }
        let contract = app.service_contract(service)?;
        let command = contract
            .handlers
            .get(&(ServiceHandlerKind::Command, "Echo".into()))
            .ok_or("service contract omitted its command")?;
        if command
            != &registration
                .payload_schema
                .ok_or("missing command schema")?()
        {
            return Err("service contract disagrees with its command executor".into());
        }
    }
    let host = ApplicationHost::new(Node::in_memory(), app)?
        .with_access_policy(Arc::new(AllowAllAccessPolicy))?;
    if host.exec_command(left::Echo {
        value: "left".to_owned(),
    })? != "left"
        || host.exec_command(right::Echo { value: 19 })? != 19
    {
        return Err("one service's executor shadowed another's".into());
    }
    Ok(())
}
