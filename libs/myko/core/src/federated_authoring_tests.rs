use std::sync::Arc;

use myko_federation::{AllowAllAccessPolicy, Node, NodeId};

use crate::{ApplicationHost, item::Eventable, prelude::*};

mod macro_hygiene {
    use crate::myko_item;

    // This module intentionally does not import the command capability traits.
    // Generated handlers must bring their own extension methods into scope, and
    // disabling arbitrary field filters must leave opaque fields unconstrained.
    #[myko_item(filters = false, deletes = false)]
    pub struct OpaqueRecord {
        external_id: uuid::Uuid,
    }

    #[myko_item(scope_root, deletes = false)]
    pub struct Catalog {
        label: String,
    }

    #[myko_item(
        service = Catalog,
        scoped_by = crate::federated_authoring_tests::macro_hygiene::Catalog,
        deletes = false
    )]
    pub struct CatalogEntry {
        label: String,
    }

    #[test]
    fn generated_ids_keep_the_retained_constructor() {
        assert_eq!(OpaqueRecordId::new("opaque").as_ref(), "opaque");
    }

    #[test]
    fn scoped_items_preserve_the_qualified_parent_path() {
        let item = CatalogEntry {
            id: CatalogEntryId::new("entry"),
            catalog_id: CatalogId::new("catalog"),
            label: "entry".to_owned(),
        };
        assert_eq!(item.catalog_id.as_ref(), "catalog");
    }
}

#[myko_service(Project, Task)]
pub struct Planning;

#[myko_item(service = Planning, scope_root)]
pub struct Project {
    title: String,
}

#[myko_item(service = Planning, scoped_by = Project)]
pub struct Task {
    title: String,
}

#[myko_command(bool, item = Task)]
pub struct CompleteTask {
    task_id: TaskId,
}

impl CommandHandler for CompleteTask {
    fn scope(&self, _node_id: NodeId) -> ProjectId {
        ProjectId::from("project")
    }

    fn execute(self, _ctx: CommandContext) -> Result<bool, crate::command::CommandError> {
        Ok(true)
    }
}

#[myko_query(Task, item = Task)]
#[derive(PartialEq, Eq)]
pub struct TasksNamed {
    title: String,
}

impl QueryHandler for TasksNamed {
    fn test_entity(ctx: QueryTestContext<Self>) -> bool {
        ctx.item.title == ctx.query.title
    }
}

#[test]
fn retained_items_carry_service_and_scope_contracts() {
    let project_id = ProjectId::from("project");
    let task = Task {
        id: TaskId::from("task"),
        project_id: project_id.clone(),
        title: "Ship".to_owned(),
    };

    assert_eq!(Planning::SERVICE_ID, Project::SERVICE_ID);
    assert_eq!(Project::SCOPE, ItemScope::Root);
    assert!(matches!(Task::SCOPE, ItemScope::ScopedBy { .. }));
    assert_eq!(MykoItem::scope_id(&task), &project_id);
    assert_eq!(task.parent_id(), &project_id);
    assert_eq!(Task::ENTITY_NAME_STATIC, "Task");
}

#[test]
fn application_activation_filters_the_retained_inventory() {
    let application = MykoApplication::builder().service::<Planning>().build();

    assert!(application.handlers().query("GetAllProjects").is_some());
    assert!(application.handlers().query("GetAllTasks").is_some());
    assert!(application.handlers().query("GetProjectsByQuery").is_some());
    assert!(application.handlers().query("TasksNamed").is_some());
    assert!(application.handlers().report("CountProjects").is_some());
    assert!(application.handlers().report("GetProjectById").is_some());
    assert!(application.handlers().query("GetAllServers").is_none());
    assert!(
        application
            .handlers()
            .command_ids()
            .any(|command| command == "CompleteTask")
    );
}

#[test]
fn retained_commands_carry_typed_service_and_scope_contracts() {
    fn require_typed_command<C>()
    where
        C: MykoCommandContract<Output = bool, Service = Planning, Scope = Project>,
    {
    }

    require_typed_command::<CompleteTask>();
    assert_eq!(CompleteTask::SERVICE_ID, Planning::SERVICE_ID);
    assert_eq!(CompleteTask::ITEM_TYPE, Some(Task::ITEM_TYPE));
}

#[test]
fn item_owned_macro_commands_keep_the_durable_executor() -> Result<(), String> {
    let application = MykoApplication::builder().service::<Planning>().build();
    let host = ApplicationHost::new(Node::in_memory(), application)?
        .with_access_policy(Arc::new(AllowAllAccessPolicy))
        .map_err(|error| error.to_string())?;

    let completed = host
        .exec_command(CompleteTask {
            task_id: TaskId::from("task"),
        })
        .map_err(|error| error.to_string())?;

    if !completed {
        return Err("durable command did not return its typed completion".to_owned());
    }
    Ok(())
}
