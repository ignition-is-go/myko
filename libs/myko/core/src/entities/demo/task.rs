use super::status::{DemoStatus, DemoStatusId, GetDemoStatusById};
use crate::prelude::*;

/// A small canonical entity used by client integration demos.
#[myko_item]
#[derive(Eq)]
pub struct DemoTask {
    pub title: String,
    pub completed: bool,
    #[belongs_to(DemoStatus)]
    pub status_id: DemoStatusId,
}

/// Returns every demo task.
#[myko_query(DemoTask)]
pub struct GetDemoTasks {}

impl QueryHandler for GetDemoTasks {
    fn test_entity(_ctx: QueryTestContext<Self>) -> bool {
        true
    }
}

/// Creates a demo task with an application-supplied stable ID.
#[myko_command(bool)]
pub struct CreateDemoTask {
    pub id: DemoTaskId,
    pub title: String,
    pub completed: bool,
    pub status_id: DemoStatusId,
}

impl CommandHandler for CreateDemoTask {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        ctx.emit_set(&DemoTask {
            id: self.id,
            title: self.title,
            completed: self.completed,
            status_id: self.status_id,
        })?;
        Ok(true)
    }
}

/// Changes a demo task title while retaining the rest of the task.
#[myko_command(bool)]
pub struct RenameDemoTask {
    pub id: DemoTaskId,
    pub title: String,
}

impl CommandHandler for RenameDemoTask {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        let task = ctx
            .exec_report(GetDemoTaskById {
                id: self.id.clone(),
            })?
            .ok_or_else(|| {
                CommandError::new(
                    ctx.tx(),
                    "RenameDemoTask",
                    format!("demo task {} not found", self.id),
                )
            })?;
        ctx.emit_set(&DemoTask {
            id: self.id,
            title: self.title,
            completed: task.completed,
            status_id: task.status_id.clone(),
        })?;
        Ok(true)
    }
}

/// Assigns a status to a task while retaining its other fields.
#[myko_command(bool)]
pub struct SetDemoTaskStatus {
    pub id: DemoTaskId,
    pub status_id: DemoStatusId,
}

impl CommandHandler for SetDemoTaskStatus {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        ctx.exec_report(GetDemoStatusById {
            id: self.status_id.clone(),
        })?
        .ok_or_else(|| {
            CommandError::new(
                ctx.tx(),
                "SetDemoTaskStatus",
                format!("demo status {} not found", self.status_id),
            )
        })?;
        let task = ctx
            .exec_report(GetDemoTaskById {
                id: self.id.clone(),
            })?
            .ok_or_else(|| {
                CommandError::new(
                    ctx.tx(),
                    "SetDemoTaskStatus",
                    format!("demo task {} not found", self.id),
                )
            })?;
        ctx.emit_set(&DemoTask {
            id: self.id,
            title: task.title.clone(),
            completed: task.completed,
            status_id: self.status_id,
        })?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_task_round_trips_through_json() {
        let task = DemoTask {
            id: DemoTaskId::from("task-1"),
            title: "Connect a client".to_owned(),
            completed: true,
            status_id: DemoStatusId::from("done"),
        };

        let json = serde_json::to_string(&task);
        assert!(json.is_ok());
        let decoded = json.and_then(|json| serde_json::from_str::<DemoTask>(&json));
        assert_eq!(decoded.ok(), Some(task));
    }
}
