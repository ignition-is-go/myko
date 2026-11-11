use super::task::GetDemoTasks;
use crate::prelude::*;

/// Consumer-presented status metadata used by client integration demos.
#[myko_item]
#[derive(Eq)]
pub struct DemoStatus {
    pub name: String,
    pub color: String,
    pub emoji: String,
}

/// Returns every demo status.
#[myko_query(DemoStatus)]
pub struct GetDemoStatuses {}

impl QueryHandler for GetDemoStatuses {
    fn test_entity(_ctx: QueryTestContext<Self>) -> bool {
        true
    }
}

/// Creates a status with application-owned presentation metadata.
#[myko_command(bool)]
pub struct CreateDemoStatus {
    pub id: DemoStatusId,
    pub name: String,
    pub color: String,
    pub emoji: String,
}

impl CommandHandler for CreateDemoStatus {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        ctx.emit_set(&DemoStatus {
            id: self.id,
            name: self.name,
            color: self.color,
            emoji: self.emoji,
        })?;
        Ok(true)
    }
}

/// Changes a status name without changing its presentation metadata.
#[myko_command(bool)]
pub struct RenameDemoStatus {
    pub id: DemoStatusId,
    pub name: String,
}

impl CommandHandler for RenameDemoStatus {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        let status = ctx
            .exec_report(GetDemoStatusById {
                id: self.id.clone(),
            })?
            .ok_or_else(|| {
                CommandError::new(
                    ctx.tx(),
                    "RenameDemoStatus",
                    format!("demo status {} not found", self.id),
                )
            })?;
        ctx.emit_set(&DemoStatus {
            id: self.id,
            name: self.name,
            color: status.color.clone(),
            emoji: status.emoji.clone(),
        })?;
        Ok(true)
    }
}

/// Deletes a status only when no task currently references it.
#[myko_command(DeleteDemoStatusResult)]
pub struct DeleteUnreferencedDemoStatus {
    pub id: DemoStatusId,
}

impl CommandHandler for DeleteUnreferencedDemoStatus {
    fn execute(self, ctx: CommandContext) -> Result<DeleteDemoStatusResult, CommandError> {
        let referenced = ctx
            .exec_query(GetDemoTasks {})?
            .iter()
            .any(|task| task.status_id == self.id);
        if referenced {
            return Err(CommandError::new(
                ctx.tx(),
                "DeleteUnreferencedDemoStatus",
                format!("demo status {} is referenced by a task", self.id),
            ));
        }
        let status = ctx
            .exec_report(GetDemoStatusById {
                id: self.id.clone(),
            })?
            .ok_or_else(|| {
                CommandError::new(
                    ctx.tx(),
                    "DeleteUnreferencedDemoStatus",
                    format!("demo status {} not found", self.id),
                )
            })?;
        ctx.emit_del(status)?;
        Ok(DeleteDemoStatusResult { deleted: true })
    }
}
