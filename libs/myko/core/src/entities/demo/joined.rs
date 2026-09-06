use std::sync::Arc;

use hyphae::{InnerJoinExt as _, MapEntriesExt as _, MapQuery};

use super::{
    DemoStatus, DemoStatusId, DemoTask, DemoTaskStatusIdRelation, GetDemoStatuses, GetDemoTasks,
};
use crate::prelude::*;

/// A task projected with the presentation metadata of its current status.
///
/// `id` is always the task ID, so consumers can retain row identity when
/// either side of the join changes.
#[allow(clippy::derive_partial_eq_without_eq)]
#[myko_view_item]
pub struct DemoTaskWithStatus {
    pub id: Arc<str>,
    pub title: String,
    pub completed: bool,
    pub status_id: DemoStatusId,
    pub status_name: String,
    pub status_color: String,
    pub status_emoji: String,
}

/// Returns every demo task whose referenced status currently exists.
#[myko_view(DemoTaskWithStatus)]
pub struct GetDemoTasksWithStatus {}

impl ViewHandler for GetDemoTasksWithStatus {
    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl crate::view::ViewBuildOutput<Item = Self::Item> {
        crate::view::LocalView::new({
            joined_demo_tasks(
                ctx.view_context.query_map_by_str(GetDemoTasks {}),
                ctx.view_context.query_map_by_str(GetDemoStatuses {}),
            )
        })
    }
}

fn joined_demo_tasks(
    tasks: impl MapQuery<Key = Arc<str>, Value = Arc<DemoTask>>,
    statuses: impl MapQuery<Key = Arc<str>, Value = Arc<DemoStatus>>,
) -> impl MapQuery<Key = Arc<str>, Value = Arc<DemoTaskWithStatus>> {
    statuses
        .inner_join_fk::<DemoTaskStatusIdRelation, _>(tasks)
        .map_entries(|(_status_id, task_id), (status, task)| {
            (
                task_id.clone(),
                Arc::new(DemoTaskWithStatus {
                    id: task_id.clone(),
                    title: task.title.clone(),
                    completed: task.completed,
                    status_id: task.status_id.clone(),
                    status_name: status.name.clone(),
                    status_color: status.color.clone(),
                    status_emoji: status.emoji.clone(),
                }),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use hyphae::{CellMap, MapDiff, MapQuery as _};

    use super::*;
    use crate::test_util::scheduler_test_serial;

    fn task(id: &str, status_id: &str) -> Arc<DemoTask> {
        Arc::new(DemoTask {
            id: id.into(),
            title: id.to_owned(),
            completed: false,
            status_id: status_id.into(),
        })
    }

    fn status(id: &str, name: &str) -> Arc<DemoStatus> {
        Arc::new(DemoStatus {
            id: id.into(),
            name: name.to_owned(),
            color: "#000000".to_owned(),
            emoji: "○".to_owned(),
        })
    }

    fn collect_updates(
        diff: &MapDiff<Arc<str>, Arc<DemoTaskWithStatus>>,
        into: &mut Vec<Arc<str>>,
    ) {
        match diff {
            MapDiff::Update { key, .. } => into.push(key.clone()),
            MapDiff::Batch { changes } => {
                for change in changes {
                    collect_updates(change, into);
                }
            }
            MapDiff::Initial { .. } | MapDiff::Insert { .. } | MapDiff::Remove { .. } => {}
        }
    }

    #[test]
    fn status_update_only_updates_referencing_joined_rows() {
        let _serial = scheduler_test_serial();
        let tasks = CellMap::new();
        let statuses = CellMap::new();
        tasks.insert("task-a".into(), task("task-a", "todo"));
        tasks.insert("task-b".into(), task("task-b", "done"));
        statuses.insert("todo".into(), status("todo", "Todo"));
        statuses.insert("done".into(), status("done", "Done"));
        let joined = joined_demo_tasks(tasks, statuses.clone()).materialize();
        let changed = Arc::new(Mutex::new(Vec::new()));
        let changed_for_guard = changed.clone();
        let _guard = joined.subscribe_diffs(move |diff| {
            collect_updates(
                diff,
                &mut changed_for_guard
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        });

        statuses.insert("todo".into(), status("todo", "Backlog"));

        let changed = changed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!changed.is_empty());
        assert!(changed.iter().all(|key| key.as_ref() == "task-a"));
        drop(changed);
        assert_eq!(
            joined
                .get_value(&Arc::<str>::from("task-a"))
                .map(|row| row.status_name.clone()),
            Some("Backlog".to_owned())
        );
        assert_eq!(
            joined
                .get_value(&Arc::<str>::from("task-b"))
                .map(|row| row.status_name.clone()),
            Some("Done".to_owned())
        );
    }

    #[test]
    fn membership_is_joined_but_output_identity_is_the_task_id() {
        let _serial = scheduler_test_serial();
        let tasks = CellMap::new();
        let statuses = CellMap::new();
        tasks.insert("task-a".into(), task("task-a", "todo"));
        let joined = joined_demo_tasks(tasks.clone(), statuses.clone()).materialize();
        assert!(joined.is_empty());

        statuses.insert("todo".into(), status("todo", "Todo"));
        let key: Arc<str> = "task-a".into();
        assert_eq!(
            joined.get_value(&key).map(|row| row.id.clone()),
            Some(key.clone())
        );

        tasks.insert("task-a".into(), task("task-a", "missing"));
        assert!(!joined.contains_key(&key));
        statuses.insert("missing".into(), status("missing", "Missing"));
        assert_eq!(joined.get_value(&key).map(|row| row.id.clone()), Some(key));
    }
}
