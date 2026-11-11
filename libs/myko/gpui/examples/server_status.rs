// Shared native/Wasm demo. Styling remains consumer-owned.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use gpui::{
    App, Bounds, Context, Entity, SharedString, WindowBounds, WindowOptions, div, prelude::*, px,
    rgb, size,
};
use myko::entities::{
    demo::{
        CreateDemoStatus, CreateDemoTask, DeleteDemoStatusResult, DeleteDemoTask,
        DeleteDemoTaskResult, DeleteUnreferencedDemoStatus, DemoStatus, DemoTask,
        DemoTaskWithStatus, GetDemoStatuses, GetDemoTasksWithStatus, RenameDemoStatus,
        RenameDemoTask, SetDemoTaskStatus,
    },
    server::{GetConnectedServer, GetPeerServers, Server},
};
use myko_gpui::{
    CommandSlot, CommandState, CrudCommands, CrudController, CrudRowActions, FineQueryList,
    LoadState, MapEntry, QueryStore, Remote, command, connection_status,
    fine_query_list_from_store_with_key, live_query, live_query_store, live_view_store,
    observe_crud_store, observe_remote, provide_myko,
};

type TaskCrud = CrudController<DemoTask, CreateDemoTask, String, bool, bool, DeleteDemoTaskResult>;
type StatusCrud =
    CrudController<DemoStatus, CreateDemoStatus, String, bool, bool, DeleteDemoStatusResult>;

struct ServerStatus {
    connection: Entity<Remote<myko::client::ConnectionStatus>>,
    connected: Entity<Remote<Vec<Arc<Server>>>>,
    peers: Entity<Remote<Vec<Arc<Server>>>>,
    tasks: Entity<FineQueryList<DemoTaskWithStatus, DemoTaskRow>>,
    statuses: Entity<FineQueryList<DemoStatus, DemoStatusRow>>,
    task_crud: Entity<TaskCrud>,
    status_crud: Entity<StatusCrud>,
    _subscriptions: Vec<gpui::Subscription>,
}

const fn load_label<T>(state: &LoadState<T>) -> &'static str {
    match state {
        LoadState::Loading { .. } => "loading",
        LoadState::Ready(_) => "ready",
        LoadState::Error { .. } => "error",
    }
}

const fn command_label<R: myko::hyphae::CellValue>(
    state: Option<&CommandState<R>>,
) -> &'static str {
    match state {
        None => "idle",
        Some(CommandState::Pending) => "pending",
        Some(CommandState::Success(_)) => "success",
        Some(CommandState::Failed(_)) => "failed",
    }
}

fn presentation_color(value: &str) -> u32 {
    u32::from_str_radix(value.trim_start_matches('#'), 16).unwrap_or(0x71_80_96)
}

struct DemoTaskRow {
    key: Arc<str>,
    entry: Entity<MapEntry<Arc<DemoTaskWithStatus>>>,
    status_store: Entity<QueryStore<DemoStatus>>,
    crud: Entity<TaskCrud>,
    actions: Entity<CrudRowActions<bool, DeleteDemoTaskResult>>,
    _action_observation: gpui::Subscription,
    status_option_ids: Vec<Arc<str>>,
    status_option_observations: Vec<gpui::Subscription>,
    set_status: CommandSlot<bool>,
    _entry_observation: gpui::Subscription,
    _status_store_observation: gpui::Subscription,
}

impl DemoTaskRow {
    fn new(
        key: Arc<str>,
        entry: Entity<MapEntry<Arc<DemoTaskWithStatus>>>,
        status_store: Entity<QueryStore<DemoStatus>>,
        crud: Entity<TaskCrud>,
        cx: &mut Context<Self>,
    ) -> Self {
        let entry_observation = cx.observe(&entry, |_row, _entry, cx| cx.notify());
        let status_store_observation = cx.observe(&status_store, |_row, _store, cx| cx.notify());
        let actions = crud.update(cx, |crud, cx| crud.row_actions_for(key.clone(), cx));
        let action_observation = cx.observe(&actions, |_row, _actions, cx| cx.notify());
        Self {
            key,
            entry,
            status_store,
            crud,
            actions,
            _action_observation: action_observation,
            status_option_ids: Vec::new(),
            status_option_observations: Vec::new(),
            set_status: CommandSlot::new(),
            _entry_observation: entry_observation,
            _status_store_observation: status_store_observation,
        }
    }

    fn task_input(task: &DemoTaskWithStatus) -> Arc<DemoTask> {
        Arc::new(DemoTask {
            id: task.id.as_ref().into(),
            title: task.title.clone(),
            completed: task.completed,
            status_id: task.status_id.clone(),
        })
    }

    fn rename(&self, cx: &mut Context<Self>) {
        let Some(task) = self.entry.read(cx).value().cloned() else {
            return;
        };
        let title = format!("{} (renamed)", task.title);
        self.crud.update(cx, |crud, cx| {
            crud.rename(self.key.clone(), Self::task_input(&task), title, cx);
        });
    }

    fn delete(&self, cx: &mut Context<Self>) {
        let Some(task) = self.entry.read(cx).value().cloned() else {
            return;
        };
        self.crud.update(cx, |crud, cx| {
            crud.delete(self.key.clone(), Self::task_input(&task), cx);
        });
    }

    fn set_status(&mut self, status_id: &Arc<str>, cx: &mut Context<Self>) {
        let Some(task) = self.entry.read(cx).value().cloned() else {
            return;
        };
        let request = SetDemoTaskStatus {
            id: task.id.as_ref().into(),
            status_id: status_id.as_ref().into(),
        };
        self.set_status
            .try_start(cx, move |cx| command(&request, cx));
    }

    fn cycle_status(&mut self, cx: &mut Context<Self>) {
        let Some(task) = self.entry.read(cx).value().cloned() else {
            return;
        };
        let mut keys = self.status_store.read(cx).keys().to_vec();
        keys.sort();
        let next = keys
            .iter()
            .position(|key| key.as_ref() == task.status_id.as_ref())
            .and_then(|index| keys.get(index.saturating_add(1)))
            .or_else(|| keys.first())
            .cloned();
        if let Some(status_id) = next {
            self.set_status(&status_id, cx);
        }
    }

    fn status_options(&mut self, cx: &mut Context<Self>) -> Vec<Arc<DemoStatus>> {
        let mut ids = self.status_store.read(cx).keys().to_vec();
        ids.sort();
        if ids != self.status_option_ids {
            let entries = {
                let store = self.status_store.read(cx);
                ids.iter()
                    .filter_map(|id| store.entry(id))
                    .collect::<Vec<_>>()
            };
            self.status_option_observations = entries
                .iter()
                .map(|entry| cx.observe(entry, |_row, _entry, cx| cx.notify()))
                .collect();
            self.status_option_ids = ids;
        }
        let entries = {
            let store = self.status_store.read(cx);
            self.status_option_ids
                .iter()
                .filter_map(|id| store.entry(id))
                .collect::<Vec<_>>()
        };
        entries
            .iter()
            .filter_map(|entry| entry.read(cx).value().cloned())
            .collect()
    }
}

impl Render for DemoTaskRow {
    fn render(&mut self, _window: &mut gpui::Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(task) = self.entry.read(cx).value().cloned() else {
            return div();
        };
        let status_options = self.status_options(cx);
        let (rename_state, delete_state) = self.actions.read_with(cx, |actions, cx| {
            (
                command_label(actions.rename_state(cx)),
                command_label(actions.delete_state(cx)),
            )
        });
        let set_state = command_label(self.set_status.state(cx));
        let status_text = format!(
            "{} {} · {}",
            task.status_emoji, task.status_name, task.status_color
        );
        let color = presentation_color(&task.status_color);
        div()
            .flex()
            .gap(px(10.))
            .p(px(10.))
            .border_1()
            .border_color(rgb(0x40_40_4a))
            .rounded(px(6.))
            .child(if task.completed { "✓" } else { "○" })
            .child(task.title.clone())
            .child(
                div()
                    .id(SharedString::from(format!("status-demo-task-{}", self.key)))
                    .cursor_pointer()
                    .px(px(7.))
                    .rounded(px(4.))
                    .bg(rgb(color))
                    .on_click(cx.listener(|row, _, _window, cx| row.cycle_status(cx)))
                    .child(status_text),
            )
            .child(
                div()
                    .flex()
                    .gap(px(4.))
                    .children(status_options.into_iter().map(|status| {
                        let status_id: Arc<str> = status.id.clone().into();
                        div()
                            .id(SharedString::from(format!(
                                "select-demo-task-status-{}-{status_id}",
                                self.key
                            )))
                            .cursor_pointer()
                            .px(px(5.))
                            .rounded(px(4.))
                            .bg(rgb(presentation_color(&status.color)))
                            .on_click(cx.listener(move |row, _, _window, cx| {
                                row.set_status(&status_id, cx);
                            }))
                            .child(format!("{} {}", status.emoji, status.name))
                    })),
            )
            .child(format!("set: {set_state}"))
            .child(
                div()
                    .id(SharedString::from(format!("rename-demo-task-{}", self.key)))
                    .cursor_pointer()
                    .on_click(cx.listener(|row, _, _window, cx| row.rename(cx)))
                    .child("Rename"),
            )
            .child(rename_state.to_owned())
            .child(
                div()
                    .id(SharedString::from(format!("delete-demo-task-{}", self.key)))
                    .cursor_pointer()
                    .on_click(cx.listener(|row, _, _window, cx| row.delete(cx)))
                    .child("Delete"),
            )
            .child(delete_state.to_owned())
    }
}

struct DemoStatusRow {
    key: Arc<str>,
    entry: Entity<MapEntry<Arc<DemoStatus>>>,
    crud: Entity<StatusCrud>,
    actions: Entity<CrudRowActions<bool, DeleteDemoStatusResult>>,
    _action_observation: gpui::Subscription,
    _entry_observation: gpui::Subscription,
}

impl DemoStatusRow {
    fn new(
        key: Arc<str>,
        entry: Entity<MapEntry<Arc<DemoStatus>>>,
        crud: Entity<StatusCrud>,
        cx: &mut Context<Self>,
    ) -> Self {
        let entry_observation = cx.observe(&entry, |_row, _entry, cx| cx.notify());
        let actions = crud.update(cx, |crud, cx| crud.row_actions_for(key.clone(), cx));
        let action_observation = cx.observe(&actions, |_row, _actions, cx| cx.notify());
        Self {
            key,
            entry,
            crud,
            actions,
            _action_observation: action_observation,
            _entry_observation: entry_observation,
        }
    }

    fn rename(&self, cx: &mut Context<Self>) {
        let Some(status) = self.entry.read(cx).value().cloned() else {
            return;
        };
        let name = format!("{} (renamed)", status.name);
        self.crud.update(cx, |crud, cx| {
            crud.rename(self.key.clone(), status, name, cx);
        });
    }

    fn delete(&self, cx: &mut Context<Self>) {
        let Some(status) = self.entry.read(cx).value().cloned() else {
            return;
        };
        self.crud.update(cx, |crud, cx| {
            crud.delete(self.key.clone(), status, cx);
        });
    }
}

impl Render for DemoStatusRow {
    fn render(&mut self, _window: &mut gpui::Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(status) = self.entry.read(cx).value() else {
            return div();
        };
        let (rename_state, delete_state) = self.actions.read_with(cx, |actions, cx| {
            (
                command_label(actions.rename_state(cx)),
                command_label(actions.delete_state(cx)),
            )
        });
        div()
            .flex()
            .gap(px(10.))
            .p(px(10.))
            .rounded(px(6.))
            .bg(rgb(presentation_color(&status.color)))
            .child(status.emoji.clone())
            .child(status.name.clone())
            .child(status.color.clone())
            .child(
                div()
                    .id(SharedString::from(format!(
                        "rename-demo-status-{}",
                        self.key
                    )))
                    .cursor_pointer()
                    .on_click(cx.listener(|row, _, _window, cx| row.rename(cx)))
                    .child("Rename deterministic"),
            )
            .child(format!("rename: {rename_state}"))
            .child(
                div()
                    .id(SharedString::from(format!(
                        "delete-demo-status-{}",
                        self.key
                    )))
                    .cursor_pointer()
                    .on_click(cx.listener(|row, _, _window, cx| row.delete(cx)))
                    .child("Delete"),
            )
            .child(format!("delete: {delete_state}"))
    }
}

fn server_row(server: &Server) -> impl IntoElement {
    div()
        .p(px(8.))
        .border_1()
        .border_color(rgb(0x40_40_4a))
        .rounded(px(6.))
        .child(format!(
            "{}:{} · version {}",
            server.address, server.port, server.version
        ))
}

impl Render for ServerStatus {
    fn render(&mut self, _window: &mut gpui::Window, cx: &mut Context<Self>) -> impl IntoElement {
        let connection = self.connection.read(cx);
        let connected = self.connected.read(cx);
        let peers = self.peers.read(cx);
        let connection_text: SharedString = connection.value().map_or_else(
            || load_label(connection.state()).into(),
            |value| format!("{value:?}").into(),
        );
        div()
            .size_full()
            .flex()
            .flex_col()
            .gap(px(12.))
            .p(px(20.))
            .bg(rgb(0x18_18_1e))
            .text_color(rgb(0xf0_f0_f2))
            .child(div().text_size(px(24.)).child("Myko GPUI"))
            .child(format!(
                "Connection: {connection_text} · connected: {} · peers: {}",
                load_label(connected.state()),
                load_label(peers.state())
            ))
            .child(
                div()
                    .text_size(px(18.))
                    .child("Demo tasks · click a colored status to cycle"),
            )
            .child(
                div()
                    .id("create-demo-task")
                    .cursor_pointer()
                    .on_click(cx.listener(|status, _, window, cx| {
                        status.task_crud.update(cx, |crud, cx| {
                            crud.create_from_provider(window, cx);
                        });
                    }))
                    .child(format!(
                        "Create deterministic task · {}",
                        command_label(self.task_crud.read(cx).create_state(cx))
                    )),
            )
            .child(self.tasks.clone())
            .child(div().text_size(px(18.)).child("Status styling + CRUD"))
            .child(
                div()
                    .id("create-demo-status")
                    .cursor_pointer()
                    .on_click(cx.listener(|status, _, window, cx| {
                        status.status_crud.update(cx, |crud, cx| {
                            crud.create_from_provider(window, cx);
                        });
                    }))
                    .child(format!(
                        "Create status via provider · {}",
                        command_label(self.status_crud.read(cx).create_state(cx))
                    )),
            )
            .child(self.statuses.clone())
            .child(div().text_size(px(18.)).child("Connected server"))
            .children(
                connected
                    .value()
                    .into_iter()
                    .flatten()
                    .map(|server| server_row(server)),
            )
            .when(peers.value().is_some_and(Vec::is_empty), |view| {
                view.child(
                    div()
                        .text_color(rgb(0x9b_9b_a7))
                        .child("No peers advertised"),
                )
            })
            .children(
                peers
                    .value()
                    .into_iter()
                    .flatten()
                    .map(|server| server_row(server)),
            )
    }
}

#[cfg(not(target_family = "wasm"))]
fn server_address() -> String {
    std::env::var("MYKO_DEMO_URL").unwrap_or_else(|_| "ws://127.0.0.1:5155/myko".to_owned())
}

#[cfg(target_family = "wasm")]
fn server_address() -> String {
    "ws://127.0.0.1:5155/myko".to_owned()
}

fn task_crud(status_store: Entity<QueryStore<DemoStatus>>, cx: &mut App) -> Entity<TaskCrud> {
    let next_task = Arc::new(AtomicUsize::new(1));
    let commands = CrudCommands::new()
        .with_create(|input: CreateDemoTask, cx| command(&input, cx))
        .with_create_input(move |_window, cx| {
            let mut keys = status_store.read(cx).keys().to_vec();
            keys.sort();
            let status_id = keys.first()?.as_ref().into();
            let number = next_task.fetch_add(1, Ordering::Relaxed);
            Some(CreateDemoTask {
                id: format!("gpui-task-{number}").into(),
                title: format!("Generated task {number}"),
                completed: false,
                status_id,
            })
        })
        .with_rename(|task: Arc<DemoTask>, title: String, cx| {
            command(
                &RenameDemoTask {
                    id: task.id.clone(),
                    title,
                },
                cx,
            )
        })
        .with_delete(|task: Arc<DemoTask>, cx| {
            command(
                &DeleteDemoTask {
                    id: task.id.clone(),
                },
                cx,
            )
        });
    cx.new(|_| CrudController::new(commands))
}

fn status_crud(cx: &mut App) -> Entity<StatusCrud> {
    let next_status = Arc::new(AtomicUsize::new(1));
    let commands = CrudCommands::new()
        .with_create(|input: CreateDemoStatus, cx| command(&input, cx))
        .with_create_input(move |_window, _cx| {
            let number = next_status.fetch_add(1, Ordering::Relaxed);
            let (color, emoji) = match number.rem_euclid(4) {
                1 => ("#805ad5", "◆"),
                2 => ("#3182ce", "●"),
                3 => ("#dd6b20", "▲"),
                _ => ("#d53f8c", "♥"),
            };
            Some(CreateDemoStatus {
                id: format!("gpui-status-{number}").into(),
                name: format!("Generated {number}"),
                color: color.to_owned(),
                emoji: emoji.to_owned(),
            })
        })
        .with_rename(|status: Arc<DemoStatus>, name: String, cx| {
            command(
                &RenameDemoStatus {
                    id: status.id.clone(),
                    name,
                },
                cx,
            )
        })
        .with_delete(|status: Arc<DemoStatus>, cx| {
            command(
                &DeleteUnreferencedDemoStatus {
                    id: status.id.clone(),
                },
                cx,
            )
        });
    cx.new(|_| CrudController::new(commands))
}

#[allow(clippy::too_many_lines)]
fn launch(cx: &mut App) {
    provide_myko(server_address(), cx);
    let bounds = Bounds::centered(None, size(px(1000.), px(820.)), cx);
    let window = cx.open_window(
        WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            titlebar: Some(gpui::TitlebarOptions {
                title: Some("Myko GPUI Demo".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        |_window, cx| {
            let connection = connection_status(cx);
            let connected = live_query(GetConnectedServer {}, cx);
            let peers = live_query(GetPeerServers {}, cx);
            let status_store = live_query_store(GetDemoStatuses {}, cx);

            let task_crud = task_crud(status_store.clone(), cx);
            let status_crud = status_crud(cx);

            let task_store = live_view_store(GetDemoTasksWithStatus {}, cx);
            let tasks = fine_query_list_from_store_with_key(
                task_store.clone(),
                {
                    let task_crud = task_crud.clone();
                    let status_store = status_store.clone();
                    move |key, entry, cx| {
                        let task_crud = task_crud.clone();
                        let status_store = status_store.clone();
                        cx.new(|cx| DemoTaskRow::new(key, entry, status_store, task_crud, cx))
                    }
                },
                || {
                    div()
                        .text_color(rgb(0x9b_9b_a7))
                        .child("Loading demo tasks")
                },
                |message| div().text_color(rgb(0xe0_60_60)).child(message.to_owned()),
                || div().text_color(rgb(0x9b_9b_a7)).child("No demo tasks"),
                |rows| div().flex().flex_col().gap(px(6.)).children(rows),
                cx,
            );
            let statuses = fine_query_list_from_store_with_key(
                status_store.clone(),
                {
                    let status_crud = status_crud.clone();
                    move |key, entry, cx| {
                        let status_crud = status_crud.clone();
                        cx.new(|cx| DemoStatusRow::new(key, entry, status_crud, cx))
                    }
                },
                || div().text_color(rgb(0x9b_9b_a7)).child("Loading statuses"),
                |message| div().text_color(rgb(0xe0_60_60)).child(message.to_owned()),
                || div().text_color(rgb(0x9b_9b_a7)).child("No statuses"),
                |rows| div().flex().flex_col().gap(px(6.)).children(rows),
                cx,
            );
            cx.new(move |cx| {
                let subscriptions = vec![
                    observe_remote(&connection, cx),
                    observe_remote(&connected, cx),
                    observe_remote(&peers, cx),
                    cx.observe(&task_crud, |_status, _crud, cx| cx.notify()),
                    cx.observe(&status_crud, |_status, _crud, cx| cx.notify()),
                    observe_crud_store(&task_crud, &task_store, cx),
                    observe_crud_store(&status_crud, &status_store, cx),
                ];
                ServerStatus {
                    connection,
                    connected,
                    peers,
                    tasks,
                    statuses,
                    task_crud,
                    status_crud,
                    _subscriptions: subscriptions,
                }
            })
        },
    );
    if let Err(error) = window {
        myko::tracing::error!(%error, "failed to open Myko GPUI demo window");
        cx.quit();
        return;
    }
    cx.activate(true);
}

#[cfg(not(target_family = "wasm"))]
fn main() {
    gpui_platform::application().run(launch);
}

#[cfg(target_family = "wasm")]
thread_local! {
    static APPLICATION: std::cell::RefCell<Option<gpui::ApplicationHandle>> = const { std::cell::RefCell::new(None) };
}

#[cfg(target_family = "wasm")]
fn main() {
    gpui_platform::web_init();
    let app = gpui_platform::application().run_embedded(launch);
    APPLICATION.with(|slot| *slot.borrow_mut() = Some(app));
}
