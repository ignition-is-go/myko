use std::{fmt::Debug, sync::Arc};

use gpui::{
    AnyElement, App, AppContext as _, Context, Entity, EventEmitter, Render, Subscription, Window,
};
use hyphae_gpui::{CellEntity, CellEntityStatus, ToGpuiEntity as _};
use myko::hyphae::{Cell, CellImmutable, CellValue};
use serde::de::DeserializeOwned;

use crate::client::myko;

/// The lifecycle of a single Myko command.
///
/// `Pending` covers both commands queued while disconnected and commands that
/// have been delivered and are awaiting a response. Myko's client preserves
/// queued commands and flushes them after reconnecting, but does not currently
/// expose a separate delivery acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandState<R> {
    Pending,
    Success(Arc<R>),
    Failed(Arc<str>),
}

/// The single semantic terminal transition emitted by a [`Command`].
///
/// Subscribe to this event for side effects. Observe the command entity only
/// when rendering its current [`CommandState`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandEvent<R> {
    Success(Arc<R>),
    Failed(Arc<str>),
}

impl<R> CommandEvent<R> {
    fn state(&self) -> CommandState<R> {
        match self {
            Self::Success(value) => CommandState::Success(value.clone()),
            Self::Failed(error) => CommandState::Failed(error.clone()),
        }
    }
}

/// GPUI-owned lifecycle for one command.
///
/// The entity owns the Hyphae-to-GPUI source and its observation. It starts in
/// [`CommandState::Pending`] synchronously and only then processes a response
/// on GPUI's foreground executor.
pub struct Command<R: CellValue> {
    state: CommandState<R>,
    _observation: Subscription,
    _source: Entity<CellEntity<Option<Result<R, String>>>>,
}

impl<R: CellValue> EventEmitter<CommandEvent<R>> for Command<R> {}

impl<R: CellValue> Command<R> {
    #[must_use]
    pub const fn state(&self) -> &CommandState<R> {
        &self.state
    }

    #[must_use]
    pub const fn is_pending(&self) -> bool {
        matches!(self.state, CommandState::Pending)
    }

    fn apply(&mut self, event: CommandEvent<R>, cx: &mut Context<Self>) {
        if !matches!(self.state, CommandState::Pending) {
            return;
        }
        self.state = event.state();
        cx.emit(event);
        cx.notify();
    }
}

fn source_event<R: CellValue>(
    source: &CellEntity<Option<Result<R, String>>>,
) -> Option<CommandEvent<R>> {
    match source.status() {
        CellEntityStatus::Error(error) => Some(CommandEvent::Failed(error.clone().into())),
        CellEntityStatus::Complete if source.value().is_none_or(Option::is_none) => Some(
            CommandEvent::Failed("command completed without a result".into()),
        ),
        CellEntityStatus::Complete | CellEntityStatus::Active => {
            source.value().and_then(|result| {
                result.as_ref().map(|result| match result {
                    Ok(value) => CommandEvent::Success(Arc::new(value.clone())),
                    Err(error) => CommandEvent::Failed(error.clone().into()),
                })
            })
        }
    }
}

pub fn command_from_cell<R>(
    cell: &Cell<Option<Result<R, String>>, CellImmutable>,
    cx: &mut App,
) -> Entity<Command<R>>
where
    R: CellValue,
{
    let source = cell.to_gpui_entity(cx);
    cx.new(move |cx| {
        let observation = cx.observe(&source, |command: &mut Command<R>, source, cx| {
            if let Some(event) = source.read_with(cx, |source, _| source_event(source)) {
                command.apply(event, cx);
            }
        });

        // A serialization/encoding failure is a synchronous seed in the Myko
        // cell. GPUI deferral preserves an observable Pending construction
        // boundary without an empty background task or scheduler round trip.
        if let Some(event) = source_event(source.read(cx)) {
            let command = cx.weak_entity();
            cx.defer(move |cx| {
                let _ = command.update(cx, |command, cx| {
                    command.apply(event, cx);
                });
            });
        }

        Command {
            state: CommandState::Pending,
            _observation: observation,
            _source: source,
        }
    })
}

/// Send a command and return its first-class GPUI lifecycle entity.
///
/// The returned entity is `Pending` before this function returns, including
/// when the client is disconnected and has queued the command for reconnect.
pub fn command<C, R>(command: &C, cx: &mut App) -> Entity<Command<R>>
where
    C: serde::Serialize + Clone + myko::core::command::CommandId + Send + Sync + 'static,
    R: DeserializeOwned + Clone + Debug + PartialEq + Send + Sync + 'static,
{
    let cell = myko(cx).client().send_command::<C, R>(command);
    command_from_cell(&cell, cx)
}

/// A retained, owner-local slot for the latest invocation of one command.
///
/// Store a slot as a field on any GPUI-owned value. It retains both the command
/// entity and the subscription which forwards that command's lifecycle
/// notifications to the owner. Starting one slot never observes or notifies a
/// controller, collection, or any other owner.
///
/// [`Self::try_start`] constructs its command lazily, so attempting to start
/// while the current command is pending cannot accidentally send a duplicate.
/// A terminal command remains available for rendering until the next start.
pub struct CommandSlot<R: CellValue> {
    command: Option<Entity<Command<R>>>,
    observation: Option<Subscription>,
}

impl<R: CellValue> Default for CommandSlot<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: CellValue> CommandSlot<R> {
    /// Construct an idle command slot.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            command: None,
            observation: None,
        }
    }

    /// The latest invocation, including its terminal state.
    #[must_use]
    pub const fn command(&self) -> Option<&Entity<Command<R>>> {
        self.command.as_ref()
    }

    /// The latest invocation's state, or `None` before the first start.
    #[must_use]
    pub fn state<'a>(&self, cx: &'a App) -> Option<&'a CommandState<R>> {
        self.command
            .as_ref()
            .map(|command| command.read(cx).state())
    }

    /// Whether this slot currently contains a pending invocation.
    #[must_use]
    pub fn is_pending(&self, cx: &App) -> bool {
        self.command
            .as_ref()
            .is_some_and(|command| command.read(cx).is_pending())
    }

    /// Retain an already-created command unless this slot is pending.
    ///
    /// Prefer [`Self::try_start`] when creating the entity sends the command,
    /// since an eager command has already been sent by the time this guard runs.
    pub fn start<Owner>(&mut self, command: Entity<Command<R>>, cx: &mut Context<Owner>) -> bool
    where
        Owner: 'static,
    {
        if self.is_pending(cx) {
            return false;
        }
        self.observation = Some(cx.subscribe(&command, |_owner, _command, _event, cx| {
            cx.notify();
        }));
        self.command = Some(command);
        cx.notify();
        true
    }

    /// Lazily create and retain a command unless this slot is pending.
    ///
    /// The factory is not called when a pending invocation already exists.
    pub fn try_start<Owner>(
        &mut self,
        cx: &mut Context<Owner>,
        create: impl FnOnce(&mut App) -> Entity<Command<R>>,
    ) -> bool
    where
        Owner: 'static,
    {
        if self.is_pending(cx) {
            return false;
        }
        let command = create(cx);
        self.start(command, cx)
    }
}

/// Retains and observes multiple commands that may be pending concurrently.
///
/// Use [`CommandSlot`] for a single user action where a second invocation must
/// be suppressed until the first completes. Use this tracker for event streams
/// where every invocation is meaningful and dropping an overlapping command
/// would lose a mutation.
pub struct CommandTracker<R: CellValue> {
    entries: Vec<(Entity<Command<R>>, Subscription)>,
}

impl<R: CellValue> Default for CommandTracker<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: CellValue> CommandTracker<R> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn pending_count(&self, cx: &App) -> usize {
        self.entries
            .iter()
            .filter(|(command, _)| command.read(cx).is_pending())
            .count()
    }

    /// Drop completed entries while retaining every in-flight command.
    pub fn prune_finished(&mut self, cx: &App) {
        self.entries
            .retain(|(command, _)| command.read(cx).is_pending());
    }

    /// Retain a command and deliver its one terminal transition to its owner.
    pub fn track<Owner>(
        &mut self,
        command: Entity<Command<R>>,
        cx: &mut Context<Owner>,
        mut callback: impl FnMut(&mut Owner, CommandState<R>, &mut Context<Owner>) + 'static,
    ) where
        Owner: 'static,
    {
        self.prune_finished(cx);
        let observation = on_command_change(&command, cx, move |owner, state, cx| {
            callback(owner, state, cx);
        });
        self.entries.push((command, observation));
        cx.notify();
    }

    /// Retain a command and report only its terminal failure to the owner.
    ///
    /// This is the common mutation-stream case where success is reflected by
    /// a live query and only an error needs local UI state. The owner is
    /// notified after the failure callback; successful completion is retained
    /// until the next prune without invoking the callback.
    pub fn track_failure<Owner>(
        &mut self,
        command: Entity<Command<R>>,
        cx: &mut Context<Owner>,
        mut callback: impl FnMut(&mut Owner, Arc<str>, &mut Context<Owner>) + 'static,
    ) where
        Owner: 'static,
    {
        self.track(command, cx, move |owner, state, cx| {
            if let CommandState::Failed(message) = state {
                callback(owner, message, cx);
                cx.notify();
            }
        });
    }
}

/// Observe a command from an owning entity and redraw it on lifecycle changes.
pub fn observe_command<Owner, R>(
    command: &Entity<Command<R>>,
    cx: &mut Context<Owner>,
) -> Subscription
where
    Owner: 'static,
    R: CellValue,
{
    cx.subscribe(command, |_owner, _command, _event, cx| cx.notify())
}

/// Run an owner-aware callback when a command's state changes.
///
/// The initial `Pending` state is not replayed. A command has exactly one
/// terminal transition, so a terminal callback runs at most once. Unlike a
/// render callback, this is an appropriate place to update another entity or
/// global state.
pub fn on_command_change<Owner, R>(
    command: &Entity<Command<R>>,
    cx: &mut Context<Owner>,
    mut callback: impl FnMut(&mut Owner, CommandState<R>, &mut Context<Owner>) + 'static,
) -> Subscription
where
    Owner: 'static,
    R: CellValue,
{
    cx.subscribe(command, move |owner, _command, event, cx| {
        callback(owner, event.state(), cx);
    })
}

/// Run a window- and owner-aware callback when a command's state changes.
///
/// This helper keeps lifecycle side effects out of rendering. The callback can
/// show a window-local toast or navigate, update GPUI global state, or update
/// another entity. The initial `Pending` state is not replayed and the single
/// terminal transition is delivered at most once.
///
/// ```ignore
/// let observation = observe_command_in(&command, window, cx, |owner, state, window, cx| {
///     match state {
///         CommandState::Success(_) => owner.show_toast("Saved", window, cx),
///         CommandState::Failed(error) => owner.show_error(&error, window, cx),
///         CommandState::Pending => {}
///     }
///     // Global and other-entity updates can also be performed through `cx`.
/// });
/// ```
pub fn observe_command_in<Owner, R>(
    command: &Entity<Command<R>>,
    window: &mut Window,
    cx: &mut Context<Owner>,
    mut callback: impl FnMut(&mut Owner, CommandState<R>, &mut Window, &mut Context<Owner>) + 'static,
) -> Subscription
where
    Owner: 'static,
    R: CellValue,
{
    cx.subscribe_in(
        command,
        window,
        move |owner, _command, event, window, cx| {
            callback(owner, event.state(), window, cx);
        },
    )
}

type PendingHook = dyn FnMut(&mut App) + 'static;
type SuccessHook<R> = dyn FnMut(&R, &mut App) + 'static;
type FailedHook = dyn FnMut(&str, &mut App) + 'static;

/// Optional event-wise callbacks for [`command_boundary`].
///
/// Hooks run during entity construction/observation, never during rendering.
pub struct CommandHooks<R> {
    pending: Option<Box<PendingHook>>,
    success: Option<Box<SuccessHook<R>>>,
    failed: Option<Box<FailedHook>>,
}

impl<R> Default for CommandHooks<R> {
    fn default() -> Self {
        Self {
            pending: None,
            success: None,
            failed: None,
        }
    }
}

impl<R> CommandHooks<R> {
    #[must_use]
    pub fn on_pending(mut self, hook: impl FnMut(&mut App) + 'static) -> Self {
        self.pending = Some(Box::new(hook));
        self
    }

    #[must_use]
    pub fn on_success(mut self, hook: impl FnMut(&R, &mut App) + 'static) -> Self {
        self.success = Some(Box::new(hook));
        self
    }

    #[must_use]
    pub fn on_failed(mut self, hook: impl FnMut(&str, &mut App) + 'static) -> Self {
        self.failed = Some(Box::new(hook));
        self
    }

    fn pending(&mut self, cx: &mut App) {
        if let Some(mut hook) = self.pending.take() {
            hook(cx);
        }
    }

    fn transition(&mut self, event: &CommandEvent<R>, cx: &mut App) {
        match event {
            CommandEvent::Success(value) => {
                if let Some(mut hook) = self.success.take() {
                    hook(value, cx);
                }
            }
            CommandEvent::Failed(error) => {
                if let Some(mut hook) = self.failed.take() {
                    hook(error, cx);
                }
            }
        }
    }
}

type CommandRenderer<R> = dyn Fn(&CommandState<R>) -> AnyElement + 'static;

/// Self-observing, styling-agnostic command boundary.
pub struct CommandBoundary<R: CellValue> {
    command: Entity<Command<R>>,
    render: Box<CommandRenderer<R>>,
    hooks: CommandHooks<R>,
    _observation: Subscription,
}

impl<R: CellValue> Render for CommandBoundary<R> {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        cx: &mut Context<Self>,
    ) -> impl gpui::IntoElement {
        (self.render)(self.command.read(cx).state())
    }
}

fn boundary_for_command<R, F>(
    command: Entity<Command<R>>,
    render: F,
    mut hooks: CommandHooks<R>,
    cx: &mut App,
) -> Entity<CommandBoundary<R>>
where
    R: CellValue,
    F: Fn(&CommandState<R>) -> AnyElement + 'static,
{
    cx.new(move |cx| {
        hooks.pending(cx);
        let observation = cx.subscribe(
            &command,
            |boundary: &mut CommandBoundary<R>, _command, event, cx| {
                boundary.hooks.transition(event, cx);
                cx.notify();
            },
        );
        CommandBoundary {
            command,
            render: Box::new(render),
            hooks,
            _observation: observation,
        }
    })
}

/// Send a command and construct a retained lifecycle boundary.
///
/// `render` is styling-agnostic. `hooks` fire at most once each and run on
/// GPUI's foreground executor outside the render path. The pending hook runs
/// synchronously while the boundary is constructed.
pub fn command_boundary<C, R, F>(
    request: &C,
    render: F,
    hooks: CommandHooks<R>,
    cx: &mut App,
) -> Entity<CommandBoundary<R>>
where
    C: serde::Serialize + Clone + myko::core::command::CommandId + Send + Sync + 'static,
    R: DeserializeOwned + Clone + Debug + PartialEq + Send + Sync + 'static,
    F: Fn(&CommandState<R>) -> AnyElement + 'static,
{
    let command = command(request, cx);
    boundary_for_command(command, render, hooks, cx)
}

#[cfg(test)]
#[allow(clippy::needless_pass_by_ref_mut)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use gpui::{AppContext as _, IntoElement as _, TestAppContext};
    use myko::hyphae::{Cell, Mutable as _};

    use super::{
        CommandEvent, CommandHooks, CommandSlot, CommandState, CommandTracker,
        boundary_for_command, command_from_cell,
    };

    struct SlotOwner {
        slot: CommandSlot<u32>,
    }

    struct Probe {
        _observation: gpui::Subscription,
    }

    struct TrackerOwner {
        tracker: CommandTracker<u32>,
        results: Vec<u32>,
        errors: Vec<String>,
    }

    #[gpui::test]
    fn pending_is_synchronous_and_terminal_transition_is_one_shot(cx: &mut TestAppContext) {
        let writer = Cell::new(None::<Result<u32, String>>);
        let cell = writer.clone().lock();
        let command = cx.update(|cx| command_from_cell(&cell, cx));
        assert!(matches!(
            command.read_with(cx, |command, _| command.state().clone()),
            CommandState::Pending
        ));

        writer.set(Some(Ok(7)));
        cx.run_until_parked();
        assert!(matches!(
            command.read_with(cx, |command, _| command.state().clone()),
            CommandState::Success(value) if *value == 7
        ));

        writer.set(Some(Err("late".into())));
        cx.run_until_parked();
        assert!(matches!(
            command.read_with(cx, |command, _| command.state().clone()),
            CommandState::Success(value) if *value == 7
        ));
    }

    #[gpui::test]
    fn terminal_transition_is_emitted_once_as_a_typed_event(cx: &mut TestAppContext) {
        let writer = Cell::new(None::<Result<u32, String>>);
        let cell = writer.clone().lock();
        let command = cx.update(|cx| command_from_cell(&cell, cx));
        let events = Arc::new(Mutex::new(Vec::<CommandEvent<u32>>::new()));
        let probe = cx.new({
            let command = command.clone();
            let events = events.clone();
            move |cx| Probe {
                _observation: cx.subscribe(&command, move |_probe, _command, event, _cx| {
                    events
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(event.clone());
                }),
            }
        });

        cx.run_until_parked();
        writer.set(Some(Ok(7)));
        cx.run_until_parked();
        cx.run_until_parked();
        writer.set(Some(Err("late".into())));
        cx.run_until_parked();
        cx.run_until_parked();
        probe.read_with(cx, |_probe, _cx| ());
        assert!(command.read_with(cx, |command, _| {
            matches!(command.state(), CommandState::Success(value) if **value == 7)
        }));

        assert_eq!(
            *events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            [CommandEvent::Success(Arc::new(7))],
        );
    }

    #[gpui::test]
    fn slot_pending_guard_is_lazy(cx: &mut TestAppContext) {
        let writer = Cell::new(None::<Result<u32, String>>);
        let cell = writer.lock();
        let owner = cx.new(|_| SlotOwner {
            slot: CommandSlot::new(),
        });
        let calls = Arc::new(AtomicUsize::new(0));

        assert!(owner.update(cx, |owner, cx| {
            let calls = calls.clone();
            owner.slot.try_start(cx, move |cx| {
                calls.fetch_add(1, Ordering::Relaxed);
                command_from_cell(&cell, cx)
            })
        }));
        let guarded_cell = Cell::new(None::<Result<u32, String>>).lock();
        assert!(!owner.update(cx, |owner, cx| {
            let calls = calls.clone();
            owner.slot.try_start(cx, move |cx| {
                calls.fetch_add(1, Ordering::Relaxed);
                command_from_cell(&guarded_cell, cx)
            })
        }));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(owner.read_with(cx, |owner, cx| owner.slot.is_pending(cx)));
    }

    #[gpui::test]
    fn slot_can_restart_after_terminal_state(cx: &mut TestAppContext) {
        let first_writer = Cell::new(None::<Result<u32, String>>);
        let first = first_writer.clone().lock();
        let second_writer = Cell::new(None::<Result<u32, String>>);
        let second = second_writer.clone().lock();
        let owner = cx.new(|_| SlotOwner {
            slot: CommandSlot::new(),
        });
        assert!(owner.update(cx, |owner, cx| {
            owner.slot.try_start(cx, |cx| command_from_cell(&first, cx))
        }));
        first_writer.set(Some(Ok(1)));
        cx.run_until_parked();
        assert!(owner.read_with(cx, |owner, cx| {
            matches!(owner.slot.state(cx), Some(CommandState::Success(value)) if **value == 1)
        }));

        assert!(owner.update(cx, |owner, cx| {
            owner
                .slot
                .try_start(cx, |cx| command_from_cell(&second, cx))
        }));
        assert!(owner.read_with(cx, |owner, cx| owner.slot.is_pending(cx)));
        second_writer.set(Some(Ok(2)));
        cx.run_until_parked();
        assert!(owner.read_with(cx, |owner, cx| {
            matches!(owner.slot.state(cx), Some(CommandState::Success(value)) if **value == 2)
        }));
    }

    #[gpui::test]
    fn slot_notifications_are_isolated_to_the_owning_entity(cx: &mut TestAppContext) {
        let first_writer = Cell::new(None::<Result<u32, String>>);
        let first = first_writer.clone().lock();
        let second_writer = Cell::new(None::<Result<u32, String>>);
        let second = second_writer.lock();
        let first_owner = cx.new(|_| SlotOwner {
            slot: CommandSlot::new(),
        });
        let second_owner = cx.new(|_| SlotOwner {
            slot: CommandSlot::new(),
        });
        first_owner.update(cx, |owner, cx| {
            owner.slot.try_start(cx, |cx| command_from_cell(&first, cx));
        });
        second_owner.update(cx, |owner, cx| {
            owner
                .slot
                .try_start(cx, |cx| command_from_cell(&second, cx));
        });
        cx.run_until_parked();

        let first_notifications = Arc::new(AtomicUsize::new(0));
        let second_notifications = Arc::new(AtomicUsize::new(0));
        let first_owner_for_probe = first_owner.clone();
        let _first_probe = cx.new({
            let count = first_notifications.clone();
            move |cx| Probe {
                _observation: cx.observe(&first_owner_for_probe, move |_probe, _owner, _cx| {
                    count.fetch_add(1, Ordering::Relaxed);
                }),
            }
        });
        let _second_probe = cx.new({
            let count = second_notifications.clone();
            move |cx| Probe {
                _observation: cx.observe(&second_owner, move |_probe, _owner, _cx| {
                    count.fetch_add(1, Ordering::Relaxed);
                }),
            }
        });

        first_writer.set(Some(Ok(1)));
        cx.run_until_parked();
        assert!(first_owner.read_with(cx, |owner, cx| {
            matches!(owner.slot.state(cx), Some(CommandState::Success(value)) if **value == 1)
        }));
        assert_eq!(first_notifications.load(Ordering::Relaxed), 1);
        assert_eq!(second_notifications.load(Ordering::Relaxed), 0);
    }

    #[gpui::test]
    fn hooks_run_once_on_events_and_not_during_render(cx: &mut TestAppContext) {
        let writer = Cell::new(None::<Result<u32, String>>);
        let cell = writer.clone().lock();
        let command = cx.update(|cx| command_from_cell(&cell, cx));
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let hooks = CommandHooks::default()
            .on_pending({
                let events = events.clone();
                move |_| {
                    events
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push("pending".into());
                }
            })
            .on_success({
                let events = events.clone();
                move |value, _| {
                    events
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(format!("success:{value}"));
                }
            })
            .on_failed({
                let events = events.clone();
                move |error, _| {
                    events
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(format!("failed:{error}"));
                }
            });
        let _boundary = cx.update(|cx| {
            boundary_for_command(command, |_| gpui::div().into_any_element(), hooks, cx)
        });
        assert_eq!(
            *events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ["pending"]
        );

        writer.set(Some(Ok(9)));
        cx.run_until_parked();
        writer.set(Some(Err("ignored".into())));
        cx.run_until_parked();
        assert_eq!(
            *events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ["pending", "success:9"]
        );
    }

    #[gpui::test]
    fn seeded_failure_is_deferred_until_after_construction(cx: &mut TestAppContext) {
        let cell = Cell::new(Some(Err::<u32, _>("encode".to_string()))).lock();
        let command = cx.update(|cx| {
            let command = command_from_cell(&cell, cx);
            assert!(matches!(command.read(cx).state(), CommandState::Pending));
            command
        });
        cx.run_until_parked();
        assert!(matches!(
            command.read_with(cx, |command, _| command.state().clone()),
            CommandState::Failed(error) if error.as_ref() == "encode"
        ));
    }

    #[gpui::test]
    fn tracker_retains_overlapping_commands_without_suppressing_them(cx: &mut TestAppContext) {
        let first_writer = Cell::new(None::<Result<u32, String>>);
        let second_writer = Cell::new(None::<Result<u32, String>>);
        let (first, second) = cx.update(|cx| {
            (
                command_from_cell(&first_writer.clone().lock(), cx),
                command_from_cell(&second_writer.clone().lock(), cx),
            )
        });
        let owner = cx.new(|_| TrackerOwner {
            tracker: CommandTracker::new(),
            results: Vec::new(),
            errors: Vec::new(),
        });
        owner.update(cx, |owner, cx| {
            owner.tracker.track(first, cx, |owner, state, _| {
                if let CommandState::Success(value) = state {
                    owner.results.push(*value);
                }
            });
            owner.tracker.track(second, cx, |owner, state, _| {
                if let CommandState::Success(value) = state {
                    owner.results.push(*value);
                }
            });
            assert_eq!(owner.tracker.len(), 2);
            assert_eq!(owner.tracker.pending_count(cx), 2);
        });

        second_writer.set(Some(Ok(2)));
        first_writer.set(Some(Ok(1)));
        cx.run_until_parked();

        owner.read_with(cx, |owner, cx| {
            let mut results = owner.results.clone();
            results.sort_unstable();
            assert_eq!(results, [1, 2]);
            assert_eq!(owner.tracker.pending_count(cx), 0);
        });
    }

    #[gpui::test]
    fn tracker_failure_callback_ignores_success_and_reports_errors(cx: &mut TestAppContext) {
        let success_writer = Cell::new(None::<Result<u32, String>>);
        let failure_writer = Cell::new(None::<Result<u32, String>>);
        let (success, failure) = cx.update(|cx| {
            (
                command_from_cell(&success_writer.clone().lock(), cx),
                command_from_cell(&failure_writer.clone().lock(), cx),
            )
        });
        let owner = cx.new(|_| TrackerOwner {
            tracker: CommandTracker::new(),
            results: Vec::new(),
            errors: Vec::new(),
        });
        owner.update(cx, |owner, cx| {
            owner.tracker.track_failure(success, cx, |owner, error, _| {
                owner.errors.push(error.to_string());
            });
            owner.tracker.track_failure(failure, cx, |owner, error, _| {
                owner.errors.push(error.to_string());
            });
        });

        success_writer.set(Some(Ok(7)));
        failure_writer.set(Some(Err("network".into())));
        cx.run_until_parked();

        owner.read_with(cx, |owner, _| {
            assert_eq!(owner.errors, ["network"]);
        });
    }
}
