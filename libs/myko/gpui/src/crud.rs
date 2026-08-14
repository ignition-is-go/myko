use std::{collections::HashMap, sync::Arc};

use gpui::{App, AppContext as _, Context, Entity, Subscription, Window};
use myko::hyphae::CellValue;

use crate::{Command, CommandState};

type CreateFactory<I, R> = dyn Fn(I, &mut App) -> Entity<Command<R>> + Send + Sync + 'static;
type CreateInputProvider<I> = dyn Fn(&mut Window, &mut App) -> Option<I> + Send + Sync + 'static;
type RenameFactory<T, I, R> =
    dyn Fn(Arc<T>, I, &mut App) -> Entity<Command<R>> + Send + Sync + 'static;
type DeleteFactory<T, R> = dyn Fn(Arc<T>, &mut App) -> Entity<Command<R>> + Send + Sync + 'static;

/// The command capabilities available to a [`CrudController`].
///
/// Factories are deliberately independent of a particular Myko command type.
/// They can call [`crate::command`] themselves, which keeps the controller
/// useful when create, rename, and delete use unrelated request types.
pub struct CrudCommands<
    T,
    CreateInput,
    RenameInput,
    CreateResult: CellValue,
    RenameResult: CellValue,
    DeleteResult: CellValue,
> {
    create: Option<Arc<CreateFactory<CreateInput, CreateResult>>>,
    create_input: Option<Arc<CreateInputProvider<CreateInput>>>,
    rename: Option<Arc<RenameFactory<T, RenameInput, RenameResult>>>,
    delete: Option<Arc<DeleteFactory<T, DeleteResult>>>,
}

impl<T, CreateInput, RenameInput, CR: CellValue, RR: CellValue, DR: CellValue> Clone
    for CrudCommands<T, CreateInput, RenameInput, CR, RR, DR>
{
    fn clone(&self) -> Self {
        Self {
            create: self.create.clone(),
            create_input: self.create_input.clone(),
            rename: self.rename.clone(),
            delete: self.delete.clone(),
        }
    }
}

impl<T, CreateInput, RenameInput, CR: CellValue, RR: CellValue, DR: CellValue> Default
    for CrudCommands<T, CreateInput, RenameInput, CR, RR, DR>
{
    fn default() -> Self {
        Self {
            create: None,
            create_input: None,
            rename: None,
            delete: None,
        }
    }
}

impl<T, CreateInput, RenameInput, CR: CellValue, RR: CellValue, DR: CellValue>
    CrudCommands<T, CreateInput, RenameInput, CR, RR, DR>
{
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_create(
        mut self,
        factory: impl Fn(CreateInput, &mut App) -> Entity<Command<CR>> + Send + Sync + 'static,
    ) -> Self {
        self.create = Some(Arc::new(factory));
        self
    }

    /// Configures an application-owned source for create input.
    ///
    /// Returning `None` represents cancellation and does not create a command.
    #[must_use]
    pub fn with_create_input(
        mut self,
        provider: impl Fn(&mut Window, &mut App) -> Option<CreateInput> + Send + Sync + 'static,
    ) -> Self {
        self.create_input = Some(Arc::new(provider));
        self
    }

    /// Alias for [`Self::with_create_input`] emphasizing prompt-backed use.
    #[must_use]
    pub fn with_create_provider(
        self,
        provider: impl Fn(&mut Window, &mut App) -> Option<CreateInput> + Send + Sync + 'static,
    ) -> Self {
        self.with_create_input(provider)
    }

    #[must_use]
    pub fn with_rename(
        mut self,
        factory: impl Fn(Arc<T>, RenameInput, &mut App) -> Entity<Command<RR>> + Send + Sync + 'static,
    ) -> Self {
        self.rename = Some(Arc::new(factory));
        self
    }

    #[must_use]
    pub fn with_delete(
        mut self,
        factory: impl Fn(Arc<T>, &mut App) -> Entity<Command<DR>> + Send + Sync + 'static,
    ) -> Self {
        self.delete = Some(Arc::new(factory));
        self
    }

    #[must_use]
    pub const fn can_create(&self) -> bool {
        self.create.is_some()
    }

    #[must_use]
    pub const fn can_rename(&self) -> bool {
        self.rename.is_some()
    }

    #[must_use]
    pub const fn can_delete(&self) -> bool {
        self.delete.is_some()
    }
}

/// Independently observable command slots for one query row.
///
/// Observe this entity from a row component to redraw only that row. Terminal
/// commands remain available for error/success rendering and are replaced by
/// the next invocation of the same operation.
pub struct CrudRowActions<RR: CellValue, DR: CellValue> {
    rename: Option<Entity<Command<RR>>>,
    delete: Option<Entity<Command<DR>>>,
    rename_observation: Option<Subscription>,
    delete_observation: Option<Subscription>,
}

impl<RR: CellValue, DR: CellValue> CrudRowActions<RR, DR> {
    const fn new() -> Self {
        Self {
            rename: None,
            delete: None,
            rename_observation: None,
            delete_observation: None,
        }
    }

    #[must_use]
    pub fn rename_command(&self) -> Option<Entity<Command<RR>>> {
        self.rename.clone()
    }

    #[must_use]
    pub fn delete_command(&self) -> Option<Entity<Command<DR>>> {
        self.delete.clone()
    }

    #[must_use]
    pub fn rename_state<'a>(&self, cx: &'a App) -> Option<&'a CommandState<RR>> {
        self.rename.as_ref().map(|command| command.read(cx).state())
    }

    #[must_use]
    pub fn delete_state<'a>(&self, cx: &'a App) -> Option<&'a CommandState<DR>> {
        self.delete.as_ref().map(|command| command.read(cx).state())
    }

    fn rename_pending(&self, cx: &App) -> bool {
        self.rename
            .as_ref()
            .is_some_and(|command| command.read(cx).is_pending())
    }

    fn delete_pending(&self, cx: &App) -> bool {
        self.delete
            .as_ref()
            .is_some_and(|command| command.read(cx).is_pending())
    }

    fn start_rename(&mut self, command: Entity<Command<RR>>, cx: &mut Context<Self>) {
        self.rename_observation = Some(cx.observe(&command, |_row, _command, cx| cx.notify()));
        self.rename = Some(command);
        cx.notify();
    }

    fn start_delete(&mut self, command: Entity<Command<DR>>, cx: &mut Context<Self>) {
        self.delete_observation = Some(cx.observe(&command, |_row, _command, cx| cx.notify()));
        self.delete = Some(command);
        cx.notify();
    }
}

/// Styling-agnostic CRUD command coordinator for a live query UI.
///
/// Store this value in a GPUI entity. `create` changes notify the controller;
/// rename/delete changes notify the stable [`CrudRowActions`] entity for their
/// key, avoiding collection-wide redraws. Use [`crate::on_command_change`] or
/// [`crate::observe_command_in`] with the returned command entities for toasts,
/// navigation, cache changes, and other lifecycle side effects.
pub struct CrudController<
    T,
    CreateInput,
    RenameInput,
    CreateResult: CellValue,
    RenameResult: CellValue,
    DeleteResult: CellValue,
> {
    commands: CrudCommands<T, CreateInput, RenameInput, CreateResult, RenameResult, DeleteResult>,
    create: Option<Entity<Command<CreateResult>>>,
    create_observation: Option<Subscription>,
    rows: HashMap<Arc<str>, Entity<CrudRowActions<RenameResult, DeleteResult>>>,
}

impl<T, CreateInput, RenameInput, CR: CellValue, RR: CellValue, DR: CellValue>
    CrudController<T, CreateInput, RenameInput, CR, RR, DR>
{
    #[must_use]
    pub fn new(commands: CrudCommands<T, CreateInput, RenameInput, CR, RR, DR>) -> Self {
        Self {
            commands,
            create: None,
            create_observation: None,
            rows: HashMap::new(),
        }
    }

    #[must_use]
    pub const fn can_create(&self) -> bool {
        self.commands.can_create()
    }

    #[must_use]
    pub const fn can_rename(&self) -> bool {
        self.commands.can_rename()
    }

    #[must_use]
    pub const fn can_delete(&self) -> bool {
        self.commands.can_delete()
    }

    /// Starts create, returning `false` when unsupported or already pending.
    pub fn create(&mut self, input: CreateInput, cx: &mut Context<Self>) -> bool
    where
        T: 'static,
        CreateInput: 'static,
        RenameInput: 'static,
    {
        let Some(factory) = self.commands.create.clone() else {
            return false;
        };
        if self
            .create
            .as_ref()
            .is_some_and(|command| command.read(cx).is_pending())
        {
            return false;
        }
        let command = factory(input, cx);
        self.create_observation = Some(cx.observe(&command, |_controller, _command, cx| {
            cx.notify();
        }));
        self.create = Some(command);
        cx.notify();
        true
    }

    /// Obtains input from the configured provider and starts create.
    ///
    /// This is intended for application-owned forms or modal prompts. It
    /// returns `false` when no provider exists, the provider cancels, create is
    /// unsupported, or a create command is already pending.
    pub fn create_from_provider(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool
    where
        T: 'static,
        CreateInput: 'static,
        RenameInput: 'static,
    {
        if !self.can_create()
            || self
                .create
                .as_ref()
                .is_some_and(|command| command.read(cx).is_pending())
        {
            return false;
        }
        let Some(provider) = self.commands.create_input.clone() else {
            return false;
        };
        let Some(input) = provider(window, cx) else {
            return false;
        };
        self.create(input, cx)
    }

    /// Alias for [`Self::create_from_provider`] for prompt-oriented callers.
    pub fn prompt_create(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool
    where
        T: 'static,
        CreateInput: 'static,
        RenameInput: 'static,
    {
        self.create_from_provider(window, cx)
    }

    /// Starts rename for `key`, returning `false` when unsupported or pending.
    pub fn rename(
        &mut self,
        key: Arc<str>,
        item: Arc<T>,
        input: RenameInput,
        cx: &mut Context<Self>,
    ) -> bool
    where
        T: 'static,
        CreateInput: 'static,
        RenameInput: 'static,
    {
        let Some(factory) = self.commands.rename.clone() else {
            return false;
        };
        if self
            .rows
            .get(&key)
            .is_some_and(|row| row.read(cx).rename_pending(cx))
        {
            return false;
        }
        let command = factory(item, input, cx);
        let row = self.row_actions_for(key, cx);
        row.update(cx, |row, cx| row.start_rename(command, cx));
        true
    }

    /// Starts delete for `key`, returning `false` when unsupported or pending.
    pub fn delete(&mut self, key: Arc<str>, item: Arc<T>, cx: &mut Context<Self>) -> bool
    where
        T: 'static,
        CreateInput: 'static,
        RenameInput: 'static,
    {
        let Some(factory) = self.commands.delete.clone() else {
            return false;
        };
        if self
            .rows
            .get(&key)
            .is_some_and(|row| row.read(cx).delete_pending(cx))
        {
            return false;
        }
        let command = factory(item, cx);
        let row = self.row_actions_for(key, cx);
        row.update(cx, |row, cx| row.start_delete(command, cx));
        true
    }

    /// Returns the stable action slots for `key`, creating them when needed.
    ///
    /// Row components can call this during construction and observe the returned
    /// entity once, before any action has started. Rename and delete reuse the
    /// same entity until [`Self::retain_row_actions`] removes the key.
    pub fn row_actions_for(
        &mut self,
        key: Arc<str>,
        cx: &mut Context<Self>,
    ) -> Entity<CrudRowActions<RR, DR>>
    where
        T: 'static,
        CreateInput: 'static,
        RenameInput: 'static,
    {
        self.rows
            .entry(key)
            .or_insert_with(|| cx.new(|_| CrudRowActions::new()))
            .clone()
    }

    #[must_use]
    pub fn create_command(&self) -> Option<Entity<Command<CR>>> {
        self.create.clone()
    }

    #[must_use]
    pub fn create_state<'a>(&self, cx: &'a App) -> Option<&'a CommandState<CR>> {
        self.create.as_ref().map(|command| command.read(cx).state())
    }

    #[must_use]
    pub fn row_actions(&self, key: &str) -> Option<Entity<CrudRowActions<RR, DR>>> {
        self.rows.get(key).cloned()
    }

    /// Discards action slots for keys no longer present in the authoritative query.
    ///
    /// Call this from the query store's membership observation. Rows that remain
    /// keep the same action entity, while a later reinsertion starts with fresh
    /// operation state.
    pub fn retain_row_actions(&mut self, keys: &[Arc<str>]) {
        self.rows
            .retain(|key, _| keys.iter().any(|candidate| candidate == key));
    }

    #[must_use]
    pub fn rename_command(&self, key: &str, cx: &App) -> Option<Entity<Command<RR>>> {
        self.rows
            .get(key)
            .and_then(|row| row.read_with(cx, |row, _| row.rename_command()))
    }

    #[must_use]
    pub fn delete_command(&self, key: &str, cx: &App) -> Option<Entity<Command<DR>>> {
        self.rows
            .get(key)
            .and_then(|row| row.read_with(cx, |row, _| row.delete_command()))
    }

    #[must_use]
    pub fn rename_state(&self, key: &str, cx: &App) -> Option<CommandState<RR>> {
        self.rename_command(key, cx)
            .map(|command| command.read(cx).state().clone())
    }

    #[must_use]
    pub fn delete_state(&self, key: &str, cx: &App) -> Option<CommandState<DR>> {
        self.delete_command(key, cx)
            .map(|command| command.read(cx).state().clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use gpui::{AppContext as _, Subscription, TestAppContext};
    use myko::hyphae::{Cell, Mutable as _};

    use super::{CrudCommands, CrudController};
    use crate::command::command_from_cell;

    struct Probe {
        _observation: Subscription,
    }

    #[gpui::test]
    fn capabilities_and_pending_slots_are_enforced(cx: &mut TestAppContext) {
        let calls = Arc::new(AtomicUsize::new(0));
        let writer = Cell::new(None::<Result<u32, String>>);
        let commands = CrudCommands::<u32, u32, String, u32, bool, String>::new().with_create({
            let calls = calls.clone();
            let cell = writer.clone().lock();
            move |_, cx| {
                calls.fetch_add(1, Ordering::Relaxed);
                command_from_cell(&cell, cx)
            }
        });
        assert!(commands.can_create());
        assert!(!commands.can_rename());
        assert!(!commands.can_delete());

        let controller = cx.new(|_| CrudController::new(commands));
        assert!(controller.update(cx, |controller, cx| controller.create(1, cx)));
        assert!(!controller.update(cx, |controller, cx| controller.create(2, cx)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(controller.read_with(cx, |controller, cx| controller.create_state(cx).is_some()));

        writer.set(Some(Ok(1)));
        cx.run_until_parked();
        assert!(controller.update(cx, |controller, cx| controller.create(3, cx)));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[gpui::test]
    fn row_operations_are_independent(cx: &mut TestAppContext) {
        let rename_writer = Cell::new(None::<Result<u32, String>>);
        let delete_writer = Cell::new(None::<Result<String, String>>);
        let commands = CrudCommands::<u32, (), (), bool, u32, String>::new()
            .with_rename({
                let cell = rename_writer.lock();
                move |_, (), cx| command_from_cell(&cell, cx)
            })
            .with_delete({
                let cell = delete_writer.lock();
                move |_, cx| command_from_cell(&cell, cx)
            });
        let controller = cx.new(|_| CrudController::new(commands));
        let key: Arc<str> = "one".into();
        assert!(controller.update(cx, |controller, cx| {
            controller.rename(key.clone(), Arc::new(1), (), cx)
        }));
        assert!(!controller.update(cx, |controller, cx| {
            controller.rename(key.clone(), Arc::new(1), (), cx)
        }));
        let actions = controller.read_with(cx, |controller, _| controller.row_actions("one"));
        let actions_found = actions.is_some();
        let Some(actions) = actions else {
            assert!(actions_found, "rename must create row actions");
            return;
        };
        assert!(controller.update(cx, |controller, cx| {
            controller.delete(key.clone(), Arc::new(1), cx)
        }));
        let actions_after_delete =
            controller.read_with(cx, |controller, _| controller.row_actions("one"));
        let actions_after_delete_found = actions_after_delete.is_some();
        let Some(actions_after_delete) = actions_after_delete else {
            assert!(actions_after_delete_found, "delete must keep row actions");
            return;
        };
        assert_eq!(actions.entity_id(), actions_after_delete.entity_id());
        assert!(actions.read_with(cx, |row, cx| {
            row.rename_state(cx).is_some() && row.delete_state(cx).is_some()
        }));
    }

    #[gpui::test]
    fn command_updates_notify_only_the_affected_row(cx: &mut TestAppContext) {
        let first_writer = Cell::new(None::<Result<u32, String>>);
        let second_writer = Cell::new(None::<Result<u32, String>>);
        let commands = CrudCommands::<u32, (), (), bool, u32, String>::new().with_rename({
            let first = first_writer.clone().lock();
            let second = second_writer.lock();
            move |item, (), cx| {
                if *item == 1 {
                    command_from_cell(&first, cx)
                } else {
                    command_from_cell(&second, cx)
                }
            }
        });
        let controller = cx.new(|_| CrudController::new(commands));
        let first_key: Arc<str> = "one".into();
        let second_key: Arc<str> = "two".into();
        assert!(controller.update(cx, |controller, cx| {
            controller.rename(first_key.clone(), Arc::new(1), (), cx)
        }));
        assert!(controller.update(cx, |controller, cx| {
            controller.rename(second_key.clone(), Arc::new(2), (), cx)
        }));
        let first = controller.read_with(cx, |controller, _| controller.row_actions("one"));
        let first_found = first.is_some();
        let Some(first) = first else {
            assert!(first_found, "first row actions must exist");
            return;
        };
        let second = controller.read_with(cx, |controller, _| controller.row_actions("two"));
        let second_found = second.is_some();
        let Some(second) = second else {
            assert!(second_found, "second row actions must exist");
            return;
        };

        let first_notifications = Arc::new(AtomicUsize::new(0));
        let second_notifications = Arc::new(AtomicUsize::new(0));
        let controller_notifications = Arc::new(AtomicUsize::new(0));
        let _first_probe = cx.new({
            let count = first_notifications.clone();
            move |cx| Probe {
                _observation: cx.observe(&first, move |_probe, _row, _cx| {
                    count.fetch_add(1, Ordering::Relaxed);
                }),
            }
        });
        let _second_probe = cx.new({
            let count = second_notifications.clone();
            move |cx| Probe {
                _observation: cx.observe(&second, move |_probe, _row, _cx| {
                    count.fetch_add(1, Ordering::Relaxed);
                }),
            }
        });
        let _controller_probe = cx.new({
            let count = controller_notifications.clone();
            let controller = controller.clone();
            move |cx| Probe {
                _observation: cx.observe(&controller, move |_probe, _controller, _cx| {
                    count.fetch_add(1, Ordering::Relaxed);
                }),
            }
        });

        first_writer.set(Some(Ok(10)));
        cx.run_until_parked();
        assert_eq!(first_notifications.load(Ordering::Relaxed), 1);
        assert_eq!(second_notifications.load(Ordering::Relaxed), 0);
        assert_eq!(controller_notifications.load(Ordering::Relaxed), 0);
    }

    #[gpui::test]
    fn removed_keys_prune_action_slots_without_replacing_retained_rows(cx: &mut TestAppContext) {
        let writer = Cell::new(None::<Result<u32, String>>);
        let commands = CrudCommands::<u32, (), (), bool, u32, String>::new().with_rename({
            let cell = writer.lock();
            move |_, (), cx| command_from_cell(&cell, cx)
        });
        let controller = cx.new(|_| CrudController::new(commands));
        for (key, item) in [(Arc::<str>::from("one"), 1), (Arc::<str>::from("two"), 2)] {
            assert!(controller.update(cx, |controller, cx| {
                controller.rename(key, Arc::new(item), (), cx)
            }));
        }
        assert!(
            controller
                .read_with(cx, |controller, _| controller.row_actions("one"))
                .is_some()
        );
        let retained = controller.read_with(cx, |controller, _| controller.row_actions("two"));
        let retained_found = retained.is_some();
        let Some(retained) = retained else {
            assert!(retained_found, "retained row actions must exist");
            return;
        };

        controller.update(cx, |controller, _cx| {
            controller.retain_row_actions(&[Arc::from("two")]);
        });

        assert!(
            controller
                .read_with(cx, |controller, _| controller.row_actions("one"))
                .is_none()
        );
        assert_eq!(
            controller
                .read_with(cx, |controller, _| controller.row_actions("two"))
                .map(|actions| actions.entity_id()),
            Some(retained.entity_id())
        );
    }
}
