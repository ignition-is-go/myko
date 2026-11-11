use gpui::{AnyElement, App, Entity, IntoElement, RenderOnce, Window};

use crate::{LoadState, Remote};

/// Styling-agnostic state renderer. Each branch may return a different GPUI
/// element type; the selected branch is erased to `AnyElement`.
pub fn render_remote<T, L, R, E, LE, RE, EE>(
    remote: &Entity<Remote<T>>,
    cx: &App,
    loading: L,
    ready: R,
    error: E,
) -> AnyElement
where
    T: Send + Sync + 'static,
    L: FnOnce(Option<&T>) -> LE,
    R: FnOnce(&T) -> RE,
    E: FnOnce(&str, Option<&T>) -> EE,
    LE: IntoElement,
    RE: IntoElement,
    EE: IntoElement,
{
    match remote.read(cx).state() {
        LoadState::Loading { stale } => loading(stale.as_deref()).into_any_element(),
        LoadState::Ready(value) => ready(value).into_any_element(),
        LoadState::Error { message, stale } => error(message, stale.as_deref()).into_any_element(),
    }
}

/// List-specific renderer adding an explicit successfully-loaded empty branch.
pub fn render_remote_list<T, L, E, Empty, Ready, LE, EE, EmptyE, ReadyE>(
    remote: &Entity<Remote<Vec<T>>>,
    cx: &App,
    loading: L,
    error: E,
    empty: Empty,
    ready: Ready,
) -> AnyElement
where
    T: Send + Sync + 'static,
    L: FnOnce(Option<&[T]>) -> LE,
    E: FnOnce(&str, Option<&[T]>) -> EE,
    Empty: FnOnce() -> EmptyE,
    Ready: FnOnce(&[T]) -> ReadyE,
    LE: IntoElement,
    EE: IntoElement,
    EmptyE: IntoElement,
    ReadyE: IntoElement,
{
    match remote.read(cx).state() {
        LoadState::Loading { stale } => {
            loading(stale.as_deref().map(Vec::as_slice)).into_any_element()
        }
        LoadState::Ready(items) if items.is_empty() => empty().into_any_element(),
        LoadState::Ready(items) => ready(items).into_any_element(),
        LoadState::Error { message, stale } => {
            error(message, stale.as_deref().map(Vec::as_slice)).into_any_element()
        }
    }
}

/// Retained GPUI component form of a remote state renderer.
pub struct RemoteRender<T, L, R, E>
where
    T: Send + Sync + 'static,
{
    pub remote: Entity<Remote<T>>,
    pub loading: L,
    pub ready: R,
    pub error: E,
}

impl<T, L, R, E, LE, RE, EE> RenderOnce for RemoteRender<T, L, R, E>
where
    T: Send + Sync + 'static,
    L: FnOnce(Option<&T>, &mut Window, &mut App) -> LE + 'static,
    R: FnOnce(&T, &mut Window, &mut App) -> RE + 'static,
    E: FnOnce(&str, Option<&T>, &mut Window, &mut App) -> EE + 'static,
    LE: IntoElement,
    RE: IntoElement,
    EE: IntoElement,
{
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        enum Snapshot<T> {
            Loading(Option<std::sync::Arc<T>>),
            Ready(std::sync::Arc<T>),
            Error(std::sync::Arc<str>, Option<std::sync::Arc<T>>),
        }
        let snapshot = match self.remote.read(cx).state() {
            LoadState::Loading { stale } => Snapshot::Loading(stale.clone()),
            LoadState::Ready(value) => Snapshot::Ready(value.clone()),
            LoadState::Error { message, stale } => Snapshot::Error(message.clone(), stale.clone()),
        };
        match snapshot {
            Snapshot::Loading(stale) => {
                (self.loading)(stale.as_deref(), window, cx).into_any_element()
            }
            Snapshot::Ready(value) => (self.ready)(&value, window, cx).into_any_element(),
            Snapshot::Error(message, stale) => {
                (self.error)(&message, stale.as_deref(), window, cx).into_any_element()
            }
        }
    }
}
