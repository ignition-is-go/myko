//! Durable history query and replay support.

use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use crate::{
    TS,
    common::with_id::{WithId, WithTypedId},
    core::capability::HistoryReading,
    hyphae::{Cell, CellImmutable, CellMutable, Mutable, Signal, Watchable},
    item::{AnyItem, Eventable},
    query::{QueryHandler, QueryWindowBuildArgs, WindowedQuerySnapshot, WindowedQuerySource},
    server::HandlerRegistry,
    store::StoreRegistry,
    wire::{MEvent, QueryWindow},
};

/// A durable event returned by an entity-history lookup.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct HistoryEvent {
    pub id: i64,
    pub created_at: String,
    pub event: MEvent,
}

crate::register_typegen_type!(HistoryEvent);

impl WithId for HistoryEvent {
    fn id(&self) -> Arc<str> {
        self.id.to_string().into()
    }
}

impl WithTypedId for HistoryEvent {
    type Id = i64;

    fn typed_id(&self) -> Self::Id {
        self.id
    }
}

impl AnyItem for HistoryEvent {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn entity_type(&self) -> &'static str {
        Self::ENTITY_NAME_STATIC
    }

    fn equals(&self, other: &dyn AnyItem) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
}

impl Eventable for HistoryEvent {
    const ENTITY_NAME_STATIC: &'static str = "HistoryEvent";
}

/// One bounded page from an entity's complete durable history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryPage {
    pub events: Vec<HistoryEvent>,
    pub total_count: usize,
}

/// Typed identity of an entity whose durable event history is queried.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HistoryEntityKey {
    pub item_type: Arc<str>,
    pub item_id: Arc<str>,
}

/// A committed `PostgreSQL` history row observed through the backend's notify stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedHistoryEvent {
    pub key: HistoryEntityKey,
    pub row_id: i64,
}

impl HistoryEntityKey {
    #[must_use]
    pub fn new(item_type: impl Into<Arc<str>>, item_id: impl Into<Arc<str>>) -> Self {
        Self {
            item_type: item_type.into(),
            item_id: item_id.into(),
        }
    }
}

/// Live, server-windowed durable history for one entity, newest first.
#[myko_macros::myko_query(HistoryEvent)]
pub struct EntityHistory {
    pub item_type: String,
    pub item_id: Arc<str>,
}

impl EntityHistory {
    /// Build a history query from the entity's registered Rust type rather
    /// than duplicating its wire name at the call site.
    #[must_use]
    pub fn for_entity<T: Eventable>(item_id: impl Into<Arc<str>>) -> Self {
        Self {
            item_type: T::ENTITY_NAME_STATIC.to_string(),
            item_id: item_id.into(),
        }
    }
}

impl QueryHandler for EntityHistory {
    fn build_window(
        ctx: QueryWindowBuildArgs<Self>,
    ) -> Result<Option<WindowedQuerySource>, String> {
        let key = HistoryEntityKey::new(ctx.query.item_type.clone(), ctx.query.item_id.clone());
        Ok(Some(entity_history_window_source(
            ctx.query_context,
            key,
            ctx.window,
        )?))
    }
}

fn refresh_history_window(
    context: &crate::query::QueryBuildContext,
    key: &HistoryEntityKey,
    selection: &Mutex<QueryWindow>,
    snapshots: &Cell<Arc<WindowedQuerySnapshot>, CellMutable>,
) -> Result<(), String> {
    let window = selection
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let page = context.entity_history_page(key, &window)?;
    let entries = page
        .events
        .into_iter()
        .map(|event| {
            let id = event.id();
            let event: Arc<dyn AnyItem> = Arc::new(event);
            (id, event)
        })
        .collect();
    snapshots.set(Arc::new(WindowedQuerySnapshot {
        entries,
        total_count: page.total_count,
        window: Some(window),
    }));
    Ok(())
}

fn entity_history_window_source(
    context: crate::query::QueryBuildContext,
    key: HistoryEntityKey,
    initial_window: QueryWindow,
) -> Result<WindowedQuerySource, String> {
    let snapshots = Cell::new(Arc::new(WindowedQuerySnapshot {
        entries: Vec::new(),
        total_count: 0,
        window: Some(initial_window.clone()),
    }));
    let selection = Arc::new(Mutex::new(initial_window));
    let dispatch = Arc::new(Mutex::new(()));

    let snapshots_weak = snapshots.downgrade();
    let context_for_commits = context.clone();
    let key_for_commits = key.clone();
    let selection_for_commits = selection.clone();
    let dispatch_for_commits = dispatch.clone();
    let guard = context.committed_history_event().subscribe(move |signal| {
        let Signal::Value(committed) = signal else {
            return;
        };
        let Some(committed) = committed.as_ref() else {
            return;
        };
        if committed.key != key_for_commits {
            return;
        }
        let _dispatch = dispatch_for_commits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(snapshots) = snapshots_weak.upgrade() else {
            return;
        };
        if let Err(error) = refresh_history_window(
            &context_for_commits,
            &key_for_commits,
            &selection_for_commits,
            &snapshots,
        ) {
            tracing::warn!(%error, "could not refresh entity history window");
        }
    });
    snapshots.own(guard);

    {
        let _dispatch = dispatch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        refresh_history_window(&context, &key, &selection, &snapshots)?;
    }

    let snapshots_for_window = snapshots.downgrade();
    let context_for_window = context;
    let key_for_window = key;
    let selection_for_window = selection;
    let dispatch_for_window = dispatch;
    let set_window = move |next: Option<QueryWindow>| {
        let Some(next) = next else {
            return;
        };
        let _dispatch = dispatch_for_window
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *selection_for_window
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        let Some(snapshots) = snapshots_for_window.upgrade() else {
            return;
        };
        if let Err(error) = refresh_history_window(
            &context_for_window,
            &key_for_window,
            &selection_for_window,
            &snapshots,
        ) {
            tracing::warn!(%error, "could not move entity history window");
        }
    };

    Ok(WindowedQuerySource::new(snapshots.lock(), set_window))
}

/// Provider for replaying historical events into a temporary `StoreRegistry`.
///
/// Implemented by the server layer (e.g., `PostgresHistoryReplayProvider`)
/// to enable point-in-time snapshots without coupling myko to a
/// specific persistence backend.
pub trait HistoryReplayProvider: Send + Sync {
    /// Latest committed history row observed through the backend notification stream.
    fn committed_history_event(&self) -> Cell<Option<Arc<CommittedHistoryEvent>>, CellImmutable> {
        Cell::new(None).lock()
    }

    /// Load one window from an entity's durable history, newest first.
    ///
    /// # Errors
    ///
    /// Returns an error when history is unsupported or the page cannot be
    /// read.
    fn entity_history_page(
        &self,
        key: &HistoryEntityKey,
        window: &QueryWindow,
    ) -> Result<HistoryPage, String> {
        let _ = (key, window);
        Err("Entity history is not supported by this provider".to_string())
    }

    /// Replay all events with `created_at <= until` into a fresh `StoreRegistry`.
    ///
    /// `until` is an ISO 8601 timestamp string.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn replay_to_store(
        &self,
        until: &str,
        handler_registry: &HandlerRegistry,
    ) -> Result<Arc<StoreRegistry>, String>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use hyphae::Gettable;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::{
        query::{QueryBuildContext, QueryContext},
        request::RequestContext,
        search::SearchIndex,
        server::{
            HandlerRegistry, MykoServerContext, MykoServerRuntime, PersisterRouter,
            RelationshipManager,
        },
        wire::MEventType,
    };

    struct TestHistoryProvider {
        rows: Mutex<Vec<HistoryEvent>>,
        committed: Cell<Option<Arc<CommittedHistoryEvent>>, CellMutable>,
        reads: AtomicUsize,
    }

    impl TestHistoryProvider {
        fn new(rows: Vec<HistoryEvent>) -> Self {
            Self {
                rows: Mutex::new(rows),
                committed: Cell::new(None),
                reads: AtomicUsize::new(0),
            }
        }

        fn commit(&self, key: HistoryEntityKey, event: HistoryEvent) {
            let row_id = event.id;
            self.rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(0, event);
            self.committed
                .set(Some(Arc::new(CommittedHistoryEvent { key, row_id })));
        }
    }

    impl HistoryReplayProvider for TestHistoryProvider {
        fn committed_history_event(
            &self,
        ) -> Cell<Option<Arc<CommittedHistoryEvent>>, CellImmutable> {
            self.committed.clone().lock()
        }

        fn entity_history_page(
            &self,
            _key: &HistoryEntityKey,
            window: &QueryWindow,
        ) -> Result<HistoryPage, String> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let rows = self
                .rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let total_count = rows.len();
            let start = window.offset.min(total_count);
            let end = start.saturating_add(window.limit).min(total_count);
            Ok(HistoryPage {
                events: rows[start..end].to_vec(),
                total_count,
            })
        }

        fn replay_to_store(
            &self,
            _until: &str,
            _handler_registry: &HandlerRegistry,
        ) -> Result<Arc<StoreRegistry>, String> {
            Err("unused in history-window tests".to_string())
        }
    }

    fn history_event(id: i64) -> HistoryEvent {
        HistoryEvent {
            id,
            created_at: format!("event-{id}"),
            event: MEvent {
                item: json!({ "id": "entity-1", "value": id }),
                change_type: MEventType::SET,
                item_type: "TestEntity".into(),
                created_at: format!("event-{id}").into(),
                tx: format!("tx-{id}").into(),
                source_id: None,
            },
        }
    }

    fn query_context(provider: Arc<TestHistoryProvider>) -> QueryBuildContext {
        let host_id = Uuid::new_v4();
        let registry = Arc::new(StoreRegistry::new());
        let server = Arc::new(MykoServerContext::new(
            host_id,
            registry.clone(),
            Arc::new(HandlerRegistry::new()),
            Arc::new(RelationshipManager::new()),
            Arc::new(PersisterRouter::default()),
            Arc::new(SearchIndex::new()),
            MykoServerRuntime {
                peer_clients: Arc::new(dashmap::DashMap::new()),
                event_sink: None,
                history_replay: Some(provider),
            },
        ));
        QueryBuildContext::new(
            Arc::new(QueryContext {
                req: Arc::new(RequestContext::from_client(
                    "history-test".into(),
                    "client-test".into(),
                    host_id,
                )),
            }),
            registry,
            Some(server),
        )
    }

    #[test]
    fn history_window_pages_at_the_provider_and_refreshes_matching_commits() {
        let provider = Arc::new(TestHistoryProvider::new(vec![
            history_event(3),
            history_event(2),
            history_event(1),
        ]));
        let key = HistoryEntityKey::new("TestEntity", "entity-1");
        let source = entity_history_window_source(
            query_context(provider.clone()),
            key.clone(),
            QueryWindow {
                offset: 0,
                limit: 2,
            },
        )
        .expect("history source");

        let initial = source.snapshots().get();
        assert_eq!(initial.total_count, 3);
        assert_eq!(
            initial
                .entries
                .iter()
                .map(|(id, _)| id.as_ref())
                .collect::<Vec<_>>(),
            vec!["3", "2"]
        );
        assert_eq!(provider.reads.load(Ordering::Relaxed), 1);

        source.set_window(Some(QueryWindow {
            offset: 2,
            limit: 2,
        }));
        let older = source.snapshots().get();
        assert_eq!(
            older
                .entries
                .iter()
                .map(|(id, _)| id.as_ref())
                .collect::<Vec<_>>(),
            vec!["1"]
        );

        provider.committed.set(Some(Arc::new(CommittedHistoryEvent {
            key: HistoryEntityKey::new("OtherEntity", "entity-1"),
            row_id: 99,
        })));
        assert_eq!(provider.reads.load(Ordering::Relaxed), 2);

        provider.commit(key, history_event(4));
        let refreshed = source.snapshots().get();
        assert_eq!(refreshed.total_count, 4);
        assert_eq!(
            refreshed
                .entries
                .iter()
                .map(|(id, _)| id.as_ref())
                .collect::<Vec<_>>(),
            vec!["2", "1"]
        );
        assert_eq!(provider.reads.load(Ordering::Relaxed), 3);
    }
}
