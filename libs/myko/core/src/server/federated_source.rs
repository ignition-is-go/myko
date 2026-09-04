//! Federation-backed sources for the retained reactive map runtime.

use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use hyphae::{Cell, CellImmutable, CellMap, CellMutable, Gettable as _, MapDiff, Mutable as _};
use myko_federation::{
    EventEnvelope, LogPosition, MutationOperation, Node, NodeEvent, NodeId, ScopeId,
    ScopeSelection, ScopeTopology, ServiceId, SubscriptionLiveness,
};

use crate::{
    MykoItem,
    item::{AnyItem, Eventable},
    query::FilteredCellMap,
};

/// Durable source selection supplied once at the retained registration edge.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FederatedRequest {
    pub source_node: Option<NodeId>,
    pub scope_id: Option<ScopeId>,
}

/// One atomic publication from a durable source into a retained map watch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapRevision {
    pub diff: Option<MapDiff<Arc<str>, Arc<dyn AnyItem>>>,
    pub frontier: Option<LogPosition>,
    pub epoch: u64,
    pub liveness: SubscriptionLiveness,
}

/// One current typed item together with its immutable authoritative source.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourcedItem<T> {
    pub source_node: NodeId,
    pub item: T,
    first_changed_at: u64,
    last_changed_at: u64,
    change_index: u32,
}

impl<T> SourcedItem<T> {
    /// Return the source revision that first created this item.
    #[must_use]
    pub const fn first_changed_at(&self) -> u64 {
        self.first_changed_at
    }

    /// Return the latest source revision that changed this item.
    #[must_use]
    pub const fn last_changed_at(&self) -> u64 {
        self.last_changed_at
    }

    /// Return the item's position within its latest atomic source batch.
    #[must_use]
    pub const fn change_index(&self) -> u32 {
        self.change_index
    }
}

/// Collision-free identity of one item in a multi-source projection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourcedItemKey<I> {
    pub source_node: NodeId,
    pub item_id: I,
}

/// Retained reactive map spanning every authoritative source in one selection.
pub type SourcedItemMap<T> =
    CellMap<SourcedItemKey<<T as MykoItem>::Id>, Arc<SourcedItem<T>>, CellImmutable>;

type SourcedProjection<T> = BTreeMap<SourcedItemKey<<T as MykoItem>::Id>, SourcedItem<T>>;
type SourcedProjectionDiff<T> = MapDiff<SourcedItemKey<<T as MykoItem>::Id>, Arc<SourcedItem<T>>>;

struct SourcedMapSource<T>
where
    T: MykoItem,
{
    rows: SourcedItemMap<T>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl<T> SourcedMapSource<T>
where
    T: MykoItem,
{
    fn start(
        node: &Node,
        selection: ScopeSelection,
        executor: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        let history = node.events_after(None).map_err(|error| error.to_string())?;
        let through = history.last().map(|event| event.position);
        let topology = node.scope_topology().map_err(|error| error.to_string())?;
        let mut items = BTreeMap::new();
        for envelope in &history {
            let _diff = apply_sourced_item_event::<T>(&mut items, &selection, &topology, envelope)?;
        }
        let rows_writer = CellMap::<SourcedItemKey<T::Id>, Arc<SourcedItem<T>>, CellMutable>::new()
            .with_name("myko.federated.sourced_rows");
        rows_writer.replace_all(
            items
                .iter()
                .map(|(key, item)| (key.clone(), Arc::new(item.clone())))
                .collect::<Vec<_>>(),
        );
        let rows = rows_writer.clone().lock();
        let mut events = node.subscribe(through).map_err(|error| error.to_string())?;
        let task_node = node.clone();
        let task = executor.spawn(async move {
            loop {
                let envelope = match events.recv_async().await {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        tracing::error!(%error, "multi-source item projection disconnected");
                        return;
                    }
                };
                let topology = match task_node.scope_topology() {
                    Ok(topology) => topology,
                    Err(error) => {
                        tracing::error!(%error, "multi-source item topology became unavailable");
                        return;
                    }
                };
                match apply_sourced_item_event::<T>(&mut items, &selection, &topology, &envelope) {
                    Ok(Some(diff)) => rows_writer.apply_diff_owned(diff),
                    Ok(None) => {}
                    Err(error) => {
                        tracing::error!(%error, "multi-source item projection failed");
                        return;
                    }
                }
            }
        });
        Ok(Self {
            rows,
            task: Some(task),
        })
    }

    fn rows(&self) -> SourcedItemMap<T> {
        self.rows.clone()
    }
}

impl<T> Drop for SourcedMapSource<T>
where
    T: MykoItem,
{
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// A durable item projection materialized directly into Myko's retained map.
pub struct FederatedMapSource {
    rows: FilteredCellMap,
    revision: Cell<MapRevision, CellImmutable>,
    published: Arc<Mutex<MapRevision>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl FederatedMapSource {
    /// Start one typed projection on the supplied executor.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be projected or followed.
    pub fn start<T>(
        node: &Node,
        source_node: Option<NodeId>,
        scope_id: Option<ScopeId>,
        executor: &tokio::runtime::Handle,
    ) -> Result<Self, String>
    where
        T: MykoItem + Eventable + AnyItem,
    {
        let (initial, mut watch) = node
            .watch_item_projection::<T>(source_node, scope_id)
            .map_err(|error| error.to_string())?;
        let rows_writer = CellMap::<Arc<str>, Arc<dyn AnyItem>, CellMutable>::new()
            .with_name("myko.federated.rows");
        let initial_rows = initial
            .projection
            .values()
            .map(|item| {
                let item: Arc<dyn AnyItem> = Arc::new(item.clone());
                (item.id(), item)
            })
            .collect::<Vec<_>>();
        rows_writer.replace_all(initial_rows.clone());
        let initial_revision = MapRevision {
            diff: Some(MapDiff::Initial {
                entries: initial_rows,
            }),
            frontier: initial.through,
            epoch: 0,
            liveness: SubscriptionLiveness::Current,
        };
        let revision_writer =
            Cell::new(initial_revision.clone()).with_name("myko.federated.revision");
        let published = Arc::new(Mutex::new(initial_revision));
        let published_for_task = Arc::clone(&published);
        let rows = rows_writer.clone().lock();
        let revision = revision_writer.clone().lock();
        let task = executor.spawn(async move {
            loop {
                match watch.recv_async().await {
                    Ok(update) => {
                        let diff = update.diff.as_ref().map(erase_state_diff::<T>);
                        let next = MapRevision {
                            diff: diff.clone(),
                            frontier: Some(update.position),
                            epoch: 0,
                            liveness: SubscriptionLiveness::Current,
                        };
                        hyphae::batch(|| {
                            if let Some(diff) = diff {
                                rows_writer.apply_diff_owned(diff);
                            }
                            revision_writer.set(next.clone());
                        });
                        *published_for_task
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
                    }
                    Err(error) => {
                        let previous = revision_writer.get();
                        let invalid = MapRevision {
                            diff: None,
                            frontier: previous.frontier,
                            epoch: previous.epoch,
                            liveness: SubscriptionLiveness::Invalid {
                                reason: error.to_string(),
                            },
                        };
                        revision_writer.set(invalid.clone());
                        *published_for_task
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = invalid;
                        return;
                    }
                }
            }
        });
        Ok(Self {
            rows,
            revision,
            published,
            task: Some(task),
        })
    }

    #[must_use]
    pub fn rows(&self) -> FilteredCellMap {
        self.rows.clone()
    }

    #[must_use]
    pub fn revision(&self) -> Cell<MapRevision, CellImmutable> {
        self.revision.clone()
    }

    async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _stopped = task.await;
        }
    }
}

impl Drop for FederatedMapSource {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SourceKey {
    item: TypeId,
    source_node: Option<NodeId>,
    scope_id: Option<ScopeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SourcedSourceKey {
    item: TypeId,
    selection: ScopeSelection,
}

struct DedicatedExecutor {
    handle: tokio::runtime::Handle,
    shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl DedicatedExecutor {
    fn start() -> Result<Self, String> {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("myko-federated-source".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("failed to start federated source runtime: {error}"));
                match runtime {
                    Ok(runtime) => {
                        let _sent = ready_tx.send(Ok(runtime.handle().clone()));
                        let _shutdown = runtime.block_on(shutdown_rx);
                    }
                    Err(error) => {
                        let _sent = ready_tx.send(Err(error));
                    }
                }
            })
            .map_err(|error| format!("failed to start federated source thread: {error}"))?;
        match ready_rx.recv() {
            Ok(Ok(handle)) => Ok(Self {
                handle,
                shutdown: Mutex::new(Some(shutdown_tx)),
                thread: Mutex::new(Some(thread)),
            }),
            Ok(Err(error)) => {
                let _joined = thread.join();
                Err(error)
            }
            Err(error) => {
                let _joined = thread.join();
                Err(format!(
                    "federated source thread stopped during startup: {error}"
                ))
            }
        }
    }
}

impl Drop for DedicatedExecutor {
    fn drop(&mut self) {
        if let Some(shutdown) = self
            .shutdown
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _sent = shutdown.send(());
        }
        if let Some(thread) = self
            .thread
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            && thread.thread().id() != std::thread::current().id()
        {
            let _joined = thread.join();
        }
    }
}

/// Node-owned federation projections shared by all retained handlers.
pub struct FederatedRuntime {
    node: Node,
    executor: DedicatedExecutor,
    sources: Mutex<HashMap<SourceKey, Arc<FederatedMapSource>>>,
    sourced_sources: Mutex<HashMap<SourcedSourceKey, Arc<dyn Any + Send + Sync>>>,
}

impl FederatedRuntime {
    /// Create the one source runtime owned by an application node.
    ///
    /// # Errors
    ///
    /// Returns an error when the joined executor thread cannot start.
    pub fn new(node: Node) -> Result<Self, String> {
        Ok(Self {
            node,
            executor: DedicatedExecutor::start()?,
            sources: Mutex::new(HashMap::new()),
            sourced_sources: Mutex::new(HashMap::new()),
        })
    }

    /// Return the identity used to resolve handler-relative source declarations.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    /// Return one shared retained map for the exact typed source selection.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cache is unavailable or projection
    /// history cannot be opened.
    pub fn items<T>(
        &self,
        source_node: Option<NodeId>,
        scope_id: Option<ScopeId>,
    ) -> Result<Arc<FederatedMapSource>, String>
    where
        T: MykoItem + Eventable + AnyItem,
    {
        let key = SourceKey {
            item: TypeId::of::<T>(),
            source_node,
            scope_id: scope_id.clone(),
        };
        let mut sources = self
            .sources
            .lock()
            .map_err(|_| "federated source cache is poisoned".to_owned())?;
        if let Some(source) = sources.get(&key) {
            return Ok(Arc::clone(source));
        }
        let source = Arc::new(FederatedMapSource::start::<T>(
            &self.node,
            source_node,
            scope_id,
            &self.executor.handle,
        )?);
        sources.insert(key, Arc::clone(&source));
        drop(sources);
        Ok(source)
    }

    /// Return a shared typed map spanning every source in one exact scope.
    ///
    /// The composite key and row both retain source identity. Rows also retain
    /// their first/latest source positions and latest batch index.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be projected or the source cache
    /// is unavailable.
    pub fn items_across_sources<T>(&self, scope_id: ScopeId) -> Result<SourcedItemMap<T>, String>
    where
        T: MykoItem + Eventable + AnyItem,
    {
        self.items_across_sources_selected(ScopeSelection::Exact(scope_id))
    }

    /// Return a shared typed map spanning every source in an exact scope or subtree.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be projected or the source cache
    /// is unavailable.
    pub fn items_across_sources_selected<T>(
        &self,
        selection: ScopeSelection,
    ) -> Result<SourcedItemMap<T>, String>
    where
        T: MykoItem + Eventable + AnyItem,
    {
        let key = SourcedSourceKey {
            item: TypeId::of::<T>(),
            selection: selection.clone(),
        };
        let mut sources = self
            .sourced_sources
            .lock()
            .map_err(|_| "federated sourced-item cache is poisoned".to_owned())?;
        if let Some(source) = sources.get(&key) {
            return Arc::clone(source)
                .downcast::<SourcedMapSource<T>>()
                .map(|source| source.rows())
                .map_err(|_| "federated sourced-item cache type mismatch".to_owned());
        }
        let source = Arc::new(SourcedMapSource::<T>::start(
            &self.node,
            selection,
            &self.executor.handle,
        )?);
        let erased: Arc<dyn Any + Send + Sync> = source.clone();
        sources.insert(key, erased);
        drop(sources);
        Ok(source.rows())
    }

    /// Return whether every opened source for this selection is current at
    /// the authoritative frontier.
    #[must_use]
    pub fn selection_is_current_at(
        &self,
        source_node: Option<NodeId>,
        scope_id: Option<&ScopeId>,
        frontier: Option<LogPosition>,
    ) -> bool {
        let Ok(sources) = self.sources.lock() else {
            return false;
        };
        let mut matched = false;
        for (_, source) in sources
            .iter()
            .filter(|(key, _)| key.source_node == source_node && key.scope_id.as_ref() == scope_id)
        {
            matched = true;
            let revision = source
                .published
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let reached_frontier = match frontier {
                None => true,
                Some(required) => revision.frontier.is_some_and(|current| current >= required),
            };
            if revision.liveness != SubscriptionLiveness::Current || !reached_frontier {
                return false;
            }
        }
        matched
    }

    /// Stop and join every durable source driver.
    pub async fn shutdown(&self) {
        let sources = self
            .sources
            .lock()
            .map(|mut sources| std::mem::take(&mut *sources))
            .unwrap_or_default();
        for source in sources.into_values() {
            if let Ok(source) = Arc::try_unwrap(source) {
                source.shutdown().await;
            }
        }
        let sourced_sources = self
            .sourced_sources
            .lock()
            .map(|mut sources| std::mem::take(&mut *sources))
            .unwrap_or_default();
        drop(sourced_sources);
    }
}

fn apply_sourced_item_event<T>(
    items: &mut SourcedProjection<T>,
    selection: &ScopeSelection,
    topology: &ScopeTopology,
    envelope: &EventEnvelope,
) -> Result<Option<SourcedProjectionDiff<T>>, String>
where
    T: MykoItem,
{
    let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
        return Ok(None);
    };
    if command.request.service_id != ServiceId::new(T::SERVICE_ID) {
        return Ok(None);
    }
    let mut changes = Vec::new();
    for (index, mutation) in batch.changes.iter().enumerate() {
        if !mutation.is::<T>() {
            continue;
        }
        let affected_scope = ScopeId::new(
            mutation
                .scope_id
                .as_deref()
                .unwrap_or(batch.scope_id.as_str()),
        );
        if !selection.contains_scope(&affected_scope, topology) {
            continue;
        }
        match mutation.operation {
            MutationOperation::Set => {
                let item = mutation
                    .decode_set_in_scope::<T>(Some(batch.scope_id.as_str()))
                    .map_err(|error| error.to_string())?;
                let key = SourcedItemKey {
                    source_node: envelope.origin.node_id,
                    item_id: item.item_id().clone(),
                };
                let change_index = u32::try_from(index).map_err(|error| {
                    format!("item batch contains too many ordered changes: {error}")
                })?;
                let sourced = SourcedItem {
                    source_node: envelope.origin.node_id,
                    item,
                    first_changed_at: items
                        .get(&key)
                        .map_or_else(|| envelope.position.get(), SourcedItem::first_changed_at),
                    last_changed_at: envelope.position.get(),
                    change_index,
                };
                match items.insert(key.clone(), sourced.clone()) {
                    None => changes.push(MapDiff::Insert {
                        key,
                        value: Arc::new(sourced),
                    }),
                    Some(old_value) if old_value != sourced => changes.push(MapDiff::Update {
                        key,
                        old_value: Arc::new(old_value),
                        new_value: Arc::new(sourced),
                    }),
                    Some(_) => {}
                }
            }
            MutationOperation::Delete => {
                mutation
                    .validate_envelope()
                    .map_err(|error| error.to_string())?;
                let key = SourcedItemKey {
                    source_node: envelope.origin.node_id,
                    item_id: T::Id::from(mutation.item_id.clone()),
                };
                if let Some(old_value) = items.remove(&key) {
                    changes.push(MapDiff::Remove {
                        key,
                        old_value: Arc::new(old_value),
                    });
                }
            }
        }
    }
    Ok(match changes.len() {
        0 => None,
        1 => changes.pop(),
        _ => Some(MapDiff::Batch { changes }),
    })
}

fn erase_state_diff<T>(
    diff: &MapDiff<T::Id, Arc<myko_items::ItemState<T>>>,
) -> MapDiff<Arc<str>, Arc<dyn AnyItem>>
where
    T: MykoItem + Eventable + AnyItem,
{
    match diff {
        MapDiff::Initial { entries } => MapDiff::Initial {
            entries: entries
                .iter()
                .map(|(id, state)| {
                    let item: Arc<dyn AnyItem> = Arc::new(state.value().clone());
                    (Arc::from(id.as_ref()), item)
                })
                .collect(),
        },
        MapDiff::Insert { key, value } => MapDiff::Insert {
            key: Arc::from(key.as_ref()),
            value: Arc::new(value.value().clone()),
        },
        MapDiff::Remove { key, old_value } => MapDiff::Remove {
            key: Arc::from(key.as_ref()),
            old_value: Arc::new(old_value.value().clone()),
        },
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => MapDiff::Update {
            key: Arc::from(key.as_ref()),
            old_value: Arc::new(old_value.value().clone()),
            new_value: Arc::new(new_value.value().clone()),
        },
        MapDiff::Batch { changes } => MapDiff::Batch {
            changes: changes.iter().map(erase_state_diff::<T>).collect(),
        },
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::unwrap_used
)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use hyphae::{Definite, Gettable as _, MapExt as _, Materialize, Signal, Watchable as _};
    use myko_federation::{
        AuthorityPresentation, BatchId, ChangeBatch, CommandId, CommandRequest, ItemMutation,
        PrincipalId, ResourceClaim, ResourceClaimKind, ServiceId,
    };

    use super::*;
    use crate::{
        ApplicationHost, MykoApplication, MykoService,
        core::capability::Viewing as _,
        myko_item, myko_query, myko_report, myko_report_output, myko_service, myko_view,
        request::RequestContext,
        search::SearchIndex,
        server::{
            MykoServerContext, MykoServerRuntime, RelationshipManager, persister::PersisterRouter,
        },
        store::StoreRegistry,
        wire::{EncodedCommandMessage, MykoMessage},
    };

    #[derive(Clone)]
    struct NodeFrameSink(Arc<Mutex<Vec<myko_wire::NodeFrame>>>);

    impl crate::server::SessionSink for NodeFrameSink {
        fn send(&self, _message: MykoMessage) {}

        fn send_serialized_command(
            &self,
            _tx: Arc<str>,
            _command_id: String,
            _payload: EncodedCommandMessage,
        ) {
        }

        fn send_node_frame(&self, frame: myko_wire::NodeFrame) -> Result<(), String> {
            self.0
                .lock()
                .map_err(|_| "test node frame sink is poisoned".to_owned())?
                .push(frame);
            Ok(())
        }
    }

    #[myko_service(ProjectionRecord)]
    pub struct ProjectionService;

    #[myko_item(service = ProjectionService, scope_root)]
    pub struct ProjectionRecord {
        value: u64,
    }

    #[myko_query(ProjectionRecord, item = ProjectionRecord)]
    #[derive(PartialEq, Eq)]
    pub struct ProjectionRecords;

    impl crate::query::QueryHandler for ProjectionRecords {
        fn build_view(
            ctx: crate::query::QueryBuildArgs<Self>,
        ) -> Option<impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<dyn AnyItem>>> {
            Some(
                ctx.federated_items::<ProjectionRecord>()
                    .expect("test federation source is configured"),
            )
        }
    }

    #[myko_view(ProjectionRecord, item = ProjectionRecord)]
    #[derive(PartialEq, Eq)]
    pub struct ProjectionRecordView;

    impl crate::view::ViewHandler for ProjectionRecordView {
        fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
            Some(ScopeId::for_item::<ProjectionRecord>(
                &ProjectionRecordId::from("record"),
            ))
        }

        fn build_cell(
            ctx: crate::view::ViewBuildArgs<Self>,
        ) -> impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
            crate::item::typed_map_arc_from_any_item::<ProjectionRecord>(
                ctx.federated_items::<ProjectionRecord>()
                    .expect("test federation source is configured"),
                "ProjectionRecordView",
            )
        }
    }

    #[myko_view(ProjectionRecord, item = ProjectionRecord)]
    #[derive(PartialEq, Eq)]
    pub struct NestedProjectionRecordView;

    impl crate::view::ViewHandler for NestedProjectionRecordView {
        fn build_cell(
            ctx: crate::view::ViewBuildArgs<Self>,
        ) -> impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
            ctx.view_context.view(ProjectionRecordView)
        }
    }

    #[myko_report_output]
    #[derive(Eq)]
    pub struct FederatedProjectionCount {
        count: usize,
    }

    #[myko_report(FederatedProjectionCount, item = ProjectionRecord)]
    #[derive(PartialEq, Eq)]
    pub struct FederatedProjectionCountReport {}

    impl crate::report::ReportHandler for FederatedProjectionCountReport {
        type Output = FederatedProjectionCount;

        fn compute(
            &self,
            ctx: crate::report::ReportContext,
        ) -> impl Materialize<Arc<Self::Output>, Definite> {
            ctx.federated_items::<ProjectionRecord>()
                .expect("test federation source is configured")
                .size()
                .map(|count| Arc::new(FederatedProjectionCount { count: *count }))
        }
    }

    fn retained_context(node: Node) -> Result<Arc<MykoServerContext>, String> {
        let application = MykoApplication::builder()
            .service::<ProjectionService>()
            .build();
        let registry = Arc::new(StoreRegistry::new());
        MykoServerContext::new(
            uuid::Uuid::new_v4(),
            registry,
            Arc::clone(application.handlers()),
            Arc::new(RelationshipManager::new()),
            Arc::new(PersisterRouter::default()),
            Arc::new(SearchIndex::new()),
            MykoServerRuntime {
                peer_clients: Arc::new(dashmap::DashMap::new()),
                event_sink: None,
                history_replay: None,
            },
        )
        .with_federation(node)
        .map(Arc::new)
    }

    fn commit(node: &Node, item: Option<&ProjectionRecord>) -> Result<(), String> {
        let scope_id = ScopeId::for_item::<ProjectionRecord>(&ProjectionRecordId::from("record"));
        let command = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new(ProjectionService::SERVICE_ID),
            scope_id: scope_id.clone(),
            principal_id: PrincipalId::new("test:owner"),
            authority: AuthorityPresentation::direct_node(PrincipalId::new("test:owner")),
            resource_claims: vec![ResourceClaim::scope(
                scope_id.clone(),
                ResourceClaimKind::Primary,
            )],
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: "projection.record".to_owned(),
            payload: Vec::new(),
        };
        node.admit(command.clone())
            .map_err(|error| error.to_string())?;
        let change = item.map_or_else(
            || ItemMutation::delete::<ProjectionRecord>(&ProjectionRecordId::from("record")),
            |item| ItemMutation::set(item).expect("test item serializes"),
        );
        node.commit(
            command.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: command.id,
                service_id: command.service_id,
                scope_id,
                causal_parents: Vec::new(),
                changes: vec![change],
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn revision_observers_see_matching_rows_in_the_same_wave() -> Result<(), String> {
        let node = Node::in_memory();
        let scope_id = ScopeId::for_item::<ProjectionRecord>(&ProjectionRecordId::from("record"));
        let source = FederatedMapSource::start::<ProjectionRecord>(
            &node,
            Some(node.node_id()),
            Some(scope_id),
            &tokio::runtime::Handle::current(),
        )?;
        let rows = source.rows();
        let inconsistent = Arc::new(AtomicBool::new(false));
        let check_rows = rows.clone();
        let check_inconsistent = Arc::clone(&inconsistent);
        let _guard = source.revision().subscribe(move |signal| {
            let Signal::Value(revision) = signal else {
                return;
            };
            let snapshot = check_rows.snapshot();
            match revision.diff.as_ref() {
                Some(MapDiff::Insert { key, .. } | MapDiff::Update { key, .. }) => {
                    if !snapshot.iter().any(|(row, _)| row == key) {
                        check_inconsistent.store(true, Ordering::Relaxed);
                    }
                }
                Some(MapDiff::Remove { key, .. }) if snapshot.iter().any(|(row, _)| row == key) => {
                    check_inconsistent.store(true, Ordering::Relaxed);
                }
                _ => {}
            }
        });

        commit(
            &node,
            Some(&ProjectionRecord {
                id: ProjectionRecordId::from("record"),
                value: 1,
            }),
        )?;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while rows.snapshot().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "insert did not reach retained map".to_owned())?;
        commit(&node, None)?;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !rows.snapshot().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "delete did not reach retained map".to_owned())?;
        assert!(!inconsistent.load(Ordering::Relaxed));
        Ok(())
    }

    #[tokio::test]
    async fn sourced_projection_preserves_colliding_ids_and_revision_metadata() -> Result<(), String>
    {
        let local = Node::in_memory();
        let remote = Node::in_memory();
        commit(
            &local,
            Some(&ProjectionRecord {
                id: ProjectionRecordId::from("record"),
                value: 1,
            }),
        )?;
        commit(
            &remote,
            Some(&ProjectionRecord {
                id: ProjectionRecordId::from("record"),
                value: 2,
            }),
        )?;
        for event in remote
            .events_after(None)
            .map_err(|error| error.to_string())?
        {
            let _status = local.ingest(event).map_err(|error| error.to_string())?;
        }

        let runtime = FederatedRuntime::new(local.clone())?;
        let scope_id = ScopeId::for_item::<ProjectionRecord>(&ProjectionRecordId::from("record"));
        let rows = runtime.items_across_sources::<ProjectionRecord>(scope_id)?;
        let local_key = SourcedItemKey {
            source_node: local.node_id(),
            item_id: ProjectionRecordId::from("record"),
        };
        let remote_key = SourcedItemKey {
            source_node: remote.node_id(),
            item_id: ProjectionRecordId::from("record"),
        };
        let local_row = rows
            .get_value(&local_key)
            .ok_or_else(|| "local sourced row is missing".to_owned())?;
        let remote_row = rows
            .get_value(&remote_key)
            .ok_or_else(|| "remote sourced row is missing".to_owned())?;
        assert_eq!(local_row.item.value, 1);
        assert_eq!(remote_row.item.value, 2);
        assert_eq!(local_row.first_changed_at(), local_row.last_changed_at());
        assert_eq!(remote_row.first_changed_at(), remote_row.last_changed_at());
        assert_eq!(local_row.change_index(), 0);

        let first_changed_at = local_row.first_changed_at();
        commit(
            &local,
            Some(&ProjectionRecord {
                id: ProjectionRecordId::from("record"),
                value: 3,
            }),
        )?;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while rows
                .get_value(&local_key)
                .is_none_or(|row| row.item.value != 3)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "local sourced update did not arrive".to_owned())?;
        let updated = rows
            .get_value(&local_key)
            .ok_or_else(|| "updated sourced row is missing".to_owned())?;
        assert_eq!(updated.first_changed_at(), first_changed_at);
        assert!(updated.last_changed_at() > first_changed_at);
        assert_eq!(updated.change_index(), 0);
        runtime.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn releasing_one_consumer_keeps_the_node_owned_source_live() -> Result<(), String> {
        let node = Node::in_memory();
        let scope_id = ScopeId::for_item::<ProjectionRecord>(&ProjectionRecordId::from("record"));
        let runtime = FederatedRuntime::new(node.clone())?;
        let first =
            runtime.items::<ProjectionRecord>(Some(node.node_id()), Some(scope_id.clone()))?;
        let second = runtime.items::<ProjectionRecord>(Some(node.node_id()), Some(scope_id))?;
        assert!(Arc::ptr_eq(&first, &second));
        drop(first);

        commit(
            &node,
            Some(&ProjectionRecord {
                id: ProjectionRecordId::from("record"),
                value: 1,
            }),
        )?;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while second.rows().snapshot().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "shared source stopped with its first consumer".to_owned())?;

        drop(second);
        runtime.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn retained_query_registration_materializes_the_durable_source() -> Result<(), String> {
        let node = Node::in_memory();
        let record_id = ProjectionRecordId::from("record");
        let scope_id = ScopeId::for_item::<ProjectionRecord>(&record_id);
        commit(
            &node,
            Some(&ProjectionRecord {
                id: record_id,
                value: 1,
            }),
        )?;
        let server = retained_context(node.clone())?;
        let request = Arc::new(RequestContext::new(
            Arc::from("test-query"),
            None,
            vec![Arc::from("test")],
            server.host_id,
            chrono::Utc::now().to_rfc3339(),
        ));
        let rows = server.handler_registry.open_federated_query(
            "ProjectionRecords",
            serde_json::json!({}),
            request,
            Arc::clone(&server),
            FederatedRequest {
                source_node: Some(node.node_id()),
                scope_id: Some(scope_id),
            },
        )?;
        assert_eq!(rows.snapshot().len(), 1);

        commit(&node, None)?;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !rows.snapshot().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "retained query did not observe durable deletion".to_owned())?;
        Ok(())
    }

    #[tokio::test]
    async fn retained_view_registration_materializes_the_durable_source() -> Result<(), String> {
        let node = Node::in_memory();
        let record_id = ProjectionRecordId::from("record");
        let scope_id = ScopeId::for_item::<ProjectionRecord>(&record_id);
        commit(
            &node,
            Some(&ProjectionRecord {
                id: record_id,
                value: 1,
            }),
        )?;
        let server = retained_context(node.clone())?;
        let request = Arc::new(RequestContext::new(
            Arc::from("test-view"),
            None,
            vec![Arc::from("test")],
            server.host_id,
            chrono::Utc::now().to_rfc3339(),
        ));
        let rows = server.handler_registry.open_federated_view(
            "ProjectionRecordView",
            serde_json::json!({}),
            request,
            Arc::clone(&server),
            FederatedRequest {
                source_node: Some(node.node_id()),
                scope_id: Some(scope_id),
            },
        )?;
        assert_eq!(rows.snapshot().len(), 1);

        commit(&node, None)?;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !rows.snapshot().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "retained view did not observe durable deletion".to_owned())?;
        Ok(())
    }

    #[tokio::test]
    async fn nested_views_preserve_the_federated_source_request() -> Result<(), String> {
        let node = Node::in_memory();
        let record_id = ProjectionRecordId::from("record");
        commit(
            &node,
            Some(&ProjectionRecord {
                id: record_id,
                value: 1,
            }),
        )?;
        let server = retained_context(node.clone())?;
        let request = Arc::new(RequestContext::internal(
            Arc::from("nested-view"),
            server.host_id,
            "test",
        ));
        let rows = server.handler_registry.open_federated_view(
            "NestedProjectionRecordView",
            serde_json::json!({}),
            request,
            Arc::clone(&server),
            FederatedRequest {
                source_node: Some(node.node_id()),
                scope_id: Some(ScopeId::new("outer-scope")),
            },
        )?;

        assert_eq!(rows.snapshot().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn retained_report_registration_materializes_the_durable_source() -> Result<(), String> {
        let node = Node::in_memory();
        let record_id = ProjectionRecordId::from("record");
        let scope_id = ScopeId::for_item::<ProjectionRecord>(&record_id);
        commit(
            &node,
            Some(&ProjectionRecord {
                id: record_id,
                value: 1,
            }),
        )?;
        let server = retained_context(node.clone())?;
        let request = Arc::new(RequestContext::new(
            Arc::from("test-report"),
            None,
            vec![Arc::from("test")],
            server.host_id,
            chrono::Utc::now().to_rfc3339(),
        ));
        let count = server.handler_registry.open_federated_report(
            "FederatedProjectionCountReport",
            serde_json::json!({}),
            request,
            Arc::clone(&server),
            FederatedRequest {
                source_node: Some(node.node_id()),
                scope_id: Some(scope_id),
            },
        )?;
        assert_eq!(
            count
                .get()
                .as_any()
                .downcast_ref::<FederatedProjectionCount>()
                .map(|output| output.count),
            Some(1),
        );

        commit(&node, None)?;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let current = count.get();
                let count = current
                    .as_any()
                    .downcast_ref::<FederatedProjectionCount>()
                    .map(|output| output.count);
                if count == Some(0) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "retained report did not observe durable deletion".to_owned())?;
        Ok(())
    }

    #[tokio::test]
    async fn application_host_opens_handlers_in_the_retained_session() -> Result<(), String> {
        let node = Node::in_memory();
        let record_id = ProjectionRecordId::from("record");
        let scope_id = ScopeId::for_item::<ProjectionRecord>(&record_id);
        commit(
            &node,
            Some(&ProjectionRecord {
                id: record_id,
                value: 1,
            }),
        )?;
        let application = MykoApplication::builder()
            .service::<ProjectionService>()
            .build();
        let host = ApplicationHost::new(node.clone(), application)?;
        let frames = Arc::new(Mutex::new(Vec::new()));
        let mut session = crate::server::ClientSession::new(
            Arc::from("node-client"),
            NodeFrameSink(Arc::clone(&frames)),
        );

        host.open_handler(
            &mut session,
            Arc::from("handler-request"),
            myko_wire::HandlerRequest {
                kind: myko_federation::HandlerKind::Query,
                handler_id: "ProjectionRecords".to_owned(),
                source_node: Some(node.node_id()),
                scope_id: Some(scope_id),
                params: serde_json::json!({}),
            },
        )?;

        assert_eq!(session.subscription_count(), 1);
        assert!(frames.lock().map_err(|_| "test sink poisoned")?.iter().any(
            |frame| matches!(frame, myko_wire::NodeFrame::HandlerState { state, .. }
                if state.row_keys.as_ref().is_some_and(|keys| keys == &["record"])),
        ));
        session.cancel_all();
        host.shutdown().await;
        Ok(())
    }
}
