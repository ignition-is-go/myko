//! Application-defined reactive queries, reports, and views for Myko 7.
//!
//! Redb and federation supply durable events. This crate turns those events
//! into long-lived Hyphae dependency cells and gives an application one schema
//! registry for the handlers it elects to expose. Transport adapters can serve
//! the registered contracts without becoming the owners of application data.

#![forbid(unsafe_code)]

use std::{
    any::Any,
    collections::BTreeMap,
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use hyphae::{MapExt as _, Materialize as _};
use myko_federation::{
    CommandStateRequest, CommandStateSnapshot, CommandStateStream, LiveSubscription,
    LiveSubscriptionState, LogPosition, MutationOperation, MykoCommand, MykoItem, Node, NodeError,
    NodeEvent, NodeId, ScopeId, ServiceId, SubscriptionLiveness, live_subscription,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use tokio::task::JoinHandle;

pub use myko_federation::ItemQuery as QueryHandler;

/// Failure while registering or building an application reactive handler.
#[derive(Debug, Error)]
pub enum AppError {
    /// Durable state could not be projected or followed.
    #[error(transparent)]
    Node(#[from] NodeError),
    /// The application attempted to register the same stable contract twice.
    #[error("duplicate {kind} handler ID {id}")]
    DuplicateHandler { kind: &'static str, id: String },
    /// A caller requested a contract the application did not register.
    #[error("unregistered {kind} handler ID {id}")]
    UnregisteredHandler { kind: &'static str, id: String },
    /// Reactive dependency ownership could not be updated.
    #[error("reactive application state unavailable: {0}")]
    State(String),
    /// Handler parameters or lifecycle state could not be encoded.
    #[error("handler serialization failed: {0}")]
    Serialization(String),
}

/// Kind of application-owned reactive handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandlerKind {
    Query,
    Report,
    View,
}

impl HandlerKind {
    /// Returns the stable lowercase wire name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Report => "report",
            Self::View => "view",
        }
    }
}

/// Transport-neutral request for one registered reactive handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct HandlerRequest {
    pub kind: HandlerKind,
    pub handler_id: String,
    pub source_node: Option<NodeId>,
    pub scope_id: Option<ScopeId>,
    pub params: Value,
}

/// Type-erased lifecycle state used only at transport boundaries.
pub type ErasedHandlerState = LiveSubscriptionState<Value, Value>;

/// One current typed item together with its immutable authoritative source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct SourcedItem<T> {
    pub source_node: NodeId,
    pub item: T,
}

/// Application-defined reactive scalar or aggregate.
pub trait ReportHandler:
    Clone + fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Output: hyphae::CellValue + Serialize + DeserializeOwned;
    type Cursor: hyphae::CellValue + Serialize + DeserializeOwned;

    /// Stable application wire identity for this report.
    const REPORT_ID: &'static str;

    /// Builds the report once from long-lived reactive dependencies.
    ///
    /// The returned cell must be derived from the supplied context rather than
    /// periodically recomputed by a timer. Myko retains every dependency
    /// driver created through `context` for the result's lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error when a reactive dependency cannot be established or
    /// the application cannot construct a valid report state.
    fn build(
        &self,
        context: &HandlerContext,
    ) -> Result<LiveSubscription<Self::Output, Self::Cursor>, AppError>;
}

/// Application-defined reactive collection or joined read model.
pub trait ViewHandler:
    Clone + fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Item: hyphae::CellValue + Serialize + DeserializeOwned;
    type Cursor: hyphae::CellValue + Serialize + DeserializeOwned;

    /// Stable application wire identity for this view.
    const VIEW_ID: &'static str;

    /// Builds the view once from long-lived reactive dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error when a reactive dependency cannot be established or
    /// the application cannot construct a valid view state.
    fn build(
        &self,
        context: &HandlerContext,
    ) -> Result<LiveSubscription<Vec<Self::Item>, Self::Cursor>, AppError>;
}

/// Explicit list of the typed handlers exposed by one application.
#[derive(Debug, Clone, Default)]
pub struct ApplicationSchema {
    queries: BTreeMap<&'static str, Arc<dyn ErasedHandlerFactory>>,
    reports: BTreeMap<&'static str, Arc<dyn ErasedHandlerFactory>>,
    views: BTreeMap<&'static str, Arc<dyn ErasedHandlerFactory>>,
}

impl ApplicationSchema {
    /// Creates an empty application schema.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            queries: BTreeMap::new(),
            reports: BTreeMap::new(),
            views: BTreeMap::new(),
        }
    }

    /// Starts a fluent, fallible schema declaration.
    #[must_use]
    pub fn builder() -> ApplicationSchemaBuilder {
        ApplicationSchemaBuilder::new()
    }

    /// Registers one typed item query contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the stable ID is already registered.
    pub fn register_query<Q: QueryHandler>(&mut self) -> Result<(), AppError> {
        insert_handler(
            &mut self.queries,
            HandlerKind::Query,
            Q::QUERY_ID,
            Arc::new(QueryFactory::<Q>(PhantomData)),
        )
    }

    /// Registers one application report contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the stable ID is already registered.
    pub fn register_report<R: ReportHandler>(&mut self) -> Result<(), AppError> {
        insert_handler(
            &mut self.reports,
            HandlerKind::Report,
            R::REPORT_ID,
            Arc::new(ReportFactory::<R>(PhantomData)),
        )
    }

    /// Registers one application view contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the stable ID is already registered.
    pub fn register_view<V: ViewHandler>(&mut self) -> Result<(), AppError> {
        insert_handler(
            &mut self.views,
            HandlerKind::View,
            V::VIEW_ID,
            Arc::new(ViewFactory::<V>(PhantomData)),
        )
    }
}

/// Reusable collection of application contracts.
///
/// Feature crates implement this trait to contribute their handlers without
/// centralizing every registration in one monolithic function.
pub trait ApplicationModule {
    /// Registers this module's query, report, and view contracts.
    ///
    /// # Errors
    ///
    /// Returns an error when stable handler identities conflict.
    fn register(schema: &mut ApplicationSchema) -> Result<(), AppError>;
}

/// Fluent declaration of one immutable application schema.
#[derive(Debug, Default)]
pub struct ApplicationSchemaBuilder {
    schema: ApplicationSchema,
}

impl ApplicationSchemaBuilder {
    /// Creates an empty schema declaration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one typed item query.
    ///
    /// # Errors
    ///
    /// Returns an error when its stable ID is already registered.
    pub fn query<Q: QueryHandler>(mut self) -> Result<Self, AppError> {
        self.schema.register_query::<Q>()?;
        Ok(self)
    }

    /// Adds one reactive report.
    ///
    /// # Errors
    ///
    /// Returns an error when its stable ID is already registered.
    pub fn report<R: ReportHandler>(mut self) -> Result<Self, AppError> {
        self.schema.register_report::<R>()?;
        Ok(self)
    }

    /// Adds one reactive view.
    ///
    /// # Errors
    ///
    /// Returns an error when its stable ID is already registered.
    pub fn view<V: ViewHandler>(mut self) -> Result<Self, AppError> {
        self.schema.register_view::<V>()?;
        Ok(self)
    }

    /// Adds every contract contributed by a reusable application module.
    ///
    /// # Errors
    ///
    /// Returns an error when the module conflicts with prior declarations.
    pub fn module<M: ApplicationModule>(mut self) -> Result<Self, AppError> {
        M::register(&mut self.schema)?;
        Ok(self)
    }

    /// Finishes the immutable schema declaration.
    #[must_use]
    pub fn build(self) -> ApplicationSchema {
        self.schema
    }
}

/// Fluent construction of an application node and its declarative schema.
pub struct ApplicationNodeBuilder {
    node: Node,
    schema: ApplicationSchemaBuilder,
}

impl ApplicationNodeBuilder {
    /// Starts an application around an existing durable/federated node.
    #[must_use]
    pub fn new(node: Node) -> Self {
        Self {
            node,
            schema: ApplicationSchema::builder(),
        }
    }

    /// Adds one typed query contract.
    ///
    /// # Errors
    ///
    /// Returns an error when its stable ID is already registered.
    pub fn query<Q: QueryHandler>(mut self) -> Result<Self, AppError> {
        self.schema = self.schema.query::<Q>()?;
        Ok(self)
    }

    /// Adds one reactive report contract.
    ///
    /// # Errors
    ///
    /// Returns an error when its stable ID is already registered.
    pub fn report<R: ReportHandler>(mut self) -> Result<Self, AppError> {
        self.schema = self.schema.report::<R>()?;
        Ok(self)
    }

    /// Adds one reactive view contract.
    ///
    /// # Errors
    ///
    /// Returns an error when its stable ID is already registered.
    pub fn view<V: ViewHandler>(mut self) -> Result<Self, AppError> {
        self.schema = self.schema.view::<V>()?;
        Ok(self)
    }

    /// Adds a reusable module's handler contracts.
    ///
    /// # Errors
    ///
    /// Returns an error when its stable IDs conflict.
    pub fn module<M: ApplicationModule>(mut self) -> Result<Self, AppError> {
        self.schema = self.schema.module::<M>()?;
        Ok(self)
    }

    /// Attaches the completed immutable schema to the node.
    #[must_use]
    pub fn build(self) -> ApplicationNode {
        ApplicationNode::new(self.node, self.schema.build())
    }
}

fn insert_handler(
    handlers: &mut BTreeMap<&'static str, Arc<dyn ErasedHandlerFactory>>,
    kind: HandlerKind,
    id: &'static str,
    factory: Arc<dyn ErasedHandlerFactory>,
) -> Result<(), AppError> {
    if id.is_empty() || handlers.insert(id, factory).is_some() {
        return Err(AppError::DuplicateHandler {
            kind: kind.as_str(),
            id: id.to_owned(),
        });
    }
    Ok(())
}

trait ErasedHandlerFactory: fmt::Debug + Send + Sync {
    fn watch(
        &self,
        node: Node,
        request: &HandlerRequest,
    ) -> Result<ErasedHandlerSubscription, AppError>;
}

struct QueryFactory<Q>(PhantomData<fn() -> Q>);

impl<Q: QueryHandler> fmt::Debug for QueryFactory<Q> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("QueryFactory")
            .field(&Q::QUERY_ID)
            .finish()
    }
}

impl<Q: QueryHandler> ErasedHandlerFactory for QueryFactory<Q> {
    fn watch(
        &self,
        node: Node,
        request: &HandlerRequest,
    ) -> Result<ErasedHandlerSubscription, AppError> {
        let source_node = request.source_node.ok_or_else(|| {
            AppError::State("query handler request omitted its authoritative source".to_owned())
        })?;
        let scope_id = request.scope_id.clone().ok_or_else(|| {
            AppError::State("query handler request omitted its federation scope".to_owned())
        })?;
        let query = serde_json::from_value::<Q>(request.params.clone())
            .map_err(|error| AppError::Serialization(error.to_string()))?;
        let context = HandlerContext::new(node);
        let live = context.query(source_node, scope_id, query)?;
        Ok(erase_handler(HandlerSubscription {
            live,
            _runtime: context.runtime,
        }))
    }
}

struct ReportFactory<R>(PhantomData<fn() -> R>);

impl<R: ReportHandler> fmt::Debug for ReportFactory<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ReportFactory")
            .field(&R::REPORT_ID)
            .finish()
    }
}

impl<R: ReportHandler> ErasedHandlerFactory for ReportFactory<R> {
    fn watch(
        &self,
        node: Node,
        request: &HandlerRequest,
    ) -> Result<ErasedHandlerSubscription, AppError> {
        let report = serde_json::from_value::<R>(request.params.clone())
            .map_err(|error| AppError::Serialization(error.to_string()))?;
        let context = HandlerContext::new(node);
        let live = report.build(&context)?;
        Ok(erase_handler(HandlerSubscription {
            live,
            _runtime: context.runtime,
        }))
    }
}

struct ViewFactory<V>(PhantomData<fn() -> V>);

impl<V: ViewHandler> fmt::Debug for ViewFactory<V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ViewFactory")
            .field(&V::VIEW_ID)
            .finish()
    }
}

impl<V: ViewHandler> ErasedHandlerFactory for ViewFactory<V> {
    fn watch(
        &self,
        node: Node,
        request: &HandlerRequest,
    ) -> Result<ErasedHandlerSubscription, AppError> {
        let view = serde_json::from_value::<V>(request.params.clone())
            .map_err(|error| AppError::Serialization(error.to_string()))?;
        let context = HandlerContext::new(node);
        let live = view.build(&context)?;
        Ok(erase_handler(HandlerSubscription {
            live,
            _runtime: context.runtime,
        }))
    }
}

struct DependencyDriver {
    task: JoinHandle<()>,
    invalidate: Box<dyn Fn() + Send + Sync>,
}

#[derive(Default)]
struct HandlerRuntime {
    drivers: Mutex<Vec<DependencyDriver>>,
}

impl Drop for HandlerRuntime {
    fn drop(&mut self) {
        let drivers = match self.drivers.get_mut() {
            Ok(drivers) => drivers,
            Err(poisoned) => poisoned.into_inner(),
        };
        for driver in drivers {
            (driver.invalidate)();
            driver.task.abort();
        }
    }
}

/// Read-only capabilities available while a report or view is built.
///
/// The context intentionally has no mutation or command-emission API. Handler
/// code can compose durable query and command-state cells, while writes remain
/// in declared command handlers.
#[derive(Clone)]
pub struct HandlerContext {
    node: Node,
    runtime: Arc<HandlerRuntime>,
}

impl HandlerContext {
    fn new(node: Node) -> Self {
        Self {
            node,
            runtime: Arc::new(HandlerRuntime::default()),
        }
    }

    /// Starts one gap-free typed item query dependency.
    ///
    /// # Errors
    ///
    /// Returns an error if current state cannot be projected, a gap-free
    /// follow cannot be established, or the dependency cannot be retained.
    pub fn query<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<LiveSubscription<Q::Output>, AppError>
    where
        Q: QueryHandler,
    {
        let (initial, mut watch) = self.node.watch_items_in(source_node, scope_id, query)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(initial.value),
            through: initial.through,
            liveness: SubscriptionLiveness::Current,
        });
        let task_writer = writer.clone();
        let task = tokio::spawn(async move {
            loop {
                match watch.recv_async().await {
                    Ok(update) => task_writer.publish(update.value, Some(update.position)),
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        });
        self.retain_driver(task, move || {
            writer.invalidate("reactive handler dependency dropped");
        })?;
        Ok(live)
    }

    /// Starts one gap-free provenance-preserving projection across every
    /// authoritative source represented in this node's replicated history.
    ///
    /// This is the dynamic fan-in primitive for views such as a mesh roster.
    /// A newly observed source enters the same subscription; application code
    /// does not discover peers, open one stream per peer, or poll a catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if matching history is malformed or cannot be followed.
    pub fn federated_items<T>(
        &self,
        scope_id: ScopeId,
    ) -> Result<LiveSubscription<Vec<SourcedItem<T>>>, AppError>
    where
        T: MykoItem,
    {
        let history = self.node.events_after(None)?;
        let through = history.last().map(|event| event.position);
        let mut items = BTreeMap::new();
        for envelope in &history {
            let _changed = apply_sourced_item_event::<T>(&mut items, &scope_id, envelope)?;
        }
        let mut events = self.node.subscribe(through)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(items.values().cloned().collect()),
            through,
            liveness: SubscriptionLiveness::Current,
        });
        let task_writer = writer.clone();
        let task = tokio::spawn(async move {
            loop {
                let envelope = match events.recv_async().await {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                };
                match apply_sourced_item_event::<T>(&mut items, &scope_id, &envelope) {
                    Ok(true) => task_writer
                        .publish(items.values().cloned().collect(), Some(envelope.position)),
                    Ok(false) => {}
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        });
        self.retain_driver(task, move || {
            writer.invalidate("reactive handler dependency dropped");
        })?;
        Ok(live)
    }

    /// Starts one gap-free typed command-catalog dependency.
    ///
    /// The initial catalog is pinned to a durable node-log ceiling before the
    /// replay-then-live follow is opened, so commits racing construction are
    /// delivered after the snapshot rather than lost.
    ///
    /// # Errors
    ///
    /// Returns an error if state cannot be read or followed.
    pub fn commands<C>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
    ) -> Result<LiveSubscription<CommandStateSnapshot>, AppError>
    where
        C: MykoCommand,
    {
        let through = self
            .node
            .events_after(None)?
            .last()
            .map(|event| event.position);
        let mut request = CommandStateRequest::for_declared::<C>(source_node, scope_id);
        request.snapshot_through = through;
        let snapshot = self.node.command_states(request)?;
        let follow = snapshot.follow_request()?;
        let mut stream = CommandStateStream::from_snapshot(&snapshot)?;
        let mut events = self.node.subscribe(through)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(snapshot),
            through,
            liveness: SubscriptionLiveness::Current,
        });
        let task_writer = writer.clone();
        let task = tokio::spawn(async move {
            loop {
                let envelope = match events.recv_async().await {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                };
                let Some(update) = follow.update_from_envelope(&envelope) else {
                    continue;
                };
                match stream.apply(&update) {
                    Ok(snapshot) => task_writer.publish(snapshot, Some(update.through)),
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        });
        self.retain_driver(task, move || {
            writer.invalidate("reactive handler dependency dropped");
        })?;
        Ok(live)
    }

    fn retain_driver(
        &self,
        task: JoinHandle<()>,
        invalidate: impl Fn() + Send + Sync + 'static,
    ) -> Result<(), AppError> {
        self.runtime
            .drivers
            .lock()
            .map_err(|_| AppError::State("dependency registry is poisoned".to_owned()))?
            .push(DependencyDriver {
                task,
                invalidate: Box::new(invalidate),
            });
        Ok(())
    }
}

fn apply_sourced_item_event<T>(
    items: &mut BTreeMap<(String, String), SourcedItem<T>>,
    scope_id: &ScopeId,
    envelope: &myko_federation::EventEnvelope,
) -> Result<bool, NodeError>
where
    T: MykoItem,
{
    let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
        return Ok(false);
    };
    if command.request.service_id != ServiceId::new(T::SERVICE_ID)
        || command.request.scope_id != *scope_id
    {
        return Ok(false);
    }
    let mut changed = false;
    for mutation in &batch.changes {
        if mutation.item_type != T::ITEM_TYPE {
            continue;
        }
        if mutation.service_id != T::SERVICE_ID || mutation.schema_version != T::SCHEMA_VERSION {
            return Err(NodeError::InvalidItemMutation(format!(
                "federated item schema {}/{}@{} does not match {}/{}@{}",
                mutation.service_id,
                mutation.item_type,
                mutation.schema_version,
                T::SERVICE_ID,
                T::ITEM_TYPE,
                T::SCHEMA_VERSION
            )));
        }
        let key = (
            envelope.origin.node_id.to_string(),
            mutation.item_id.clone(),
        );
        match mutation.operation {
            MutationOperation::Set => {
                let item = mutation
                    .decode_set::<T>()
                    .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?;
                items.insert(
                    key,
                    SourcedItem {
                        source_node: envelope.origin.node_id,
                        item,
                    },
                );
                changed = true;
            }
            MutationOperation::Delete => {
                mutation
                    .validate_envelope()
                    .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?;
                changed |= items.remove(&key).is_some();
            }
        }
    }
    Ok(changed)
}

/// A live handler result plus ownership of all of its dependency drivers.
pub struct HandlerSubscription<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveSubscription<T, C>,
    _runtime: Arc<HandlerRuntime>,
}

impl<T, C> HandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns the handler's composable Hyphae lifecycle cell.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<T, C> {
        &self.live
    }
}

/// Type-erased registered handler result retained by a peer transport.
pub struct ErasedHandlerSubscription {
    live: LiveSubscription<Value, Value>,
    _owner: Box<dyn Any + Send>,
}

impl ErasedHandlerSubscription {
    /// Returns the serializable Hyphae lifecycle cell served to peer clients.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<Value, Value> {
        &self.live
    }
}

fn erase_handler<T, C>(subscription: HandlerSubscription<T, C>) -> ErasedHandlerSubscription
where
    T: hyphae::CellValue + Serialize,
    C: hyphae::CellValue + Serialize,
{
    let state = subscription
        .live()
        .state()
        .clone()
        .map(erase_state::<T, C>)
        .materialize()
        .with_name("myko.application.erased_handler");
    ErasedHandlerSubscription {
        live: LiveSubscription::from_state_cell(state),
        _owner: Box::new(subscription),
    }
}

fn erase_state<T, C>(state: &LiveSubscriptionState<T, C>) -> ErasedHandlerState
where
    T: Serialize,
    C: Serialize,
{
    let value = match state.value.as_ref().map(serde_json::to_value).transpose() {
        Ok(value) => value,
        Err(error) => {
            return ErasedHandlerState {
                value: None,
                through: None,
                liveness: SubscriptionLiveness::Invalid {
                    reason: format!("handler value serialization failed: {error}"),
                },
            };
        }
    };
    let through = match state.through.as_ref().map(serde_json::to_value).transpose() {
        Ok(through) => through,
        Err(error) => {
            return ErasedHandlerState {
                value,
                through: None,
                liveness: SubscriptionLiveness::Invalid {
                    reason: format!("handler cursor serialization failed: {error}"),
                },
            };
        }
    };
    ErasedHandlerState {
        value,
        through,
        liveness: state.liveness.clone(),
    }
}

/// One Myko node with its application handler schema.
#[derive(Clone)]
pub struct ApplicationNode {
    node: Node,
    schema: Arc<ApplicationSchema>,
}

impl ApplicationNode {
    /// Starts a declarative application-node builder.
    #[must_use]
    pub fn builder(node: Node) -> ApplicationNodeBuilder {
        ApplicationNodeBuilder::new(node)
    }

    /// Attaches an immutable application schema to a node substrate.
    #[must_use]
    pub fn new(node: Node, schema: ApplicationSchema) -> Self {
        Self {
            node,
            schema: Arc::new(schema),
        }
    }

    /// Returns the underlying durable/federated node.
    #[must_use]
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// Builds any registered handler through its transport-neutral wire form.
    ///
    /// # Errors
    ///
    /// Returns an error when the stable ID is absent, the handler kind does
    /// not match, parameters are malformed, or its dependencies cannot start.
    pub fn watch_handler(
        &self,
        request: &HandlerRequest,
    ) -> Result<ErasedHandlerSubscription, AppError> {
        let handlers = match request.kind {
            HandlerKind::Query => &self.schema.queries,
            HandlerKind::Report => &self.schema.reports,
            HandlerKind::View => &self.schema.views,
        };
        let factory = handlers.get(request.handler_id.as_str()).ok_or_else(|| {
            AppError::UnregisteredHandler {
                kind: request.kind.as_str(),
                id: request.handler_id.clone(),
            }
        })?;
        factory.watch(self.node.clone(), request)
    }

    /// Builds a registered item query as a long-lived Hyphae subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when the query is not registered or cannot be watched.
    pub fn watch_query<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<HandlerSubscription<Q::Output>, AppError>
    where
        Q: QueryHandler,
    {
        require_handler(&self.schema.queries, "query", Q::QUERY_ID)?;
        let context = HandlerContext::new(self.node.clone());
        let live = context.query(source_node, scope_id, query)?;
        Ok(HandlerSubscription {
            live,
            _runtime: context.runtime,
        })
    }

    /// Builds a registered reactive report.
    ///
    /// # Errors
    ///
    /// Returns an error when the report is not registered or cannot be built.
    pub fn watch_report<R>(
        &self,
        report: &R,
    ) -> Result<HandlerSubscription<R::Output, R::Cursor>, AppError>
    where
        R: ReportHandler,
    {
        require_handler(&self.schema.reports, "report", R::REPORT_ID)?;
        let context = HandlerContext::new(self.node.clone());
        let live = report.build(&context)?;
        Ok(HandlerSubscription {
            live,
            _runtime: context.runtime,
        })
    }

    /// Builds a registered reactive view.
    ///
    /// # Errors
    ///
    /// Returns an error when the view is not registered or cannot be built.
    pub fn watch_view<V>(
        &self,
        view: &V,
    ) -> Result<HandlerSubscription<Vec<V::Item>, V::Cursor>, AppError>
    where
        V: ViewHandler,
    {
        require_handler(&self.schema.views, "view", V::VIEW_ID)?;
        let context = HandlerContext::new(self.node.clone());
        let live = view.build(&context)?;
        Ok(HandlerSubscription {
            live,
            _runtime: context.runtime,
        })
    }
}

/// Shared in-memory harness for application contract and transport-adapter tests.
pub mod testing {
    use myko_federation::{
        BatchId, ChangeBatch, CommandId, CommandRequest, LogPosition, MykoItem, PrincipalId,
        ScopeId, ServiceId,
    };

    use super::{AppError, ApplicationNode, ApplicationSchema, Node};

    /// One isolated node plus the application schema under test.
    pub struct ApplicationTestHarness {
        node: Node,
        schema: ApplicationSchema,
    }

    impl Default for ApplicationTestHarness {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ApplicationTestHarness {
        /// Creates an isolated in-memory node with an empty schema.
        #[must_use]
        pub fn new() -> Self {
            Self {
                node: Node::in_memory(),
                schema: ApplicationSchema::new(),
            }
        }

        /// Returns the durable/federated substrate used by the test.
        #[must_use]
        pub const fn node(&self) -> &Node {
            &self.node
        }

        /// Returns the mutable schema for registering test contracts.
        pub const fn schema_mut(&mut self) -> &mut ApplicationSchema {
            &mut self.schema
        }

        /// Builds an application handle while retaining this harness.
        #[must_use]
        pub fn application(&self) -> ApplicationNode {
            ApplicationNode::new(self.node.clone(), self.schema.clone())
        }

        /// Commits one typed item through the same immutable command/batch path
        /// used by production nodes.
        ///
        /// # Errors
        ///
        /// Returns an error if admission, item encoding, or commit fails.
        pub fn set_item<T: MykoItem>(
            &self,
            scope_id: ScopeId,
            item: &T,
        ) -> Result<LogPosition, AppError> {
            let command = CommandRequest {
                id: CommandId::new(),
                service_id: ServiceId::new(T::SERVICE_ID),
                scope_id: scope_id.clone(),
                principal_id: PrincipalId::new("test:application-harness"),
                command_type: format!("{}.test_set", T::ITEM_TYPE),
                payload: Vec::new(),
            };
            let _admitted = self.node.admit(command.clone())?;
            let committed = self.node.commit(
                command.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: command.id,
                    service_id: command.service_id,
                    scope_id,
                    causal_parents: Vec::new(),
                    changes: vec![
                        myko_federation::ItemMutation::set(item)
                            .map_err(|error| AppError::State(error.to_string()))?,
                    ],
                },
                Vec::new(),
            )?;
            Ok(committed.updated_at.sequence)
        }
    }
}

fn require_handler(
    handlers: &BTreeMap<&'static str, Arc<dyn ErasedHandlerFactory>>,
    kind: &'static str,
    id: &'static str,
) -> Result<(), AppError> {
    if !handlers.contains_key(id) {
        return Err(AppError::UnregisteredHandler {
            kind,
            id: id.to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use hyphae::{Signal, Watchable as _};
    use myko_federation::{
        BatchId, ChangeBatch, CommandId, CommandRequest, PrincipalId, ServiceId,
    };
    use myko_items::{ItemMutation, ItemProjection, ItemQuery, myko_item};

    use super::*;

    #[myko_item(service = "myko.app.test", scope_root)]
    pub struct CounterItem {
        pub value: u64,
    }

    #[derive(Debug, Clone, Copy, Serialize, serde::Deserialize)]
    struct SumCounters;

    impl ItemQuery for SumCounters {
        type Item = CounterItem;
        type Output = u64;
        const QUERY_ID: &'static str = "myko.app.test.sum_counters";

        fn execute(self, projection: &ItemProjection<Self::Item>) -> Self::Output {
            projection.values().map(|item| item.value).sum()
        }
    }

    #[derive(Debug, Clone, Copy, Serialize, serde::Deserialize)]
    struct CounterReport {
        source_node: NodeId,
    }

    impl ReportHandler for CounterReport {
        type Output = String;
        type Cursor = LogPosition;
        const REPORT_ID: &'static str = "myko.app.test.counter_report";

        fn build(
            &self,
            context: &HandlerContext,
        ) -> Result<LiveSubscription<Self::Output>, AppError> {
            Ok(context
                .query(self.source_node, ScopeId::new("counter"), SumCounters)?
                .map_value(|value| format!("count:{value}")))
        }
    }

    fn commit_counter(node: &Node, id: &str, value: u64) -> Result<(), AppError> {
        let command = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("myko.app.test"),
            scope_id: ScopeId::new("counter"),
            principal_id: PrincipalId::new("test:owner"),
            command_type: "counter.set".to_owned(),
            payload: Vec::new(),
        };
        let _admission = node.admit(command.clone())?;
        let item = CounterItem {
            id: CounterItemId::from(id),
            value,
        };
        let _committed =
            node.commit(
                command.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: command.id,
                    service_id: command.service_id,
                    scope_id: command.scope_id,
                    causal_parents: Vec::new(),
                    changes: vec![ItemMutation::set(&item).map_err(|error| {
                        AppError::State(format!("test mutation failed: {error}"))
                    })?],
                },
                Vec::new(),
            )?;
        Ok(())
    }

    #[tokio::test]
    async fn registered_report_is_driven_by_query_cell_without_polling() {
        let node = Node::in_memory();
        let mut schema = ApplicationSchema::new();
        assert!(schema.register_query::<SumCounters>().is_ok());
        assert!(schema.register_report::<CounterReport>().is_ok());
        let app = ApplicationNode::new(node.clone(), schema);
        let report = app.watch_report(&CounterReport {
            source_node: node.node_id(),
        });
        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };
        assert_eq!(report.live().current().value.as_deref(), Some("count:0"));

        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_callback = Arc::clone(&observed);
        let _guard = report.live().state().subscribe(move |signal| {
            if let Signal::Value(state) = signal
                && let Some(value) = &state.value
                && let Ok(mut observed) = observed_for_callback.lock()
            {
                observed.push(value.clone());
            }
        });
        assert!(commit_counter(&node, "first", 3).is_ok());

        let updated = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if observed
                    .lock()
                    .is_ok_and(|values| values.iter().any(|value| value == "count:3"))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(updated.is_ok());
    }
}
