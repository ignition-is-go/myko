//! Application-defined reactive queries, reports, and views for Myko 7.
//!
//! Redb and federation supply durable events. This crate turns those events
//! into long-lived Hyphae dependency cells and gives an application one module
//! registry for the handlers it elects to expose. Transport adapters can serve
//! the registered contracts without becoming the owners of application data.

#![forbid(unsafe_code)]

extern crate self as myko_app;

use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fmt,
    future::Future,
    marker::PhantomData,
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use hyphae::{Gettable as _, MapDiff, Signal, SubscriptionGuard, Watchable as _};
use myko_federation::{
    AccessOperation, AccessRequest, CommandClient as FederationCommandClient, CommandClientFuture,
    CommandContext as FederationCommandContext, CommandDispatchResult, CommandHandlerError,
    CommandId, CommandRequest, CommandResponse, CommandSnapshot, CommandStateRequest,
    CommandStateSnapshot, CommandStateStream, CommandSubmission, CommandWatch, CommandWatchFuture,
    CommandWatchingClient as FederationCommandWatchingClient, EdgeEnds, EndpointSpec, EntityRef,
    GraphEdge, ItemProjection, ItemQuery, ItemScope, LiveCollection, LiveCollectionRevision,
    LiveCollectionState, LiveSubscription, LiveSubscriptionState, LogPosition, MutationOperation,
    MykoCommand, MykoItem, MykoOperation, MykoService, Node, NodeError, NodeEvent, NodeId,
    PendingCommandSubscription, PrincipalId, ScopeId, ServiceId, ServiceTypeId,
    SubscriptionLiveness, TypedCommandClientFuture, TypedEdgeEnds, live_collection,
    live_subscription,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use tokio::task::JoinHandle;

pub use myko_app_macros::{myko_query, myko_report, myko_view};
/// Framework-selected durable command failure.
pub type CommandError = CommandHandlerError;

/// Pluggable full-text index used by the sealed [`capability::Searching`]
/// capability.
pub trait SearchProvider: fmt::Debug + Send + Sync + 'static {
    /// Returns matching stable item IDs in provider-defined relevance order.
    ///
    /// # Errors
    ///
    /// Returns a backend-safe diagnostic when the index cannot answer.
    fn search(&self, item_type: &str, query: &str, limit: usize) -> Result<Vec<Arc<str>>, String>;
}

#[derive(Clone)]
struct SearchService(Arc<dyn SearchProvider>);

/// Typed one-hop access over an ordinary Myko edge item projection.
#[derive(Clone)]
pub struct EdgeQuery<E>
where
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
{
    edges: LiveSubscription<Vec<E>>,
}

impl<E> EdgeQuery<E>
where
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
{
    const fn new(edges: LiveSubscription<Vec<E>>) -> Self {
        Self { edges }
    }

    fn endpoints(edge: &E) -> (EntityRef, EntityRef) {
        E::Ends::erase(&edge.ends())
    }

    fn current_matching(&self, predicate: impl Fn(&(EntityRef, EntityRef)) -> bool) -> Vec<Arc<E>> {
        self.edges
            .current()
            .value
            .unwrap_or_default()
            .into_iter()
            .filter(|edge| predicate(&Self::endpoints(edge)))
            .map(Arc::new)
            .collect()
    }

    /// Current edges whose A endpoint matches `value`.
    #[must_use]
    pub fn from(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Vec<Arc<E>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value);
        self.current_matching(|(a, _)| a == &endpoint)
    }

    /// Current edges whose B endpoint matches `value`.
    #[must_use]
    pub fn to(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Vec<Arc<E>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value);
        self.current_matching(|(_, b)| b == &endpoint)
    }

    /// Current edges matching an exact A/B pair.
    #[must_use]
    pub fn between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Vec<Arc<E>> {
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a);
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b);
        self.current_matching(|(edge_a, edge_b)| edge_a == &a && edge_b == &b)
    }

    /// Reactive counterpart of [`Self::from`].
    #[must_use]
    pub fn watch_from(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> LiveSubscription<Vec<E>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value);
        self.edges.map_value(move |edges| {
            edges
                .iter()
                .filter(|edge| Self::endpoints(edge).0 == endpoint)
                .cloned()
                .collect()
        })
    }

    /// Reactive counterpart of [`Self::to`].
    #[must_use]
    pub fn watch_to(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> LiveSubscription<Vec<E>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value);
        self.edges.map_value(move |edges| {
            edges
                .iter()
                .filter(|edge| Self::endpoints(edge).1 == endpoint)
                .cloned()
                .collect()
        })
    }

    /// Reactive counterpart of [`Self::between`].
    #[must_use]
    pub fn watch_between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> LiveSubscription<Vec<E>> {
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a);
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b);
        self.edges.map_value(move |edges| {
            edges
                .iter()
                .filter(|edge| {
                    let (edge_a, edge_b) = Self::endpoints(edge);
                    edge_a == a && edge_b == b
                })
                .cloned()
                .collect()
        })
    }
}

/// Direction used by a bounded typed edge traversal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Forward,
    Reverse,
    Both,
}

/// Result of a bounded breadth-first graph traversal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct TraversalResult {
    pub nodes: Vec<EntityRef>,
    pub edge_ids: Vec<Arc<str>>,
    pub truncated: bool,
}

/// Bounded traversal over one ordinary Myko edge item type.
pub struct TraversalBuilder<E>
where
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
{
    query: EdgeQuery<E>,
    start: Option<EntityRef>,
    direction: Direction,
    max_depth: Option<usize>,
    max_nodes: Option<usize>,
    max_edges: Option<usize>,
    collect_edges: bool,
}

impl<E> TraversalBuilder<E>
where
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
{
    const fn new(query: EdgeQuery<E>) -> Self {
        Self {
            query,
            start: None,
            direction: Direction::Forward,
            max_depth: None,
            max_nodes: None,
            max_edges: None,
            collect_edges: true,
        }
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn start(mut self, value: <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value) -> Self {
        self.start = Some(<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(
            &value,
        ));
        self
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn start_to(
        mut self,
        value: <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Self {
        self.start = Some(<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(
            &value,
        ));
        self
    }

    #[must_use]
    pub const fn direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    #[must_use]
    pub const fn max_depth(mut self, value: usize) -> Self {
        self.max_depth = Some(value);
        self
    }

    #[must_use]
    pub const fn max_nodes(mut self, value: usize) -> Self {
        self.max_nodes = Some(value);
        self
    }

    #[must_use]
    pub const fn max_edges(mut self, value: usize) -> Self {
        self.max_edges = Some(value);
        self
    }

    #[must_use]
    pub const fn nodes_only(mut self) -> Self {
        self.collect_edges = false;
        self
    }

    /// Executes a bounded breadth-first traversal against one coherent edge snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the required start or safety bounds are absent or zero.
    pub fn execute(self) -> Result<TraversalResult, AppError> {
        let start = self
            .start
            .ok_or_else(|| AppError::State("traversal start endpoint is required".to_owned()))?;
        let max_depth = self
            .max_depth
            .ok_or_else(|| AppError::State("traversal max_depth is required".to_owned()))?;
        let max_nodes = self
            .max_nodes
            .ok_or_else(|| AppError::State("traversal max_nodes is required".to_owned()))?;
        if max_nodes == 0 {
            return Err(AppError::State(
                "traversal max_nodes must be greater than zero".to_owned(),
            ));
        }
        let max_edges = self.max_edges.unwrap_or(usize::MAX);
        if max_edges == 0 {
            return Err(AppError::State(
                "traversal max_edges must be greater than zero".to_owned(),
            ));
        }
        let edges = self.query.edges.current().value.unwrap_or_default();
        let mut visited = BTreeSet::from([start.clone()]);
        let mut queue = VecDeque::from([(start.clone(), 0_usize)]);
        let mut edge_ids = BTreeSet::new();
        let mut traversed_edges = 0_usize;
        let mut truncated = false;
        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            for edge in &edges {
                let (a, b) = EdgeQuery::<E>::endpoints(edge);
                let neighbor = match self.direction {
                    Direction::Forward | Direction::Both if a == node => Some(b),
                    Direction::Reverse | Direction::Both if b == node => Some(a),
                    Direction::Forward | Direction::Reverse | Direction::Both => None,
                };
                let Some(neighbor) = neighbor else {
                    continue;
                };
                if traversed_edges >= max_edges {
                    truncated = true;
                    break;
                }
                traversed_edges = traversed_edges.saturating_add(1);
                if self.collect_edges {
                    edge_ids.insert(Arc::<str>::from(edge.id().as_ref()));
                }
                if visited.contains(&neighbor) {
                    continue;
                }
                if visited.len().saturating_sub(1) >= max_nodes {
                    truncated = true;
                    break;
                }
                visited.insert(neighbor.clone());
                queue.push_back((neighbor, depth.saturating_add(1)));
            }
            if truncated {
                break;
            }
        }
        visited.remove(&start);
        Ok(TraversalResult {
            nodes: visited.into_iter().collect(),
            edge_ids: edge_ids.into_iter().collect(),
            truncated,
        })
    }
}

/// One type-erased item retained as its original Rust value.
pub trait ErasedItem: fmt::Debug + Send + Sync + 'static {
    fn service_id(&self) -> ServiceTypeId;
    fn item_type(&self) -> &'static str;
    fn id(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
}

impl<T: MykoItem> ErasedItem for T {
    fn service_id(&self) -> ServiceTypeId {
        T::SERVICE_ID
    }

    fn item_type(&self) -> &'static str {
        T::ITEM_TYPE
    }

    fn id(&self) -> &str {
        self.id().as_ref()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

type ErasedItemReader = dyn Fn(&Node, NodeId, &ScopeId) -> Result<Vec<Arc<dyn ErasedItem>>, AppError>
    + Send
    + Sync
    + 'static;
type ItemTopologyRestorer = dyn Fn(&Node) -> Result<(), AppError> + Send + Sync + 'static;

struct RegisteredItemSchema {
    reader: Arc<ErasedItemReader>,
    restore_topology: Arc<ItemTopologyRestorer>,
}

type ItemReaderMap = BTreeMap<(ServiceTypeId, &'static str), RegisteredItemSchema>;

/// Runtime item-schema registry populated by activated Myko modules.
///
/// It preserves v6's relationship-graph escape hatch for runtime-determined
/// item types while keeping values typed until a caller explicitly downcasts.
#[derive(Clone, Default)]
pub struct ItemRegistry {
    readers: Arc<RwLock<ItemReaderMap>>,
}

impl fmt::Debug for ItemRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.readers.read().map_or(0, |readers| readers.len());
        formatter
            .debug_struct("ItemRegistry")
            .field("registered_items", &count)
            .finish()
    }
}

impl ItemRegistry {
    fn register<T: MykoItem>(&self) -> Result<(), AppError> {
        let reader = |node: &Node, source_node: NodeId, scope_id: &ScopeId| {
            let projection = replay_items::<T>(node, source_node, scope_id, None)?;
            Ok(projection
                .values()
                .cloned()
                .map(|item| {
                    let item: Arc<dyn ErasedItem> = Arc::new(item);
                    item
                })
                .collect())
        };
        let restore_topology = |node: &Node| restore_item_scope_topology::<T>(node);
        let previous = self
            .readers
            .write()
            .map_err(|_| AppError::State("item registry is poisoned".to_owned()))?
            .insert(
                (T::SERVICE_ID, T::ITEM_TYPE),
                RegisteredItemSchema {
                    reader: Arc::new(reader),
                    restore_topology: Arc::new(restore_topology),
                },
            );
        if previous.is_some() {
            return Err(AppError::DuplicateItemRegistration {
                service_id: T::SERVICE_ID,
                item_type: T::ITEM_TYPE,
            });
        }
        Ok(())
    }

    /// Projects a runtime-selected item schema without converting values to JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema is inactive or history is malformed.
    pub fn items(
        &self,
        node: &Node,
        source_node: NodeId,
        scope_id: &ScopeId,
        service_id: &str,
        item_type: &str,
    ) -> Result<Vec<Arc<dyn ErasedItem>>, AppError> {
        let reader = self
            .readers
            .read()
            .map_err(|_| AppError::State("item registry is poisoned".to_owned()))?
            .iter()
            .find_map(|((registered_service, registered_item), schema)| {
                (registered_service.as_str() == service_id && *registered_item == item_type)
                    .then(|| Arc::clone(&schema.reader))
            })
            .ok_or_else(|| AppError::UnregisteredItem {
                service_id: service_id.to_owned(),
                item_type: item_type.to_owned(),
            })?;
        reader(node, source_node, scope_id)
    }

    fn restore_topology(&self, node: &Node) -> Result<(), AppError> {
        let restorers = self
            .readers
            .read()
            .map_err(|_| AppError::State("item registry is poisoned".to_owned()))?
            .values()
            .map(|schema| Arc::clone(&schema.restore_topology))
            .collect::<Vec<_>>();
        for restore in restorers {
            restore(node)?;
        }
        Ok(())
    }
}

/// Typed process-local services made available to sealed handler contexts.
///
/// Values never cross a transport boundary. They are application capabilities
/// such as an index, workspace catalog, or provider runtime, and are looked up
/// by Rust type rather than by service-name strings.
#[derive(Clone)]
pub struct ApplicationResources {
    values: Arc<RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>,
}

impl Default for ApplicationResources {
    fn default() -> Self {
        let mut values = HashMap::<TypeId, Arc<dyn Any + Send + Sync>>::new();
        values.insert(
            TypeId::of::<ItemRegistry>(),
            Arc::new(ItemRegistry::default()),
        );
        Self {
            values: Arc::new(RwLock::new(values)),
        }
    }
}

impl fmt::Debug for ApplicationResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.values.read().map_or(0, |values| values.len());
        formatter
            .debug_struct("ApplicationResources")
            .field("registered_types", &count)
            .finish()
    }
}

impl ApplicationResources {
    /// Installs or replaces one typed application service.
    ///
    /// # Errors
    ///
    /// Returns an error if another thread poisoned the resource registry.
    pub fn insert<T>(&self, value: T) -> Result<Option<Arc<T>>, AppError>
    where
        T: Send + Sync + 'static,
    {
        let previous = self
            .values
            .write()
            .map_err(|_| AppError::State("application resource registry is poisoned".to_owned()))?
            .insert(TypeId::of::<T>(), Arc::new(value));
        Ok(previous.and_then(|value| value.downcast::<T>().ok()))
    }

    /// Resolves one typed application service.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry is unavailable or `T` was not installed.
    pub fn get<T>(&self) -> Result<Arc<T>, AppError>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .read()
            .map_err(|_| AppError::State("application resource registry is poisoned".to_owned()))?
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
            .ok_or_else(|| AppError::MissingResource {
                type_name: std::any::type_name::<T>(),
            })
    }
}

/// Application execution contract generated and required by `#[myko_command]`.
///
/// A command declaration without its handler is a compile error:
///
/// ```compile_fail
/// use myko_items::{myko_command, myko_item, myko_service};
///
/// #[myko_service(Record)]
/// pub struct DocsService;
///
/// #[myko_item(service = DocsService)]
/// pub struct Record {}
///
/// #[myko_command(item = Record)]
/// struct MissingCommandHandler;
/// ```
pub trait CommandHandler: MykoCommand {
    /// Selects the concrete entity whose typed ID defines this command's scope.
    fn scope(&self, node_id: NodeId) -> <Self::Scope as MykoItem>::Id;

    /// Executes against Myko's sealed command capability context.
    ///
    /// # Errors
    ///
    /// Return [`CommandError::Reject`] for terminal domain failures or
    /// [`CommandError::Retry`] when a transient dependency should be retried.
    fn execute(
        self,
        context: CommandContext<Self::Service, Self::Scope>,
    ) -> Result<Self::Output, CommandError>;
}

/// Typed application-command client with all Myko admission mechanics hidden.
pub trait CommandClient: FederationCommandWatchingClient {
    /// Durably admits one typed command without exposing its wire envelope.
    fn submit_command<C>(&self, command: C) -> CommandClientFuture<'_, Self::Error>
    where
        Self: Sized,
        C: CommandHandler,
    {
        self.submit_typed_command(command)
    }

    /// Executes one bounded typed command and awaits its typed result.
    fn exec_command<C>(&self, command: C) -> TypedCommandClientFuture<'_, C::Output, Self::Error>
    where
        Self: Sized,
        C: CommandHandler,
    {
        self.exec_typed_command(command)
    }
}

impl<T> CommandClient for T where T: FederationCommandWatchingClient {}

/// Application query handler with access to Myko's reactive build context.
///
/// A custom query declaration without this handler is a compile error:
///
/// ```compile_fail
/// use myko_app::myko_query;
/// use myko_items::{ItemProjection, ItemQuery, myko_item, myko_service};
///
/// #[myko_service(Record)]
/// pub struct DocsService;
///
/// #[myko_item(service = DocsService)]
/// pub struct Record {}
///
/// #[myko_query(Record)]
/// struct MissingQueryHandler;
///
/// impl ItemQuery for MissingQueryHandler {
///     type Item = Record;
///     type Output = Vec<Record>;
///
///     fn execute(self, projection: &ItemProjection<Record>) -> Self::Output {
///         projection.values().cloned().collect()
///     }
/// }
/// ```
///
/// The low-level [`ItemQuery`] contract remains transport-neutral. Implementing
/// this trait opts a custom query into application registration and allows it
/// to compose other reactive dependencies through [`QueryBuildContext`].
pub trait QueryHandler: ItemQuery {
    /// Builds this query's long-lived result.
    ///
    /// Simple handlers can use the default projection-backed implementation;
    /// complex handlers override it to compose other query/report/view cells.
    ///
    /// # Errors
    ///
    /// Returns an error when the reactive dependency graph cannot be built.
    fn build(
        &self,
        context: &QueryBuildContext,
    ) -> Result<LiveSubscription<Self::Output>, AppError> {
        context
            .core
            .query(context.source_node, context.scope_id.clone(), self.clone())
    }
}

/// Implementation details referenced by Myko's generated registration code.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

/// Sealed handler capabilities. Context types opt into only the operations
/// valid for that handler class; downstream code can call but cannot forge
/// capabilities.
pub mod capability {
    use std::sync::Arc;

    use myko_federation::{
        CommandContext as FederationCommandContext, CommandRequest, CommandSnapshot, ItemQuery,
        MykoCommand, MykoItem, Node, NodeError,
    };

    use super::{ApplicationResources, CommandContext, CommandError, CommandHandler, ItemRegistry};

    pub(crate) mod sealed {
        pub trait Sealed {}
    }

    /// Access to immutable request identity and authorization metadata.
    pub trait RequestScoped: sealed::Sealed {
        #[doc(hidden)]
        fn __request(&self) -> &Arc<CommandRequest>;

        #[doc(hidden)]
        fn __scope_id(&self) -> &myko_federation::ScopeId {
            &self.__request().scope_id
        }

        fn command_id(&self) -> myko_federation::CommandId {
            self.__request().id
        }

        fn principal_id(&self) -> &myko_federation::PrincipalId {
            &self.__request().principal_id
        }

        fn scope_id(&self) -> &myko_federation::ScopeId {
            self.__scope_id()
        }
    }

    /// The sealed accessor for capabilities backed by a live Myko node.
    pub trait NodeScoped: sealed::Sealed {
        #[doc(hidden)]
        fn __node(&self) -> &Node;

        /// Returns the identity of the node executing this handler.
        fn node_id(&self) -> myko_federation::NodeId {
            self.__node().node_id()
        }
    }

    /// Typed access to application-installed process-local services.
    pub trait ResourceScoped: sealed::Sealed {
        #[doc(hidden)]
        fn __resources(&self) -> &ApplicationResources;

        /// Resolves a service by Rust type, without stringly typed wiring.
        ///
        /// # Errors
        ///
        /// Returns an error when the service is not installed.
        fn resource<T>(&self) -> Result<Arc<T>, super::AppError>
        where
            T: Send + Sync + 'static,
        {
            self.__resources().get::<T>()
        }
    }

    /// Access to the runtime item schemas activated by the application.
    pub trait RegistryScoped: ResourceScoped + NodeScoped {
        /// Returns the type-erased, Rust-value item registry used by graph walks.
        ///
        /// # Errors
        ///
        /// Returns an error only when application construction was corrupted.
        fn registry(&self) -> Result<Arc<ItemRegistry>, super::AppError> {
            self.resource::<ItemRegistry>()
        }
    }

    /// Read-only graph access over ordinary activated Myko edge items.
    pub trait GraphQuerying: ReactiveScoped {
        /// Opens typed one-hop access to one edge projection.
        ///
        /// # Errors
        ///
        /// Returns an error when the edge snapshot/follow dependency cannot be established.
        fn edges<E>(
            &self,
            source_node: myko_federation::NodeId,
            scope_id: myko_federation::ScopeId,
        ) -> Result<super::EdgeQuery<E>, super::AppError>
        where
            E: myko_federation::GraphEdge,
            E::Ends: myko_federation::TypedEdgeEnds,
            E::GetAllQuery: Default,
        {
            self.__reactive()
                .query(source_node, scope_id, E::GetAllQuery::default())
                .map(super::EdgeQuery::new)
        }

        /// Starts a bounded traversal over one typed edge projection.
        ///
        /// # Errors
        ///
        /// Returns an error when the edge snapshot/follow dependency cannot be established.
        fn traverse<E>(
            &self,
            source_node: myko_federation::NodeId,
            scope_id: myko_federation::ScopeId,
        ) -> Result<super::TraversalBuilder<E>, super::AppError>
        where
            E: myko_federation::GraphEdge,
            E::Ends: myko_federation::TypedEdgeEnds,
            E::GetAllQuery: Default,
        {
            self.edges(source_node, scope_id)
                .map(super::TraversalBuilder::new)
        }
    }

    /// Read-only full-text search over an application-installed index.
    pub trait Searching: ResourceScoped {
        /// Returns matching stable item IDs in provider-defined relevance order.
        ///
        /// # Errors
        ///
        /// Returns an error when no provider is installed or the backend fails.
        fn search(
            &self,
            item_type: &str,
            query: &str,
            limit: usize,
        ) -> Result<Vec<Arc<str>>, super::AppError> {
            self.resource::<super::SearchService>()?
                .0
                .search(item_type, query, limit)
                .map_err(super::AppError::State)
        }
    }

    /// Point-in-time typed reads available to command handlers.
    pub trait CommandQuerying: RequestScoped + NodeScoped {
        /// Executes a typed query in the command's authoritative scope.
        ///
        /// # Errors
        ///
        /// Returns an error when the projection cannot be read.
        fn exec_query<Q>(&self, query: Q) -> Result<Q::Output, CommandError>
        where
            Q: ItemQuery,
        {
            self.__node()
                .query_items_in(self.__node().node_id(), self.scope_id(), query)
                .map_err(|error| CommandError::retry(error.to_string()))
        }
    }

    /// Typed atomic item publication. Read-only contexts do not implement it.
    pub trait EventPublishing: sealed::Sealed {
        type Service: myko_federation::MykoService;
        type Scope: MykoItem;

        #[doc(hidden)]
        fn __federation_command_context(&self) -> &FederationCommandContext;

        #[doc(hidden)]
        fn __mutation_scope_id(&self) -> &myko_federation::ScopeId;

        /// Adds a typed replacement to the command's atomic batch.
        ///
        /// # Errors
        ///
        /// Service mismatches are rejected by the type system. Scoped foreign
        /// keys are checked against the active nested-command scope.
        fn emit_set<T>(&self, item: &T) -> Result<(), CommandError>
        where
            T: MykoItem<Service = Self::Service, Scope = Self::Scope>,
        {
            if matches!(
                T::SCOPE,
                myko_federation::ItemScope::ScopedBy { .. }
                    | myko_federation::ItemScope::RootScopedBy { .. }
            ) {
                let declared_scope = myko_federation::ScopeId::for_entity(&item.scope_ref());
                if &declared_scope != self.__mutation_scope_id() {
                    return Err(CommandError::reject(format!(
                        "item belongs to scope {declared_scope}; active command scope is {}",
                        self.__mutation_scope_id()
                    )));
                }
            }
            self.__federation_command_context()
                .emit_set_in(self.__mutation_scope_id(), item)
                .map_err(|error| CommandError::retry(error.to_string()))
        }

        /// Adds a typed deletion to the command's atomic batch.
        ///
        /// # Errors
        ///
        /// Service mismatches are rejected by the type system; defensive
        /// runtime validation may still fail.
        fn emit_delete<T>(&self, id: &T::Id) -> Result<(), CommandError>
        where
            T: MykoItem<Service = Self::Service, Scope = Self::Scope>,
        {
            self.__federation_command_context()
                .emit_delete_in::<T>(self.__mutation_scope_id(), id)
                .map_err(|error| CommandError::retry(error.to_string()))
        }
    }

    /// Bounded in-process command composition within one atomic command.
    ///
    /// A nested command owned by another service is a compile error:
    ///
    /// ```compile_fail
    /// use myko_app::{CommandContext, CommandError, CommandHandler};
    /// use myko_app::capability::CommandExecuting as _;
    /// use myko_federation::NodeId;
    /// use myko_items::{myko_command, myko_item, myko_service};
    ///
    /// #[myko_service(Alpha)]
    /// pub struct AlphaService;
    /// #[myko_item(service = AlphaService, scope_root)]
    /// pub struct Alpha {}
    ///
    /// #[myko_service(Beta)]
    /// pub struct BetaService;
    /// #[myko_item(service = BetaService, scope_root)]
    /// pub struct Beta {}
    ///
    /// #[myko_command(bool, item = Beta)]
    /// struct ChangeBeta { id: BetaId }
    /// impl CommandHandler for ChangeBeta {
    ///     fn scope(&self, _node_id: NodeId) -> BetaId { self.id.clone() }
    ///     fn execute(
    ///         self,
    ///         _context: CommandContext<BetaService, Beta>,
    ///     ) -> Result<bool, CommandError> { Ok(true) }
    /// }
    ///
    /// #[myko_command(bool, item = Alpha)]
    /// struct ChangeAlpha { id: AlphaId, beta_id: BetaId }
    /// impl CommandHandler for ChangeAlpha {
    ///     fn scope(&self, _node_id: NodeId) -> AlphaId { self.id.clone() }
    ///     fn execute(
    ///         self,
    ///         context: CommandContext<AlphaService, Alpha>,
    ///     ) -> Result<bool, CommandError> {
    ///         context.exec_command(ChangeBeta { id: self.beta_id })
    ///     }
    /// }
    /// ```
    ///
    /// Nested commands inherit the admitted command's stable identity,
    /// principal, resources, and atomic service batch. Each nested command
    /// selects its own concrete scope inside that batch. They are trusted
    /// in-process service implementation and do not perform another transport
    /// authorization check. Work that needs its own durable lifecycle must
    /// instead be admitted as a separate command.
    pub trait CommandExecuting: sealed::Sealed {
        type Service: myko_federation::MykoService;
        type Scope: MykoItem;

        #[doc(hidden)]
        fn __typed_command_context(&self) -> &CommandContext<Self::Service, Self::Scope>;

        /// Executes another typed handler in this command's atomic context.
        ///
        /// Commands from another service do not type-check. Different scopes
        /// in this service share the outer command's atomic change batch.
        ///
        /// # Errors
        ///
        /// Returns the nested handler's explicit rejection or retry.
        fn exec_command<C>(&self, command: C) -> Result<C::Output, CommandError>
        where
            C: CommandHandler<Service = Self::Service>,
        {
            let context = self.__typed_command_context();
            let nested_scope = command.scope(context.node_id());
            let nested_scope_id = myko_federation::ScopeId::for_item::<C::Scope>(&nested_scope);
            command.execute(context.retarget::<C::Scope>(nested_scope_id))
        }
    }

    /// Nested durable command submission, independent from item publication.
    pub trait CommandSending: NodeScoped + RequestScoped {
        /// Submits a nested typed command to the same node.
        ///
        /// # Errors
        ///
        /// Returns an error when durable admission fails.
        fn submit_command<C: CommandHandler>(
            &self,
            command: C,
        ) -> Result<CommandSnapshot, CommandError> {
            let scope = command.scope(self.node_id());
            let scope_id = myko_federation::ScopeId::for_item::<C::Scope>(&scope);
            self.__node()
                .submit_authenticated_command(scope_id, self.principal_id().clone(), &command)
                .map_err(|error| CommandError::retry(error.to_string()))
        }
    }

    /// Sealed access to the dependency runtime shared by one reactive handler.
    pub trait ReactiveScoped: NodeScoped {
        #[doc(hidden)]
        fn __reactive(&self) -> &super::ContextCore;
    }

    /// Gap-free reactive item queries. Only reactive read contexts implement it.
    pub trait Querying: ReactiveScoped {
        /// Opens a replay-then-live typed item query.
        ///
        /// # Errors
        ///
        /// Returns an error when the snapshot or live dependency cannot be established.
        fn query<Q>(
            &self,
            source_node: myko_federation::NodeId,
            scope_id: myko_federation::ScopeId,
            query: Q,
        ) -> Result<myko_federation::LiveSubscription<Q::Output>, super::AppError>
        where
            Q: ItemQuery,
        {
            self.__reactive().query(source_node, scope_id, query)
        }

        /// Opens a replay-then-live typed query across every scope owned by one
        /// authoritative source.
        ///
        /// # Errors
        ///
        /// Returns an error when the snapshot or live dependency cannot be established.
        fn query_from<Q>(
            &self,
            source_node: myko_federation::NodeId,
            query: Q,
        ) -> Result<myko_federation::LiveSubscription<Q::Output>, super::AppError>
        where
            Q: ItemQuery,
        {
            self.__reactive().query_from(source_node, query)
        }
    }

    /// Dynamic federation fan-in over every authoritative item source.
    pub trait FederatedQuerying: ReactiveScoped {
        /// Opens a replay-then-live typed query across every authoritative
        /// source represented in one application scope.
        ///
        /// # Errors
        ///
        /// Returns an error when the snapshot or live dependency cannot be established.
        fn query_across_sources<Q>(
            &self,
            scope_id: myko_federation::ScopeId,
            query: Q,
        ) -> Result<myko_federation::LiveSubscription<Q::Output>, super::AppError>
        where
            Q: ItemQuery,
        {
            self.__reactive().query_across_sources(scope_id, query)
        }

        /// Opens a reactive projection across every authoritative source.
        ///
        /// # Errors
        ///
        /// Returns an error when history or its live continuation is invalid.
        fn federated_items<T>(
            &self,
            scope_id: myko_federation::ScopeId,
        ) -> Result<myko_federation::LiveSubscription<Vec<super::SourcedItem<T>>>, super::AppError>
        where
            T: MykoItem,
        {
            self.__reactive().federated_items(scope_id)
        }
    }

    /// Reactive command-lifecycle reads. This grants no command execution.
    pub trait CommandWatching: ReactiveScoped {
        /// Opens a gap-free typed command-state subscription.
        ///
        /// # Errors
        ///
        /// Returns an error when the command catalog cannot be materialized.
        fn commands<C>(
            &self,
            source_node: myko_federation::NodeId,
            scope_id: myko_federation::ScopeId,
        ) -> Result<
            myko_federation::LiveSubscription<myko_federation::CommandStateSnapshot>,
            super::AppError,
        >
        where
            C: MykoCommand,
        {
            self.__reactive().commands::<C>(source_node, scope_id)
        }
    }

    /// Compose a reactive report inside another read handler.
    pub trait Reporting: ReactiveScoped {
        /// Builds a sub-report in the current dependency runtime.
        ///
        /// # Errors
        ///
        /// Returns an error when the sub-report cannot be constructed.
        fn report<R>(
            &self,
            report: &R,
        ) -> Result<myko_federation::LiveSubscription<R::Output, R::Cursor>, super::AppError>
        where
            R: super::ReportHandler,
        {
            report.build(&super::ReportContext {
                core: self.__reactive().clone(),
            })
        }
    }

    /// Compose a keyed reactive view inside another read handler.
    pub trait Viewing: ReactiveScoped {
        /// Builds a sub-view in the current dependency runtime.
        ///
        /// # Errors
        ///
        /// Returns an error when the sub-view cannot be constructed.
        fn view<V>(
            &self,
            view: &V,
        ) -> Result<myko_federation::LiveCollection<V::Item, V::Cursor>, super::AppError>
        where
            V: super::ViewHandler,
        {
            view.build(&super::ViewContext {
                core: self.__reactive().clone(),
            })
        }
    }

    /// Adapt a whole-collection subscription into a keyed view lifecycle.
    pub trait CollectionBuilding: ReactiveScoped {
        /// Converts whole snapshots into identity-preserving view revisions.
        ///
        /// # Errors
        ///
        /// Returns an error when duplicate keys or dependency ownership are invalid.
        fn collection_from_subscription<T, C>(
            &self,
            live: &myko_federation::LiveSubscription<Vec<T>, C>,
            item_key: impl Fn(&T) -> Arc<str> + Send + Sync + 'static,
        ) -> Result<myko_federation::LiveCollection<T, C>, super::AppError>
        where
            T: hyphae::CellValue,
            C: hyphae::CellValue,
        {
            self.__reactive()
                .collection_from_subscription(live, item_key)
        }
    }

    /// Read immutable durable node history without gaining write capability.
    pub trait HistoryReading: NodeScoped {
        /// Reads durable envelopes after an optional node-local cursor.
        ///
        /// # Errors
        ///
        /// Returns an error when durable history is unavailable or corrupt.
        fn history_after(
            &self,
            after: Option<myko_federation::LogPosition>,
        ) -> Result<Vec<myko_federation::EventEnvelope>, super::AppError> {
            self.__node()
                .events_after(after)
                .map_err(super::AppError::Node)
        }
    }

    /// Point-in-time typed replay over durable item history.
    pub trait Replaying: HistoryReading {
        /// Reconstructs one typed service/scope projection through `until`.
        ///
        /// # Errors
        ///
        /// Returns an error when history contains an invalid typed mutation.
        fn replay_items<T: MykoItem>(
            &self,
            source_node: myko_federation::NodeId,
            scope_id: &myko_federation::ScopeId,
            until: Option<myko_federation::LogPosition>,
        ) -> Result<myko_federation::ItemProjection<T>, super::AppError> {
            super::replay_items(self.__node(), source_node, scope_id, until)
        }
    }

    const fn command_capability_matrix<S, R, C>()
    where
        S: myko_federation::MykoService,
        R: MykoItem,
        C: RequestScoped
            + ResourceScoped
            + RegistryScoped
            + NodeScoped
            + CommandQuerying
            + EventPublishing<Service = S, Scope = R>
            + CommandExecuting<Service = S, Scope = R>
            + CommandSending,
    {
    }

    #[allow(dead_code)]
    const fn command_context_capability_matrix<S, R>()
    where
        S: myko_federation::MykoService,
        R: MykoItem,
    {
        command_capability_matrix::<S, R, CommandContext<S, R>>();
    }

    const fn reactive_capability_matrix<C>()
    where
        C: RegistryScoped
            + ResourceScoped
            + NodeScoped
            + ReactiveScoped
            + Querying
            + GraphQuerying
            + Searching
            + FederatedQuerying
            + CommandWatching
            + Reporting
            + Viewing
            + CollectionBuilding,
    {
    }

    const _: () = reactive_capability_matrix::<super::ReportContext>();
    const _: () = reactive_capability_matrix::<super::ViewContext>();

    const fn query_build_capability_matrix<C>()
    where
        C: RegistryScoped
            + ResourceScoped
            + NodeScoped
            + ReactiveScoped
            + Querying
            + GraphQuerying
            + Searching
            + Reporting
            + HistoryReading,
    {
    }

    const _: () = query_build_capability_matrix::<super::QueryBuildContext>();

    const fn history_capability<C: HistoryReading>() {}
    const fn replay_capability<C: Replaying>() {}

    const _: () = history_capability::<super::ReportContext>();
    const _: () = history_capability::<super::QueryBuildContext>();
    const _: () = replay_capability::<super::ReportContext>();

    #[allow(dead_code)]
    fn _node_error_is_deliberately_not_the_handler_error(_: NodeError) {}
}

/// Framework-owned context supplied to every command handler.
///
/// Its capabilities are structural: it can read command-scoped state, compose
/// nested handlers into one atomic item batch, and submit separately lived
/// commands. Report and view contexts do not implement the write traits.
pub struct CommandContext<S: MykoService, R: MykoItem> {
    request: Arc<myko_federation::CommandRequest>,
    scope_id: ScopeId,
    inner: FederationCommandContext,
    resources: ApplicationResources,
    boundary: PhantomData<fn() -> (S, R)>,
}

impl<S: MykoService, R: MykoItem> CommandContext<S, R> {
    #[doc(hidden)]
    #[must_use]
    pub fn from_federation(
        inner: FederationCommandContext,
        resources: ApplicationResources,
    ) -> Self {
        Self {
            request: Arc::new(inner.request().clone()),
            scope_id: inner.request().scope_id.clone(),
            inner,
            resources,
            boundary: PhantomData,
        }
    }

    fn retarget<R2: MykoItem>(&self, scope_id: ScopeId) -> CommandContext<S, R2> {
        CommandContext {
            request: self.request.clone(),
            scope_id,
            inner: self.inner.clone(),
            resources: self.resources.clone(),
            boundary: PhantomData,
        }
    }
}

impl<S: MykoService, R: MykoItem> Clone for CommandContext<S, R> {
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            scope_id: self.scope_id.clone(),
            inner: self.inner.clone(),
            resources: self.resources.clone(),
            boundary: PhantomData,
        }
    }
}

impl<S: MykoService, R: MykoItem> fmt::Debug for CommandContext<S, R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandContext")
            .field("request", &self.request)
            .field("inner", &self.inner)
            .field("resources", &self.resources)
            .finish_non_exhaustive()
    }
}

impl<S: MykoService, R: MykoItem> capability::sealed::Sealed for CommandContext<S, R> {}

impl<S: MykoService, R: MykoItem> capability::RequestScoped for CommandContext<S, R> {
    fn __request(&self) -> &Arc<myko_federation::CommandRequest> {
        &self.request
    }

    fn __scope_id(&self) -> &ScopeId {
        &self.scope_id
    }
}

impl<S: MykoService, R: MykoItem> capability::NodeScoped for CommandContext<S, R> {
    fn __node(&self) -> &Node {
        self.inner.node()
    }
}

impl<S: MykoService, R: MykoItem> capability::ResourceScoped for CommandContext<S, R> {
    fn __resources(&self) -> &ApplicationResources {
        &self.resources
    }
}
impl<S: MykoService, R: MykoItem> capability::RegistryScoped for CommandContext<S, R> {}

impl<S: MykoService, R: MykoItem> capability::CommandQuerying for CommandContext<S, R> {}
impl<S: MykoService, R: MykoItem> capability::EventPublishing for CommandContext<S, R> {
    type Service = S;
    type Scope = R;

    fn __federation_command_context(&self) -> &FederationCommandContext {
        &self.inner
    }

    fn __mutation_scope_id(&self) -> &ScopeId {
        &self.scope_id
    }
}
impl<S: MykoService, R: MykoItem> capability::CommandExecuting for CommandContext<S, R> {
    type Service = S;
    type Scope = R;

    fn __typed_command_context(&self) -> &Self {
        self
    }
}
impl<S: MykoService, R: MykoItem> capability::CommandSending for CommandContext<S, R> {}

/// Failure while registering or building an application reactive handler.
#[derive(Debug, Error)]
pub enum AppError {
    /// Durable state could not be projected or followed.
    #[error(transparent)]
    Node(#[from] NodeError),
    /// The application attempted to register the same stable contract twice.
    #[error("duplicate {kind} handler ID {id}")]
    DuplicateHandler { kind: &'static str, id: String },
    /// The application attempted to activate the same service twice.
    #[error("duplicate application service ID {id}")]
    DuplicateService { id: String },
    /// A caller requested a contract the application did not register.
    #[error("unregistered {kind} handler ID {id}")]
    UnregisteredHandler { kind: &'static str, id: String },
    /// Reactive dependency ownership could not be updated.
    #[error("reactive application state unavailable: {0}")]
    State(String),
    /// Handler parameters or lifecycle state could not be encoded.
    #[error("handler serialization failed: {0}")]
    Serialization(String),
    /// A handler requested a process-local service its application did not install.
    #[error("application resource {type_name} is not installed")]
    MissingResource { type_name: &'static str },
    /// An activated service attempted to register one item schema twice.
    #[error("item schema {service_id}/{item_type} is already registered")]
    DuplicateItemRegistration {
        service_id: ServiceTypeId,
        item_type: &'static str,
    },
    /// A runtime graph walk requested an item schema outside the application.
    #[error("item schema {service_id}/{item_type} is not registered")]
    UnregisteredItem {
        service_id: String,
        item_type: String,
    },
}

/// Kind of application-owned reactive handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandlerKind {
    Command,
    Query,
    Report,
    View,
}

impl HandlerKind {
    /// Returns the stable lowercase wire name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
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

/// One incremental update to a keyed application view.
///
/// A handler stream always starts with an [`ErasedHandlerState`] snapshot. Once
/// established, keyed views can send only changed rows while retaining the
/// cursor, liveness, and ordering of the authoritative collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ErasedViewDelta {
    pub upserts: Vec<Value>,
    pub deletes: Vec<String>,
    /// Replacement row ordering when membership or order changed.
    ///
    /// `None` retains the order from the preceding authoritative snapshot,
    /// avoiding a full list of row IDs when only a row's contents changed.
    pub order: Option<Vec<String>>,
    pub through: Option<Value>,
    pub liveness: SubscriptionLiveness,
}

/// One current typed item together with its immutable authoritative source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct SourcedItem<T> {
    pub source_node: NodeId,
    pub item: T,
}

/// Application-defined reactive scalar or aggregate.
///
/// ```compile_fail
/// use myko_app::myko_report;
///
/// #[myko_report(u64)]
/// struct MissingReportHandler;
/// ```
pub trait ReportHandler:
    MykoOperation + Clone + fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Output: hyphae::CellValue + Serialize + DeserializeOwned;
    type Cursor: hyphae::CellValue + Serialize + DeserializeOwned;

    /// Stable application wire identity for this report.
    const REPORT_ID: &'static str = Self::OPERATION_ID;

    /// Returns the federation scope whose authority protects this report.
    ///
    /// Reports spanning public or multiple scopes may leave this unset. A
    /// scoped report should derive the value from its typed parameters so
    /// transports can authorize it without application-defined wire metadata.
    #[must_use]
    fn access_scope(&self) -> Option<ScopeId> {
        None
    }

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
        context: &ReportContext,
    ) -> Result<LiveSubscription<Self::Output, Self::Cursor>, AppError>;
}

/// Application-defined reactive collection or joined read model.
///
/// ```compile_fail
/// use myko_app::myko_view;
///
/// #[myko_view(String)]
/// struct MissingViewHandler;
/// ```
pub trait ViewHandler:
    MykoOperation + Clone + fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Item: hyphae::CellValue + Serialize + DeserializeOwned;
    type Cursor: hyphae::CellValue + Serialize + DeserializeOwned;

    /// Stable application wire identity for this view.
    const VIEW_ID: &'static str = Self::OPERATION_ID;

    /// Returns the federation scope whose authority protects this view.
    ///
    /// Views spanning public or multiple scopes may leave this unset. A
    /// scoped view should derive the value from its typed parameters so
    /// transports can authorize it without application-defined wire metadata.
    #[must_use]
    fn access_scope(&self) -> Option<ScopeId> {
        None
    }

    /// Returns the stable identity of one row across view revisions.
    fn item_key(item: &Self::Item) -> Arc<str>;

    /// Builds the view once from long-lived reactive dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error when a reactive dependency cannot be established or
    /// the application cannot construct a valid view state.
    fn build(
        &self,
        context: &ViewContext,
    ) -> Result<LiveCollection<Self::Item, Self::Cursor>, AppError>;
}

/// Typed authorization helpers for application handler subscriptions.
///
/// Handler topic encoding is an internal transport concern. Access policies
/// use these helpers so application code never constructs or compares those
/// wire topic strings itself.
pub trait HandlerAccessRequest {
    /// Returns whether this request follows exactly the given item query.
    #[must_use]
    fn query_is<Q: ItemQuery>(&self) -> bool;

    /// Returns whether this request follows exactly the given report.
    #[must_use]
    fn report_is<R: ReportHandler>(&self) -> bool;

    /// Returns whether this request follows exactly the given view.
    #[must_use]
    fn view_is<V: ViewHandler>(&self) -> bool;
}

impl HandlerAccessRequest for AccessRequest {
    fn query_is<Q: ItemQuery>(&self) -> bool {
        follows_handler(self, HandlerKind::Query, Q::QUERY_ID)
    }

    fn report_is<R: ReportHandler>(&self) -> bool {
        follows_handler(self, HandlerKind::Report, R::REPORT_ID)
    }

    fn view_is<V: ViewHandler>(&self) -> bool {
        follows_handler(self, HandlerKind::View, V::VIEW_ID)
    }
}

fn follows_handler(request: &AccessRequest, kind: HandlerKind, handler_id: &str) -> bool {
    let [topic] = request.live_topics.as_slice() else {
        return false;
    };
    request.operation == AccessOperation::FollowHandler
        && topic == &format!("handler:{}:{handler_id}", kind.as_str())
}

/// Stable generated identity of one application service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MykoServiceId {
    pub service_id: ServiceTypeId,
}

impl MykoServiceId {
    #[must_use]
    pub const fn of<S: MykoService>() -> Self {
        Self {
            service_id: S::SERVICE_ID,
        }
    }
}

const fn service_type_id<S: MykoService>() -> TypeId {
    TypeId::of::<S>()
}

/// Inventory entry generated for one item-owned application handler.
#[doc(hidden)]
pub struct HandlerRegistration {
    owner: Option<fn() -> TypeId>,
    kind: HandlerKind,
    handler_id: &'static str,
    register: fn(&mut MykoApplication) -> Result<(), AppError>,
}

impl HandlerRegistration {
    /// Creates a generated item-owned command registration.
    #[doc(hidden)]
    #[must_use]
    pub const fn command<I, C>() -> Self
    where
        I: MykoItem,
        C: CommandHandler<Service = I::Service, Scope = I::Scope>,
    {
        Self {
            owner: Some(service_type_id::<I::Service>),
            kind: HandlerKind::Command,
            handler_id: C::COMMAND_TYPE,
            register: register_command::<C>,
        }
    }

    /// Creates a generated service-owned command registration.
    #[doc(hidden)]
    #[must_use]
    pub const fn service_command<S, C>() -> Self
    where
        S: MykoService,
        C: CommandHandler<Service = S>,
    {
        Self {
            owner: Some(service_type_id::<S>),
            kind: HandlerKind::Command,
            handler_id: C::COMMAND_TYPE,
            register: register_command::<C>,
        }
    }

    /// Creates a command registration without an item-module association.
    #[doc(hidden)]
    #[must_use]
    pub const fn global_command<C: CommandHandler>() -> Self {
        Self {
            owner: None,
            kind: HandlerKind::Command,
            handler_id: C::COMMAND_TYPE,
            register: register_command::<C>,
        }
    }

    /// Creates a generated custom-query registration.
    #[doc(hidden)]
    #[must_use]
    pub const fn query<I, Q>() -> Self
    where
        I: MykoItem,
        Q: QueryHandler<Item = I>,
    {
        Self {
            owner: Some(service_type_id::<I::Service>),
            kind: HandlerKind::Query,
            handler_id: Q::QUERY_ID,
            register: register_query::<Q>,
        }
    }

    /// Creates a generated report registration.
    #[doc(hidden)]
    #[must_use]
    pub const fn report<I, R>() -> Self
    where
        I: MykoItem,
        R: ReportHandler,
    {
        Self {
            owner: Some(service_type_id::<I::Service>),
            kind: HandlerKind::Report,
            handler_id: R::REPORT_ID,
            register: register_report::<R>,
        }
    }

    /// Creates a report registration without an item-module association.
    #[doc(hidden)]
    #[must_use]
    pub const fn global_report<R: ReportHandler>() -> Self {
        Self {
            owner: None,
            kind: HandlerKind::Report,
            handler_id: R::REPORT_ID,
            register: register_report::<R>,
        }
    }

    /// Creates a generated view registration.
    #[doc(hidden)]
    #[must_use]
    pub const fn view<I, V>() -> Self
    where
        I: MykoItem,
        V: ViewHandler,
    {
        Self {
            owner: Some(service_type_id::<I::Service>),
            kind: HandlerKind::View,
            handler_id: V::VIEW_ID,
            register: register_view::<V>,
        }
    }

    /// Creates a view registration without an item-module association.
    #[doc(hidden)]
    #[must_use]
    pub const fn global_view<V: ViewHandler>() -> Self {
        Self {
            owner: None,
            kind: HandlerKind::View,
            handler_id: V::VIEW_ID,
            register: register_view::<V>,
        }
    }
}

inventory::collect!(HandlerRegistration);

trait ErasedCommandFactory: fmt::Debug + Send + Sync {
    fn authenticate(
        &self,
        node_id: NodeId,
        principal_id: PrincipalId,
        submission: CommandSubmission,
    ) -> Result<CommandRequest, NodeError>;

    fn dispatch(
        &self,
        node: &Node,
        resources: ApplicationResources,
        command_id: CommandId,
    ) -> Result<CommandDispatchResult, AppError>;
}

struct CommandFactory<C>(PhantomData<fn() -> C>);

impl<C> fmt::Debug for CommandFactory<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CommandFactory")
            .field(&std::any::type_name::<C>())
            .finish()
    }
}

impl<C> ErasedCommandFactory for CommandFactory<C>
where
    C: CommandHandler,
{
    fn authenticate(
        &self,
        node_id: NodeId,
        principal_id: PrincipalId,
        submission: CommandSubmission,
    ) -> Result<CommandRequest, NodeError> {
        let command: C = serde_json::from_slice(&submission.payload)
            .map_err(|error| NodeError::CommandDecoding(error.to_string()))?;
        let scope = command.scope(node_id);
        Ok(submission.authenticate(ScopeId::for_item::<C::Scope>(&scope), principal_id))
    }

    fn dispatch(
        &self,
        node: &Node,
        resources: ApplicationResources,
        command_id: CommandId,
    ) -> Result<CommandDispatchResult, AppError> {
        node.dispatch_declared_command::<C, _>(command_id, |declared| {
            declared
                .body()
                .clone()
                .execute(CommandContext::from_federation(
                    declared.command_context().clone(),
                    resources,
                ))
        })
        .map_err(AppError::Node)
    }
}

type CommandFactoryMap = BTreeMap<(ServiceTypeId, &'static str), Arc<dyn ErasedCommandFactory>>;

/// Declarative composition of the services exposed by one Myko application.
///
/// Applications select typed services explicitly. Each selected service
/// contributes its item modules and handlers, keeping link-time discovery from
/// accidentally activating every handler present in the process.
#[derive(Debug, Clone, Default)]
pub struct MykoApplication {
    services: BTreeSet<MykoServiceId>,
    globals_registered: bool,
    resources: ApplicationResources,
    commands: CommandFactoryMap,
    queries: BTreeMap<&'static str, Arc<dyn ErasedHandlerFactory>>,
    reports: BTreeMap<&'static str, Arc<dyn ErasedHandlerFactory>>,
    views: BTreeMap<&'static str, Arc<dyn ErasedHandlerFactory>>,
}

impl MykoApplication {
    /// Creates an empty application declaration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            services: BTreeSet::new(),
            globals_registered: false,
            resources: ApplicationResources::default(),
            commands: BTreeMap::new(),
            queries: BTreeMap::new(),
            reports: BTreeMap::new(),
            views: BTreeMap::new(),
        }
    }

    /// Starts a fluent, fallible application declaration.
    #[must_use]
    pub fn builder() -> MykoApplicationBuilder {
        MykoApplicationBuilder::new()
    }

    /// Returns the stable identities of the explicitly activated services.
    #[must_use]
    pub fn services(&self) -> impl ExactSizeIterator<Item = MykoServiceId> + '_ {
        self.services.iter().copied()
    }

    /// Returns the shared typed service registry used by handler contexts.
    #[must_use]
    pub fn resources(&self) -> ApplicationResources {
        self.resources.clone()
    }

    fn restore_item_scope_topology(&self, node: &Node) -> Result<(), AppError> {
        self.resources.get::<ItemRegistry>()?.restore_topology(node)
    }

    /// Adds one framework-owned service to an already composed application.
    ///
    /// Node compositions use this for Myko's own operational entities. User
    /// applications should continue selecting their services through
    /// [`MykoApplication::builder`].
    ///
    /// # Errors
    ///
    /// Returns an error when the service conflicts with an existing
    /// declaration.
    #[doc(hidden)]
    pub fn with_framework_service<S>(mut self) -> Result<Self, AppError>
    where
        S: MykoService,
        S::Items: ServiceModules<S>,
    {
        if self.services.contains(&MykoServiceId::of::<S>()) {
            return Ok(self);
        }
        self.activate_service::<S>()?;
        Ok(self)
    }

    fn activate_service<S>(&mut self) -> Result<(), AppError>
    where
        S: MykoService,
        S::Items: ServiceModules<S>,
    {
        let service_id = MykoServiceId::of::<S>();
        if self.services.contains(&service_id) {
            return Err(AppError::DuplicateService {
                id: service_id.service_id.as_str().to_owned(),
            });
        }

        <S::Items as ServiceModules<S>>::register(self)?;
        let include_globals = !self.globals_registered;
        let service_type = TypeId::of::<S>();
        for registration in
            inventory::iter::<HandlerRegistration>
                .into_iter()
                .filter(|registration| {
                    registration
                        .owner
                        .is_some_and(|owner| owner() == service_type)
                        || (include_globals && registration.owner.is_none())
                })
        {
            if registration.handler_id.is_empty() {
                return Err(AppError::DuplicateHandler {
                    kind: registration.kind.as_str(),
                    id: String::new(),
                });
            }
            (registration.register)(self)?;
        }
        self.globals_registered = true;
        self.services.insert(service_id);
        Ok(())
    }

    fn register_item_module<I: MykoItem>(&mut self) -> Result<(), AppError> {
        self.resources.get::<ItemRegistry>()?.register::<I>()?;
        self.register_projection_query::<I::GetAllQuery>()?;
        self.register_projection_query::<I::GetByIdQuery>()?;
        self.register_projection_query::<I::GetByIdsQuery>()
    }

    fn register_query<Q: QueryHandler>(&mut self) -> Result<(), AppError> {
        insert_handler(
            &mut self.queries,
            HandlerKind::Query,
            Q::QUERY_ID,
            Arc::new(QueryFactory::<Q>(PhantomData)),
        )
    }

    fn register_projection_query<Q: ItemQuery>(&mut self) -> Result<(), AppError> {
        insert_handler(
            &mut self.queries,
            HandlerKind::Query,
            Q::QUERY_ID,
            Arc::new(ProjectionQueryFactory::<Q>(PhantomData)),
        )
    }

    fn register_command<C: CommandHandler>(&mut self) -> Result<(), AppError> {
        let key = (C::SERVICE_ID, C::COMMAND_TYPE);
        if C::SERVICE_ID.is_empty()
            || C::COMMAND_TYPE.is_empty()
            || self
                .commands
                .insert(key, Arc::new(CommandFactory::<C>(PhantomData)))
                .is_some()
        {
            return Err(AppError::DuplicateHandler {
                kind: HandlerKind::Command.as_str(),
                id: format!("{}/{}", C::SERVICE_ID, C::COMMAND_TYPE),
            });
        }
        Ok(())
    }

    fn register_report<R: ReportHandler>(&mut self) -> Result<(), AppError> {
        insert_handler(
            &mut self.reports,
            HandlerKind::Report,
            R::REPORT_ID,
            Arc::new(ReportFactory::<R>(PhantomData)),
        )
    }

    fn register_view<V: ViewHandler>(&mut self) -> Result<(), AppError> {
        insert_handler(
            &mut self.views,
            HandlerKind::View,
            V::VIEW_ID,
            Arc::new(ViewFactory::<V>(PhantomData)),
        )
    }
}

fn register_query<Q: QueryHandler>(application: &mut MykoApplication) -> Result<(), AppError> {
    application.register_query::<Q>()
}

fn register_command<C: CommandHandler>(application: &mut MykoApplication) -> Result<(), AppError> {
    application.register_command::<C>()
}

fn register_report<R: ReportHandler>(application: &mut MykoApplication) -> Result<(), AppError> {
    application.register_report::<R>()
}

fn register_view<V: ViewHandler>(application: &mut MykoApplication) -> Result<(), AppError> {
    application.register_view::<V>()
}

/// Registers the statically typed item-module tuple owned by a service.
#[doc(hidden)]
pub trait ServiceModules<S: MykoService> {
    fn register(application: &mut MykoApplication) -> Result<(), AppError>;
}

macro_rules! service_modules {
    ($($item:ident),+ $(,)?) => {
        impl<S, $($item),+> ServiceModules<S> for ($($item,)+)
        where
            S: MykoService<Items = ($($item,)+)>,
            $($item: MykoItem<Service = S>,)+
        {
            fn register(application: &mut MykoApplication) -> Result<(), AppError> {
                $(application.register_item_module::<$item>()?;)+
                Ok(())
            }
        }
    };
}

service_modules!(A);
service_modules!(A, B);
service_modules!(A, B, C);
service_modules!(A, B, C, D);
service_modules!(A, B, C, D, E);
service_modules!(A, B, C, D, E, F);
service_modules!(A, B, C, D, E, F, G);
service_modules!(A, B, C, D, E, F, G, H);
service_modules!(A, B, C, D, E, F, G, H, I);
service_modules!(A, B, C, D, E, F, G, H, I, J);
service_modules!(A, B, C, D, E, F, G, H, I, J, K);
service_modules!(A, B, C, D, E, F, G, H, I, J, K, L);
service_modules!(A, B, C, D, E, F, G, H, I, J, K, L, M);
service_modules!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
service_modules!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
service_modules!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

/// Fluent declaration of one immutable Myko application.
#[derive(Debug, Default)]
pub struct MykoApplicationBuilder {
    application: MykoApplication,
}

impl MykoApplicationBuilder {
    /// Creates an empty application declaration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds every item module and handler contributed by one typed service.
    ///
    /// # Errors
    ///
    /// Returns an error when the service conflicts with prior declarations.
    pub fn service<S>(mut self) -> Result<Self, AppError>
    where
        S: MykoService,
        S::Items: ServiceModules<S>,
    {
        self.application.activate_service::<S>()?;
        Ok(self)
    }

    /// Installs one typed process-local service for handler contexts.
    ///
    /// # Errors
    ///
    /// Returns an error when the application resource registry is unavailable.
    pub fn resource<T>(self, value: T) -> Result<Self, AppError>
    where
        T: Send + Sync + 'static,
    {
        let _previous = self.application.resources.insert(value)?;
        Ok(self)
    }

    /// Installs the application's full-text search backend.
    ///
    /// # Errors
    ///
    /// Returns an error when the application resource registry is unavailable.
    pub fn search_provider<P>(self, provider: P) -> Result<Self, AppError>
    where
        P: SearchProvider,
    {
        let _previous = self
            .application
            .resources
            .insert(SearchService(Arc::new(provider)))?;
        Ok(self)
    }

    /// Finishes the immutable application declaration.
    #[must_use]
    pub fn build(self) -> MykoApplication {
        self.application
    }
}

/// Fluent construction of an application node and its declarative services.
pub struct ApplicationNodeBuilder {
    node: Node,
    application: MykoApplicationBuilder,
}

impl ApplicationNodeBuilder {
    /// Starts an application around an existing durable/federated node.
    #[must_use]
    pub fn new(node: Node) -> Self {
        Self {
            node,
            application: MykoApplication::builder(),
        }
    }

    /// Adds a reusable service's item modules and handler contracts.
    ///
    /// # Errors
    ///
    /// Returns an error when its stable IDs conflict.
    pub fn service<S>(mut self) -> Result<Self, AppError>
    where
        S: MykoService,
        S::Items: ServiceModules<S>,
    {
        self.application = self.application.service::<S>()?;
        Ok(self)
    }

    /// Attaches the completed immutable application to the node.
    #[must_use]
    pub fn build(self) -> ApplicationNode {
        ApplicationNode::new(self.node, self.application.build())
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
        resources: ApplicationResources,
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
        resources: ApplicationResources,
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
        let context = QueryBuildContext::new(node, resources, source_node, scope_id);
        let live = query.build(&context)?;
        Ok(erase_handler(HandlerSubscription {
            live,
            runtime: context.core.runtime,
        }))
    }
}

struct ProjectionQueryFactory<Q>(PhantomData<fn() -> Q>);

impl<Q: ItemQuery> fmt::Debug for ProjectionQueryFactory<Q> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ProjectionQueryFactory")
            .field(&Q::QUERY_ID)
            .finish()
    }
}

impl<Q: ItemQuery> ErasedHandlerFactory for ProjectionQueryFactory<Q> {
    fn watch(
        &self,
        node: Node,
        resources: ApplicationResources,
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
        let context = ContextCore::new(node, resources);
        let live = context.query(source_node, scope_id, query)?;
        Ok(erase_handler(HandlerSubscription {
            live,
            runtime: context.runtime,
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
        resources: ApplicationResources,
        request: &HandlerRequest,
    ) -> Result<ErasedHandlerSubscription, AppError> {
        let report = serde_json::from_value::<R>(request.params.clone())
            .map_err(|error| AppError::Serialization(error.to_string()))?;
        if request.scope_id != report.access_scope() {
            return Err(AppError::State(
                "report access scope does not match its typed parameters".to_owned(),
            ));
        }
        let context = ReportContext::new(node, resources);
        let live = report.build(&context)?;
        Ok(erase_handler(HandlerSubscription {
            live,
            runtime: context.core.runtime,
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
        resources: ApplicationResources,
        request: &HandlerRequest,
    ) -> Result<ErasedHandlerSubscription, AppError> {
        let view = serde_json::from_value::<V>(request.params.clone())
            .map_err(|error| AppError::Serialization(error.to_string()))?;
        if request.scope_id != view.access_scope() {
            return Err(AppError::State(
                "view access scope does not match its typed parameters".to_owned(),
            ));
        }
        let context = ViewContext::new(node, resources);
        let live = view.build(&context)?;
        Ok(erase_view_handler::<V>(ViewSubscription {
            live,
            runtime: context.core.runtime,
        }))
    }
}

struct DependencyDriver {
    task: JoinHandle<()>,
    invalidate: Box<dyn Fn() + Send + Sync>,
}

static HANDLER_DRIVER_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();

fn spawn_handler_driver(
    future: impl Future<Output = ()> + Send + 'static,
) -> Result<JoinHandle<()>, AppError> {
    let runtime = HANDLER_DRIVER_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("myko-app-handler")
                .enable_all()
                .build()
                .map_err(|error| format!("failed to start handler runtime: {error}"))
        })
        .as_ref()
        .map_err(|error| AppError::State(error.clone()))?;
    Ok(runtime.spawn(future))
}

#[derive(Default)]
struct HandlerRuntime {
    drivers: Mutex<Vec<DependencyDriver>>,
    guards: Mutex<Vec<SubscriptionGuard>>,
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

impl HandlerRuntime {
    async fn shutdown(&self) {
        let drivers = match self.drivers.lock() {
            Ok(mut drivers) => std::mem::take(&mut *drivers),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        let guards = match self.guards.lock() {
            Ok(mut guards) => std::mem::take(&mut *guards),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        drop(guards);
        for driver in &drivers {
            (driver.invalidate)();
            driver.task.abort();
        }
        for driver in drivers {
            let _stopped = driver.task.await;
        }
    }
}

/// Read-only capabilities available while a report or view is built.
///
/// The context intentionally has no mutation or command-emission API. Handler
/// code can compose durable query and command-state cells, while writes remain
/// in declared command handlers.
#[derive(Clone)]
#[doc(hidden)]
pub struct ContextCore {
    node: Node,
    resources: ApplicationResources,
    runtime: Arc<HandlerRuntime>,
}

impl ContextCore {
    fn new(node: Node, resources: ApplicationResources) -> Self {
        Self {
            node,
            resources,
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
        Q: ItemQuery,
    {
        let (initial, mut watch) = self.node.watch_items_in(source_node, scope_id, query)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(initial.value),
            through: initial.through,
            liveness: SubscriptionLiveness::Current,
        });
        let task_writer = writer.clone();
        let task = spawn_handler_driver(async move {
            loop {
                match watch.recv_async().await {
                    Ok(update) => task_writer.publish(update.value, Some(update.position)),
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        })?;
        self.retain_driver(task, move || {
            writer.invalidate("reactive handler dependency dropped");
        })?;
        Ok(live)
    }

    /// Materializes one source's typed items across every scope as a retained
    /// replay-then-live subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the gap-free snapshot/live boundary cannot be
    /// established or its retained driver cannot be started.
    pub fn query_from<Q>(
        &self,
        source_node: NodeId,
        query: Q,
    ) -> Result<LiveSubscription<Q::Output>, AppError>
    where
        Q: ItemQuery,
    {
        let (initial, mut watch) = self.node.watch_items_from(source_node, query)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(initial.value),
            through: initial.through,
            liveness: SubscriptionLiveness::Current,
        });
        let task_writer = writer.clone();
        let task = spawn_handler_driver(async move {
            loop {
                match watch.recv_async().await {
                    Ok(update) => task_writer.publish(update.value, Some(update.position)),
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        })?;
        self.retain_driver(task, move || {
            writer.invalidate("reactive handler dependency dropped");
        })?;
        Ok(live)
    }

    /// Materializes one scope's typed items across every authoritative source
    /// as a retained replay-then-live subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the gap-free snapshot/live boundary cannot be
    /// established or its retained driver cannot be started.
    pub fn query_across_sources<Q>(
        &self,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<LiveSubscription<Q::Output>, AppError>
    where
        Q: ItemQuery,
    {
        let (initial, mut watch) = self.node.watch_items_across_sources_in(scope_id, query)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(initial.value),
            through: initial.through,
            liveness: SubscriptionLiveness::Current,
        });
        let task_writer = writer.clone();
        let task = spawn_handler_driver(async move {
            loop {
                match watch.recv_async().await {
                    Ok(update) => task_writer.publish(update.value, Some(update.position)),
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        })?;
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
        let task = spawn_handler_driver(async move {
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
                    Ok(false) => task_writer.advance_through(Some(envelope.position)),
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        })?;
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
        let follow = snapshot.watch_request()?;
        let mut stream = CommandStateStream::from_snapshot(&snapshot)?;
        let mut events = self.node.subscribe(through)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(snapshot),
            through,
            liveness: SubscriptionLiveness::Current,
        });
        let task_writer = writer.clone();
        let task = spawn_handler_driver(async move {
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
        })?;
        self.retain_driver(task, move || {
            writer.invalidate("reactive handler dependency dropped");
        })?;
        Ok(live)
    }

    /// Adapts a legacy whole-collection dependency into an identity-preserving
    /// Myko view.
    ///
    /// New handlers should construct a [`LiveCollection`] directly. This
    /// compatibility seam exists so applications can migrate one producer at
    /// a time while every downstream consumer already receives typed row
    /// additions, updates, and removals.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial or a later snapshot contains a
    /// duplicate key, or when the dependency guard cannot be retained.
    pub fn collection_from_subscription<T, C>(
        &self,
        live: &LiveSubscription<Vec<T>, C>,
        item_key: impl Fn(&T) -> Arc<str> + Send + Sync + 'static,
    ) -> Result<LiveCollection<T, C>, AppError>
    where
        T: hyphae::CellValue,
        C: hyphae::CellValue,
    {
        let initial = live.current();
        let initial_rows = initial
            .value
            .as_deref()
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(|item| (item_key(&item), Arc::new(item)))
            .collect();
        let (writer, collection) = live_collection(
            initial_rows,
            LiveCollectionState {
                through: initial.through,
                liveness: initial.liveness,
            },
        );
        let item_key = Arc::new(item_key);
        let guard = live.state().subscribe(move |signal| {
            let Signal::Value(state) = signal else {
                return;
            };
            match &state.liveness {
                SubscriptionLiveness::Current => {
                    let rows = state
                        .value
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .cloned()
                        .map(|item| (item_key(&item), Arc::new(item)))
                        .collect();
                    if let Err(error) = writer.reconcile(rows, state.through.clone()) {
                        writer.invalidate(error.to_string());
                    }
                }
                SubscriptionLiveness::Resynchronizing { reason } => {
                    writer.resynchronizing(reason.clone());
                }
                SubscriptionLiveness::Invalid { reason } => writer.invalidate(reason.clone()),
                SubscriptionLiveness::Connecting => {}
            }
        });
        self.retain_guard(guard)?;
        Ok(collection)
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

    fn retain_guard(&self, guard: SubscriptionGuard) -> Result<(), AppError> {
        self.runtime
            .guards
            .lock()
            .map_err(|_| AppError::State("dependency guard registry is poisoned".to_owned()))?
            .push(guard);
        Ok(())
    }
}

/// Read-only context supplied to report handlers.
///
/// It can compose queries, reports, views, federation fan-in, and command
/// lifecycle subscriptions. It intentionally does not implement
/// [`capability::EventPublishing`] or [`capability::CommandSending`].
#[derive(Clone)]
pub struct ReportContext {
    core: ContextCore,
}

impl ReportContext {
    fn new(node: Node, resources: ApplicationResources) -> Self {
        Self {
            core: ContextCore::new(node, resources),
        }
    }
}

/// Read-only context supplied while a keyed view is built.
///
/// Its capability set mirrors report composition but excludes every mutation
/// and command-submission operation.
///
/// ```compile_fail
/// use myko_app::{ViewContext, capability::EventPublishing as _};
/// use myko_items::{myko_item, myko_service};
///
/// #[myko_service(Record)]
/// pub struct DocsService;
///
/// #[myko_item(service = DocsService)]
/// pub struct Record {}
///
/// fn cannot_mutate(context: &ViewContext, record: &Record) {
///     context.emit_set(record);
/// }
/// ```
#[derive(Clone)]
pub struct ViewContext {
    core: ContextCore,
}

impl ViewContext {
    fn new(node: Node, resources: ApplicationResources) -> Self {
        Self {
            core: ContextCore::new(node, resources),
        }
    }
}

/// Read-only context supplied while a custom query builds its reactive value.
///
/// It can compose queries, reports, search, graph reads, and history, but does
/// not gain view construction, federation fan-in, command watching, or any
/// write capability. The authoritative source and federation scope come from
/// the query request so the default [`QueryHandler::build`] implementation
/// does not make applications thread transport metadata through query types.
#[derive(Clone)]
pub struct QueryBuildContext {
    core: ContextCore,
    source_node: NodeId,
    scope_id: ScopeId,
}

impl QueryBuildContext {
    fn new(
        node: Node,
        resources: ApplicationResources,
        source_node: NodeId,
        scope_id: ScopeId,
    ) -> Self {
        Self {
            core: ContextCore::new(node, resources),
            source_node,
            scope_id,
        }
    }

    /// Returns the immutable authoritative source selected by this request.
    #[must_use]
    pub const fn source_node(&self) -> NodeId {
        self.source_node
    }

    /// Returns the immutable federation scope selected by this request.
    #[must_use]
    pub const fn scope_id(&self) -> &ScopeId {
        &self.scope_id
    }
}

macro_rules! impl_reactive_context_capabilities {
    ($context:ty) => {
        impl capability::sealed::Sealed for $context {}

        impl capability::NodeScoped for $context {
            fn __node(&self) -> &Node {
                &self.core.node
            }
        }

        impl capability::ResourceScoped for $context {
            fn __resources(&self) -> &ApplicationResources {
                &self.core.resources
            }
        }
        impl capability::RegistryScoped for $context {}

        impl capability::ReactiveScoped for $context {
            fn __reactive(&self) -> &ContextCore {
                &self.core
            }
        }

        impl capability::Querying for $context {}
        impl capability::GraphQuerying for $context {}
        impl capability::Searching for $context {}
        impl capability::FederatedQuerying for $context {}
        impl capability::CommandWatching for $context {}
        impl capability::Reporting for $context {}
        impl capability::Viewing for $context {}
        impl capability::CollectionBuilding for $context {}
    };
}

impl_reactive_context_capabilities!(ReportContext);
impl_reactive_context_capabilities!(ViewContext);

impl capability::sealed::Sealed for QueryBuildContext {}
impl capability::NodeScoped for QueryBuildContext {
    fn __node(&self) -> &Node {
        &self.core.node
    }
}
impl capability::ResourceScoped for QueryBuildContext {
    fn __resources(&self) -> &ApplicationResources {
        &self.core.resources
    }
}
impl capability::RegistryScoped for QueryBuildContext {}
impl capability::ReactiveScoped for QueryBuildContext {
    fn __reactive(&self) -> &ContextCore {
        &self.core
    }
}
impl capability::Querying for QueryBuildContext {}
impl capability::GraphQuerying for QueryBuildContext {}
impl capability::Searching for QueryBuildContext {}
impl capability::Reporting for QueryBuildContext {}
impl capability::HistoryReading for ReportContext {}
impl capability::Replaying for ReportContext {}
impl capability::HistoryReading for QueryBuildContext {}

fn restore_item_scope_topology<T: MykoItem>(node: &Node) -> Result<(), AppError> {
    if !matches!(T::SCOPE, ItemScope::RootScopedBy { .. }) {
        return Ok(());
    }
    let mut relations = Vec::new();
    for envelope in node.events_after(None)? {
        let NodeEvent::CommandCommitted { batch, .. } = &envelope.event else {
            continue;
        };
        for mutation in &batch.changes {
            if !mutation.is::<T>() || mutation.operation != MutationOperation::Set {
                continue;
            }
            let item = mutation
                .decode_set_in_scope::<T>(Some(batch.scope_id.as_str()))
                .map_err(|error| AppError::Node(NodeError::CorruptHistory(error.to_string())))?;
            let parent = item.belongs_to().ok_or_else(|| {
                AppError::Node(NodeError::CorruptHistory(format!(
                    "nested scope root {}/{} omitted its typed parent",
                    T::SERVICE_ID,
                    T::ITEM_TYPE
                )))
            })?;
            relations.push((
                ScopeId::for_item::<T>(item.id()),
                ScopeId::for_entity(&parent),
            ));
        }
    }
    node.install_derived_scope_relations(&relations)?;
    Ok(())
}

fn replay_items<T: MykoItem>(
    node: &Node,
    source_node: NodeId,
    scope_id: &ScopeId,
    until: Option<LogPosition>,
) -> Result<ItemProjection<T>, AppError> {
    let mut projection = ItemProjection::default();
    for envelope in node.events_after(None)? {
        if until.is_some_and(|ceiling| envelope.position > ceiling)
            || envelope.origin.node_id != source_node
        {
            continue;
        }
        let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
            continue;
        };
        if command.request.service_id != ServiceId::new(T::SERVICE_ID) {
            continue;
        }
        for (index, mutation) in batch.changes.iter().enumerate() {
            if !mutation.affects_scope::<T>(batch.scope_id.as_str(), scope_id.as_str()) {
                continue;
            }
            let change_index = u32::try_from(index).map_err(|error| {
                AppError::Node(NodeError::CorruptHistory(format!(
                    "item batch contains too many ordered changes: {error}"
                )))
            })?;
            let _changed = projection
                .apply_at_order_in_scope(
                    mutation,
                    Some(batch.scope_id.as_str()),
                    envelope.position.get(),
                    change_index,
                )
                .map_err(|error| AppError::Node(NodeError::CorruptHistory(error.to_string())))?;
        }
    }
    Ok(projection)
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
    if command.request.service_id != ServiceId::new(T::SERVICE_ID) {
        return Ok(false);
    }
    let mut changed = false;
    for mutation in &batch.changes {
        if !mutation.affects_scope::<T>(batch.scope_id.as_str(), scope_id.as_str()) {
            continue;
        }
        if mutation.item_type != T::ITEM_TYPE {
            continue;
        }
        if mutation.service_id != T::SERVICE_ID.as_str()
            || mutation.schema_version != T::SCHEMA_VERSION
        {
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
                    .decode_set_in_scope::<T>(Some(batch.scope_id.as_str()))
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
    runtime: Arc<HandlerRuntime>,
}

/// A live identity-preserving view plus ownership of its dependency graph.
pub struct ViewSubscription<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveCollection<T, C>,
    runtime: Arc<HandlerRuntime>,
}

impl<T, C> ViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns the view's keyed Hyphae collection and lifecycle cells.
    #[must_use]
    pub const fn live(&self) -> &LiveCollection<T, C> {
        &self.live
    }

    /// Stops every dependency driver and waits for it to release retained
    /// node and persistence handles.
    pub async fn shutdown(self) {
        self.runtime.shutdown().await;
    }
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

    /// Stops every dependency driver and waits for it to release retained
    /// node and persistence handles.
    pub async fn shutdown(self) {
        self.runtime.shutdown().await;
    }
}

/// The next frame a transport should write for an application handler.
///
/// Concrete handler values stay typed until this method constructs the frame;
/// `serde_json::Value` is used solely by the wire-compatible frame payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErasedHandlerFrame {
    /// Complete initial lifecycle snapshot, or replacement after a reset.
    State(ErasedHandlerState),
    /// Incremental rows for a keyed view after its initial snapshot.
    ViewDelta(ErasedViewDelta),
}

/// Type-erased registered handler result retained by a peer transport.
pub struct ErasedHandlerSubscription {
    inner: Box<dyn ErasedHandlerDriver>,
}

impl ErasedHandlerSubscription {
    /// Subscribes a transport wakeup to the handler's native typed cell.
    pub fn subscribe(&self, wake: impl Fn() + Send + Sync + 'static) -> SubscriptionGuard {
        self.inner.subscribe(Box::new(wake))
    }

    /// Encodes the next snapshot or keyed delta at the transport boundary.
    ///
    /// `None` means the typed lifecycle cell has not changed since the last
    /// frame served on this connection.
    ///
    /// # Errors
    ///
    /// Returns an error when a typed handler value or cursor cannot be encoded
    /// into its transport-boundary representation.
    pub fn next_frame(&mut self) -> Result<Option<ErasedHandlerFrame>, AppError> {
        self.inner.next_frame()
    }

    /// Stops the erased handler graph and waits for all retained node handles
    /// to be released.
    pub async fn shutdown(self) {
        let runtime = Arc::clone(self.inner.runtime());
        runtime.shutdown().await;
    }
}

trait ErasedHandlerDriver: Send {
    fn runtime(&self) -> &Arc<HandlerRuntime>;
    fn subscribe(&self, wake: Box<dyn Fn() + Send + Sync>) -> SubscriptionGuard;
    fn next_frame(&mut self) -> Result<Option<ErasedHandlerFrame>, AppError>;
}

struct SnapshotHandlerDriver<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    subscription: HandlerSubscription<T, C>,
    sent: Option<LiveSubscriptionState<T, C>>,
}

impl<T, C> ErasedHandlerDriver for SnapshotHandlerDriver<T, C>
where
    T: hyphae::CellValue + Serialize,
    C: hyphae::CellValue + Serialize,
{
    fn runtime(&self) -> &Arc<HandlerRuntime> {
        &self.subscription.runtime
    }

    fn subscribe(&self, wake: Box<dyn Fn() + Send + Sync>) -> SubscriptionGuard {
        self.subscription.live().state().subscribe(move |_| wake())
    }

    fn next_frame(&mut self) -> Result<Option<ErasedHandlerFrame>, AppError> {
        let current = self.subscription.live().current();
        if self.sent.as_ref() == Some(&current) {
            return Ok(None);
        }
        self.sent = Some(current.clone());
        Ok(Some(ErasedHandlerFrame::State(erase_state(&current))))
    }
}

fn erase_handler<T, C>(subscription: HandlerSubscription<T, C>) -> ErasedHandlerSubscription
where
    T: hyphae::CellValue + Serialize,
    C: hyphae::CellValue + Serialize,
{
    ErasedHandlerSubscription {
        inner: Box::new(SnapshotHandlerDriver {
            subscription,
            sent: None,
        }),
    }
}

type WakeCallback = Arc<dyn Fn() + Send + Sync>;
type PendingViewRevisions<V> = Arc<
    Mutex<VecDeque<LiveCollectionRevision<<V as ViewHandler>::Item, <V as ViewHandler>::Cursor>>>,
>;

fn erase_view_handler<V>(
    subscription: ViewSubscription<V::Item, V::Cursor>,
) -> ErasedHandlerSubscription
where
    V: ViewHandler,
{
    let rows = subscription
        .live()
        .rows()
        .snapshot()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let initial_revision = subscription.live().revision().get();
    let pending = Arc::new(Mutex::new(VecDeque::from([initial_revision])));
    let wake = Arc::new(Mutex::new(None::<WakeCallback>));
    let pending_for_callback = Arc::clone(&pending);
    let wake_for_callback = Arc::clone(&wake);
    let revision_guard = subscription.live().revision().subscribe(move |signal| {
        let Signal::Value(revision) = signal else {
            return;
        };
        if let Ok(mut pending) = pending_for_callback.lock()
            && pending.back() != Some(revision.as_ref())
        {
            pending.push_back(revision.as_ref().clone());
        }
        if let Ok(wake) = wake_for_callback.lock()
            && let Some(wake) = wake.as_ref()
        {
            wake();
        }
    });
    ErasedHandlerSubscription {
        inner: Box::new(ViewHandlerDriver::<V> {
            subscription,
            pending,
            wake,
            _revision_guard: revision_guard,
            rows,
            initialized: false,
        }),
    }
}

struct ViewHandlerDriver<V>
where
    V: ViewHandler,
{
    subscription: ViewSubscription<V::Item, V::Cursor>,
    pending: PendingViewRevisions<V>,
    wake: Arc<Mutex<Option<WakeCallback>>>,
    _revision_guard: SubscriptionGuard,
    rows: BTreeMap<Arc<str>, Arc<V::Item>>,
    initialized: bool,
}

impl<V> ErasedHandlerDriver for ViewHandlerDriver<V>
where
    V: ViewHandler,
{
    fn runtime(&self) -> &Arc<HandlerRuntime> {
        &self.subscription.runtime
    }

    fn subscribe(&self, wake: Box<dyn Fn() + Send + Sync>) -> SubscriptionGuard {
        if let Ok(mut registered) = self.wake.lock() {
            *registered = Some(Arc::from(wake));
            if self.pending.lock().is_ok_and(|pending| !pending.is_empty())
                && let Some(wake) = registered.as_ref()
            {
                wake();
            }
        }
        let registered = Arc::clone(&self.wake);
        SubscriptionGuard::from_callback(move || {
            if let Ok(mut wake) = registered.lock() {
                *wake = None;
            }
        })
    }

    fn next_frame(&mut self) -> Result<Option<ErasedHandlerFrame>, AppError> {
        let revision = self
            .pending
            .lock()
            .map_err(|_| AppError::State("view revision queue is poisoned".to_owned()))?
            .pop_front();
        let Some(revision) = revision else {
            return Ok(None);
        };
        if !self.initialized {
            if let Some(diff) = revision.diff.as_ref() {
                apply_view_diff(diff, &mut self.rows, &mut Vec::new(), &mut Vec::new())?;
            }
            self.initialized = true;
            return Ok(Some(ErasedHandlerFrame::State(erase_collection_state(
                &self.rows,
                &revision.state,
            ))));
        }
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        let membership_changed = if let Some(diff) = revision.diff.as_ref() {
            apply_view_diff(diff, &mut self.rows, &mut upserts, &mut deletes)?
        } else {
            false
        };
        let through = revision
            .state
            .through
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| AppError::Serialization(error.to_string()))?;
        Ok(Some(ErasedHandlerFrame::ViewDelta(ErasedViewDelta {
            upserts,
            deletes,
            order: membership_changed.then(|| {
                self.rows
                    .keys()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            }),
            through,
            liveness: revision.state.liveness,
        })))
    }
}

#[allow(clippy::needless_collect)] // Keys must survive clearing the authoritative map.
fn apply_view_diff<T>(
    diff: &MapDiff<Arc<str>, Arc<T>>,
    rows: &mut BTreeMap<Arc<str>, Arc<T>>,
    upserts: &mut Vec<Value>,
    deletes: &mut Vec<String>,
) -> Result<bool, AppError>
where
    T: hyphae::CellValue + Serialize,
{
    match diff {
        MapDiff::Initial { entries } => {
            let old_keys = rows.keys().cloned().collect::<Vec<_>>();
            rows.clear();
            for (key, value) in entries {
                rows.insert(Arc::clone(key), Arc::clone(value));
                upserts.push(
                    serde_json::to_value(value.as_ref())
                        .map_err(|error| AppError::Serialization(error.to_string()))?,
                );
            }
            deletes.extend(
                old_keys
                    .into_iter()
                    .filter(|key| !rows.contains_key(key))
                    .map(|key| key.to_string()),
            );
            Ok(true)
        }
        MapDiff::Insert { key, value } => {
            rows.insert(Arc::clone(key), Arc::clone(value));
            upserts.push(
                serde_json::to_value(value.as_ref())
                    .map_err(|error| AppError::Serialization(error.to_string()))?,
            );
            Ok(true)
        }
        MapDiff::Remove { key, .. } => {
            rows.remove(key);
            deletes.push(key.to_string());
            Ok(true)
        }
        MapDiff::Update { key, new_value, .. } => {
            rows.insert(Arc::clone(key), Arc::clone(new_value));
            upserts.push(
                serde_json::to_value(new_value.as_ref())
                    .map_err(|error| AppError::Serialization(error.to_string()))?,
            );
            Ok(false)
        }
        MapDiff::Batch { changes } => {
            let mut membership_changed = false;
            for change in changes {
                membership_changed |= apply_view_diff(change, rows, upserts, deletes)?;
            }
            Ok(membership_changed)
        }
    }
}

fn erase_collection_state<T, C>(
    rows: &BTreeMap<Arc<str>, Arc<T>>,
    state: &LiveCollectionState<C>,
) -> ErasedHandlerState
where
    T: Serialize,
    C: Serialize,
{
    erase_state(&LiveSubscriptionState {
        value: Some(rows.values().map(AsRef::as_ref).collect::<Vec<_>>()),
        through: state.through.as_ref(),
        liveness: state.liveness.clone(),
    })
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

/// One Myko node with its composed application.
#[derive(Clone, Debug)]
pub struct ApplicationNode {
    node: Node,
    application: Arc<MykoApplication>,
}

/// Retained, event-driven execution of every command registered by an
/// application.
///
/// Dropping the guard stops dispatch. Keeping it alive is the complete
/// lifecycle contract; applications do not need their own polling loop or
/// command supervisor.
pub struct CommandDispatchGuard {
    task: Option<JoinHandle<()>>,
    failure: Arc<RwLock<Option<String>>>,
}

impl fmt::Debug for CommandDispatchGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandDispatchGuard")
            .field(
                "finished",
                &self.task.as_ref().is_none_or(JoinHandle::is_finished),
            )
            .field("failure", &self.failure())
            .finish()
    }
}

impl CommandDispatchGuard {
    /// Returns a terminal framework failure when dispatch stopped
    /// unexpectedly.
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        self.failure.read().ok().and_then(|failure| failure.clone())
    }

    /// Stops dispatch and waits for the retained subscription task to release
    /// its application/node handles.
    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _stopped = task.await;
        }
    }
}

impl Drop for CommandDispatchGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn record_dispatch_failure(failure: &RwLock<Option<String>>, reason: String) {
    if let Ok(mut failure) = failure.write() {
        *failure = Some(reason);
    }
}

impl ApplicationNode {
    /// Starts a declarative application-node builder.
    #[must_use]
    pub fn builder(node: Node) -> ApplicationNodeBuilder {
        ApplicationNodeBuilder::new(node)
    }

    /// Attaches an immutable application to a node substrate.
    #[must_use]
    pub fn new(node: Node, application: MykoApplication) -> Self {
        Self {
            node,
            application: Arc::new(application),
        }
    }

    /// Attaches an application after restoring typed scope relationships from
    /// immutable history written by earlier Myko 7 schemas.
    ///
    /// Native runtimes use this constructor before opening any transport or
    /// releasing the startup-ready barrier.
    ///
    /// # Errors
    ///
    /// Returns an error when registered item history is malformed or contains
    /// conflicting nested-scope parentage.
    pub fn attach(node: Node, application: MykoApplication) -> Result<Self, AppError> {
        application.restore_item_scope_topology(&node)?;
        Ok(Self::new(node, application))
    }

    /// Returns the underlying durable/federated node.
    #[must_use]
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// Returns the stable identity of this application node.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    /// Returns the shared typed process-local resources used by handler contexts.
    ///
    /// The registry remains process-local; exposing it here lets a node runtime
    /// install reactive adapters after transport and operating-system services
    /// have started, without adding those services to the wire protocol.
    #[must_use]
    pub fn resources(&self) -> ApplicationResources {
        self.application.resources()
    }

    /// Starts lossless command dispatch for this application.
    ///
    /// The returned guard owns the subscription. Current pending commands are
    /// replayed before live admissions, and no polling or application-defined
    /// command-ID routing is involved.
    ///
    /// # Errors
    ///
    /// Returns an error when the pending-command subscription or the shared
    /// Myko handler runtime cannot be started.
    pub fn drive_commands(&self) -> Result<CommandDispatchGuard, AppError> {
        let mut pending = self.watch_pending_commands()?;
        let application = self.clone();
        let failure = Arc::new(RwLock::new(None));
        let task_failure = Arc::clone(&failure);
        let task = spawn_handler_driver(async move {
            loop {
                let command = match pending.recv_async().await {
                    Ok(command) => command,
                    Err(error) => {
                        record_dispatch_failure(&task_failure, error.to_string());
                        return;
                    }
                };
                if let Err(error) = application.dispatch_registered_command(command.request.id) {
                    record_dispatch_failure(&task_failure, error.to_string());
                    return;
                }
            }
        })?;
        Ok(CommandDispatchGuard {
            task: Some(task),
            failure,
        })
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
            HandlerKind::Command => {
                return Err(AppError::UnregisteredHandler {
                    kind: HandlerKind::Command.as_str(),
                    id: request.handler_id.clone(),
                });
            }
            HandlerKind::Query => &self.application.queries,
            HandlerKind::Report => &self.application.reports,
            HandlerKind::View => &self.application.views,
        };
        let factory = handlers.get(request.handler_id.as_str()).ok_or_else(|| {
            AppError::UnregisteredHandler {
                kind: request.kind.as_str(),
                id: request.handler_id.clone(),
            }
        })?;
        factory.watch(self.node.clone(), self.application.resources(), request)
    }

    /// Returns whether this composed application can authenticate and execute
    /// a transport submission's generated command contract.
    #[doc(hidden)]
    #[must_use]
    pub fn handles_submission(&self, submission: &CommandSubmission) -> bool {
        self.find_command_factory(submission.service_id.as_str(), &submission.command_type)
            .is_some()
    }

    /// Dispatches one registered command through its concrete handler and
    /// framework-owned capability context.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is not part of this application or
    /// its durable admission/handler/commit lifecycle fails.
    pub fn dispatch_command<C>(
        &self,
        command_id: CommandId,
    ) -> Result<CommandDispatchResult, AppError>
    where
        C: CommandHandler,
    {
        self.command_factory(C::SERVICE_ID.as_str(), C::COMMAND_TYPE)?
            .dispatch(&self.node, self.application.resources(), command_id)
    }

    /// Submits and dispatches one typed command through its registered handler.
    ///
    /// This is the process-boundary counterpart to
    /// [`CommandExecuting::exec_command`]: supervisors can drive bounded
    /// application transitions without inspecting command metadata or
    /// manually carrying Myko's internal command identity between calls.
    ///
    /// # Errors
    ///
    /// Returns an error when submission, handler lookup, or typed dispatch
    /// fails.
    pub fn exec_command<C>(&self, command: C) -> Result<C::Output, AppError>
    where
        C: CommandHandler,
    {
        let submitted = self.submit_authenticated_command(
            PrincipalId::new(format!("node:{}", self.node.node_id())),
            &command,
        )?;
        drop(command);
        let result = self.dispatch_command::<C>(submitted.request.id)?;
        result
            .command
            .typed_completion::<C>()?
            .ok_or_else(|| AppError::State("command handler did not produce a result".to_owned()))
    }

    /// Executes through a principal already authenticated by a Myko transport.
    #[doc(hidden)]
    pub fn exec_authenticated_command<C>(
        &self,
        principal_id: myko_federation::PrincipalId,
        command: C,
    ) -> Result<C::Output, AppError>
    where
        C: CommandHandler,
    {
        let submitted = self.submit_authenticated_command(principal_id, &command)?;
        drop(command);
        let result = self.dispatch_command::<C>(submitted.request.id)?;
        result
            .command
            .typed_completion::<C>()?
            .ok_or_else(|| AppError::State("command handler did not produce a result".to_owned()))
    }

    /// Admits a typed command for a principal already authenticated by Myko.
    #[doc(hidden)]
    pub fn submit_authenticated_command<C>(
        &self,
        principal_id: PrincipalId,
        command: &C,
    ) -> Result<CommandSnapshot, AppError>
    where
        C: CommandHandler,
    {
        let scope = command.scope(self.node.node_id());
        self.node
            .submit_authenticated_command(
                ScopeId::for_item::<C::Scope>(&scope),
                principal_id,
                command,
            )
            .map_err(Into::into)
    }

    /// Resolves a transport submission through the registered typed handler,
    /// deriving its application scope and binding the authenticated principal.
    #[doc(hidden)]
    pub fn authenticate_command_submission(
        &self,
        principal_id: PrincipalId,
        submission: CommandSubmission,
    ) -> Result<CommandRequest, NodeError> {
        let factory = self
            .find_command_factory(submission.service_id.as_str(), &submission.command_type)
            .ok_or_else(|| {
                NodeError::InvalidCommandState(format!(
                    "application does not register command {}/{}",
                    submission.service_id, submission.command_type
                ))
            })?;
        factory.authenticate(self.node.node_id(), principal_id, submission)
    }

    /// Dispatches one admitted command through the concrete handler selected
    /// by its generated service and operation identities.
    ///
    /// This is the application-runtime entry point used when a transport or
    /// supervisor receives only durable command metadata. The handler body is
    /// decoded directly into its registered Rust type; applications do not
    /// switch on service strings or deserialize payloads themselves.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is unknown, its generated handler is
    /// not part of this application, or typed dispatch fails.
    pub fn dispatch_registered_command(
        &self,
        command_id: CommandId,
    ) -> Result<CommandDispatchResult, AppError> {
        let command = self
            .node
            .command(command_id)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        self.command_factory(
            command.request.service_id.as_str(),
            &command.request.command_type,
        )?
        .dispatch(&self.node, self.application.resources(), command_id)
    }

    /// Dispatches each currently executable local command whose generated
    /// handler belongs to this composed application, in durable admission
    /// order.
    ///
    /// One call makes at most one attempt per pending command. Rejected and
    /// retrying outcomes are recorded by the ordinary Myko lifecycle; an
    /// application runtime decides when a later dependency change or retry
    /// schedule warrants another pass.
    ///
    /// # Errors
    ///
    /// Returns an error when pending history cannot be read or a registered
    /// command cannot complete its typed dispatch transition.
    pub fn dispatch_pending_commands(&self) -> Result<Vec<CommandDispatchResult>, AppError> {
        let mut dispatched = Vec::new();
        for command in self.node.pending_local_application_commands()? {
            let Some(factory) = self.find_command_factory(
                command.request.service_id.as_str(),
                &command.request.command_type,
            ) else {
                continue;
            };
            dispatched.push(factory.dispatch(
                &self.node,
                self.application.resources(),
                command.request.id,
            )?);
        }
        Ok(dispatched)
    }

    /// Subscribes to locally executable commands owned by any handler in this
    /// composed application.
    ///
    /// The initial pending set is replayed before live admissions, so an
    /// application runtime can drive every registered handler through one
    /// event loop without command-specific polling or supervisor wiring.
    ///
    /// # Errors
    ///
    /// Returns an error when durable command history cannot be subscribed.
    pub fn watch_pending_commands(&self) -> Result<PendingCommandSubscription, AppError> {
        self.node
            .watch_pending_local_application_commands()
            .map_err(Into::into)
    }

    /// Dispatches pending commands for the service that owns `I`.
    ///
    /// The framework retains durable admission ordering, command-contract
    /// selection, and wire decoding. Application supervisors select a typed
    /// entity boundary and never inspect command metadata or payload bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when pending history cannot be read or a registered
    /// command cannot complete its typed dispatch transition.
    pub fn dispatch_pending_for<I: MykoItem>(
        &self,
    ) -> Result<Vec<CommandDispatchResult>, AppError> {
        let mut dispatched = Vec::new();
        for command in self
            .node
            .pending_local_service_commands(I::SERVICE_ID.as_str())?
        {
            let Some(factory) = self.find_command_factory(
                command.request.service_id.as_str(),
                &command.request.command_type,
            ) else {
                continue;
            };
            dispatched.push(factory.dispatch(
                &self.node,
                self.application.resources(),
                command.request.id,
            )?);
        }
        Ok(dispatched)
    }

    /// Dispatches every pending command of one concrete typed contract.
    ///
    /// Myko owns pending discovery, stable identity lookup, payload decoding,
    /// and handler selection. The caller receives only the typed declaration
    /// and its durable dispatch result.
    ///
    /// # Errors
    ///
    /// Returns an error when pending history is malformed, the handler is not
    /// registered, or a lifecycle transition fails.
    pub fn dispatch_pending<C>(&self) -> Result<Vec<CommandDispatchResult>, AppError>
    where
        C: CommandHandler,
    {
        self.command_factory(C::SERVICE_ID.as_str(), C::COMMAND_TYPE)?;
        let mut dispatched = Vec::new();
        for pending in self
            .node
            .pending_local_commands(C::SERVICE_ID.as_str(), C::COMMAND_TYPE)?
        {
            let result = self.dispatch_command::<C>(pending.request.id)?;
            dispatched.push(result);
        }
        Ok(dispatched)
    }

    fn command_factory(
        &self,
        service_id: &str,
        command_type: &str,
    ) -> Result<&Arc<dyn ErasedCommandFactory>, AppError> {
        self.find_command_factory(service_id, command_type)
            .ok_or_else(|| AppError::UnregisteredHandler {
                kind: HandlerKind::Command.as_str(),
                id: format!("{service_id}/{command_type}"),
            })
    }

    fn find_command_factory(
        &self,
        service_id: &str,
        command_type: &str,
    ) -> Option<&Arc<dyn ErasedCommandFactory>> {
        self.application.commands.iter().find_map(
            |((registered_service, registered_command), factory)| {
                (registered_service.as_str() == service_id && *registered_command == command_type)
                    .then_some(factory)
            },
        )
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
        Q: ItemQuery,
    {
        require_handler(&self.application.queries, "query", Q::QUERY_ID)?;
        let context = ContextCore::new(self.node.clone(), self.application.resources());
        let live = context.query(source_node, scope_id, query)?;
        Ok(HandlerSubscription {
            live,
            runtime: context.runtime,
        })
    }

    /// Builds a registered custom query through its reactive handler.
    ///
    /// This is the process-local half of the transport-neutral application
    /// client; application code normally reaches it through that routed
    /// client rather than selecting a transport itself.
    ///
    /// # Errors
    ///
    /// Returns an error when the query is not registered or its reactive
    /// dependency graph cannot be built.
    #[doc(hidden)]
    pub fn watch_registered_query<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<HandlerSubscription<Q::Output>, AppError>
    where
        Q: QueryHandler,
    {
        require_handler(&self.application.queries, "query", Q::QUERY_ID)?;
        let context = QueryBuildContext::new(
            self.node.clone(),
            self.application.resources(),
            source_node,
            scope_id,
        );
        let live = query.build(&context)?;
        Ok(HandlerSubscription {
            live,
            runtime: context.core.runtime,
        })
    }

    /// Opens a retained typed query across every scope owned by one source.
    ///
    /// # Errors
    ///
    /// Returns an error if the query handler is not registered or its gap-free
    /// snapshot/live boundary cannot be established.
    pub fn watch_query_from<Q>(
        &self,
        source_node: NodeId,
        query: Q,
    ) -> Result<HandlerSubscription<Q::Output>, AppError>
    where
        Q: ItemQuery,
    {
        require_handler(&self.application.queries, "query", Q::QUERY_ID)?;
        let context = ContextCore::new(self.node.clone(), self.application.resources());
        let live = context.query_from(source_node, query)?;
        Ok(HandlerSubscription {
            live,
            runtime: context.runtime,
        })
    }

    /// Opens a retained typed query across every authoritative source in one
    /// application scope.
    ///
    /// # Errors
    ///
    /// Returns an error if the query handler is not registered or its gap-free
    /// snapshot/live boundary cannot be established.
    pub fn watch_query_across_sources<Q>(
        &self,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<HandlerSubscription<Q::Output>, AppError>
    where
        Q: ItemQuery,
    {
        require_handler(&self.application.queries, "query", Q::QUERY_ID)?;
        let context = ContextCore::new(self.node.clone(), self.application.resources());
        let live = context.query_across_sources(scope_id, query)?;
        Ok(HandlerSubscription {
            live,
            runtime: context.runtime,
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
        require_handler(&self.application.reports, "report", R::REPORT_ID)?;
        let context = ReportContext::new(self.node.clone(), self.application.resources());
        let live = report.build(&context)?;
        Ok(HandlerSubscription {
            live,
            runtime: context.core.runtime,
        })
    }

    /// Builds a registered reactive view.
    ///
    /// # Errors
    ///
    /// Returns an error when the view is not registered or cannot be built.
    pub fn watch_view<V>(&self, view: &V) -> Result<ViewSubscription<V::Item, V::Cursor>, AppError>
    where
        V: ViewHandler,
    {
        require_handler(&self.application.views, "view", V::VIEW_ID)?;
        let context = ViewContext::new(self.node.clone(), self.application.resources());
        let live = view.build(&context)?;
        Ok(ViewSubscription {
            live,
            runtime: context.core.runtime,
        })
    }
}

impl FederationCommandClient for ApplicationNode {
    type Error = NodeError;

    fn submit_submission(
        &self,
        submission: CommandSubmission,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            let principal_id = PrincipalId::new(format!("node:{}", self.node.node_id()));
            let request = self.authenticate_command_submission(principal_id, submission)?;
            let command = self.node.submit(request)?;
            Ok(CommandResponse {
                source_node: self.node.node_id(),
                command: Some(command),
            })
        })
    }

    fn command_state(&self, command_id: CommandId) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            Ok(CommandResponse {
                source_node: self.node.node_id(),
                command: self.node.command(command_id)?,
            })
        })
    }

    fn cancel_command(
        &self,
        command_id: CommandId,
        reason: String,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            Ok(CommandResponse {
                source_node: self.node.node_id(),
                command: Some(self.node.cancel(command_id, reason)?),
            })
        })
    }
}

impl FederationCommandWatchingClient for ApplicationNode {
    type Subscription = CommandWatch;

    fn watch_command(
        &self,
        command_id: CommandId,
    ) -> CommandWatchFuture<'_, Self::Subscription, Self::Error> {
        Box::pin(async move {
            let (_current, subscription) = self.node.watch_command(command_id)?;
            Ok(subscription)
        })
    }
}

/// Shared in-memory harness for application contract and transport-adapter tests.
pub mod testing {
    use myko_federation::{
        BatchId, ChangeBatch, CommandId, CommandRequest, LogPosition, MykoItem, PrincipalId,
        ScopeId, ServiceId,
    };

    use super::{AppError, ApplicationNode, MykoApplication, Node};

    /// One isolated node plus the Myko application under test.
    pub struct ApplicationTestHarness {
        node: Node,
        application: MykoApplication,
    }

    impl Default for ApplicationTestHarness {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ApplicationTestHarness {
        /// Creates an isolated in-memory node with an empty application.
        #[must_use]
        pub fn new() -> Self {
            Self {
                node: Node::in_memory(),
                application: MykoApplication::new(),
            }
        }

        /// Returns the durable/federated substrate used by the test.
        #[must_use]
        pub const fn node(&self) -> &Node {
            &self.node
        }

        /// Builds an application handle while retaining this harness.
        #[must_use]
        pub fn application(&self) -> ApplicationNode {
            ApplicationNode::new(self.node.clone(), self.application.clone())
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
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use hyphae::{Signal, Watchable as _};
    use myko_federation::{
        BatchId, ChangeBatch, CommandId, CommandRequest, PrincipalId, ServiceId,
    };
    use myko_items::{
        ItemMutation, ItemProjection, ItemQuery, myko_command, myko_item, myko_service,
    };

    use super::*;
    use crate::capability::{
        CollectionBuilding as _, CommandExecuting as _, EventPublishing as _, GraphQuerying as _,
        Querying as _, RegistryScoped as _, Searching as _,
    };

    struct ReleaseProbe(Arc<AtomicBool>);

    impl Drop for ReleaseProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[myko_service(CounterItem, CounterLink)]
    pub struct TestService;

    #[myko_item(service = TestService, scope_root)]
    pub struct CounterItem {
        pub value: u64,
    }

    #[myko_item(service = TestService, scope_root)]
    pub struct CounterLink {
        pub from: CounterItemId,
        pub to: CounterItemId,
    }

    #[myko_service(ProjectRoot)]
    pub struct ProjectService;

    #[myko_item(service = ProjectService, scope_root)]
    pub struct ProjectRoot {
        pub name: String,
    }

    #[myko_service(ProjectTask)]
    pub struct ProjectTaskService;

    #[myko_item(service = ProjectTaskService, scoped_by = ProjectRoot)]
    pub struct ProjectTask {
        pub title: String,
    }

    #[myko_service(Scene, SceneElement)]
    pub struct SceneService;

    #[myko_item(service = SceneService, scope_root, scoped_by = ProjectRoot)]
    pub struct Scene {
        pub name: String,
    }

    #[myko_item(service = SceneService, scoped_by = Scene)]
    pub struct SceneElement {
        pub name: String,
    }

    impl GraphEdge for CounterLink {
        type Ends = myko_federation::Directed<
            myko_federation::ConcreteEndpoint<CounterItem>,
            myko_federation::ConcreteEndpoint<CounterItem>,
        >;

        fn ends(&self) -> <Self::Ends as EdgeEnds>::Values {
            (self.from.clone(), self.to.clone())
        }
    }

    #[myko_command(bool, item = CounterItem)]
    struct SetCounter {
        id: CounterItemId,
        value: u64,
    }

    impl CommandHandler for SetCounter {
        fn scope(&self, _node_id: NodeId) -> CounterItemId {
            CounterItemId::from("counter")
        }

        fn execute(
            self,
            context: CommandContext<TestService, CounterItem>,
        ) -> Result<bool, CommandError> {
            context.emit_set(&CounterItem {
                id: self.id,
                value: self.value,
            })?;
            Ok(true)
        }
    }

    #[myko_command(bool, service = TestService, scope = CounterItem)]
    struct ComposeCounter {
        id: CounterItemId,
        value: u64,
    }

    impl CommandHandler for ComposeCounter {
        fn scope(&self, _node_id: NodeId) -> CounterItemId {
            CounterItemId::from("counter")
        }

        fn execute(
            self,
            context: CommandContext<TestService, CounterItem>,
        ) -> Result<bool, CommandError> {
            context.exec_command(SetCounter {
                id: self.id,
                value: self.value,
            })
        }
    }

    #[myko_command(bool, item = CounterItem)]
    struct SetCounterInScope {
        scope: CounterItemId,
        id: CounterItemId,
        value: u64,
    }

    impl CommandHandler for SetCounterInScope {
        fn scope(&self, _node_id: NodeId) -> CounterItemId {
            self.scope.clone()
        }

        fn execute(
            self,
            context: CommandContext<TestService, CounterItem>,
        ) -> Result<bool, CommandError> {
            context.emit_set(&CounterItem {
                id: self.id,
                value: self.value,
            })?;
            Ok(true)
        }
    }

    #[myko_command(bool, item = CounterItem)]
    struct ComposeAcrossCounterScopes {
        outer_scope: CounterItemId,
        inner_scope: CounterItemId,
    }

    impl CommandHandler for ComposeAcrossCounterScopes {
        fn scope(&self, _node_id: NodeId) -> CounterItemId {
            self.outer_scope.clone()
        }

        fn execute(
            self,
            context: CommandContext<TestService, CounterItem>,
        ) -> Result<bool, CommandError> {
            let inner_scope = self.inner_scope;
            context.exec_command(SetCounterInScope {
                scope: inner_scope.clone(),
                id: inner_scope,
                value: 99,
            })
        }
    }

    #[myko_command(bool, item = Scene)]
    struct CreateSceneInScope {
        project_id: ProjectRootId,
        scene_id: SceneId,
    }

    impl CommandHandler for CreateSceneInScope {
        fn scope(&self, _node_id: NodeId) -> SceneId {
            self.scene_id.clone()
        }

        fn execute(
            self,
            context: CommandContext<SceneService, Scene>,
        ) -> Result<bool, CommandError> {
            context.emit_set(&Scene {
                id: self.scene_id,
                project_root_id: self.project_id,
                name: "opening".to_owned(),
            })?;
            Ok(true)
        }
    }

    #[myko_command(bool, item = SceneElement)]
    struct AddSceneElement {
        scene_id: SceneId,
        element_id: SceneElementId,
    }

    impl CommandHandler for AddSceneElement {
        fn scope(&self, _node_id: NodeId) -> SceneId {
            self.scene_id.clone()
        }

        fn execute(
            self,
            context: CommandContext<SceneService, Scene>,
        ) -> Result<bool, CommandError> {
            context.emit_set(&SceneElement {
                id: self.element_id,
                scene_id: self.scene_id,
                name: "camera".to_owned(),
            })?;
            Ok(true)
        }
    }

    #[myko_command(bool, service = SceneService, scope = ProjectRoot)]
    struct CreateProjectScene {
        project: ProjectRootId,
        scene: SceneId,
        element: SceneElementId,
    }

    impl CommandHandler for CreateProjectScene {
        fn scope(&self, _node_id: NodeId) -> ProjectRootId {
            self.project.clone()
        }

        fn execute(
            self,
            context: CommandContext<SceneService, ProjectRoot>,
        ) -> Result<bool, CommandError> {
            context.exec_command(CreateSceneInScope {
                project_id: self.project,
                scene_id: self.scene.clone(),
            })?;
            context.exec_command(AddSceneElement {
                scene_id: self.scene,
                element_id: self.element,
            })
        }
    }

    #[myko_command(bool, item = ProjectTask)]
    struct SetProjectTask {
        project_id: ProjectRootId,
        task_id: ProjectTaskId,
    }

    impl CommandHandler for SetProjectTask {
        fn scope(&self, _node_id: NodeId) -> ProjectRootId {
            self.project_id.clone()
        }

        fn execute(
            self,
            context: CommandContext<ProjectTaskService, ProjectRoot>,
        ) -> Result<bool, CommandError> {
            context.emit_set(&ProjectTask {
                id: self.task_id,
                project_root_id: self.project_id,
                title: "cross-service scope".to_owned(),
            })?;
            Ok(true)
        }
    }

    #[myko_query(CounterItem)]
    #[derive(Copy)]
    struct SumCounters;

    impl ItemQuery for SumCounters {
        type Item = CounterItem;
        type Output = u64;
        fn execute(self, projection: &ItemProjection<Self::Item>) -> Self::Output {
            projection.values().map(|item| item.value).sum()
        }
    }

    impl QueryHandler for SumCounters {}

    #[myko_report(String, item = CounterItem)]
    #[derive(Copy)]
    struct CounterReport {
        source_node: NodeId,
    }

    impl ReportHandler for CounterReport {
        type Output = String;
        type Cursor = LogPosition;

        fn access_scope(&self) -> Option<ScopeId> {
            Some(ScopeId::new("counter"))
        }

        fn build(
            &self,
            context: &ReportContext,
        ) -> Result<LiveSubscription<Self::Output>, AppError> {
            Ok(context
                .query(self.source_node, ScopeId::new("counter"), SumCounters)?
                .map_value(|value| format!("count:{value}")))
        }
    }

    #[myko_view(CounterItem, item = CounterItem)]
    #[derive(Copy)]
    struct CounterView {
        source_node: NodeId,
    }

    impl ViewHandler for CounterView {
        type Item = CounterItem;
        type Cursor = LogPosition;

        fn access_scope(&self) -> Option<ScopeId> {
            Some(ScopeId::new("counter"))
        }

        fn item_key(item: &Self::Item) -> Arc<str> {
            Arc::from(item.id.to_string())
        }

        fn build(
            &self,
            context: &ViewContext,
        ) -> Result<LiveCollection<Self::Item, Self::Cursor>, AppError> {
            let counters = context.query(
                self.source_node,
                ScopeId::new("counter"),
                GetAllCounterItems,
            )?;
            context.collection_from_subscription(&counters, Self::item_key)
        }
    }

    fn commit_counter(node: &Node, id: &str, value: u64) -> Result<(), AppError> {
        commit_counter_in(node, ScopeId::new("counter"), id, value)
    }

    fn counter_command_scope() -> ScopeId {
        ScopeId::for_item::<CounterItem>(&CounterItemId::from("counter"))
    }

    fn commit_counter_in(
        node: &Node,
        scope_id: ScopeId,
        id: &str,
        value: u64,
    ) -> Result<(), AppError> {
        let command = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new(TestService::SERVICE_ID),
            scope_id,
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

    fn commit_legacy_scene(node: &Node, project_id: &str, scene_id: &str) -> Result<(), AppError> {
        let legacy_project_scope = ScopeId::new(format!("project_root:{project_id}"));
        let command = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new(SceneService::SERVICE_ID),
            scope_id: legacy_project_scope,
            principal_id: PrincipalId::new("test:owner"),
            command_type: "legacy.scene.set".to_owned(),
            payload: Vec::new(),
        };
        let _admission = node.admit(command.clone())?;
        let mutation = ItemMutation {
            service_id: Scene::SERVICE_ID.as_str().to_owned(),
            item_type: Scene::ITEM_TYPE.to_owned(),
            item_id: scene_id.to_owned(),
            schema_version: Scene::SCHEMA_VERSION,
            roots_scope: false,
            belongs_to: None,
            scope_id: None,
            operation: MutationOperation::Set,
            payload: Some(
                serde_json::to_vec(&serde_json::json!({
                    "id": scene_id,
                    "name": "legacy opening",
                }))
                .map_err(|error| AppError::Serialization(error.to_string()))?,
            ),
        };
        let _committed = node.commit(
            command.id,
            myko_federation::ChangeBatch {
                id: BatchId::new(),
                command_id: command.id,
                service_id: command.service_id,
                scope_id: command.scope_id,
                causal_parents: Vec::new(),
                changes: vec![mutation],
            },
            Vec::new(),
        )?;
        Ok(())
    }

    #[test]
    fn application_attachment_restores_legacy_nested_items_and_topology() {
        let node = Node::in_memory();
        assert!(commit_legacy_scene(&node, "project-1", "scene-1").is_ok());
        let application = MykoApplication::builder()
            .service::<ProjectService>()
            .and_then(MykoApplicationBuilder::service::<SceneService>)
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::attach(node.clone(), application);
        assert!(app.is_ok());
        let Ok(app) = app else {
            return;
        };
        let scene_id = SceneId::from("scene-1");
        let scene_scope = ScopeId::for_item::<Scene>(&scene_id);
        let scenes = app.watch_query(
            node.node_id(),
            scene_scope.clone(),
            GetSceneById {
                id: scene_id.clone(),
            },
        );
        assert!(matches!(
            scenes,
            Ok(ref scenes)
                if matches!(
                    scenes.live().current().value.as_ref(),
                    Some(Some(scene))
                        if scene.id == scene_id
                            && scene.project_root_id == ProjectRootId::from("project-1")
                )
        ));
        let project_scope = ScopeId::for_item::<ProjectRoot>(&ProjectRootId::from("project-1"));
        assert_eq!(
            node.scope_topology()
                .ok()
                .and_then(|topology| topology.parent(&scene_scope).cloned()),
            Some(project_scope)
        );

        let reopened_application = MykoApplication::builder()
            .service::<ProjectService>()
            .and_then(MykoApplicationBuilder::service::<SceneService>)
            .map(MykoApplicationBuilder::build);
        assert!(
            reopened_application
                .and_then(|application| ApplicationNode::attach(node, application))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn registered_query_from_is_live_across_every_source_scope() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<TestService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        assert!(commit_counter_in(&node, ScopeId::new("first"), "first", 1).is_ok());
        let counters = app.watch_query_from(node.node_id(), SumCounters);
        assert!(counters.is_ok());
        let Ok(counters) = counters else {
            return;
        };
        assert_eq!(counters.live().current().value, Some(1));

        assert!(commit_counter_in(&node, ScopeId::new("second"), "second", 2).is_ok());
        let updated = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if counters.live().current().value == Some(3) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(updated.is_ok());
    }

    #[tokio::test]
    async fn registered_query_across_sources_is_live_within_one_scope() {
        let node = Node::in_memory();
        let remote = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<TestService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let scope_id = ScopeId::new("shared");
        assert!(commit_counter_in(&node, scope_id.clone(), "local", 1).is_ok());
        assert!(commit_counter_in(&remote, scope_id.clone(), "remote", 2).is_ok());
        let history = remote.events_after(None);
        assert!(history.is_ok());
        let Ok(history) = history else {
            return;
        };
        for event in history {
            assert!(node.ingest(event).is_ok());
        }

        let counters = app.watch_query_across_sources(scope_id.clone(), SumCounters);
        assert!(counters.is_ok());
        let Ok(counters) = counters else {
            return;
        };
        assert_eq!(counters.live().current().value, Some(3));

        assert!(commit_counter_in(&remote, scope_id, "later", 3).is_ok());
        let history = remote.events_after(None);
        assert!(history.is_ok());
        let Ok(history) = history else {
            return;
        };
        for event in history {
            assert!(node.ingest(event).is_ok());
        }
        let updated = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if counters.live().current().value == Some(6) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(updated.is_ok());
    }

    #[tokio::test]
    async fn registered_report_is_driven_by_query_cell_without_polling() {
        let node = Node::in_memory();
        let composed = MykoApplication::builder().service::<TestService>();
        assert!(composed.is_ok());
        let Ok(builder) = composed else {
            return;
        };
        let application = builder.build();
        assert_eq!(
            application.services().collect::<Vec<_>>(),
            vec![MykoServiceId::of::<TestService>()]
        );
        let app = ApplicationNode::new(node.clone(), application);
        assert!(
            app.watch_query(node.node_id(), ScopeId::new("counter"), GetAllCounterItems,)
                .is_ok()
        );
        assert!(
            app.watch_query(node.node_id(), ScopeId::new("counter"), SumCounters)
                .is_ok()
        );
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

    #[test]
    fn registered_view_is_live_without_an_ambient_tokio_runtime() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<TestService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let view = app.watch_view(&CounterView {
            source_node: node.node_id(),
        });
        assert!(view.is_ok());
        let Ok(view) = view else {
            return;
        };
        assert_eq!(
            view.live().current_state().liveness,
            SubscriptionLiveness::Current
        );

        let (changed, updates) = std::sync::mpsc::sync_channel(1);
        let _guard = view.live().revision().subscribe(move |_| {
            let _result = changed.try_send(());
        });
        assert!(commit_counter(&node, "view-counter", 13).is_ok());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while view.live().rows().snapshot().is_empty()
            && let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now())
            && updates.recv_timeout(remaining).is_ok()
        {}
        let rows = view.live().rows().snapshot();
        assert!(
            rows.iter()
                .any(|(_, counter)| counter.id == CounterItemId::from("view-counter"))
        );
    }

    #[test]
    fn erased_handlers_verify_access_scope_from_typed_parameters() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<TestService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let params = serde_json::to_value(CounterView {
            source_node: node.node_id(),
        });
        assert!(params.is_ok());
        let Ok(params) = params else {
            return;
        };
        let mut request = HandlerRequest {
            kind: HandlerKind::View,
            handler_id: CounterView::VIEW_ID.to_owned(),
            source_node: None,
            scope_id: None,
            params,
        };
        assert!(matches!(
            app.watch_handler(&request),
            Err(AppError::State(message)) if message.contains("access scope")
        ));
        request.scope_id = Some(ScopeId::new("counter"));
        assert!(app.watch_handler(&request).is_ok());
    }

    #[test]
    fn registered_command_dispatch_selects_its_typed_handler_from_durable_metadata() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<TestService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let submitted = app.submit_authenticated_command(
            PrincipalId::new("test:owner"),
            &SetCounter {
                id: CounterItemId::from("command-counter"),
                value: 7,
            },
        );
        assert!(submitted.is_ok());
        let Ok(submitted) = submitted else {
            return;
        };
        let dispatched = app.dispatch_registered_command(submitted.request.id);
        assert!(matches!(
            dispatched,
            Ok(result) if result.disposition == myko_federation::CommandDispatchDisposition::Committed
        ));
        let values =
            node.query_items_in(node.node_id(), &counter_command_scope(), GetAllCounterItems);
        assert!(matches!(
            values,
            Ok(items) if items.len() == 1 && items.first().is_some_and(|item| item.value == 7)
        ));

        let queued = app.submit_authenticated_command(
            PrincipalId::new("test:owner"),
            &SetCounter {
                id: CounterItemId::from("queued-counter"),
                value: 9,
            },
        );
        assert!(queued.is_ok());
        let Ok(queued) = queued else {
            return;
        };
        let unrelated = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("other.application"),
            scope_id: ScopeId::new("counter"),
            principal_id: PrincipalId::new("test:owner"),
            command_type: "OtherCommand".to_owned(),
            payload: Vec::new(),
        };
        assert!(node.submit(unrelated.clone()).is_ok());
        let pending = app.dispatch_pending_commands();
        assert!(matches!(
            pending,
            Ok(results)
                if results.len() == 1
                    && results.first().is_some_and(|result| {
                        result.command.request.id == queued.request.id
                            && result.disposition
                                == myko_federation::CommandDispatchDisposition::Committed
                    })
        ));
        assert!(matches!(
            node.command(unrelated.id),
            Ok(Some(command)) if command.state == myko_federation::CommandState::Submitted
        ));
    }

    #[tokio::test]
    async fn retained_command_guard_dispatches_registered_commands_without_polling() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<TestService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let guard = app.drive_commands();
        assert!(guard.is_ok());
        let Ok(guard) = guard else {
            return;
        };

        let submitted = app
            .submit_command(SetCounter {
                id: CounterItemId::from("guard-counter"),
                value: 19,
            })
            .await;
        assert!(submitted.is_ok());
        let committed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let values = node.query_items_in(
                    node.node_id(),
                    &counter_command_scope(),
                    GetAllCounterItems,
                );
                if values.is_ok_and(|items| {
                    items
                        .iter()
                        .any(|item| item.id == CounterItemId::from("guard-counter"))
                }) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(committed.is_ok());
        assert_eq!(guard.failure(), None);
    }

    #[test]
    fn nested_commands_share_the_outer_atomic_context() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<TestService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let executed = app.exec_authenticated_command(
            PrincipalId::new("test:owner"),
            ComposeCounter {
                id: CounterItemId::from("nested-counter"),
                value: 11,
            },
        );
        assert_eq!(executed.ok(), Some(true));
        let values =
            node.query_items_in(node.node_id(), &counter_command_scope(), GetAllCounterItems);
        assert!(matches!(
            values,
            Ok(items)
                if items.len() == 1
                    && items.first().is_some_and(|item| item.value == 11)
        ));
    }

    #[test]
    fn nested_commands_commit_multiple_scopes_atomically() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<TestService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let executed = app.exec_authenticated_command(
            PrincipalId::new("test:owner"),
            ComposeAcrossCounterScopes {
                outer_scope: CounterItemId::from("one"),
                inner_scope: CounterItemId::from("two"),
            },
        );
        assert_eq!(executed.ok(), Some(true));
        let outer_values = node.query_items_in(
            node.node_id(),
            &ScopeId::for_item::<CounterItem>(&CounterItemId::from("one")),
            GetAllCounterItems,
        );
        assert!(matches!(outer_values, Ok(items) if items.is_empty()));
        let inner_values = node.query_items_in(
            node.node_id(),
            &ScopeId::for_item::<CounterItem>(&CounterItemId::from("two")),
            GetAllCounterItems,
        );
        assert!(matches!(
            inner_values,
            Ok(items)
                if items.len() == 1
                    && items.first().is_some_and(|item| item.value == 99)
        ));
    }

    #[test]
    fn parent_scoped_command_creates_a_nested_scope_atomically() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<SceneService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let project_id = ProjectRootId::from("project-1");
        let scene_id = SceneId::from("scene-1");
        let element_id = SceneElementId::from("element-1");
        let project_scope = ScopeId::for_item::<ProjectRoot>(&project_id);
        let scene_scope = ScopeId::for_item::<Scene>(&scene_id);
        let watched = node.watch_items_in(node.node_id(), scene_scope.clone(), GetAllSceneElements);
        assert!(watched.is_ok());
        let Ok((_snapshot, mut watch)) = watched else {
            return;
        };
        let executed = app.exec_authenticated_command(
            PrincipalId::new("test:owner"),
            CreateProjectScene {
                project: project_id.clone(),
                scene: scene_id.clone(),
                element: element_id.clone(),
            },
        );
        assert_eq!(executed.ok(), Some(true));

        let update = watch.recv_timeout(std::time::Duration::from_secs(1));
        assert!(matches!(
            update,
            Ok(Some(update)) if update.value.len() == 1
        ));
        let topology = node.scope_topology();
        assert!(matches!(
            topology,
            Ok(topology) if topology.parent(&scene_scope) == Some(&project_scope)
        ));
        let scenes = node.query_items_in(node.node_id(), &scene_scope, GetAllScenes);
        assert!(matches!(
            scenes,
            Ok(items)
                if items.len() == 1
                    && items.first().is_some_and(|scene| {
                        scene.id == scene_id && scene.project_root_id == project_id
                    })
        ));
        let elements = node.query_items_in(node.node_id(), &scene_scope, GetAllSceneElements);
        assert!(matches!(
            elements,
            Ok(items)
                if items.len() == 1
                    && items.first().is_some_and(|element| {
                        element.id == element_id && element.scene_id == scene_id
                    })
        ));
    }

    #[tokio::test]
    async fn handler_runtime_shutdown_awaits_dependency_release() -> Result<(), String> {
        let released = Arc::new(AtomicBool::new(false));
        let task_released = Arc::clone(&released);
        let (started, task_started) = std::sync::mpsc::sync_channel(1);
        let task = spawn_handler_driver(async move {
            let _probe = ReleaseProbe(task_released);
            let _sent = started.send(());
            std::future::pending::<()>().await;
        })
        .map_err(|error| error.to_string())?;
        task_started
            .recv_timeout(std::time::Duration::from_secs(1))
            .map_err(|error| error.to_string())?;
        let runtime = HandlerRuntime::default();
        {
            let mut drivers = runtime
                .drivers
                .lock()
                .map_err(|_| "dependency registry is poisoned".to_owned())?;
            drivers.push(DependencyDriver {
                task,
                invalidate: Box::new(|| {}),
            });
            drop(drivers);
        }

        runtime.shutdown().await;

        if !released.load(Ordering::Acquire) {
            return Err("dependency task retained its resources after shutdown".to_owned());
        }
        Ok(())
    }

    #[test]
    fn item_command_scope_may_be_rooted_in_another_service() {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<ProjectTaskService>()
            .map(MykoApplicationBuilder::build);
        assert!(application.is_ok());
        let Ok(application) = application else {
            return;
        };
        let app = ApplicationNode::new(node.clone(), application);
        let project_id = ProjectRootId::from("project-1");
        let executed = app.exec_authenticated_command(
            PrincipalId::new("test:owner"),
            SetProjectTask {
                project_id: project_id.clone(),
                task_id: ProjectTaskId::from("task-1"),
            },
        );
        assert_eq!(executed.ok(), Some(true));
        let tasks = node.query_items_in(
            node.node_id(),
            &ScopeId::for_item::<ProjectRoot>(&project_id),
            GetAllProjectTasks,
        );
        assert!(matches!(
            tasks,
            Ok(tasks) if tasks.first().is_some_and(|task| task.title == "cross-service scope")
        ));
    }

    #[test]
    fn application_rejects_duplicate_module_activation() {
        let first = MykoApplication::builder().service::<TestService>();
        assert!(first.is_ok());
        if let Ok(builder) = first {
            let duplicate = builder.service::<TestService>();
            assert!(matches!(duplicate, Err(AppError::DuplicateService { .. })));
        }
    }

    #[derive(Debug)]
    struct TestSearch;

    impl SearchProvider for TestSearch {
        fn search(
            &self,
            item_type: &str,
            query: &str,
            limit: usize,
        ) -> Result<Vec<Arc<str>>, String> {
            Ok(vec![format!("{item_type}:{query}:{limit}").into()])
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines, clippy::redundant_closure_for_method_calls)]
    async fn graph_and_search_capabilities_use_typed_application_state() {
        let builder = MykoApplication::builder()
            .service::<TestService>()
            .and_then(|builder| builder.search_provider(TestSearch));
        assert!(builder.is_ok());
        let Ok(builder) = builder else {
            return;
        };
        let application = builder.build();
        let node = Node::in_memory();
        assert!(commit_counter(&node, "registry-counter", 9).is_ok());
        let link = CounterLink {
            id: CounterLinkId::from("counter-link"),
            from: CounterItemId::from("registry-counter"),
            to: CounterItemId::from("other-counter"),
        };
        let command = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new(CounterLink::SERVICE_ID),
            scope_id: ScopeId::new("counter"),
            principal_id: PrincipalId::new("test:owner"),
            command_type: "counter.link".to_owned(),
            payload: Vec::new(),
        };
        assert!(node.admit(command.clone()).is_ok());
        let change = ItemMutation::set(&link);
        assert!(change.is_ok());
        let Ok(change) = change else {
            return;
        };
        assert!(
            node.commit(
                command.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: command.id,
                    service_id: command.service_id,
                    scope_id: command.scope_id,
                    causal_parents: Vec::new(),
                    changes: vec![change],
                },
                Vec::new(),
            )
            .is_ok()
        );
        let context = ReportContext::new(node.clone(), application.resources());
        let edges = context.edges::<CounterLink>(node.node_id(), ScopeId::new("counter"));
        assert!(edges.is_ok());
        let Ok(edges) = edges else {
            return;
        };
        assert_eq!(
            edges.from(&CounterItemId::from("registry-counter")),
            vec![Arc::new(link)]
        );
        assert!(matches!(
            edges
                .watch_to(&CounterItemId::from("other-counter"))
                .current()
                .value,
            Some(edges) if edges.len() == 1
        ));
        let traversal = context
            .traverse::<CounterLink>(node.node_id(), ScopeId::new("counter"))
            .map(|traversal| {
                traversal
                    .start(CounterItemId::from("registry-counter"))
                    .max_depth(2)
                    .max_nodes(8)
            })
            .and_then(TraversalBuilder::execute);
        assert!(matches!(
            traversal,
            Ok(result)
                if result.nodes
                    == vec![EntityRef::new(
                        CounterItem::SERVICE_ID,
                        CounterItem::ITEM_TYPE,
                        "other-counter",
                    )]
                    && result.edge_ids == vec![Arc::<str>::from("counter-link")]
                    && !result.truncated
        ));
        assert!(matches!(
            context.search("CounterItem", "needle", 3),
            Ok(ids) if ids.as_slice() == [Arc::<str>::from("CounterItem:needle:3")]
        ));
        let registry = context.registry();
        assert!(registry.is_ok());
        let Ok(registry) = registry else {
            return;
        };
        let items = registry.items(
            &node,
            node.node_id(),
            &ScopeId::new("counter"),
            CounterItem::SERVICE_ID.as_str(),
            CounterItem::ITEM_TYPE,
        );
        assert!(matches!(
            items,
            Ok(items)
                if items.len() == 1
                    && items.first().is_some_and(|item| {
                        item.as_any()
                            .downcast_ref::<CounterItem>()
                            .is_some_and(|item| item.value == 9)
                    })
        ));
    }
}
