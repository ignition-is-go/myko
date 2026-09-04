use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    ApplicationResources, command::CommandError, core::capability::RequestScoped,
    query::QueryParams, request::RequestContext, server::MykoServerContext,
};

/// Context provided to command handlers for accessing dependencies.
///
/// `CommandContext` allows handlers to:
/// - Emit SET/DEL events
/// - Execute queries against the store
/// - Access request context (tx, `client_id`, lineage, `host_id`)
#[derive(Clone)]
pub struct CommandContext {
    /// Request context with tracing information (tx, `client_id`, lineage, `host_id`).
    pub req: Arc<RequestContext>,

    /// The command ID being executed (for error reporting).
    pub command_id: Arc<str>,

    server_ctx: Option<Arc<MykoServerContext>>,
    #[cfg(not(target_arch = "wasm32"))]
    federation: Option<myko_federation::CommandContext>,
    resources: ApplicationResources,
    #[cfg(not(target_arch = "wasm32"))]
    scope_id: Option<myko_federation::ScopeId>,
}

impl CommandContext {
    /// Create a new `CommandContext`.
    #[must_use]
    pub fn new(
        command_id: Arc<str>,
        req: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
    ) -> Self {
        Self {
            req,
            command_id,
            server_ctx: Some(server_ctx),
            #[cfg(not(target_arch = "wasm32"))]
            federation: None,
            resources: ApplicationResources::default(),
            #[cfg(not(target_arch = "wasm32"))]
            scope_id: None,
        }
    }

    #[allow(clippy::expect_used)]
    const fn server_ctx(&self) -> &Arc<MykoServerContext> {
        self.server_ctx
            .as_ref()
            .expect("retained command context has a server runtime")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience accessors
    // ─────────────────────────────────────────────────────────────────────────

    /// Get the request creation timestamp.
    #[must_use]
    pub fn created_at(&self) -> &str {
        &self.req.created_at
    }

    /// Execute a query and return the first result.
    ///
    /// This performs a one-shot query against the store.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn exec_query_first<Q>(&self, query: Q) -> Result<Option<Arc<Q::Item>>, CommandError>
    where
        Q: QueryParams,
        Q::Item: DeserializeOwned + std::fmt::Debug + Send + Sync + Clone + 'static,
    {
        {
            Ok(self
                .server_ctx()
                .query_snapshot(query, self.req.clone())
                .into_iter()
                .next())
        }
    }

    /// Execute a query and return all results.
    ///
    /// This performs a one-shot query against the store.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn exec_query<Q>(&self, query: Q) -> Result<Vec<Arc<Q::Item>>, CommandError>
    where
        Q: QueryParams,
        Q::Item: DeserializeOwned + std::fmt::Debug + Send + Sync + Clone + 'static,
    {
        {
            Ok(self
                .server_ctx()
                .query_snapshot(query, self.req.clone())
                .into_iter()
                .collect())
        }
    }

    /// Create a command context over a durable federation command.
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    #[must_use]
    pub fn from_federation(
        inner: myko_federation::CommandContext,
        resources: ApplicationResources,
    ) -> Self {
        let request = inner.request();
        let req = Arc::new(RequestContext::internal(
            Arc::from(request.id.to_string()),
            uuid::Uuid::new_v4(),
            "durable-command",
        ));
        Self {
            req,
            command_id: Arc::from(request.command_type.as_str()),
            server_ctx: None,
            scope_id: Some(request.scope_id.clone()),
            federation: Some(inner),
            resources,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn federation(&self) -> Result<&myko_federation::CommandContext, CommandError> {
        self.federation
            .as_ref()
            .ok_or_else(|| CommandError::reject("command is not executing in a durable node"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn durable_scope(&self) -> Result<&myko_federation::ScopeId, CommandError> {
        self.scope_id
            .as_ref()
            .ok_or_else(|| CommandError::reject("command has no durable scope"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn retarget(&self, scope_id: myko_federation::ScopeId) -> Self {
        let mut context = self.clone();
        context.scope_id = Some(scope_id);
        context
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    /// Return the durable node executing this command.
    ///
    /// # Panics
    ///
    /// Panics if called from a legacy store-backed command context. Typed
    /// durable command handlers are constructed with a federation context.
    #[allow(clippy::expect_used)]
    pub fn node_id(&self) -> myko_federation::NodeId {
        self.federation()
            .expect("durable command context")
            .node()
            .node_id()
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    /// Return the authenticated principal executing this durable command.
    ///
    /// # Panics
    ///
    /// Panics if called from a legacy store-backed command context.
    #[allow(clippy::expect_used)]
    pub fn principal_id(&self) -> &myko_federation::PrincipalId {
        &self
            .federation()
            .expect("durable command context")
            .request()
            .principal_id
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    /// Return the authority principal carried by this durable command.
    ///
    /// # Panics
    ///
    /// Panics if called from a legacy store-backed command context.
    #[allow(clippy::expect_used)]
    pub fn authority_principal(&self) -> &myko_federation::Principal {
        &self
            .federation()
            .expect("durable command context")
            .request()
            .authority
            .principal
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Read the authoritative durable scope topology.
    ///
    /// # Errors
    ///
    /// Returns an error outside durable execution or when topology is unavailable.
    pub fn scope_topology(&self) -> Result<myko_federation::ScopeTopology, CommandError> {
        self.federation()?
            .node()
            .scope_topology()
            .map_err(command_capability_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Resolve a typed process-local application resource and record its capability use.
    ///
    /// # Errors
    ///
    /// Returns an error when the resource is absent, the context is not
    /// durable, or capability recording fails.
    pub fn resource<T>(&self) -> Result<Arc<T>, crate::AppError>
    where
        T: Send + Sync + 'static,
    {
        if let Some(capability) = self.resources.capability::<T>()? {
            self.federation()
                .map_err(|error| crate::AppError::State(error.message))?
                .record_actual_capability(capability)
                .map_err(crate::AppError::Node)?;
        }
        self.resources.get::<T>()
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Execute one typed item query against the active durable command context.
    ///
    /// # Errors
    ///
    /// Returns an error when the context is not durable or the query fails.
    pub fn exec_item_query<Q>(
        &self,
        query: Q,
    ) -> Result<myko_federation::ItemQueryResult<Q>, CommandError>
    where
        Q: myko_federation::ItemQuery,
    {
        self.federation()?
            .query(query)
            .map_err(command_capability_error)
    }

    /// Execute a typed query over a declared exact scope or nested subtree.
    ///
    /// The durable command context records the selected read in its actual
    /// claim set before projecting local authoritative history.
    ///
    /// # Errors
    ///
    /// Returns an error when the selection was not declared, the context is
    /// not durable, or the typed projection fails.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn exec_selected_query<Q>(
        &self,
        selection: myko_federation::ScopeSelection,
        query: Q,
    ) -> Result<myko_federation::ItemQueryResult<Q>, CommandError>
    where
        Q: myko_federation::ItemQuery,
    {
        self.federation()?
            .query_selected(selection, query)
            .map_err(command_capability_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Emit one typed SET mutation in the active durable scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the item belongs to another scope or emission fails.
    pub fn emit_set<T>(&self, item: &T) -> Result<(), CommandError>
    where
        T: myko_federation::MykoItem,
    {
        let scope = self.durable_scope()?;
        if matches!(
            T::SCOPE,
            myko_federation::ItemScope::ScopedBy { .. }
                | myko_federation::ItemScope::RootScopedBy { .. }
        ) {
            let declared = myko_federation::ScopeId::for_entity(&item.scope_ref());
            if &declared != scope {
                return Err(CommandError::reject(format!(
                    "item belongs to scope {declared}; active command scope is {scope}"
                )));
            }
        }
        self.federation()?
            .emit_set_in(scope, item)
            .map_err(command_capability_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Emit one typed deletion in the active durable scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the context is not durable or emission fails.
    pub fn emit_delete<T>(&self, id: &T::Id) -> Result<(), CommandError>
    where
        T: myko_federation::MykoItem,
    {
        self.federation()?
            .emit_delete_in::<T>(self.durable_scope()?, id)
            .map_err(command_capability_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Execute a nested typed command under the outer command's declared authority.
    ///
    /// # Errors
    ///
    /// Returns an error when service, resource, or capability constraints are violated.
    pub fn exec_command<C>(&self, command: C) -> Result<C::Result, CommandError>
    where
        C: CommandHandler + myko_federation::MykoCommandContract<Output = C::Result>,
    {
        if self.federation()?.request().service_id.as_str() != C::SERVICE_ID.as_str() {
            return Err(CommandError::reject(
                "nested commands must belong to the outer durable service",
            ));
        }
        let scope = command.scope(self.node_id());
        let scope_id = myko_federation::ScopeId::for_item::<C::Scope>(&scope);
        for claim in normalized_command_claims(&command, self.node_id(), scope_id.clone()) {
            self.federation()?
                .validate_declared_claim(&claim)
                .map_err(command_capability_error)?;
        }
        for capability in command.required_capabilities() {
            self.federation()?
                .record_actual_capability(capability)
                .map_err(command_capability_error)?;
        }
        command.execute(self.retarget(scope_id))
    }

    /// Execute a report and return the current value.
    ///
    /// This allows command handlers to query reports for decision making.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn exec_report<R>(
        &self,
        report: R,
    ) -> Result<<R as crate::report::ReportHandler>::Output, CommandError>
    where
        R: crate::report::ReportParams + Clone,
    {
        {
            use hyphae::Gettable;
            Ok(self
                .server_ctx()
                .report(report, self.req.clone())
                .get()
                .as_ref()
                .clone())
        }
    }

    /// Look up the existing edge occupying the candidate's unique pair and
    /// scope. Used by generated idempotent graph commands.
    #[doc(hidden)]
    pub fn graph_unique_edge<E>(
        &self,
        a: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
        b: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
        scope: Option<&<E::Scope as crate::graph::EdgeScope>::Value>,
    ) -> Result<Option<Arc<E>>, CommandError>
    where
        E: crate::graph::GraphEdge,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        if E::PAIR_POLICY != crate::graph::PairPolicy::Unique {
            return Err(CommandError::new(
                self.req.tx.to_string(),
                self.command_id.to_string(),
                format!("{} does not declare unique pairs", E::ENTITY_NAME_STATIC),
            ));
        }

        let query = self.server_ctx().edges::<E>();
        let result = scope.map_or_else(
            || query.one_between(a, b),
            |scope| query.one_between_in_scope(scope, a, b),
        );
        result.map_err(|error| {
            CommandError::new(
                self.req.tx.to_string(),
                self.command_id.to_string(),
                error.to_string(),
            )
        })
    }

    /// Atomically make `edges` the exact edge set at endpoint A and within the
    /// optional edge scope. Generated graph commands use this entry point.
    #[doc(hidden)]
    pub fn graph_sync_from<E>(
        &self,
        endpoint: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
        scope: Option<&serde_json::Value>,
        edges: &[E],
    ) -> Result<crate::graph::GraphSyncResult, CommandError>
    where
        E: crate::graph::GraphEdge + Clone + PartialEq,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        let endpoint =
            <<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::erase(
                endpoint,
            )
            .map_err(|error| self.graph_command_error(error))?;
        self.graph_sync_at(crate::graph::EndPosition::A, &endpoint, scope, edges)
    }

    /// Endpoint-B counterpart of [`Self::graph_sync_from`].
    #[doc(hidden)]
    pub fn graph_sync_to<E>(
        &self,
        endpoint: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
        scope: Option<&serde_json::Value>,
        edges: &[E],
    ) -> Result<crate::graph::GraphSyncResult, CommandError>
    where
        E: crate::graph::GraphEdge + Clone + PartialEq,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        let endpoint =
            <<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::erase(
                endpoint,
            )
            .map_err(|error| self.graph_command_error(error))?;
        self.graph_sync_at(crate::graph::EndPosition::B, &endpoint, scope, edges)
    }

    fn graph_command_error(&self, error: impl std::fmt::Display) -> CommandError {
        CommandError::new(
            self.req.tx.to_string(),
            self.command_id.to_string(),
            error.to_string(),
        )
    }

    #[allow(clippy::too_many_lines)]
    fn graph_sync_at<E>(
        &self,
        position: crate::graph::EndPosition,
        endpoint: &crate::graph::EndpointValue,
        scope: Option<&serde_json::Value>,
        edges: &[E],
    ) -> Result<crate::graph::GraphSyncResult, CommandError>
    where
        E: crate::graph::GraphEdge + Clone + PartialEq,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        use crate::core::item::{AnyItem, downcast_any_item_arc};
        use crate::graph::{EdgeEnds, EdgeScope};

        let graph = self
            .server_ctx()
            .graph_index()
            .ok_or_else(|| self.graph_command_error("application has no graph registrations"))?;
        let expected_scope = scope
            .map(crate::graph::IndexValue::from_serializable)
            .transpose()
            .map_err(|error| self.graph_command_error(error))?;
        let is_scoped = <E::Scope as EdgeScope>::scope_type().is_some();
        if is_scoped != expected_scope.is_some() {
            return Err(self.graph_command_error(if is_scoped {
                "scoped graph reconciliation requires a scope"
            } else {
                "unscoped graph reconciliation does not accept a scope"
            }));
        }

        let mut desired_ids = HashSet::with_capacity(edges.len());
        for edge in edges {
            let ends =
                E::Ends::erase(&edge.ends()).map_err(|error| self.graph_command_error(error))?;
            let candidate = match position {
                crate::graph::EndPosition::A => &ends.a,
                crate::graph::EndPosition::B => &ends.b,
            };
            if candidate != endpoint {
                return Err(
                    self.graph_command_error("desired edge does not match the reconciled endpoint")
                );
            }
            let edge_scope = edge
                .scope()
                .as_ref()
                .map(E::Scope::erase)
                .transpose()
                .map_err(|error| self.graph_command_error(error))?;
            if edge_scope != expected_scope {
                return Err(
                    self.graph_command_error("desired edge does not match the reconciled scope")
                );
            }
            if !desired_ids.insert(edge.id()) {
                return Err(self.graph_command_error("desired edge IDs must be unique"));
            }
        }

        for _ in 0..64 {
            let generation = graph.generation();
            let ids = graph.edge_ids_at(E::ENTITY_NAME_STATIC, position, endpoint);
            let current = self
                .server_ctx()
                .registry
                .get(E::ENTITY_NAME_STATIC)
                .map_or_else(HashMap::new, |store| {
                    ids.into_iter()
                        .filter_map(|id| store.get_value(&id))
                        .filter_map(|item| downcast_any_item_arc::<E>(&item, "graph endpoint sync"))
                        .filter(|edge| {
                            edge.scope()
                                .as_ref()
                                .map(E::Scope::erase)
                                .transpose()
                                .is_ok_and(|scope| scope == expected_scope)
                        })
                        .map(|edge| (edge.id(), edge))
                        .collect()
                });

            let mut result = crate::graph::GraphSyncResult::default();
            let mut upserts = Vec::<Arc<dyn AnyItem>>::new();
            for edge in edges {
                let id = edge.id();
                match current.get(&id) {
                    Some(old) if E::eq(old.as_ref(), edge) => {
                        result.unchanged = result.unchanged.saturating_add(1);
                    }
                    Some(_) => {
                        result.updated = result.updated.saturating_add(1);
                        upserts.push(Arc::new(edge.clone()));
                    }
                    None => {
                        result.inserted = result.inserted.saturating_add(1);
                        upserts.push(Arc::new(edge.clone()));
                    }
                }
            }
            let deletes = current
                .into_iter()
                .filter(|(id, _)| !desired_ids.contains(id))
                .map(|(_, edge)| {
                    result.deleted = result.deleted.saturating_add(1);
                    let edge: Arc<dyn AnyItem> = edge;
                    edge
                })
                .collect::<Vec<_>>();

            if upserts.is_empty() && deletes.is_empty() {
                if graph.generation() == generation {
                    return Ok(result);
                }
                continue;
            }
            if self
                .server_ctx()
                .replace_batch_any_if_graph_generation(upserts, deletes, generation)
                .map_err(|error| self.graph_command_error(error))?
            {
                return Ok(result);
            }
        }
        Err(self.graph_command_error(
            "graph endpoint kept changing; reconciliation retry budget exhausted",
        ))
    }
}

// Capability impls. The command context is the only one that carries
// `EventPublishing` (emit typed SET/DEL events) and `CommandSending` (dispatch
// nested commands) — which is exactly why a report/view/query/saga handler
// cannot emit or dispatch: they don't implement these traits. The bodies live
// in `core::capability`; these impls just wire the accessors.
impl crate::core::capability::sealed::Sealed for CommandContext {}
impl crate::core::capability::RequestScoped for CommandContext {
    fn __request(&self) -> &Arc<RequestContext> {
        &self.req
    }
}
impl crate::core::capability::ServerScoped for CommandContext {
    fn __server_ctx(&self) -> &Arc<MykoServerContext> {
        self.server_ctx()
    }
}
impl crate::core::capability::EventPublishing for CommandContext {
    fn __command_id(&self) -> &Arc<str> {
        &self.command_id
    }
}
impl crate::core::capability::CommandSending for CommandContext {
    fn __command_ctx(&self) -> CommandContext {
        self.clone()
    }
}

/// Trait for command handlers.
///
/// Implement this directly on your command params struct.
/// The command is already deserialized when `execute` is called.
///
/// # Example
///
/// ```text
/// // Define params with #[myko_command(...)]:
/// #[myko_command(result = "()")]
/// pub struct DeleteMachine {
///   pub id: Arc<str>,
/// }
///
/// // Implement the business logic:
/// impl CommandHandler for DeleteMachine {
///   fn execute(self, ctx: CommandContext) -> Result<(), CommandError> {
///     // validate input, query current state, emit SET/DEL events
///     Ok(())
///   }
/// }
///
/// // Register the handler (usually near the command definition):
/// register_command_handler!(DeleteMachine);
/// ```
pub trait CommandHandler: crate::command::CommandParams {
    /// Select the concrete durable scope for generated federation commands.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::unreachable)]
    fn scope(
        &self,
        _node_id: myko_federation::NodeId,
    ) -> <<Self as myko_federation::MykoCommandContract>::Scope as myko_federation::MykoItem>::Id
    where
        Self: myko_federation::MykoCommandContract,
    {
        unreachable!("durable command handlers must select a scope")
    }

    /// Declare durable resources used by this command before execution.
    #[cfg(not(target_arch = "wasm32"))]
    fn authority_claims(
        &self,
        node_id: myko_federation::NodeId,
    ) -> Vec<myko_federation::ResourceClaim>
    where
        Self: myko_federation::MykoCommandContract,
    {
        let scope = self.scope(node_id);
        vec![myko_federation::ResourceClaim::scope(
            myko_federation::ScopeId::for_item::<Self::Scope>(&scope),
            myko_federation::ResourceClaimKind::Primary,
        )]
    }

    /// Declare opaque application capabilities used by a durable command.
    #[cfg(not(target_arch = "wasm32"))]
    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        Vec::new()
    }

    /// Execute the command synchronously.
    ///
    /// `self` is the deserialized command parameters (owned, consumed by execution).
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    #[allow(clippy::unreachable)]
    fn execute(self, _ctx: CommandContext) -> Result<Self::Result, CommandError> {
        unreachable!("command handlers execute on the server")
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn command_capability_error(error: myko_federation::NodeError) -> CommandError {
    use myko_federation::NodeError;
    match error {
        error @ (NodeError::AuthorizationDenied(_)
        | NodeError::ItemServiceMismatch { .. }
        | NodeError::InvalidItemMutation(_)
        | NodeError::CommandSchemaMismatch { .. }) => CommandError::reject(error.to_string()),
        error => CommandError::retry(error.to_string()),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn normalized_command_claims<C>(
    command: &C,
    node_id: myko_federation::NodeId,
    scope_id: myko_federation::ScopeId,
) -> Vec<myko_federation::ResourceClaim>
where
    C: CommandHandler + myko_federation::MykoCommandContract,
{
    use myko_federation::{AccessOperation, FederationPermission, ResourceClaimKind};
    let mut claims = command.authority_claims(node_id);
    if !claims.iter().any(|claim| {
        claim.kind == ResourceClaimKind::Primary
            && claim.selection == myko_federation::ScopeSelection::Exact(scope_id.clone())
    }) {
        claims.push(myko_federation::ResourceClaim::scope(
            scope_id,
            ResourceClaimKind::Primary,
        ));
    }
    for claim in claims
        .iter_mut()
        .filter(|claim| claim.kind == ResourceClaimKind::Primary)
    {
        claim
            .service_id
            .get_or_insert_with(|| myko_federation::ServiceId::new(C::SERVICE_ID));
        if let Some(item_type) = C::ITEM_TYPE {
            claim.item_type.get_or_insert_with(|| item_type.to_owned());
        }
        if !claim
            .required_permissions
            .contains(&FederationPermission::Write)
        {
            claim.required_permissions.push(FederationPermission::Write);
        }
        if !claim
            .required_operations
            .contains(&AccessOperation::SubmitCommand)
        {
            claim
                .required_operations
                .push(AccessOperation::SubmitCommand);
        }
    }
    claims
}

/// Type-erased durable command lifecycle selected by an activated service.
#[cfg(not(target_arch = "wasm32"))]
pub trait DurableCommandExecutor: Send + Sync + 'static {
    /// Authenticate and validate an untrusted command submission.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload or typed command contract is invalid.
    fn authenticate(
        &self,
        node_id: myko_federation::NodeId,
        principal_id: myko_federation::PrincipalId,
        submission: myko_federation::CommandSubmission,
    ) -> Result<myko_federation::CommandRequest, myko_federation::NodeError>;

    /// Dispatch an admitted command through its typed handler.
    ///
    /// # Errors
    ///
    /// Returns an error when declaration, authorization, execution, or commit fails.
    fn dispatch(
        &self,
        node: &myko_federation::Node,
        resources: ApplicationResources,
        command_id: myko_federation::CommandId,
        trusted_framework: bool,
    ) -> Result<myko_federation::CommandDispatchResult, crate::AppError>;
}

#[cfg(not(target_arch = "wasm32"))]
struct DurableCommandExecutorAdapter<C>(std::marker::PhantomData<fn() -> C>);

#[cfg(not(target_arch = "wasm32"))]
impl<C> DurableCommandExecutor for DurableCommandExecutorAdapter<C>
where
    C: CommandHandler
        + myko_federation::MykoCommand
        + myko_federation::MykoCommandContract<Output = C::Result>,
{
    fn authenticate(
        &self,
        node_id: myko_federation::NodeId,
        principal_id: myko_federation::PrincipalId,
        submission: myko_federation::CommandSubmission,
    ) -> Result<myko_federation::CommandRequest, myko_federation::NodeError> {
        let command: C = serde_json::from_slice(&submission.payload)
            .map_err(|error| myko_federation::NodeError::CommandDecoding(error.to_string()))?;
        let scope = command.scope(node_id);
        let scope_id = myko_federation::ScopeId::for_item::<C::Scope>(&scope);
        let mut request = submission.authenticate(scope_id.clone(), principal_id);
        request.resource_claims = normalized_command_claims(&command, node_id, scope_id);
        request.application_capabilities = command.required_capabilities();
        Ok(request)
    }

    fn dispatch(
        &self,
        node: &myko_federation::Node,
        resources: ApplicationResources,
        command_id: myko_federation::CommandId,
        trusted_framework: bool,
    ) -> Result<myko_federation::CommandDispatchResult, crate::AppError> {
        let handle = |declared: &mut myko_federation::DeclaredCommandContext<C>| {
            declared
                .body()
                .clone()
                .execute(CommandContext::from_federation(
                    declared.command_context().clone(),
                    resources,
                ))
                .map_err(CommandError::into_federation)
        };
        if trusted_framework {
            node.dispatch_trusted_framework_command::<C, _>(command_id, handle)
        } else {
            node.dispatch_declared_command::<C, _>(command_id, handle)
        }
        .map_err(crate::AppError::Node)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
#[must_use]
pub fn durable_command_executor<C>() -> Arc<dyn DurableCommandExecutor>
where
    C: CommandHandler
        + myko_federation::MykoCommand
        + myko_federation::MykoCommandContract<Output = C::Result>,
{
    Arc::new(DurableCommandExecutorAdapter::<C>(std::marker::PhantomData))
}

/// Type-erased command executor for dynamic dispatch.
///
/// This is used internally by the registry to execute commands
/// without knowing their concrete types at compile time.
pub trait DynCommandExecutor: Send + Sync + 'static {
    /// The command ID this executor handles
    fn command_id(&self) -> &'static str;

    /// Execute the command from a JSON value
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn execute_from_value(
        &self,
        command: Value,
        ctx: CommandContext,
    ) -> Result<Value, CommandError>;
}

/// Adapter that wraps a `CommandHandler` to provide `DynCommandExecutor`
pub struct CommandExecutorAdapter<C: CommandHandler> {
    _phantom: std::marker::PhantomData<C>,
}

impl<C: CommandHandler> CommandExecutorAdapter<C> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<C: CommandHandler> Default for CommandExecutorAdapter<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: CommandHandler> DynCommandExecutor for CommandExecutorAdapter<C> {
    fn command_id(&self) -> &'static str {
        C::command_id_static()
    }

    fn execute_from_value(
        &self,
        mut command: Value,
        ctx: CommandContext,
    ) -> Result<Value, CommandError> {
        // `tx` belongs to CommandRequest and is already represented by the
        // RequestContext. Leaving it in the flattened wire object makes a
        // command with `#[serde(deny_unknown_fields)]` reject every valid
        // native-WS and MCP invocation before its handler can run.
        if let Some(params) = command.as_object_mut() {
            params.remove("tx");
        }

        // Deserialize the command
        let cmd: C = serde_json::from_value(command).map_err(|e| {
            CommandError::new(
                ctx.tx(),
                C::command_id_static(),
                format!("Failed to deserialize command: {e}"),
            )
        })?;

        // Execute the handler. Same "myko.command" span as
        // CommandContext::execute_command, for the inbound (ws/wire)
        // dispatch path — this is the DynCommandExecutor adapter used when
        // a command arrives already-serialized rather than called directly.
        let _span = tracing::trace_span!("myko.command", cmd = C::command_id_static()).entered();
        // "external": arrived pre-serialized — the funnel for both native-WS
        // command dispatch and MCP HTTP/WS in-process tool calls — see
        // `dispatch_metrics::record_command`.
        crate::server::dispatch_metrics::record_command(C::command_id_static(), "external");
        let result = cmd.execute(ctx)?;

        // Serialize the result
        serde_json::to_value(result).map_err(|e| {
            CommandError::new(
                String::new(),
                C::command_id_static(),
                format!("Failed to serialize result: {e}"),
            )
        })
    }
}

/// Type-erased command executor factory for inventory registration
pub type CommandExecutorFactory = fn() -> Box<dyn DynCommandExecutor>;

/// Registration entry for command handlers
pub struct CommandHandlerRegistration {
    pub command_id: &'static str,
    pub service_id: Option<crate::ServiceTypeId>,
    pub factory: CommandExecutorFactory,
    #[cfg(not(target_arch = "wasm32"))]
    pub durable_factory: Option<fn() -> Arc<dyn DurableCommandExecutor>>,
}

inventory::collect!(CommandHandlerRegistration);

/// Macro to register a command handler with the inventory.
///
/// # Example
///
/// ```text
/// register_command_handler!(DeleteMachine);
/// ```
#[macro_export]
macro_rules! register_command_handler {
    ($cmd:ty) => {
        $crate::register_command_handler!($cmd, service_id = None);
    };
    ($cmd:ty, service_id = $service_id:expr) => {
        $crate::inventory::submit! {
            $crate::command::CommandHandlerRegistration {
                command_id: <$cmd as $crate::command::CommandIdStatic>::COMMAND_ID,
                service_id: $service_id,
                factory: || Box::new($crate::command::CommandExecutorAdapter::<$cmd>::new()),
                durable_factory: None,
            }
        }
    };
}

/// Register a generated durable command on the retained handler catalog.
#[cfg(not(target_arch = "wasm32"))]
#[macro_export]
macro_rules! register_durable_command_handler {
    ($cmd:ty, service_id = $service_id:expr) => {
        $crate::inventory::submit! {
            $crate::command::CommandHandlerRegistration {
                command_id: <$cmd as $crate::command::CommandIdStatic>::COMMAND_ID,
                service_id: Some($service_id),
                factory: || Box::new($crate::command::CommandExecutorAdapter::<$cmd>::new()),
                durable_factory: Some($crate::command::durable_command_executor::<$cmd>),
            }
        }
    };
}
