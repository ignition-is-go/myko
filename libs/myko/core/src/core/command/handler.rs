use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    command::CommandError, core::capability::RequestScoped, query::QueryParams,
    request::RequestContext, server::MykoServerContext,
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

    server_ctx: Arc<MykoServerContext>,
}

impl CommandContext {
    /// Create a new `CommandContext`.
    #[must_use]
    pub const fn new(
        command_id: Arc<str>,
        req: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
    ) -> Self {
        Self {
            req,
            command_id,
            server_ctx,
        }
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
                .server_ctx
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
                .server_ctx
                .query_snapshot(query, self.req.clone())
                .into_iter()
                .collect())
        }
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
                .server_ctx
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

        let query = self.server_ctx.edges::<E>();
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
            .server_ctx
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
                .server_ctx
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
                .server_ctx
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
        &self.server_ctx
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
    pub factory: CommandExecutorFactory,
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
        $crate::inventory::submit! {
            $crate::command::CommandHandlerRegistration {
                command_id: <$cmd as $crate::command::CommandIdStatic>::COMMAND_ID,
                factory: || Box::new($crate::command::CommandExecutorAdapter::<$cmd>::new()),
            }
        }
    };
}
