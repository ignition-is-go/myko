use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, RwLock},
};

#[cfg(not(target_arch = "wasm32"))]
use hyphae::{MapExt as _, Materialize as _};

use crate::{MykoService, ServiceTypeId, server::HandlerRegistry};

/// Failure while composing or executing a retained application.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Node(#[from] myko_federation::NodeError),
    #[error("duplicate {kind} handler ID {id}")]
    DuplicateHandler { kind: &'static str, id: String },
    #[error("duplicate application service ID {id}")]
    DuplicateService { id: String },
    #[error("unregistered {kind} handler ID {id}")]
    UnregisteredHandler { kind: &'static str, id: String },
    #[error("reactive application state unavailable: {0}")]
    State(String),
    #[error("handler serialization failed: {0}")]
    Serialization(String),
    #[error("application resource {type_name} is not installed")]
    MissingResource { type_name: &'static str },
}

/// Typed process-local services available to command handlers.
#[derive(Clone, Default)]
pub struct ApplicationResources {
    values: Arc<RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>,
    capabilities: Arc<RwLock<HashMap<TypeId, myko_federation::CapabilityId>>>,
}

impl std::fmt::Debug for ApplicationResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApplicationResources")
            .field(
                "values",
                &self.values.read().map_or(0, |values| values.len()),
            )
            .finish_non_exhaustive()
    }
}

impl ApplicationResources {
    /// Install or replace one typed process-local resource.
    ///
    /// # Errors
    ///
    /// Returns an error when the resource registry lock is poisoned.
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

    /// Resolve one typed process-local resource.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry is unavailable or the resource has
    /// not been installed.
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

    fn register_capability<T: 'static>(
        &self,
        capability: myko_federation::CapabilityId,
    ) -> Result<(), AppError> {
        self.capabilities
            .write()
            .map_err(|_| AppError::State("application resource registry is poisoned".to_owned()))?
            .insert(TypeId::of::<T>(), capability);
        Ok(())
    }

    pub(crate) fn capability<T: 'static>(
        &self,
    ) -> Result<Option<myko_federation::CapabilityId>, AppError> {
        self.capabilities
            .read()
            .map_err(|_| AppError::State("application resource registry is poisoned".to_owned()))
            .map(|capabilities| capabilities.get(&TypeId::of::<T>()).cloned())
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod prepared_request;
#[cfg(not(target_arch = "wasm32"))]
pub use myko_federation::AccessTarget;
#[cfg(not(target_arch = "wasm32"))]
pub use prepared_request::{HistorySelection, PreparedEnvelope, PreparedRequest};

/// An immutable selection of application services and their retained handlers.
pub struct MykoApplication {
    services: BTreeSet<ServiceTypeId>,
    handlers: Arc<HandlerRegistry>,
    resources: ApplicationResources,
    capabilities: BTreeMap<myko_federation::CapabilityId, myko_federation::ApplicationCapability>,
    durable_commands:
        BTreeMap<(ServiceTypeId, &'static str), Arc<dyn crate::command::DurableCommandExecutor>>,
}

impl Default for MykoApplication {
    fn default() -> Self {
        Self::new()
    }
}

impl MykoApplication {
    /// Start a typed application declaration.
    #[must_use]
    pub fn builder() -> MykoApplicationBuilder {
        MykoApplicationBuilder::default()
    }

    /// Return the explicitly activated services.
    #[must_use]
    pub fn services(&self) -> impl ExactSizeIterator<Item = ServiceTypeId> + '_ {
        self.services.iter().copied()
    }

    /// Return the inventory registry filtered to activated services.
    #[must_use]
    pub const fn handlers(&self) -> &Arc<HandlerRegistry> {
        &self.handlers
    }

    #[must_use]
    pub fn resources(&self) -> ApplicationResources {
        self.resources.clone()
    }

    /// Return the capabilities declared by this application.
    #[must_use]
    pub fn authority_capabilities(
        &self,
    ) -> impl ExactSizeIterator<Item = &myko_federation::ApplicationCapability> {
        self.capabilities.values()
    }
}

/// Typed builder for one retained Myko application.
#[derive(Debug, Default)]
pub struct MykoApplicationBuilder {
    services: BTreeSet<ServiceTypeId>,
    resources: ApplicationResources,
    capabilities: BTreeMap<myko_federation::CapabilityId, myko_federation::ApplicationCapability>,
}

/// One retained Myko application attached to a durable federation node.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct ApplicationHost {
    node: myko_federation::Node,
    application: Arc<MykoApplication>,
    server: Arc<crate::server::MykoServerContext>,
    access_policy: Option<Arc<dyn myko_federation::AccessPolicy>>,
}

/// Joined, event-driven dispatch of every durable command in an application.
pub struct CommandDispatchGuard {
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    failure: Arc<RwLock<Option<String>>>,
}

impl CommandDispatchGuard {
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        self.failure.read().ok().and_then(|failure| failure.clone())
    }

    // Guards share an async shutdown shape with transport/runtime guards even
    // though joining this dedicated thread is synchronous.
    #[allow(clippy::unused_async)]
    pub async fn shutdown(mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _joined = thread.join();
        }
    }
}

impl Drop for CommandDispatchGuard {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(thread) = self.thread.take()
            && thread.thread().id() != std::thread::current().id()
        {
            let _joined = thread.join();
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ApplicationHost {
    /// Attach an immutable retained application to a durable node.
    ///
    /// # Errors
    ///
    /// Returns an error when its joined durable-source executor cannot start.
    pub fn new(node: myko_federation::Node, application: MykoApplication) -> Result<Self, String> {
        let application = Arc::new(application);
        let server = Arc::new(
            crate::server::MykoServerContext::for_federated_application(
                node.clone(),
                Arc::clone(application.handlers()),
            )?
            .with_application_resources(application.resources()),
        );
        Ok(Self {
            node,
            application,
            server,
            access_policy: None,
        })
    }

    #[must_use]
    pub const fn node(&self) -> &myko_federation::Node {
        &self.node
    }

    #[must_use]
    pub fn node_id(&self) -> myko_federation::NodeId {
        self.node.node_id()
    }

    /// Install the policy used for durable command admission.
    ///
    /// # Errors
    ///
    /// Returns an error when the node's policy registry is unavailable.
    pub fn with_access_policy(
        mut self,
        access_policy: Arc<dyn myko_federation::AccessPolicy>,
    ) -> Result<Self, AppError> {
        self.node
            .set_command_access_policy(Arc::clone(&access_policy))?;
        self.access_policy = Some(access_policy);
        Ok(self)
    }

    #[must_use]
    pub fn resources(&self) -> ApplicationResources {
        self.application.resources()
    }

    /// Return the capabilities declared by the attached application.
    #[must_use]
    pub fn authority_capabilities(
        &self,
    ) -> impl ExactSizeIterator<Item = &myko_federation::ApplicationCapability> {
        self.application.authority_capabilities()
    }

    /// Return the service identities activated on this host.
    #[must_use]
    pub fn service_type_ids(&self) -> impl ExactSizeIterator<Item = ServiceTypeId> + '_ {
        self.application.services()
    }

    /// Return whether this application activates the submitted service and
    /// links its retained command registration.
    #[must_use]
    pub fn handles_submission(&self, submission: &myko_federation::CommandSubmission) -> bool {
        self.find_command(&submission.service_id, &submission.command_type)
            .is_some()
    }

    fn find_command(
        &self,
        service_id: &myko_federation::ServiceId,
        command_type: &str,
    ) -> Option<&Arc<dyn crate::command::DurableCommandExecutor>> {
        self.application
            .durable_commands
            .iter()
            .find_map(|((service, command), factory)| {
                (service.as_str() == service_id.as_str() && *command == command_type)
                    .then_some(factory)
            })
    }

    fn typed_command<C>(&self) -> Result<&Arc<dyn crate::command::DurableCommandExecutor>, AppError>
    where
        C: myko_federation::MykoCommandContract,
    {
        self.application
            .durable_commands
            .get(&(C::SERVICE_ID, C::OPERATION_ID))
            .ok_or_else(|| AppError::UnregisteredHandler {
                kind: "command",
                id: format!("{}/{}", C::SERVICE_ID, C::OPERATION_ID),
            })
    }

    /// Convert an untrusted submission into a typed authenticated request.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is not registered or its payload does
    /// not satisfy the registered command contract.
    pub fn authenticate_command_submission(
        &self,
        principal_id: myko_federation::PrincipalId,
        submission: myko_federation::CommandSubmission,
    ) -> Result<myko_federation::CommandRequest, myko_federation::NodeError> {
        self.find_command(&submission.service_id, &submission.command_type)
            .ok_or_else(|| {
                myko_federation::NodeError::InvalidCommandState(format!(
                    "application does not register command {}/{}",
                    submission.service_id, submission.command_type
                ))
            })?
            .authenticate(self.node.node_id(), principal_id, submission)
    }

    /// Dispatch one already-admitted command through its retained handler.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is unknown, its handler is absent, or
    /// execution cannot reach a durable terminal state.
    pub fn dispatch_registered_command(
        &self,
        command_id: myko_federation::CommandId,
    ) -> Result<myko_federation::CommandDispatchResult, AppError> {
        let command = self
            .node
            .command(command_id)?
            .ok_or(myko_federation::NodeError::UnknownCommand(command_id))?;
        self.find_command(&command.request.service_id, &command.request.command_type)
            .ok_or_else(|| AppError::UnregisteredHandler {
                kind: "command",
                id: format!(
                    "{}/{}",
                    command.request.service_id, command.request.command_type
                ),
            })?
            .dispatch(&self.node, self.application.resources(), command_id, false)
    }

    /// Admit one typed command for an authenticated principal without running it.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication, authorization, or durable
    /// admission fails.
    pub fn submit_authenticated_command<C>(
        &self,
        principal_id: myko_federation::PrincipalId,
        command: &C,
    ) -> Result<myko_federation::CommandSnapshot, AppError>
    where
        C: crate::command::CommandHandler
            + myko_federation::MykoCommand
            + myko_federation::MykoCommandContract<Output = C::Result>,
    {
        let submission = myko_federation::CommandSubmission::for_command(command)?;
        let request = self.typed_command::<C>()?.authenticate(
            self.node.node_id(),
            principal_id.clone(),
            submission,
        )?;
        self.node
            .prepare_command(principal_id, request)
            .map_err(myko_federation::NodeError::from)?
            .submit()
            .map_err(AppError::Node)
    }

    /// Start dispatch for pending commands owned by this application.
    ///
    /// Temporary authority unavailability defers that command without stopping
    /// other work. Prepared effects resume from the journal without rerunning
    /// handlers. The journal also reconstructs deferred work after restart.
    ///
    /// # Errors
    ///
    /// Returns an error when the pending-command watch or dispatch thread
    /// cannot be started.
    pub fn drive_commands(&self) -> Result<CommandDispatchGuard, AppError> {
        let pending = self.node.watch_pending_local_application_commands()?;
        let application = self.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let failure = Arc::new(RwLock::new(None));
        let thread_failure = Arc::clone(&failure);
        let thread = std::thread::Builder::new()
            .name("myko-command-dispatch".to_owned())
            .spawn(move || {
                if let Err(error) = application.dispatch_pending_commands(pending, &thread_stop)
                    && let Ok(mut failure) = thread_failure.write()
                {
                    *failure = Some(error.to_string());
                }
            })
            .map_err(|error| {
                AppError::State(format!("failed to start command dispatch: {error}"))
            })?;
        Ok(CommandDispatchGuard {
            stop,
            thread: Some(thread),
            failure,
        })
    }

    fn dispatch_pending_commands(
        &self,
        mut pending: myko_federation::PendingCommandSubscription,
        stop: &std::sync::atomic::AtomicBool,
    ) -> Result<(), AppError> {
        use std::time::{Duration, Instant};

        const SHUTDOWN_CHECK: Duration = Duration::from_millis(50);
        const AUTHORITY_RETRY: Duration = Duration::from_millis(250);
        let mut queued = BTreeMap::<myko_federation::CommandId, Instant>::new();
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            let timeout = queued.values().min().map_or(SHUTDOWN_CHECK, |next| {
                next.saturating_duration_since(Instant::now())
                    .min(SHUTDOWN_CHECK)
            });
            if let Some(command) = pending.recv_timeout(timeout)? {
                // A lifecycle replay must not defeat an existing retry delay.
                queued
                    .entry(command.request.id)
                    .or_insert_with(Instant::now);
            }
            let now = Instant::now();
            let next = queued
                .iter()
                .filter(|(_, ready)| **ready <= now)
                .min_by_key(|(_, ready)| **ready)
                .map(|(id, _)| *id);
            let Some(command_id) = next else {
                continue;
            };
            queued.remove(&command_id);
            match self.dispatch_registered_command(command_id) {
                Ok(_) => {}
                Err(AppError::Node(myko_federation::NodeError::AuthorityUnavailable(_))) => {
                    let retry_at =
                        Instant::now().checked_add(AUTHORITY_RETRY).ok_or_else(|| {
                            AppError::State(
                                "authority retry deadline exceeds clock range".to_owned(),
                            )
                        })?;
                    queued.insert(command_id, retry_at);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn dispatch_typed<C>(
        &self,
        command_id: myko_federation::CommandId,
        trusted_framework: bool,
    ) -> Result<myko_federation::CommandDispatchResult, AppError>
    where
        C: myko_federation::MykoCommandContract,
    {
        self.typed_command::<C>()?.dispatch(
            &self.node,
            self.application.resources(),
            command_id,
            trusted_framework,
        )
    }

    /// Admit and execute one typed command as this node.
    ///
    /// # Errors
    ///
    /// Returns an error when admission, dispatch, or typed result decoding fails.
    pub fn exec_command<C>(&self, command: C) -> Result<C::Output, AppError>
    where
        C: crate::command::CommandHandler
            + myko_federation::MykoCommand
            + myko_federation::MykoCommandContract<Output = C::Result>,
    {
        self.exec_authenticated_command(
            myko_federation::PrincipalId::for_node(self.node.node_id()),
            command,
        )
    }

    /// Admit and execute one typed command for an authenticated principal.
    ///
    /// # Errors
    ///
    /// Returns an error when admission, dispatch, or typed result decoding fails.
    // Consuming a command expresses one-shot intent even though admission
    // serializes it before the durable handler reconstructs its own value.
    #[allow(clippy::needless_pass_by_value)]
    pub fn exec_authenticated_command<C>(
        &self,
        principal_id: myko_federation::PrincipalId,
        command: C,
    ) -> Result<C::Output, AppError>
    where
        C: crate::command::CommandHandler
            + myko_federation::MykoCommand
            + myko_federation::MykoCommandContract<Output = C::Result>,
    {
        let submitted = self.submit_authenticated_command(principal_id, &command)?;
        let result = self.dispatch_typed::<C>(submitted.request.id, false)?;
        result
            .command
            .typed_completion::<C>()?
            .ok_or_else(|| AppError::State("command handler did not produce a result".to_owned()))
    }

    /// Execute a typed command using an explicit authority presentation.
    ///
    /// # Errors
    ///
    /// Returns an error when the executor does not match the presentation or
    /// the command cannot be authorized and completed.
    #[allow(clippy::needless_pass_by_value)]
    pub fn exec_authorized_command<C>(
        &self,
        authenticated_executor: myko_federation::PrincipalId,
        presentation: myko_federation::AuthorityPresentation,
        command: C,
    ) -> Result<C::Output, AppError>
    where
        C: crate::command::CommandHandler
            + myko_federation::MykoCommand
            + myko_federation::MykoCommandContract<Output = C::Result>,
    {
        if authenticated_executor != presentation.executor.id {
            return Err(AppError::State(
                "authority executor does not match authenticated principal".to_owned(),
            ));
        }
        let submission = myko_federation::CommandSubmission::for_command(&command)?;
        let mut request = self.typed_command::<C>()?.authenticate(
            self.node.node_id(),
            authenticated_executor.clone(),
            submission,
        )?;
        request.principal_id = presentation.principal.id.clone();
        request.authority = presentation;
        let submitted = self
            .node
            .prepare_command(authenticated_executor, request)
            .map_err(myko_federation::NodeError::from)?
            .submit()?;
        let result = self.dispatch_typed::<C>(submitted.request.id, false)?;
        result
            .command
            .typed_completion::<C>()?
            .ok_or_else(|| AppError::State("command handler did not produce a result".to_owned()))
    }

    /// Execute a framework-owned command through the trusted internal lane.
    ///
    /// # Errors
    ///
    /// Returns an error when trusted admission, dispatch, or typed result
    /// decoding fails.
    #[allow(clippy::needless_pass_by_value)]
    pub fn exec_trusted_framework_command<C>(
        &self,
        presentation: myko_federation::AuthorityPresentation,
        command: C,
    ) -> Result<C::Output, AppError>
    where
        C: crate::command::CommandHandler
            + myko_federation::MykoCommand
            + myko_federation::MykoCommandContract<Output = C::Result>,
    {
        let submission = myko_federation::CommandSubmission::for_command(&command)?;
        let mut request = self.typed_command::<C>()?.authenticate(
            self.node.node_id(),
            presentation.executor.id.clone(),
            submission,
        )?;
        request.principal_id = presentation.principal.id.clone();
        request.authority = presentation;
        let result = self
            .node
            .dispatch_trusted_framework_submission(request, |command_id| {
                self.dispatch_typed::<C>(command_id, true)
            })?;
        result
            .command
            .typed_completion::<C>()?
            .ok_or_else(|| AppError::State("command handler did not produce a result".to_owned()))
    }

    #[must_use]
    pub const fn application(&self) -> &Arc<MykoApplication> {
        &self.application
    }

    #[must_use]
    pub const fn server(&self) -> &Arc<crate::server::MykoServerContext> {
        &self.server
    }

    /// Return whether every retained source opened for a selection has
    /// reached the durable node's current authoritative frontier.
    #[must_use]
    pub fn source_selection_is_current(
        &self,
        source_node: Option<myko_federation::NodeId>,
        scope_id: &myko_federation::ScopeId,
        frontier: Option<myko_federation::LogPosition>,
    ) -> bool {
        self.server.federated().is_some_and(|runtime| {
            runtime.selection_is_current_at(source_node, Some(scope_id), frontier)
        })
    }

    /// Open one typed durable item source without introducing a handler.
    ///
    /// Framework policies use this for indexed internal facts that must stay
    /// authoritative but are not themselves an application-facing query.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable source cannot be opened or contains
    /// values that do not match the requested item type.
    pub fn watch_items<T>(
        &self,
        source_node: Option<myko_federation::NodeId>,
        scope_id: Option<myko_federation::ScopeId>,
    ) -> Result<crate::view::TypedViewCellMap<T>, String>
    where
        T: crate::MykoItem + crate::item::Eventable + crate::item::AnyItem,
    {
        let source = self
            .server
            .federated()
            .ok_or_else(|| "application has no federation runtime".to_owned())?
            .items::<T>(source_node, scope_id)?;
        Ok(crate::item::typed_map_arc_from_any_item::<T>(
            source.rows(),
            "ApplicationHost::watch_items",
        ))
    }

    /// Open one exact scope across every authoritative source while retaining
    /// source identity and revision metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the federation runtime or projection is unavailable.
    pub fn watch_items_across_sources<T>(
        &self,
        scope_id: myko_federation::ScopeId,
    ) -> Result<crate::server::SourcedItemMap<T>, String>
    where
        T: crate::MykoItem + crate::item::Eventable + crate::item::AnyItem,
    {
        self.server
            .federated()
            .ok_or_else(|| "application has no federation runtime".to_owned())?
            .items_across_sources::<T>(scope_id)
    }

    /// Read typed items from all origins at one current local history cut.
    ///
    /// This trusted in-process read uses the same projection and completeness
    /// assessment as retained sources, without waiting for their background tasks.
    /// Callers must inspect liveness before treating the rows as complete. It does
    /// not establish remote coverage, custody, or caller authorization.
    ///
    /// # Errors
    ///
    /// Returns an error when local history or its typed projection cannot be read.
    pub fn snapshot_items_across_sources_selected<T>(
        &self,
        selection: &myko_federation::ScopeSelection,
    ) -> Result<myko_federation::LiveSubscriptionState<crate::SourcedItemSnapshot<T>>, String>
    where
        T: crate::MykoItem,
    {
        let snapshot = myko_federation::SelectedHistorySnapshot::current(&self.node)
            .map_err(|error| error.to_string())?;
        crate::server::federated_source::selected_snapshot_state::<T>(&snapshot, selection)
    }

    /// Return the shared durable source behind an internal typed projection.
    #[doc(hidden)]
    pub fn item_source<T>(
        &self,
        source_node: Option<myko_federation::NodeId>,
        scope_id: Option<myko_federation::ScopeId>,
    ) -> Result<Arc<crate::server::federated_source::FederatedMapSource>, String>
    where
        T: crate::MykoItem + crate::item::Eventable + crate::item::AnyItem,
    {
        self.server
            .federated()
            .ok_or_else(|| "application has no federation runtime".to_owned())?
            .items::<T>(source_node, scope_id)
    }

    /// Open a prepared handler directly into the retained client session.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler is absent, malformed, or cannot open
    /// its durable projection.
    pub fn open_handler<W: crate::server::SessionSink>(
        &self,
        session: &mut crate::server::ClientSession<W>,
        tx: Arc<str>,
        request: myko_wire::HandlerRequest,
    ) -> Result<(), String> {
        let request_context = Arc::new(crate::request::RequestContext::internal(
            Arc::clone(&tx),
            self.server.host_id,
            "node-handler",
        ));
        let source = crate::server::federated_source::FederatedRequest {
            source_node: request.source_node,
            scope_id: request.scope_id.clone(),
        };
        match request.kind {
            myko_federation::HandlerKind::Query | myko_federation::HandlerKind::View => {
                let required_cut = self
                    .node
                    .local_history_cut()
                    .map_err(|error| error.to_string())?;
                let output = self.server.open_native_map(&request, request_context)?;
                session.subscribe_node_handler_map(tx, output, required_cut)?;
            }
            myko_federation::HandlerKind::Report => {
                let report = self.server.handler_registry.open_federated_report(
                    &request.handler_id,
                    request.params,
                    request_context,
                    Arc::clone(&self.server),
                    source,
                )?;
                session.subscribe_node_handler_report(tx, report)?;
            }
            myko_federation::HandlerKind::Command => {
                return Err("commands are admitted through SubmitCommand".to_owned());
            }
        }
        Ok(())
    }

    pub(crate) fn handler_authority(
        &self,
        request: &myko_wire::HandlerRequest,
    ) -> Result<crate::server::HandlerAuthority, String> {
        self.application.handlers.handler_authority(
            request.kind,
            &request.handler_id,
            request.params.clone(),
            self.node.node_id(),
        )
    }

    /// Open a typed local-map view against a durable source selection.
    ///
    /// # Errors
    ///
    /// Returns an error when the view is absent, malformed, or returns a retained
    /// publication. Raw maps cannot preserve retained publication evidence.
    pub fn watch_view_at<V>(
        &self,
        source_node: Option<myko_federation::NodeId>,
        scope_id: Option<myko_federation::ScopeId>,
        view: &V,
    ) -> Result<crate::view::TypedViewCellMap<V::Item>, String>
    where
        V: crate::view::ViewParams,
    {
        let request = Arc::new(crate::request::RequestContext::internal(
            Arc::from(uuid::Uuid::new_v4().to_string()),
            self.server.host_id,
            "node-view",
        ));
        let rows = self
            .application
            .handlers
            .open_federated_view(
                V::view_id_static().as_ref(),
                serde_json::to_value(view).map_err(|error| error.to_string())?,
                request,
                Arc::clone(&self.server),
                crate::server::federated_source::FederatedRequest {
                    source_node,
                    scope_id,
                },
            )?
            .into_local_map()?;
        Ok(crate::item::typed_map_arc_from_any_item::<V::Item>(
            rows,
            "ApplicationHost::watch_view",
        ))
    }

    /// Open a typed local-map view using its declared source and scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the view cannot be serialized, found, or opened,
    /// including when its output is a retained publication rather than a local map.
    pub fn watch_view<V>(&self, view: &V) -> Result<crate::view::TypedViewCellMap<V::Item>, String>
    where
        V: crate::view::ViewParams,
    {
        self.watch_view_at(
            view.source_node(self.node.node_id()),
            view.scope_id(self.node.node_id()),
            view,
        )
    }

    /// Open an in-process local-map view as a live collection.
    ///
    /// The row map remains the only mutable state. Its absent history cut and
    /// local current state do not certify durable or federated completeness.
    /// Retained publications must use the native handler subscription path.
    ///
    /// # Errors
    ///
    /// Returns an error when the typed view cannot be opened or returns a retained
    /// publication, whose evidence this local-map adapter cannot preserve.
    pub fn watch_view_live<V>(
        &self,
        view: &V,
    ) -> Result<myko_federation::LiveCollection<V::Item>, String>
    where
        V: crate::view::ViewParams,
    {
        let rows = self.watch_view(view)?;
        let state = hyphae::Cell::new(myko_federation::LiveCollectionState {
            through: None::<myko_federation::LogPosition>,
            liveness: myko_federation::SubscriptionLiveness::Current,
        })
        .lock();
        Ok(myko_federation::CollectionPlan::materialize(
            myko_federation::MapCollectionPlan::new(rows, state),
        ))
    }

    /// Open one typed retained report against a durable source selection.
    ///
    /// # Errors
    ///
    /// Returns an error when the report is absent, malformed, or produces a
    /// different output type than its registration declares.
    /// # Panics
    ///
    /// Panics only if a report registration violates its own declared output
    /// type; the retained registration boundary validates this invariant.
    #[allow(clippy::expect_used)]
    pub fn watch_report_at<R>(
        &self,
        source_node: Option<myko_federation::NodeId>,
        scope_id: Option<myko_federation::ScopeId>,
        report: &R,
    ) -> Result<
        hyphae::Cell<Arc<<R as crate::report::ReportHandler>::Output>, hyphae::CellImmutable>,
        String,
    >
    where
        R: crate::report::ReportParams
            + crate::report::ReportOutputType<Output = <R as crate::report::ReportHandler>::Output>,
    {
        let request = Arc::new(crate::request::RequestContext::internal(
            Arc::from(uuid::Uuid::new_v4().to_string()),
            self.server.host_id,
            "node-report",
        ));
        let report = self.application.handlers.open_federated_report(
            R::report_id_static(),
            serde_json::to_value(report).map_err(|error| error.to_string())?,
            request,
            Arc::clone(&self.server),
            crate::server::federated_source::FederatedRequest {
                source_node,
                scope_id,
            },
        )?;
        Ok(report
            .map(|value| {
                Arc::new(
                    value
                        .as_any()
                        .downcast_ref::<<R as crate::report::ReportHandler>::Output>()
                        .expect("report registration returned its declared output type")
                        .clone(),
                )
            })
            .materialize())
    }

    /// Open a typed retained report using its declared source and scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the report cannot be serialized, found, or opened.
    pub fn watch_report<R>(
        &self,
        report: &R,
    ) -> Result<
        hyphae::Cell<Arc<<R as crate::report::ReportHandler>::Output>, hyphae::CellImmutable>,
        String,
    >
    where
        R: crate::report::ReportParams
            + crate::report::ReportOutputType<Output = <R as crate::report::ReportHandler>::Output>,
    {
        self.watch_report_at(
            report.source_node(self.node.node_id()),
            report.scope_id(self.node.node_id()),
            report,
        )
    }

    /// Open an in-process report with the lifecycle shape used by transport clients.
    ///
    /// # Errors
    ///
    /// Returns an error when the typed report cannot be opened.
    pub fn watch_report_live<R>(
        &self,
        report: &R,
    ) -> Result<
        myko_federation::LiveSubscription<<R as crate::report::ReportHandler>::Output>,
        String,
    >
    where
        R: crate::report::ReportParams
            + crate::report::ReportOutputType<Output = <R as crate::report::ReportHandler>::Output>,
        <R as crate::report::ReportHandler>::Output: hyphae::CellValue,
    {
        let state = self
            .watch_report(report)?
            .map(|value| myko_federation::LiveSubscriptionState {
                value: Some(value.as_ref().clone()),
                through: None::<myko_federation::LogPosition>,
                liveness: myko_federation::SubscriptionLiveness::Current,
            })
            .materialize();
        Ok(myko_federation::LiveSubscription::from_state_cell(state))
    }

    pub async fn shutdown(&self) {
        self.server.shutdown_federation().await;
    }
}

impl myko_federation::CommandClient for ApplicationHost {
    type Error = myko_federation::NodeError;

    fn submit_submission(
        &self,
        submission: myko_federation::CommandSubmission,
    ) -> myko_federation::CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            let principal = myko_federation::PrincipalId::for_node(self.node.node_id());
            let request = self.authenticate_command_submission(principal.clone(), submission)?;
            let command = self
                .node
                .prepare_command(principal, request)
                .map_err(myko_federation::NodeError::from)?
                .submit()?;
            Ok(myko_federation::CommandResponse {
                source_node: self.node.node_id(),
                command: Some(command),
            })
        })
    }

    fn command_state(
        &self,
        command_id: myko_federation::CommandId,
    ) -> myko_federation::CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            Ok(myko_federation::CommandResponse {
                source_node: self.node.node_id(),
                command: self.node.command(command_id)?,
            })
        })
    }

    fn cancel_command(
        &self,
        command_id: myko_federation::CommandId,
        reason: String,
    ) -> myko_federation::CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            Ok(myko_federation::CommandResponse {
                source_node: self.node.node_id(),
                command: Some(self.node.cancel(command_id, reason)?),
            })
        })
    }
}

impl ApplicationHost {
    /// Admit one typed-erased command submission with an explicit authority presentation.
    ///
    /// This is used by in-process client facades whose authenticated principal
    /// is application-owned rather than the node process identity.
    ///
    /// # Errors
    ///
    /// Returns an error when command authentication, authorization, or durable
    /// admission fails.
    pub fn submit_authorized_submission(
        &self,
        presentation: myko_federation::AuthorityPresentation,
        submission: myko_federation::CommandSubmission,
    ) -> Result<myko_federation::CommandResponse, myko_federation::NodeError> {
        let authenticated_executor = presentation.executor.id.clone();
        let mut request =
            self.authenticate_command_submission(authenticated_executor.clone(), submission)?;
        request.principal_id = presentation.principal.id.clone();
        request.authority = presentation;
        let command = self
            .node
            .prepare_command(authenticated_executor, request)
            .map_err(myko_federation::NodeError::from)?
            .submit()?;
        Ok(myko_federation::CommandResponse {
            source_node: self.node.node_id(),
            command: Some(command),
        })
    }
}

impl myko_federation::CommandWatchingClient for ApplicationHost {
    type Subscription = myko_federation::CommandWatch;

    fn watch_command(
        &self,
        command_id: myko_federation::CommandId,
    ) -> myko_federation::CommandWatchFuture<'_, Self::Subscription, Self::Error> {
        Box::pin(async move {
            let (_current, subscription) = self.node.watch_command(command_id)?;
            Ok(subscription)
        })
    }
}

impl MykoApplicationBuilder {
    /// Activate one typed service.
    #[must_use]
    pub fn service<S: MykoService>(mut self) -> Self {
        self.services.insert(S::SERVICE_ID);
        self
    }

    /// Install a typed process-local resource protected by a stable capability.
    ///
    /// # Errors
    ///
    /// Returns an error when the resource or capability registry is unavailable.
    pub fn resource<T>(
        mut self,
        capability: myko_federation::ApplicationCapability,
        value: T,
    ) -> Result<Self, AppError>
    where
        T: Send + Sync + 'static,
    {
        self.resources
            .register_capability::<T>(capability.id.clone())?;
        let _previous = self.resources.insert(value)?;
        self.capabilities.insert(capability.id.clone(), capability);
        Ok(self)
    }

    /// Declare a resource capability whose value is installed by the host.
    ///
    /// # Errors
    ///
    /// Returns an error when the capability registry is unavailable.
    pub fn resource_capability<T>(
        mut self,
        capability: myko_federation::ApplicationCapability,
    ) -> Result<Self, AppError>
    where
        T: Send + Sync + 'static,
    {
        self.resources
            .register_capability::<T>(capability.id.clone())?;
        self.capabilities.insert(capability.id.clone(), capability);
        Ok(self)
    }

    /// Register an application capability without a process-local resource.
    ///
    /// # Errors
    ///
    /// Returns an error when the capability identity is already registered.
    pub fn capability(
        mut self,
        capability: myko_federation::ApplicationCapability,
    ) -> Result<Self, AppError> {
        if self
            .capabilities
            .insert(capability.id.clone(), capability)
            .is_some()
        {
            return Err(AppError::State(
                "application capability is registered more than once".to_owned(),
            ));
        }
        Ok(self)
    }

    /// Activate an already type-checked service identity while migrating a
    /// host from the former application registry.
    #[doc(hidden)]
    #[must_use]
    pub fn service_id(mut self, service: ServiceTypeId) -> Self {
        self.services.insert(service);
        self
    }

    /// Freeze service activation and collect the retained handler inventory.
    #[must_use]
    pub fn build(self) -> MykoApplication {
        let handlers = Arc::new(HandlerRegistry::for_services(&self.services));
        let durable_commands = inventory::iter::<crate::command::CommandHandlerRegistration>
            .into_iter()
            .filter_map(|registration| {
                let service = registration.service_id?;
                self.services
                    .contains(&service)
                    .then_some((service, registration))
            })
            .filter_map(|(service, registration)| {
                registration
                    .durable_factory
                    .map(|factory| ((service, registration.command_id), factory()))
            })
            .collect();
        MykoApplication {
            services: self.services,
            handlers,
            resources: self.resources,
            capabilities: self.capabilities,
            durable_commands,
        }
    }
}

impl MykoApplication {
    #[must_use]
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Add one framework-owned durable service to an existing declaration.
    #[doc(hidden)]
    #[must_use]
    pub fn with_framework_service<S: MykoService>(self) -> Self {
        let mut builder = MykoApplicationBuilder {
            services: self.services,
            resources: self.resources,
            capabilities: self.capabilities,
        };
        builder.services.insert(S::SERVICE_ID);
        builder.build()
    }

    /// Declare a framework resource capability before the value is installed.
    #[doc(hidden)]
    pub fn with_framework_resource_capability<T: Send + Sync + 'static>(
        self,
        capability: myko_federation::ApplicationCapability,
    ) -> Result<Self, AppError> {
        let builder = MykoApplicationBuilder {
            services: self.services,
            resources: self.resources,
            capabilities: self.capabilities,
        };
        builder.resource_capability::<T>(capability).map(Self::from)
    }
}

impl From<MykoApplicationBuilder> for MykoApplication {
    fn from(builder: MykoApplicationBuilder) -> Self {
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use myko_federation::{
        AccessAttempt, AccessPolicy, AuthorityPresentation, AuthorityUnavailable,
        CommandClient as _, CommandWatchingClient as _, Node, NodeError, PrincipalId,
        PrincipalKind,
    };

    use super::*;
    use crate::{CommandContext, command::CommandError, prelude::*};

    #[derive(Debug)]
    struct UnavailablePolicy;

    impl AccessPolicy for UnavailablePolicy {
        fn decide<'a>(
            &'a self,
            _request: &'a AccessAttempt,
        ) -> myko_federation::PolicyDecision<'a> {
            Err(AuthorityUnavailable::StateNotCurrent).into()
        }
    }

    #[myko_service(UnavailableRoot)]
    pub struct UnavailableService;

    #[myko_item(service = UnavailableService, scope_root)]
    pub struct UnavailableRoot {
        label: String,
    }

    #[myko_command(bool, item = UnavailableRoot)]
    pub struct UnavailableCommand {
        id: UnavailableRootId,
    }

    impl CommandHandler for UnavailableCommand {
        fn scope(&self, _node_id: myko_federation::NodeId) -> UnavailableRootId {
            self.id.clone()
        }

        fn execute(self, _ctx: CommandContext) -> Result<bool, CommandError> {
            Ok(true)
        }
    }

    fn unavailable_host() -> Result<ApplicationHost, AppError> {
        ApplicationHost::new(
            Node::in_memory(),
            MykoApplication::builder()
                .service::<UnavailableService>()
                .build(),
        )
        .map_err(AppError::State)?
        .with_access_policy(Arc::new(UnavailablePolicy))
    }

    fn unavailable_command() -> UnavailableCommand {
        UnavailableCommand {
            id: UnavailableRootId::from("unavailable-root"),
        }
    }

    #[tokio::test]
    async fn typed_command_resumption_does_not_submit_again() -> Result<(), String> {
        let host = ApplicationHost::new(
            Node::in_memory(),
            MykoApplication::builder()
                .service::<UnavailableService>()
                .build(),
        )?
        .with_access_policy(Arc::new(myko_federation::AllowAllAccessPolicy))
        .map_err(|error| error.to_string())?;
        let submitted = host
            .submit_typed_command(unavailable_command())
            .await
            .map_err(|error| error.to_string())?
            .command
            .ok_or("command not retained")?;
        let waiting = host.await_typed_command::<UnavailableCommand>(submitted.request.id);
        tokio::pin!(waiting);
        if tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
            .await
            .is_ok()
        {
            return Err("resumption dispatched the pending command".to_owned());
        }
        host.dispatch_registered_command(submitted.request.id)
            .map_err(|error| error.to_string())?;
        if !waiting.await.map_err(|error| error.to_string())? {
            return Err("resumption lost the typed result".to_owned());
        }
        let before = host
            .node()
            .events_after(None)
            .map_err(|error| error.to_string())?;
        if !host
            .await_typed_command::<UnavailableCommand>(submitted.request.id)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("completed resumption lost the typed result".to_owned());
        }
        if before
            != host
                .node()
                .events_after(None)
                .map_err(|error| error.to_string())?
        {
            return Err("resumption wrote a new command lifecycle".to_owned());
        }
        if !matches!(
            host.await_typed_command::<UnavailableCommand>(myko_federation::CommandId::new())
                .await,
            Err(NodeError::UnknownCommand(_))
        ) {
            return Err("resumption did not report an unknown command".to_owned());
        }
        Ok(())
    }

    fn assert_unavailable_node_error(error: NodeError) -> Result<(), String> {
        match error {
            NodeError::AuthorityUnavailable(AuthorityUnavailable::StateNotCurrent) => Ok(()),
            NodeError::AuthorizationDenied(message) => Err(format!(
                "unavailable authority became authorization denial: {message}"
            )),
            other => Err(format!("unexpected node error: {other:?}")),
        }
    }

    fn assert_unavailable_app_error(error: AppError) -> Result<(), String> {
        match error {
            AppError::Node(error) => assert_unavailable_node_error(error),
            AppError::State(message) => Err(format!(
                "unavailable authority became application state error: {message}"
            )),
            other => Err(format!("unexpected application error: {other:?}")),
        }
    }

    fn assert_no_durable_events(host: &ApplicationHost) -> Result<(), String> {
        if host
            .node()
            .events_after(None)
            .map_err(|error| error.to_string())?
            .is_empty()
        {
            Ok(())
        } else {
            Err("unavailable command admission wrote durable history".to_owned())
        }
    }

    #[test]
    fn authenticated_command_submission_preserves_unavailable_without_rejection()
    -> Result<(), String> {
        let host = unavailable_host().map_err(|error| error.to_string())?;
        let command = unavailable_command();

        let Err(error) =
            host.submit_authenticated_command(PrincipalId::new("node:caller"), &command)
        else {
            return Err("unavailable authority submitted a command".to_owned());
        };

        assert_unavailable_app_error(error)?;
        assert_no_durable_events(&host)
    }

    #[test]
    fn authorized_command_execution_preserves_unavailable_without_rejection() -> Result<(), String>
    {
        let host = unavailable_host().map_err(|error| error.to_string())?;
        let command = unavailable_command();
        let principal =
            myko_federation::Principal::new(PrincipalId::new("node:caller"), PrincipalKind::Node);

        let Err(error) = host.exec_authorized_command(
            principal.id.clone(),
            AuthorityPresentation::direct(principal),
            command,
        ) else {
            return Err("unavailable authority executed a command".to_owned());
        };

        assert_unavailable_app_error(error)?;
        assert_no_durable_events(&host)
    }

    #[tokio::test]
    async fn command_client_submission_preserves_unavailable_without_rejection()
    -> Result<(), String> {
        let host = unavailable_host().map_err(|error| error.to_string())?;
        let command = unavailable_command();
        let submission = myko_federation::CommandSubmission::for_command(&command)
            .map_err(|error| error.to_string())?;
        let command_id = submission.id;

        let Err(error) = host.submit_submission(submission).await else {
            return Err("unavailable authority submitted through command client".to_owned());
        };

        assert_unavailable_node_error(error)?;
        if host
            .node()
            .command(command_id)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err(
                "unavailable command client submission wrote a durable lifecycle".to_owned(),
            );
        }
        Ok(())
    }

    #[test]
    fn authorized_submission_preserves_unavailable_without_rejection() -> Result<(), String> {
        let host = unavailable_host().map_err(|error| error.to_string())?;
        let command = unavailable_command();
        let submission = myko_federation::CommandSubmission::for_command(&command)
            .map_err(|error| error.to_string())?;
        let command_id = submission.id;
        let principal =
            myko_federation::Principal::new(PrincipalId::new("node:caller"), PrincipalKind::Node);

        let Err(error) =
            host.submit_authorized_submission(AuthorityPresentation::direct(principal), submission)
        else {
            return Err("unavailable authority submitted through authorized facade".to_owned());
        };

        assert_unavailable_node_error(error)?;
        if host
            .node()
            .command(command_id)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("unavailable authorized submission wrote a durable lifecycle".to_owned());
        }
        Ok(())
    }
}
