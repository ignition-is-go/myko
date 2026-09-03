//! Transport-neutral request execution for Myko node connections.
//!
//! A transport authenticates a principal, decodes one canonical
//! [`NodeRequest`], and drains the returned [`NodeFrameStream`]. Unix sockets,
//! Iroh streams, `WebSockets`, and in-process tests therefore share request
//! semantics and differ only in framing and authentication.

#![forbid(unsafe_code)]
// Denials retain their structured explanation while they cross session seams.
#![allow(clippy::result_large_err)]

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use myko_app::{ApplicationNode, ErasedHandlerFrame, HandlerRequest};
use myko_federation::{
    AccessOperation, AccessPolicy, AccessRequest, AuthorityPresentation, AuthorizationDecision,
    AuthorizationExplanation, AuthorizationPhase, AuthorizationReport, CommandId, CommandResponse,
    CommandSubmission, DenyDecision, LiveEventHub, LogPosition, Node, NodeId, PermitDecision,
    Principal, PrincipalId, ReplicationBatch, ReplicationSelection, ResourceClaim,
    ResourceClaimKind, ResourceVisibility, ScopeCatalogPage, ScopeId, ScopedReplicationBatch,
    SelectedReplicationBatch, ServiceId,
};
use myko_wire::{NodeFrame, NodeRequest, NodeRequestEnvelope};
use sha2::{Digest, Sha256};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Interval, MissedTickBehavior},
};

const MAX_SCOPE_CATALOG_PAGE: usize = 1_024;
const MAX_LIVE_TOPICS: usize = 256;
const MAX_LIVE_TOPIC_BYTES: usize = 256;
const LIVE_SUBSCRIPTION_CAPACITY: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};

struct AccessMetadata {
    operation: AccessOperation,
    service_id: Option<ServiceId>,
    scope_id: Option<ScopeId>,
    command_id: Option<CommandId>,
    command_type: Option<String>,
    command_principal_id: Option<PrincipalId>,
    scope_selections: Vec<myko_federation::ScopeSelection>,
    resource_claims: Vec<ResourceClaim>,
    application_capabilities: Vec<myko_federation::CapabilityId>,
    arguments_digest: Option<String>,
    live_topics: Vec<String>,
}

struct AuthorizationPulse {
    policy_revision: watch::Receiver<u64>,
    authority_revision: Option<flume::Receiver<u64>>,
    access_policy: Arc<RwLock<Arc<dyn AccessPolicy>>>,
    deadline: Interval,
}

impl AuthorizationPulse {
    fn new(
        policy_revision: watch::Receiver<u64>,
        access_policy: Arc<RwLock<Arc<dyn AccessPolicy>>>,
    ) -> Self {
        let authority_revision = access_policy
            .read()
            .map_or(None, |policy| policy.subscribe_changes());
        let mut deadline = tokio::time::interval(Duration::from_millis(50));
        deadline.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self {
            policy_revision,
            authority_revision,
            access_policy,
            deadline,
        }
    }

    async fn changed(&mut self) {
        tokio::select! {
            _ = self.policy_revision.changed() => {
                self.authority_revision = self
                    .access_policy
                    .read()
                    .map_or(None, |policy| policy.subscribe_changes());
            }
            () = wait_for_authority_change(self.authority_revision.as_ref()) => {}
            _ = self.deadline.tick() => {}
        }
    }
}

async fn wait_for_authority_change(receiver: Option<&flume::Receiver<u64>>) {
    match receiver {
        Some(receiver) => {
            let _changed = receiver.recv_async().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// Boxed future returned by a federation request router.
pub type NodeRouteFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Routes the same canonical request envelope to another Myko node.
///
/// Transport adapters never select commands or subscriptions themselves. They
/// authenticate a principal and hand the envelope to [`NodeSessionService`];
/// this router is the one federation seam used regardless of whether that
/// envelope arrived over a Unix socket, Iroh, or WebSocket.
pub trait NodeRequestRouter: std::fmt::Debug + Send + Sync + 'static {
    /// Resolves a typed service to one directly reachable capable peer.
    ///
    /// `None` means no route is currently known. The session layer asks only
    /// when the connected node does not compile the submitted command.
    ///
    /// # Errors
    ///
    /// Returns an error when the peer capability projection is unavailable.
    fn service_destination(&self, service_id: &ServiceId) -> Result<Option<NodeId>, String>;

    /// Forwards an envelope and emits the destination node's frames unchanged.
    fn route<'a>(
        &'a self,
        envelope: NodeRequestEnvelope,
        frames: &'a flume::Sender<NodeFrame>,
    ) -> NodeRouteFuture<'a>;
}

/// Shared semantic endpoint behind every Myko transport adapter.
#[derive(Clone)]
pub struct NodeSessionService {
    node: Node,
    application: Arc<RwLock<Option<ApplicationNode>>>,
    live_events: LiveEventHub,
    access_policy: Arc<RwLock<Arc<dyn AccessPolicy>>>,
    policy_revision: watch::Sender<u64>,
    router: Arc<RwLock<Option<Weak<dyn NodeRequestRouter>>>>,
}

impl std::fmt::Debug for NodeSessionService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeSessionService")
            .field("node_id", &self.node.node_id())
            .field(
                "application",
                &self
                    .application
                    .read()
                    .is_ok_and(|application| application.is_some()),
            )
            .finish_non_exhaustive()
    }
}

impl NodeSessionService {
    /// Creates a session service for a node without application handlers.
    #[must_use]
    pub fn new(node: Node, access_policy: Arc<dyn AccessPolicy>) -> Self {
        Self::new_inner(node, None, access_policy)
    }

    /// Creates a session service for a composed Myko application.
    #[must_use]
    pub fn for_application(
        application: ApplicationNode,
        access_policy: Arc<dyn AccessPolicy>,
    ) -> Self {
        let node = application.node().clone();
        Self::new_inner(node, Some(application), access_policy)
    }

    fn new_inner(
        node: Node,
        application: Option<ApplicationNode>,
        access_policy: Arc<dyn AccessPolicy>,
    ) -> Self {
        let _installed = node.set_command_access_policy(Arc::clone(&access_policy));
        let live_events = LiveEventHub::new(node.node_id());
        let (policy_revision, _) = watch::channel(0);
        Self {
            node,
            application: Arc::new(RwLock::new(application)),
            live_events,
            access_policy: Arc::new(RwLock::new(access_policy)),
            policy_revision,
            router: Arc::new(RwLock::new(None)),
        }
    }

    /// Returns the node served by this semantic endpoint.
    #[must_use]
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// Returns the shared best-effort live-event hub.
    #[must_use]
    pub const fn live_events(&self) -> &LiveEventHub {
        &self.live_events
    }

    fn authorization_pulse(&self) -> AuthorizationPulse {
        AuthorizationPulse::new(
            self.policy_revision.subscribe(),
            Arc::clone(&self.access_policy),
        )
    }

    /// Attaches or replaces the application handlers served by this node.
    ///
    /// Every transport sharing this service observes the replacement. The
    /// application must wrap the same durable node identity.
    ///
    /// # Errors
    ///
    /// Returns an error for a different node identity or a poisoned lock.
    pub fn set_application(&self, application: ApplicationNode) -> Result<(), String> {
        if application.node().node_id() != self.node.node_id() {
            return Err("application belongs to another Myko node".to_owned());
        }
        let mut current = self
            .application
            .write()
            .map_err(|_| "application lock is poisoned".to_owned())?;
        *current = Some(application);
        drop(current);
        Ok(())
    }

    /// Detaches application handlers after every retained handler has stopped.
    ///
    /// Native node compositions call this during deterministic shutdown to
    /// release the application and its typed resource graph before the shared
    /// transport is dropped. New requests must already have been stopped.
    ///
    /// # Errors
    ///
    /// Returns an error when the shared application slot is poisoned.
    #[doc(hidden)]
    pub fn clear_application(&self) -> Result<(), String> {
        let mut current = self
            .application
            .write()
            .map_err(|_| "application lock is poisoned".to_owned())?;
        *current = None;
        drop(current);
        Ok(())
    }

    /// Replaces authorization for new and existing sessions.
    ///
    /// # Errors
    ///
    /// Returns an error when the policy lock is poisoned.
    pub fn set_access_policy(&self, policy: Arc<dyn AccessPolicy>) -> Result<(), String> {
        self.node
            .set_command_access_policy(Arc::clone(&policy))
            .map_err(|error| error.to_string())?;
        let mut current = self
            .access_policy
            .write()
            .map_err(|_| "access-policy lock is poisoned".to_owned())?;
        *current = policy;
        drop(current);
        self.policy_revision
            .send_modify(|revision| *revision = revision.saturating_add(1));
        Ok(())
    }

    /// Installs the federation router shared by every transport adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when the router lock is poisoned.
    pub fn set_router(&self, router: &Arc<dyn NodeRequestRouter>) -> Result<(), String> {
        let mut current = self
            .router
            .write()
            .map_err(|_| "node-router lock is poisoned".to_owned())?;
        *current = Some(Arc::downgrade(router));
        drop(current);
        Ok(())
    }

    /// Opens one canonical request and returns its finite or long-lived frame
    /// stream. Dropping the stream cancels all associated subscriptions.
    pub async fn open(
        &self,
        principal: PrincipalId,
        envelope: NodeRequestEnvelope,
    ) -> NodeFrameStream {
        self.open_authenticated(Principal::node(principal), envelope)
            .await
    }

    /// Opens a request bound to the complete transport-authenticated identity,
    /// including its principal kind. IDs alone are not authority credentials.
    #[allow(clippy::too_many_lines)] // Keeps authentication and first-frame ordering in one task.
    pub async fn open_authenticated(
        &self,
        authenticated: Principal,
        envelope: NodeRequestEnvelope,
    ) -> NodeFrameStream {
        self.node.wait_until_ready().await;
        let (send, receive) = flume::unbounded();
        let service = self.clone();
        let task = tokio::spawn(async move {
            let principal = authenticated.id.clone();
            let mut envelope = envelope;
            let request = envelope.request.clone();
            let mut presentation = envelope
                .authority
                .clone()
                .unwrap_or_else(|| AuthorityPresentation::direct(authenticated.clone()));
            if presentation.executor != authenticated {
                let decision = policy_denial(
                    &principal,
                    &presentation,
                    AccessOperation::ReadHistory,
                    "authority executor does not match the authenticated transport principal"
                        .to_owned(),
                );
                let _ignored = send
                    .send_async(NodeFrame::Authorization {
                        decision: Box::new(decision),
                    })
                    .await;
                return;
            }
            if !matches!(request, NodeRequest::Submit { .. })
                && !matches!(request, NodeRequest::ApproveAuthority { .. })
            {
                match service.authorize(&principal, &presentation, &request) {
                    Ok(Some(permit)) => {
                        if let Some(lease) = permit.lease.as_ref() {
                            presentation.active_lease = Some(lease.id.clone());
                        }
                        let _ignored = send
                            .send_async(NodeFrame::Authorization {
                                decision: Box::new(AuthorizationDecision::Permit(permit)),
                            })
                            .await;
                    }
                    Ok(None) => {}
                    Err(decision) => {
                        let _ignored = send
                            .send_async(NodeFrame::Authorization {
                                decision: Box::new(decision),
                            })
                            .await;
                        return;
                    }
                }
            }
            envelope.authority = Some(presentation.clone());
            if envelope.destination.is_none()
                && let NodeRequest::Submit { command } = &request
            {
                let handles_locally = service
                    .application
                    .read()
                    .map_err(|_| "application lock is poisoned".to_owned())
                    .map(|application| {
                        application
                            .as_ref()
                            .is_some_and(|application| application.handles_submission(command))
                    });
                match handles_locally {
                    Ok(true) => {}
                    Ok(false) => {
                        let route = service
                            .router
                            .read()
                            .map_err(|_| "node-router lock is poisoned".to_owned())
                            .and_then(|router| {
                                router.as_ref().and_then(Weak::upgrade).ok_or_else(|| {
                                    format!(
                                        "node {} does not execute service {} and has no federation router",
                                        service.node.node_id(), command.service_id
                                    )
                                })
                            })
                            .and_then(|router| {
                                router.service_destination(&command.service_id)?.ok_or_else(|| {
                                    format!(
                                        "no connected Myko peer advertises service {}",
                                        command.service_id
                                    )
                                })
                            });
                        match route {
                            Ok(destination) => envelope.destination = Some(destination),
                            Err(message) => {
                                let _ignored = send.send_async(NodeFrame::Error { message }).await;
                                return;
                            }
                        }
                    }
                    Err(message) => {
                        let _ignored = send.send_async(NodeFrame::Error { message }).await;
                        return;
                    }
                }
            }
            let destination = envelope.destination;
            let local = destination.is_none_or(|node_id| node_id == service.node.node_id());
            let result = if local {
                service.run(principal, presentation, request, &send).await
            } else {
                match destination {
                    Some(destination) => {
                        let router = service
                            .router
                            .read()
                            .map_err(|_| "node-router lock is poisoned".to_owned())
                            .and_then(|router| {
                                router.as_ref().and_then(Weak::upgrade).ok_or_else(|| {
                                    format!(
                                        "node {} has no federation route to {destination}",
                                        service.node.node_id()
                                    )
                                })
                            });
                        match router {
                            Ok(router) => router.route(envelope, &send).await,
                            Err(message) => Err(message),
                        }
                    }
                    None => Err("non-local request omitted its destination".to_owned()),
                }
            };
            if let Err(message) = result {
                let _ignored = send.send_async(NodeFrame::Error { message }).await;
            }
        });
        NodeFrameStream { receive, task }
    }

    async fn run(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        match request.clone() {
            NodeRequest::Identify => self.identify(send).await,
            NodeRequest::ListScopes { after, limit } => {
                self.list_scopes(&principal, &presentation, after, limit, send)
                    .await
            }
            NodeRequest::Pull { after } => self.pull(after, send).await,
            NodeRequest::PullScope { scope_id, after } => {
                self.pull_scope(scope_id, after, send).await
            }
            NodeRequest::PullSelected { selection, after } => {
                self.pull_selected(&principal, &presentation, &request, selection, after, send)
                    .await
            }
            NodeRequest::Follow { after } => {
                self.follow(principal, presentation, request, after, send)
                    .await
            }
            NodeRequest::FollowScope { scope_id, after } => {
                self.follow_scope(principal, presentation, request, scope_id, after, send)
                    .await
            }
            NodeRequest::FollowSelected { selection, after } => {
                self.follow_selected(principal, presentation, request, selection, after, send)
                    .await
            }
            NodeRequest::FollowLive { topics } => {
                self.follow_live(principal, presentation, request, topics, send)
                    .await
            }
            NodeRequest::Submit { command } => {
                self.submit(principal, presentation, command, send).await
            }
            NodeRequest::Command { command_id } => self.command(command_id, send).await,
            NodeRequest::CommandState { request } => {
                self.command_state(&principal, &presentation, request, send)
                    .await
            }
            NodeRequest::WatchCommands { request: watch } => {
                self.watch_commands(principal, presentation, request, watch, send)
                    .await
            }
            NodeRequest::WatchCommand { command_id } => {
                self.watch_command(principal, presentation, request, command_id, send)
                    .await
            }
            NodeRequest::Cancel { command_id, reason } => {
                self.cancel(command_id, reason, send).await
            }
            NodeRequest::ItemState { request } => self.item_state(request, send).await,
            NodeRequest::FollowItems { request: follow } => {
                self.follow_items(principal, presentation, request, follow, send)
                    .await
            }
            NodeRequest::FollowHandler { request: handler } => {
                self.follow_handler(principal, presentation, request, handler, send)
                    .await
            }
            NodeRequest::ApproveAuthority {
                challenge_id,
                approved,
            } => {
                self.approve_authority(&principal, &presentation, &challenge_id, approved, send)
                    .await
            }
        }
    }

    async fn identify(&self, send: &flume::Sender<NodeFrame>) -> Result<(), String> {
        emit(
            send,
            NodeFrame::Hello {
                source_node: self.node.node_id(),
            },
        )
        .await
    }

    async fn pull(
        &self,
        after: Option<LogPosition>,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        self.identify(send).await?;
        let batch = self.node.export(after).map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::Batch {
                batch: Box::new(batch),
            },
        )
        .await
    }

    async fn pull_scope(
        &self,
        scope_id: ScopeId,
        after: Option<LogPosition>,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        self.identify(send).await?;
        let batch = self
            .node
            .export_scope(scope_id, after)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::ScopedBatch {
                batch: Box::new(batch),
            },
        )
        .await
    }

    async fn pull_selected(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
        selection: ReplicationSelection,
        after: Option<LogPosition>,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let selection = match self.constrain_replication(
            principal,
            presentation,
            request,
            &selection,
            AuthorizationPhase::Admission,
        ) {
            Ok(selection) => selection,
            Err(decision) => {
                return emit(
                    send,
                    NodeFrame::Authorization {
                        decision: Box::new(decision),
                    },
                )
                .await;
            }
        };
        self.identify(send).await?;
        let batch = self
            .node
            .export_selected(selection, after)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::SelectedBatch {
                batch: Box::new(batch),
            },
        )
        .await
    }

    async fn submit(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        submission: CommandSubmission,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let application = self
            .application
            .read()
            .map_err(|_| "application lock is poisoned".to_owned())?
            .clone()
            .ok_or_else(|| "this node does not expose a Myko application".to_owned())?;
        let command = application
            .authenticate_command_submission(principal.clone(), submission)
            .map_err(|error| error.to_string())?
            .with_authority(presentation.clone());
        let prepared = match self.node.prepare_command(principal, command) {
            Ok(prepared) => prepared,
            Err(decision) => {
                return emit(
                    send,
                    NodeFrame::Authorization {
                        decision: Box::new(decision),
                    },
                )
                .await;
            }
        };
        emit(
            send,
            NodeFrame::Authorization {
                decision: Box::new(AuthorizationDecision::Permit(prepared.permit().clone())),
            },
        )
        .await?;
        let command = prepared.submit().map_err(|error| error.to_string())?;
        self.emit_command(send, Some(command)).await
    }

    async fn command(
        &self,
        command_id: CommandId,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let command = self
            .node
            .command(command_id)
            .map_err(|error| error.to_string())?;
        self.emit_command(send, command).await
    }

    async fn command_state(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: myko_federation::CommandStateRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let mut page = self
            .node
            .command_state_page(request)
            .map_err(|error| error.to_string())?;
        let unfiltered_len = page.commands.len();
        page.commands.retain(|entry| {
            self.command_snapshot_authorized(
                principal,
                presentation,
                AccessOperation::ReadCommands,
                &entry.command,
            )
        });
        if page.commands.len() != unfiltered_len {
            // A cursor containing a hidden command ID is an existence oracle.
            // Fail closed rather than claim a complete catalog through an
            // authorization gap; a future opaque cursor can preserve paging.
            page.next_after_command_id = None;
        }
        emit(
            send,
            NodeFrame::CommandState {
                page: Box::new(page),
            },
        )
        .await
    }

    async fn watch_commands(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        watch: myko_federation::CommandWatchRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        if watch.serving_node != self.node.node_id() {
            return Err("command watch cursor belongs to another serving node".to_owned());
        }
        let mut events = self
            .node
            .subscribe(watch.after)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::CommandWatchReady {
                request: Box::new(watch.clone()),
            },
        )
        .await?;
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    if let Some(update) = watch.update_from_envelope(&event)
                        && self.command_snapshot_authorized(
                            &principal,
                            &presentation,
                            AccessOperation::WatchCommands,
                            &update.command,
                        )
                    {
                        emit(send, NodeFrame::CommandUpdate { update: Box::new(update) }).await?;
                    }
                }
                () = authorization.changed() => {
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    // The broad catalog grant can remain valid while a grant
                    // covering one already-visible command is revoked. Close
                    // so the client must refetch a freshly filtered snapshot
                    // instead of retaining that command indefinitely.
                    return Ok(());
                }
            }
        }
    }

    fn command_snapshot_authorized(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        operation: AccessOperation,
        command: &myko_federation::CommandSnapshot,
    ) -> bool {
        let request = &command.request;
        let access = AccessRequest {
            principal_id: principal.clone(),
            presentation: presentation.clone(),
            operation,
            service_id: Some(request.service_id.clone()),
            scope_id: Some(request.scope_id.clone()),
            command_id: Some(request.id),
            command_type: Some(request.command_type.clone()),
            command_principal_id: Some(request.principal_id.clone()),
            scope_selections: request
                .resource_claims
                .iter()
                .map(|claim| claim.selection.clone())
                .collect(),
            resource_claims: request.resource_claims.clone(),
            application_capabilities: request.application_capabilities.clone(),
            arguments_digest: request.arguments_digest.clone(),
            effect_digest: None,
            lease: None,
            authorization_phase: AuthorizationPhase::Continuation,
            topology: self.node.scope_topology().ok(),
            live_topics: Vec::new(),
        };
        self.access_policy
            .read()
            .ok()
            .is_some_and(|policy| policy.decide(&access).is_permit())
    }

    async fn watch_command(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        command_id: CommandId,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let (initial, mut commands) = self
            .node
            .watch_command(command_id)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::Command {
                response: Box::new(initial),
            },
        )
        .await?;
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                command = commands.recv_async() => {
                    let command = command.map_err(|error| error.to_string())?;
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    self.emit_command(send, Some(command)).await?;
                }
                () = authorization.changed() => {
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn cancel(
        &self,
        command_id: CommandId,
        reason: String,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let command = self
            .node
            .cancel(command_id, reason)
            .map_err(|error| error.to_string())?;
        self.emit_command(send, Some(command)).await
    }

    async fn item_state(
        &self,
        request: myko_federation::ItemStateRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let page = self
            .node
            .item_state_page(request)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::ItemState {
                page: Box::new(page),
            },
        )
        .await
    }

    async fn follow_items(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        follow: myko_federation::ItemFollowRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        if follow.serving_node != self.node.node_id()
            || follow.item_type.is_empty()
            || follow.schema_version == 0
        {
            return Err(
                "typed item stream does not match this serving node or a valid schema".to_owned(),
            );
        }
        let mut events = self
            .node
            .subscribe(follow.after)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::ItemFollowReady {
                request: Box::new(follow.clone()),
            },
        )
        .await?;
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    if let Some(update) = follow.update_from_envelope(&event).map_err(|error| error.to_string())? {
                        emit(send, NodeFrame::ItemUpdate { update: Box::new(update) }).await?;
                    }
                }
                () = authorization.changed() => {
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn list_scopes(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        after: Option<ScopeId>,
        limit: u32,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let limit = usize::try_from(limit).map_err(|error| error.to_string())?;
        if limit == 0 || limit > MAX_SCOPE_CATALOG_PAGE {
            return Err(format!(
                "scope catalog limit must be between 1 and {MAX_SCOPE_CATALOG_PAGE}"
            ));
        }
        let policy = self
            .access_policy
            .read()
            .map_err(|_| "access-policy lock is poisoned".to_owned())?
            .clone();
        let mut scopes = Vec::with_capacity(limit.saturating_add(1));
        for scope_id in self.node.scope_ids().map_err(|error| error.to_string())? {
            if after
                .as_ref()
                .is_some_and(|cursor| scope_id.as_str() <= cursor.as_str())
            {
                continue;
            }
            let mut access = AccessRequest::scoped(
                principal.clone(),
                presentation.clone(),
                AccessOperation::ReadHistory,
                scope_id.clone(),
            );
            access.topology = Some(
                self.node
                    .scope_topology()
                    .map_err(|error| error.to_string())?,
            );
            if policy.decide(&access).is_permit() {
                scopes.push(scope_id);
                if scopes.len() > limit {
                    break;
                }
            }
        }
        let has_more = scopes.len() > limit;
        if has_more {
            let _extra = scopes.pop();
        }
        let next_after = has_more.then(|| scopes.last().cloned()).flatten();
        emit(
            send,
            NodeFrame::ScopeCatalog {
                page: Box::new(ScopeCatalogPage {
                    source_node: self.node.node_id(),
                    scopes,
                    next_after,
                }),
            },
        )
        .await
    }

    async fn follow(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        after: Option<LogPosition>,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let mut events = self
            .node
            .subscribe(after)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::Hello {
                source_node: self.node.node_id(),
            },
        )
        .await?;
        let mut cursor = after;
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    let through = event.position;
                    emit(send, NodeFrame::Batch { batch: Box::new(ReplicationBatch {
                        source_node: self.node.node_id(), after: cursor, through: Some(through), events: vec![event],
                    }) }).await?;
                    cursor = Some(through);
                }
                () = authorization.changed() => {
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn follow_scope(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        scope_id: ScopeId,
        after: Option<LogPosition>,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let mut events = self
            .node
            .subscribe(after)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::Hello {
                source_node: self.node.node_id(),
            },
        )
        .await?;
        let mut cursor = after;
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    let through = event.position;
                    let selected = if event
                        .event
                        .affected_scope_ids()
                        .iter()
                        .all(|affected| affected == &scope_id)
                    {
                        vec![event]
                    } else {
                        Vec::new()
                    };
                    emit(send, NodeFrame::ScopedBatch { batch: Box::new(ScopedReplicationBatch {
                        source_node: self.node.node_id(), scope_id: scope_id.clone(), after: cursor,
                        through: Some(through), events: selected,
                    }) }).await?;
                    cursor = Some(through);
                }
                () = authorization.changed() => {
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn follow_selected(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        requested_selection: ReplicationSelection,
        after: Option<LogPosition>,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let initial_selection = self
            .constrain_replication(
                &principal,
                &presentation,
                &request,
                &requested_selection,
                AuthorizationPhase::Continuation,
            )
            .map_err(|decision| decision.public_message())?;
        let initial = self
            .node
            .export_selected(initial_selection, after)
            .map_err(|error| error.to_string())?;
        let mut events = self
            .node
            .subscribe(initial.through)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::Hello {
                source_node: self.node.node_id(),
            },
        )
        .await?;
        emit(
            send,
            NodeFrame::SelectedBatch {
                batch: Box::new(initial.clone()),
            },
        )
        .await?;
        let mut topology = self
            .node
            .scope_topology()
            .map_err(|error| error.to_string())?;
        let mut cursor = initial.through;
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    let selection = match self.constrain_replication(
                        &principal,
                        &presentation,
                        &request,
                        &requested_selection,
                        AuthorizationPhase::Continuation,
                    ) {
                        Ok(selection) => selection,
                        Err(decision) => {
                            emit(send, NodeFrame::Authorization { decision: Box::new(decision) }).await?;
                            return Ok(());
                        }
                    };
                    let through = event.position;
                    topology
                        .observe_event(&event.event)
                        .map_err(|error| error.to_string())?;
                    let selected = if selection.includes_in(&event.event, &topology) {
                        vec![event]
                    } else {
                        Vec::new()
                    };
                    let topology_proof = topology.proof_for(match &selection {
                        ReplicationSelection::Scopes(scopes)
                        | ReplicationSelection::Intersection { scopes, .. } => scopes.as_slice(),
                        ReplicationSelection::All
                        | ReplicationSelection::Service(_)
                        | ReplicationSelection::ServiceScope { .. } => &[],
                    });
                    emit(send, NodeFrame::SelectedBatch { batch: Box::new(SelectedReplicationBatch {
                        source_node: self.node.node_id(), selection, after: cursor,
                        through: Some(through), topology: topology_proof,
                        events: selected,
                    }) }).await?;
                    cursor = Some(through);
                }
                () = authorization.changed() => {
                    if let Err(decision) = self.constrain_replication(
                        &principal,
                        &presentation,
                        &request,
                        &requested_selection,
                        AuthorizationPhase::Continuation,
                    ) {
                        emit(send, NodeFrame::Authorization { decision: Box::new(decision) }).await?;
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn follow_live(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        topics: Vec<String>,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        validate_live_topics(&topics)?;
        let mut events = self
            .live_events
            .subscribe(topics, LIVE_SUBSCRIPTION_CAPACITY)
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::Hello {
                source_node: self.node.node_id(),
            },
        )
        .await?;
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    emit(send, NodeFrame::Live { event: Box::new(event) }).await?;
                }
                () = authorization.changed() => {
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn follow_handler(
        &self,
        principal: PrincipalId,
        presentation: AuthorityPresentation,
        request: NodeRequest,
        handler: HandlerRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let application = self
            .application
            .read()
            .map_err(|_| "application lock is poisoned".to_owned())?
            .clone()
            .ok_or_else(|| "this node does not expose a Myko application".to_owned())?;
        let mut subscription = application
            .watch_handler(&handler)
            .map_err(|error| error.to_string())?;
        let (wake_tx, wake_rx) = flume::bounded(1);
        let _guard = subscription.subscribe(move || {
            let _ignored = wake_tx.try_send(());
        });
        let mut authorization = self.authorization_pulse();
        loop {
            if !self
                .stream_authorized(&principal, &presentation, &request, send)
                .await?
            {
                return Ok(());
            }
            while let Some(frame) = subscription
                .next_frame()
                .map_err(|error| error.to_string())?
            {
                let frame = match frame {
                    ErasedHandlerFrame::State(state) => NodeFrame::HandlerState {
                        state: Box::new(state),
                    },
                    ErasedHandlerFrame::ViewDelta(delta) => NodeFrame::HandlerViewDelta {
                        delta: Box::new(delta),
                    },
                };
                emit(send, frame).await?;
            }
            tokio::select! {
                wake = wake_rx.recv_async() => {
                    if wake.is_err() { return Ok(()); }
                }
                () = authorization.changed() => {
                }
            }
        }
    }

    async fn emit_command(
        &self,
        send: &flume::Sender<NodeFrame>,
        command: Option<myko_federation::CommandSnapshot>,
    ) -> Result<(), String> {
        emit(
            send,
            NodeFrame::Command {
                response: Box::new(CommandResponse {
                    source_node: self.node.node_id(),
                    command,
                }),
            },
        )
        .await
    }

    async fn approve_authority(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        challenge_id: &myko_federation::ChallengeId,
        approved: bool,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let policy = self
            .access_policy
            .read()
            .map_err(|_| "access-policy lock is poisoned".to_owned())?
            .clone();
        match policy.approve(principal, presentation, challenge_id, approved) {
            Ok(decision) => {
                emit(
                    send,
                    NodeFrame::Approval {
                        decision: Box::new(decision),
                    },
                )
                .await
            }
            Err(decision) => {
                emit(
                    send,
                    NodeFrame::Authorization {
                        decision: Box::new(decision),
                    },
                )
                .await
            }
        }
    }

    async fn stream_authorized(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<bool, String> {
        match self.authorize_continuation(principal, presentation, request) {
            Ok(_) => Ok(true),
            Err(decision) => {
                emit(
                    send,
                    NodeFrame::Authorization {
                        decision: Box::new(decision),
                    },
                )
                .await?;
                Ok(false)
            }
        }
    }

    fn authorize(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
    ) -> Result<Option<PermitDecision>, AuthorizationDecision> {
        self.authorize_with_phase(
            principal,
            presentation,
            request,
            AuthorizationPhase::Admission,
        )
    }

    fn authorize_continuation(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
    ) -> Result<Option<PermitDecision>, AuthorizationDecision> {
        self.authorize_with_phase(
            principal,
            presentation,
            request,
            AuthorizationPhase::Continuation,
        )
    }

    fn authorize_with_phase(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
        authorization_phase: AuthorizationPhase,
    ) -> Result<Option<PermitDecision>, AuthorizationDecision> {
        if matches!(
            request,
            NodeRequest::Identify | NodeRequest::ListScopes { .. }
        ) {
            return Ok(None);
        }
        let access =
            match self.access_request(principal, presentation, request, authorization_phase) {
                Ok(access) => access,
                Err(missing_command_decision)
                    if matches!(
                        request,
                        NodeRequest::Command { .. }
                            | NodeRequest::WatchCommand { .. }
                            | NodeRequest::Cancel { .. }
                    ) =>
                {
                    let operation = match request {
                        NodeRequest::Command { .. } => AccessOperation::ReadCommand,
                        NodeRequest::WatchCommand { .. } => AccessOperation::WatchCommand,
                        NodeRequest::Cancel { .. } => AccessOperation::CancelCommand,
                        _ => return Err(missing_command_decision),
                    };
                    return Err(undiscoverable_command_decision(presentation, operation));
                }
                Err(decision) => return Err(decision),
            };
        if let NodeRequest::PullSelected { selection, .. }
        | NodeRequest::FollowSelected { selection, .. } = request
        {
            return self
                .constrain_replication(
                    principal,
                    presentation,
                    request,
                    selection,
                    authorization_phase,
                )
                .map(|_| None);
        }
        let operation = access.operation;
        let decision = self
            .access_policy
            .read()
            .map_err(|_| policy_unavailable(principal, presentation, operation))?
            .decide(&access);
        match decision {
            AuthorizationDecision::Permit(permit) => Ok(Some(permit)),
            _decision
                if matches!(
                    request,
                    NodeRequest::Command { .. }
                        | NodeRequest::WatchCommand { .. }
                        | NodeRequest::Cancel { .. }
                ) =>
            {
                Err(undiscoverable_command_decision(
                    presentation,
                    access.operation,
                ))
            }
            decision => Err(decision),
        }
    }

    fn access_request(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
        authorization_phase: AuthorizationPhase,
    ) -> Result<AccessRequest, AuthorizationDecision> {
        let metadata = self.access_metadata(request).map_err(|message| {
            policy_denial(
                principal,
                presentation,
                AccessOperation::ReadHistory,
                message,
            )
        })?;
        let mut access = AccessRequest {
            principal_id: principal.clone(),
            presentation: presentation.clone(),
            operation: metadata.operation,
            service_id: metadata.service_id,
            scope_id: metadata.scope_id,
            command_id: metadata.command_id,
            command_type: metadata.command_type,
            command_principal_id: metadata.command_principal_id,
            resource_claims: metadata.resource_claims,
            scope_selections: metadata.scope_selections,
            application_capabilities: metadata.application_capabilities,
            arguments_digest: metadata.arguments_digest,
            effect_digest: None,
            lease: presentation.requested_lease,
            authorization_phase,
            topology: self.node.scope_topology().ok(),
            live_topics: metadata.live_topics,
        };
        if let NodeRequest::FollowHandler { request: handler } = request {
            let application = self
                .application
                .read()
                .map_err(|_| {
                    policy_denial(
                        principal,
                        presentation,
                        AccessOperation::FollowHandler,
                        "application registry is unavailable".to_owned(),
                    )
                })?
                .clone()
                .ok_or_else(|| {
                    policy_denial(
                        principal,
                        presentation,
                        AccessOperation::FollowHandler,
                        "this node does not expose a Myko application".to_owned(),
                    )
                })?;
            let dependencies = application
                .handler_authority_claims(handler)
                .map_err(|error| {
                    policy_denial(
                        principal,
                        presentation,
                        AccessOperation::FollowHandler,
                        error.to_string(),
                    )
                })?;
            access
                .scope_selections
                .extend(dependencies.iter().map(|claim| claim.selection.clone()));
            access.resource_claims.extend(dependencies);
        }
        if access.resource_claims.is_empty()
            && let Some(scope_id) = access.scope_id.clone()
        {
            access.resource_claims =
                vec![ResourceClaim::scope(scope_id, ResourceClaimKind::Primary)];
        }
        Ok(access)
    }

    fn constrain_replication(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
        selection: &ReplicationSelection,
        authorization_phase: AuthorizationPhase,
    ) -> Result<ReplicationSelection, AuthorizationDecision> {
        let access = self.access_request(principal, presentation, request, authorization_phase)?;
        let topology = access.topology.clone().ok_or_else(|| {
            policy_denial(
                principal,
                presentation,
                access.operation,
                "scope topology is incomplete".to_owned(),
            )
        })?;
        self.access_policy
            .read()
            .map_err(|_| policy_unavailable(principal, presentation, access.operation))?
            .constrain_replication(&access, selection, &topology)
    }

    fn access_metadata(&self, request: &NodeRequest) -> Result<AccessMetadata, String> {
        Ok(match request {
            NodeRequest::Identify | NodeRequest::ListScopes { .. } => {
                return Err("request is authorized outside access metadata".to_owned());
            }
            NodeRequest::Pull { .. } => metadata(AccessOperation::ReadHistory),
            NodeRequest::PullScope { scope_id, .. } => {
                scoped(AccessOperation::ReadHistory, scope_id)
            }
            NodeRequest::PullSelected { selection, .. } => {
                selected(AccessOperation::ReadHistory, selection)
            }
            NodeRequest::Follow { .. } => metadata(AccessOperation::FollowHistory),
            NodeRequest::FollowScope { scope_id, .. } => {
                scoped(AccessOperation::FollowHistory, scope_id)
            }
            NodeRequest::FollowSelected { selection, .. } => {
                selected(AccessOperation::FollowHistory, selection)
            }
            NodeRequest::FollowLive { topics } => live_access(topics),
            NodeRequest::Submit { .. } => {
                return Err("command submission is authorized after typed routing".to_owned());
            }
            NodeRequest::Command { command_id } => {
                self.command_access(AccessOperation::ReadCommand, *command_id)?
            }
            NodeRequest::CommandState { request } => catalog(
                AccessOperation::ReadCommands,
                &request.service_id,
                &request.scope_id,
                &request.command_type,
            ),
            NodeRequest::WatchCommands { request } => catalog(
                AccessOperation::WatchCommands,
                &request.service_id,
                &request.scope_id,
                &request.command_type,
            ),
            NodeRequest::WatchCommand { command_id } => {
                self.command_access(AccessOperation::WatchCommand, *command_id)?
            }
            NodeRequest::Cancel { command_id, .. } => {
                self.command_access(AccessOperation::CancelCommand, *command_id)?
            }
            NodeRequest::ItemState { request } => item_access(
                AccessOperation::ReadItems,
                request.source_node,
                &request.service_id,
                &request.scope_id,
                &request.item_type,
            ),
            NodeRequest::FollowItems { request } => item_access(
                AccessOperation::FollowItems,
                Some(request.source_node),
                &request.service_id,
                &request.scope_id,
                &request.item_type,
            ),
            NodeRequest::FollowHandler { request } => handler_access(request),
            NodeRequest::ApproveAuthority { .. } => {
                return Err("authority approval is handled outside access metadata".to_owned());
            }
        })
    }

    fn command_access(
        &self,
        operation: AccessOperation,
        command_id: CommandId,
    ) -> Result<AccessMetadata, String> {
        Ok(self
            .node
            .command(command_id)
            .map_err(|error| error.to_string())?
            .map_or_else(
                || {
                    let mut metadata = metadata(operation);
                    metadata.command_id = Some(command_id);
                    metadata
                },
                |command| {
                    let request = command.request;
                    AccessMetadata {
                        operation,
                        service_id: Some(request.service_id),
                        scope_id: Some(request.scope_id),
                        command_id: Some(command_id),
                        command_type: Some(request.command_type),
                        command_principal_id: Some(request.principal_id),
                        scope_selections: request
                            .resource_claims
                            .iter()
                            .map(|claim| claim.selection.clone())
                            .collect(),
                        resource_claims: request.resource_claims,
                        application_capabilities: request.application_capabilities,
                        arguments_digest: request.arguments_digest,
                        live_topics: Vec::new(),
                    }
                },
            ))
    }
}

/// Frames produced for one canonical request.
pub struct NodeFrameStream {
    receive: flume::Receiver<NodeFrame>,
    task: JoinHandle<()>,
}

impl NodeFrameStream {
    /// Receives the next frame, or `None` after the finite request completes.
    pub async fn recv(&mut self) -> Option<NodeFrame> {
        self.receive.recv_async().await.ok()
    }
}

impl Drop for NodeFrameStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn policy_unavailable(
    principal: &PrincipalId,
    presentation: &AuthorityPresentation,
    operation: AccessOperation,
) -> AuthorizationDecision {
    policy_denial(
        principal,
        presentation,
        operation,
        "authority policy is unavailable".to_owned(),
    )
}

fn undiscoverable_command_decision(
    presentation: &AuthorityPresentation,
    operation: AccessOperation,
) -> AuthorizationDecision {
    AuthorizationDecision::Deny(DenyDecision {
        report: AuthorizationReport {
            evaluated_at: chrono::Utc::now(),
            principal: presentation.principal.clone(),
            executor: presentation.executor.clone(),
            operation,
            explanations: vec![AuthorizationExplanation {
                code: "undiscoverable".to_owned(),
                message: "command is unavailable".to_owned(),
                grant_id: None,
                delegation_id: None,
                obligation_id: None,
                constraint: None,
            }],
        },
        visibility: ResourceVisibility::Undiscoverable,
    })
}

fn policy_denial(
    _principal: &PrincipalId,
    presentation: &AuthorityPresentation,
    operation: AccessOperation,
    message: String,
) -> AuthorizationDecision {
    AuthorizationDecision::Deny(DenyDecision {
        report: AuthorizationReport {
            evaluated_at: chrono::Utc::now(),
            principal: presentation.principal.clone(),
            executor: presentation.executor.clone(),
            operation,
            explanations: vec![AuthorizationExplanation {
                code: "authority_unavailable".to_owned(),
                message,
                grant_id: None,
                delegation_id: None,
                obligation_id: None,
                constraint: None,
            }],
        },
        visibility: ResourceVisibility::Unauthorized,
    })
}

async fn emit(send: &flume::Sender<NodeFrame>, frame: NodeFrame) -> Result<(), String> {
    send.send_async(frame)
        .await
        .map_err(|_| "client disconnected".to_owned())
}

const fn metadata(operation: AccessOperation) -> AccessMetadata {
    AccessMetadata {
        operation,
        service_id: None,
        scope_id: None,
        command_id: None,
        command_type: None,
        command_principal_id: None,
        scope_selections: Vec::new(),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        live_topics: Vec::new(),
    }
}

fn scoped(operation: AccessOperation, scope_id: &ScopeId) -> AccessMetadata {
    let mut metadata = metadata(operation);
    metadata.scope_id = Some(scope_id.clone());
    metadata
}

fn selected(operation: AccessOperation, selection: &ReplicationSelection) -> AccessMetadata {
    match selection {
        ReplicationSelection::All => metadata(operation),
        ReplicationSelection::Service(service_id) => {
            let mut metadata = metadata(operation);
            metadata.service_id = Some(service_id.clone());
            metadata
        }
        ReplicationSelection::ServiceScope {
            service_id,
            scope_id,
        } => {
            let mut metadata = scoped(operation, scope_id);
            metadata.service_id = Some(service_id.clone());
            metadata
        }
        ReplicationSelection::Scopes(selections) => {
            let mut metadata = metadata(operation);
            metadata.scope_selections.clone_from(selections);
            metadata.resource_claims = selections
                .iter()
                .cloned()
                .map(|selection| ResourceClaim {
                    selection,
                    kind: ResourceClaimKind::Primary,
                    source_node: None,
                    service_id: None,
                    item_type: None,
                    item_id: None,
                    required_permissions: Vec::new(),
                    required_operations: Vec::new(),
                    required_capabilities: Vec::new(),
                })
                .collect();
            metadata
        }
        ReplicationSelection::Intersection { requested, .. } => selected(operation, requested),
    }
}

fn live_access(topics: &[String]) -> AccessMetadata {
    let mut metadata = metadata(AccessOperation::SubscribeLive);
    metadata.live_topics = topics.to_vec();
    metadata.resource_claims = topics
        .iter()
        .map(|topic| {
            ResourceClaim::scope(
                ScopeId::new(format!("myko.live/{topic}")),
                ResourceClaimKind::Primary,
            )
        })
        .collect();
    metadata.scope_selections = metadata
        .resource_claims
        .iter()
        .map(|claim| claim.selection.clone())
        .collect();
    metadata
}

fn item_access(
    operation: AccessOperation,
    source_node: Option<NodeId>,
    service_id: &ServiceId,
    scope_id: &ScopeId,
    item_type: &str,
) -> AccessMetadata {
    let mut metadata = scoped(operation, scope_id);
    metadata.service_id = Some(service_id.clone());
    metadata.resource_claims = vec![ResourceClaim {
        selection: myko_federation::ScopeSelection::Exact(scope_id.clone()),
        kind: ResourceClaimKind::Primary,
        source_node,
        service_id: Some(service_id.clone()),
        item_type: Some(item_type.to_owned()),
        item_id: None,
        required_permissions: Vec::new(),
        required_operations: Vec::new(),
        required_capabilities: Vec::new(),
    }];
    metadata
}

fn handler_access(request: &HandlerRequest) -> AccessMetadata {
    let topic = format!("handler:{}:{}", request.kind.as_str(), request.handler_id);
    let scope = request
        .scope_id
        .clone()
        .unwrap_or_else(|| ScopeId::new(format!("myko.{topic}")));
    let mut metadata = scoped(AccessOperation::FollowHandler, &scope);
    metadata.live_topics = vec![topic];
    metadata.resource_claims = vec![ResourceClaim::scope(scope, ResourceClaimKind::Primary)];
    let encoded = serde_json::to_vec(&(request.source_node, &request.params)).unwrap_or_default();
    metadata.arguments_digest = Some(format!("{:x}", Sha256::digest(encoded)));
    metadata
}

fn catalog(
    operation: AccessOperation,
    service_id: &ServiceId,
    scope_id: &ScopeId,
    command_type: &str,
) -> AccessMetadata {
    let mut metadata = scoped(operation, scope_id);
    metadata.service_id = Some(service_id.clone());
    metadata.command_type = Some(command_type.to_owned());
    metadata
}

fn validate_live_topics(topics: &[String]) -> Result<(), String> {
    if topics.len() > MAX_LIVE_TOPICS {
        return Err(format!(
            "live subscription exceeds {MAX_LIVE_TOPICS} topics"
        ));
    }
    if let Some(topic) = topics
        .iter()
        .find(|topic| topic.is_empty() || topic.len() > MAX_LIVE_TOPIC_BYTES)
    {
        return Err(format!(
            "live topic must contain 1..={MAX_LIVE_TOPIC_BYTES} bytes, got {}",
            topic.len()
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use myko_federation::{
        AllowAllAccessPolicy, AuthorityPresentation, CommandRequest, DenyAllAccessPolicy,
        PrincipalKind, ScopeSelection,
    };

    use super::*;

    #[derive(Debug)]
    struct CapturingPolicy {
        allow: AtomicBool,
        expected_item_type: Option<&'static str>,
        seen: Mutex<Vec<AccessRequest>>,
    }

    impl CapturingPolicy {
        fn allow() -> Self {
            Self {
                allow: AtomicBool::new(true),
                expected_item_type: None,
                seen: Mutex::new(Vec::new()),
            }
        }

        fn item_type(expected: &'static str) -> Self {
            Self {
                allow: AtomicBool::new(true),
                expected_item_type: Some(expected),
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl AccessPolicy for CapturingPolicy {
        fn authorize(&self, request: &AccessRequest) -> Result<(), String> {
            self.seen.lock().unwrap().push(request.clone());
            if !self.allow.load(Ordering::Acquire) {
                return Err("revoked".to_owned());
            }
            if let Some(expected) = self.expected_item_type
                && !request
                    .resource_claims
                    .iter()
                    .all(|claim| claim.item_type.as_deref() == Some(expected))
            {
                return Err("item type constraint rejected".to_owned());
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct CatalogEntryPolicy {
        entries_allowed: bool,
    }

    impl AccessPolicy for CatalogEntryPolicy {
        fn authorize(&self, request: &AccessRequest) -> Result<(), String> {
            if request.operation == AccessOperation::WatchCommands
                && request.command_id.is_some()
                && !self.entries_allowed
            {
                return Err("stored command claims were revoked".to_owned());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn full_principal_kind_is_transport_bound() {
        let authenticated = Principal::new(PrincipalId::new("same-id"), PrincipalKind::Node);
        let asserted = Principal::new(PrincipalId::new("same-id"), PrincipalKind::Person);
        let session = NodeSessionService::new(Node::in_memory(), Arc::new(AllowAllAccessPolicy));
        let envelope = NodeRequestEnvelope::connected(NodeRequest::PullScope {
            scope_id: ScopeId::new("scope:a"),
            after: None,
        })
        .with_authority(AuthorityPresentation::direct(asserted));
        let mut frames = session.open_authenticated(authenticated, envelope).await;
        assert!(matches!(
            frames.recv().await,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Deny(_))
        ));
        assert!(frames.recv().await.is_none());
    }

    #[tokio::test]
    async fn live_topics_are_grantable_and_permit_precedes_prompt_revocation() {
        let policy = Arc::new(CapturingPolicy::allow());
        let session = NodeSessionService::new(Node::in_memory(), policy.clone());
        let principal = PrincipalId::new("node:subscriber");
        let mut frames = session
            .open(
                principal,
                NodeRequestEnvelope::connected(NodeRequest::FollowLive {
                    topics: vec!["project.changed".to_owned()],
                }),
            )
            .await;
        assert!(matches!(
            frames.recv().await,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Permit(_))
        ));
        assert!(matches!(frames.recv().await, Some(NodeFrame::Hello { .. })));
        let seen = policy.seen.lock().unwrap().clone();
        assert!(seen.iter().any(|request| {
            request.operation == AccessOperation::SubscribeLive
                && request.resource_claims.iter().any(|claim| {
                    claim.selection
                        == ScopeSelection::Exact(ScopeId::new("myko.live/project.changed"))
                })
        }));

        session
            .set_access_policy(Arc::new(DenyAllAccessPolicy))
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), frames.recv()).await,
            Ok(Some(NodeFrame::Authorization { decision }))
                if matches!(*decision, AuthorizationDecision::Deny(_))
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), frames.recv()).await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn catalog_watch_closes_when_visible_entry_authority_changes() {
        let node = Node::in_memory();
        let session = NodeSessionService::new(
            node.clone(),
            Arc::new(CatalogEntryPolicy {
                entries_allowed: true,
            }),
        );
        let principal = PrincipalId::new("node:catalog-reader");
        let service_id = ServiceId::new("test.service");
        let scope_id = ScopeId::new("scope:catalog");
        let mut frames = session
            .open(
                principal.clone(),
                NodeRequestEnvelope::connected(NodeRequest::WatchCommands {
                    request: myko_federation::CommandWatchRequest {
                        serving_node: node.node_id(),
                        source_node: node.node_id(),
                        service_id: service_id.clone(),
                        scope_id: scope_id.clone(),
                        command_type: "test.command".to_owned(),
                        after: None,
                    },
                }),
            )
            .await;
        assert!(matches!(
            frames.recv().await,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Permit(_))
        ));
        assert!(matches!(
            frames.recv().await,
            Some(NodeFrame::CommandWatchReady { .. })
        ));

        let command_id = CommandId::new();
        node.admit(CommandRequest {
            id: command_id,
            service_id,
            scope_id: scope_id.clone(),
            principal_id: principal.clone(),
            authority: AuthorityPresentation::direct_node(principal.clone()),
            resource_claims: vec![ResourceClaim::scope(scope_id, ResourceClaimKind::Primary)],
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: "test.command".to_owned(),
            payload: Vec::new(),
        })
        .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), frames.recv()).await,
            Ok(Some(NodeFrame::CommandUpdate { update })) if update.command.request.id == command_id
        ));

        session
            .set_access_policy(Arc::new(CatalogEntryPolicy {
                entries_allowed: false,
            }))
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), frames.recv()).await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn item_type_constraints_and_command_existence_are_fail_closed() {
        let node = Node::in_memory();
        let policy = Arc::new(CapturingPolicy::item_type("VisibleItem"));
        let session = NodeSessionService::new(node.clone(), policy);
        let principal = PrincipalId::new("node:reader");
        let request = |item_type: &str| {
            NodeRequestEnvelope::connected(NodeRequest::ItemState {
                request: myko_federation::ItemStateRequest {
                    source_node: None,
                    service_id: ServiceId::new("test.service"),
                    scope_id: ScopeId::new("scope:item"),
                    item_type: item_type.to_owned(),
                    schema_version: 1,
                    snapshot_through: None,
                    after_item_id: None,
                    page_size: 1,
                },
            })
        };
        let mut permitted = session
            .open(principal.clone(), request("VisibleItem"))
            .await;
        assert!(matches!(
            permitted.recv().await,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Permit(_))
        ));
        let mut denied = session.open(principal.clone(), request("HiddenItem")).await;
        assert!(matches!(
            denied.recv().await,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Deny(_))
        ));

        let command_id = CommandId::new();
        node.admit(CommandRequest {
            id: command_id,
            service_id: ServiceId::new("test.service"),
            scope_id: ScopeId::new("scope:command"),
            principal_id: principal.clone(),
            authority: AuthorityPresentation::direct_node(principal.clone()),
            resource_claims: vec![ResourceClaim::scope(
                ScopeId::new("scope:command"),
                ResourceClaimKind::Primary,
            )],
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: "test.command".to_owned(),
            payload: Vec::new(),
        })
        .unwrap();
        let hidden = NodeSessionService::new(node, Arc::new(DenyAllAccessPolicy));
        let mut existing = hidden
            .open(
                principal.clone(),
                NodeRequestEnvelope::connected(NodeRequest::Command { command_id }),
            )
            .await;
        let mut unknown = hidden
            .open(
                principal,
                NodeRequestEnvelope::connected(NodeRequest::Command {
                    command_id: CommandId::new(),
                }),
            )
            .await;
        let existing = existing.recv().await.unwrap();
        let unknown = unknown.recv().await.unwrap();
        let message = |frame: NodeFrame| match frame {
            NodeFrame::Authorization { decision } => decision.public_message(),
            frame => panic!("unexpected frame: {frame:?}"),
        };
        assert_eq!(message(existing), message(unknown));
    }
}
