//! Transport-neutral request execution for Myko node connections.
//!
//! A transport authenticates a principal, decodes one canonical
//! [`NodeRequest`], and drains the returned [`NodeFrameStream`]. Unix sockets,
//! Iroh streams, `WebSockets`, and in-process tests therefore share request
//! semantics and differ only in framing and authentication.

#![forbid(unsafe_code)]

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, RwLock, Weak},
};

use myko_app::{ApplicationNode, ErasedHandlerFrame, HandlerRequest};
use myko_federation::{
    AccessOperation, AccessPolicy, AccessRequest, CommandId, CommandResponse, CommandSubmission,
    LiveEventHub, LogPosition, Node, NodeId, PrincipalId, ReplicationBatch, ScopeCatalogPage,
    ScopeId, ScopedReplicationBatch, ServiceId,
};
use myko_wire::{NodeFrame, NodeRequest, NodeRequestEnvelope};
use tokio::{sync::watch, task::JoinHandle};

const MAX_SCOPE_CATALOG_PAGE: usize = 1_024;
const MAX_LIVE_TOPICS: usize = 256;
const MAX_LIVE_TOPIC_BYTES: usize = 256;
const LIVE_SUBSCRIPTION_CAPACITY: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};

type AccessMetadata = (
    AccessOperation,
    Option<ServiceId>,
    Option<ScopeId>,
    Option<CommandId>,
    Option<String>,
    Option<PrincipalId>,
    Vec<String>,
);

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
        self.node.wait_until_ready().await;
        let (send, receive) = flume::unbounded();
        let service = self.clone();
        let task = tokio::spawn(async move {
            let mut envelope = envelope;
            let request = envelope.request.clone();
            if !matches!(request, NodeRequest::Submit { .. })
                && let Err(message) = service.authorize(&principal, &request)
            {
                let _ignored = send.send_async(NodeFrame::Error { message }).await;
                return;
            }
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
                service.run(principal, request, &send).await
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
        request: NodeRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        match request.clone() {
            NodeRequest::Identify => self.identify(send).await,
            NodeRequest::ListScopes { after, limit } => {
                self.list_scopes(&principal, after, limit, send).await
            }
            NodeRequest::Pull { after } => self.pull(after, send).await,
            NodeRequest::PullScope { scope_id, after } => {
                self.pull_scope(scope_id, after, send).await
            }
            NodeRequest::Follow { after } => self.follow(principal, request, after, send).await,
            NodeRequest::FollowScope { scope_id, after } => {
                self.follow_scope(principal, request, scope_id, after, send)
                    .await
            }
            NodeRequest::FollowLive { topics } => {
                self.follow_live(principal, request, topics, send).await
            }
            NodeRequest::Submit { command } => self.submit(principal, command, send).await,
            NodeRequest::Command { command_id } => self.command(command_id, send).await,
            NodeRequest::CommandState { request } => self.command_state(request, send).await,
            NodeRequest::WatchCommands { request: watch } => {
                self.watch_commands(principal, request, watch, send).await
            }
            NodeRequest::WatchCommand { command_id } => {
                self.watch_command(principal, request, command_id, send)
                    .await
            }
            NodeRequest::Cancel { command_id, reason } => {
                self.cancel(command_id, reason, send).await
            }
            NodeRequest::ItemState { request } => self.item_state(request, send).await,
            NodeRequest::FollowItems { request: follow } => {
                self.follow_items(principal, request, follow, send).await
            }
            NodeRequest::FollowHandler { request: handler } => {
                self.follow_handler(principal, request, handler, send).await
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

    async fn submit(
        &self,
        principal: PrincipalId,
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
            .map_err(|error| error.to_string())?;
        self.authorize_submitted_command(&principal, &command)?;
        let command = self
            .node
            .submit(command)
            .map_err(|error| error.to_string())?;
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
        request: myko_federation::CommandStateRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let page = self
            .node
            .command_state_page(request)
            .map_err(|error| error.to_string())?;
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
        let mut policy_changes = self.policy_revision.subscribe();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    self.authorize(&principal, &request)?;
                    if let Some(update) = watch.update_from_envelope(&event) {
                        emit(send, NodeFrame::CommandUpdate { update: Box::new(update) }).await?;
                    }
                }
                changed = policy_changes.changed() => {
                    if changed.is_err() { return Ok(()); }
                    self.authorize(&principal, &request)?;
                }
            }
        }
    }

    async fn watch_command(
        &self,
        principal: PrincipalId,
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
        let mut policy_changes = self.policy_revision.subscribe();
        loop {
            tokio::select! {
                command = commands.recv_async() => {
                    let command = command.map_err(|error| error.to_string())?;
                    self.authorize(&principal, &request)?;
                    self.emit_command(send, Some(command)).await?;
                }
                changed = policy_changes.changed() => {
                    if changed.is_err() { return Ok(()); }
                    self.authorize(&principal, &request)?;
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
        let mut policy_changes = self.policy_revision.subscribe();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    self.authorize(&principal, &request)?;
                    if let Some(update) = follow.update_from_envelope(&event).map_err(|error| error.to_string())? {
                        emit(send, NodeFrame::ItemUpdate { update: Box::new(update) }).await?;
                    }
                }
                changed = policy_changes.changed() => {
                    if changed.is_err() { return Ok(()); }
                    self.authorize(&principal, &request)?;
                }
            }
        }
    }

    async fn list_scopes(
        &self,
        principal: &PrincipalId,
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
            let access = AccessRequest {
                principal_id: principal.clone(),
                operation: AccessOperation::ReadHistory,
                service_id: None,
                scope_id: Some(scope_id.clone()),
                command_id: None,
                command_type: None,
                command_principal_id: None,
                live_topics: Vec::new(),
            };
            if policy.authorize(&access).is_ok() {
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
        let mut policy_changes = self.policy_revision.subscribe();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    self.authorize(&principal, &request)?;
                    let through = event.position;
                    emit(send, NodeFrame::Batch { batch: Box::new(ReplicationBatch {
                        source_node: self.node.node_id(), after: cursor, through: Some(through), events: vec![event],
                    }) }).await?;
                    cursor = Some(through);
                }
                changed = policy_changes.changed() => {
                    if changed.is_err() { return Ok(()); }
                    self.authorize(&principal, &request)?;
                }
            }
        }
    }

    async fn follow_scope(
        &self,
        principal: PrincipalId,
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
        let mut policy_changes = self.policy_revision.subscribe();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    self.authorize(&principal, &request)?;
                    let through = event.position;
                    let selected = if event.event.scope_id() == &scope_id { vec![event] } else { Vec::new() };
                    emit(send, NodeFrame::ScopedBatch { batch: Box::new(ScopedReplicationBatch {
                        source_node: self.node.node_id(), scope_id: scope_id.clone(), after: cursor,
                        through: Some(through), events: selected,
                    }) }).await?;
                    cursor = Some(through);
                }
                changed = policy_changes.changed() => {
                    if changed.is_err() { return Ok(()); }
                    self.authorize(&principal, &request)?;
                }
            }
        }
    }

    async fn follow_live(
        &self,
        principal: PrincipalId,
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
        let mut policy_changes = self.policy_revision.subscribe();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let event = event.map_err(|error| error.to_string())?;
                    self.authorize(&principal, &request)?;
                    emit(send, NodeFrame::Live { event: Box::new(event) }).await?;
                }
                changed = policy_changes.changed() => {
                    if changed.is_err() { return Ok(()); }
                    self.authorize(&principal, &request)?;
                }
            }
        }
    }

    async fn follow_handler(
        &self,
        principal: PrincipalId,
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
        let mut policy_changes = self.policy_revision.subscribe();
        loop {
            self.authorize(&principal, &request)?;
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
                changed = policy_changes.changed() => {
                    if changed.is_err() { return Ok(()); }
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

    fn authorize(&self, principal: &PrincipalId, request: &NodeRequest) -> Result<(), String> {
        if matches!(
            request,
            NodeRequest::Identify | NodeRequest::ListScopes { .. }
        ) {
            return Ok(());
        }
        let (
            operation,
            service_id,
            scope_id,
            command_id,
            command_type,
            command_principal_id,
            live_topics,
        ) = self.access_metadata(request)?;
        self.access_policy
            .read()
            .map_err(|_| "access-policy lock is poisoned".to_owned())?
            .authorize(&AccessRequest {
                principal_id: principal.clone(),
                operation,
                service_id,
                scope_id,
                command_id,
                command_type,
                command_principal_id,
                live_topics,
            })
            .map_err(|message| format!("access denied: {message}"))
    }

    fn authorize_submitted_command(
        &self,
        principal: &PrincipalId,
        command: &myko_federation::CommandRequest,
    ) -> Result<(), String> {
        let (
            operation,
            service_id,
            scope_id,
            command_id,
            command_type,
            command_principal_id,
            live_topics,
        ) = submitted_command_access(command);
        self.access_policy
            .read()
            .map_err(|_| "access-policy lock is poisoned".to_owned())?
            .authorize(&AccessRequest {
                principal_id: principal.clone(),
                operation,
                service_id,
                scope_id,
                command_id,
                command_type,
                command_principal_id,
                live_topics,
            })
            .map_err(|message| format!("access denied: {message}"))
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
            NodeRequest::Follow { .. } => metadata(AccessOperation::FollowHistory),
            NodeRequest::FollowScope { scope_id, .. } => {
                scoped(AccessOperation::FollowHistory, scope_id)
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
                &request.service_id,
                &request.scope_id,
            ),
            NodeRequest::FollowItems { request } => item_access(
                AccessOperation::FollowItems,
                &request.service_id,
                &request.scope_id,
            ),
            NodeRequest::FollowHandler { request } => handler_access(request),
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
                    (
                        operation,
                        None,
                        None,
                        Some(command_id),
                        None,
                        None,
                        Vec::new(),
                    )
                },
                |command| {
                    (
                        operation,
                        Some(command.request.service_id),
                        Some(command.request.scope_id),
                        Some(command_id),
                        Some(command.request.command_type),
                        Some(command.request.principal_id),
                        Vec::new(),
                    )
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

async fn emit(send: &flume::Sender<NodeFrame>, frame: NodeFrame) -> Result<(), String> {
    send.send_async(frame)
        .await
        .map_err(|_| "client disconnected".to_owned())
}

const fn metadata(operation: AccessOperation) -> AccessMetadata {
    (operation, None, None, None, None, None, Vec::new())
}

fn scoped(operation: AccessOperation, scope_id: &ScopeId) -> AccessMetadata {
    (
        operation,
        None,
        Some(scope_id.clone()),
        None,
        None,
        None,
        Vec::new(),
    )
}

fn live_access(topics: &[String]) -> AccessMetadata {
    (
        AccessOperation::SubscribeLive,
        None,
        None,
        None,
        None,
        None,
        topics.to_vec(),
    )
}

fn submitted_command_access(command: &myko_federation::CommandRequest) -> AccessMetadata {
    (
        AccessOperation::SubmitCommand,
        Some(command.service_id.clone()),
        Some(command.scope_id.clone()),
        Some(command.id),
        Some(command.command_type.clone()),
        Some(command.principal_id.clone()),
        Vec::new(),
    )
}

fn item_access(
    operation: AccessOperation,
    service_id: &ServiceId,
    scope_id: &ScopeId,
) -> AccessMetadata {
    (
        operation,
        Some(service_id.clone()),
        Some(scope_id.clone()),
        None,
        None,
        None,
        Vec::new(),
    )
}

fn handler_access(request: &HandlerRequest) -> AccessMetadata {
    (
        AccessOperation::FollowHandler,
        None,
        request.scope_id.clone(),
        None,
        None,
        None,
        vec![format!(
            "handler:{}:{}",
            request.kind.as_str(),
            request.handler_id
        )],
    )
}

fn catalog(
    operation: AccessOperation,
    service_id: &ServiceId,
    scope_id: &ScopeId,
    command_type: &str,
) -> AccessMetadata {
    (
        operation,
        Some(service_id.clone()),
        Some(scope_id.clone()),
        None,
        Some(command_type.to_owned()),
        None,
        Vec::new(),
    )
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
