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
    collections::BTreeMap,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use hyphae::{Cell, CellImmutable, SubscriptionGuard, Watchable as _};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy, AccessTarget, AuthorityPresentation,
    AuthorityUnavailable, AuthorizationDecision, AuthorizationExplanation, AuthorizationFailure,
    AuthorizationPhase, AuthorizationReport, CommandId, CommandResponse, CommandSubmission,
    DenyAllAccessPolicy, DenyDecision, FederationPermission, LiveEventHub, LogPosition, Node,
    NodeId, PermitDecision, Principal, PrincipalId, ReplicationBatch, ReplicationSelection,
    ResourceClaim, ResourceClaimKind, ResourceVisibility, ScopeCatalogPage, ScopeId,
    ScopedReplicationBatch, ServiceId,
    control_quorum::{
        ControlBallot, ControlHead, ControlValue, SignedControlProposal, SignedControlVote,
    },
};
use myko_wire::{HandlerRequest, NodeFrame, NodeRequest, NodeRequestEnvelope};
use sha2::{Digest, Sha256};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, Interval, MissedTickBehavior},
};
use tracing::Instrument as _;

const MAX_SCOPE_CATALOG_PAGE: usize = 1_024;
const MAX_LIVE_TOPICS: usize = 256;
const MAX_LIVE_TOPIC_BYTES: usize = 256;
const SESSION_FRAME_CAPACITY: usize = 256;
const SCOPE_CATALOG_SCAN_PAGE: NonZeroUsize = match NonZeroUsize::new(MAX_SCOPE_CATALOG_PAGE) {
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};
const LIVE_SUBSCRIPTION_CAPACITY: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};

struct AccessPreparation {
    operation: AccessOperation,
    target: AccessTarget,
    resource_claims: Vec<ResourceClaim>,
    application_capabilities: Vec<myko_federation::CapabilityId>,
    arguments_digest: Option<String>,
}

struct AuthorizationPulse {
    policy_revision: watch::Receiver<u64>,
    authority_revision: Option<AuthorityRevisionWake>,
    access_policy: Arc<RwLock<Arc<dyn AccessPolicy>>>,
    deadline: Option<Interval>,
}

struct AuthorityRevisionWake {
    _revision: Cell<u64, CellImmutable>,
    changes: flume::Receiver<()>,
    _guard: SubscriptionGuard,
}

impl AuthorityRevisionWake {
    fn new(policy: &dyn AccessPolicy) -> Option<Self> {
        let revision = policy.revision_cell()?;
        let (send, changes) = flume::bounded(1);
        let guard = revision.subscribe(move |_| {
            let _ = send.try_send(());
        });
        Some(Self {
            _revision: revision,
            changes,
            _guard: guard,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthorizationWake {
    Revision,
    Deadline,
}

impl AuthorizationPulse {
    fn new(
        policy_revision: watch::Receiver<u64>,
        access_policy: Arc<RwLock<Arc<dyn AccessPolicy>>>,
    ) -> Self {
        let authority_revision = access_policy
            .read()
            .map_or(None, |policy| AuthorityRevisionWake::new(policy.as_ref()));
        let deadline = fallback_authorization_deadline(authority_revision.is_none());
        Self {
            policy_revision,
            authority_revision,
            access_policy,
            deadline,
        }
    }

    async fn changed(&mut self) -> AuthorizationWake {
        tokio::select! {
            _ = self.policy_revision.changed() => {
                self.authority_revision = self
                    .access_policy
                    .read()
                    .map_or(None, |policy| AuthorityRevisionWake::new(policy.as_ref()));
                self.deadline = fallback_authorization_deadline(
                    self.authority_revision.is_none(),
                );
                AuthorizationWake::Revision
            }
            () = wait_for_authority_change(self.authority_revision.as_ref()) => {
                AuthorizationWake::Revision
            }
            () = wait_for_authorization_deadline(self.deadline.as_mut()) => {
                AuthorizationWake::Deadline
            }
        }
    }
}

fn fallback_authorization_deadline(enabled: bool) -> Option<Interval> {
    enabled.then(|| {
        let period = Duration::from_millis(50);
        let start = Instant::now()
            .checked_add(period)
            .unwrap_or_else(Instant::now);
        let mut deadline = tokio::time::interval_at(start, period);
        deadline.set_missed_tick_behavior(MissedTickBehavior::Delay);
        deadline
    })
}

async fn wait_for_authorization_deadline(deadline: Option<&mut Interval>) {
    match deadline {
        Some(deadline) => {
            deadline.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn wait_for_authority_change(wake: Option<&AuthorityRevisionWake>) {
    match wake {
        Some(wake) => {
            let _changed = wake.changes.recv_async().await;
        }
        None => std::future::pending::<()>().await,
    }
}

fn trace_authorization_requirements(
    access: &AccessAttempt,
    authorization_phase: AuthorizationPhase,
) {
    if authorization_phase == AuthorizationPhase::Admission {
        tracing::debug!(
            operation = ?access.operation,
            scope_id = ?access.scope_id(),
            scope_selections = ?access.scope_selections(),
            resource_claims = access.resource_claims.len(),
            capabilities = access.application_capabilities.len(),
            "evaluating session authority"
        );
    } else {
        tracing::trace!(
            operation = ?access.operation,
            authorization_phase = ?authorization_phase,
            scope_id = ?access.scope_id(),
            scope_selections = ?access.scope_selections(),
            resource_claims = access.resource_claims.len(),
            capabilities = access.application_capabilities.len(),
            "reevaluating live session authority"
        );
    }
    tracing::trace!(
        claims = ?access.resource_claims,
        application_capabilities = ?access.application_capabilities,
        "session authority requirements"
    );
}

fn trace_authorization_decision(
    operation: AccessOperation,
    authorization_phase: AuthorizationPhase,
    decision: &AuthorizationDecision,
    elapsed: Duration,
) {
    if authorization_phase == AuthorizationPhase::Admission {
        tracing::debug!(
            operation = ?operation,
            decision = ?decision,
            elapsed_ms = elapsed.as_millis(),
            "session authority evaluated"
        );
    } else {
        tracing::trace!(
            operation = ?operation,
            authorization_phase = ?authorization_phase,
            decision = ?decision,
            elapsed_ms = elapsed.as_millis(),
            "live session authority reevaluated"
        );
    }
}

fn debug_initial_handler_progress(
    handler: &HandlerRequest,
    stage: &'static str,
    elapsed: Duration,
    frame: Option<&'static str>,
) {
    tracing::debug!(
        handler_kind = handler.kind.as_str(),
        handler_id = %handler.handler_id,
        stage,
        elapsed_ms = elapsed.as_millis(),
        frame,
        "initial handler stream progress"
    );
}

#[derive(Clone)]
struct NodeFrameSink(flume::Sender<NodeFrame>);

impl crate::server::SessionSink for NodeFrameSink {
    fn send(&self, _message: crate::wire::MykoMessage) {}

    fn send_serialized_command(
        &self,
        _tx: Arc<str>,
        _command_id: String,
        _payload: crate::wire::EncodedCommandMessage,
    ) {
    }

    fn send_node_frame(&self, frame: NodeFrame) -> Result<(), String> {
        self.0.send(frame).map_err(|error| error.to_string())
    }
}

/// Boxed future returned by a federation request router.
pub type NodeRouteFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Routes the same canonical request envelope to another Myko node.
///
/// Transport adapters never select commands or subscriptions themselves. They
/// authenticate a principal and hand the envelope to [`FederatedSession`];
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

/// Future returned by an installed certified-control endpoint.
pub type AuthorityControlFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AuthorizationFailure>> + Send + 'a>>;

/// Request body for one certified-control proposal transport call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityControlProposeRequest {
    pub head: ControlHead,
    pub ballot: ControlBallot,
    pub promises: Vec<SignedControlVote>,
    pub value: ControlValue,
}

/// Local authority coordinator behind authenticated controller transport calls.
///
/// Transport adapters authenticate the peer and deliver typed requests here.
/// This endpoint owns certified-history validation, local controller key use,
/// and the durable [`Node::vote_control`] / [`Node::propose_control`] path.
pub trait AuthorityControlEndpoint: std::fmt::Debug + Send + Sync + 'static {
    /// Persist a prepare vote for the certified predecessor head.
    fn prepare<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        head: ControlHead,
        ballot: ControlBallot,
    ) -> AuthorityControlFuture<'a, SignedControlVote>;

    /// Persist a proposal after validating promise recovery and payload meaning.
    fn propose<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        request: AuthorityControlProposeRequest,
    ) -> AuthorityControlFuture<'a, SignedControlProposal>;

    /// Persist an accept vote for one certified proposal.
    fn accept<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        head: ControlHead,
        proposal: SignedControlProposal,
    ) -> AuthorityControlFuture<'a, SignedControlVote>;
}

/// Shared semantic endpoint behind every Myko transport adapter.
#[derive(Clone)]
pub struct FederatedSession {
    node: Node,
    application: Arc<RwLock<Option<crate::ApplicationHost>>>,
    live_events: LiveEventHub,
    access_policy: Arc<RwLock<Arc<dyn AccessPolicy>>>,
    policy_revision: watch::Sender<u64>,
    router: Arc<RwLock<Option<Weak<dyn NodeRequestRouter>>>>,
    authority_control: Arc<RwLock<Option<Arc<dyn AuthorityControlEndpoint>>>>,
}

impl std::fmt::Debug for FederatedSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FederatedSession")
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

impl FederatedSession {
    /// Creates a session service for a node without application handlers.
    #[must_use]
    pub fn new(node: Node, access_policy: Arc<dyn AccessPolicy>) -> Self {
        Self::new_inner(node, None, access_policy)
    }

    /// Creates a session service for a composed Myko application.
    #[must_use]
    pub fn for_application(
        application: crate::ApplicationHost,
        access_policy: Arc<dyn AccessPolicy>,
    ) -> Self {
        let node = application.node().clone();
        Self::new_inner(node, Some(application), access_policy)
    }

    fn new_inner(
        node: Node,
        application: Option<crate::ApplicationHost>,
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
            authority_control: Arc::new(RwLock::new(None)),
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
    pub fn set_application(&self, application: crate::ApplicationHost) -> Result<(), String> {
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

    /// Replaces the installed application policy with fail-closed framework
    /// state, releasing any application graph retained by the policy.
    ///
    /// Node shutdown uses this before dropping its durable application so an
    /// authority implementation may safely own an application handle without
    /// keeping the journal open after the node has stopped.
    ///
    /// # Errors
    ///
    /// Returns an error when the node or access-policy lock is unavailable.
    pub fn clear_access_policy(&self) -> Result<(), String> {
        self.set_access_policy(Arc::new(DenyAllAccessPolicy))
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

    /// Installs the local certified-control endpoint used by controller peers.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint slot is poisoned.
    pub fn set_authority_control(
        &self,
        endpoint: Option<Arc<dyn AuthorityControlEndpoint>>,
    ) -> Result<(), String> {
        let mut current = self
            .authority_control
            .write()
            .map_err(|_| "authority-control endpoint lock is poisoned".to_owned())?;
        *current = endpoint;
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
        let request_kind = envelope.request.kind();
        let destination = envelope.destination;
        tracing::debug!(
            node_id = %self.node.node_id(),
            principal_id = %authenticated.id,
            principal_kind = ?authenticated.kind,
            request = request_kind,
            destination = ?destination,
            "session request waiting for node readiness"
        );
        self.node.wait_until_ready().await;
        tracing::debug!(
            node_id = %self.node.node_id(),
            principal_id = %authenticated.id,
            request = request_kind,
            "node ready; opening session request"
        );
        let (send, receive) = flume::bounded(SESSION_FRAME_CAPACITY);
        let service = self.clone();
        let span = tracing::debug_span!(
            "myko.session.request",
            node_id = %self.node.node_id(),
            principal_id = %authenticated.id,
            request = request_kind,
            destination = ?destination,
        );
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
                tracing::warn!("rejected authority executor that did not match transport principal");
                return;
            }
            if !matches!(request, NodeRequest::Submit { .. })
                && !matches!(request, NodeRequest::ApproveAuthority { .. })
                && !matches!(
                    request,
                    NodeRequest::ControlPrepare { .. }
                        | NodeRequest::ControlPropose { .. }
                        | NodeRequest::ControlAccept { .. }
                )
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
                        tracing::debug!("request admission authorized");
                    }
                    Ok(None) => {
                        tracing::debug!("request does not require an authority decision");
                    }
                    Err(failure) => {
                        tracing::warn!(failure = ?failure, "request admission failed");
                        let _ignored = emit_authorization_failure(&send, failure).await;
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
                            Ok(destination) => {
                                tracing::debug!(
                                    routed_destination = %destination,
                                    service_id = %command.service_id,
                                    "routed command to capable peer"
                                );
                                envelope.destination = Some(destination);
                            }
                            Err(message) => {
                                tracing::error!(error = %message, "command routing failed");
                                let _ignored = send.send_async(NodeFrame::Error { message }).await;
                                return;
                            }
                        }
                    }
                    Err(message) => {
                        tracing::error!(error = %message, "application routing lookup failed");
                        let _ignored = send.send_async(NodeFrame::Error { message }).await;
                        return;
                    }
                }
            }
            let destination = envelope.destination;
            let local = destination.is_none_or(|node_id| node_id == service.node.node_id());
            tracing::debug!(local, destination = ?destination, "dispatching session request");
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
            match result {
                Ok(()) => tracing::debug!("session request completed"),
                Err(message) => {
                    tracing::error!(error = %message, "session request failed");
                    let _ignored = send.send_async(NodeFrame::Error { message }).await;
                }
            }
        }
        .instrument(span));
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
            NodeRequest::ControlPrepare { head, ballot } => {
                self.control_prepare(&principal, &presentation, head, ballot, send)
                    .await
            }
            NodeRequest::ControlPropose {
                head,
                ballot,
                promises,
                value,
            } => {
                self.control_propose(
                    &principal,
                    &presentation,
                    AuthorityControlProposeRequest {
                        head,
                        ballot,
                        promises,
                        value,
                    },
                    send,
                )
                .await
            }
            NodeRequest::ControlAccept { head, proposal } => {
                self.control_accept(&principal, &presentation, head, *proposal, send)
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
            Err(failure) => return emit_authorization_failure(send, failure).await,
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
            Err(failure) => return emit_authorization_failure(send, failure).await,
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
        let commands = std::mem::take(&mut page.commands);
        let mut authorized_commands = Vec::with_capacity(commands.len());
        for entry in commands {
            match self.command_snapshot_authorized(
                principal,
                presentation,
                AccessOperation::ReadCommands,
                &entry.command,
            ) {
                Ok(true) => authorized_commands.push(entry),
                Ok(false) => {}
                Err(reason) => {
                    return emit(send, NodeFrame::AuthorityUnavailable { reason }).await;
                }
            }
        }
        page.commands = authorized_commands;
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
        let mut command_watch = self
            .node
            .watch_commands(watch.clone())
            .map_err(|error| error.to_string())?;
        emit(
            send,
            NodeFrame::CommandWatchReady {
                request: Box::new(watch.clone()),
            },
        )
        .await?;
        let snapshot = self
            .node
            .command_states(myko_federation::CommandStateRequest {
                source_node: Some(watch.source_node),
                service_id: watch.service_id.clone(),
                scope_id: watch.scope_id.clone(),
                command_type: watch.command_type.clone(),
                snapshot_through: watch.after,
                after_command_id: None,
                page_size: myko_federation::DEFAULT_COMMAND_STATE_PAGE_SIZE,
            })
            .map_err(|error| error.to_string())?;
        let mut visible_commands = BTreeMap::new();
        for entry in snapshot.commands {
            match self.command_snapshot_authorized(
                &principal,
                &presentation,
                AccessOperation::WatchCommands,
                &entry.command,
            ) {
                Ok(true) => {
                    visible_commands.insert(entry.command.request.id.to_string(), entry.command);
                }
                Ok(false) => {}
                Err(reason) => {
                    return emit(send, NodeFrame::AuthorityUnavailable { reason }).await;
                }
            }
        }
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                update = command_watch.recv_async() => {
                    let mut update = update.map_err(|error| error.to_string())?;
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    let mut allowed = Vec::with_capacity(update.commands.len());
                    for entry in &update.commands {
                        let command_id = entry.command.request.id.to_string();
                        match self.command_snapshot_authorized(
                            &principal,
                            &presentation,
                            AccessOperation::WatchCommands,
                            &entry.command,
                        ) {
                            Ok(true) => allowed.push((command_id, entry.clone())),
                            Ok(false) if visible_commands.contains_key(&command_id) => return Ok(()),
                            Ok(false) => {}
                            Err(reason) => {
                                emit(send, NodeFrame::AuthorityUnavailable { reason }).await?;
                                return Ok(());
                            }
                        }
                    }
                    if allowed.is_empty() {
                        continue;
                    }
                    update.commands = allowed.iter().map(|(_, entry)| entry.clone()).collect();
                    emit(send, NodeFrame::CommandUpdate { update: Box::new(update) }).await?;
                    for (command_id, entry) in allowed {
                        visible_commands.insert(command_id, entry.command);
                    }
                }
                wake = authorization.changed() => {
                    if !self.stream_authorized(&principal, &presentation, &request, send).await? {
                        return Ok(());
                    }
                    match wake {
                        // The broad catalog grant can remain valid while a
                        // grant covering one already-visible command changes.
                        // Refetching is the only way to retract stale entries.
                        AuthorizationWake::Revision => return Ok(()),
                        AuthorizationWake::Deadline => {
                            if self.has_revoked_visible_command(
                                &principal,
                                &presentation,
                                &visible_commands,
                            ) {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    fn has_revoked_visible_command(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        visible_commands: &BTreeMap<String, myko_federation::CommandSnapshot>,
    ) -> bool {
        visible_commands.values().any(|command| {
            !matches!(
                self.command_snapshot_authorized(
                    principal,
                    presentation,
                    AccessOperation::WatchCommands,
                    command,
                ),
                Ok(true)
            )
        })
    }

    fn command_snapshot_authorized(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        operation: AccessOperation,
        command: &myko_federation::CommandSnapshot,
    ) -> Result<bool, AuthorityUnavailable> {
        let request = &command.request;
        let access = AccessAttempt {
            principal_id: principal.clone(),
            presentation: presentation.clone(),
            operation,
            target: AccessTarget::KnownCommand {
                command_id: request.id,
                service_id: request.service_id.clone(),
                scope_id: request.scope_id.clone(),
                command_type: request.command_type.clone(),
                principal_id: request.principal_id.clone(),
            },
            resource_claims: request.resource_claims.clone(),
            application_capabilities: request.application_capabilities.clone(),
            arguments_digest: request.arguments_digest.clone(),
            effect_digest: None,
            lease: None,
            authorization_phase: AuthorizationPhase::Continuation,
            topology: self.node.scope_topology().ok(),
        };
        self.access_policy
            .read()
            .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?
            .decide(&access)
            .map(|decision| matches!(decision, AuthorizationDecision::Permit(_)))
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
                _ = authorization.changed() => {
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
                _ = authorization.changed() => {
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
        let topology = self
            .node
            .scope_topology()
            .map_err(|error| error.to_string())?;
        let mut scopes = Vec::with_capacity(limit.saturating_add(1));
        let mut scan_after = after;
        while scopes.len() <= limit {
            let page = self
                .node
                .scope_ids_page(scan_after.as_ref(), SCOPE_CATALOG_SCAN_PAGE)
                .map_err(|error| error.to_string())?;
            let page_len = page.len();
            if page_len == 0 {
                break;
            }
            for scope_id in page {
                scan_after = Some(scope_id.clone());
                let mut access = AccessAttempt::scoped(
                    principal.clone(),
                    presentation.clone(),
                    AccessOperation::ReadHistory,
                    scope_id.clone(),
                );
                access.topology = Some(topology.clone());
                match policy.decide(&access) {
                    Ok(AuthorizationDecision::Permit(_)) => {
                        scopes.push(scope_id);
                        if scopes.len() > limit {
                            break;
                        }
                    }
                    Ok(
                        AuthorizationDecision::Deny(_) | AuthorizationDecision::Challenge { .. },
                    ) => {}
                    Err(reason) => {
                        return emit(send, NodeFrame::AuthorityUnavailable { reason }).await;
                    }
                }
            }
            if scopes.len() > limit || page_len < SCOPE_CATALOG_SCAN_PAGE.get() {
                break;
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
                _ = authorization.changed() => {
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
                    let selected = if ReplicationSelection::Scopes(vec![
                        myko_federation::ScopeSelection::Exact(scope_id.clone()),
                    ]).includes(&event.event) {
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
                _ = authorization.changed() => {
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
        let mut cursor = initial.through;
        let mut authorization = self.authorization_pulse();
        loop {
            tokio::select! {
                event = events.recv_async() => {
                    let _event = event.map_err(|error| error.to_string())?;
                    let selection = match self.constrain_replication(
                        &principal,
                        &presentation,
                        &request,
                        &requested_selection,
                        AuthorizationPhase::Continuation,
                    ) {
                        Ok(selection) => selection,
                        Err(decision) => {
                            emit_authorization_failure(send, decision).await?;
                            return Ok(());
                        }
                    };
                    let batch = self.node.export_selected(selection, cursor)
                        .map_err(|error| error.to_string())?;
                    if batch.through != cursor {
                        cursor = batch.through;
                        emit(send, NodeFrame::SelectedBatch { batch: Box::new(batch) }).await?;
                    }
                }
                _ = authorization.changed() => {
                    if let Err(decision) = self.constrain_replication(
                        &principal,
                        &presentation,
                        &request,
                        &requested_selection,
                        AuthorizationPhase::Continuation,
                    ) {
                        emit_authorization_failure(send, decision).await?;
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
                _ = authorization.changed() => {
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
        tracing::debug!(
            handler_kind = handler.kind.as_str(),
            handler_id = %handler.handler_id,
            source_node = ?handler.source_node,
            scope_id = ?handler.scope_id,
            "opening handler subscription"
        );
        let application = self
            .application
            .read()
            .map_err(|_| "application lock is poisoned".to_owned())?
            .clone()
            .ok_or_else(|| "this node does not expose a Myko application".to_owned())?;
        let opened = std::time::Instant::now();
        let tx: Arc<str> = Arc::from(uuid::Uuid::new_v4().to_string());
        let mut session = crate::server::ClientSession::new(
            Arc::from(principal.as_str()),
            NodeFrameSink(send.clone()),
        );
        application.open_handler(&mut session, tx, handler.clone())?;
        tracing::debug!(
            handler_kind = handler.kind.as_str(),
            handler_id = %handler.handler_id,
            "handler subscription opened"
        );
        debug_initial_handler_progress(&handler, "handler_opened", opened.elapsed(), None);
        let mut authorization = self.authorization_pulse();
        loop {
            let _wake = authorization.changed().await;
            tracing::trace!("handler subscription woke for authority change");
            if !self
                .stream_authorized(&principal, &presentation, &request, send)
                .await?
            {
                tracing::debug!(
                    handler_kind = handler.kind.as_str(),
                    handler_id = %handler.handler_id,
                    "handler subscription authorization ended"
                );
                return Ok(());
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
        match policy
            .approve(principal, presentation, challenge_id, approved)
            .await
        {
            Ok(decision) => {
                emit(
                    send,
                    NodeFrame::Approval {
                        decision: Box::new(decision),
                    },
                )
                .await
            }
            Err(failure) => emit_authorization_failure(send, failure).await,
        }
    }

    async fn control_prepare(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        head: ControlHead,
        ballot: ControlBallot,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let endpoint = match self.authority_control_endpoint() {
            Ok(endpoint) => endpoint,
            Err(failure) => return emit_authorization_failure(send, failure).await,
        };
        match endpoint
            .prepare(principal, presentation, head, ballot)
            .await
        {
            Ok(vote) => {
                emit(
                    send,
                    NodeFrame::ControlVote {
                        vote: Box::new(vote),
                    },
                )
                .await
            }
            Err(failure) => emit_authorization_failure(send, failure).await,
        }
    }

    async fn control_propose(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: AuthorityControlProposeRequest,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let endpoint = match self.authority_control_endpoint() {
            Ok(endpoint) => endpoint,
            Err(failure) => return emit_authorization_failure(send, failure).await,
        };
        match endpoint.propose(principal, presentation, request).await {
            Ok(proposal) => {
                emit(
                    send,
                    NodeFrame::ControlProposal {
                        proposal: Box::new(proposal),
                    },
                )
                .await
            }
            Err(failure) => emit_authorization_failure(send, failure).await,
        }
    }

    async fn control_accept(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        head: ControlHead,
        proposal: SignedControlProposal,
        send: &flume::Sender<NodeFrame>,
    ) -> Result<(), String> {
        let endpoint = match self.authority_control_endpoint() {
            Ok(endpoint) => endpoint,
            Err(failure) => return emit_authorization_failure(send, failure).await,
        };
        match endpoint
            .accept(principal, presentation, head, proposal)
            .await
        {
            Ok(vote) => {
                emit(
                    send,
                    NodeFrame::ControlVote {
                        vote: Box::new(vote),
                    },
                )
                .await
            }
            Err(failure) => emit_authorization_failure(send, failure).await,
        }
    }

    fn authority_control_endpoint(
        &self,
    ) -> Result<Arc<dyn AuthorityControlEndpoint>, AuthorizationFailure> {
        let endpoint = self
            .authority_control
            .read()
            .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?
            .clone()
            .ok_or(AuthorityUnavailable::CoordinationUnavailable)?;
        Ok(endpoint)
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
            Err(failure) => {
                emit_authorization_failure(send, failure).await?;
                Ok(false)
            }
        }
    }

    fn authorize(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
    ) -> Result<Option<PermitDecision>, AuthorizationFailure> {
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
    ) -> Result<Option<PermitDecision>, AuthorizationFailure> {
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
    ) -> Result<Option<PermitDecision>, AuthorizationFailure> {
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
                        _ => return Err(decision_to_failure(missing_command_decision)),
                    };
                    return Err(decision_to_failure(undiscoverable_command_decision(
                        presentation,
                        operation,
                    )));
                }
                Err(decision) => return Err(decision_to_failure(decision)),
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
        trace_authorization_requirements(&access, authorization_phase);
        let started = std::time::Instant::now();
        let decision = self
            .access_policy
            .read()
            .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?
            .decide(&access);
        if let Ok(decision) = &decision {
            trace_authorization_decision(
                operation,
                authorization_phase,
                decision,
                started.elapsed(),
            );
        }
        match decision {
            Ok(AuthorizationDecision::Permit(permit)) => Ok(Some(permit)),
            Ok(_decision)
                if matches!(
                    request,
                    NodeRequest::Command { .. }
                        | NodeRequest::WatchCommand { .. }
                        | NodeRequest::Cancel { .. }
                ) =>
            {
                Err(decision_to_failure(undiscoverable_command_decision(
                    presentation,
                    access.operation,
                )))
            }
            Ok(decision) => match decision.into_permit() {
                Ok(_) => Err(AuthorizationFailure::Unavailable(
                    AuthorityUnavailable::PolicyUnavailable,
                )),
                Err(failure) => Err(failure),
            },
            Err(reason) => Err(AuthorizationFailure::Unavailable(reason)),
        }
    }

    fn access_request(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
        request: &NodeRequest,
        authorization_phase: AuthorizationPhase,
    ) -> Result<AccessAttempt, AuthorizationDecision> {
        let prepared = self.prepare_access(request).map_err(|message| {
            policy_denial(
                principal,
                presentation,
                AccessOperation::ReadHistory,
                message,
            )
        })?;
        let mut access = AccessAttempt {
            principal_id: principal.clone(),
            presentation: presentation.clone(),
            operation: prepared.operation,
            target: prepared.target,
            resource_claims: prepared.resource_claims,
            application_capabilities: prepared.application_capabilities,
            arguments_digest: prepared.arguments_digest,
            effect_digest: None,
            lease: presentation.requested_lease,
            authorization_phase,
            topology: self.node.scope_topology().ok(),
        };
        if access.resource_claims.is_empty()
            && let Some(scope_id) = access.scope_id().cloned()
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
    ) -> Result<ReplicationSelection, AuthorizationFailure> {
        let access = self
            .access_request(principal, presentation, request, authorization_phase)
            .map_err(decision_to_failure)?;
        let topology = access.topology.clone().ok_or_else(|| {
            decision_to_failure(policy_denial(
                principal,
                presentation,
                access.operation,
                "scope topology is incomplete".to_owned(),
            ))
        })?;
        self.access_policy
            .read()
            .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?
            .constrain_replication(&access, selection, &topology)
    }

    fn prepare_access(&self, request: &NodeRequest) -> Result<AccessPreparation, String> {
        Ok(match request {
            NodeRequest::Identify | NodeRequest::ListScopes { .. } => {
                return Err("request is authorized outside access metadata".to_owned());
            }
            NodeRequest::Pull { .. } => {
                history_access(AccessOperation::ReadHistory, &ReplicationSelection::All)
            }
            NodeRequest::PullScope { scope_id, .. } => history_access(
                AccessOperation::ReadHistory,
                &ReplicationSelection::Scopes(vec![myko_federation::ScopeSelection::Exact(
                    scope_id.clone(),
                )]),
            ),
            NodeRequest::PullSelected { selection, .. } => {
                selected(AccessOperation::ReadHistory, selection)
            }
            NodeRequest::Follow { .. } => {
                history_access(AccessOperation::FollowHistory, &ReplicationSelection::All)
            }
            NodeRequest::FollowScope { scope_id, .. } => history_access(
                AccessOperation::FollowHistory,
                &ReplicationSelection::Scopes(vec![myko_federation::ScopeSelection::Exact(
                    scope_id.clone(),
                )]),
            ),
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
            NodeRequest::FollowHandler { request } => self.handler_access(request)?,
            NodeRequest::ApproveAuthority { .. } => {
                return Err("authority approval is handled outside prepared access".to_owned());
            }
            NodeRequest::ControlPrepare { .. }
            | NodeRequest::ControlPropose { .. }
            | NodeRequest::ControlAccept { .. } => {
                return Err("authority control is handled outside prepared access".to_owned());
            }
        })
    }

    fn command_access(
        &self,
        operation: AccessOperation,
        command_id: CommandId,
    ) -> Result<AccessPreparation, String> {
        Ok(self
            .node
            .command(command_id)
            .map_err(|error| error.to_string())?
            .map_or_else(
                || preparation(operation, AccessTarget::Command(command_id)),
                |command| {
                    let request = command.request;
                    AccessPreparation {
                        operation,
                        target: AccessTarget::KnownCommand {
                            command_id,
                            service_id: request.service_id,
                            scope_id: request.scope_id,
                            command_type: request.command_type,
                            principal_id: request.principal_id,
                        },
                        resource_claims: request.resource_claims,
                        application_capabilities: request.application_capabilities,
                        arguments_digest: request.arguments_digest,
                    }
                },
            ))
    }

    fn handler_access(&self, request: &HandlerRequest) -> Result<AccessPreparation, String> {
        let application = self
            .application
            .read()
            .map_err(|_| "application lock is poisoned".to_owned())?
            .clone()
            .ok_or_else(|| "this node does not expose a Myko application".to_owned())?;
        let authority = application.handler_authority(request)?;
        if authority.source_node != request.source_node || authority.scope_id != request.scope_id {
            return Err("handler source or scope does not match its typed parameters".to_owned());
        }
        let mut prepared = handler_access(request);
        if let Some(scope_id) = &request.scope_id {
            prepared
                .resource_claims
                .push(handler_subscription_claim(ResourceClaim {
                    selection: myko_federation::ScopeSelection::Exact(scope_id.clone()),
                    kind: ResourceClaimKind::Primary,
                    source_node: request.source_node,
                    service_id: None,
                    item_type: None,
                    item_id: None,
                    required_permissions: Vec::new(),
                    required_operations: Vec::new(),
                    required_capabilities: Vec::new(),
                }));
        }
        prepared.resource_claims.extend(authority.resource_claims);
        prepared.application_capabilities = authority.application_capabilities;
        Ok(prepared)
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

async fn emit_authorization_failure(
    send: &flume::Sender<NodeFrame>,
    failure: AuthorizationFailure,
) -> Result<(), String> {
    match failure {
        AuthorizationFailure::Deny(denied) => {
            emit(
                send,
                NodeFrame::Authorization {
                    decision: Box::new(AuthorizationDecision::Deny(*denied)),
                },
            )
            .await
        }
        AuthorizationFailure::Challenge { challenge, report } => {
            emit(
                send,
                NodeFrame::Authorization {
                    decision: Box::new(AuthorizationDecision::Challenge {
                        challenge: *challenge,
                        report: *report,
                    }),
                },
            )
            .await
        }
        AuthorizationFailure::Unavailable(reason) => {
            emit(send, NodeFrame::AuthorityUnavailable { reason }).await
        }
    }
}

fn decision_to_failure(decision: AuthorizationDecision) -> AuthorizationFailure {
    match decision {
        AuthorizationDecision::Permit(_) => {
            AuthorizationFailure::Unavailable(AuthorityUnavailable::PolicyUnavailable)
        }
        AuthorizationDecision::Deny(denied) => AuthorizationFailure::Deny(Box::new(denied)),
        AuthorizationDecision::Challenge { challenge, report } => AuthorizationFailure::Challenge {
            challenge: Box::new(challenge),
            report: Box::new(report),
        },
    }
}

const fn preparation(operation: AccessOperation, target: AccessTarget) -> AccessPreparation {
    AccessPreparation {
        operation,
        target,
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
    }
}

fn history_access(
    operation: AccessOperation,
    selection: &ReplicationSelection,
) -> AccessPreparation {
    let mut prepared = preparation(operation, AccessTarget::History(selection.clone()));
    let selections = match selection {
        ReplicationSelection::ServiceScope { scope_id, .. } => {
            vec![myko_federation::ScopeSelection::Exact(scope_id.clone())]
        }
        ReplicationSelection::Scopes(selections)
        | ReplicationSelection::Intersection {
            scopes: selections, ..
        } => selections.clone(),
        ReplicationSelection::All | ReplicationSelection::Service(_) => Vec::new(),
    };
    prepared.resource_claims = selections
        .into_iter()
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
    prepared
}

fn selected(operation: AccessOperation, selection: &ReplicationSelection) -> AccessPreparation {
    history_access(operation, selection)
}

fn live_access(topics: &[String]) -> AccessPreparation {
    let mut prepared = preparation(
        AccessOperation::SubscribeLive,
        AccessTarget::LiveTopics(topics.to_vec()),
    );
    prepared.resource_claims = topics
        .iter()
        .map(|topic| {
            ResourceClaim::scope(
                ScopeId::new(format!("myko.live/{topic}")),
                ResourceClaimKind::Primary,
            )
        })
        .collect();
    prepared
}

fn item_access(
    operation: AccessOperation,
    source_node: Option<NodeId>,
    service_id: &ServiceId,
    scope_id: &ScopeId,
    item_type: &str,
) -> AccessPreparation {
    let mut prepared = preparation(
        operation,
        AccessTarget::Items {
            source_node,
            service_id: service_id.clone(),
            scope_id: scope_id.clone(),
            item_type: item_type.to_owned(),
        },
    );
    prepared.resource_claims = vec![ResourceClaim {
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
    prepared
}

fn handler_access(request: &HandlerRequest) -> AccessPreparation {
    let mut prepared = preparation(
        AccessOperation::FollowHandler,
        AccessTarget::Handler {
            access: myko_federation::HandlerAccess {
                kind: request.kind,
                handler_id: request.handler_id.clone(),
            },
            source_node: request.source_node,
            scope_id: request.scope_id.clone(),
        },
    );
    let encoded = serde_json::to_vec(&(request.source_node, &request.params)).unwrap_or_default();
    prepared.arguments_digest = Some(format!("{:x}", Sha256::digest(encoded)));
    prepared
}

fn handler_subscription_claim(mut claim: ResourceClaim) -> ResourceClaim {
    if !claim
        .required_permissions
        .contains(&FederationPermission::ReadState)
    {
        claim
            .required_permissions
            .push(FederationPermission::ReadState);
    }
    if !claim
        .required_permissions
        .contains(&FederationPermission::Subscribe)
    {
        claim
            .required_permissions
            .push(FederationPermission::Subscribe);
    }
    if !claim
        .required_operations
        .contains(&AccessOperation::FollowHandler)
    {
        claim
            .required_operations
            .push(AccessOperation::FollowHandler);
    }
    claim
}

fn catalog(
    operation: AccessOperation,
    service_id: &ServiceId,
    scope_id: &ScopeId,
    command_type: &str,
) -> AccessPreparation {
    preparation(
        operation,
        AccessTarget::CommandCatalog {
            source_node: None,
            service_id: service_id.clone(),
            scope_id: scope_id.clone(),
            command_type: command_type.to_owned(),
        },
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

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use myko_federation::{
        AllowAllAccessPolicy, AuthorityPresentation, BatchId, ChangeBatch, CommandRequest,
        CommandStateRequest, DenyAllAccessPolicy, EventEnvelope, EventId, FrameworkControlEvent,
        PrincipalKind, RetainedHistoryStatement, ScopeSelection, SignedRetainedHistoryStatement,
        StorageIncarnationId,
        control_quorum::{ControlSlot, ControlVote, ControlVoteKind, ControllerId},
    };

    use super::*;

    #[crate::myko_view_item]
    struct ProtectedHandlerRow {
        id: Arc<str>,
    }

    #[crate::myko_view(ProtectedHandlerRow)]
    #[derive(PartialEq, Eq)]
    struct ProtectedHandlerView {
        #[ts(type = "string")]
        source_node: NodeId,
        #[ts(type = "string")]
        scope_id: ScopeId,
    }

    impl crate::view::ViewHandler for ProtectedHandlerView {
        fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
            Some(self.source_node)
        }

        fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
            Some(self.scope_id.clone())
        }

        fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
            vec![myko_federation::CapabilityId::new("test.handler.runtime")]
        }

        fn build_cell(
            _context: crate::view::ViewBuildArgs<Self>,
        ) -> impl crate::view::ViewBuildOutput<Item = Self::Item> {
            crate::view::LocalView::new(hyphae::CellMap::new().lock())
        }
    }

    #[derive(Debug)]
    struct CapturingPolicy {
        allow: AtomicBool,
        expected_item_type: Option<&'static str>,
        seen: Mutex<Vec<AccessAttempt>>,
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
        fn decide(
            &self,
            request: &AccessAttempt,
        ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
            self.seen.lock().unwrap().push(request.clone());
            let rule = if !self.allow.load(Ordering::Acquire) {
                Err("revoked".to_owned())
            } else if let Some(expected) = self.expected_item_type
                && !request
                    .resource_claims
                    .iter()
                    .all(|claim| claim.item_type.as_deref() == Some(expected))
            {
                Err("item type constraint rejected".to_owned())
            } else {
                Ok(())
            };
            Ok(AuthorizationDecision::from_rule(request, rule))
        }
    }

    #[derive(Debug)]
    struct RecordingControlEndpoint {
        seen: Mutex<
            Vec<(
                PrincipalId,
                AuthorityPresentation,
                ControlHead,
                ControlBallot,
            )>,
        >,
        vote: SignedControlVote,
    }

    impl RecordingControlEndpoint {
        fn new(vote: SignedControlVote) -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
                vote,
            }
        }
    }

    impl AuthorityControlEndpoint for RecordingControlEndpoint {
        fn prepare<'a>(
            &'a self,
            principal: &'a PrincipalId,
            presentation: &'a AuthorityPresentation,
            head: ControlHead,
            ballot: ControlBallot,
        ) -> AuthorityControlFuture<'a, SignedControlVote> {
            self.seen
                .lock()
                .unwrap()
                .push((principal.clone(), presentation.clone(), head, ballot));
            Box::pin(async move { Ok(self.vote.clone()) })
        }

        fn propose<'a>(
            &'a self,
            _principal: &'a PrincipalId,
            _presentation: &'a AuthorityPresentation,
            _request: AuthorityControlProposeRequest,
        ) -> AuthorityControlFuture<'a, SignedControlProposal> {
            Box::pin(async move {
                Err(AuthorizationFailure::Unavailable(
                    AuthorityUnavailable::CoordinationUnavailable,
                ))
            })
        }

        fn accept<'a>(
            &'a self,
            _principal: &'a PrincipalId,
            _presentation: &'a AuthorityPresentation,
            _head: ControlHead,
            _proposal: SignedControlProposal,
        ) -> AuthorityControlFuture<'a, SignedControlVote> {
            Box::pin(async move {
                Err(AuthorizationFailure::Unavailable(
                    AuthorityUnavailable::CoordinationUnavailable,
                ))
            })
        }
    }

    #[derive(Debug)]
    struct CatalogEntryPolicy {
        entries_allowed: bool,
        entries_expire_at: Option<tokio::time::Instant>,
    }

    impl AccessPolicy for CatalogEntryPolicy {
        fn decide(
            &self,
            request: &AccessAttempt,
        ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
            let rule = if request.operation == AccessOperation::WatchCommands
                && request.command_id().is_some()
                && (!self.entries_allowed
                    || self
                        .entries_expire_at
                        .is_some_and(|deadline| tokio::time::Instant::now() >= deadline))
            {
                Err("stored command claims were revoked".to_owned())
            } else {
                Ok(())
            };
            Ok(AuthorizationDecision::from_rule(request, rule))
        }
    }

    #[derive(Debug)]
    struct SelectiveCatalogPolicy {
        denied: CommandId,
    }
    impl AccessPolicy for SelectiveCatalogPolicy {
        fn decide(
            &self,
            request: &AccessAttempt,
        ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
            let rule = if request.operation == AccessOperation::WatchCommands
                && request.command_id() == Some(self.denied)
            {
                Err("selected catalog entry is hidden".to_owned())
            } else {
                Ok(())
            };
            Ok(AuthorizationDecision::from_rule(request, rule))
        }
    }

    #[derive(Debug)]
    struct TemporarilyUnavailablePolicy {
        unavailable: AtomicBool,
    }

    impl AccessPolicy for TemporarilyUnavailablePolicy {
        fn decide(
            &self,
            request: &AccessAttempt,
        ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
            if self.unavailable.load(Ordering::Acquire) {
                Err(AuthorityUnavailable::StateNotCurrent)
            } else {
                Ok(AuthorizationDecision::from_rule(request, Ok(())))
            }
        }
    }

    #[tokio::test]
    async fn full_principal_kind_is_transport_bound() {
        let authenticated = Principal::new(PrincipalId::new("same-id"), PrincipalKind::Node);
        let asserted = Principal::new(PrincipalId::new("same-id"), PrincipalKind::Person);
        let session = FederatedSession::new(Node::in_memory(), Arc::new(AllowAllAccessPolicy));
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
        let session = FederatedSession::new(Node::in_memory(), policy.clone());
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
    async fn unavailable_authority_is_retryable_not_a_denial() {
        let policy = Arc::new(TemporarilyUnavailablePolicy {
            unavailable: AtomicBool::new(true),
        });
        let session = FederatedSession::new(Node::in_memory(), policy.clone());
        let request = NodeRequestEnvelope::connected(NodeRequest::PullScope {
            scope_id: ScopeId::new("retryable:scope"),
            after: None,
        });
        let mut unavailable = session
            .open(PrincipalId::new("node:subscriber"), request.clone())
            .await;
        assert!(matches!(
            unavailable.recv().await,
            Some(NodeFrame::AuthorityUnavailable {
                reason: AuthorityUnavailable::StateNotCurrent,
            })
        ));
        assert!(unavailable.recv().await.is_none());

        policy.unavailable.store(false, Ordering::Release);
        let mut retried = session
            .open(PrincipalId::new("node:subscriber"), request)
            .await;
        assert!(matches!(
            retried.recv().await,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Permit(_))
        ));
        assert!(matches!(
            retried.recv().await,
            Some(NodeFrame::Hello { .. })
        ));
        assert!(matches!(
            retried.recv().await,
            Some(NodeFrame::ScopedBatch { batch }) if batch.events.is_empty()
        ));
    }

    #[tokio::test]
    async fn control_prepare_uses_authenticated_endpoint() {
        let session = FederatedSession::new(Node::in_memory(), Arc::new(DenyAllAccessPolicy));
        let head = ControlHead([1; 32]);
        let ballot = ControlBallot {
            counter: 7,
            proposer: ControllerId([2; 32]),
        };
        let vote = SignedControlVote {
            message: ControlVote {
                slot: ControlSlot {
                    realm: ScopeId::new("authority-realm"),
                    epoch: myko_federation::control_quorum::ControlEpochId([3; 32]),
                    predecessor: head,
                },
                ballot,
                controller: ControllerId([4; 32]),
                vote: ControlVoteKind::Promise { accepted: None },
            },
            signature: [5; 64],
        };
        let endpoint = Arc::new(RecordingControlEndpoint::new(vote.clone()));
        session
            .set_authority_control(Some(endpoint.clone()))
            .unwrap();
        let principal = Principal::new(
            PrincipalId::new("node:controller-peer"),
            PrincipalKind::Node,
        );
        let mut frames = session
            .open_authenticated(
                principal.clone(),
                NodeRequestEnvelope::connected(NodeRequest::ControlPrepare { head, ballot }),
            )
            .await;

        assert!(matches!(
            frames.recv().await,
            Some(NodeFrame::ControlVote { vote: actual }) if *actual == vote
        ));
        assert!(frames.recv().await.is_none());
        let seen = endpoint.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        let Some((seen_principal, seen_presentation, seen_head, seen_ballot)) = seen.first() else {
            return;
        };
        assert_eq!(seen_principal, &principal.id);
        assert_eq!(seen_presentation.executor, principal);
        assert_eq!(*seen_head, head);
        assert_eq!(*seen_ballot, ballot);
    }

    #[test]
    fn handler_identity_is_typed_and_not_an_authority_scope() {
        let request = HandlerRequest {
            kind: myko_federation::HandlerKind::View,
            handler_id: "ForestView".to_owned(),
            source_node: None,
            scope_id: None,
            params: serde_json::Value::Null,
        };
        let prepared = handler_access(&request);
        assert!(prepared.resource_claims.is_empty());
        assert_eq!(
            prepared.target,
            AccessTarget::Handler {
                access: myko_federation::HandlerAccess {
                    kind: myko_federation::HandlerKind::View,
                    handler_id: "ForestView".to_owned(),
                },
                source_node: None,
                scope_id: None,
            }
        );

        let scope = ScopeId::new("forest:catalog");
        let claim = handler_subscription_claim(ResourceClaim::scope(
            scope.clone(),
            ResourceClaimKind::Referenced,
        ));
        assert_eq!(claim.selection, ScopeSelection::Exact(scope));
        assert!(
            claim
                .required_permissions
                .contains(&myko_federation::FederationPermission::ReadState)
        );
        assert!(
            claim
                .required_permissions
                .contains(&myko_federation::FederationPermission::Subscribe)
        );
        assert!(
            claim
                .required_operations
                .contains(&AccessOperation::FollowHandler)
        );
    }

    #[tokio::test]
    async fn handler_authority_includes_typed_scope_and_runtime_capability() {
        let node = Node::in_memory();
        let source_node = node.node_id();
        let scope_id = ScopeId::new("protected:handler");
        let application = crate::ApplicationHost::new(node, crate::MykoApplication::new()).unwrap();
        let policy = Arc::new(CapturingPolicy::allow());
        let session = FederatedSession::for_application(application, policy.clone());
        let request = NodeRequest::FollowHandler {
            request: HandlerRequest {
                kind: myko_federation::HandlerKind::View,
                handler_id: "ProtectedHandlerView".to_owned(),
                source_node: Some(source_node),
                scope_id: Some(scope_id.clone()),
                params: serde_json::to_value(ProtectedHandlerView {
                    source_node,
                    scope_id: scope_id.clone(),
                })
                .unwrap(),
            },
        };
        let mut frames = session
            .open(
                PrincipalId::new("node:subscriber"),
                NodeRequestEnvelope::connected(request),
            )
            .await;
        assert!(matches!(
            frames.recv().await,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Permit(_))
        ));
        let seen = policy.seen.lock().unwrap();
        let access = seen
            .iter()
            .find(|access| access.operation == AccessOperation::FollowHandler)
            .unwrap();
        assert!(
            access
                .application_capabilities
                .contains(&myko_federation::CapabilityId::new("test.handler.runtime"))
        );
        assert!(access.resource_claims.iter().any(|claim| {
            claim.selection == ScopeSelection::Exact(scope_id.clone())
                && claim.source_node == Some(source_node)
                && claim
                    .required_permissions
                    .contains(&FederationPermission::ReadState)
        }));
        drop(seen);
    }

    #[tokio::test]
    async fn catalog_watch_closes_when_visible_entry_authority_changes() {
        let node = Node::in_memory();
        let session = FederatedSession::new(
            node.clone(),
            Arc::new(CatalogEntryPolicy {
                entries_allowed: true,
                entries_expire_at: None,
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
        assert!(
            tokio::time::timeout(Duration::from_millis(175), frames.recv())
                .await
                .is_err(),
            "deadline rechecks must not close an unchanged catalog watch"
        );

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
            Ok(Some(NodeFrame::CommandUpdate { update }))
                if update.commands.iter().any(|entry| entry.command.request.id == command_id)
        ));

        session
            .set_access_policy(Arc::new(CatalogEntryPolicy {
                entries_allowed: false,
                entries_expire_at: None,
            }))
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), frames.recv()).await,
            Ok(None)
        ));
    }

    struct PendingCatalog {
        node: Node,
        parent: myko_federation::EventEnvelope,
        watch: myko_federation::CommandWatchRequest,
        commands: Vec<CommandId>,
    }

    fn pending_catalog() -> PendingCatalog {
        let principal = PrincipalId::new("node:catalog-reader");
        let scope_id = ScopeId::new("scope:catalog");
        let service_id = ServiceId::new("test.service");
        let request = |id| CommandRequest {
            id,
            service_id: service_id.clone(),
            scope_id: scope_id.clone(),
            principal_id: principal.clone(),
            authority: AuthorityPresentation::direct_node(principal.clone()),
            resource_claims: vec![ResourceClaim::scope(
                scope_id.clone(),
                ResourceClaimKind::Primary,
            )],
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: "test.command".to_owned(),
            payload: Vec::new(),
        };
        let parent_source = Node::in_memory();
        let parent = parent_source
            .admit(request(CommandId::new()))
            .unwrap()
            .snapshot()
            .clone();
        let parent_envelope = parent_source.events_after(None).unwrap().pop().unwrap();
        let source = Node::in_memory();
        let mut command_ids = Vec::new();
        for _ in 0..2 {
            let command = request(CommandId::new());
            command_ids.push(command.id);
            let executing = source.admit(command.clone()).unwrap().snapshot().clone();
            source
                .commit(
                    command.id,
                    ChangeBatch {
                        id: BatchId::new(),
                        command_id: command.id,
                        service_id: service_id.clone(),
                        scope_id: scope_id.clone(),
                        causal_parents: vec![executing.updated_at, parent.updated_at],
                        changes: Vec::new(),
                    },
                    b"completed".to_vec(),
                )
                .unwrap();
        }
        let target = Node::in_memory();
        for event in source.events_after(None).unwrap() {
            target.ingest(event).unwrap();
        }
        let watch = target
            .command_states(CommandStateRequest {
                source_node: Some(source.node_id()),
                service_id: service_id.clone(),
                scope_id: scope_id.clone(),
                command_type: "test.command".to_owned(),
                snapshot_through: None,
                after_command_id: None,
                page_size: 32,
            })
            .unwrap()
            .watch_request()
            .unwrap();
        PendingCatalog {
            node: target,
            parent: parent_envelope,
            watch,
            commands: command_ids,
        }
    }

    #[tokio::test]
    async fn catalog_watch_emits_one_frame_for_two_commands_released_by_one_parent() {
        let fixture = pending_catalog();
        let session = FederatedSession::new(fixture.node.clone(), Arc::new(AllowAllAccessPolicy));
        let mut frames = session
            .open(
                PrincipalId::new("node:catalog-reader"),
                NodeRequestEnvelope::connected(NodeRequest::WatchCommands {
                    request: fixture.watch,
                }),
            )
            .await;
        assert!(
            matches!(frames.recv().await, Some(NodeFrame::Authorization { decision }) if matches!(*decision, AuthorizationDecision::Permit(_)))
        );
        assert!(matches!(
            frames.recv().await,
            Some(NodeFrame::CommandWatchReady { .. })
        ));
        fixture.node.ingest(fixture.parent).unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(1), frames.recv())
            .await
            .unwrap()
            .unwrap();
        let NodeFrame::CommandUpdate { update } = frame else {
            panic!("late parent did not release a command catalog batch");
        };
        assert_eq!(update.commands.len(), 2);
        assert!(
            update
                .commands
                .iter()
                .all(|entry| entry.command.state.is_committed())
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), frames.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn selected_follow_waits_at_unresolved_history_and_releases_it_after_the_parent() {
        let fixture = pending_catalog();
        let session = FederatedSession::new(fixture.node.clone(), Arc::new(AllowAllAccessPolicy));
        let mut frames = session
            .open(
                PrincipalId::new("node:catalog-reader"),
                NodeRequestEnvelope::connected(NodeRequest::FollowSelected {
                    selection: ReplicationSelection::All,
                    after: None,
                }),
            )
            .await;
        assert!(matches!(frames.recv().await, Some(NodeFrame::Hello { .. })));
        let Some(NodeFrame::SelectedBatch { batch: initial }) = frames.recv().await else {
            panic!("selected follow did not send its initial page");
        };
        assert_eq!(initial.events.len(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), frames.recv())
                .await
                .is_err()
        );
        fixture.node.ingest(fixture.parent).unwrap();
        let Some(NodeFrame::SelectedBatch { batch: released }) =
            tokio::time::timeout(Duration::from_secs(1), frames.recv())
                .await
                .unwrap()
        else {
            panic!("selected follow did not release the pending page");
        };
        assert_eq!(released.after, initial.through);
        assert_eq!(
            released
                .events
                .iter()
                .filter(|event| matches!(
                    event.event,
                    myko_federation::NodeEvent::CommandCommitted { .. }
                ))
                .count(),
            2
        );
        assert!(released.through > initial.through);
    }

    #[tokio::test]
    async fn scope_follow_advances_past_subtree_control_without_leaking_it() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let node = Node::in_memory();
            let principal = PrincipalId::new("node:scope-reader");
            let scope_id = ScopeId::new("scope:retained");
            node.admit(CommandRequest {
                id: CommandId::new(),
                service_id: ServiceId::new("test.service"),
                scope_id: scope_id.clone(),
                principal_id: principal.clone(),
                authority: AuthorityPresentation::direct_node(principal.clone()),
                resource_claims: vec![ResourceClaim::scope(
                    scope_id.clone(),
                    ResourceClaimKind::Primary,
                )],
                application_capabilities: Vec::new(),
                arguments_digest: None,
                command_type: "test.command".to_owned(),
                payload: Vec::new(),
            })
            .unwrap();
            let seed = node.events_after(None).unwrap().pop().unwrap();
            let snapshot = myko_federation::SelectedHistorySnapshot::current(&node).unwrap();
            let subtree_manifest = snapshot
                .retained_manifest(&ScopeSelection::Subtree(scope_id.clone()))
                .unwrap();
            let exact_manifest = snapshot
                .retained_manifest(&ScopeSelection::Exact(scope_id.clone()))
                .unwrap();
            let control = |origin_node, sequence, signer, signature, manifest| {
                let statement = RetainedHistoryStatement::new(
                    node.node_id(),
                    StorageIncarnationId::new(),
                    seed.origin,
                    manifest,
                )
                .unwrap();
                let signed =
                    SignedRetainedHistoryStatement::from_signature(statement, signer, signature);
                EventEnvelope {
                    position: LogPosition::FIRST,
                    origin: EventId::new(origin_node, LogPosition::new(sequence)),
                    recorded_at: chrono::Utc::now(),
                    event: myko_federation::NodeEvent::FrameworkControl(
                        FrameworkControlEvent::RetainedHistoryStatement(signed),
                    ),
                }
            };
            let statement_source = NodeId::new();
            let subtree_control = control(statement_source, 1, [1; 32], [1; 64], &subtree_manifest);
            let mut exact_control = control(statement_source, 2, [2; 32], [2; 64], &exact_manifest);
            node.ingest(subtree_control).unwrap();
            let myko_federation::IngestStatus::Applied { position } =
                node.ingest(exact_control.clone()).unwrap()
            else {
                panic!("exact control fixture was unexpectedly a duplicate");
            };
            exact_control.position = position;

            let session = FederatedSession::new(node.clone(), Arc::new(AllowAllAccessPolicy));
            let mut frames = session
                .open(
                    principal,
                    NodeRequestEnvelope::connected(NodeRequest::FollowScope {
                        scope_id: scope_id.clone(),
                        after: None,
                    }),
                )
                .await;
            assert!(matches!(
                frames.recv().await,
                Some(NodeFrame::Authorization { decision })
                    if matches!(*decision, AuthorizationDecision::Permit(_))
            ));
            assert!(matches!(frames.recv().await, Some(NodeFrame::Hello { .. })));

            let Some(NodeFrame::ScopedBatch { batch: seed_batch }) = frames.recv().await else {
                panic!("scope follow did not replay the seeded command");
            };
            assert_eq!(seed_batch.after, None);
            assert_eq!(seed_batch.events, vec![seed]);

            let Some(NodeFrame::ScopedBatch {
                batch: subtree_batch,
            }) = frames.recv().await
            else {
                panic!("scope follow did not advance past the subtree control record");
            };
            assert_eq!(subtree_batch.after, seed_batch.through);
            assert!(
                subtree_batch
                    .through
                    .is_some_and(|through| Some(through) > seed_batch.through)
            );
            assert!(subtree_batch.events.is_empty());

            let Some(NodeFrame::ScopedBatch { batch: exact_batch }) = frames.recv().await else {
                panic!("scope follow did not deliver the exact control record");
            };
            assert_eq!(exact_batch.after, subtree_batch.through);
            assert_eq!(exact_batch.events, vec![exact_control]);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn catalog_watch_filters_a_release_batch_and_refetches_after_authority_changes() {
        let fixture = pending_catalog();
        let watch = fixture.watch;
        let denied = *fixture.commands.get(1).unwrap();
        let session = FederatedSession::new(
            fixture.node.clone(),
            Arc::new(SelectiveCatalogPolicy { denied }),
        );
        let mut frames = session
            .open(
                PrincipalId::new("node:catalog-reader"),
                NodeRequestEnvelope::connected(NodeRequest::WatchCommands {
                    request: watch.clone(),
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

        fixture.node.ingest(fixture.parent).unwrap();
        let update = tokio::time::timeout(Duration::from_secs(1), frames.recv())
            .await
            .unwrap()
            .unwrap();
        let NodeFrame::CommandUpdate { update } = update else {
            panic!("late causal parent did not release a command-catalog batch");
        };
        assert_eq!(update.commands.len(), 1);
        assert!(
            update
                .commands
                .iter()
                .all(|entry| entry.command.state.is_committed()
                    && entry.command.request.id != denied)
        );
        session
            .set_access_policy(Arc::new(AllowAllAccessPolicy))
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), frames.recv()).await,
            Ok(None)
        ));
        let mut refetched = session
            .open(
                PrincipalId::new("node:catalog-reader"),
                NodeRequestEnvelope::connected(NodeRequest::CommandState {
                    request: CommandStateRequest {
                        source_node: Some(watch.source_node),
                        service_id: watch.service_id,
                        scope_id: watch.scope_id,
                        command_type: watch.command_type,
                        snapshot_through: None,
                        after_command_id: None,
                        page_size: 32,
                    },
                }),
            )
            .await;
        assert!(
            matches!(refetched.recv().await, Some(NodeFrame::Authorization { decision }) if matches!(*decision, AuthorizationDecision::Permit(_)))
        );
        assert!(
            matches!(refetched.recv().await, Some(NodeFrame::CommandState { page }) if page.commands.len() == 2)
        );
    }

    #[tokio::test]
    async fn catalog_watch_closes_when_visible_entry_authority_expires() {
        let node = Node::in_memory();
        let principal = PrincipalId::new("node:catalog-reader");
        let service_id = ServiceId::new("test.service");
        let scope_id = ScopeId::new("scope:catalog");
        let command_id = CommandId::new();
        node.admit(CommandRequest {
            id: command_id,
            service_id: service_id.clone(),
            scope_id: scope_id.clone(),
            principal_id: principal.clone(),
            authority: AuthorityPresentation::direct_node(principal.clone()),
            resource_claims: vec![ResourceClaim::scope(
                scope_id.clone(),
                ResourceClaimKind::Primary,
            )],
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: "test.command".to_owned(),
            payload: Vec::new(),
        })
        .unwrap();
        let session = FederatedSession::new(
            node.clone(),
            Arc::new(CatalogEntryPolicy {
                entries_allowed: true,
                entries_expire_at: Some(tokio::time::Instant::now() + Duration::from_millis(125)),
            }),
        );
        let mut frames = session
            .open(
                principal,
                NodeRequestEnvelope::connected(NodeRequest::WatchCommands {
                    request: myko_federation::CommandWatchRequest {
                        serving_node: node.node_id(),
                        source_node: node.node_id(),
                        service_id,
                        scope_id,
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
        assert!(matches!(
            frames.recv().await,
            Some(NodeFrame::CommandUpdate { update })
                if update.commands.iter().any(|entry| entry.command.request.id == command_id)
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), frames.recv()).await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn item_type_constraints_and_command_existence_are_fail_closed() {
        let node = Node::in_memory();
        let policy = Arc::new(CapturingPolicy::item_type("VisibleItem"));
        let session = FederatedSession::new(node.clone(), policy);
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
        let hidden = FederatedSession::new(node, Arc::new(DenyAllAccessPolicy));
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
