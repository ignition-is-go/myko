use super::*;

/// Pluggable atomic command log and subscription backend.
pub trait NodeBackend: Send + Sync + 'static {
    /// Returns this node's stable identity.
    fn node_id(&self) -> NodeId;

    /// Bind one proposer ballot to its full signed proposal before release.
    ///
    /// # Errors
    /// Rejects unavailable persistence, journal failure, and proposal conflicts.
    fn propose_control(
        &self,
        _request: &crate::control_quorum::ControlProposalRequest<'_>,
        _key: &ed25519_dalek::SigningKey,
    ) -> Result<crate::control_quorum::SignedControlProposal, NodeError> {
        Err(NodeError::DurableJournalRequired)
    }

    /// Serialize controller recovery and voting with the durable journal append.
    ///
    /// # Errors
    /// Rejects unavailable persistence, conflicting history, and superseded votes.
    fn vote_control(
        &self,
        _request: &crate::control_quorum::ControlVoteRequest<'_>,
        _key: &ed25519_dalek::SigningKey,
    ) -> Result<crate::control_quorum::SignedControlVote, NodeError> {
        Err(NodeError::DurableJournalRequired)
    }

    /// Returns the durable store identity, or `None` without a durable store.
    ///
    /// # Errors
    ///
    /// Returns an error if the store identity cannot be read.
    fn storage_incarnation(&self) -> Result<Option<StorageIncarnationId>, NodeError> {
        Ok(None)
    }

    /// Durably records an unverified retained-history assertion.
    ///
    /// Signature, obligation, membership, and custody validation remain the
    /// caller's responsibility.
    ///
    /// # Errors
    ///
    /// Returns an error when this backend has no durable journal or the
    /// statement does not match the store identity or supplied retained manifest.
    fn record_retained_history_statement(
        &self,
        _signed: SignedRetainedHistoryStatement,
        _manifest: &SelectedHistoryManifest,
    ) -> Result<EventEnvelope, NodeError> {
        Err(NodeError::DurableJournalRequired)
    }

    /// Durably submits a command without granting execution to the caller.
    ///
    /// # Errors
    ///
    /// Returns an error on storage failure or conflicting reuse of a command ID.
    fn submit(&self, request: CommandRequest) -> Result<CommandSnapshot, NodeError>;

    /// Atomically claims a submitted command for local execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown or backend state cannot be updated.
    fn claim(&self, command_id: CommandId) -> Result<CommandAdmission, NodeError>;

    /// Atomically admits a stable command or returns its existing lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an error on storage failure or conflicting reuse of a command ID.
    fn admit(&self, request: CommandRequest) -> Result<CommandAdmission, NodeError>;

    /// Atomically commits the command result and its complete change batch.
    ///
    /// # Errors
    ///
    /// Returns an error when the command cannot be committed atomically.
    fn commit(
        &self,
        command_id: CommandId,
        batch: ChangeBatch,
        result: Vec<u8>,
    ) -> Result<CommandSnapshot, NodeError>;

    /// Durably freezes one computed handler effect before policy evaluation.
    ///
    /// An exact retry returns the retained prepared state; a different body for
    /// the same command is rejected.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is absent, no longer executable, or
    /// the prepared body does not match its admitted command.
    fn prepare_authorization(
        &self,
        command_id: CommandId,
        effect: PreparedCommandEffect,
    ) -> Result<CommandSnapshot, NodeError>;

    /// Commits an already prepared effect by digest after authorization permits it.
    ///
    /// # Errors
    ///
    /// Returns an error when the command has no matching prepared effect.
    fn commit_prepared_authorization(
        &self,
        command_id: CommandId,
        effect_digest: &str,
    ) -> Result<CommandSnapshot, NodeError>;

    /// Parks an already prepared effect behind a durable challenge.
    ///
    /// # Errors
    ///
    /// Returns an error when the command has no matching prepared effect or a
    /// different challenge already owns it.
    fn await_prepared_authorization(
        &self,
        command_id: CommandId,
        effect_digest: &str,
        challenge_id: ChallengeId,
    ) -> Result<CommandSnapshot, NodeError>;

    /// Rejects an executing command before any authoritative change is committed.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is absent or no longer executing.
    fn reject(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError>;

    /// Releases an executing command for a later attempt after a transient
    /// handler failure.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is absent, no longer executing, or
    /// the retry lifecycle cannot be durably appended.
    fn retry(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError>;

    /// Resubmits a parked command with the approval that released it.
    ///
    /// # Errors
    ///
    /// Returns an error if the challenge no longer owns the parked command.
    fn resume_authorization(
        &self,
        command_id: CommandId,
        challenge_id: &ChallengeId,
        approval_id: ApprovalId,
    ) -> Result<CommandSnapshot, NodeError>;

    /// Advances a parked exact effect to its next required challenge without
    /// rerunning the application handler.
    ///
    /// # Errors
    ///
    /// Returns an error if the current challenge cannot be durably advanced.
    fn advance_authorization(
        &self,
        command_id: CommandId,
        challenge_id: &ChallengeId,
        next_challenge_id: ChallengeId,
        approval_id: ApprovalId,
    ) -> Result<CommandSnapshot, NodeError>;

    /// Cancels submitted or executing work without committing graph changes.
    ///
    /// A terminal command is returned unchanged, making repeated cancellation
    /// idempotent and allowing callers to detect a commit that won the race.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is absent or storage cannot append the
    /// terminal lifecycle event.
    fn cancel(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError>;

    /// Reads the current lifecycle of a command.
    ///
    /// # Errors
    ///
    /// Returns an error when backend state cannot be read.
    fn command(&self, command_id: CommandId) -> Result<Option<CommandSnapshot>, NodeError>;

    /// Returns the node that originated a command's first immutable event.
    ///
    /// Backends may override this history-derived default with an index.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    fn command_origin(&self, command_id: CommandId) -> Result<Option<NodeId>, NodeError> {
        Ok(self.events_after(None)?.into_iter().find_map(|envelope| {
            let command = match envelope.event {
                NodeEvent::CommandLifecycle(command)
                | NodeEvent::CommandCommitted { command, .. } => command,
                NodeEvent::FrameworkControl(_) => return None,
            };
            (command.request.id == command_id).then_some(envelope.origin.node_id)
        }))
    }

    /// Reads immutable events after an exclusive cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    fn events_after(&self, after: Option<LogPosition>) -> Result<Vec<EventEnvelope>, NodeError>;

    /// Returns dependency-complete events in causal order at one local log cut.
    ///
    /// A parent received after `through` cannot release a child in this snapshot.
    ///
    /// # Errors
    /// Returns an error when history is unavailable or causally invalid.
    fn causal_events_through(&self, through: LogPosition) -> Result<Vec<EventEnvelope>, NodeError> {
        let history = self
            .events_after(None)?
            .into_iter()
            .filter(|event| event.position <= through)
            .collect::<Vec<_>>();
        Ok(crate::causal::causal_replay(&history)?
            .into_iter()
            .cloned()
            .collect())
    }

    /// Reads at most `limit` immutable events after an exclusive cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    fn events_page(
        &self,
        after: Option<LogPosition>,
        limit: NonZeroUsize,
    ) -> Result<Vec<EventEnvelope>, NodeError>;

    /// Returns the latest immutable event position without replaying history.
    ///
    /// # Errors
    ///
    /// Returns an error when backend state cannot be read.
    fn latest_position(&self) -> Result<Option<LogPosition>, NodeError>;

    /// Reads one stable sorted page of observed scope IDs.
    ///
    /// # Errors
    ///
    /// Returns an error when backend state cannot be read.
    fn scope_ids_page(
        &self,
        after: Option<&ScopeId>,
        limit: NonZeroUsize,
    ) -> Result<Vec<ScopeId>, NodeError>;

    /// Returns the latest local log position committed in one exact typed
    /// service scope.
    ///
    /// This is the authoritative frontier used to prove that a reactive
    /// projection has caught up before it participates in a decision. Backends
    /// should answer from an index rather than replaying history.
    #[doc(hidden)]
    fn service_scope_position(
        &self,
        service_id: &ServiceId,
        scope_id: &ScopeId,
    ) -> Result<Option<LogPosition>, NodeError>;

    /// Subscribes without a replay/live race. The cursor is exclusive.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot establish the subscription.
    fn subscribe(&self, after: Option<LogPosition>) -> Result<EventSubscription, NodeError>;

    /// Returns scope topology established by dependency-complete history.
    /// Retained events with missing ancestors must not establish authority.
    ///
    /// # Errors
    ///
    /// Returns an error when backend state cannot be read.
    #[doc(hidden)]
    fn scope_topology(&self) -> Result<ScopeTopology, NodeError> {
        let Some(through) = self.latest_position()? else {
            return Ok(ScopeTopology::default());
        };
        ScopeTopology::from_events(&self.causal_events_through(through)?)
    }

    /// Idempotently ingests an immutable event received from another node.
    ///
    /// # Errors
    ///
    /// Returns an error when replicated history conflicts with an existing
    /// stable command ID or contains an invalid change batch.
    fn ingest(&self, event: EventEnvelope) -> Result<IngestStatus, NodeError>;
}

/// Cloneable application handle to a transport- and storage-neutral node.
#[derive(Clone)]
pub struct Node {
    backend: Arc<dyn NodeBackend>,
    readiness: Arc<NodeReadiness>,
    command_dispatch: Arc<ReentrantMutex<()>>,
    command_access_policy: Arc<RwLock<Option<Weak<dyn AccessPolicy>>>>,
    replication_coverage: Arc<RwLock<HashMap<(NodeId, ReplicationSelection), ReplicationCoverage>>>,
    replication_resumptions: SharedReplicationPositions,
    replication_sources: Arc<RwLock<HashMap<NodeId, ReplicationSourceAvailability>>>,
}

type ReplicationPositions = HashMap<(NodeId, ReplicationSelection), Option<LogPosition>>;
type SharedReplicationPositions = Arc<RwLock<ReplicationPositions>>;

#[derive(Clone, Copy)]
struct ReplicationCoverage {
    source_through: Option<LogPosition>,
    local_cut: Option<LogPosition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationSourceAvailability {
    Reachable,
    Unreachable,
    Undiscoverable,
}

/// A command whose complete declared claims were authorized before handler
/// execution. Private fields prevent callers from manufacturing preflight.
pub struct PreparedCommand {
    node: Node,
    request: CommandRequest,
    permit: PermitDecision,
}

impl PreparedCommand {
    #[must_use]
    pub const fn permit(&self) -> &PermitDecision {
        &self.permit
    }

    /// Durably submits the exact request that produced this preflight permit.
    ///
    /// # Errors
    ///
    /// Returns an error when durable submission fails or conflicts with an
    /// existing command identity.
    pub fn submit(self) -> Result<CommandSnapshot, NodeError> {
        self.node.backend.submit(self.request)
    }
}

#[derive(Debug, Default)]
struct NodeReadiness {
    startup_gates: AtomicUsize,
    waiters: Mutex<Vec<Waker>>,
}

/// RAII ownership of one unfinished node-startup phase.
///
/// Every transport waits until all startup gates have been released before it
/// serves application or federation requests. Dropping the guard releases its
/// phase, including during error unwinding.
#[derive(Debug)]
pub struct NodeStartupGuard {
    readiness: Arc<NodeReadiness>,
    released: bool,
}

impl NodeStartupGuard {
    /// Completes this startup phase and wakes transports when it was the last.
    pub fn ready(mut self) {
        self.release();
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        if self.readiness.startup_gates.fetch_sub(1, Ordering::AcqRel) == 1 {
            let mut registered = self
                .readiness
                .waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let waiters = std::mem::take(&mut *registered);
            drop(registered);
            for waiter in waiters {
                waiter.wake();
            }
        }
    }
}

impl Drop for NodeStartupGuard {
    fn drop(&mut self) {
        self.release();
    }
}

struct NodeReadyFuture {
    readiness: Arc<NodeReadiness>,
}

impl Future for NodeReadyFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.readiness.startup_gates.load(Ordering::Acquire) == 0 {
            return Poll::Ready(());
        }
        let mut waiters = self
            .readiness
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.readiness.startup_gates.load(Ordering::Acquire) == 0 {
            return Poll::Ready(());
        }
        if !waiters
            .iter()
            .any(|waiter| waiter.will_wake(context.waker()))
        {
            waiters.push(context.waker().clone());
        }
        Poll::Pending
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Node")
            .field("node_id", &self.node_id())
            .finish_non_exhaustive()
    }
}

/// A typed application command plus Myko-owned admission metadata.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct DeclaredCommand<C: MykoCommand> {
    pub(super) id: CommandId,
    pub(super) scope_id: ScopeId,
    pub(super) principal_id: PrincipalId,
    pub(super) body: C,
}

#[cfg(test)]
impl<C: MykoCommand> DeclaredCommand<C> {
    /// Creates a typed command ready for submission through any transport.
    #[must_use]
    pub const fn new(id: CommandId, scope_id: ScopeId, principal_id: PrincipalId, body: C) -> Self {
        Self {
            id,
            scope_id,
            principal_id,
            body,
        }
    }

    /// Encodes the declared command into the transport-neutral request shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the typed body cannot be serialized.
    pub fn request(&self) -> Result<CommandRequest, NodeError> {
        let authority = AuthorityPresentation::direct_node(self.principal_id.clone());
        Ok(CommandRequest {
            id: self.id,
            service_id: ServiceId::new(C::SERVICE_ID),
            scope_id: self.scope_id.clone(),
            principal_id: self.principal_id.clone(),
            authority,
            resource_claims: vec![ResourceClaim {
                selection: ScopeSelection::Exact(self.scope_id.clone()),
                kind: ResourceClaimKind::Primary,
                source_node: None,
                service_id: Some(ServiceId::new(C::SERVICE_ID)),
                item_type: None,
                item_id: None,
                required_permissions: vec![FederationPermission::Write],
                required_operations: vec![AccessOperation::SubmitCommand],
                required_capabilities: Vec::new(),
            }],
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: C::COMMAND_TYPE.to_owned(),
            payload: serde_json::to_vec(&self.body)
                .map_err(|error| NodeError::CommandEncoding(error.to_string()))?,
        })
    }
}

/// Boxed transport-neutral command operation.
pub type CommandClientFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CommandResponse, E>> + Send + 'a>>;

/// Boxed update from a transport-neutral command lifecycle subscription.
pub type CommandSubscriptionFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CommandSnapshot, E>> + Send + 'a>>;

/// Boxed setup of one transport-neutral command lifecycle subscription.
pub type CommandWatchFuture<'a, S, E> = Pin<Box<dyn Future<Output = Result<S, E>> + Send + 'a>>;

/// Boxed typed completion of one submitted application command.
pub type TypedCommandClientFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Common command surface implemented by embedded and remote node clients.
///
/// Applications submit and observe the same stable command contract whether
/// the endpoint is in-process, native Iroh, or an optional short-lived edge
/// adapter. Claiming and execution are intentionally absent from this client
/// interface.
pub trait CommandClient: Send + Sync {
    type Error: From<NodeError> + Send + 'static;

    /// Durably submits one transport-authenticated wire value without claiming execution.
    #[doc(hidden)]
    fn submit_submission(&self, command: CommandSubmission)
    -> CommandClientFuture<'_, Self::Error>;

    /// Reads the current durable lifecycle for a stable command ID.
    fn command_state(&self, command_id: CommandId) -> CommandClientFuture<'_, Self::Error>;

    /// Durably cancels submitted or executing work.
    fn cancel_command(
        &self,
        command_id: CommandId,
        reason: String,
    ) -> CommandClientFuture<'_, Self::Error>;

    /// Submits a typed application command without exposing its wire envelope.
    #[doc(hidden)]
    fn submit_typed_command<C>(&self, command: C) -> CommandClientFuture<'_, Self::Error>
    where
        Self: Sized,
        C: MykoCommand,
    {
        let submission = CommandSubmission::for_command(&command).map_err(Self::Error::from);
        Box::pin(async move { self.submit_submission(submission?).await })
    }
}

/// Current-then-live command lifecycle independent of its transport.
pub trait CommandSubscription: Send {
    type Error: From<NodeError> + Send + 'static;

    /// Returns the latest coherently observed durable state.
    fn current(&self) -> &CommandSnapshot;

    /// Waits for the next durable lifecycle transition.
    fn recv(&mut self) -> CommandSubscriptionFuture<'_, Self::Error>;
}

/// Command client that can watch one command through its typed result.
///
/// The default execution helper owns admission/watch races and typed result
/// decoding so application clients never inspect command IDs, wire results, or
/// lifecycle variants.
pub trait CommandWatchingClient: CommandClient {
    type Subscription: CommandSubscription<Error = Self::Error>;

    /// Opens a gap-free current-then-live lifecycle subscription.
    fn watch_command(
        &self,
        command_id: CommandId,
    ) -> CommandWatchFuture<'_, Self::Subscription, Self::Error>;

    /// Opens a command lifecycle at the authoritative node returned by
    /// admission.
    ///
    /// Direct clients already terminate at that node and may use the default
    /// implementation. Routed edge clients override this method so an
    /// automatically placed command remains on the same node for its complete
    /// lifecycle.
    #[doc(hidden)]
    fn watch_command_at(
        &self,
        _source_node: NodeId,
        command_id: CommandId,
    ) -> CommandWatchFuture<'_, Self::Subscription, Self::Error> {
        self.watch_command(command_id)
    }

    /// Submits a command and watches it until its typed result is durable.
    #[doc(hidden)]
    fn exec_typed_command<C>(
        &self,
        command: C,
    ) -> TypedCommandClientFuture<'_, C::Output, Self::Error>
    where
        Self: Sized,
        C: MykoCommand,
    {
        let submission = CommandSubmission::for_command(&command).map_err(Self::Error::from);
        Box::pin(async move {
            let submission = submission?;
            let command_id = submission.id;
            let response = self.submit_submission(submission).await?;
            let current = response
                .command
                .ok_or_else(|| Self::Error::from(NodeError::UnknownCommand(command_id)))?;
            if let Some(result) = current.typed_completion::<C>().map_err(Self::Error::from)? {
                return Ok(result);
            }
            let mut subscription = self
                .watch_command_at(response.source_node, command_id)
                .await?;
            loop {
                if let Some(result) = subscription
                    .current()
                    .typed_completion::<C>()
                    .map_err(Self::Error::from)?
                {
                    return Ok(result);
                }
                let _updated = subscription.recv().await?;
            }
        })
    }
}

impl CommandSubscription for CommandWatch {
    type Error = NodeError;

    fn current(&self) -> &CommandSnapshot {
        Self::current(self)
    }

    fn recv(&mut self) -> CommandSubscriptionFuture<'_, Self::Error> {
        Box::pin(self.recv_async())
    }
}

/// Boxed transport-neutral command-catalog page operation.
pub type CommandStatePageFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CommandStatePage, E>> + Send + 'a>>;

/// Boxed transport-neutral complete command-catalog operation.
pub type CommandStateFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CommandStateSnapshot, E>> + Send + 'a>>;

/// Common bounded command-catalog surface for embedded and remote clients.
pub trait CommandStateClient: Send + Sync {
    type Error: From<NodeError> + Send + 'static;

    /// Reads one bounded page of current command lifecycle state.
    fn command_state_page(
        &self,
        request: CommandStateRequest,
    ) -> CommandStatePageFuture<'_, Self::Error>;

    /// Reads every page of one cursor-stable current command catalog.
    fn command_states(&self, request: CommandStateRequest) -> CommandStateFuture<'_, Self::Error>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let first = self.command_state_page(request).await?;
            let (mut snapshot, mut next) =
                CommandStateSnapshot::from_first_page(first).map_err(Self::Error::from)?;
            while let Some(request) = next {
                let page = self.command_state_page(request.clone()).await?;
                next = snapshot
                    .append_page(&request, page)
                    .map_err(Self::Error::from)?;
            }
            Ok(snapshot)
        })
    }
}

impl CommandStateClient for Node {
    type Error = NodeError;

    fn command_state_page(
        &self,
        request: CommandStateRequest,
    ) -> CommandStatePageFuture<'_, Self::Error> {
        Box::pin(std::future::ready(Self::command_state_page(self, request)))
    }
}

/// Boxed transport-neutral current-state page operation.
pub type ItemStatePageFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<ItemStatePage, E>> + Send + 'a>>;

/// Boxed transport-neutral complete current-state operation.
pub type ItemStateFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<ItemStateSnapshot, E>> + Send + 'a>>;

/// Boxed transport-neutral typed current-state operation.
pub type TypedItemClientFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<ItemQuerySnapshot<T>, E>> + Send + 'a>>;

/// Common one-shot typed state surface for embedded and remote node clients.
///
/// Lossless watches still use replication/follow streams. This facade gives
/// short-lived clients a bounded current projection without importing or
/// decoding application history.
pub trait ItemClient: Send + Sync {
    type Error: From<NodeError> + Send + 'static;

    /// Reads one bounded schema-specific current-state page.
    fn item_state_page(&self, request: ItemStateRequest) -> ItemStatePageFuture<'_, Self::Error>;

    /// Reads every page of one cursor-stable current-state snapshot.
    fn item_state(&self, request: ItemStateRequest) -> ItemStateFuture<'_, Self::Error>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let first = self.item_state_page(request).await?;
            let (mut snapshot, mut next) =
                ItemStateSnapshot::from_first_page(first).map_err(Self::Error::from)?;
            while let Some(request) = next {
                let page = self.item_state_page(request.clone()).await?;
                next = snapshot
                    .append_page(&request, page)
                    .map_err(Self::Error::from)?;
            }
            Ok(snapshot)
        })
    }

    /// Reads and executes a generated typed query through the common client.
    fn query_items<'a, Q>(
        &'a self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> TypedItemClientFuture<'a, ItemQueryResult<Q>, Self::Error>
    where
        Self: Sized,
        Q: ItemQuery + Send + 'a,
        Q::Item: Send,
        ItemQueryResult<Q>: Send + 'a,
    {
        let request = ItemStateRequest::for_item::<Q::Item>(source_node, scope_id);
        Box::pin(async move {
            self.item_state(request)
                .await?
                .query(query)
                .map_err(Self::Error::from)
        })
    }

    /// Reads the serving node's authoritative items and executes a typed query.
    fn query_serving_items<'a, Q>(
        &'a self,
        scope_id: ScopeId,
        query: Q,
    ) -> TypedItemClientFuture<'a, ItemQueryResult<Q>, Self::Error>
    where
        Self: Sized,
        Q: ItemQuery + Send + 'a,
        Q::Item: Send,
        ItemQueryResult<Q>: Send + 'a,
    {
        let request = ItemStateRequest::for_serving_item::<Q::Item>(scope_id);
        Box::pin(async move {
            self.item_state(request)
                .await?
                .query(query)
                .map_err(Self::Error::from)
        })
    }
}

impl ItemClient for Node {
    type Error = NodeError;

    fn item_state_page(&self, request: ItemStateRequest) -> ItemStatePageFuture<'_, Self::Error> {
        Box::pin(std::future::ready(Self::item_state_page(self, request)))
    }
}

/// Result of claiming a locally originated declared command.
#[derive(Debug)]
pub enum DeclaredCommandAdmission<C: MykoCommand> {
    /// Execute the decoded application command exactly once.
    Execute(DeclaredCommandContext<C>),
    /// The command already has a durable lifecycle; do not execute it again.
    Resume(CommandSnapshot),
}

/// How one pending declared command was resolved by framework dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandDispatchDisposition {
    /// The application handler emitted and committed its typed result.
    Committed,
    /// The framework durably rejected a malformed body or handler failure.
    Rejected,
    /// The handler durably released a transient failure for another attempt.
    Retrying,
    /// Another claimant already advanced the command lifecycle.
    Resumed,
}

impl CommandDispatchDisposition {
    const fn for_resumed_state(state: &CommandState) -> Self {
        match state {
            CommandState::Rejected { .. } | CommandState::Cancelled { .. } => Self::Rejected,
            CommandState::Retrying { .. }
            | CommandState::AuthorizationPrepared { .. }
            | CommandState::AuthorizationPending { .. } => Self::Retrying,
            _ => Self::Resumed,
        }
    }
}

/// Application-selected lifecycle for a declared handler failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandHandlerError {
    /// The command is invalid for the domain and must become terminal.
    Reject(String),
    /// The command remains valid but a transient dependency is unavailable.
    Retry(String),
}

impl CommandHandlerError {
    #[must_use]
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject(reason.into())
    }

    #[must_use]
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry(reason.into())
    }
}

impl From<String> for CommandHandlerError {
    fn from(reason: String) -> Self {
        Self::Reject(reason)
    }
}

/// Durable outcome of dispatching one pending declared command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandDispatchResult {
    pub command: CommandSnapshot,
    pub disposition: CommandDispatchDisposition,
}

impl CommandDispatchResult {
    /// Returns the framework-owned stable identity without exposing its wire request.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command.request.id
    }

    /// Returns the current durable lifecycle without exposing its wire request.
    #[must_use]
    pub const fn state(&self) -> &CommandState {
        &self.command.state
    }
}

/// Myko-owned execution context paired with a decoded command body.
#[derive(Debug)]
pub struct DeclaredCommandContext<C: MykoCommand> {
    inner: CommandContext,
    body: C,
}

impl<C: MykoCommand> DeclaredCommandContext<C> {
    /// Returns the decoded application command body.
    #[must_use]
    pub const fn body(&self) -> &C {
        &self.body
    }

    /// Returns a cloneable atomic command capability substrate.
    #[doc(hidden)]
    #[must_use]
    pub const fn command_context(&self) -> &CommandContext {
        &self.inner
    }

    /// Returns the immutable Myko request metadata.
    #[must_use]
    pub const fn request(&self) -> &CommandRequest {
        self.inner.request()
    }

    /// Adds a typed item replacement to this command's atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item cannot be encoded.
    pub fn emit_set<T: MykoItem>(&mut self, item: &T) -> Result<(), NodeError> {
        self.inner.emit_set(item)
    }

    /// Adds a typed item deletion to this command's atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item belongs to another service.
    pub fn emit_delete<T: MykoItem>(&mut self, id: &T::Id) -> Result<(), NodeError> {
        self.inner.emit_delete::<T>(id)
    }

    /// Queries typed current state in this command's service and scope.
    ///
    /// # Errors
    ///
    /// Returns an error if the typed projection cannot be materialized.
    pub fn query<Q: ItemQuery>(&self, query: Q) -> Result<ItemQueryResult<Q>, NodeError> {
        self.inner.query(query)
    }

    /// Atomically commits emitted items and this command's declared result.
    ///
    /// # Errors
    ///
    /// Returns an error if result encoding or durable commit fails.
    pub fn commit(self, result: &C::Output) -> Result<CommandSnapshot, NodeError> {
        self.inner.commit(result)
    }

    /// Rejects the command without committing emitted items.
    ///
    /// # Errors
    ///
    /// Returns an error if durable rejection fails.
    pub fn reject(self, reason: impl Into<String>) -> Result<CommandSnapshot, NodeError> {
        self.inner.reject(reason)
    }

    /// Releases a transient failure for another ordered dispatch attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the retry state cannot be durably appended.
    pub fn retry(self, reason: impl Into<String>) -> Result<CommandSnapshot, NodeError> {
        self.inner.retry(reason)
    }
}

pub(super) fn decode_declared_body<C: MykoCommand>(
    request: &CommandRequest,
) -> Result<C, NodeError> {
    if request.service_id != C::SERVICE_ID || request.command_type != C::COMMAND_TYPE {
        return Err(NodeError::CommandSchemaMismatch {
            expected_service: C::SERVICE_ID.as_str(),
            expected_command: C::COMMAND_TYPE,
            actual_service: request.service_id.as_str().to_owned(),
            actual_command: request.command_type.clone(),
        });
    }
    serde_json::from_slice(&request.payload)
        .map_err(|error| NodeError::CommandDecoding(error.to_string()))
}

pub(super) fn decode_typed_command_state<C: MykoCommand>(
    entry: &CommandStateEntry,
) -> Result<TypedCommandState<C>, NodeError> {
    let command = entry.command.request.command::<C>()?;
    let result = entry
        .command
        .result
        .as_deref()
        .map(serde_json::from_slice)
        .transpose()
        .map_err(|error| NodeError::ResultDecoding(error.to_string()))?;
    Ok(TypedCommandState {
        admitted_at: entry.admitted_at,
        last_changed_at: entry.last_changed_at,
        id: entry.command.request.id,
        scope_id: entry.command.request.scope_id.clone(),
        principal_id: entry.command.request.principal_id.clone(),
        command,
        state: entry.command.state.clone(),
        result,
        updated_at: entry.command.updated_at,
    })
}

/// Result of claiming a command through Myko's typed application boundary.
#[derive(Debug)]
pub enum TypedCommandAdmission {
    /// This node owns execution and has created an atomic item context.
    Execute(CommandContext),
    /// The command already has a durable lifecycle; do not execute it again.
    Resume(CommandSnapshot),
}

fn item_query_claims<Q>(
    selection: ScopeSelection,
    source_node: Option<NodeId>,
    query: &Q,
) -> Vec<ResourceClaim>
where
    Q: ItemQuery,
{
    let Some(item_ids) = query.selected_item_ids() else {
        return vec![item_query_claim::<Q>(selection, source_node, None)];
    };
    item_ids
        .iter()
        .map(|item_id| {
            item_query_claim::<Q>(selection.clone(), source_node, Some(item_id.as_ref()))
        })
        .collect()
}

fn item_query_claim<Q>(
    selection: ScopeSelection,
    source_node: Option<NodeId>,
    item_id: Option<&str>,
) -> ResourceClaim
where
    Q: ItemQuery,
{
    ResourceClaim {
        selection,
        kind: ResourceClaimKind::Referenced,
        source_node,
        service_id: Some(ServiceId::new(Q::Item::SERVICE_ID)),
        item_type: Some(Q::Item::ITEM_TYPE.to_owned()),
        item_id: item_id.map(ToOwned::to_owned),
        required_permissions: vec![FederationPermission::ReadState],
        required_operations: vec![AccessOperation::ReadItems],
        required_capabilities: Vec::new(),
    }
}

/// Atomic application command context owned by Myko.
///
/// Handlers emit typed item sets/deletes and a typed result. Myko supplies the
/// batch identity, service/scope identity, causal parent, serialization,
/// validation, and durable commit.
#[derive(Debug, Clone)]
pub struct CommandContext {
    node: Node,
    command: CommandSnapshot,
    authorization: CommandAuthorization,
    changes: Arc<Mutex<Vec<ItemMutation>>>,
    actual_claims: Arc<Mutex<Vec<ResourceClaim>>>,
    actual_capabilities: Arc<Mutex<Vec<CapabilityId>>>,
    causal_reads: Arc<Mutex<HashSet<EventId>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandAuthorization {
    Enforce,
    TrustedFramework,
}

impl CommandContext {
    /// Returns the immutable request being executed.
    #[must_use]
    pub const fn request(&self) -> &CommandRequest {
        &self.command.request
    }

    /// Returns the node executing this command.
    #[doc(hidden)]
    #[must_use]
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// Returns how many typed mutations this command has emitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the shared atomic mutation batch is unavailable.
    pub fn change_count(&self) -> Result<usize, NodeError> {
        self.changes
            .lock()
            .map(|changes| changes.len())
            .map_err(|_| NodeError::Poisoned)
    }

    /// Adds a typed item replacement to this command's atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item cannot be encoded.
    pub fn emit_set<T: MykoItem>(&self, item: &T) -> Result<(), NodeError> {
        self.emit_set_in(&self.command.request.scope_id, item)
    }

    /// Adds a typed replacement placed in an explicit scope of this service batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item cannot be encoded or belongs to another service.
    pub fn emit_set_in<T: MykoItem>(&self, scope_id: &ScopeId, item: &T) -> Result<(), NodeError> {
        self.require_item_service::<T>()?;
        self.record_actual_claim(ResourceClaim {
            selection: ScopeSelection::Exact(scope_id.clone()),
            kind: ResourceClaimKind::Affected,
            source_node: Some(self.node.node_id()),
            service_id: Some(ServiceId::new(T::SERVICE_ID)),
            item_type: Some(T::ITEM_TYPE.to_owned()),
            item_id: Some(item.item_id().as_ref().to_owned()),
            required_permissions: vec![FederationPermission::Write],
            required_operations: Vec::new(),
            required_capabilities: Vec::new(),
        })?;
        let mut mutation = ItemMutation::set(item)
            .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?;
        mutation.scope_id = Some(scope_id.as_str().to_owned());
        self.changes
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .push(mutation);
        Ok(())
    }

    /// Adds a typed item deletion to this command's atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item belongs to another service.
    pub fn emit_delete<T: MykoItem>(&self, id: &T::Id) -> Result<(), NodeError> {
        self.emit_delete_in::<T>(&self.command.request.scope_id, id)
    }

    /// Adds a typed deletion placed in an explicit scope of this service batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item belongs to another service.
    pub fn emit_delete_in<T: MykoItem>(
        &self,
        scope_id: &ScopeId,
        id: &T::Id,
    ) -> Result<(), NodeError> {
        self.require_item_service::<T>()?;
        self.record_actual_claim(ResourceClaim {
            selection: ScopeSelection::Exact(scope_id.clone()),
            kind: ResourceClaimKind::Affected,
            source_node: Some(self.node.node_id()),
            service_id: Some(ServiceId::new(T::SERVICE_ID)),
            item_type: Some(T::ITEM_TYPE.to_owned()),
            item_id: Some(id.as_ref().to_owned()),
            required_permissions: vec![FederationPermission::Write],
            required_operations: Vec::new(),
            required_capabilities: Vec::new(),
        })?;
        let mut mutation = ItemMutation::delete::<T>(id);
        mutation.scope_id = Some(scope_id.as_str().to_owned());
        self.changes
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .push(mutation);
        Ok(())
    }

    /// Reads a logical scope from a fixed local history cut. Observed batches
    /// in the command's own atomic scope become required replay dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error if current typed state cannot be materialized.
    #[allow(clippy::needless_pass_by_value)] // Typed query APIs accept an owned one-shot request.
    pub fn query<Q>(&self, query: Q) -> Result<ItemQueryResult<Q>, NodeError>
    where
        Q: ItemQuery,
    {
        for claim in item_query_claims(
            ScopeSelection::Exact(self.command.request.scope_id.clone()),
            None,
            &query,
        ) {
            self.record_actual_claim(claim)?;
        }
        let (_, history) = self.node.causal_snapshot()?;
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        let service_scope = Some((&service_id, &self.command.request.scope_id));
        let projection = project_item_history(&history, None, service_scope)?;
        self.causal_reads
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .extend(
                history
                    .iter()
                    .filter(|event| {
                        matches!(event.event, NodeEvent::CommandCommitted { .. })
                            // Output-only replicas must not need a foreign service's private history.
                            && command_from_event(&event.event).is_some_and(|command| {
                                command.request.service_id == self.command.request.service_id
                            })
                            && event.event.affected_scope_ids().iter().any(|scope| {
                                scope.equivalent_to(&self.command.request.scope_id)
                            })
                            && item_history_scope_matches::<Q::Item>(event, service_scope)
                    })
                    .map(|event| event.origin),
            );
        Ok(__snapshot_item_query(&query, &projection))
    }

    /// Executes a command-safe authoritative exact/subtree query after
    /// validating the selected read against preflight claims.
    ///
    /// # Errors
    ///
    /// Returns an error when the selection was not declared, current history
    /// is invalid, or the typed projection cannot be evaluated.
    #[allow(clippy::needless_pass_by_value)] // Mirrors the other owned typed query capabilities.
    pub fn query_selected<Q>(
        &self,
        selection: ScopeSelection,
        query: Q,
    ) -> Result<ItemQueryResult<Q>, NodeError>
    where
        Q: ItemQuery,
    {
        for claim in item_query_claims(selection.clone(), None, &query) {
            self.record_actual_claim(claim)?;
        }
        let (_, history) = self.node.causal_snapshot()?;
        let topology = ScopeTopology::from_events(&history)?;
        let mut projection = ItemProjection::default();
        let mut observed = Vec::new();
        for (index, envelope) in history.iter().enumerate() {
            let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
                continue;
            };
            if command.request.service_id != Q::Item::SERVICE_ID {
                continue;
            }
            let _changed = apply_selected_item_envelope(
                &mut projection,
                envelope,
                None,
                &selection,
                None,
                &topology,
                projection_revision(index)?,
            )?;
            if command.request.service_id == self.command.request.service_id
                && envelope
                    .event
                    .affected_scope_ids()
                    .iter()
                    .any(|scope| scope.equivalent_to(&self.command.request.scope_id))
                && batch.changes.iter().any(|mutation| {
                    item_mutation_scope::<Q::Item>(mutation, &batch.scope_id)
                        .is_some_and(|scope| selection.contains_scope(&scope, &topology))
                })
            {
                observed.push(envelope.origin);
            }
        }
        self.causal_reads
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .extend(observed);
        Ok(__snapshot_item_query(&query, &projection))
    }

    fn require_item_service<T: MykoItem>(&self) -> Result<(), NodeError> {
        if self.command.request.service_id != T::SERVICE_ID {
            return Err(NodeError::ItemServiceMismatch {
                command_service: self.command.request.service_id.as_str().to_owned(),
                item_service: T::SERVICE_ID.as_str(),
            });
        }
        Ok(())
    }

    /// Records a handler-observed claim after proving it was declared during
    /// preflight. This check happens before the read or mutation is performed.
    #[doc(hidden)]
    pub fn record_actual_claim(&self, claim: ResourceClaim) -> Result<(), NodeError> {
        let topology = self.node.scope_topology()?;
        if !self
            .command
            .request
            .resource_claims
            .iter()
            .any(|declared| declared.covers_actual(&claim, &topology))
        {
            return Err(NodeError::AuthorizationDenied(format!(
                "{} handler used undeclared claim {claim:?}",
                self.command.request.command_type
            )));
        }
        let mut actual = self.actual_claims.lock().map_err(|_| NodeError::Poisoned)?;
        if !actual.contains(&claim) {
            actual.push(claim);
        }
        drop(actual);
        Ok(())
    }

    /// Verifies a nested handler's declared authority before entering it,
    /// without treating a potential claim as an actual effect.
    #[doc(hidden)]
    pub fn validate_declared_claim(&self, claim: &ResourceClaim) -> Result<(), NodeError> {
        let topology = self.node.scope_topology()?;
        if self
            .command
            .request
            .resource_claims
            .iter()
            .any(|declared| declared.covers_declared(claim, &topology))
        {
            Ok(())
        } else {
            Err(NodeError::AuthorizationDenied(format!(
                "nested handler under {} declared authority outside outer preflight: {claim:?}",
                self.command.request.command_type
            )))
        }
    }

    /// Records an opaque application capability before nested execution. The
    /// outer command must have declared it during transport preflight.
    #[doc(hidden)]
    pub fn record_actual_capability(&self, capability: CapabilityId) -> Result<(), NodeError> {
        if !self
            .command
            .request
            .application_capabilities
            .contains(&capability)
        {
            return Err(NodeError::AuthorizationDenied(format!(
                "handler used undeclared application capability {capability}"
            )));
        }
        let mut actual = self
            .actual_capabilities
            .lock()
            .map_err(|_| NodeError::Poisoned)?;
        if !actual.contains(&capability) {
            actual.push(capability);
        }
        drop(actual);
        Ok(())
    }

    /// Atomically commits emitted items and a JSON-encoded typed result.
    ///
    /// # Errors
    ///
    /// Returns an error if the result cannot be encoded or the durable commit
    /// fails.
    pub fn commit<R: Serialize>(self, result: &R) -> Result<CommandSnapshot, NodeError> {
        let encoded = serde_json::to_vec(result)
            .map_err(|error| NodeError::ResultEncoding(error.to_string()))?;
        self.commit_bytes(encoded)
    }

    fn prepare_batch(&self) -> Result<ChangeBatch, NodeError> {
        let changes = self
            .changes
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .clone();
        let mut causal_parents = self
            .causal_reads
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .iter()
            .copied()
            .collect::<Vec<_>>();
        causal_parents.push(self.command.updated_at);
        let mut batch = ChangeBatch {
            id: BatchId::new(),
            command_id: self.command.request.id,
            service_id: self.command.request.service_id.clone(),
            scope_id: self.command.request.scope_id.clone(),
            causal_parents,
            changes,
        };
        // Bind known local write predecessors into the exact effect being authorized.
        // Approval resumption must not change this batch when newer writes arrive.
        batch
            .causal_parents
            .extend(crate::causal::scoped_author_parents(
                &self.node.events_after(None)?,
                self.node.node_id(),
                &batch,
            ));
        batch
            .causal_parents
            .sort_unstable_by_key(|event| (event.node_id, event.sequence));
        batch.causal_parents.dedup();
        Ok(batch)
    }

    /// Atomically commits emitted items and an application-owned result body.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable commit fails.
    pub fn commit_bytes(self, result: Vec<u8>) -> Result<CommandSnapshot, NodeError> {
        let batch = self.prepare_batch()?;
        let actual_claims = self
            .actual_claims
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .clone();
        let actual_capabilities = self
            .actual_capabilities
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .clone();
        let mut prospective_topology = self.node.scope_topology()?;
        prospective_topology.observe_event(&NodeEvent::CommandCommitted {
            command: self.command.clone(),
            batch: batch.clone(),
        })?;
        if self.authorization == CommandAuthorization::TrustedFramework {
            return self.node.commit(self.command.request.id, batch, result);
        }
        let effect = PreparedCommandEffect::new(
            self.command.updated_at,
            batch,
            result,
            actual_claims,
            actual_capabilities,
            prospective_topology,
        )?;
        let prepared = self
            .node
            .prepare_authorization(self.command.request.id, effect)?;
        let command = self.node.resolve_prepared_authorization(prepared)?;
        if let CommandState::Rejected { reason } = &command.state {
            return Err(NodeError::AuthorizationDenied(reason.clone()));
        }
        Ok(command)
    }

    /// Rejects this executing command without committing emitted items.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable rejection fails.
    pub fn reject(self, reason: impl Into<String>) -> Result<CommandSnapshot, NodeError> {
        self.node.reject(self.command.request.id, reason)
    }

    /// Releases this execution attempt for a later handler retry.
    ///
    /// Emitted items are discarded; only the retry lifecycle and reason are
    /// durably recorded.
    ///
    /// # Errors
    ///
    /// Returns an error if the retry state cannot be durably appended.
    pub fn retry(self, reason: impl Into<String>) -> Result<CommandSnapshot, NodeError> {
        self.node.retry(self.command.request.id, reason)
    }
}

impl Node {
    /// Creates a node backed by an in-memory immutable log.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::with_backend(Arc::new(InMemoryBackend::new(NodeId::new())))
    }

    /// Opens an event-sourced node over a durable journal.
    ///
    /// The complete command projection and replay log are reconstructed from
    /// immutable history before the node is returned.
    ///
    /// # Errors
    ///
    /// Returns an error when journal metadata or history cannot be recovered.
    pub fn from_journal(journal: Arc<dyn EventJournal>) -> Result<Self, NodeError> {
        Ok(Self::with_backend(Arc::new(InMemoryBackend::from_journal(
            journal,
        )?)))
    }

    /// Creates a node around a storage plugin.
    #[must_use]
    pub fn with_backend(backend: Arc<dyn NodeBackend>) -> Self {
        Self {
            backend,
            readiness: Arc::new(NodeReadiness::default()),
            command_dispatch: Arc::new(ReentrantMutex::new(())),
            command_access_policy: Arc::new(RwLock::new(None)),
            replication_coverage: Arc::new(RwLock::new(HashMap::new())),
            replication_resumptions: Arc::new(RwLock::new(HashMap::new())),
            replication_sources: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Registers a transport-validated durable cursor for continuity checking.
    /// This does not establish query completeness; only ingestion of a fresh
    /// authenticated batch whose `after` equals this cursor can do so.
    #[doc(hidden)]
    pub fn prepare_replication_resume(
        &self,
        source_node: NodeId,
        selection: ReplicationSelection,
        position: Option<LogPosition>,
    ) -> Result<(), NodeError> {
        self.replication_resumptions
            .write()
            .map_err(|_| NodeError::Poisoned)?
            .insert((source_node, selection), position);
        Ok(())
    }

    /// Records transport-owned knowledge about a configured replication
    /// source. This never establishes completeness by itself.
    #[doc(hidden)]
    pub fn mark_replication_source_reachable(&self, source_node: NodeId) -> Result<(), NodeError> {
        self.replication_sources
            .write()
            .map_err(|_| NodeError::Poisoned)?
            .insert(source_node, ReplicationSourceAvailability::Reachable);
        Ok(())
    }

    #[doc(hidden)]
    pub fn mark_replication_source_unreachable(
        &self,
        source_node: NodeId,
    ) -> Result<(), NodeError> {
        self.replication_sources
            .write()
            .map_err(|_| NodeError::Poisoned)?
            .insert(source_node, ReplicationSourceAvailability::Unreachable);
        Ok(())
    }

    #[doc(hidden)]
    pub fn mark_replication_source_undiscoverable(
        &self,
        source_node: NodeId,
    ) -> Result<(), NodeError> {
        self.replication_sources
            .write()
            .map_err(|_| NodeError::Poisoned)?
            .insert(source_node, ReplicationSourceAvailability::Undiscoverable);
        Ok(())
    }

    /// Installs the policy used for effect-phase command admission. Session
    /// preflight and commit admission must share this exact policy instance.
    ///
    /// # Errors
    /// Returns an error if the policy slot is poisoned.
    #[allow(clippy::needless_pass_by_value)] // The caller deliberately transfers policy installation intent.
    pub fn set_command_access_policy(
        &self,
        policy: Arc<dyn AccessPolicy>,
    ) -> Result<(), NodeError> {
        *self
            .command_access_policy
            .write()
            .map_err(|_| NodeError::Poisoned)? = Some(Arc::downgrade(&policy));
        Ok(())
    }

    fn record_replication_coverage(
        &self,
        source_node: NodeId,
        selection: ReplicationSelection,
        after: Option<LogPosition>,
        through: Option<LogPosition>,
    ) -> Result<(), NodeError> {
        self.mark_replication_source_reachable(source_node)?;
        let key = (source_node, selection);
        let trusted_resume = self
            .replication_resumptions
            .read()
            .map_err(|_| NodeError::Poisoned)?
            .get(&key)
            .is_some_and(|position| *position == after);
        let receipt = ReplicationCoverage {
            source_through: through,
            local_cut: self.backend.latest_position()?,
        };
        let mut coverage = self
            .replication_coverage
            .write()
            .map_err(|_| NodeError::Poisoned)?;
        match coverage.get(&key) {
            Some(previous) if previous.source_through == after => {
                coverage.insert(key.clone(), receipt);
            }
            None if after.is_none() || trusted_resume => {
                coverage.insert(key.clone(), receipt);
            }
            _ => {}
        }
        drop(coverage);
        if trusted_resume {
            self.replication_resumptions
                .write()
                .map_err(|_| NodeError::Poisoned)?
                .remove(&key);
        }
        Ok(())
    }

    fn selected_projection_coverage(
        &self,
        source_node: NodeId,
        service_id: &ServiceId,
        authorized: &[ScopeSelection],
        snapshot: &SelectedHistorySnapshot,
    ) -> Result<(ProjectionCoverage, Option<LogPosition>), NodeError> {
        if source_node == self.node_id() {
            return Ok((ProjectionCoverage::LocalAuthoritative, snapshot.through));
        }
        let topology = &snapshot.topology;
        let availability = self
            .replication_sources
            .read()
            .map_err(|_| NodeError::Poisoned)?
            .get(&source_node)
            .copied();
        if availability == Some(ReplicationSourceAvailability::Unreachable) {
            return Ok((ProjectionCoverage::Unreachable, None));
        }
        if availability == Some(ReplicationSourceAvailability::Undiscoverable) {
            return Ok((ProjectionCoverage::Undiscoverable, None));
        }
        let coverage = self
            .replication_coverage
            .read()
            .map_err(|_| NodeError::Poisoned)?;
        let observed_source = snapshot.observed_source(source_node);
        let mut through: Option<LogPosition> = None;
        let all_covered = authorized.iter().all(|requested| {
            let best = coverage
                .iter()
                .filter(|((candidate_source, selection), receipt)| {
                    candidate_source == &source_node
                        && receipt.local_cut <= snapshot.through
                        && selection.covers_scope_selection(service_id, requested, topology)
                })
                .filter_map(|(_, receipt)| receipt.source_through)
                .max();
            let found = coverage
                .iter()
                .any(|((candidate_source, selection), receipt)| {
                    candidate_source == &source_node
                        && receipt.local_cut <= snapshot.through
                        && selection.covers_scope_selection(service_id, requested, topology)
                });
            if found {
                through = match (through, best) {
                    (Some(current), Some(candidate)) => Some(current.min(candidate)),
                    (None, value) | (value, None) => value,
                };
            }
            found
        });
        Ok(if all_covered && !authorized.is_empty() {
            (ProjectionCoverage::ReplicatedComplete, through)
        } else if availability.is_none() && !observed_source {
            (ProjectionCoverage::Undiscoverable, None)
        } else {
            (ProjectionCoverage::ReplicatedIncomplete, None)
        })
    }

    /// Releases an authority-pending command only for its exact challenge and
    /// attaches the immutable approval to the original presentation.
    #[doc(hidden)]
    pub fn resume_authorization(
        &self,
        command_id: CommandId,
        challenge_id: &ChallengeId,
        approval_id: ApprovalId,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend
            .resume_authorization(command_id, challenge_id, approval_id)
    }

    /// Freezes a computed handler effect before invoking live authorization.
    #[doc(hidden)]
    pub fn prepare_authorization(
        &self,
        command_id: CommandId,
        effect: PreparedCommandEffect,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.prepare_authorization(command_id, effect)
    }

    /// Commits a previously prepared effect after an exact authorization permit.
    #[doc(hidden)]
    pub fn commit_prepared_authorization(
        &self,
        command_id: CommandId,
        effect_digest: &str,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend
            .commit_prepared_authorization(command_id, effect_digest)
    }

    /// Parks a previously prepared effect behind an exact authority challenge.
    #[doc(hidden)]
    pub fn await_prepared_authorization(
        &self,
        command_id: CommandId,
        effect_digest: &str,
        challenge_id: ChallengeId,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend
            .await_prepared_authorization(command_id, effect_digest, challenge_id)
    }

    #[doc(hidden)]
    pub fn advance_authorization(
        &self,
        command_id: CommandId,
        challenge_id: &ChallengeId,
        next_challenge_id: ChallengeId,
        approval_id: ApprovalId,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend
            .advance_authorization(command_id, challenge_id, next_challenge_id, approval_id)
    }

    /// Holds the node below its startup-ready barrier until the guard is
    /// completed or dropped.
    #[must_use]
    pub fn hold_startup(&self) -> NodeStartupGuard {
        self.readiness.startup_gates.fetch_add(1, Ordering::AcqRel);
        NodeStartupGuard {
            readiness: Arc::clone(&self.readiness),
            released: false,
        }
    }

    /// Returns whether every declared startup phase has completed.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.readiness.startup_gates.load(Ordering::Acquire) == 0
    }

    /// Waits without polling until every declared startup phase has completed.
    pub async fn wait_until_ready(&self) {
        NodeReadyFuture {
            readiness: Arc::clone(&self.readiness),
        }
        .await;
    }

    /// Returns the stable node identity.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.backend.node_id()
    }

    /// Returns the durable store identity, or `None` without a durable store.
    ///
    /// This identity alone does not detect a restored or copied database.
    ///
    /// # Errors
    ///
    /// Returns an error if the store identity cannot be read.
    pub fn storage_incarnation(&self) -> Result<Option<StorageIncarnationId>, NodeError> {
        self.backend.storage_incarnation()
    }

    /// Durably submits a command without making the client its executor.
    ///
    /// # Errors
    ///
    /// Returns an error on backend failure or conflicting command reuse.
    pub fn submit(&self, request: CommandRequest) -> Result<CommandSnapshot, NodeError> {
        let authenticated_executor = request.authority.executor.id.clone();
        self.prepare_command(authenticated_executor, request)
            .map_err(NodeError::from)?
            .submit()
    }

    /// Authorizes all declared command resources before any handler runs.
    /// The authenticated executor comes from the transport or the embedding
    /// application boundary and cannot be replaced by the wire presentation.
    #[doc(hidden)]
    #[allow(clippy::needless_pass_by_value)] // Authentication boundaries transfer the observed identity.
    pub fn prepare_command(
        &self,
        authenticated_executor: PrincipalId,
        request: CommandRequest,
    ) -> Result<PreparedCommand, AuthorizationFailure> {
        let permit = self
            .command_authorization(
                &authenticated_executor,
                &request,
                AuthorizationPhase::Admission,
            )?
            .into_permit()?;
        Ok(PreparedCommand {
            node: self.clone(),
            request,
            permit,
        })
    }

    fn command_authorization(
        &self,
        authenticated_executor: &PrincipalId,
        request: &CommandRequest,
        phase: AuthorizationPhase,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        let mut access = AccessAttempt::scoped(
            authenticated_executor.clone(),
            request.authority.clone(),
            AccessOperation::SubmitCommand,
            request.scope_id.clone(),
        );
        access.target = AccessTarget::KnownCommand {
            command_id: request.id,
            service_id: request.service_id.clone(),
            scope_id: request.scope_id.clone(),
            command_type: request.command_type.clone(),
            principal_id: request.principal_id.clone(),
        };
        access.resource_claims.clone_from(&request.resource_claims);
        access
            .application_capabilities
            .clone_from(&request.application_capabilities);
        access
            .arguments_digest
            .clone_from(&request.arguments_digest);
        access.authorization_phase = phase;
        access.topology = Some(
            self.scope_topology()
                .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?,
        );
        if &request.authority.executor.id != authenticated_executor
            || request.authority.principal.id != request.principal_id
        {
            return DenyAllAccessPolicy.decide(&access).into_immediate();
        }
        let Some(policy) = self
            .command_access_policy
            .read()
            .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?
            .as_ref()
            .and_then(Weak::upgrade)
        else {
            return Err(AuthorityUnavailable::PolicyUnavailable);
        };
        policy.decide(&access).into_immediate()
    }

    /// Reconstruct the authorization request from a retained prepared command.
    ///
    /// Callers supply only its stable ID. Claims, presentation, effect digest,
    /// and prospective topology come from the recorded command and effect.
    /// This provides retained evidence, not permission to execute the effect.
    ///
    /// # Errors
    /// Rejects unknown commands, commands not awaiting effect authorization,
    /// invalid prepared digests, and unavailable command history.
    pub fn prepared_command_access(
        &self,
        command_id: CommandId,
    ) -> Result<AccessAttempt, NodeError> {
        let command = self
            .command(command_id)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        let effect = match &command.state {
            CommandState::AuthorizationPrepared { effect } => effect.clone(),
            CommandState::AuthorizationPending { batch, result, .. } => {
                let retained = self
                    .events_after(None)?
                    .into_iter()
                    .rev()
                    .find_map(|event| {
                        let NodeEvent::CommandLifecycle(saved) = event.event else {
                            return None;
                        };
                        if saved.request.id != command_id {
                            return None;
                        }
                        let CommandState::AuthorizationPrepared { effect } = saved.state else {
                            return None;
                        };
                        Some((saved.request, effect))
                    })
                    .ok_or_else(|| {
                        NodeError::InvalidCommandState(
                            "pending command has no retained prepared effect".to_owned(),
                        )
                    })?;
                if retained.0 != command.request
                    || retained.1.batch() != batch.as_ref()
                    || retained.1.result() != result
                {
                    return Err(NodeError::CommandConflict(command_id));
                }
                retained.1
            }
            _ => {
                return Err(NodeError::InvalidCommandState(
                    "command is not awaiting prepared-effect authorization".to_owned(),
                ));
            }
        };
        effect.validate_digest()?;
        Ok(Self::prepared_effect_access_request(
            &command.request,
            &effect,
        ))
    }

    fn prepared_effect_access_request(
        request: &CommandRequest,
        effect: &PreparedCommandEffect,
    ) -> AccessAttempt {
        AccessAttempt {
            admission_id: None,
            principal_id: request.authority.executor.id.clone(),
            presentation: request.authority.clone(),
            operation: AccessOperation::SubmitCommand,
            target: AccessTarget::KnownCommand {
                command_id: request.id,
                service_id: request.service_id.clone(),
                scope_id: request.scope_id.clone(),
                command_type: request.command_type.clone(),
                principal_id: request.principal_id.clone(),
            },
            resource_claims: effect.resource_claims().to_vec(),
            application_capabilities: effect.application_capabilities().to_vec(),
            arguments_digest: request.arguments_digest.clone(),
            effect_digest: Some(effect.effect_digest().to_owned()),
            lease: None,
            authorization_phase: effect.authorization_phase(),
            topology: Some(effect.topology_proof().clone()),
        }
    }

    fn resolve_prepared_authorization(
        &self,
        command: CommandSnapshot,
    ) -> Result<CommandSnapshot, NodeError> {
        let CommandState::AuthorizationPrepared { effect } = command.state else {
            return Ok(command);
        };
        effect.validate_digest()?;
        let request = Self::prepared_effect_access_request(&command.request, &effect);
        let Some(policy) = self
            .command_access_policy
            .read()
            .map_err(|_| NodeError::Poisoned)?
            .as_ref()
            .and_then(Weak::upgrade)
        else {
            return Err(AuthorityUnavailable::PolicyUnavailable.into());
        };
        match policy.decide(&request).into_immediate()? {
            AuthorizationDecision::Permit(_) => {
                self.commit_prepared_authorization(command.request.id, effect.effect_digest())
            }
            AuthorizationDecision::Challenge { challenge, .. } => self
                .await_prepared_authorization(
                    command.request.id,
                    effect.effect_digest(),
                    challenge.id,
                ),
            AuthorizationDecision::Deny(denied) => {
                let reason = AuthorizationDecision::Deny(denied).public_message();
                self.reject(command.request.id, reason)
            }
        }
    }

    /// Durably submits a typed application command without executing it.
    ///
    /// # Errors
    ///
    /// Returns an error if its body cannot be encoded, storage fails, or its
    /// stable identity conflicts with a different request.
    pub fn submit_command<C: MykoCommand>(
        &self,
        scope_id: ScopeId,
        command: &C,
    ) -> Result<CommandSnapshot, NodeError> {
        self.submit_authenticated_command(scope_id, PrincipalId::for_node(self.node_id()), command)
    }

    /// Submits through a principal already authenticated by a Myko session.
    #[doc(hidden)]
    pub fn submit_authenticated_command<C: MykoCommand>(
        &self,
        scope_id: ScopeId,
        principal_id: PrincipalId,
        command: &C,
    ) -> Result<CommandSnapshot, NodeError> {
        self.submit(CommandRequest::for_command(
            scope_id,
            principal_id,
            command,
        )?)
    }

    /// Atomically claims a submitted command for a local handler.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown or cannot be updated.
    pub fn claim(&self, command_id: CommandId) -> Result<CommandAdmission, NodeError> {
        self.backend.claim(command_id)
    }

    /// Claims a locally originated command and creates a typed atomic item
    /// context for its handler.
    ///
    /// Replicated command events are projections, not executable work on the
    /// observing node. This method enforces that invariant before claiming.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown, originated on another node,
    /// or cannot be claimed durably.
    pub fn begin_command(&self, command_id: CommandId) -> Result<TypedCommandAdmission, NodeError> {
        self.begin_command_with_authorization(command_id, CommandAuthorization::Enforce)
    }

    fn begin_command_with_authorization(
        &self,
        command_id: CommandId,
        authorization: CommandAuthorization,
    ) -> Result<TypedCommandAdmission, NodeError> {
        let current = self
            .command(command_id)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        let origin = self
            .command_origin(command_id)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if origin != self.node_id() {
            return Err(NodeError::ForeignCommand { command_id, origin });
        }
        if authorization == CommandAuthorization::Enforce
            && matches!(
                current.state,
                CommandState::Submitted | CommandState::Retrying { .. }
            )
        {
            let decision = self.command_authorization(
                &current.request.authority.executor.id,
                &current.request,
                AuthorizationPhase::Continuation,
            )?;
            if !decision.is_permit() {
                return Ok(match self.claim(command_id)? {
                    CommandAdmission::Execute(_) => TypedCommandAdmission::Resume(
                        self.backend.reject(command_id, decision.public_message())?,
                    ),
                    CommandAdmission::Resume(command) => TypedCommandAdmission::Resume(command),
                });
            }
        }
        Ok(match self.claim(command_id)? {
            CommandAdmission::Execute(command) => TypedCommandAdmission::Execute(CommandContext {
                node: self.clone(),
                authorization,
                actual_claims: Arc::new(Mutex::new(
                    command
                        .request
                        .resource_claims
                        .iter()
                        .filter(|claim| claim.kind == ResourceClaimKind::Primary)
                        .cloned()
                        .collect(),
                )),
                actual_capabilities: Arc::new(Mutex::new(
                    command.request.application_capabilities.clone(),
                )),
                command,
                changes: Arc::new(Mutex::new(Vec::new())),
                causal_reads: Arc::new(Mutex::new(HashSet::new())),
            }),
            CommandAdmission::Resume(command) => {
                let command = if authorization == CommandAuthorization::Enforce {
                    self.resolve_prepared_authorization(command)?
                } else {
                    command
                };
                TypedCommandAdmission::Resume(command)
            }
        })
    }

    /// Claims and decodes a locally originated declared command.
    ///
    /// The service and command wire identities must exactly match `C` before
    /// application code receives the payload.
    ///
    /// # Errors
    ///
    /// Returns an error if admission fails, the command originated elsewhere,
    /// its declared schema does not match, or its payload is malformed.
    pub fn begin_declared_command<C: MykoCommand>(
        &self,
        command_id: CommandId,
    ) -> Result<DeclaredCommandAdmission<C>, NodeError> {
        self.begin_declared_command_with_authorization(command_id, CommandAuthorization::Enforce)
    }

    fn begin_declared_command_with_authorization<C: MykoCommand>(
        &self,
        command_id: CommandId,
        authorization: CommandAuthorization,
    ) -> Result<DeclaredCommandAdmission<C>, NodeError> {
        let snapshot = self
            .command(command_id)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        let body = decode_declared_body::<C>(&snapshot.request)?;
        match self.begin_command_with_authorization(command_id, authorization)? {
            TypedCommandAdmission::Execute(context) => {
                Ok(DeclaredCommandAdmission::Execute(DeclaredCommandContext {
                    inner: context,
                    body,
                }))
            }
            TypedCommandAdmission::Resume(snapshot) => {
                Ok(DeclaredCommandAdmission::Resume(snapshot))
            }
        }
    }

    /// Atomically admits an idempotent command.
    ///
    /// # Errors
    ///
    /// Returns an error on backend failure or conflicting command reuse.
    pub fn admit(&self, request: CommandRequest) -> Result<CommandAdmission, NodeError> {
        self.backend.admit(request)
    }

    /// Submits a framework-owned command without invoking the policy that the
    /// command itself maintains.
    #[doc(hidden)]
    pub fn submit_trusted_framework_command(
        &self,
        request: CommandRequest,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.submit(request)
    }

    /// Atomically appends the command's complete authoritative change batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is absent, no longer executing, or the
    /// batch does not match its service and scope.
    pub fn commit(
        &self,
        command_id: CommandId,
        batch: ChangeBatch,
        result: Vec<u8>,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.commit(command_id, batch, result)
    }

    /// Rejects an executing command without committing graph changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is absent or no longer executing.
    pub fn reject(
        &self,
        command_id: CommandId,
        reason: impl Into<String>,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.reject(command_id, reason.into())
    }

    /// Releases an executing command for another ordered handler attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is absent, no longer executing, or the
    /// retry state cannot be durably appended.
    pub fn retry(
        &self,
        command_id: CommandId,
        reason: impl Into<String>,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.retry(command_id, reason.into())
    }

    /// Cancels submitted or executing work without committing graph changes.
    ///
    /// Terminal commands are returned unchanged, so callers can distinguish a
    /// successful cancellation from a commit or rejection that won the race.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is absent or cancellation cannot be
    /// durably recorded.
    pub fn cancel(
        &self,
        command_id: CommandId,
        reason: impl Into<String>,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.cancel(command_id, reason.into())
    }

    /// Reads the current lifecycle for a command.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot be read.
    pub fn command(&self, command_id: CommandId) -> Result<Option<CommandSnapshot>, NodeError> {
        self.backend.command(command_id)
    }

    pub(super) fn command_through(
        &self,
        command_id: CommandId,
        through: Option<LogPosition>,
    ) -> Result<Option<CommandSnapshot>, NodeError> {
        let history = through
            .map(|cut| self.causal_events_through(cut))
            .transpose()?
            .unwrap_or_default();
        Ok(materialize_command_snapshot(&history, command_id))
    }

    /// Reads one current command state and starts a gap-free lifecycle watch.
    ///
    /// The snapshot is reconstructed from the same bounded history prefix used
    /// to establish the subscription, so a concurrent transition is delivered
    /// after it rather than falling into a query-to-subscribe race.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown or history/subscription
    /// access fails.
    pub fn watch_command(
        &self,
        command_id: CommandId,
    ) -> Result<(CommandResponse, CommandWatch), NodeError> {
        let through = self.backend.latest_position()?;
        let events = self.subscribe(through)?;
        let current = self
            .command_through(command_id, through)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        Ok((
            CommandResponse {
                source_node: self.node_id(),
                command: Some(current.clone()),
            },
            CommandWatch {
                node: self.clone(),
                command_id,
                current,
                events,
            },
        ))
    }

    /// Waits for a command to become visible, then watches its lifecycle
    /// without a visibility-to-subscribe race.
    ///
    /// This is the local-node path for a command submitted through another
    /// mesh peer: the remote response may arrive before replication makes the
    /// command visible in this node's projection. The subscription is opened
    /// before checking current state, so the first replicated lifecycle cannot
    /// be missed.
    ///
    /// # Errors
    ///
    /// Returns an error if history access or the live subscription fails.
    pub async fn watch_command_eventually(
        &self,
        command_id: CommandId,
    ) -> Result<(CommandResponse, CommandWatch), NodeError> {
        let through = self.backend.latest_position()?;
        let mut events = self.subscribe(through)?;
        let current = match self.command_through(command_id, through)? {
            Some(current) => current,
            None => loop {
                let envelope = events.recv_async().await?;
                if let Some(command) = self.command_through(command_id, Some(envelope.position))? {
                    break command;
                }
            },
        };
        Ok((
            CommandResponse {
                source_node: self.node_id(),
                command: Some(current.clone()),
            },
            CommandWatch {
                node: self.clone(),
                command_id,
                current,
                events,
            },
        ))
    }

    /// Returns the stable node identity that first originated a command.
    ///
    /// This lets an application distinguish locally admitted work from a
    /// replicated projection before performing a node-local effect.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn command_origin(&self, command_id: CommandId) -> Result<Option<NodeId>, NodeError> {
        self.backend.command_origin(command_id)
    }

    /// Returns locally originated submitted, retrying, and prepared commands
    /// for one stable wire contract in their original admission order.
    ///
    /// Current lifecycle state comes from the backend projection rather than
    /// whichever raw lifecycle event happened to appear last. Replicated
    /// submissions are never returned as locally executable work.
    ///
    /// # Errors
    ///
    /// Returns an error when history or current command state cannot be read,
    /// or when history references a command missing from the projection.
    pub fn pending_local_commands(
        &self,
        service_id: &str,
        command_type: &str,
    ) -> Result<Vec<CommandSnapshot>, NodeError> {
        Ok(self
            .pending_local_service_commands(service_id)?
            .into_iter()
            .filter(|command| command.request.command_type == command_type)
            .collect())
    }

    /// Returns every locally originated submitted, retrying, or prepared command
    /// for one service in original admission order across command types.
    ///
    /// # Errors
    ///
    /// Returns an error when history or current command state cannot be read,
    /// or when history references a command missing from the projection.
    pub fn pending_local_service_commands(
        &self,
        service_id: &str,
    ) -> Result<Vec<CommandSnapshot>, NodeError> {
        let history = self.events_after(None)?;
        Ok(materialize_pending_local_commands(
            &history,
            self.node_id(),
            Some(&ServiceId::new(service_id)),
            None,
        )
        .into())
    }

    /// Returns every locally originated submitted, retrying, or prepared
    /// application command in original admission order across services and types.
    ///
    /// # Errors
    ///
    /// Returns an error when history or current command state cannot be read,
    /// or when history references a command missing from the projection.
    pub fn pending_local_application_commands(&self) -> Result<Vec<CommandSnapshot>, NodeError> {
        let history = self.events_after(None)?;
        Ok(materialize_pending_local_commands(&history, self.node_id(), None, None).into())
    }

    /// Returns saved local effects awaiting authority, including parked approvals.
    /// These commands must not enter the handler execution queue again.
    ///
    /// # Errors
    /// Returns an error when retained command history cannot be read.
    pub fn pending_local_authorization_commands(&self) -> Result<Vec<CommandSnapshot>, NodeError> {
        let history = self.events_after(None)?;
        Ok(
            materialize_local_commands(&history, self.node_id(), None, None, |state| {
                matches!(
                    state,
                    CommandState::AuthorizationPrepared { .. }
                        | CommandState::AuthorizationPending { .. }
                )
            })
            .into(),
        )
    }

    /// Starts a gap-free work feed for every locally originated command in one
    /// application service.
    ///
    /// The returned feed first yields commands that were submitted, retrying, or
    /// prepared at the captured history boundary, then follows new admissions,
    /// retries, and prepared effects. Resume prepared effects through command
    /// admission rather than executing their handlers again. Replicated command
    /// lifecycles and commands awaiting approval are omitted from the initial queue.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or a lossless event
    /// subscription cannot be established.
    pub fn watch_pending_local_service_commands(
        &self,
        service_id: impl Into<String>,
    ) -> Result<PendingCommandSubscription, NodeError> {
        self.watch_pending_local_commands(Some(ServiceId::new(service_id)), None)
    }

    /// Starts a gap-free work feed for every locally originated application
    /// command, regardless of service or concrete operation.
    ///
    /// This is the framework-facing feed used by a composed application
    /// runtime. Applications should consume their generated handler registry
    /// rather than splitting this feed back into manually named services.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or a lossless event
    /// subscription cannot be established.
    pub fn watch_pending_local_application_commands(
        &self,
    ) -> Result<PendingCommandSubscription, NodeError> {
        self.watch_pending_local_commands(None, None)
    }

    /// Starts a gap-free work feed for one declared command contract.
    ///
    /// Myko owns restart catch-up, local-origin filtering, and the transition
    /// from replay to live delivery. The consuming service only claims and
    /// handles the yielded stable command IDs.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or a lossless event
    /// subscription cannot be established.
    pub fn watch_pending_typed<C: MykoCommand>(
        &self,
    ) -> Result<PendingCommandSubscription, NodeError> {
        self.watch_pending_local_commands(
            Some(ServiceId::new(C::SERVICE_ID)),
            Some(C::COMMAND_TYPE.to_owned()),
        )
    }

    /// Returns submitted or retrying command bodies of one typed contract.
    ///
    /// Myko owns service/type filtering and typed request decoding; application
    /// code receives typed values instead of rebuilding wire checks. Prepared
    /// effects are omitted because their handlers must not execute again; use
    /// the pending snapshot or watch APIs to recover those command IDs.
    ///
    /// # Errors
    ///
    /// Returns an error when pending history cannot be read or a matching
    /// command does not satisfy its declared wire contract.
    pub fn pending_typed<C: MykoCommand>(&self) -> Result<Vec<C>, NodeError> {
        self.pending_local_commands(C::SERVICE_ID.as_str(), C::COMMAND_TYPE)?
            .iter()
            .filter(|command| {
                matches!(
                    command.state,
                    CommandState::Submitted | CommandState::Retrying { .. }
                )
            })
            .map(|command| command.request.command::<C>())
            .collect()
    }

    fn watch_pending_local_commands(
        &self,
        service_id: Option<ServiceId>,
        command_type: Option<String>,
    ) -> Result<PendingCommandSubscription, NodeError> {
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let events = self.subscribe(through)?;
        let local_node = self.node_id();
        let pending = materialize_pending_local_commands(
            &history,
            local_node,
            service_id.as_ref(),
            command_type.as_deref(),
        );
        Ok(PendingCommandSubscription {
            local_node,
            service_id,
            command_type,
            pending,
            events,
        })
    }

    /// Dispatches one command through its declared payload/result contract.
    ///
    /// # Errors
    ///
    /// Returns an error when admission, commit, or durable rejection fails.
    pub fn dispatch_declared_command<C, F>(
        &self,
        command_id: CommandId,
        handle: F,
    ) -> Result<CommandDispatchResult, NodeError>
    where
        C: MykoCommand,
        F: FnOnce(&mut DeclaredCommandContext<C>) -> Result<C::Output, CommandHandlerError>,
    {
        self.dispatch_declared_command_with_authorization(
            command_id,
            handle,
            CommandAuthorization::Enforce,
        )
    }

    /// Dispatches a framework-owned command without invoking the policy that
    /// the command itself maintains.
    #[doc(hidden)]
    pub fn dispatch_trusted_framework_command<C, F>(
        &self,
        command_id: CommandId,
        handle: F,
    ) -> Result<CommandDispatchResult, NodeError>
    where
        C: MykoCommand,
        F: FnOnce(&mut DeclaredCommandContext<C>) -> Result<C::Output, CommandHandlerError>,
    {
        self.dispatch_declared_command_with_authorization(
            command_id,
            handle,
            CommandAuthorization::TrustedFramework,
        )
    }

    fn dispatch_declared_command_with_authorization<C, F>(
        &self,
        command_id: CommandId,
        handle: F,
        authorization: CommandAuthorization,
    ) -> Result<CommandDispatchResult, NodeError>
    where
        C: MykoCommand,
        F: FnOnce(&mut DeclaredCommandContext<C>) -> Result<C::Output, CommandHandlerError>,
    {
        // Claim, execute, and commit are one process-local ownership interval.
        // A competing synchronous caller must observe the terminal result, not
        // the transient `Executing` snapshot produced by the retained driver.
        let _dispatch = self.command_dispatch.lock();
        match self.begin_declared_command_with_authorization::<C>(command_id, authorization) {
            Ok(DeclaredCommandAdmission::Execute(mut context)) => {
                let handled = handle(&mut context);
                let (command, disposition) = match handled {
                    Ok(output) => {
                        let command = context.commit(&output)?;
                        let disposition = if matches!(
                            command.state,
                            CommandState::AuthorizationPrepared { .. }
                                | CommandState::AuthorizationPending { .. }
                        ) {
                            CommandDispatchDisposition::Retrying
                        } else {
                            CommandDispatchDisposition::Committed
                        };
                        (command, disposition)
                    }
                    Err(CommandHandlerError::Reject(reason)) => (
                        context.reject(format!("declared command handler failed: {reason}"))?,
                        CommandDispatchDisposition::Rejected,
                    ),
                    Err(CommandHandlerError::Retry(reason)) => {
                        (context.retry(reason)?, CommandDispatchDisposition::Retrying)
                    }
                };
                Ok(CommandDispatchResult {
                    command,
                    disposition,
                })
            }
            Ok(DeclaredCommandAdmission::Resume(command)) => {
                let command = if authorization == CommandAuthorization::Enforce {
                    self.resolve_prepared_authorization(command)?
                } else {
                    command
                };
                let disposition = CommandDispatchDisposition::for_resumed_state(&command.state);
                Ok(CommandDispatchResult {
                    command,
                    disposition,
                })
            }
            Err(
                error @ (NodeError::CommandDecoding(_) | NodeError::CommandSchemaMismatch { .. }),
            ) => {
                let reason = format!("invalid declared command: {error}");
                let (command, disposition) =
                    match self.begin_command_with_authorization(command_id, authorization)? {
                        TypedCommandAdmission::Execute(context) => (
                            context.reject(reason)?,
                            CommandDispatchDisposition::Rejected,
                        ),
                        TypedCommandAdmission::Resume(command) => {
                            let disposition =
                                CommandDispatchDisposition::for_resumed_state(&command.state);
                            (command, disposition)
                        }
                    };
                Ok(CommandDispatchResult {
                    command,
                    disposition,
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Dispatches every currently pending local command declared as `C`.
    ///
    /// Myko owns ordered discovery, local-origin admission, payload decoding,
    /// atomic commit, and durable rejection. The application closure owns only
    /// domain validation, typed item emission, and the declared result.
    ///
    /// A malformed matching payload is rejected without preventing later
    /// commands from running. Handlers explicitly classify domain rejection
    /// versus a transient retry.
    ///
    /// # Errors
    ///
    /// Returns an error when history, admission, commit, or rejection fails.
    pub fn dispatch_declared<C, F>(
        &self,
        mut handle: F,
    ) -> Result<Vec<CommandDispatchResult>, NodeError>
    where
        C: MykoCommand,
        F: FnMut(&mut DeclaredCommandContext<C>) -> Result<C::Output, CommandHandlerError>,
    {
        let mut results = Vec::new();
        for pending in self.pending_local_commands(C::SERVICE_ID.as_str(), C::COMMAND_TYPE)? {
            results.push(
                self.dispatch_declared_command::<C, _>(pending.request.id, |context| {
                    handle(context)
                })?,
            );
        }
        Ok(results)
    }

    /// Reads immutable events after an exclusive cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot be read.
    pub fn events_after(
        &self,
        after: Option<LogPosition>,
    ) -> Result<Vec<EventEnvelope>, NodeError> {
        self.backend.events_after(after)
    }

    /// Returns the latest authoritative commit position in one typed service
    /// scope.
    ///
    /// Framework projections use this frontier to prove their Hyphae state is
    /// current before making a safety-sensitive decision.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend frontier index is unavailable.
    #[doc(hidden)]
    pub fn authoritative_position_in<S: MykoService>(
        &self,
        scope_id: &ScopeId,
    ) -> Result<Option<LogPosition>, NodeError> {
        self.backend
            .service_scope_position(&ServiceId::new(S::SERVICE_ID), scope_id)
    }

    /// Materializes one bounded page of current command states.
    ///
    /// The first page fixes a serving-log ceiling retained by every
    /// continuation, so concurrent lifecycle transitions cannot create gaps or
    /// duplicates in the collected catalog.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid page request or malformed history.
    pub fn command_state_page(
        &self,
        mut request: CommandStateRequest,
    ) -> Result<CommandStatePage, NodeError> {
        validate_command_state_request(&request)?;
        let source_node = request.source_node.unwrap_or_else(|| self.node_id());
        request.source_node = Some(source_node);
        let latest = self.backend.latest_position()?;
        let through = match request.snapshot_through {
            Some(requested) if latest.is_none_or(|latest| requested > latest) => {
                return Err(NodeError::InvalidCommandState(format!(
                    "command-state snapshot cursor {} is newer than serving history",
                    requested.get()
                )));
            }
            Some(requested) => Some(requested),
            None => latest,
        };
        request.snapshot_through = through;
        let history = through
            .map(|cut| self.causal_events_through(cut))
            .transpose()?
            .unwrap_or_default();
        let current = materialize_command_state_entries(history, source_node, &request, through);
        let page_size = usize::try_from(request.page_size).map_err(|error| {
            NodeError::InvalidCommandState(format!(
                "command-state page size is not addressable: {error}"
            ))
        })?;
        let mut commands = current
            .into_iter()
            .filter(|(command_id, _entry)| {
                request
                    .after_command_id
                    .as_deref()
                    .is_none_or(|cursor| command_id.as_str() > cursor)
            })
            .take(page_size.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = commands.len() > page_size;
        if has_more {
            let _overflow = commands.pop();
        }
        let next_after_command_id = has_more
            .then(|| {
                commands
                    .last()
                    .map(|(command_id, _entry)| command_id.clone())
            })
            .flatten();
        let commands = commands
            .into_iter()
            .map(|(_command_id, entry)| entry)
            .collect();
        Ok(CommandStatePage {
            serving_node: self.node_id(),
            through,
            request,
            commands,
            next_after_command_id,
        })
    }

    /// Collects every page of one cursor-stable current command catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if any page changes identity, ordering, or cursor.
    pub fn command_states(
        &self,
        request: CommandStateRequest,
    ) -> Result<CommandStateSnapshot, NodeError> {
        let first = self.command_state_page(request)?;
        let (mut snapshot, mut next) = CommandStateSnapshot::from_first_page(first)?;
        while let Some(request) = next {
            let page = self.command_state_page(request.clone())?;
            next = snapshot.append_page(&request, page)?;
        }
        Ok(snapshot)
    }

    /// Follows causally ready entries for a source-bound command catalog.
    ///
    /// One late ancestor can release several entries. They share one atomic
    /// update and cursor so reconnect cannot skip part of the release.
    ///
    /// # Errors
    /// Returns an error for a foreign serving cursor, invalid request, or unavailable history.
    pub fn watch_commands(
        &self,
        request: CommandWatchRequest,
    ) -> Result<CommandCatalogWatch, NodeError> {
        let latest = self.backend.latest_position()?;
        if request.serving_node != self.node_id()
            || request
                .after
                .is_some_and(|cut| latest.is_none_or(|latest| cut > latest))
        {
            return Err(NodeError::InvalidCommandState(
                "command watch cursor does not belong to serving history".to_owned(),
            ));
        }
        let current = self.command_catalog_through(&request, request.after)?;
        let events = self.subscribe(request.after)?;
        Ok(CommandCatalogWatch {
            node: self.clone(),
            request,
            current,
            events,
        })
    }

    pub(super) fn command_catalog_through(
        &self,
        request: &CommandWatchRequest,
        through: Option<LogPosition>,
    ) -> Result<BTreeMap<String, CommandStateEntry>, NodeError> {
        let state_request = CommandStateRequest {
            source_node: Some(request.source_node),
            service_id: request.service_id.clone(),
            scope_id: request.scope_id.clone(),
            command_type: request.command_type.clone(),
            snapshot_through: through,
            after_command_id: None,
            page_size: DEFAULT_COMMAND_STATE_PAGE_SIZE,
        };
        validate_command_state_request(&state_request)?;
        let history = through
            .map(|cut| self.causal_events_through(cut))
            .transpose()?
            .unwrap_or_default();
        Ok(materialize_command_state_entries(
            history,
            request.source_node,
            &state_request,
            through,
        ))
    }

    /// Materializes one bounded page of schema-specific current state.
    ///
    /// The first page fixes a node-log ceiling. Continuation requests retain
    /// that ceiling, so commits arriving during pagination cannot create gaps
    /// or duplicates in the collected snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid page request or malformed matching
    /// history.
    pub fn item_state_page(
        &self,
        mut request: ItemStateRequest,
    ) -> Result<ItemStatePage, NodeError> {
        if request.item_type.is_empty()
            || request.schema_version == 0
            || request.page_size == 0
            || request.page_size > MAX_ITEM_STATE_PAGE_SIZE
            || request.after_item_id.as_ref().is_some_and(String::is_empty)
        {
            return Err(NodeError::InvalidItemState(format!(
                "item-state request requires a schema, a non-empty cursor, and a page size between 1 and {MAX_ITEM_STATE_PAGE_SIZE}"
            )));
        }
        let source_node = request.source_node.unwrap_or_else(|| self.node_id());
        request.source_node = Some(source_node);
        let history = self.events_after(None)?;
        let latest = history.last().map(|envelope| envelope.position);
        let through = match request.snapshot_through {
            Some(requested) if latest.is_none_or(|latest| requested > latest) => {
                return Err(NodeError::InvalidItemState(format!(
                    "item-state snapshot cursor {} is newer than serving history",
                    requested.get()
                )));
            }
            Some(requested) => Some(requested),
            None => latest,
        };
        request.snapshot_through = through;
        let current = materialize_item_state_entries(history, source_node, &request, through)?;
        let page_size = usize::try_from(request.page_size).map_err(|error| {
            NodeError::InvalidItemState(format!("item-state page size is not addressable: {error}"))
        })?;
        let mut items = current
            .into_values()
            .filter(|item| {
                request
                    .after_item_id
                    .as_deref()
                    .is_none_or(|cursor| item.mutation.item_id.as_str() > cursor)
            })
            .take(page_size.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = items.len() > page_size;
        if has_more {
            let _overflow = items.pop();
        }
        let next_after_item_id = has_more
            .then(|| items.last().map(|item| item.mutation.item_id.clone()))
            .flatten();
        Ok(ItemStatePage {
            serving_node: self.node_id(),
            through,
            request,
            items,
            next_after_item_id,
        })
    }

    /// Materializes every bounded page of one current-state snapshot locally.
    ///
    /// Transport clients use the equivalent framework-owned asynchronous
    /// collector on [`ItemClient::item_state`].
    ///
    /// # Errors
    ///
    /// Returns an error if any page is invalid or matching history is
    /// malformed.
    pub fn item_state_snapshot(
        &self,
        request: ItemStateRequest,
    ) -> Result<ItemStateSnapshot, NodeError> {
        let first = self.item_state_page(request)?;
        let (mut snapshot, mut next) = ItemStateSnapshot::from_first_page(first)?;
        while let Some(request) = next {
            let page = self.item_state_page(request.clone())?;
            next = snapshot.append_page(&request, page)?;
        }
        Ok(snapshot)
    }

    /// Materializes current state for one typed item schema from all known
    /// local and replicated command batches.
    ///
    /// Applications normally use [`Self::query_items`] rather than handling
    /// federation envelopes or serialized item mutations themselves.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or contains a malformed
    /// mutation for `T`.
    pub fn project_items<T: MykoItem>(&self) -> Result<ItemProjection<T>, NodeError> {
        self.project_items_from::<T>(None)
    }

    /// Materializes one typed schema, optionally restricted to its immutable
    /// source-node identity.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or contains a malformed
    /// mutation for `T`.
    pub fn project_items_from<T: MykoItem>(
        &self,
        source_node: Option<NodeId>,
    ) -> Result<ItemProjection<T>, NodeError> {
        self.project_items_matching::<T>(source_node, None)
    }

    fn project_items_matching<T: MykoItem>(
        &self,
        source_node: Option<NodeId>,
        service_scope: Option<(&ServiceId, &ScopeId)>,
    ) -> Result<ItemProjection<T>, NodeError> {
        let history = if source_node.is_some() {
            self.events_after(None)?
        } else {
            self.causal_snapshot()?.1
        };
        project_item_history(&history, source_node, service_scope)
    }

    /// Returns dependency-complete history at a fixed observer-local cut.
    ///
    /// Projection adapters must derive values and topology from this same history.
    /// This local read does not authorize disclosure to a remote principal.
    ///
    /// # Errors
    ///
    /// Returns an error if retained history cannot be read or ordered.
    pub fn causal_events_through(
        &self,
        through: LogPosition,
    ) -> Result<Vec<EventEnvelope>, NodeError> {
        self.backend.causal_events_through(through)
    }

    /// Freezes a local history cut and returns its dependency-complete events.
    ///
    /// Adapters can subscribe after the returned cut without a replay/live gap.
    /// The cut may include unresolved events omitted from the returned history;
    /// it is not proof of complete replicated coverage or authorization to serve it.
    ///
    /// # Errors
    ///
    /// Returns an error if the cut or its retained history cannot be read.
    pub fn causal_snapshot(&self) -> Result<(Option<LogPosition>, Vec<EventEnvelope>), NodeError> {
        let through = self.backend.latest_position()?;
        let history = through
            .map(|cut| self.causal_events_through(cut))
            .transpose()?
            .unwrap_or_default();
        Ok((through, history))
    }

    /// Capture the latest local recording position for a new read or subscription.
    ///
    /// A position is a target, not evidence that a projection has consumed it.
    ///
    /// # Errors
    ///
    /// Returns an error if the local journal head cannot be read.
    pub fn local_history_cut(&self) -> Result<Option<LogPosition>, NodeError> {
        self.backend.latest_position()
    }

    /// Executes a generated typed query against current local and replicated
    /// item state.
    ///
    /// # Errors
    ///
    /// Returns an error when item state cannot be materialized from history.
    #[allow(clippy::needless_pass_by_value)] // The typed query is a one-shot snapshot request.
    pub fn query_items<Q>(&self, query: Q) -> Result<ItemQueryResult<Q>, NodeError>
    where
        Q: ItemQuery,
    {
        Ok(__snapshot_item_query(
            &query,
            &self.project_items::<Q::Item>()?,
        ))
    }

    /// Executes a generated typed query against one authoritative source's
    /// current item state.
    ///
    /// # Errors
    ///
    /// Returns an error when source state cannot be materialized from history.
    #[allow(clippy::needless_pass_by_value)] // The typed query is a one-shot snapshot request.
    pub fn query_items_from<Q>(
        &self,
        source_node: NodeId,
        query: Q,
    ) -> Result<ItemQueryResult<Q>, NodeError>
    where
        Q: ItemQuery,
    {
        Ok(__snapshot_item_query(
            &query,
            &self.project_items_from::<Q::Item>(Some(source_node))?,
        ))
    }

    /// Executes a typed query within one authoritative source, application
    /// service, and federation scope.
    ///
    /// This is the normal application-facing projection boundary: storage and
    /// replicated history stay behind the node while the caller works only
    /// with generated item/query types.
    ///
    /// # Errors
    ///
    /// Returns an error when scoped item state cannot be materialized.
    #[allow(clippy::needless_pass_by_value)] // The typed query is a one-shot snapshot request.
    pub fn query_items_in<Q>(
        &self,
        source_node: NodeId,
        scope_id: &ScopeId,
        query: Q,
    ) -> Result<ItemQueryResult<Q>, NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        Ok(__snapshot_item_query(
            &query,
            &self.project_items_matching::<Q::Item>(
                Some(source_node),
                Some((&service_id, scope_id)),
            )?,
        ))
    }

    /// Executes a typed query within one application service and federation
    /// scope across every authoritative source represented in this node.
    ///
    /// This preserves source provenance during ingestion while allowing
    /// naturally federated application state, such as an agent mailbox, to be
    /// consumed without decoding raw history.
    ///
    /// # Errors
    ///
    /// Returns an error when scoped item state cannot be materialized.
    #[allow(clippy::needless_pass_by_value)] // The typed query is a one-shot snapshot request.
    pub fn query_items_across_sources_in<Q>(
        &self,
        scope_id: &ScopeId,
        query: Q,
    ) -> Result<ItemQueryResult<Q>, NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        Ok(__snapshot_item_query(
            &query,
            &self.project_items_matching::<Q::Item>(None, Some((&service_id, scope_id)))?,
        ))
    }

    /// Queries one exact scope or nested subtree after the installed policy
    /// derives its authorized intersection. Neither authorization nor
    /// replication completeness is accepted from the caller.
    ///
    /// # Errors
    /// Returns an error when history or nested topology is malformed.
    pub fn query_items_selected<Q>(
        &self,
        authenticated_executor: PrincipalId,
        presentation: AuthorityPresentation,
        source_node: NodeId,
        requested: &ScopeSelection,
        query: Q,
    ) -> Result<SelectedQueryResult<ItemQueryResult<Q>>, NodeError>
    where
        Q: ItemQuery,
    {
        self.query_items_selected_at(
            SelectedQueryRead {
                authenticated_executor,
                presentation,
                source_node,
                requested,
                phase: AuthorizationPhase::Admission,
                through: self.backend.latest_position()?,
            },
            query,
        )
    }

    /// Looks up one typed item inside an authorization-filtered selection.
    /// `AuthoritativelyAbsent` is returned only when the entire requested view
    /// is authorized and the node owns complete current-state coverage.
    ///
    /// # Errors
    ///
    /// Returns an error when history, topology, or policy evaluation fails.
    pub fn query_item_selected<T>(
        &self,
        authenticated_executor: PrincipalId,
        presentation: AuthorityPresentation,
        source_node: NodeId,
        requested: &ScopeSelection,
        query: T::GetByIdQuery,
    ) -> Result<SelectedQueryResult<Option<T>>, NodeError>
    where
        T: MykoItem,
    {
        let mut result = self
            .query_items_selected(
                authenticated_executor,
                presentation,
                source_node,
                requested,
                query,
            )?
            .map(|items| items.into_iter().next());
        if result.complete
            && result.requested_fully_authorized
            && result.value.as_ref().is_some_and(Option::is_none)
        {
            result.visibility = ResourceVisibility::AuthoritativelyAbsent;
        }
        Ok(result)
    }

    #[allow(clippy::too_many_lines)] // Keeps one fail-closed visibility derivation auditable.
    #[allow(clippy::needless_pass_by_value)] // The typed query is a one-shot snapshot request.
    pub(super) fn query_items_selected_at<Q>(
        &self,
        read: SelectedQueryRead<'_>,
        query: Q,
    ) -> Result<SelectedQueryResult<ItemQueryResult<Q>>, NodeError>
    where
        Q: ItemQuery,
    {
        let SelectedQueryRead {
            authenticated_executor,
            presentation,
            source_node,
            requested,
            phase: authorization_phase,
            through,
        } = read;
        let snapshot = SelectedHistorySnapshot::at(self, through)?;
        let topology = &snapshot.topology;
        let Some(policy) = self
            .command_access_policy
            .read()
            .map_err(|_| NodeError::Poisoned)?
            .as_ref()
            .and_then(Weak::upgrade)
        else {
            return Err(AuthorityUnavailable::PolicyUnavailable.into());
        };
        let mut access = AccessAttempt::scoped(
            authenticated_executor,
            presentation,
            AccessOperation::ReadItems,
            requested.root().clone(),
        );
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        access.target = AccessTarget::Items {
            source_node: Some(source_node),
            service_id: service_id.clone(),
            scope_id: requested.root().clone(),
            item_type: Q::Item::ITEM_TYPE.to_owned(),
        };
        access.resource_claims = vec![ResourceClaim {
            selection: requested.clone(),
            kind: ResourceClaimKind::Primary,
            source_node: Some(source_node),
            service_id: Some(service_id),
            item_type: Some(Q::Item::ITEM_TYPE.to_owned()),
            item_id: None,
            required_permissions: vec![FederationPermission::ReadState],
            required_operations: vec![AccessOperation::ReadItems],
            required_capabilities: Vec::new(),
        }];
        access.authorization_phase = authorization_phase;
        access.topology = Some(topology.clone());
        let (authorized, policy_covers_request) = match policy.constrain_replication(
            &access,
            &ReplicationSelection::Scopes(vec![requested.clone()]),
            topology,
        ) {
            Ok(ReplicationSelection::Scopes(authorized)) => (authorized, true),
            Ok(ReplicationSelection::Intersection { scopes, .. }) => {
                let covers = scopes
                    .iter()
                    .any(|selection| selection.covers_in(requested, topology));
                (scopes, covers)
            }
            Ok(selection) => match selection {
                ReplicationSelection::ServiceScope { scope_id, .. } => {
                    (vec![ScopeSelection::Exact(scope_id)], true)
                }
                ReplicationSelection::All | ReplicationSelection::Service(_) => {
                    (vec![requested.clone()], true)
                }
                ReplicationSelection::Scopes(_) | ReplicationSelection::Intersection { .. } => {
                    (Vec::new(), false)
                }
            },
            Err(decision) => {
                let decision = decision.into_decision()?;
                return Ok(SelectedQueryResult {
                    value: None,
                    visibility: match decision {
                        AuthorizationDecision::Deny(ref denied) => denied.visibility,
                        AuthorizationDecision::Challenge { .. } => ResourceVisibility::Unauthorized,
                        AuthorizationDecision::Permit(_) => ResourceVisibility::Unbound,
                    },
                    coverage: if source_node == self.node_id() {
                        ProjectionCoverage::LocalAuthoritative
                    } else {
                        ProjectionCoverage::ReplicatedIncomplete
                    },
                    through: None,
                    complete: false,
                    requested_fully_authorized: false,
                    authorization: Some(decision),
                    included_scopes: Vec::new(),
                });
            }
        };
        let requested_scopes = match requested {
            ScopeSelection::Exact(scope) => vec![scope.clone()],
            ScopeSelection::Subtree(root) => std::iter::once(root.clone())
                .chain(topology.descendants(root))
                .collect(),
        };
        let fully_authorized = policy_covers_request
            && requested_scopes.iter().all(|scope| {
                authorized
                    .iter()
                    .any(|selection| selection.contains_scope(scope, topology))
            });
        let (mut coverage, mut through) = self.selected_projection_coverage(
            source_node,
            &ServiceId::new(Q::Item::SERVICE_ID),
            &authorized,
            &snapshot,
        )?;
        if !matches!(
            coverage,
            ProjectionCoverage::Unreachable | ProjectionCoverage::Undiscoverable
        ) && snapshot.has_pending_for::<Q::Item>(source_node, &authorized)
        {
            coverage = ProjectionCoverage::HistoryIncomplete;
            through = None;
        }
        let source_complete = matches!(
            coverage,
            ProjectionCoverage::LocalAuthoritative | ProjectionCoverage::ReplicatedComplete
        );
        let topology_complete = !policy_covers_request
            || !matches!(requested, ScopeSelection::Subtree(root) if !topology.knows(root));
        let complete = source_complete && topology_complete;
        let mut projection = ItemProjection::default();
        for (index, envelope) in snapshot.ready.iter().enumerate() {
            if envelope.origin.node_id != source_node {
                continue;
            }
            let NodeEvent::CommandCommitted { command, .. } = &envelope.event else {
                continue;
            };
            if command.request.service_id != Q::Item::SERVICE_ID {
                continue;
            }
            let _changed = apply_selected_item_envelope(
                &mut projection,
                envelope,
                Some(source_node),
                requested,
                Some(&authorized),
                topology,
                projection_revision(index)?,
            )?;
        }
        let visibility = match coverage {
            ProjectionCoverage::Unreachable => ResourceVisibility::Unreachable,
            ProjectionCoverage::Undiscoverable => ResourceVisibility::Undiscoverable,
            ProjectionCoverage::ReplicatedIncomplete => ResourceVisibility::NotReplicated,
            ProjectionCoverage::HistoryIncomplete => ResourceVisibility::HistoryIncomplete,
            ProjectionCoverage::LocalAuthoritative | ProjectionCoverage::ReplicatedComplete
                if !topology_complete =>
            {
                ResourceVisibility::TopologyIncomplete
            }
            ProjectionCoverage::LocalAuthoritative | ProjectionCoverage::ReplicatedComplete => {
                ResourceVisibility::Present
            }
        };
        Ok(SelectedQueryResult {
            value: Some(__snapshot_item_query(&query, &projection)),
            visibility,
            coverage,
            through,
            complete,
            requested_fully_authorized: fully_authorized,
            authorization: None,
            included_scopes: authorized
                .iter()
                .map(|selection| selection.root().clone())
                .collect(),
        })
    }

    /// Starts a gap-free selected query watch. Authorization and local-source
    /// completeness are derived exactly as in [`Self::query_items_selected`].
    ///
    /// # Errors
    ///
    /// Returns an error when the initial snapshot or event subscription cannot
    /// be established.
    pub fn watch_items_selected<Q>(
        &self,
        authenticated_executor: PrincipalId,
        presentation: AuthorityPresentation,
        source_node: NodeId,
        requested: ScopeSelection,
        query: Q,
    ) -> Result<
        (
            SelectedQueryResult<ItemQueryResult<Q>>,
            SelectedQueryWatch<Q>,
        ),
        NodeError,
    >
    where
        Q: ItemQuery,
    {
        let through = self.backend.latest_position()?;
        let snapshot = self.query_items_selected_at(
            SelectedQueryRead {
                authenticated_executor: authenticated_executor.clone(),
                presentation: presentation.clone(),
                source_node,
                requested: &requested,
                phase: AuthorizationPhase::Admission,
                through,
            },
            query.clone(),
        )?;
        let mut events = self.subscribe(through)?;
        let authorization_revision = self
            .command_access_policy
            .read()
            .map_err(|_| NodeError::Poisoned)?
            .as_ref()
            .and_then(Weak::upgrade)
            .and_then(|policy| policy.revision_cell());
        let (wake_send, wake) = flume::bounded(1);
        let authorization_guard = authorization_revision.map(|revision| {
            let wake_send = wake_send.clone();
            revision.subscribe(move |_| {
                let _ = wake_send.try_send(SelectedQueryWake::Policy);
            })
        });
        let periodic_recheck = authorization_guard.is_none();
        std::thread::Builder::new()
            .name("myko-selected-query-watch".to_owned())
            .spawn(move || {
                loop {
                    match events.recv_timeout(Duration::from_millis(50)) {
                        Ok(Some(event)) => {
                            if wake_send
                                .send(SelectedQueryWake::Event(event.position))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(_) => break,
                        Ok(None) => {
                            if periodic_recheck
                                && wake_send.try_send(SelectedQueryWake::Timer).is_err()
                                && wake_send.is_disconnected()
                            {
                                break;
                            }
                        }
                    }
                }
            })
            .map_err(|error| NodeError::Backend(error.to_string()))?;
        Ok((
            snapshot,
            SelectedQueryWatch {
                node: self.clone(),
                authenticated_executor,
                presentation,
                source_node,
                requested,
                query,
                wake,
                _authorization_guard: authorization_guard,
                cursor: through,
            },
        ))
    }

    /// Returns authoritative sources that have changed one typed item schema
    /// in the requested service scope.
    ///
    /// Source discovery is derived inside the framework so applications never
    /// inspect command envelopes or serialized mutation payloads.
    ///
    /// # Errors
    ///
    /// Returns an error when durable history cannot be read.
    pub fn item_sources_in<T: MykoItem>(
        &self,
        scope_id: &ScopeId,
    ) -> Result<Vec<NodeId>, NodeError> {
        let mut sources = BTreeMap::new();
        for envelope in self.events_after(None)? {
            let NodeEvent::CommandCommitted { command, batch } = envelope.event else {
                continue;
            };
            if command.request.service_id == T::SERVICE_ID
                && batch.changes.iter().any(|mutation| {
                    mutation.is::<T>()
                        && mutation.affects_scope::<T>(batch.scope_id.as_str(), scope_id.as_str())
                })
            {
                sources
                    .entry(envelope.origin.node_id.to_string())
                    .or_insert(envelope.origin.node_id);
            }
        }
        Ok(sources.into_values().collect())
    }

    /// Starts a gap-free replay-then-live typed query watch within one source,
    /// application service, and federation scope.
    ///
    /// The returned snapshot covers every event through its cursor. The watch
    /// begins strictly after that cursor, including events committed while the
    /// initial projection was being built.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read, a matching item mutation
    /// is malformed, or a gap-free subscription cannot be established.
    pub fn watch_items_in<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<(ItemQuerySnapshot<ItemQueryResult<Q>>, ItemQueryWatch<Q>), NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let mut projection = ItemProjection::default();
        for envelope in &history {
            let _changed = apply_item_envelope(
                &mut projection,
                envelope,
                Some(source_node),
                Some((&service_id, &scope_id)),
            )?;
        }
        let snapshot = ItemQuerySnapshot {
            through,
            value: __snapshot_item_query(&query, &projection),
        };
        let events = self.subscribe(through)?;
        Ok((
            snapshot,
            ItemQueryWatch {
                query,
                projection,
                source_node: Some(source_node),
                node: self.clone(),
                service_id,
                scope_id: Some(scope_id),
                events,
            },
        ))
    }

    /// Opens a gap-free typed query across every scope owned by one source.
    ///
    /// The returned snapshot and watch share one event-log boundary. Retaining
    /// the watch therefore observes every later matching commit without
    /// polling or a snapshot/subscription race.
    ///
    /// # Errors
    ///
    /// Returns an error if history cannot be projected or its live
    /// continuation cannot be opened.
    pub fn watch_items_from<Q>(
        &self,
        source_node: NodeId,
        query: Q,
    ) -> Result<(ItemQuerySnapshot<ItemQueryResult<Q>>, ItemQueryWatch<Q>), NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let mut projection = ItemProjection::default();
        for envelope in &history {
            let _changed = apply_item_envelope(&mut projection, envelope, Some(source_node), None)?;
        }
        let snapshot = ItemQuerySnapshot {
            through,
            value: __snapshot_item_query(&query, &projection),
        };
        let events = self.subscribe(through)?;
        Ok((
            snapshot,
            ItemQueryWatch {
                query,
                projection,
                source_node: Some(source_node),
                node: self.clone(),
                service_id,
                scope_id: None,
                events,
            },
        ))
    }

    /// Starts a gap-free typed query watch within one application service and
    /// federation scope across every authoritative source represented here.
    ///
    /// Newly ingested events from any source enter the same typed projection;
    /// callers never need to inspect federation envelopes or poll for sources.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read, a matching item mutation
    /// is malformed, or the subscription cannot be established.
    pub fn watch_items_across_sources_in<Q>(
        &self,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<(ItemQuerySnapshot<ItemQueryResult<Q>>, ItemQueryWatch<Q>), NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        let (through, history) = self.causal_snapshot()?;
        let projection = project_item_history(&history, None, Some((&service_id, &scope_id)))?;
        let snapshot = ItemQuerySnapshot {
            through,
            value: __snapshot_item_query(&query, &projection),
        };
        let events = self.subscribe(through)?;
        Ok((
            snapshot,
            ItemQueryWatch {
                query,
                projection,
                source_node: None,
                node: self.clone(),
                service_id,
                scope_id: Some(scope_id),
                events,
            },
        ))
    }

    /// Opens the framework's shared typed projection source.
    ///
    /// `source_node` selects one authoritative origin when present; `scope_id`
    /// selects one concrete scope when present. Myko's application layer owns
    /// the resulting driver and lets every query/report/view over the same
    /// tuple derive from one Hyphae source.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be projected or the gap-free
    /// follow cannot be established.
    #[doc(hidden)]
    pub fn watch_item_projection<T>(
        &self,
        source_node: Option<NodeId>,
        scope_id: Option<ScopeId>,
    ) -> Result<(ItemProjectionSnapshot<T>, ItemProjectionWatch<T>), NodeError>
    where
        T: MykoItem,
    {
        let service_id = ServiceId::new(T::SERVICE_ID);
        let (through, history) = if source_node.is_none() {
            self.causal_snapshot()?
        } else {
            let history = self.events_after(None)?;
            (history.last().map(|event| event.position), history)
        };
        let service_scope = scope_id.as_ref().map(|scope_id| (&service_id, scope_id));
        let projection = project_item_history(&history, source_node, service_scope)?;
        let snapshot = ItemProjectionSnapshot {
            through,
            projection: projection.clone(),
        };
        let events = self.subscribe(through)?;
        Ok((
            snapshot,
            ItemProjectionWatch {
                projection,
                source_node,
                node: self.clone(),
                service_id,
                scope_id,
                events,
            },
        ))
    }

    /// Returns every scope observed in immutable history in stable order.
    ///
    /// This is a local projection primitive. A transport must authorize each
    /// scope before disclosing its identifier to a remote principal.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn scope_ids(&self) -> Result<Vec<ScopeId>, NodeError> {
        let mut scopes = Vec::new();
        let mut after = None;
        loop {
            let page = self.scope_ids_page(after.as_ref(), DURABLE_EVENT_PAGE_LIMIT)?;
            let page_len = page.len();
            after = page.last().cloned();
            scopes.extend(page);
            if page_len < DURABLE_EVENT_PAGE_SIZE {
                break;
            }
        }
        Ok(scopes)
    }

    /// Returns one sorted page of scopes without materializing the full catalog.
    ///
    /// # Errors
    ///
    /// Returns an error when backend state cannot be read.
    pub fn scope_ids_page(
        &self,
        after: Option<&ScopeId>,
        limit: NonZeroUsize,
    ) -> Result<Vec<ScopeId>, NodeError> {
        self.backend.scope_ids_page(after, limit)
    }

    /// Reconstructs nested-scope parentage from dependency-complete history.
    ///
    /// # Errors
    ///
    /// Returns an error when history contains a cycle or attempts to reparent
    /// an existing scope root.
    pub fn scope_topology(&self) -> Result<ScopeTopology, NodeError> {
        self.backend.scope_topology()
    }

    /// Creates a replay-then-live subscription without a cursor gap.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot establish the subscription.
    pub fn subscribe(&self, after: Option<LogPosition>) -> Result<EventSubscription, NodeError> {
        self.backend.subscribe(after)
    }

    /// Starts a gap-free subscription after the node's current durable boundary.
    ///
    /// Existing history is used only to capture the boundary and is not
    /// replayed to the caller. An event committed concurrently with this call
    /// is still delivered by the backend subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when history or the backend subscription cannot be
    /// read.
    pub fn subscribe_from_now(&self) -> Result<EventSubscription, NodeError> {
        let through = self.backend.latest_position()?;
        self.subscribe(through)
    }

    /// Starts a gap-free opaque notification stream for application item changes.
    ///
    /// Existing history establishes the durable boundary but is not replayed.
    /// Command-only lifecycle transitions are filtered inside Myko, so callers
    /// never need to inspect wire envelopes or command identities.
    ///
    /// # Errors
    ///
    /// Returns an error when history or the backend subscription cannot be read.
    pub fn subscribe_item_changes_from_now(&self) -> Result<ItemChangeSubscription, NodeError> {
        self.subscribe_from_now()
            .map(|events| ItemChangeSubscription { events })
    }

    /// Idempotently ingests an immutable event received from another node.
    ///
    /// # Errors
    ///
    /// Returns an error when replicated history conflicts with local command
    /// identity or contains an invalid atomic batch.
    pub fn ingest(&self, event: EventEnvelope) -> Result<IngestStatus, NodeError> {
        self.backend.ingest(event)
    }

    /// Persist a controller response before returning its signature.
    ///
    /// The key must belong to one durable controller. Sharing it across independent
    /// stores or rolling back its history violates crash-fault voting assumptions.
    /// This local framework API does not authorize a remote request or activate an epoch.
    ///
    /// # Errors
    /// Rejects unavailable persistence, journal failure, corrupt retained votes,
    /// superseded ballots, and conflicting same-ballot values. A journal that
    /// differs from live state requires reopening before further votes.
    pub fn vote_control(
        &self,
        request: &crate::control_quorum::ControlVoteRequest<'_>,
        key: &ed25519_dalek::SigningKey,
    ) -> Result<crate::control_quorum::SignedControlVote, NodeError> {
        self.backend.vote_control(request, key)
    }

    /// Persist one proposer's value and prepare proof before it can be accepted.
    ///
    /// Exact retries recover the original signed proposal, including after reopen.
    /// The key must not be shared across independent stores or rolled back.
    /// This does not allocate ballots, run a coordinator, or activate authority.
    ///
    /// # Errors
    /// Rejects wrong keys, conflicting retained proposals, journal failure, and
    /// durable/live history disagreement requiring reopen.
    pub fn propose_control(
        &self,
        request: &crate::control_quorum::ControlProposalRequest<'_>,
        key: &ed25519_dalek::SigningKey,
    ) -> Result<crate::control_quorum::SignedControlProposal, NodeError> {
        self.backend.propose_control(request, key)
    }

    /// Durably records an unverified retained-history assertion.
    ///
    /// This does not verify the signature, obligation, membership, authority,
    /// or custody. Callers must establish those conditions before relying on
    /// the persisted statement.
    ///
    /// # Errors
    ///
    /// Returns an error when durable storage is unavailable or the statement
    /// does not exactly match the supplied manifest, local holder, and store identity.
    pub fn record_retained_history_statement(
        &self,
        signed: SignedRetainedHistoryStatement,
        manifest: &SelectedHistoryManifest,
    ) -> Result<EventEnvelope, NodeError> {
        self.backend
            .record_retained_history_statement(signed, manifest)
    }

    /// Exports immutable events after an exclusive local replay cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn export(&self, after: Option<LogPosition>) -> Result<ReplicationBatch, NodeError> {
        let events = self.backend.events_page(after, DURABLE_EVENT_PAGE_LIMIT)?;
        let through = events.last().map(|event| event.position).or(after);
        Ok(ReplicationBatch {
            source_node: self.node_id(),
            after,
            through,
            events,
        })
    }

    /// Exports one exact application scope while retaining the source cursor.
    ///
    /// The cursor advances across unrelated events without disclosing them.
    /// Consumers must keep separate cursors for separate source/scope pairs.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn export_scope(
        &self,
        scope_id: ScopeId,
        after: Option<LogPosition>,
    ) -> Result<ScopedReplicationBatch, NodeError> {
        let suffix = self.backend.events_page(after, DURABLE_EVENT_PAGE_LIMIT)?;
        let through = suffix.last().map(|event| event.position).or(after);
        let events = suffix
            .into_iter()
            .filter(|event| match &event.event {
                NodeEvent::FrameworkControl(control) => matches!(
                    control.selection(),
                    ScopeSelection::Exact(control_scope)
                        if control_scope.equivalent_to(&scope_id)
                ),
                NodeEvent::CommandLifecycle(_) | NodeEvent::CommandCommitted { .. } => event
                    .event
                    .affected_scope_ids()
                    .iter()
                    .all(|affected| affected.equivalent_to(&scope_id)),
            })
            .collect();
        Ok(ScopedReplicationBatch {
            source_node: self.node_id(),
            scope_id,
            after,
            through,
            events,
        })
    }

    /// Exports the selected application history while retaining the source cursor.
    ///
    /// The cursor stops before unresolved history. Advancing past an event whose
    /// scope relationships are not ready could omit it permanently from a subtree.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn export_selected(
        &self,
        selection: ReplicationSelection,
        after: Option<LogPosition>,
    ) -> Result<SelectedReplicationBatch, NodeError> {
        let ready_history = match self.backend.latest_position()? {
            Some(through) => self.backend.causal_events_through(through)?,
            None => Vec::new(),
        };
        let topology = ScopeTopology::from_events(&ready_history)?;
        let ready_origins = ready_history
            .iter()
            .map(|event| event.origin)
            .collect::<HashSet<_>>();
        let history = self.backend.events_page(after, DURABLE_EVENT_PAGE_LIMIT)?;
        let mut through = after;
        let mut events = Vec::new();
        for event in history {
            if !ready_origins.contains(&event.origin) {
                break;
            }
            through = Some(event.position);
            if selection.includes_in(&event.event, &topology) {
                events.push(event);
            }
        }
        let topology_proof = topology.proof_for(match &selection {
            ReplicationSelection::Scopes(scopes)
            | ReplicationSelection::Intersection { scopes, .. } => scopes.as_slice(),
            ReplicationSelection::All
            | ReplicationSelection::Service(_)
            | ReplicationSelection::ServiceScope { .. } => &[],
        });
        Ok(SelectedReplicationBatch {
            source_node: self.node_id(),
            selection,
            after,
            through,
            topology: topology_proof,
            events,
        })
    }

    /// Applies a transport-delivered replication batch idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error when any event conflicts with stable command identity
    /// or contains an invalid atomic change batch.
    pub fn ingest_batch(&self, batch: ReplicationBatch) -> Result<ReplicationReport, NodeError> {
        validate_replication_batch(&batch)?;
        let source_node = batch.source_node;
        let after = batch.after;
        let through = batch.through;
        let mut applied = 0usize;
        let mut duplicates = 0usize;
        for event in batch.events {
            match self.ingest(event)? {
                IngestStatus::Applied { .. } => applied = applied.saturating_add(1),
                IngestStatus::Duplicate => duplicates = duplicates.saturating_add(1),
            }
        }
        self.record_replication_coverage(source_node, ReplicationSelection::All, after, through)?;
        Ok(ReplicationReport {
            source_node,
            through,
            applied,
            duplicates,
        })
    }

    /// Applies a scope-filtered transport batch idempotently.
    ///
    /// Source positions may contain gaps, but every included event must belong
    /// to the declared scope and lie strictly inside the cursor interval.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch cursor is invalid, an event belongs to a
    /// different scope, or replicated command identity conflicts locally.
    pub fn ingest_scoped_batch(
        &self,
        batch: ScopedReplicationBatch,
    ) -> Result<ScopedReplicationReport, NodeError> {
        validate_scoped_replication_batch(&batch)?;
        let source_node = batch.source_node;
        let scope_id = batch.scope_id.clone();
        let after = batch.after;
        let through = batch.through;
        let mut applied = 0usize;
        let mut duplicates = 0usize;
        for event in batch.events {
            match self.ingest(event)? {
                IngestStatus::Applied { .. } => applied = applied.saturating_add(1),
                IngestStatus::Duplicate => duplicates = duplicates.saturating_add(1),
            }
        }
        self.record_replication_coverage(
            source_node,
            ReplicationSelection::Scopes(vec![ScopeSelection::Exact(scope_id.clone())]),
            after,
            through,
        )?;
        Ok(ScopedReplicationReport {
            source_node,
            scope_id,
            through,
            applied,
            duplicates,
        })
    }

    /// Applies a selection-filtered transport batch idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch cursor is invalid, an event falls
    /// outside the declared selection, or replicated command identity conflicts.
    pub fn ingest_selected_batch(
        &self,
        batch: SelectedReplicationBatch,
    ) -> Result<SelectedReplicationReport, NodeError> {
        let mut topology = self.scope_topology()?;
        validate_selected_replication_batch(&batch, &mut topology)?;
        let source_node = batch.source_node;
        let selection = batch.selection.clone();
        let after = batch.after;
        let through = batch.through;
        let mut applied = 0usize;
        let mut duplicates = 0usize;
        for event in batch.events {
            match self.ingest(event)? {
                IngestStatus::Applied { .. } => applied = applied.saturating_add(1),
                IngestStatus::Duplicate => duplicates = duplicates.saturating_add(1),
            }
        }
        self.record_replication_coverage(source_node, selection.clone(), after, through)?;
        Ok(SelectedReplicationReport {
            source_node,
            selection,
            through,
            applied,
            duplicates,
        })
    }
}

fn validate_replication_batch(batch: &ReplicationBatch) -> Result<(), NodeError> {
    let mut expected = batch
        .after
        .map_or(Ok(LogPosition::FIRST), LogPosition::next)?;
    for event in &batch.events {
        if event.position != expected {
            return Err(NodeError::InvalidReplicationBatch(format!(
                "expected source position {}, received {}",
                expected.get(),
                event.position.get()
            )));
        }
        expected = expected.next()?;
    }
    let observed_through = batch
        .events
        .last()
        .map(|event| event.position)
        .or(batch.after);
    if batch.through != observed_through {
        return Err(NodeError::InvalidReplicationBatch(format!(
            "declared through {:?} does not match observed {:?}",
            batch.through, observed_through
        )));
    }
    Ok(())
}

fn validate_scoped_replication_batch(batch: &ScopedReplicationBatch) -> Result<(), NodeError> {
    if matches!((batch.after, batch.through), (Some(_), None))
        || matches!((batch.after, batch.through), (Some(after), Some(through)) if through < after)
    {
        return Err(NodeError::InvalidReplicationBatch(
            "scoped replication cursor moved backwards".to_owned(),
        ));
    }
    let mut previous = batch.after;
    for event in &batch.events {
        if previous.is_some_and(|position| event.position <= position)
            || batch.through.is_none_or(|through| event.position > through)
        {
            return Err(NodeError::InvalidReplicationBatch(format!(
                "scoped event position {} is outside its cursor interval",
                event.position.get()
            )));
        }
        let belongs_to_scope = match &event.event {
            NodeEvent::FrameworkControl(control) => matches!(
                control.selection(),
                ScopeSelection::Exact(scope) if scope.equivalent_to(&batch.scope_id)
            ),
            NodeEvent::CommandLifecycle(_) | NodeEvent::CommandCommitted { .. } => event
                .event
                .affected_scope_ids()
                .iter()
                .all(|scope_id| scope_id.equivalent_to(&batch.scope_id)),
        };
        if !belongs_to_scope {
            return Err(NodeError::InvalidReplicationBatch(format!(
                "event at position {} does not belong to scope {}",
                event.position.get(),
                batch.scope_id
            )));
        }
        previous = Some(event.position);
    }
    Ok(())
}

fn validate_selected_replication_batch(
    batch: &SelectedReplicationBatch,
    topology: &mut ScopeTopology,
) -> Result<(), NodeError> {
    if matches!((batch.after, batch.through), (Some(_), None))
        || matches!((batch.after, batch.through), (Some(after), Some(through)) if through < after)
    {
        return Err(NodeError::InvalidReplicationBatch(
            "selected replication cursor moved backwards".to_owned(),
        ));
    }
    topology.merge_proof(&batch.topology)?;
    if let ReplicationSelection::Intersection { requested, scopes } = &batch.selection {
        let safely_bounded = scopes.iter().all(|scope| match requested.as_ref() {
            ReplicationSelection::All
            | ReplicationSelection::Service(_)
            | ReplicationSelection::ServiceScope { .. } => true,
            ReplicationSelection::Scopes(requested) => requested
                .iter()
                .any(|requested| requested.covers_in(scope, topology)),
            ReplicationSelection::Intersection {
                requested,
                scopes: outer_scopes,
            } => {
                outer_scopes
                    .iter()
                    .any(|allowed| allowed.covers_in(scope, topology))
                    && match requested.as_ref() {
                        ReplicationSelection::All
                        | ReplicationSelection::Service(_)
                        | ReplicationSelection::ServiceScope { .. } => true,
                        ReplicationSelection::Scopes(requested) => requested
                            .iter()
                            .any(|requested| requested.covers_in(scope, topology)),
                        ReplicationSelection::Intersection { .. } => false,
                    }
            }
        });
        if !safely_bounded {
            return Err(NodeError::InvalidReplicationBatch(
                "effective replication intersection is not proven beneath its request".to_owned(),
            ));
        }
    }
    let mut previous = batch.after;
    for event in &batch.events {
        if previous.is_some_and(|position| event.position <= position)
            || batch.through.is_none_or(|through| event.position > through)
        {
            return Err(NodeError::InvalidReplicationBatch(format!(
                "selected event position {} is outside its cursor interval",
                event.position.get()
            )));
        }
        topology.observe_event(&event.event)?;
        if !batch.selection.includes_in(&event.event, topology) {
            return Err(NodeError::InvalidReplicationBatch(format!(
                "event at position {} falls outside its replication selection",
                event.position.get()
            )));
        }
        previous = Some(event.position);
    }
    Ok(())
}

pub(super) fn validate_change_batch(batch: &ChangeBatch) -> Result<(), NodeError> {
    for mutation in &batch.changes {
        mutation
            .validate_envelope()
            .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?;
        if mutation.service_id != batch.service_id.as_str() {
            return Err(NodeError::InvalidItemMutation(format!(
                "item mutation belongs to service {}, batch belongs to {}",
                mutation.service_id, batch.service_id
            )));
        }
        if mutation.scope_id.as_deref() == Some("") {
            return Err(NodeError::InvalidItemMutation(
                "item mutation has an empty scope ID".to_owned(),
            ));
        }
        if mutation.operation == MutationOperation::Set
            && mutation.roots_scope
            && let Some(parent) = mutation.belongs_to.as_ref()
        {
            let child =
                ScopeId::for_parts(&mutation.service_id, &mutation.item_type, &mutation.item_id);
            let placed_scope = mutation
                .scope_id
                .as_deref()
                .unwrap_or(batch.scope_id.as_str());
            if placed_scope != child.as_str() {
                return Err(NodeError::InvalidItemMutation(format!(
                    "nested scope root {child} was placed in scope {placed_scope}"
                )));
            }
            let parent = ScopeId::for_entity(parent);
            if parent == child {
                return Err(NodeError::InvalidItemMutation(format!(
                    "scope {child} cannot belong to itself"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn apply_item_envelope<T: MykoItem>(
    projection: &mut ItemProjection<T>,
    envelope: &EventEnvelope,
    source_node: Option<NodeId>,
    service_scope: Option<(&ServiceId, &ScopeId)>,
) -> Result<bool, NodeError> {
    apply_item_envelope_at_revision(
        projection,
        envelope,
        source_node,
        service_scope,
        envelope.position.get(),
    )
}

pub(super) fn project_item_history<T: MykoItem>(
    history: &[EventEnvelope],
    source_node: Option<NodeId>,
    service_scope: Option<(&ServiceId, &ScopeId)>,
) -> Result<ItemProjection<T>, NodeError> {
    let mut projection = ItemProjection::default();
    if source_node.is_some() {
        for envelope in history {
            apply_item_envelope(&mut projection, envelope, source_node, service_scope)?;
        }
    } else {
        for (index, envelope) in history
            .iter()
            .filter(|envelope| item_history_scope_matches::<T>(envelope, service_scope))
            .enumerate()
        {
            let revision = projection_revision(index)?;
            apply_item_envelope_at_revision(
                &mut projection,
                envelope,
                None,
                service_scope,
                revision,
            )?;
        }
    }
    Ok(projection)
}

fn item_history_scope_matches<T: MykoItem>(
    envelope: &EventEnvelope,
    service_scope: Option<(&ServiceId, &ScopeId)>,
) -> bool {
    command_from_event(&envelope.event)
        .is_some_and(|command| command.request.service_id == T::SERVICE_ID)
        && service_scope.is_none_or(|(_, scope)| {
            envelope.event.scope_id().equivalent_to(scope)
                || matches!(&envelope.event, NodeEvent::CommandCommitted { batch, .. }
                if batch.changes.iter().any(|mutation| {
                    mutation.scope_id.as_ref().is_some_and(|placed| {
                        ScopeId::new(placed.clone()).equivalent_to(scope)
                    })
                }))
        })
}

fn apply_item_envelope_at_revision<T: MykoItem>(
    projection: &mut ItemProjection<T>,
    envelope: &EventEnvelope,
    source_node: Option<NodeId>,
    service_scope: Option<(&ServiceId, &ScopeId)>,
    revision: u64,
) -> Result<bool, NodeError> {
    if source_node.is_some_and(|source| source != envelope.origin.node_id) {
        return Ok(false);
    }
    let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
        return Ok(false);
    };
    if service_scope.is_some_and(|(service, _)| &command.request.service_id != service) {
        return Ok(false);
    }
    let mut changed = false;
    for (index, mutation) in batch.changes.iter().enumerate() {
        if service_scope.is_some_and(|(_, scope)| {
            !mutation.affects_scope::<T>(batch.scope_id.as_str(), scope.as_str())
        }) {
            continue;
        }
        let change_index = u32::try_from(index).map_err(|error| {
            NodeError::CorruptHistory(format!(
                "item batch contains too many ordered changes: {error}"
            ))
        })?;
        changed |= projection
            .apply_at_order_in_scope(
                mutation,
                Some(batch.scope_id.as_str()),
                revision,
                change_index,
            )
            .map_err(|error| NodeError::CorruptHistory(error.to_string()))?;
    }
    Ok(changed)
}

fn projection_revision(index: usize) -> Result<u64, NodeError> {
    u64::try_from(index)
        .ok()
        .and_then(|index| index.checked_add(1))
        .ok_or(NodeError::PositionExhausted)
}

fn apply_selected_item_envelope<T: MykoItem>(
    projection: &mut ItemProjection<T>,
    envelope: &EventEnvelope,
    source_node: Option<NodeId>,
    requested: &ScopeSelection,
    authorized: Option<&[ScopeSelection]>,
    topology: &ScopeTopology,
    revision: u64,
) -> Result<bool, NodeError> {
    if source_node.is_some_and(|source| envelope.origin.node_id != source) {
        return Ok(false);
    }
    let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
        return Ok(false);
    };
    if command.request.service_id != T::SERVICE_ID {
        return Ok(false);
    }
    let mut changed = false;
    for (index, mutation) in batch.changes.iter().enumerate() {
        let Some(scope) = item_mutation_scope::<T>(mutation, &batch.scope_id) else {
            continue;
        };
        if !requested.contains_scope(&scope, topology)
            || authorized.is_some_and(|selections| {
                !selections
                    .iter()
                    .any(|selection| selection.contains_scope(&scope, topology))
            })
        {
            continue;
        }
        let change_index = u32::try_from(index).map_err(|error| {
            NodeError::CorruptHistory(format!(
                "item batch contains too many ordered changes: {error}"
            ))
        })?;
        changed |= projection
            .apply_at_order_in_scope(
                mutation,
                Some(batch.scope_id.as_str()),
                revision,
                change_index,
            )
            .map_err(|error| NodeError::CorruptHistory(error.to_string()))?;
    }
    Ok(changed)
}

fn item_mutation_scope<T: MykoItem>(
    mutation: &ItemMutation,
    batch_scope: &ScopeId,
) -> Option<ScopeId> {
    if !mutation.is::<T>() {
        return None;
    }
    if let Some(scope) = &mutation.scope_id {
        return Some(ScopeId::new(scope.clone()));
    }
    Some(batch_scope.clone())
}
