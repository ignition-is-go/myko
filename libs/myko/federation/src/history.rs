use std::collections::BTreeSet;

use super::*;

/// One immutable entry in node history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Cursor in the observing node's replay log.
    pub position: LogPosition,
    /// Stable identity assigned by the node that originally accepted the event.
    pub origin: EventId,
    pub recorded_at: DateTime<Utc>,
    pub event: NodeEvent,
}

/// Outcome of ingesting a replicated immutable event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IngestStatus {
    Applied { position: LogPosition },
    Duplicate,
}

/// Immutable events exported from one peer's local replay cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationBatch {
    pub source_node: NodeId,
    pub after: Option<LogPosition>,
    pub through: Option<LogPosition>,
    pub events: Vec<EventEnvelope>,
}

/// Parent relationships between concrete, service-qualified scope roots.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeTopology {
    parents: BTreeMap<ScopeId, ScopeId>,
    known: BTreeSet<ScopeId>,
}

impl ScopeTopology {
    pub(super) fn from_events(events: &[EventEnvelope]) -> Result<Self, NodeError> {
        let mut topology = Self::default();
        for envelope in events {
            topology.observe_event(&envelope.event)?;
        }
        Ok(topology)
    }

    /// Incorporates nested scope roots established by one immutable event.
    ///
    /// # Errors
    ///
    /// Returns an error when the event reparents a scope, creates a cycle, or
    /// places a nested root outside the scope identified by that root.
    pub fn observe_event(&mut self, event: &NodeEvent) -> Result<(), NodeError> {
        if matches!(event, NodeEvent::FrameworkControl(_)) {
            return Ok(());
        }
        self.known.extend(event.affected_scope_ids());
        let NodeEvent::CommandCommitted { batch, .. } = event else {
            return Ok(());
        };
        for mutation in &batch.changes {
            if mutation.operation != MutationOperation::Set || !mutation.roots_scope {
                continue;
            }
            let Some(parent) = mutation.belongs_to.as_ref() else {
                continue;
            };
            let child =
                ScopeId::for_parts(&mutation.service_id, &mutation.item_type, &mutation.item_id);
            let placed_scope = mutation
                .scope_id
                .as_deref()
                .unwrap_or(batch.scope_id.as_str());
            if !ScopeId::new(placed_scope).equivalent_to(&child) {
                return Err(NodeError::InvalidItemMutation(format!(
                    "nested scope root {child} was placed in scope {placed_scope}"
                )));
            }
            let parent = ScopeId::for_entity(parent);
            if self.parent(&child).is_none()
                && !event
                    .affected_scope_ids()
                    .iter()
                    .any(|affected| affected.equivalent_to(&parent))
            {
                return Err(NodeError::InvalidItemMutation(format!(
                    "new nested scope {child} must be created in a batch that also covers parent scope {parent}"
                )));
            }
            self.insert(child, parent)?;
        }
        Ok(())
    }

    pub(super) fn insert(&mut self, child: ScopeId, parent: ScopeId) -> Result<(), NodeError> {
        if child.equivalent_to(&parent) {
            return Err(NodeError::InvalidItemMutation(format!(
                "scope {child} cannot belong to itself"
            )));
        }
        if let Some(existing) = self.parent(&child) {
            if !existing.equivalent_to(&parent) {
                return Err(NodeError::InvalidItemMutation(format!(
                    "scope {child} cannot move from parent {existing} to {parent}"
                )));
            }
            return Ok(());
        }
        if self.is_descendant_of(&parent, &child) {
            return Err(NodeError::InvalidItemMutation(format!(
                "scope parent {parent} would create a cycle beneath {child}"
            )));
        }
        self.parents.insert(child, parent);
        Ok(())
    }

    /// Returns the immediate parent of `scope_id`, when it is nested.
    #[must_use]
    pub fn parent(&self, scope_id: &ScopeId) -> Option<&ScopeId> {
        self.parents.get(scope_id).or_else(|| {
            self.parents
                .iter()
                .find_map(|(candidate, parent)| candidate.equivalent_to(scope_id).then_some(parent))
        })
    }

    /// Returns the nearest-to-farthest ancestors of one scope.
    #[must_use]
    pub fn ancestors(&self, scope_id: &ScopeId) -> Vec<ScopeId> {
        let mut ancestors = Vec::new();
        let mut current = scope_id;
        let mut visited = HashSet::new();
        while let Some(parent) = self.parent(current) {
            if !visited.insert(parent.clone()) {
                break;
            }
            ancestors.push(parent.clone());
            current = parent;
        }
        ancestors
    }

    /// Returns every recursively nested scope in stable textual order.
    #[must_use]
    pub fn descendants(&self, scope_id: &ScopeId) -> Vec<ScopeId> {
        let mut descendants = self
            .parents
            .keys()
            .filter(|candidate| self.is_descendant_of(candidate, scope_id))
            .cloned()
            .collect::<Vec<_>>();
        descendants.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        descendants
    }

    /// Returns whether this complete topology has observed a concrete scope.
    #[must_use]
    pub fn knows(&self, scope_id: &ScopeId) -> bool {
        self.known.contains(scope_id)
    }

    /// Returns every concrete scope observed by authoritative topology in
    /// stable order. This is used to narrow broad replication requests into
    /// explicit grant-checkable selections.
    #[must_use]
    pub fn scopes(&self) -> Vec<ScopeId> {
        let mut scopes = self.known.iter().cloned().collect::<Vec<_>>();
        scopes.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        scopes
    }

    #[doc(hidden)]
    #[must_use]
    pub fn proof_for(&self, selections: &[ScopeSelection]) -> Self {
        let mut proof = Self::default();
        for selection in selections {
            let roots = std::iter::once(selection.root().clone()).chain(match selection {
                ScopeSelection::Exact(_) => Vec::new(),
                ScopeSelection::Subtree(root) => self.descendants(root),
            });
            for scope in roots {
                proof.known.insert(scope.clone());
                let mut child = scope;
                while let Some(parent) = self.parents.get(&child) {
                    proof.known.insert(parent.clone());
                    proof.parents.insert(child.clone(), parent.clone());
                    child = parent.clone();
                }
            }
        }
        proof
    }

    #[doc(hidden)]
    pub fn merge_proof(&mut self, proof: &Self) -> Result<(), NodeError> {
        self.known.extend(proof.known.iter().cloned());
        for (child, parent) in &proof.parents {
            self.insert(child.clone(), parent.clone())?;
        }
        Ok(())
    }

    /// Returns whether `scope_id` is transitively nested under `ancestor`.
    #[must_use]
    pub fn is_descendant_of(&self, scope_id: &ScopeId, ancestor: &ScopeId) -> bool {
        self.ancestors(scope_id)
            .iter()
            .any(|value| value.equivalent_to(ancestor))
    }
}

/// Durable selection of authoritative history copied from one peer.
///
/// The selector is evaluated against each command event. Cursor watermarks
/// still advance across omitted events, so a follower can resume without
/// learning history outside its selection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ReplicationSelection {
    /// Copies every service and scope in the peer's history.
    #[default]
    All,
    /// Copies every scope owned by one service.
    Service(ServiceId),
    /// Copies one exact scope owned by one service.
    ServiceScope {
        service_id: ServiceId,
        scope_id: ScopeId,
    },
    /// Copies the union of exact scopes and complete nested subtrees.
    Scopes(Vec<ScopeSelection>),
    /// Framework-derived intersection of an original request and authorized
    /// scope pieces. Keeping the original selector prevents a service-scoped
    /// request from becoming a cross-service scope grant on the wire.
    Intersection {
        requested: Box<Self>,
        scopes: Vec<ScopeSelection>,
    },
}

/// One scope component in a composable replication selection.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "scope_id", rename_all = "snake_case")]
pub enum ScopeSelection {
    /// Selects only the named scope.
    Exact(ScopeId),
    /// Selects the named scope and every recursively nested scope.
    Subtree(ScopeId),
}

impl ScopeSelection {
    /// Returns the selected root scope.
    #[must_use]
    pub const fn root(&self) -> &ScopeId {
        match self {
            Self::Exact(scope) | Self::Subtree(scope) => scope,
        }
    }

    /// Returns whether this selection contains another selection under known
    /// authoritative topology.
    #[must_use]
    pub fn covers_in(&self, other: &Self, topology: &ScopeTopology) -> bool {
        match (self, other) {
            (Self::Exact(parent), Self::Exact(child)) => parent == child,
            (Self::Subtree(parent), Self::Exact(child) | Self::Subtree(child)) => {
                parent == child || topology.is_descendant_of(child, parent)
            }
            (Self::Exact(_), Self::Subtree(_)) => false,
        }
    }

    /// Returns whether this selection contains one concrete scope in the
    /// supplied authoritative topology.
    #[must_use]
    pub fn contains_scope(&self, scope: &ScopeId, topology: &ScopeTopology) -> bool {
        match self {
            Self::Exact(selected) => selected.equivalent_to(scope),
            Self::Subtree(selected) => {
                selected.equivalent_to(scope) || topology.is_descendant_of(scope, selected)
            }
        }
    }
}

impl ReplicationSelection {
    /// Returns whether this selector includes an event.
    #[must_use]
    pub fn includes(&self, event: &NodeEvent) -> bool {
        if let NodeEvent::FrameworkControl(control) = event {
            return self.includes_control(control, &ScopeTopology::default());
        }
        match self {
            Self::All => true,
            Self::Service(service_id) => event.service_id() == Some(service_id),
            Self::ServiceScope {
                service_id,
                scope_id,
            } => {
                event.service_id() == Some(service_id)
                    && event
                        .affected_scope_ids()
                        .iter()
                        .all(|affected| affected.equivalent_to(scope_id))
            }
            Self::Scopes(selections) => event.affected_scope_ids().iter().all(|scope_id| {
                selections.iter().any(|selection| match selection {
                    ScopeSelection::Exact(selected) | ScopeSelection::Subtree(selected) => {
                        selected.equivalent_to(scope_id)
                    }
                })
            }),
            Self::Intersection { requested, scopes } => {
                requested.includes(event)
                    && event
                        .affected_scope_ids()
                        .iter()
                        .all(|scope_id| scopes.iter().any(|selection| selection.root() == scope_id))
            }
        }
    }

    /// Returns whether this selector includes an event under the supplied
    /// nested-scope topology.
    #[must_use]
    pub fn includes_in(&self, event: &NodeEvent, topology: &ScopeTopology) -> bool {
        if let NodeEvent::FrameworkControl(control) = event {
            return self.includes_control(control, topology);
        }
        match self {
            Self::Scopes(selections) => event.affected_scope_ids().iter().all(|scope_id| {
                selections.iter().any(|selection| match selection {
                    ScopeSelection::Exact(selected) => selected.equivalent_to(scope_id),
                    ScopeSelection::Subtree(selected) => {
                        selected.equivalent_to(scope_id)
                            || topology.is_descendant_of(scope_id, selected)
                    }
                })
            }),
            Self::Intersection { requested, scopes } => {
                requested.includes_in(event, topology)
                    && event.affected_scope_ids().iter().all(|scope_id| {
                        scopes
                            .iter()
                            .any(|selection| selection.contains_scope(scope_id, topology))
                    })
            }
            Self::All | Self::Service(_) | Self::ServiceScope { .. } => self.includes(event),
        }
    }

    fn includes_control(&self, control: &FrameworkControlEvent, topology: &ScopeTopology) -> bool {
        match self {
            Self::All => true,
            Self::Service(_) | Self::ServiceScope { .. } => false,
            Self::Scopes(scopes) => scopes
                .iter()
                .any(|scope| scope.covers_in(&control.selection(), topology)),
            Self::Intersection { requested, scopes } => {
                requested.includes_control(control, topology)
                    && scopes
                        .iter()
                        .any(|scope| scope.covers_in(&control.selection(), topology))
            }
        }
    }

    pub(super) fn covers_scope_selection(
        &self,
        service_id: &ServiceId,
        requested: &ScopeSelection,
        topology: &ScopeTopology,
    ) -> bool {
        match self {
            Self::All => true,
            Self::Service(selected_service) => selected_service == service_id,
            Self::ServiceScope {
                service_id: selected_service,
                scope_id,
            } => {
                selected_service == service_id
                    && ScopeSelection::Exact(scope_id.clone()).covers_in(requested, topology)
            }
            Self::Scopes(selections) => selections
                .iter()
                .any(|selection| selection.covers_in(requested, topology)),
            Self::Intersection {
                requested: original,
                scopes,
            } => {
                original.covers_scope_selection(service_id, requested, topology)
                    && scopes
                        .iter()
                        .any(|selection| selection.covers_in(requested, topology))
            }
        }
    }
}

/// Immutable events matching one replication selection plus its source cursor.
///
/// Event positions may contain gaps because unrelated entries are omitted.
/// `through` advances over those gaps and is therefore selection-specific.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedReplicationBatch {
    pub source_node: NodeId,
    pub selection: ReplicationSelection,
    pub after: Option<LogPosition>,
    pub through: Option<LogPosition>,
    /// Minimal authoritative parent-edge proof for the effective selection.
    /// Hidden siblings are omitted.
    #[serde(default)]
    pub topology: ScopeTopology,
    pub events: Vec<EventEnvelope>,
}

/// Immutable events for one exact scope plus a source-log cursor watermark.
///
/// Unlike a full [`ReplicationBatch`], event positions may contain gaps because
/// entries belonging to other scopes are omitted. `through` still advances
/// over those entries, allowing a short-lived client to resume without pulling
/// the complete node history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedReplicationBatch {
    pub source_node: NodeId,
    pub scope_id: ScopeId,
    pub after: Option<LogPosition>,
    pub through: Option<LogPosition>,
    pub events: Vec<EventEnvelope>,
}

/// Resume position bound to one source history and one exact scope.
///
/// A scoped checkpoint must never be reused for another scope. If the serving
/// transport identity begins advertising a different source node, consumers
/// discard the position and replay the requested scope from its beginning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedReplicationCheckpoint {
    pub source_node: NodeId,
    pub scope_id: ScopeId,
    pub position: Option<LogPosition>,
}

/// Resume position bound to one source history and one exact selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedReplicationCheckpoint {
    pub source_node: NodeId,
    pub selection: ReplicationSelection,
    pub position: Option<LogPosition>,
}

impl SelectedReplicationCheckpoint {
    /// Creates a source- and selection-bound resume checkpoint.
    #[must_use]
    pub const fn new(
        source_node: NodeId,
        selection: ReplicationSelection,
        position: Option<LogPosition>,
    ) -> Self {
        Self {
            source_node,
            selection,
            position,
        }
    }
}

impl ScopedReplicationCheckpoint {
    /// Creates a source- and scope-bound resume checkpoint.
    #[must_use]
    pub const fn new(
        source_node: NodeId,
        scope_id: ScopeId,
        position: Option<LogPosition>,
    ) -> Self {
        Self {
            source_node,
            scope_id,
            position,
        }
    }
}

/// Result of idempotently applying a replication batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationReport {
    pub source_node: NodeId,
    pub through: Option<LogPosition>,
    pub applied: usize,
    pub duplicates: usize,
}

/// Result of applying one scope-filtered replication batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedReplicationReport {
    pub source_node: NodeId,
    pub scope_id: ScopeId,
    pub through: Option<LogPosition>,
    pub applied: usize,
    pub duplicates: usize,
}

/// Result of applying one selection-filtered replication batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedReplicationReport {
    pub source_node: NodeId,
    pub selection: ReplicationSelection,
    pub through: Option<LogPosition>,
    pub applied: usize,
    pub duplicates: usize,
}

/// One bounded, lexically ordered page of application scope identifiers.
///
/// Transport adapters filter scopes through their access policy before
/// constructing a page. The cursor is the last returned scope when more
/// authorized scopes remain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeCatalogPage {
    pub source_node: NodeId,
    pub scopes: Vec<ScopeId>,
    pub next_after: Option<ScopeId>,
}

impl ScopedReplicationReport {
    /// Returns the checked cursor for the next pull or follow of this scope.
    #[must_use]
    pub fn checkpoint(&self) -> ScopedReplicationCheckpoint {
        ScopedReplicationCheckpoint::new(self.source_node, self.scope_id.clone(), self.through)
    }
}

impl SelectedReplicationReport {
    /// Returns the checked cursor for the next pull of this selection.
    #[must_use]
    pub fn checkpoint(&self) -> SelectedReplicationCheckpoint {
        SelectedReplicationCheckpoint::new(self.source_node, self.selection.clone(), self.through)
    }
}

/// Opaque node-local identity for one transport peer's replay progress.
///
/// Cursor keys are deliberately not part of replicated graph state. A
/// transport chooses a stable namespace and peer identity, while storage
/// adapters persist the resulting local checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplicationCursorKey {
    transport: String,
    peer: String,
}

impl ReplicationCursorKey {
    /// Creates a transport-scoped peer cursor key.
    #[must_use]
    pub fn new(transport: impl Into<String>, peer: impl Into<String>) -> Self {
        Self {
            transport: transport.into(),
            peer: peer.into(),
        }
    }

    /// Returns the transport namespace.
    #[must_use]
    pub fn transport(&self) -> &str {
        &self.transport
    }

    /// Returns the transport-defined stable peer identity.
    #[must_use]
    pub fn peer(&self) -> &str {
        &self.peer
    }
}

/// Durable progress for one transport peer and one source-node history.
///
/// The source identity is part of the checkpoint because a transport peer can
/// be reconfigured with a fresh Myko journal. In that case its positions start
/// over and a follower must not apply the old journal's cursor to the new one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationCheckpoint {
    pub source_node: NodeId,
    pub position: Option<LogPosition>,
    /// Effective selection that produced this cursor.
    pub selection: ReplicationSelection,
}

impl ReplicationCheckpoint {
    /// Creates a checkpoint for a source node and its last ingested position.
    #[must_use]
    pub const fn new(source_node: NodeId, position: Option<LogPosition>) -> Self {
        Self {
            source_node,
            position,
            selection: ReplicationSelection::All,
        }
    }

    #[must_use]
    pub const fn selected(
        source_node: NodeId,
        position: Option<LogPosition>,
        selection: ReplicationSelection,
    ) -> Self {
        Self {
            source_node,
            position,
            selection,
        }
    }
}

/// Transport-neutral events observed by replicas, services, and clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeEvent {
    FrameworkControl(FrameworkControlEvent),
    CommandLifecycle(CommandSnapshot),
    CommandCommitted {
        command: CommandSnapshot,
        batch: ChangeBatch,
    },
}

impl NodeEvent {
    /// Returns the application service that owns this event.
    #[must_use]
    pub const fn service_id(&self) -> Option<&ServiceId> {
        match self {
            Self::CommandLifecycle(command) | Self::CommandCommitted { command, .. } => {
                Some(&command.request.service_id)
            }
            Self::FrameworkControl(_) => None,
        }
    }

    /// Returns the primary command scope or the control selection's root.
    /// A control root does not describe its full authorization footprint.
    #[must_use]
    pub const fn scope_id(&self) -> &ScopeId {
        match self {
            Self::FrameworkControl(control) => control.scope_id(),
            Self::CommandLifecycle(command) | Self::CommandCommitted { command, .. } => {
                &command.request.scope_id
            }
        }
    }

    /// Returns concrete scopes touched by an application event.
    /// Controls have no application mutations and return an empty set. Use
    /// selection-aware replication methods to authorize their full selection.
    #[must_use]
    pub fn affected_scope_ids(&self) -> Vec<ScopeId> {
        let Some(command) = command_from_event(self) else {
            return Vec::new();
        };
        let mut scopes = HashSet::from([self.scope_id().clone()]);
        scopes.extend(
            command
                .request
                .resource_claims
                .iter()
                .map(|claim| claim.selection.root().clone()),
        );
        if let Self::CommandCommitted { batch, .. } = self {
            scopes.extend(batch.changes.iter().filter_map(|mutation| {
                mutation
                    .scope_id
                    .as_ref()
                    .map(|scope_id| ScopeId::new(scope_id.clone()))
            }));
        }
        let mut scopes = scopes.into_iter().collect::<Vec<_>>();
        scopes.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        scopes
    }
}

pub(super) const fn command_from_event(event: &NodeEvent) -> Option<&CommandSnapshot> {
    match event {
        NodeEvent::CommandLifecycle(command) | NodeEvent::CommandCommitted { command, .. } => {
            Some(command)
        }
        NodeEvent::FrameworkControl(_) => None,
    }
}

pub(super) fn command_snapshot_supersedes(
    current: &CommandSnapshot,
    candidate: &CommandSnapshot,
) -> bool {
    let newer_at_source = candidate.updated_at.node_id == current.updated_at.node_id
        && candidate.updated_at.sequence > current.updated_at.sequence;
    if current.state.is_committed() {
        return candidate.state.is_committed() && newer_at_source;
    }
    if candidate.state.is_committed() {
        return true;
    }
    let terminal_rank = |state: &CommandState| match state {
        CommandState::Cancelled { .. } => 2,
        CommandState::Rejected { .. } => 1,
        _ => 0,
    };
    let current_rank = terminal_rank(&current.state);
    let candidate_rank = terminal_rank(&candidate.state);
    if current_rank != candidate_rank {
        return candidate_rank > current_rank;
    }
    if current_rank > 0 {
        return (candidate.updated_at.node_id, candidate.updated_at.sequence)
            > (current.updated_at.node_id, current.updated_at.sequence);
    }
    newer_at_source
}

pub(super) fn materialize_command_snapshot<'a>(
    history: impl IntoIterator<Item = &'a EventEnvelope>,
    command_id: CommandId,
) -> Option<CommandSnapshot> {
    let mut current: Option<&CommandSnapshot> = None;
    for envelope in history {
        let Some(candidate) = command_from_event(&envelope.event) else {
            continue;
        };
        if candidate.request.id == command_id
            && current.is_none_or(|existing| command_snapshot_supersedes(existing, candidate))
        {
            current = Some(candidate);
        }
    }
    current.cloned()
}

/// Errors raised by the command/history substrate.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NodeError {
    #[error("retained-history recording requires a durable journal")]
    DurableJournalRequired,
    #[error("invalid retained-history statement: {0}")]
    InvalidRetainedHistoryStatement(String),
    #[error("event origin {0:?} was reused with different immutable content")]
    EventConflict(EventId),
    #[error("required event origin {0:?} is absent from retained history")]
    MissingRetainedEvent(EventId),
    #[error("command ID {0} was reused with different content")]
    CommandConflict(CommandId),
    #[error("unknown command ID {0}")]
    UnknownCommand(CommandId),
    #[error("command ID {0} is retained but its committed history is not yet causally complete")]
    CommandHistoryIncomplete(CommandId),
    #[error("command ID {command_id} was rejected: {reason}")]
    CommandRejected {
        command_id: CommandId,
        reason: String,
    },
    #[error("command ID {command_id} was cancelled: {reason}")]
    CommandCancelled {
        command_id: CommandId,
        reason: String,
    },
    #[error("command ID {0} is not executing")]
    CommandNotExecuting(CommandId),
    #[error("command ID {command_id} originated on foreign node {origin}")]
    ForeignCommand {
        command_id: CommandId,
        origin: NodeId,
    },
    #[error("change batch does not match admitted command {0}")]
    BatchMismatch(CommandId),
    #[error("invalid item mutation: {0}")]
    InvalidItemMutation(String),
    #[error(
        "item service mismatch: command belongs to {command_service}, item belongs to {item_service}"
    )]
    ItemServiceMismatch {
        command_service: String,
        item_service: &'static str,
    },
    #[error("invalid item-state page: {0}")]
    InvalidItemState(String),
    #[error("invalid command-state page: {0}")]
    InvalidCommandState(String),
    #[error("command payload encoding failed: {0}")]
    CommandEncoding(String),
    #[error("command payload decoding failed: {0}")]
    CommandDecoding(String),
    #[error(
        "command schema mismatch: expected {expected_service}/{expected_command}, got {actual_service}/{actual_command}"
    )]
    CommandSchemaMismatch {
        expected_service: &'static str,
        expected_command: &'static str,
        actual_service: String,
        actual_command: String,
    },
    #[error("command result encoding failed: {0}")]
    ResultEncoding(String),
    #[error("command result decoding failed: {0}")]
    ResultDecoding(String),
    #[error("node state lock is poisoned")]
    Poisoned,
    #[error("node log position space is exhausted")]
    PositionExhausted,
    #[error("history cut {requested:?} exceeds the available local cut {available:?}")]
    HistoryCutUnavailable {
        requested: LogPosition,
        available: Option<LogPosition>,
    },
    #[error("event subscription is disconnected")]
    SubscriptionDisconnected,
    #[error("live-event hub state is poisoned")]
    LiveEventHubPoisoned,
    #[error("live-event sequence space is exhausted")]
    LiveEventSequenceExhausted,
    #[error("backend error: {0}")]
    Backend(String),
    #[error("corrupt event history: {0}")]
    CorruptHistory(String),
    #[error("invalid replication batch: {0}")]
    InvalidReplicationBatch(String),
    #[error("command authorization denied: {0}")]
    AuthorizationDenied(String),
    #[error("control vote rejected: {0}")]
    ControlVote(#[from] crate::control_quorum::ControlQuorumError),
    #[error("durable history differs from live state; reopen the node before voting")]
    DurableHistoryChanged,
}

/// Durable append-only storage used by the reference event-sourced backend.
///
/// Implementations must make a successful [`Self::append`] durable before
/// returning. Events are supplied in strictly increasing node-local position
/// order and must be replayed in that same order after restart.
pub trait EventJournal: Send + Sync + 'static {
    /// Returns the stable identity stored with this journal.
    ///
    /// # Errors
    ///
    /// Returns an error if journal metadata cannot be read.
    fn node_id(&self) -> Result<NodeId, NodeError>;

    /// Returns this store's persisted incarnation identity.
    ///
    /// The identity must remain stable across reopen and differ for independently
    /// initialized stores. Copying or restoring the complete store can preserve
    /// this identity, so it does not prove freshness or detect rollback.
    ///
    /// # Errors
    ///
    /// Returns an error if the persisted identity cannot be read or decoded.
    fn storage_incarnation(&self) -> Result<StorageIncarnationId, NodeError>;

    /// Replays every locally observed event in node-local position order.
    ///
    /// # Errors
    ///
    /// Returns an error if durable history cannot be read or decoded.
    fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError>;

    /// Atomically and durably appends one immutable event.
    ///
    /// # Errors
    ///
    /// Returns an error unless the event has been durably committed.
    fn append(&self, event: &EventEnvelope) -> Result<(), NodeError>;

    /// Verifies exact immutable-event inclusion in this durable journal.
    ///
    /// Receiver-local positions are deliberately ignored. This proves only
    /// that every supplied origin, timestamp, and event body is retained. It
    /// does not prove scope completeness, causal closure, authority,
    /// currentness, a signature, or continuing custody.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::MissingRetainedEvent`] when a required origin is
    /// absent, [`NodeError::EventConflict`] when the same origin has different
    /// immutable content, or a backend error when durable replay fails.
    fn verify_retained_history(&self, required: &[EventEnvelope]) -> Result<(), NodeError> {
        let replay = self.replay()?;
        let mut retained: HashMap<EventId, &EventEnvelope> = HashMap::new();
        for event in &replay {
            if let Some(previous) = retained.insert(event.origin, event)
                && (previous.recorded_at != event.recorded_at || previous.event != event.event)
            {
                return Err(NodeError::EventConflict(event.origin));
            }
        }
        for event in required {
            let Some(actual) = retained.get(&event.origin) else {
                return Err(NodeError::MissingRetainedEvent(event.origin));
            };
            if actual.recorded_at != event.recorded_at || actual.event != event.event {
                return Err(NodeError::EventConflict(event.origin));
            }
        }
        Ok(())
    }
}

/// Node-local durable checkpoints for transport replication followers.
///
/// A follower may save an identity-only checkpoint after authenticating a
/// source. It must save a positioned checkpoint only after the corresponding
/// batch was ingested successfully. A crash before that save may replay
/// duplicates, which the event substrate handles idempotently; saving a
/// position before ingest could lose history and is therefore forbidden.
pub trait ReplicationCursorStore: Send + Sync + 'static {
    /// Loads the source identity and last successfully ingested position.
    ///
    /// # Errors
    ///
    /// Returns an error when local checkpoint storage cannot be read.
    fn load_checkpoint(
        &self,
        key: &ReplicationCursorKey,
    ) -> Result<Option<ReplicationCheckpoint>, NodeError>;

    /// Durably stores source identity and replication progress for a peer.
    /// Implementations must reject attempts to move backwards within the same
    /// source history, but must allow a different source identity to replace a
    /// prior checkpoint and begin again without a position.
    ///
    /// # Errors
    ///
    /// Returns an error unless the checkpoint is durable before returning.
    fn save_checkpoint(
        &self,
        key: &ReplicationCursorKey,
        checkpoint: ReplicationCheckpoint,
    ) -> Result<(), NodeError>;
}

pub(super) const DURABLE_EVENT_SUBSCRIPTION_CAPACITY: usize = 1_024;
pub(super) const DURABLE_EVENT_PAGE_SIZE: usize = 1_024;
pub(super) const DURABLE_EVENT_PAGE_LIMIT: NonZeroUsize =
    match NonZeroUsize::new(DURABLE_EVENT_PAGE_SIZE) {
        Some(limit) => limit,
        None => NonZeroUsize::MIN,
    };

pub(super) struct DurableReplay {
    pub(super) state: Arc<Mutex<MemoryState>>,
    pub(super) after: Option<LogPosition>,
    pub(super) buffered: VecDeque<EventEnvelope>,
}

/// Replay followed by bounded, lossless live delivery from the same logical cursor.
pub struct EventSubscription {
    pub(super) replay: Option<DurableReplay>,
    pub(super) live: Option<flume::Receiver<EventEnvelope>>,
}

impl EventSubscription {
    fn next_replayed(&mut self) -> Result<Option<EventEnvelope>, NodeError> {
        let Some(replay) = self.replay.as_mut() else {
            return Ok(None);
        };
        if let Some(event) = replay.buffered.pop_front() {
            replay.after = Some(event.position);
            return Ok(Some(event));
        }

        let mut state = replay.state.lock().map_err(|_| NodeError::Poisoned)?;
        replay.buffered.extend(
            state
                .events
                .iter()
                .filter(|event| InMemoryBackend::matches_cursor(event, replay.after))
                .take(DURABLE_EVENT_PAGE_SIZE)
                .cloned(),
        );
        if let Some(event) = replay.buffered.pop_front() {
            replay.after = Some(event.position);
            drop(state);
            return Ok(Some(event));
        }

        let (sender, live) = flume::bounded(DURABLE_EVENT_SUBSCRIPTION_CAPACITY);
        state.subscribers.push(sender);
        drop(state);
        self.replay = None;
        self.live = Some(live);
        Ok(None)
    }

    fn live(&self) -> Result<&flume::Receiver<EventEnvelope>, NodeError> {
        self.live
            .as_ref()
            .ok_or(NodeError::SubscriptionDisconnected)
    }

    /// Receives the next replayed or live event.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub fn recv(&mut self) -> Result<EventEnvelope, NodeError> {
        if let Some(event) = self.next_replayed()? {
            return Ok(event);
        }
        self.live()?
            .recv()
            .map_err(|_| NodeError::SubscriptionDisconnected)
    }

    /// Attempts to receive without blocking.
    #[must_use]
    pub fn try_recv(&mut self) -> Option<EventEnvelope> {
        self.next_replayed()
            .ok()
            .flatten()
            .or_else(|| self.live.as_ref().and_then(|live| live.try_recv().ok()))
    }

    /// Receives the next event until `timeout` elapses.
    ///
    /// A timeout is reported as `Ok(None)`; a closed backend remains an error.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<EventEnvelope>, NodeError> {
        if let Some(event) = self.next_replayed()? {
            return Ok(Some(event));
        }
        match self.live()?.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
            Err(flume::RecvTimeoutError::Timeout) => Ok(None),
            Err(flume::RecvTimeoutError::Disconnected) => Err(NodeError::SubscriptionDisconnected),
        }
    }

    /// Asynchronously receives the next replayed or live event.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub async fn recv_async(&mut self) -> Result<EventEnvelope, NodeError> {
        if let Some(event) = self.next_replayed()? {
            return Ok(event);
        }
        self.live()?
            .recv_async()
            .await
            .map_err(|_| NodeError::SubscriptionDisconnected)
    }
}

/// Gap-free notification stream for committed application item changes.
///
/// Command admission, execution, retry, and cancellation transitions are
/// intentionally hidden. Application supervisors can use this as an opaque
/// dependency wakeup without inspecting event envelopes or feeding a
/// command's own retry lifecycle back into its dispatch loop.
pub struct ItemChangeSubscription {
    pub(super) events: EventSubscription,
}

impl ItemChangeSubscription {
    /// Receives the position of the next committed item change.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub fn recv(&mut self) -> Result<LogPosition, NodeError> {
        loop {
            let envelope = self.events.recv()?;
            if event_changes_items(&envelope.event) {
                return Ok(envelope.position);
            }
        }
    }

    /// Receives the next committed item change until `timeout` elapses.
    ///
    /// Unrelated command lifecycle events do not restart the timeout.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<LogPosition>, NodeError> {
        let deadline = Instant::now().checked_add(timeout);
        loop {
            let remaining = deadline.map_or(timeout, |deadline| {
                deadline.saturating_duration_since(Instant::now())
            });
            let Some(envelope) = self.events.recv_timeout(remaining)? else {
                return Ok(None);
            };
            if event_changes_items(&envelope.event) {
                return Ok(Some(envelope.position));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(None);
            }
        }
    }

    /// Asynchronously receives the position of the next committed item change.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub async fn recv_async(&mut self) -> Result<LogPosition, NodeError> {
        loop {
            let envelope = self.events.recv_async().await?;
            if event_changes_items(&envelope.event) {
                return Ok(envelope.position);
            }
        }
    }

    /// Attempts to receive a committed item change without blocking.
    #[must_use]
    pub fn try_recv(&mut self) -> Option<LogPosition> {
        while let Some(envelope) = self.events.try_recv() {
            if event_changes_items(&envelope.event) {
                return Some(envelope.position);
            }
        }
        None
    }
}

const fn event_changes_items(event: &NodeEvent) -> bool {
    matches!(
        event,
        NodeEvent::CommandCommitted { batch, .. } if !batch.changes.is_empty()
    )
}

/// Gap-free local work feed for one application service or command contract.
///
/// The initial queue is materialized from a bounded history prefix. New local
/// submissions and durable retries then arrive through the node's lossless
/// event subscription. Replicated commands are projections and never enter
/// this executable feed.
pub struct PendingCommandSubscription {
    pub(super) local_node: NodeId,
    pub(super) service_id: Option<ServiceId>,
    pub(super) command_type: Option<String>,
    pub(super) pending: VecDeque<CommandSnapshot>,
    pub(super) events: EventSubscription,
}

impl PendingCommandSubscription {
    /// Returns the exact service filter, or `None` when every local
    /// application command is observed.
    #[must_use]
    pub const fn service_id(&self) -> Option<&ServiceId> {
        self.service_id.as_ref()
    }

    /// Returns the exact command filter, or `None` for all service commands.
    #[must_use]
    pub fn command_type(&self) -> Option<&str> {
        self.command_type.as_deref()
    }

    /// Receives the next currently executable local command.
    ///
    /// A competing handler may advance the lifecycle before the caller claims
    /// it. Myko's admission API resolves that race idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying event subscription closes.
    pub fn recv(&mut self) -> Result<CommandSnapshot, NodeError> {
        loop {
            if let Some(command) = self.pending.pop_front() {
                return Ok(command);
            }
            let envelope = self.events.recv()?;
            if let Some(command) = self.match_pending(&envelope) {
                return Ok(command);
            }
        }
    }

    /// Asynchronously receives the next currently executable local command.
    ///
    /// This is the cancellation-friendly service-loop boundary for async node
    /// compositions. It preserves the same replay-first and local-origin
    /// guarantees as [`Self::recv`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying event subscription closes.
    pub async fn recv_async(&mut self) -> Result<CommandSnapshot, NodeError> {
        loop {
            if let Some(command) = self.pending.pop_front() {
                return Ok(command);
            }
            let envelope = self.events.recv_async().await?;
            if let Some(command) = self.match_pending(&envelope) {
                return Ok(command);
            }
        }
    }

    /// Receives local work until the total timeout elapses.
    ///
    /// Unrelated federation events do not restart the timeout. A timeout is
    /// reported as `Ok(None)` so a supervisor can check its shutdown signal.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying event subscription closes.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<CommandSnapshot>, NodeError> {
        if let Some(command) = self.pending.pop_front() {
            return Ok(Some(command));
        }
        let deadline = Instant::now().checked_add(timeout);
        loop {
            let remaining = deadline.map_or(timeout, |deadline| {
                deadline.saturating_duration_since(Instant::now())
            });
            let Some(envelope) = self.events.recv_timeout(remaining)? else {
                return Ok(None);
            };
            if let Some(command) = self.match_pending(&envelope) {
                return Ok(Some(command));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(None);
            }
        }
    }

    /// Attempts to receive local work without blocking.
    #[must_use]
    pub fn try_recv(&mut self) -> Option<CommandSnapshot> {
        if let Some(command) = self.pending.pop_front() {
            return Some(command);
        }
        while let Some(envelope) = self.events.try_recv() {
            if let Some(command) = self.match_pending(&envelope) {
                return Some(command);
            }
        }
        None
    }

    fn match_pending(&self, envelope: &EventEnvelope) -> Option<CommandSnapshot> {
        if envelope.origin.node_id != self.local_node {
            return None;
        }
        let command = command_from_event(&envelope.event)?;
        if self
            .service_id
            .as_ref()
            .is_some_and(|expected| command.request.service_id != *expected)
            || self
                .command_type
                .as_deref()
                .is_some_and(|expected| command.request.command_type != expected)
            || !matches!(
                command.state,
                CommandState::Submitted | CommandState::Retrying { .. }
            )
        {
            return None;
        }
        Some(command.clone())
    }
}

pub(super) fn materialize_pending_local_commands(
    history: &[EventEnvelope],
    local_node: NodeId,
    service_id: Option<&ServiceId>,
    command_type: Option<&str>,
) -> VecDeque<CommandSnapshot> {
    let mut current = HashMap::<CommandId, (LogPosition, CommandSnapshot)>::new();
    for envelope in history {
        if envelope.origin.node_id != local_node {
            continue;
        }
        let Some(command) = command_from_event(&envelope.event) else {
            continue;
        };
        if service_id.is_some_and(|expected| command.request.service_id != *expected)
            || command_type.is_some_and(|expected| command.request.command_type != expected)
        {
            continue;
        }
        match current.entry(command.request.id) {
            Entry::Vacant(entry) => {
                entry.insert((envelope.position, command.clone()));
            }
            Entry::Occupied(mut entry) => {
                if command_snapshot_supersedes(&entry.get().1, command) {
                    entry.get_mut().1 = command.clone();
                }
            }
        }
    }
    let mut pending = current
        .into_values()
        .filter(|(_, command)| {
            matches!(
                command.state,
                CommandState::Submitted | CommandState::Retrying { .. }
            )
        })
        .collect::<Vec<_>>();
    pending.sort_unstable_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            left.1
                .request
                .id
                .to_string()
                .cmp(&right.1.request.id.to_string())
        })
    });
    pending.into_iter().map(|(_, command)| command).collect()
}
