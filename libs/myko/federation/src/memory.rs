use super::*;

/// Reference backend used by embedded nodes and tests.
pub struct InMemoryBackend {
    node_id: NodeId,
    state: Arc<Mutex<MemoryState>>,
    journal: Option<Arc<dyn EventJournal>>,
}

#[derive(Default)]
pub(super) struct MemoryState {
    next_position: LogPosition,
    commands: HashMap<CommandId, CommandSnapshot>,
    scope_topology: ScopeTopology,
    scope_ids: BTreeMap<String, ScopeId>,
    pub(super) events: Vec<EventEnvelope>,
    service_scope_positions: HashMap<(ServiceId, ScopeId), LogPosition>,
    seen_origins: HashSet<EventId>,
    pub(super) subscribers: Vec<flume::Sender<EventEnvelope>>,
}

impl InMemoryBackend {
    /// Creates an empty backend with a stable node identity.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            state: Arc::new(Mutex::new(MemoryState {
                next_position: LogPosition::FIRST,
                ..MemoryState::default()
            })),
            journal: None,
        }
    }

    /// Reconstructs the reference backend from a durable immutable journal.
    ///
    /// # Errors
    ///
    /// Returns an error if history is corrupt, out of order, or cannot be read.
    pub fn from_journal(journal: Arc<dyn EventJournal>) -> Result<Self, NodeError> {
        let started = std::time::Instant::now();
        let node_id = journal.node_id()?;
        let mut state = MemoryState {
            next_position: LogPosition::FIRST,
            ..MemoryState::default()
        };
        let replay_started = std::time::Instant::now();
        let replay = journal.replay()?;
        tracing::debug!(
            node_id = %node_id,
            events = replay.len(),
            elapsed_ms = replay_started.elapsed().as_millis(),
            "durable history loaded"
        );
        for envelope in replay {
            if envelope.position != state.next_position {
                return Err(NodeError::CorruptHistory(format!(
                    "expected position {}, found {}",
                    state.next_position.get(),
                    envelope.position.get()
                )));
            }
            if !state.seen_origins.insert(envelope.origin) {
                return Err(NodeError::CorruptHistory(format!(
                    "duplicate event origin {:?}",
                    envelope.origin
                )));
            }
            Self::validate_event(&state, &envelope.event)?;
            state.scope_topology.observe_event(&envelope.event)?;
            Self::observe_scope_ids(&mut state, &envelope.event);
            Self::apply_event(&mut state, &envelope.event);
            Self::observe_service_scope_position(&mut state, node_id, &envelope);
            state.next_position = state.next_position.next()?;
            state.events.push(envelope);
        }
        let backend = Self {
            node_id,
            state: Arc::new(Mutex::new(state)),
            journal: Some(journal),
        };
        backend.requeue_abandoned_local_claims()?;
        tracing::debug!(
            node_id = %node_id,
            elapsed_ms = started.elapsed().as_millis(),
            "durable in-memory projection restored"
        );
        Ok(backend)
    }

    fn requeue_abandoned_local_claims(&self) -> Result<(), NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let mut abandoned = state
            .commands
            .values()
            .filter(|snapshot| {
                matches!(snapshot.state, CommandState::Executing)
                    && snapshot.updated_at.node_id == self.node_id
            })
            .map(|snapshot| (snapshot.updated_at.sequence, snapshot.request.clone()))
            .collect::<Vec<_>>();
        abandoned.sort_by_key(|(position, _)| *position);
        for (_, request) in abandoned {
            let position = state.next_position;
            let snapshot = CommandSnapshot {
                request,
                state: CommandState::Submitted,
                result: None,
                updated_at: EventId::new(self.node_id, position),
            };
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot))?;
        }
        drop(state);
        Ok(())
    }

    fn append_locked(
        &self,
        state: &mut MemoryState,
        event: NodeEvent,
    ) -> Result<EventEnvelope, NodeError> {
        let position = state.next_position;
        let next_position = position.next()?;
        Self::validate_event(state, &event)?;
        let envelope = EventEnvelope {
            position,
            origin: EventId::new(self.node_id, position),
            recorded_at: Utc::now(),
            event,
        };
        if let Some(journal) = &self.journal {
            journal.append(&envelope)?;
        }
        state.scope_topology.observe_event(&envelope.event)?;
        Self::observe_scope_ids(state, &envelope.event);
        Self::apply_event(state, &envelope.event);
        Self::observe_service_scope_position(state, self.node_id, &envelope);
        state.next_position = next_position;
        state.seen_origins.insert(envelope.origin);
        state.events.push(envelope.clone());
        Ok(envelope)
    }

    fn observe_service_scope_position(
        state: &mut MemoryState,
        local_node: NodeId,
        envelope: &EventEnvelope,
    ) {
        if envelope.origin.node_id != local_node {
            return;
        }
        let NodeEvent::CommandCommitted { command, .. } = &envelope.event else {
            return;
        };
        state.service_scope_positions.insert(
            (
                command.request.service_id.clone(),
                command.request.scope_id.clone(),
            ),
            envelope.position,
        );
    }

    fn observe_scope_ids(state: &mut MemoryState, event: &NodeEvent) {
        for scope_id in event.affected_scope_ids() {
            state
                .scope_ids
                .insert(scope_id.as_str().to_owned(), scope_id);
        }
    }

    fn validate_event(state: &MemoryState, event: &NodeEvent) -> Result<(), NodeError> {
        let mut scope_topology = state.scope_topology.clone();
        scope_topology.observe_event(event)?;
        match event {
            NodeEvent::CommandLifecycle(snapshot) => {
                if let Some(existing) = state.commands.get(&snapshot.request.id)
                    && existing.request != snapshot.request
                {
                    return Err(NodeError::CommandConflict(snapshot.request.id));
                }
            }
            NodeEvent::CommandCommitted { command, batch } => {
                if batch.command_id != command.request.id
                    || batch.service_id != command.request.service_id
                    || batch.scope_id != command.request.scope_id
                {
                    return Err(NodeError::BatchMismatch(command.request.id));
                }
                validate_change_batch(batch)?;
                if let Some(existing) = state.commands.get(&command.request.id)
                    && existing.request != command.request
                {
                    return Err(NodeError::CommandConflict(command.request.id));
                }
            }
        }
        Ok(())
    }

    fn apply_event(state: &mut MemoryState, event: &NodeEvent) {
        match event {
            NodeEvent::CommandLifecycle(snapshot) => {
                let should_apply = state
                    .commands
                    .get(&snapshot.request.id)
                    .is_none_or(|existing| Self::lifecycle_supersedes(existing, snapshot));
                if should_apply {
                    state.commands.insert(snapshot.request.id, snapshot.clone());
                }
            }
            NodeEvent::CommandCommitted { command, .. } => {
                state.commands.insert(command.request.id, command.clone());
            }
        }
    }

    fn lifecycle_supersedes(existing: &CommandSnapshot, incoming: &CommandSnapshot) -> bool {
        if existing.state.is_committed() {
            return false;
        }
        let existing_terminal = matches!(
            existing.state,
            CommandState::Rejected { .. } | CommandState::Cancelled { .. }
        );
        if !existing_terminal {
            return true;
        }
        let incoming_rank = match incoming.state {
            CommandState::Cancelled { .. } => 2,
            CommandState::Rejected { .. } => 1,
            _ => 0,
        };
        let existing_rank = match existing.state {
            CommandState::Cancelled { .. } => 2,
            CommandState::Rejected { .. } => 1,
            _ => 0,
        };
        let incoming_order = (
            incoming.updated_at.node_id.as_uuid(),
            incoming.updated_at.sequence,
        );
        let existing_order = (
            existing.updated_at.node_id.as_uuid(),
            existing.updated_at.sequence,
        );
        incoming_rank > existing_rank
            || (incoming_rank == existing_rank && incoming_order > existing_order)
    }

    fn broadcast_locked(state: &mut MemoryState, envelope: &EventEnvelope) {
        state
            .subscribers
            .retain(|subscriber| subscriber.send(envelope.clone()).is_ok());
    }

    pub(super) fn matches_cursor(event: &EventEnvelope, after: Option<LogPosition>) -> bool {
        after.is_none_or(|cursor| event.position > cursor)
    }
}

impl NodeBackend for InMemoryBackend {
    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn submit(&self, request: CommandRequest) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        if let Some(existing) = state.commands.get(&request.id) {
            if existing.request != request {
                return Err(NodeError::CommandConflict(request.id));
            }
            return Ok(existing.clone());
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request,
            state: CommandState::Submitted,
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn claim(&self, command_id: CommandId) -> Result<CommandAdmission, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if !matches!(
            existing.state,
            CommandState::Submitted | CommandState::Retrying { .. }
        ) {
            return Ok(CommandAdmission::Resume(existing));
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::Executing,
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(CommandAdmission::Execute(snapshot))
    }

    fn admit(&self, request: CommandRequest) -> Result<CommandAdmission, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        if let Some(existing) = state.commands.get(&request.id) {
            if existing.request != request {
                return Err(NodeError::CommandConflict(request.id));
            }
            return Ok(CommandAdmission::Resume(existing.clone()));
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request,
            state: CommandState::Executing,
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        debug_assert_eq!(envelope.position, position);
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(CommandAdmission::Execute(snapshot))
    }

    fn commit(
        &self,
        command_id: CommandId,
        batch: ChangeBatch,
        result: Vec<u8>,
    ) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;

        if existing.state.is_committed() {
            return Ok(existing);
        }
        if !matches!(existing.state, CommandState::Executing) {
            return Err(NodeError::CommandNotExecuting(command_id));
        }
        if batch.command_id != command_id
            || batch.service_id != existing.request.service_id
            || batch.scope_id != existing.request.scope_id
        {
            return Err(NodeError::BatchMismatch(command_id));
        }
        validate_change_batch(&batch)?;

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::CommittedLocally {
                batch_id: batch.id,
                position: origin,
            },
            result: Some(result),
            updated_at: origin,
        };
        let envelope = self.append_locked(
            &mut state,
            NodeEvent::CommandCommitted {
                command: snapshot.clone(),
                batch,
            },
        )?;
        debug_assert_eq!(envelope.position, position);
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn reject(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if !matches!(existing.state, CommandState::Executing) {
            return Err(NodeError::CommandNotExecuting(command_id));
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::Rejected { reason },
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn retry(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if !matches!(existing.state, CommandState::Executing) {
            return Err(NodeError::CommandNotExecuting(command_id));
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::Retrying { reason },
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn await_authorization(
        &self,
        command_id: CommandId,
        challenge_id: ChallengeId,
        batch: ChangeBatch,
        result: Vec<u8>,
    ) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if !matches!(existing.state, CommandState::Executing) {
            return Err(NodeError::CommandNotExecuting(command_id));
        }
        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::AuthorizationPending {
                challenge_id,
                batch: Box::new(batch),
                result,
                approvals: Vec::new(),
            },
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn resume_authorization(
        &self,
        command_id: CommandId,
        challenge_id: &ChallengeId,
        approval_id: ApprovalId,
    ) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        let CommandState::AuthorizationPending {
            challenge_id: expected,
            batch,
            result,
            mut approvals,
        } = existing.state
        else {
            return Err(NodeError::CommandNotExecuting(command_id));
        };
        if &expected != challenge_id {
            return Err(NodeError::CommandNotExecuting(command_id));
        }
        if !approvals.contains(&approval_id) {
            approvals.push(approval_id);
        }
        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::CommittedLocally {
                batch_id: batch.id,
                position: origin,
            },
            result: Some(result),
            updated_at: origin,
        };
        let envelope = self.append_locked(
            &mut state,
            NodeEvent::CommandCommitted {
                command: snapshot.clone(),
                batch: *batch,
            },
        )?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn advance_authorization(
        &self,
        command_id: CommandId,
        challenge_id: &ChallengeId,
        next_challenge_id: ChallengeId,
        approval_id: ApprovalId,
    ) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        let CommandState::AuthorizationPending {
            challenge_id: expected,
            batch,
            result,
            mut approvals,
        } = existing.state
        else {
            return Err(NodeError::CommandNotExecuting(command_id));
        };
        if &expected != challenge_id {
            return Err(NodeError::CommandNotExecuting(command_id));
        }
        if !approvals.contains(&approval_id) {
            approvals.push(approval_id);
        }
        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::AuthorizationPending {
                challenge_id: next_challenge_id,
                batch,
                result,
                approvals,
            },
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn cancel(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if existing.state.is_terminal_locally() {
            return Ok(existing);
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::Cancelled { reason },
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn command(&self, command_id: CommandId) -> Result<Option<CommandSnapshot>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state.commands.get(&command_id).cloned())
    }

    fn events_after(&self, after: Option<LogPosition>) -> Result<Vec<EventEnvelope>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state
            .events
            .iter()
            .filter(|event| Self::matches_cursor(event, after))
            .cloned()
            .collect())
    }

    fn events_page(
        &self,
        after: Option<LogPosition>,
        limit: NonZeroUsize,
    ) -> Result<Vec<EventEnvelope>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state
            .events
            .iter()
            .filter(|event| Self::matches_cursor(event, after))
            .take(limit.get())
            .cloned()
            .collect())
    }

    fn latest_position(&self) -> Result<Option<LogPosition>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state.events.last().map(|event| event.position))
    }

    fn scope_ids_page(
        &self,
        after: Option<&ScopeId>,
        limit: NonZeroUsize,
    ) -> Result<Vec<ScopeId>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state
            .scope_ids
            .iter()
            .filter(|(key, _)| after.is_none_or(|after| key.as_str() > after.as_str()))
            .take(limit.get())
            .map(|(_, scope_id)| scope_id.clone())
            .collect())
    }

    fn service_scope_position(
        &self,
        service_id: &ServiceId,
        scope_id: &ScopeId,
    ) -> Result<Option<LogPosition>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state
            .service_scope_positions
            .get(&(service_id.clone(), scope_id.clone()))
            .copied())
    }

    fn subscribe(&self, after: Option<LogPosition>) -> Result<EventSubscription, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        drop(state);
        Ok(EventSubscription {
            replay: Some(DurableReplay {
                state: Arc::clone(&self.state),
                after,
                buffered: VecDeque::new(),
            }),
            live: None,
        })
    }

    fn scope_topology(&self) -> Result<ScopeTopology, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state.scope_topology.clone())
    }

    fn install_derived_scope_relations(
        &self,
        relations: &[(ScopeId, ScopeId)],
    ) -> Result<(), NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let mut topology = state.scope_topology.clone();
        for (child, parent) in relations {
            topology.insert(child.clone(), parent.clone())?;
            state
                .scope_ids
                .insert(child.as_str().to_owned(), child.clone());
            state
                .scope_ids
                .insert(parent.as_str().to_owned(), parent.clone());
        }
        state.scope_topology = topology;
        drop(state);
        Ok(())
    }

    fn ingest(&self, event: EventEnvelope) -> Result<IngestStatus, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        if state.seen_origins.contains(&event.origin) {
            return Ok(IngestStatus::Duplicate);
        }

        let local_position = state.next_position;
        let next_position = local_position.next()?;
        let imported = EventEnvelope {
            position: local_position,
            origin: event.origin,
            recorded_at: event.recorded_at,
            event: event.event,
        };
        Self::validate_event(&state, &imported.event)?;
        if let Some(journal) = &self.journal {
            journal.append(&imported)?;
        }
        state.scope_topology.observe_event(&imported.event)?;
        Self::observe_scope_ids(&mut state, &imported.event);
        Self::apply_event(&mut state, &imported.event);
        Self::observe_service_scope_position(&mut state, self.node_id, &imported);
        state.next_position = next_position;
        state.seen_origins.insert(imported.origin);
        state.events.push(imported.clone());
        Self::broadcast_locked(&mut state, &imported);
        drop(state);
        Ok(IngestStatus::Applied {
            position: local_position,
        })
    }
}
