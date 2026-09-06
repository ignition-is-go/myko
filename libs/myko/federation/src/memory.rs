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
    // Retained acceptance prevents re-execution even before its causal history is ready.
    commands: HashMap<CommandId, CommandSnapshot>,
    // Validate conflicts across all retained events, never use this to grant access.
    retained_scope_topology: ScopeTopology,
    scope_ids: BTreeMap<String, ScopeId>,
    pub(super) events: Vec<EventEnvelope>,
    service_scope_positions: HashMap<(ServiceId, ScopeId), LogPosition>,
    origin_indexes: HashMap<EventId, usize>,
    causal_index: crate::causal::CausalIndex,
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
            if state
                .origin_indexes
                .insert(envelope.origin, state.events.len())
                .is_some()
            {
                return Err(NodeError::CorruptHistory(format!(
                    "duplicate event origin {:?}",
                    envelope.origin
                )));
            }
            Self::validate_event(&state, &envelope.event)?;
            let causal_append = state.causal_index.prepare(&envelope)?;
            state
                .retained_scope_topology
                .observe_event(&envelope.event)?;
            Self::observe_scope_ids(&mut state, &envelope.event);
            Self::apply_event(&mut state, &envelope.event);
            Self::observe_service_scope_position(&mut state, node_id, &envelope);
            state.next_position = state.next_position.next()?;
            state.causal_index.apply(causal_append);
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
        let causal_append = state.causal_index.prepare(&envelope)?;
        if let Some(journal) = &self.journal {
            journal.append(&envelope)?;
        }
        state
            .retained_scope_topology
            .observe_event(&envelope.event)?;
        Self::observe_scope_ids(state, &envelope.event);
        Self::apply_event(state, &envelope.event);
        Self::observe_service_scope_position(state, self.node_id, &envelope);
        state.next_position = next_position;
        state
            .origin_indexes
            .insert(envelope.origin, state.events.len());
        state.causal_index.apply(causal_append);
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

    fn validate_prepared_effect(
        command_id: CommandId,
        request: &CommandRequest,
        effect: &PreparedCommandEffect,
    ) -> Result<(), NodeError> {
        effect.validate_digest()?;
        let batch = effect.batch();
        if batch.command_id != command_id
            || batch.service_id != request.service_id
            || batch.scope_id != request.scope_id
        {
            return Err(NodeError::BatchMismatch(command_id));
        }
        validate_change_batch(batch)
    }

    fn retained_prepared_effect(
        state: &MemoryState,
        command_id: CommandId,
    ) -> Option<&PreparedCommandEffect> {
        state.events.iter().rev().find_map(|event| {
            let NodeEvent::CommandLifecycle(command) = &event.event else {
                return None;
            };
            if command.request.id != command_id {
                return None;
            }
            match &command.state {
                CommandState::AuthorizationPrepared { effect } => Some(effect.as_ref()),
                _ => None,
            }
        })
    }

    fn validate_command_batch(
        command_id: CommandId,
        request: &CommandRequest,
        batch: &ChangeBatch,
    ) -> Result<(), NodeError> {
        if batch.command_id != command_id
            || batch.service_id != request.service_id
            || batch.scope_id != request.scope_id
        {
            return Err(NodeError::BatchMismatch(command_id));
        }
        validate_change_batch(batch)
    }

    fn validate_pending_effect(
        state: &MemoryState,
        command_id: CommandId,
        request: &CommandRequest,
        batch: &ChangeBatch,
        result: &[u8],
    ) -> Result<(), NodeError> {
        Self::validate_command_batch(command_id, request, batch)?;
        if let Some(prepared) = Self::retained_prepared_effect(state, command_id)
            && (prepared.batch() != batch || prepared.result() != result)
        {
            return Err(NodeError::CommandConflict(command_id));
        }
        Ok(())
    }

    fn append_prepared_locked(
        &self,
        state: &mut MemoryState,
        request: CommandRequest,
        effect: PreparedCommandEffect,
    ) -> Result<CommandSnapshot, NodeError> {
        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request,
            state: CommandState::AuthorizationPrepared {
                effect: Box::new(effect),
            },
            result: None,
            updated_at: origin,
        };
        let envelope = self.append_locked(state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(state, &envelope);
        Ok(snapshot)
    }

    fn append_committed_effect_locked(
        &self,
        state: &mut MemoryState,
        request: CommandRequest,
        effect: PreparedCommandEffect,
    ) -> Result<CommandSnapshot, NodeError> {
        let (batch, result) = effect.into_batch_result();
        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request,
            state: CommandState::CommittedLocally {
                batch_id: batch.id,
                position: origin,
            },
            result: Some(result),
            updated_at: origin,
        };
        let envelope = self.append_locked(
            state,
            NodeEvent::CommandCommitted {
                command: snapshot.clone(),
                batch,
            },
        )?;
        Self::broadcast_locked(state, &envelope);
        Ok(snapshot)
    }

    fn validate_event(state: &MemoryState, event: &NodeEvent) -> Result<(), NodeError> {
        let mut scope_topology = state.retained_scope_topology.clone();
        scope_topology.observe_event(event)?;
        match event {
            NodeEvent::CommandLifecycle(snapshot) => {
                if let Some(existing) = state.commands.get(&snapshot.request.id)
                    && existing.request != snapshot.request
                {
                    return Err(NodeError::CommandConflict(snapshot.request.id));
                }
                if let CommandState::AuthorizationPrepared { effect } = &snapshot.state {
                    Self::validate_prepared_effect(snapshot.request.id, &snapshot.request, effect)?;
                } else if let CommandState::AuthorizationPending { batch, result, .. } =
                    &snapshot.state
                {
                    Self::validate_pending_effect(
                        state,
                        snapshot.request.id,
                        &snapshot.request,
                        batch,
                        result,
                    )?;
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
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(signed)) => {
                signed.verify_signature()?;
            }
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(signed)) => {
                signed.verify_signature()?;
            }
            NodeEvent::FrameworkControl(FrameworkControlEvent::RetainedHistoryStatement(_)) => {}
        }
        Ok(())
    }

    fn apply_event(state: &mut MemoryState, event: &NodeEvent) {
        let Some(snapshot) = command_from_event(event) else {
            return;
        };
        if state
            .commands
            .get(&snapshot.request.id)
            .is_none_or(|existing| command_snapshot_supersedes(existing, snapshot))
        {
            state.commands.insert(snapshot.request.id, snapshot.clone());
        }
    }

    fn broadcast_locked(state: &mut MemoryState, envelope: &EventEnvelope) {
        state
            .subscribers
            .retain(|subscriber| subscriber.send(envelope.clone()).is_ok());
    }

    fn visible_command(
        state: &MemoryState,
        command_id: CommandId,
    ) -> Result<Option<CommandSnapshot>, NodeError> {
        let origins = state.causal_index.ordered_origins(None);
        let history = origins
            .iter()
            .map(|origin| {
                state
                    .origin_indexes
                    .get(origin)
                    .and_then(|index| state.events.get(*index))
                    .ok_or_else(|| {
                        NodeError::CorruptHistory(
                            "causal index references absent history".to_owned(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(materialize_command_snapshot(history, command_id))
    }

    fn resume_visible_command(
        state: &MemoryState,
        command_id: CommandId,
    ) -> Result<CommandSnapshot, NodeError> {
        let visible = Self::visible_command(state, command_id)?
            .ok_or(NodeError::CommandHistoryIncomplete(command_id))?;
        if state
            .commands
            .get(&command_id)
            .is_some_and(|accepted| accepted.state.is_committed())
            && !visible.state.is_committed()
        {
            return Err(NodeError::CommandHistoryIncomplete(command_id));
        }
        Ok(visible)
    }

    pub(super) fn matches_cursor(event: &EventEnvelope, after: Option<LogPosition>) -> bool {
        after.is_none_or(|cursor| event.position > cursor)
    }
}

impl NodeBackend for InMemoryBackend {
    fn propose_control(
        &self,
        request: &crate::control_quorum::ControlProposalRequest<'_>,
        key: &ed25519_dalek::SigningKey,
    ) -> Result<crate::control_quorum::SignedControlProposal, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let journal = self
            .journal
            .as_ref()
            .ok_or(NodeError::DurableJournalRequired)?;
        if journal.replay()? != state.events {
            return Err(NodeError::DurableHistoryChanged);
        }
        let response = request.retained_response(&state.events, key)?;
        if state.events.iter().any(|event| matches!(
            &event.event,
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(existing)) if existing == &response
        )) {
            return Ok(response);
        }
        let envelope = self.append_locked(
            &mut state,
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(response.clone())),
        )?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(response)
    }

    fn vote_control(
        &self,
        request: &crate::control_quorum::ControlVoteRequest<'_>,
        key: &ed25519_dalek::SigningKey,
    ) -> Result<crate::control_quorum::SignedControlVote, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let journal = self
            .journal
            .as_ref()
            .ok_or(NodeError::DurableJournalRequired)?;
        if journal.replay()? != state.events {
            return Err(NodeError::DurableHistoryChanged);
        }
        let response = request.retained_response(&state.events, key)?;
        if state.events.iter().any(|event| matches!(
            &event.event,
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(existing)) if existing == &response
        )) {
            return Ok(response);
        }
        let envelope = self.append_locked(
            &mut state,
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(response.clone())),
        )?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(response)
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn storage_incarnation(&self) -> Result<Option<StorageIncarnationId>, NodeError> {
        self.journal
            .as_ref()
            .map(|journal| journal.storage_incarnation())
            .transpose()
    }

    fn record_retained_history_statement(
        &self,
        signed: SignedRetainedHistoryStatement,
        manifest: &SelectedHistoryManifest,
    ) -> Result<EventEnvelope, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let journal = self
            .journal
            .as_ref()
            .ok_or(NodeError::DurableJournalRequired)?;
        let statement = signed.statement();
        if statement.holder() != self.node_id
            || statement.storage_incarnation() != journal.storage_incarnation()?
            || statement.selection() != manifest.selection()
            || statement.commitment()
                != &manifest.commitment().map_err(|error| {
                    NodeError::InvalidRetainedHistoryStatement(error.to_string())
                })?
        {
            return Err(NodeError::InvalidRetainedHistoryStatement(
                "statement holder, storage incarnation, selection, or commitment does not match the local store and manifest"
                    .to_owned(),
            ));
        }
        journal.verify_retained_history(manifest.events())?;
        if let Some(existing) = state.events.iter().find(|event| {
            event.origin.node_id == self.node_id
                && matches!(
                    &event.event,
                    NodeEvent::FrameworkControl(
                        FrameworkControlEvent::RetainedHistoryStatement(existing)
                    ) if existing == &signed
                )
        }) {
            return Ok(existing.clone());
        }
        let envelope = self.append_locked(
            &mut state,
            NodeEvent::FrameworkControl(FrameworkControlEvent::RetainedHistoryStatement(signed)),
        )?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(envelope)
    }

    fn submit(&self, request: CommandRequest) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        if let Some(existing) = state.commands.get(&request.id) {
            if existing.request != request {
                return Err(NodeError::CommandConflict(request.id));
            }
            return Self::resume_visible_command(&state, request.id);
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
            return Self::resume_visible_command(&state, command_id).map(CommandAdmission::Resume);
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
            return Self::resume_visible_command(&state, request.id).map(CommandAdmission::Resume);
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
            return Self::resume_visible_command(&state, command_id);
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

    fn prepare_authorization(
        &self,
        command_id: CommandId,
        effect: PreparedCommandEffect,
    ) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if existing.state.is_committed() {
            let Some(retained) = Self::retained_prepared_effect(&state, command_id) else {
                return Err(NodeError::CommandNotExecuting(command_id));
            };
            if retained != &effect {
                return Err(NodeError::CommandConflict(command_id));
            }
            return Self::resume_visible_command(&state, command_id);
        }
        match existing.state {
            CommandState::Executing => {
                if effect.command_updated_at() != existing.updated_at {
                    return Err(NodeError::CommandConflict(command_id));
                }
                Self::validate_prepared_effect(command_id, &existing.request, &effect)?;
                let snapshot = self.append_prepared_locked(&mut state, existing.request, effect)?;
                drop(state);
                Ok(snapshot)
            }
            CommandState::AuthorizationPrepared { effect: retained } => {
                if *retained != effect {
                    return Err(NodeError::CommandConflict(command_id));
                }
                Self::resume_visible_command(&state, command_id)
            }
            CommandState::AuthorizationPending { .. } => {
                let Some(retained) = Self::retained_prepared_effect(&state, command_id) else {
                    return Err(NodeError::CommandNotExecuting(command_id));
                };
                if retained != &effect {
                    return Err(NodeError::CommandConflict(command_id));
                }
                Self::resume_visible_command(&state, command_id)
            }
            _ => Err(NodeError::CommandNotExecuting(command_id)),
        }
    }

    fn commit_prepared_authorization(
        &self,
        command_id: CommandId,
        effect_digest: &str,
    ) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if existing.state.is_committed() {
            let Some(retained) = Self::retained_prepared_effect(&state, command_id) else {
                return Err(NodeError::CommandNotExecuting(command_id));
            };
            if retained.effect_digest() != effect_digest {
                return Err(NodeError::CommandConflict(command_id));
            }
            return Self::resume_visible_command(&state, command_id);
        }
        let CommandState::AuthorizationPrepared { effect } = existing.state else {
            return Err(NodeError::CommandNotExecuting(command_id));
        };
        if effect.effect_digest() != effect_digest {
            return Err(NodeError::CommandConflict(command_id));
        }
        Self::validate_prepared_effect(command_id, &existing.request, &effect)?;
        let snapshot =
            self.append_committed_effect_locked(&mut state, existing.request, *effect)?;
        drop(state);
        Ok(snapshot)
    }

    fn await_prepared_authorization(
        &self,
        command_id: CommandId,
        effect_digest: &str,
        challenge_id: ChallengeId,
    ) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        let effect = match existing.state {
            CommandState::AuthorizationPrepared { effect } => effect,
            CommandState::AuthorizationPending {
                challenge_id: retained_challenge,
                batch,
                result,
                ..
            } => {
                let Some(prepared) = Self::retained_prepared_effect(&state, command_id) else {
                    return Err(NodeError::CommandNotExecuting(command_id));
                };
                if prepared.effect_digest() != effect_digest
                    || prepared.batch() != batch.as_ref()
                    || prepared.result() != result
                    || retained_challenge != challenge_id
                {
                    return Err(NodeError::CommandConflict(command_id));
                }
                return Self::resume_visible_command(&state, command_id);
            }
            _ => return Err(NodeError::CommandNotExecuting(command_id)),
        };
        if effect.effect_digest() != effect_digest {
            return Err(NodeError::CommandConflict(command_id));
        }
        Self::validate_prepared_effect(command_id, &existing.request, &effect)?;
        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::AuthorizationPending {
                challenge_id,
                batch: Box::new(effect.batch().clone()),
                result: effect.result().to_vec(),
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

    fn reject(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if !matches!(
            existing.state,
            CommandState::Executing | CommandState::AuthorizationPrepared { .. }
        ) {
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
        Self::validate_pending_effect(&state, command_id, &existing.request, &batch, &result)?;
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
        Self::validate_pending_effect(&state, command_id, &existing.request, &batch, &result)?;
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
            return Self::resume_visible_command(&state, command_id);
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
        Self::visible_command(&state, command_id)
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

    fn causal_events_through(&self, through: LogPosition) -> Result<Vec<EventEnvelope>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        state
            .causal_index
            .ordered_origins(Some(through))
            .into_iter()
            .map(|origin| {
                state
                    .origin_indexes
                    .get(&origin)
                    .and_then(|index| state.events.get(*index))
                    .cloned()
                    .ok_or_else(|| {
                        NodeError::CorruptHistory(
                            "causal index references absent history".to_owned(),
                        )
                    })
            })
            .collect()
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

    fn ingest(&self, event: EventEnvelope) -> Result<IngestStatus, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        if let Some(index) = state.origin_indexes.get(&event.origin) {
            let existing = state.events.get(*index).ok_or_else(|| {
                NodeError::CorruptHistory("event origin index exceeds retained history".to_owned())
            })?;
            if existing.recorded_at != event.recorded_at || existing.event != event.event {
                return Err(NodeError::EventConflict(event.origin));
            }
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
        let causal_append = state.causal_index.prepare(&imported)?;
        if let Some(journal) = &self.journal {
            journal.append(&imported)?;
        }
        state
            .retained_scope_topology
            .observe_event(&imported.event)?;
        Self::observe_scope_ids(&mut state, &imported.event);
        Self::apply_event(&mut state, &imported.event);
        Self::observe_service_scope_position(&mut state, self.node_id, &imported);
        state.next_position = next_position;
        let index = state.events.len();
        state.origin_indexes.insert(imported.origin, index);
        state.causal_index.apply(causal_append);
        state.events.push(imported.clone());
        Self::broadcast_locked(&mut state, &imported);
        drop(state);
        Ok(IngestStatus::Applied {
            position: local_position,
        })
    }
}
