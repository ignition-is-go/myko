use super::*;

/// All authoritative graph changes accepted from one command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeBatch {
    pub id: BatchId,
    pub command_id: CommandId,
    /// Service that owns every mutation and commits the batch atomically.
    pub service_id: ServiceId,
    /// Scope in which the externally admitted command was authorized.
    ///
    /// Nested commands may attach a different concrete `scope_id` to each
    /// mutation while retaining this one service-level atomic batch.
    pub scope_id: ScopeId,
    pub causal_parents: Vec<EventId>,
    pub changes: Vec<ItemMutation>,
}

/// Reconciliation outcome after concurrent changes have been considered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reconciliation {
    FullyVisible,
    PartiallySuperseded,
    FullySuperseded,
}

/// Durable lifecycle of an idempotent command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CommandState {
    Submitted,
    /// A claimed handler encountered a transient failure and released the
    /// command for another ordered attempt.
    Retrying {
        reason: String,
    },
    /// Handler effects were computed and bound to a durable authority
    /// challenge. The command is not executable again until that exact
    /// challenge is approved through an authenticated session.
    AuthorizationPending {
        challenge_id: ChallengeId,
        batch: Box<ChangeBatch>,
        result: Vec<u8>,
        /// Immutable approvals accumulated while advancing an exact parked
        /// effect through multiple obligations.
        #[serde(default)]
        approvals: Vec<ApprovalId>,
    },
    Executing,
    CommittedLocally {
        batch_id: BatchId,
        position: EventId,
    },
    Replicating {
        batch_id: BatchId,
        position: EventId,
    },
    ReplicationDelayed {
        batch_id: BatchId,
        position: EventId,
        reason: String,
    },
    Replicated {
        batch_id: BatchId,
        position: EventId,
        acknowledged_replicas: u32,
        required_replicas: u32,
    },
    Reconciled {
        batch_id: BatchId,
        position: EventId,
        outcome: Reconciliation,
    },
    Rejected {
        reason: String,
    },
    Cancelled {
        reason: String,
    },
}

impl CommandState {
    /// Returns whether command execution has ended locally.
    #[must_use]
    pub const fn is_terminal_locally(&self) -> bool {
        matches!(
            self,
            Self::CommittedLocally { .. }
                | Self::Replicating { .. }
                | Self::ReplicationDelayed { .. }
                | Self::Replicated { .. }
                | Self::Reconciled { .. }
                | Self::Rejected { .. }
                | Self::Cancelled { .. }
        )
    }

    /// Returns whether the command committed an authoritative change batch.
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(
            self,
            Self::CommittedLocally { .. }
                | Self::Replicating { .. }
                | Self::ReplicationDelayed { .. }
                | Self::Replicated { .. }
                | Self::Reconciled { .. }
        )
    }
}

/// Latest durable view of a command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSnapshot {
    pub request: CommandRequest,
    pub state: CommandState,
    pub result: Option<Vec<u8>>,
    pub updated_at: EventId,
}

impl CommandSnapshot {
    /// Decodes this command's result using its generated typed contract.
    ///
    /// `None` means the command has not produced a result. The command's
    /// durable lifecycle remains available through [`Self::state`].
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot belongs to another contract or its
    /// result bytes do not match `C::Output`.
    pub fn typed_result<C: MykoCommand>(&self) -> Result<Option<C::Output>, NodeError> {
        if self.request.service_id != C::SERVICE_ID || self.request.command_type != C::COMMAND_TYPE
        {
            return Err(NodeError::CommandSchemaMismatch {
                expected_service: C::SERVICE_ID.as_str(),
                expected_command: C::COMMAND_TYPE,
                actual_service: self.request.service_id.as_str().to_owned(),
                actual_command: self.request.command_type.clone(),
            });
        }
        self.result
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(|error| NodeError::ResultDecoding(error.to_string()))
    }

    /// Decodes a completed typed result or reports a terminal command failure.
    ///
    /// `None` means the command is still progressing. A locally terminal
    /// successful command without an encoded result is rejected as corrupt
    /// lifecycle state rather than leaving a caller waiting forever.
    ///
    /// # Errors
    ///
    /// Returns an error for a schema mismatch, invalid result, rejection,
    /// cancellation, or missing terminal result.
    pub fn typed_completion<C: MykoCommand>(&self) -> Result<Option<C::Output>, NodeError> {
        if let Some(result) = self.typed_result::<C>()? {
            return Ok(Some(result));
        }
        match &self.state {
            CommandState::Rejected { reason } => Err(NodeError::CommandRejected {
                command_id: self.request.id,
                reason: reason.clone(),
            }),
            CommandState::Cancelled { reason } => Err(NodeError::CommandCancelled {
                command_id: self.request.id,
                reason: reason.clone(),
            }),
            state if state.is_terminal_locally() => Err(NodeError::ResultDecoding(format!(
                "command {} reached {state:?} without a typed result",
                self.request.id
            ))),
            _ => Ok(None),
        }
    }
}

/// Transport-neutral response from a command endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResponse {
    /// Stable Myko identity of the node serving this response.
    pub source_node: NodeId,
    /// Current durable command state, or `None` for an unknown ID.
    pub command: Option<CommandSnapshot>,
}

/// Default number of current command states returned by one transport page.
pub const DEFAULT_COMMAND_STATE_PAGE_SIZE: u32 = 256;

/// Hard framework limit for one current-command transport page.
pub const MAX_COMMAND_STATE_PAGE_SIZE: u32 = 4_096;

/// Transport-neutral request for one page of current command state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStateRequest {
    /// Authoritative command source, or the serving node when omitted.
    pub source_node: Option<NodeId>,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub command_type: String,
    /// Immutable serving-log ceiling selected by the first page.
    pub snapshot_through: Option<LogPosition>,
    /// Exclusive lexical command-ID cursor within that snapshot.
    pub after_command_id: Option<String>,
    pub page_size: u32,
}

impl CommandStateRequest {
    /// Creates a request for one declared command contract at an explicit source.
    #[must_use]
    pub fn for_declared<C: MykoCommand>(source_node: NodeId, scope_id: ScopeId) -> Self {
        Self {
            source_node: Some(source_node),
            service_id: ServiceId::new(C::SERVICE_ID),
            scope_id,
            command_type: C::COMMAND_TYPE.to_owned(),
            snapshot_through: None,
            after_command_id: None,
            page_size: DEFAULT_COMMAND_STATE_PAGE_SIZE,
        }
    }

    /// Creates a request for the serving node's declared commands.
    #[must_use]
    pub fn for_serving_declared<C: MykoCommand>(scope_id: ScopeId) -> Self {
        Self {
            source_node: None,
            service_id: ServiceId::new(C::SERVICE_ID),
            scope_id,
            command_type: C::COMMAND_TYPE.to_owned(),
            snapshot_through: None,
            after_command_id: None,
            page_size: DEFAULT_COMMAND_STATE_PAGE_SIZE,
        }
    }

    /// Selects the requested transport page size.
    #[must_use]
    pub const fn with_page_size(mut self, page_size: u32) -> Self {
        self.page_size = page_size;
        self
    }
}

/// One current command plus durable ordering metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStateEntry {
    pub admitted_at: LogPosition,
    pub last_changed_at: LogPosition,
    pub command: CommandSnapshot,
}

/// One bounded cursor-stable page of current command states.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStatePage {
    pub serving_node: NodeId,
    pub through: Option<LogPosition>,
    pub request: CommandStateRequest,
    pub commands: Vec<CommandStateEntry>,
    pub next_after_command_id: Option<String>,
}

/// Complete current command state collected from one or more bounded pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStateSnapshot {
    pub serving_node: NodeId,
    pub through: Option<LogPosition>,
    pub request: CommandStateRequest,
    pub commands: Vec<CommandStateEntry>,
}

/// Lossless cursor request for one source/service/scope command catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandWatchRequest {
    pub serving_node: NodeId,
    pub source_node: NodeId,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub command_type: String,
    pub after: Option<LogPosition>,
}

/// One matching durable command transition on a catalog stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStateUpdate {
    pub through: LogPosition,
    pub command: CommandSnapshot,
}

/// Client-side materializer for a snapshot-then-live command catalog.
pub struct CommandStateStream {
    request: CommandWatchRequest,
    through: Option<LogPosition>,
    commands: BTreeMap<String, CommandStateEntry>,
}

/// One decoded application command lifecycle from a typed catalog.
pub struct TypedCommandState<C: MykoCommand> {
    pub admitted_at: LogPosition,
    pub last_changed_at: LogPosition,
    pub id: CommandId,
    pub scope_id: ScopeId,
    pub principal_id: PrincipalId,
    pub command: C,
    pub state: CommandState,
    pub result: Option<C::Output>,
    pub updated_at: EventId,
}

impl CommandStateSnapshot {
    /// Decodes all current commands using one typed application contract.
    ///
    /// Results retain admission order rather than lexical command-ID order.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested contract or any body/result is invalid.
    pub fn typed<C: MykoCommand>(&self) -> Result<Vec<TypedCommandState<C>>, NodeError> {
        if self.request.service_id != C::SERVICE_ID || self.request.command_type != C::COMMAND_TYPE
        {
            return Err(NodeError::CommandSchemaMismatch {
                expected_service: C::SERVICE_ID.as_str(),
                expected_command: C::COMMAND_TYPE,
                actual_service: self.request.service_id.as_str().to_owned(),
                actual_command: self.request.command_type.clone(),
            });
        }
        let mut commands = self.commands.iter().collect::<Vec<_>>();
        commands.sort_unstable_by(|left, right| {
            left.admitted_at.cmp(&right.admitted_at).then_with(|| {
                left.command
                    .request
                    .id
                    .to_string()
                    .cmp(&right.command.request.id.to_string())
            })
        });
        commands
            .into_iter()
            .map(decode_typed_command_state::<C>)
            .collect()
    }

    /// Creates the lossless follow cursor for this completed snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot has no resolved source or its request
    /// is not bound to the collected serving-log ceiling.
    pub fn watch_request(&self) -> Result<CommandWatchRequest, NodeError> {
        let source_node = self.request.source_node.ok_or_else(|| {
            NodeError::InvalidCommandState(
                "command snapshot did not resolve its authoritative source".to_owned(),
            )
        })?;
        if self.request.snapshot_through != self.through || self.request.after_command_id.is_some()
        {
            return Err(NodeError::InvalidCommandState(
                "command snapshot is not bound to one complete cursor".to_owned(),
            ));
        }
        Ok(CommandWatchRequest {
            serving_node: self.serving_node,
            source_node,
            service_id: self.request.service_id.clone(),
            scope_id: self.request.scope_id.clone(),
            command_type: self.request.command_type.clone(),
            after: self.through,
        })
    }

    pub(super) fn from_first_page(
        page: CommandStatePage,
    ) -> Result<(Self, Option<CommandStateRequest>), NodeError> {
        if page.request.after_command_id.is_some() {
            return Err(NodeError::InvalidCommandState(
                "a complete command snapshot must begin without a command cursor".to_owned(),
            ));
        }
        let next = next_command_state_request(&page)?;
        Ok((
            Self {
                serving_node: page.serving_node,
                through: page.through,
                request: page.request,
                commands: page.commands,
            },
            next,
        ))
    }

    pub(super) fn append_page(
        &mut self,
        expected_request: &CommandStateRequest,
        page: CommandStatePage,
    ) -> Result<Option<CommandStateRequest>, NodeError> {
        if &page.request != expected_request
            || page.serving_node != self.serving_node
            || page.through != self.through
        {
            return Err(NodeError::InvalidCommandState(
                "command-state pagination changed request, server, or snapshot cursor".to_owned(),
            ));
        }
        let next = next_command_state_request(&page)?;
        self.commands.extend(page.commands);
        Ok(next)
    }
}

impl CommandWatchRequest {
    /// Filters one durable envelope into this exact command contract.
    #[must_use]
    pub fn update_from_envelope(&self, envelope: &EventEnvelope) -> Option<CommandStateUpdate> {
        if envelope.origin.node_id != self.source_node {
            return None;
        }
        let command = command_from_event(&envelope.event);
        (command.request.service_id == self.service_id
            && command.request.scope_id.equivalent_to(&self.scope_id)
            && command.request.command_type == self.command_type)
            .then(|| CommandStateUpdate {
                through: envelope.position,
                command: command.clone(),
            })
    }
}

impl CommandStateStream {
    /// Starts a live materializer from a completed command snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot identity, cursor, or entries are
    /// malformed.
    pub fn from_snapshot(snapshot: &CommandStateSnapshot) -> Result<Self, NodeError> {
        let request = snapshot.watch_request()?;
        let mut commands = BTreeMap::new();
        for entry in &snapshot.commands {
            validate_command_state_entry(&request, snapshot.through, entry)?;
            let key = entry.command.request.id.to_string();
            if commands.insert(key, entry.clone()).is_some() {
                return Err(NodeError::InvalidCommandState(
                    "command snapshot contains a duplicate command ID".to_owned(),
                ));
            }
        }
        Ok(Self {
            request,
            through: snapshot.through,
            commands,
        })
    }

    /// Returns the exact remote follow request represented by this stream.
    #[must_use]
    pub const fn request(&self) -> &CommandWatchRequest {
        &self.request
    }

    /// Returns the currently materialized command catalog.
    #[must_use]
    pub fn current(&self) -> CommandStateSnapshot {
        CommandStateSnapshot {
            serving_node: self.request.serving_node,
            through: self.through,
            request: CommandStateRequest {
                source_node: Some(self.request.source_node),
                service_id: self.request.service_id.clone(),
                scope_id: self.request.scope_id.clone(),
                command_type: self.request.command_type.clone(),
                snapshot_through: self.through,
                after_command_id: None,
                page_size: DEFAULT_COMMAND_STATE_PAGE_SIZE,
            },
            commands: self.commands.values().cloned().collect(),
        }
    }

    /// Applies one matching durable transition atomically.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale stream cursor or mismatched command.
    pub fn apply(
        &mut self,
        update: &CommandStateUpdate,
    ) -> Result<CommandStateSnapshot, NodeError> {
        if self
            .through
            .is_some_and(|through| update.through <= through)
        {
            return Err(NodeError::InvalidCommandState(
                "command stream did not advance its serving cursor".to_owned(),
            ));
        }
        validate_command_update(&self.request, update)?;
        let key = update.command.request.id.to_string();
        if let Some(entry) = self.commands.get_mut(&key) {
            entry.admitted_at = entry.admitted_at.min(update.command.updated_at.sequence);
            if command_transition_is_newer(&entry.command, &update.command) {
                entry.last_changed_at = update.through;
                entry.command = update.command.clone();
            }
        } else {
            self.commands.insert(
                key,
                CommandStateEntry {
                    admitted_at: update.command.updated_at.sequence,
                    last_changed_at: update.through,
                    command: update.command.clone(),
                },
            );
        }
        self.through = Some(update.through);
        Ok(self.current())
    }
}

/// Gap-free current-then-live watch for one durable command lifecycle.
pub struct CommandWatch {
    pub(super) command_id: CommandId,
    pub(super) current: CommandSnapshot,
    pub(super) events: EventSubscription,
}

impl CommandWatch {
    /// Returns the latest lifecycle state materialized by this watch.
    #[must_use]
    pub const fn current(&self) -> &CommandSnapshot {
        &self.current
    }

    /// Waits for the command's next durable lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns an error if the event subscription closes.
    pub fn recv(&mut self) -> Result<CommandSnapshot, NodeError> {
        loop {
            let envelope = self.events.recv()?;
            let command = command_from_event(&envelope.event);
            if command.request.id == self.command_id
                && command_transition_is_newer(&self.current, command)
            {
                self.current = command.clone();
                return Ok(self.current.clone());
            }
        }
    }

    /// Asynchronously waits for the command's next durable lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns an error if the event subscription closes.
    pub async fn recv_async(&mut self) -> Result<CommandSnapshot, NodeError> {
        loop {
            let envelope = self.events.recv_async().await?;
            let command = command_from_event(&envelope.event);
            if command.request.id == self.command_id
                && command_transition_is_newer(&self.current, command)
            {
                self.current = command.clone();
                return Ok(self.current.clone());
            }
        }
    }
}

/// Result of admitting a stable command ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "admission", content = "command", rename_all = "snake_case")]
pub enum CommandAdmission {
    /// This node atomically won admission and may execute the command.
    Execute(CommandSnapshot),
    /// The command already exists; observe or resume it without re-execution.
    Resume(CommandSnapshot),
}

impl CommandAdmission {
    /// Returns the current command snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &CommandSnapshot {
        match self {
            Self::Execute(snapshot) | Self::Resume(snapshot) => snapshot,
        }
    }

    /// Returns whether the caller owns this execution attempt.
    #[must_use]
    pub const fn should_execute(&self) -> bool {
        matches!(self, Self::Execute(_))
    }
}
