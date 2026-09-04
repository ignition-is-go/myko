use super::*;

/// Initial result of a gap-free typed query watch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemQuerySnapshot<T> {
    /// Exclusive node-local cursor covered by `value`.
    pub through: Option<LogPosition>,
    pub value: T,
}

/// Default number of current item sets returned by one transport page.
pub const DEFAULT_ITEM_STATE_PAGE_SIZE: u32 = 256;

/// Hard framework limit for one current-state transport page.
pub const MAX_ITEM_STATE_PAGE_SIZE: u32 = 4_096;

/// Transport-neutral request for one page of typed current state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStateRequest {
    /// Authoritative item source, or the serving node when omitted.
    pub source_node: Option<NodeId>,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub item_type: String,
    pub schema_version: u32,
    /// Immutable node-log ceiling selected by the first page.
    pub snapshot_through: Option<LogPosition>,
    /// Exclusive lexical item-ID cursor within that snapshot.
    pub after_item_id: Option<String>,
    pub page_size: u32,
}

impl ItemStateRequest {
    /// Creates the wire request for a concrete item schema.
    #[must_use]
    pub fn for_item<T: MykoItem>(source_node: NodeId, scope_id: ScopeId) -> Self {
        Self {
            source_node: Some(source_node),
            service_id: ServiceId::new(T::SERVICE_ID),
            scope_id,
            item_type: T::ITEM_TYPE.to_owned(),
            schema_version: T::SCHEMA_VERSION,
            snapshot_through: None,
            after_item_id: None,
            page_size: DEFAULT_ITEM_STATE_PAGE_SIZE,
        }
    }

    /// Creates a request for the serving node's own authoritative items.
    #[must_use]
    pub fn for_serving_item<T: MykoItem>(scope_id: ScopeId) -> Self {
        Self {
            source_node: None,
            service_id: ServiceId::new(T::SERVICE_ID),
            scope_id,
            item_type: T::ITEM_TYPE.to_owned(),
            schema_version: T::SCHEMA_VERSION,
            snapshot_through: None,
            after_item_id: None,
            page_size: DEFAULT_ITEM_STATE_PAGE_SIZE,
        }
    }

    /// Selects the requested transport page size.
    ///
    /// The serving node validates this against
    /// [`MAX_ITEM_STATE_PAGE_SIZE`].
    #[must_use]
    pub const fn with_page_size(mut self, page_size: u32) -> Self {
        self.page_size = page_size;
        self
    }
}

/// One bounded, cursor-stable page of schema-specific current item sets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStateEntry {
    /// Authoritative source-log position of this item's latest set mutation.
    pub last_changed_at: LogPosition,
    /// Stable order of the mutation within its atomic command batch.
    pub change_index: u32,
    /// Scope carried by the immutable command batch that produced this item.
    ///
    /// This lets typed consumers reconstruct parent foreign keys for journal
    /// entries written before item mutations carried their own placement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containing_scope_id: Option<ScopeId>,
    pub mutation: ItemMutation,
}

/// One bounded, cursor-stable page of schema-specific current item sets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStatePage {
    pub serving_node: NodeId,
    pub through: Option<LogPosition>,
    pub request: ItemStateRequest,
    pub items: Vec<ItemStateEntry>,
    pub next_after_item_id: Option<String>,
}

/// Current schema-specific item state returned by an embedded or remote node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStateSnapshot {
    pub serving_node: NodeId,
    pub through: Option<LogPosition>,
    pub request: ItemStateRequest,
    pub items: Vec<ItemStateEntry>,
}

impl ItemStateSnapshot {
    /// Decodes this raw schema snapshot and executes a generated typed query.
    ///
    /// # Errors
    ///
    /// Returns an error if the response schema or any item payload is invalid.
    #[allow(clippy::needless_pass_by_value)] // The typed query is a one-shot snapshot request.
    pub fn query<Q>(&self, query: Q) -> Result<ItemQuerySnapshot<ItemQueryResult<Q>>, NodeError>
    where
        Q: ItemQuery,
    {
        if self.request.service_id != Q::Item::SERVICE_ID
            || self.request.item_type != Q::Item::ITEM_TYPE
            || self.request.schema_version != Q::Item::SCHEMA_VERSION
        {
            return Err(NodeError::InvalidItemMutation(format!(
                "item-state response schema {}/{}@{} does not match {}/{}@{}",
                self.request.service_id,
                self.request.item_type,
                self.request.schema_version,
                Q::Item::SERVICE_ID,
                Q::Item::ITEM_TYPE,
                Q::Item::SCHEMA_VERSION
            )));
        }
        let mut projection = ItemProjection::<Q::Item>::default();
        for item in &self.items {
            projection
                .apply_at_order_in_scope(
                    &item.mutation,
                    item.containing_scope_id.as_ref().map(ScopeId::as_str),
                    item.last_changed_at.get(),
                    item.change_index,
                )
                .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?;
        }
        Ok(ItemQuerySnapshot {
            through: self.through,
            value: __snapshot_item_query(&query, &projection),
        })
    }

    pub(super) fn from_first_page(
        page: ItemStatePage,
    ) -> Result<(Self, Option<ItemStateRequest>), NodeError> {
        if page.request.after_item_id.is_some() {
            return Err(NodeError::InvalidItemState(
                "a complete item snapshot must begin without an item cursor".to_owned(),
            ));
        }
        let next = next_item_state_request(&page)?;
        Ok((
            Self {
                serving_node: page.serving_node,
                through: page.through,
                request: page.request,
                items: page.items,
            },
            next,
        ))
    }

    pub(super) fn append_page(
        &mut self,
        expected_request: &ItemStateRequest,
        page: ItemStatePage,
    ) -> Result<Option<ItemStateRequest>, NodeError> {
        if &page.request != expected_request
            || page.serving_node != self.serving_node
            || page.through != self.through
        {
            return Err(NodeError::InvalidItemState(
                "item-state pagination changed request, server, or snapshot cursor".to_owned(),
            ));
        }
        let next = next_item_state_request(&page)?;
        self.items.extend(page.items);
        Ok(next)
    }

    /// Creates a durable typed-update request beginning after this snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the collected snapshot did not resolve its source
    /// identity or retain its initial request invariants.
    pub fn follow_request(&self) -> Result<ItemFollowRequest, NodeError> {
        let source_node = self.request.source_node.ok_or_else(|| {
            NodeError::InvalidItemState(
                "item-state snapshot did not resolve its authoritative source".to_owned(),
            )
        })?;
        if self.request.after_item_id.is_some() || self.request.snapshot_through != self.through {
            return Err(NodeError::InvalidItemState(
                "item-state snapshot cannot seed a durable item stream".to_owned(),
            ));
        }
        Ok(ItemFollowRequest {
            serving_node: self.serving_node,
            source_node,
            service_id: self.request.service_id.clone(),
            scope_id: self.request.scope_id.clone(),
            item_type: self.request.item_type.clone(),
            schema_version: self.request.schema_version,
            after: self.through,
        })
    }
}

/// Durable typed-item stream requested after a complete current snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemFollowRequest {
    pub serving_node: NodeId,
    pub source_node: NodeId,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub item_type: String,
    pub schema_version: u32,
    pub after: Option<LogPosition>,
}

impl ItemFollowRequest {
    /// Projects one matching atomic item update from an immutable event.
    ///
    /// Unrelated lifecycle, source, service, scope, and item events are
    /// omitted without exposing their bodies to the remote consumer.
    ///
    /// # Errors
    ///
    /// Returns an error if matching history contains an invalid or unknown
    /// schema version.
    pub fn update_from_envelope(
        &self,
        envelope: &EventEnvelope,
    ) -> Result<Option<ItemStateUpdate>, NodeError> {
        if self.after.is_some_and(|after| envelope.position <= after)
            || envelope.origin.node_id != self.source_node
        {
            return Ok(None);
        }
        let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
            return Ok(None);
        };
        if command.request.service_id != self.service_id {
            return Ok(None);
        }
        let mut changes = Vec::new();
        for mutation in &batch.changes {
            if mutation.item_type != self.item_type
                || !erased_mutation_affects_scope(mutation, &batch.scope_id, &self.scope_id)
            {
                continue;
            }
            mutation
                .validate_envelope()
                .map_err(|error| NodeError::InvalidItemState(error.to_string()))?;
            if mutation.schema_version != self.schema_version {
                return Err(NodeError::InvalidItemState(format!(
                    "item stream contains {}@{}, requested {}@{}",
                    mutation.item_type,
                    mutation.schema_version,
                    self.item_type,
                    self.schema_version
                )));
            }
            changes.push(mutation.clone());
        }
        if changes.is_empty() {
            return Ok(None);
        }
        Ok(Some(ItemStateUpdate {
            serving_node: self.serving_node,
            source_node: self.source_node,
            service_id: self.service_id.clone(),
            scope_id: self.scope_id.clone(),
            item_type: self.item_type.clone(),
            schema_version: self.schema_version,
            through: envelope.position,
            containing_scope_id: Some(batch.scope_id.clone()),
            changes,
        }))
    }
}

/// One atomic schema-filtered update on a durable typed item stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStateUpdate {
    pub serving_node: NodeId,
    pub source_node: NodeId,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub item_type: String,
    pub schema_version: u32,
    pub through: LogPosition,
    /// Scope carried by the immutable command batch producing `changes`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containing_scope_id: Option<ScopeId>,
    pub changes: Vec<ItemMutation>,
}

/// Transport-neutral typed query materializer for a snapshot plus updates.
pub struct ItemQueryStream<Q: ItemQuery> {
    query: Q,
    projection: ItemProjection<Q::Item>,
    request: ItemFollowRequest,
    through: Option<LogPosition>,
}

impl<Q: ItemQuery> ItemQueryStream<Q> {
    /// Seeds a typed query stream from one fully collected snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot schema or payload is invalid.
    pub fn from_snapshot(
        snapshot: &ItemStateSnapshot,
        query: Q,
    ) -> Result<(ItemQuerySnapshot<ItemQueryResult<Q>>, Self), NodeError> {
        if snapshot.request.service_id != Q::Item::SERVICE_ID
            || snapshot.request.item_type != Q::Item::ITEM_TYPE
            || snapshot.request.schema_version != Q::Item::SCHEMA_VERSION
        {
            return Err(NodeError::InvalidItemState(format!(
                "item stream schema {}/{}@{} does not match {}/{}@{}",
                snapshot.request.service_id,
                snapshot.request.item_type,
                snapshot.request.schema_version,
                Q::Item::SERVICE_ID,
                Q::Item::ITEM_TYPE,
                Q::Item::SCHEMA_VERSION
            )));
        }
        let request = snapshot.follow_request()?;
        let mut projection = ItemProjection::<Q::Item>::default();
        for item in &snapshot.items {
            projection
                .apply_at_order_in_scope(
                    &item.mutation,
                    item.containing_scope_id.as_ref().map(ScopeId::as_str),
                    item.last_changed_at.get(),
                    item.change_index,
                )
                .map_err(|error| NodeError::InvalidItemState(error.to_string()))?;
        }
        let value = __snapshot_item_query(&query, &projection);
        let through = snapshot.through;
        Ok((
            ItemQuerySnapshot { through, value },
            Self {
                query,
                projection,
                request,
                through,
            },
        ))
    }

    /// Returns the immutable remote stream contract.
    #[must_use]
    pub const fn request(&self) -> &ItemFollowRequest {
        &self.request
    }

    /// Computes the current typed query value without advancing the stream.
    #[must_use]
    pub fn current(&self) -> ItemQueryResult<Q> {
        __snapshot_item_query(&self.query, &self.projection)
    }

    /// Returns the current typed projection with framework ordering metadata.
    #[must_use]
    pub const fn current_projection(&self) -> &ItemProjection<Q::Item> {
        &self.projection
    }

    /// Validates and atomically applies one transport-delivered item update.
    ///
    /// # Errors
    ///
    /// Returns an error without changing the projection if stream identity,
    /// cursor ordering, schema, or any mutation is invalid.
    pub fn apply(
        &mut self,
        update: &ItemStateUpdate,
    ) -> Result<ItemQueryUpdate<ItemQueryResult<Q>>, NodeError> {
        if update.serving_node != self.request.serving_node
            || update.source_node != self.request.source_node
            || update.service_id != self.request.service_id
            || update.scope_id != self.request.scope_id
            || update.item_type != self.request.item_type
            || update.schema_version != self.request.schema_version
            || self
                .through
                .is_some_and(|through| update.through <= through)
            || update.changes.is_empty()
        {
            return Err(NodeError::InvalidItemState(
                "item update changed stream identity, regressed its cursor, or was empty"
                    .to_owned(),
            ));
        }
        let mut projection = self.projection.clone();
        for (index, mutation) in update.changes.iter().enumerate() {
            let change_index = u32::try_from(index).map_err(|error| {
                NodeError::InvalidItemState(format!(
                    "item update contains too many ordered changes: {error}"
                ))
            })?;
            if !projection
                .apply_at_order_in_scope(
                    mutation,
                    update.containing_scope_id.as_ref().map(ScopeId::as_str),
                    update.through.get(),
                    change_index,
                )
                .map_err(|error| NodeError::InvalidItemState(error.to_string()))?
            {
                return Err(NodeError::InvalidItemState(
                    "item update contained another item type".to_owned(),
                ));
            }
        }
        let value = __snapshot_item_query(&self.query, &projection);
        self.projection = projection;
        self.through = Some(update.through);
        Ok(ItemQueryUpdate {
            position: update.through,
            value,
        })
    }
}

pub(super) fn validate_command_state_entry(
    request: &CommandWatchRequest,
    through: Option<LogPosition>,
    entry: &CommandStateEntry,
) -> Result<(), NodeError> {
    if entry.command.updated_at.node_id != request.source_node
        || entry.command.request.service_id != request.service_id
        || !entry
            .command
            .request
            .scope_id
            .equivalent_to(&request.scope_id)
        || entry.command.request.command_type != request.command_type
        || entry.admitted_at > entry.command.updated_at.sequence
        || entry.admitted_at > entry.last_changed_at
        || through.is_none_or(|ceiling| entry.last_changed_at > ceiling)
    {
        return Err(NodeError::InvalidCommandState(
            "command catalog entry does not match its source, contract, or cursor".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn validate_command_update(
    request: &CommandWatchRequest,
    update: &CommandStateUpdate,
) -> Result<(), NodeError> {
    if update.command.updated_at.node_id != request.source_node
        || update.command.request.service_id != request.service_id
        || !update
            .command
            .request
            .scope_id
            .equivalent_to(&request.scope_id)
        || update.command.request.command_type != request.command_type
    {
        return Err(NodeError::InvalidCommandState(
            "command stream update does not match its source or contract".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn validate_command_state_request(
    request: &CommandStateRequest,
) -> Result<(), NodeError> {
    if request.command_type.is_empty()
        || request.page_size == 0
        || request.page_size > MAX_COMMAND_STATE_PAGE_SIZE
        || request
            .after_command_id
            .as_ref()
            .is_some_and(String::is_empty)
    {
        return Err(NodeError::InvalidCommandState(format!(
            "command-state request requires a command type, a non-empty cursor, and a page size between 1 and {MAX_COMMAND_STATE_PAGE_SIZE}"
        )));
    }
    Ok(())
}

pub(super) fn materialize_command_state_entries(
    history: Vec<EventEnvelope>,
    source_node: NodeId,
    request: &CommandStateRequest,
    through: Option<LogPosition>,
) -> BTreeMap<String, CommandStateEntry> {
    let mut current = BTreeMap::<String, CommandStateEntry>::new();
    for envelope in history {
        if through.is_some_and(|ceiling| envelope.position > ceiling)
            || envelope.origin.node_id != source_node
        {
            continue;
        }
        let command = command_from_event(&envelope.event);
        if command.request.service_id != request.service_id
            || !command.request.scope_id.equivalent_to(&request.scope_id)
            || command.request.command_type != request.command_type
        {
            continue;
        }
        let key = command.request.id.to_string();
        if let Some(entry) = current.get_mut(&key) {
            entry.admitted_at = entry.admitted_at.min(command.updated_at.sequence);
            if command_transition_is_newer(&entry.command, command) {
                entry.last_changed_at = envelope.position;
                entry.command = command.clone();
            }
        } else {
            current.insert(
                key,
                CommandStateEntry {
                    admitted_at: command.updated_at.sequence,
                    last_changed_at: envelope.position,
                    command: command.clone(),
                },
            );
        }
    }
    current
}

pub(super) fn next_command_state_request(
    page: &CommandStatePage,
) -> Result<Option<CommandStateRequest>, NodeError> {
    if page.request.snapshot_through != page.through {
        return Err(NodeError::InvalidCommandState(
            "command-state page did not bind its snapshot cursor".to_owned(),
        ));
    }
    validate_command_state_request(&page.request)?;
    let page_size = usize::try_from(page.request.page_size).map_err(|error| {
        NodeError::InvalidCommandState(format!(
            "command-state page size is not addressable: {error}"
        ))
    })?;
    if page.commands.len() > page_size {
        return Err(NodeError::InvalidCommandState(
            "command-state response exceeded its requested page size".to_owned(),
        ));
    }
    let mut previous = page.request.after_command_id.clone();
    for entry in &page.commands {
        let command_id = entry.command.request.id.to_string();
        if entry.command.request.service_id != page.request.service_id
            || !entry
                .command
                .request
                .scope_id
                .equivalent_to(&page.request.scope_id)
            || entry.command.request.command_type != page.request.command_type
            || entry.admitted_at > entry.last_changed_at
            || page
                .through
                .is_none_or(|through| entry.last_changed_at > through)
            || previous
                .as_deref()
                .is_some_and(|cursor| command_id.as_str() <= cursor)
        {
            return Err(NodeError::InvalidCommandState(
                "command-state page contains mismatched or unordered state".to_owned(),
            ));
        }
        previous = Some(command_id);
    }
    let Some(next_after) = page.next_after_command_id.as_ref() else {
        return Ok(None);
    };
    if page.commands.len() != page_size
        || page
            .commands
            .last()
            .is_none_or(|entry| entry.command.request.id.to_string() != *next_after)
    {
        return Err(NodeError::InvalidCommandState(
            "command-state continuation does not match its last full-page command".to_owned(),
        ));
    }
    let mut next = page.request.clone();
    next.after_command_id = Some(next_after.clone());
    Ok(Some(next))
}

pub(super) fn materialize_item_state_entries(
    history: Vec<EventEnvelope>,
    source_node: NodeId,
    request: &ItemStateRequest,
    through: Option<LogPosition>,
) -> Result<BTreeMap<String, ItemStateEntry>, NodeError> {
    let mut current = BTreeMap::new();
    for envelope in history {
        if through.is_some_and(|ceiling| envelope.position > ceiling)
            || envelope.origin.node_id != source_node
        {
            continue;
        }
        let NodeEvent::CommandCommitted { command, batch } = envelope.event else {
            continue;
        };
        if command.request.service_id != request.service_id {
            continue;
        }
        let containing_scope = batch.scope_id;
        for (index, mutation) in batch.changes.into_iter().enumerate() {
            if mutation.item_type != request.item_type
                || !erased_mutation_affects_scope(&mutation, &containing_scope, &request.scope_id)
            {
                continue;
            }
            let change_index = u32::try_from(index).map_err(|error| {
                NodeError::InvalidItemState(format!(
                    "item-state batch contains too many ordered changes: {error}"
                ))
            })?;
            mutation
                .validate_envelope()
                .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?;
            if mutation.schema_version != request.schema_version {
                return Err(NodeError::InvalidItemMutation(format!(
                    "item-state history contains {}@{}, requested {}@{}",
                    mutation.item_type,
                    mutation.schema_version,
                    request.item_type,
                    request.schema_version
                )));
            }
            match mutation.operation {
                MutationOperation::Set => {
                    current.insert(
                        mutation.item_id.clone(),
                        ItemStateEntry {
                            last_changed_at: envelope.position,
                            change_index,
                            containing_scope_id: Some(containing_scope.clone()),
                            mutation,
                        },
                    );
                }
                MutationOperation::Delete => {
                    current.remove(&mutation.item_id);
                }
            }
        }
    }
    Ok(current)
}

fn erased_mutation_affects_scope(
    mutation: &ItemMutation,
    batch_scope: &ScopeId,
    requested_scope: &ScopeId,
) -> bool {
    let placed_scope = mutation
        .scope_id
        .as_ref()
        .map_or_else(|| batch_scope.clone(), |scope| ScopeId::new(scope.clone()));
    placed_scope.equivalent_to(requested_scope)
}

fn next_item_state_request(page: &ItemStatePage) -> Result<Option<ItemStateRequest>, NodeError> {
    if page.request.snapshot_through != page.through {
        return Err(NodeError::InvalidItemState(
            "item-state page did not bind its snapshot cursor".to_owned(),
        ));
    }
    if page.request.page_size == 0 || page.request.page_size > MAX_ITEM_STATE_PAGE_SIZE {
        return Err(NodeError::InvalidItemState(format!(
            "item-state page size must be between 1 and {MAX_ITEM_STATE_PAGE_SIZE}"
        )));
    }
    let page_size = usize::try_from(page.request.page_size).map_err(|error| {
        NodeError::InvalidItemState(format!("item-state page size is not addressable: {error}"))
    })?;
    if page.items.len() > page_size {
        return Err(NodeError::InvalidItemState(
            "item-state response exceeded its requested page size".to_owned(),
        ));
    }
    let mut previous = page.request.after_item_id.as_deref();
    for item in &page.items {
        item.mutation
            .validate_envelope()
            .map_err(|error| NodeError::InvalidItemState(error.to_string()))?;
        if item.mutation.item_type != page.request.item_type
            || item.mutation.schema_version != page.request.schema_version
            || item.mutation.operation != MutationOperation::Set
            || page
                .through
                .is_none_or(|through| item.last_changed_at > through)
        {
            return Err(NodeError::InvalidItemState(
                "item-state page contains a mismatched, future, or non-current mutation".to_owned(),
            ));
        }
        if previous.is_some_and(|cursor| item.mutation.item_id.as_str() <= cursor) {
            return Err(NodeError::InvalidItemState(
                "item-state page IDs are not strictly increasing after its cursor".to_owned(),
            ));
        }
        previous = Some(item.mutation.item_id.as_str());
    }
    let Some(next_after_item_id) = page.next_after_item_id.as_ref() else {
        return Ok(None);
    };
    if page.items.len() != page_size
        || page
            .items
            .last()
            .is_none_or(|item| &item.mutation.item_id != next_after_item_id)
    {
        return Err(NodeError::InvalidItemState(
            "item-state continuation does not match the last full-page item".to_owned(),
        ));
    }
    let mut next = page.request.clone();
    next.after_item_id = Some(next_after_item_id.clone());
    Ok(Some(next))
}

/// One typed query result after an atomic item batch changes its projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemQueryUpdate<T> {
    /// Node-local cursor of the batch reflected in `value`.
    pub position: LogPosition,
    pub value: T,
}

/// One immutable typed item projection at a gap-free node-log boundary.
///
/// This is framework plumbing for Myko's shared Hyphae projection graph. It
/// deliberately carries Rust values rather than a serialized transport shape.
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct ItemProjectionSnapshot<T: MykoItem> {
    pub through: Option<LogPosition>,
    pub projection: ItemProjection<T>,
}

/// One atomic advance of a shared typed item projection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct ItemProjectionUpdate<T: MykoItem> {
    pub position: LogPosition,
    pub projection: ItemProjection<T>,
    pub diff: Option<MapDiff<T::Id, Arc<ItemState<T>>>>,
}

/// One authorization-filtered reactive selected-query update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedQueryUpdate<T> {
    pub position: LogPosition,
    pub result: SelectedQueryResult<T>,
}

/// Replay-then-live selected query. Rechecks use the continuation phase, so
/// idle/event-driven authorization checks never consume another bounded use.
pub struct SelectedQueryWatch<Q: ItemQuery> {
    pub(super) node: Node,
    pub(super) authenticated_executor: PrincipalId,
    pub(super) presentation: AuthorityPresentation,
    pub(super) source_node: NodeId,
    pub(super) requested: ScopeSelection,
    pub(super) query: Q,
    pub(super) wake: flume::Receiver<SelectedQueryWake>,
    pub(super) _authorization_guard: Option<SubscriptionGuard>,
    pub(super) cursor: Option<LogPosition>,
}

pub(super) enum SelectedQueryWake {
    Event(LogPosition),
    Policy,
    Timer,
}

impl<Q: ItemQuery> SelectedQueryWatch<Q> {
    fn reauthorize(&self) -> Result<SelectedQueryResult<ItemQueryResult<Q>>, NodeError> {
        let result = self.node.query_items_selected_phase(
            self.authenticated_executor.clone(),
            self.presentation.clone(),
            self.source_node,
            &self.requested,
            AuthorizationPhase::Continuation,
            self.query.clone(),
        )?;
        if let Some(decision) = result.authorization.as_ref() {
            return Err(NodeError::AuthorizationDenied(decision.public_message()));
        }
        Ok(result)
    }

    /// Waits for the next durable event and recomputes the authorization-
    /// filtered selected projection.
    ///
    /// # Errors
    ///
    /// Returns an error when history, authority, or the wake channel becomes
    /// unavailable.
    pub fn recv(&mut self) -> Result<SelectedQueryUpdate<ItemQueryResult<Q>>, NodeError> {
        loop {
            match self
                .wake
                .recv()
                .map_err(|_| NodeError::SubscriptionDisconnected)?
            {
                SelectedQueryWake::Event(position) => {
                    self.cursor = Some(position);
                    return Ok(SelectedQueryUpdate {
                        position,
                        result: self.reauthorize()?,
                    });
                }
                SelectedQueryWake::Policy => {
                    let result = self.reauthorize()?;
                    if let Some(position) = self.cursor {
                        return Ok(SelectedQueryUpdate { position, result });
                    }
                }
                SelectedQueryWake::Timer => {
                    let _still_authorized = self.reauthorize()?;
                }
            }
        }
    }

    /// Asynchronously waits for the next selected projection revision.
    ///
    /// # Errors
    ///
    /// Returns an error when history, authority, or the wake channel becomes
    /// unavailable.
    pub async fn recv_async(
        &mut self,
    ) -> Result<SelectedQueryUpdate<ItemQueryResult<Q>>, NodeError> {
        loop {
            match self
                .wake
                .recv_async()
                .await
                .map_err(|_| NodeError::SubscriptionDisconnected)?
            {
                SelectedQueryWake::Event(position) => {
                    self.cursor = Some(position);
                    return Ok(SelectedQueryUpdate {
                        position,
                        result: self.reauthorize()?,
                    });
                }
                SelectedQueryWake::Policy => {
                    let result = self.reauthorize()?;
                    if let Some(position) = self.cursor {
                        return Ok(SelectedQueryUpdate { position, result });
                    }
                }
                SelectedQueryWake::Timer => {
                    let _still_authorized = self.reauthorize()?;
                }
            }
        }
    }
}

/// Replay-then-live typed query materialization over a typed projection.
///
/// The application sees generated query results rather than federation
/// envelopes. Each update is emitted only after its complete atomic batch has
/// been applied to the typed projection.
pub struct ItemQueryWatch<Q: ItemQuery> {
    pub(super) query: Q,
    pub(super) projection: ItemProjection<Q::Item>,
    pub(super) source_node: Option<NodeId>,
    pub(super) service_id: ServiceId,
    pub(super) scope_id: Option<ScopeId>,
    pub(super) events: EventSubscription,
}

/// Gap-free replay-then-follow owner for one typed item projection.
///
/// Application code never needs this driver directly. The retained Myko host owns one
/// per `(item type, source, scope)` and exposes its Hyphae projection.
#[doc(hidden)]
pub struct ItemProjectionWatch<T: MykoItem> {
    pub(super) projection: ItemProjection<T>,
    pub(super) source_node: Option<NodeId>,
    pub(super) service_id: ServiceId,
    pub(super) scope_id: Option<ScopeId>,
    pub(super) events: EventSubscription,
}

impl<T: MykoItem> ItemProjectionWatch<T> {
    /// Waits for the next atomic service/scope revision and returns the typed
    /// projection after applying it.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable follow disconnects or a matching
    /// mutation cannot be decoded as `T`.
    pub async fn recv_async(&mut self) -> Result<ItemProjectionUpdate<T>, NodeError> {
        loop {
            let envelope = self.events.recv_async().await?;
            if let Some(update) = self.apply(&envelope)? {
                return Ok(update);
            }
        }
    }

    pub(super) fn apply(
        &mut self,
        envelope: &EventEnvelope,
    ) -> Result<Option<ItemProjectionUpdate<T>>, NodeError> {
        let advances_cursor = match &envelope.event {
            NodeEvent::CommandCommitted { command, batch } => {
                self.source_node
                    .is_none_or(|source_node| envelope.origin.node_id == source_node)
                    && command.request.service_id == self.service_id
                    && self.scope_id.as_ref().is_none_or(|scope_id| {
                        command.request.scope_id.equivalent_to(scope_id)
                            || batch.changes.iter().any(|mutation| {
                                mutation
                                    .affects_scope::<T>(batch.scope_id.as_str(), scope_id.as_str())
                                    || mutation.scope_id.as_ref().is_some_and(|placed| {
                                        ScopeId::new(placed.clone()).equivalent_to(scope_id)
                                    })
                            })
                    })
            }
            NodeEvent::CommandLifecycle(_) => false,
        };
        if !advances_cursor {
            return Ok(None);
        }
        let NodeEvent::CommandCommitted { batch, .. } = &envelope.event else {
            return Ok(None);
        };
        let mut changes = Vec::new();
        for (index, mutation) in batch.changes.iter().enumerate() {
            if self.scope_id.as_ref().is_some_and(|scope_id| {
                !mutation.affects_scope::<T>(batch.scope_id.as_str(), scope_id.as_str())
            }) {
                continue;
            }
            let before = self
                .projection
                .state_by_stored_id(&mutation.item_id)
                .cloned();
            let change_index = u32::try_from(index).map_err(|error| {
                NodeError::CorruptHistory(format!(
                    "item batch contains too many ordered changes: {error}"
                ))
            })?;
            let applied = self
                .projection
                .apply_at_order_in_scope(
                    mutation,
                    Some(batch.scope_id.as_str()),
                    envelope.position.get(),
                    change_index,
                )
                .map_err(|error| NodeError::CorruptHistory(error.to_string()))?;
            if !applied {
                continue;
            }
            let after = self
                .projection
                .state_by_stored_id(&mutation.item_id)
                .cloned();
            match (before, after) {
                (None, Some(state)) => changes.push(MapDiff::Insert {
                    key: state.value().item_id().clone(),
                    value: Arc::new(state),
                }),
                (Some(state), None) => changes.push(MapDiff::Remove {
                    key: state.value().item_id().clone(),
                    old_value: Arc::new(state),
                }),
                (Some(old_state), Some(new_state)) if old_state != new_state => {
                    changes.push(MapDiff::Update {
                        key: new_state.value().item_id().clone(),
                        old_value: Arc::new(old_state),
                        new_value: Arc::new(new_state),
                    });
                }
                (None, None) | (Some(_), Some(_)) => {}
            }
        }
        let diff = match changes.len() {
            0 => None,
            1 => changes.pop(),
            _ => Some(MapDiff::Batch { changes }),
        };
        Ok(advances_cursor.then(|| ItemProjectionUpdate {
            position: envelope.position,
            projection: self.projection.clone(),
            diff,
        }))
    }
}

impl<Q: ItemQuery> ItemQueryWatch<Q> {
    /// Computes the query's current value without advancing the subscription.
    #[must_use]
    pub fn current(&self) -> ItemQueryResult<Q> {
        __snapshot_item_query(&self.query, &self.projection)
    }

    /// Waits for the next atomic batch that changes this item projection.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription closes or matching item history is
    /// malformed.
    pub fn recv(&mut self) -> Result<ItemQueryUpdate<ItemQueryResult<Q>>, NodeError> {
        loop {
            let envelope = self.events.recv()?;
            if let Some(update) = self.apply(&envelope)? {
                return Ok(update);
            }
        }
    }

    /// Asynchronously waits for the next atomic batch that changes this item
    /// projection.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription closes or matching item history is
    /// malformed.
    pub async fn recv_async(&mut self) -> Result<ItemQueryUpdate<ItemQueryResult<Q>>, NodeError> {
        loop {
            let envelope = self.events.recv_async().await?;
            if let Some(update) = self.apply(&envelope)? {
                return Ok(update);
            }
        }
    }

    /// Waits up to `timeout` for the next atomic batch that changes this typed
    /// projection.
    ///
    /// Unrelated federation events do not restart the timeout. A timeout is
    /// reported as `Ok(None)` so synchronous application effects can check
    /// their shutdown signal without polling the underlying item state.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription closes or matching item history is
    /// malformed.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<ItemQueryUpdate<ItemQueryResult<Q>>>, NodeError> {
        let deadline = Instant::now().checked_add(timeout);
        loop {
            let remaining = deadline.map_or(timeout, |deadline| {
                deadline.saturating_duration_since(Instant::now())
            });
            let Some(envelope) = self.events.recv_timeout(remaining)? else {
                return Ok(None);
            };
            if let Some(update) = self.apply(&envelope)? {
                return Ok(Some(update));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(None);
            }
        }
    }

    /// Attempts to receive the next currently buffered relevant update.
    ///
    /// # Errors
    ///
    /// Returns an error if matching item history is malformed.
    pub fn try_recv(&mut self) -> Result<Option<ItemQueryUpdate<ItemQueryResult<Q>>>, NodeError> {
        while let Some(envelope) = self.events.try_recv() {
            if let Some(update) = self.apply(&envelope)? {
                return Ok(Some(update));
            }
        }
        Ok(None)
    }

    fn apply(
        &mut self,
        envelope: &EventEnvelope,
    ) -> Result<Option<ItemQueryUpdate<ItemQueryResult<Q>>>, NodeError> {
        let advances_cursor = match &envelope.event {
            NodeEvent::CommandCommitted { command, batch } => {
                self.source_node
                    .is_none_or(|source_node| envelope.origin.node_id == source_node)
                    && command.request.service_id == self.service_id
                    && self.scope_id.as_ref().is_none_or(|scope_id| {
                        command.request.scope_id.equivalent_to(scope_id)
                            || batch.changes.iter().any(|mutation| {
                                mutation.affects_scope::<Q::Item>(
                                    batch.scope_id.as_str(),
                                    scope_id.as_str(),
                                ) || mutation.scope_id.as_ref().is_some_and(|placed| {
                                    ScopeId::new(placed.clone()).equivalent_to(scope_id)
                                })
                            })
                    })
            }
            NodeEvent::CommandLifecycle(_) => false,
        };
        let service_scope = self
            .scope_id
            .as_ref()
            .map(|scope_id| (&self.service_id, scope_id));
        let _changed = apply_item_envelope(
            &mut self.projection,
            envelope,
            self.source_node,
            service_scope,
        )?;
        Ok(advances_cursor.then(|| ItemQueryUpdate {
            position: envelope.position,
            value: __snapshot_item_query(&self.query, &self.projection),
        }))
    }
}

/// One non-authoritative, best-effort event published by a node.
///
/// Live events are deliberately outside immutable history. A sequence gap tells
/// a consumer that its bounded subscription dropped intermediate state; the
/// consumer must recover authoritative state through a durable query or stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveEvent {
    pub source_node: NodeId,
    /// Monotonic sequence within `topic` for this source node.
    pub sequence: u64,
    pub topic: String,
    pub payload: Vec<u8>,
}

/// Result of publishing one live event to the node-local hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivePublishReport {
    /// Monotonic sequence within the published topic.
    pub sequence: u64,
    pub delivered: usize,
    pub dropped: usize,
}

/// Bounded subscription to non-authoritative live events.
pub struct LiveEventSubscription {
    live: flume::Receiver<LiveEvent>,
}

impl LiveEventSubscription {
    /// Receives the next live event.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the hub closes.
    pub fn recv(&mut self) -> Result<LiveEvent, NodeError> {
        self.live
            .recv()
            .map_err(|_| NodeError::SubscriptionDisconnected)
    }

    /// Attempts to receive a live event without blocking.
    #[must_use]
    pub fn try_recv(&mut self) -> Option<LiveEvent> {
        self.live.try_recv().ok()
    }

    /// Asynchronously receives the next live event.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the hub closes.
    pub async fn recv_async(&mut self) -> Result<LiveEvent, NodeError> {
        self.live
            .recv_async()
            .await
            .map_err(|_| NodeError::SubscriptionDisconnected)
    }
}

#[derive(Debug)]
struct LiveSubscriber {
    topics: HashSet<String>,
    sender: flume::Sender<LiveEvent>,
}

#[derive(Debug)]
struct LiveEventState {
    source_node: NodeId,
    sequences: HashMap<String, u64>,
    subscribers: Vec<LiveSubscriber>,
}

/// Transport-neutral fan-out for coalescible, non-authoritative live state.
///
/// Publishing never waits for a consumer. Each subscriber owns a bounded
/// queue, and an event is dropped only for subscribers whose queue is full.
/// An empty topic set subscribes to all topics.
#[derive(Debug, Clone)]
pub struct LiveEventHub {
    state: Arc<Mutex<LiveEventState>>,
}

impl LiveEventHub {
    /// Creates a live-event namespace for one stable node identity.
    #[must_use]
    pub fn new(source_node: NodeId) -> Self {
        Self {
            state: Arc::new(Mutex::new(LiveEventState {
                source_node,
                sequences: HashMap::new(),
                subscribers: Vec::new(),
            })),
        }
    }

    /// Returns the node that originates events from this hub.
    ///
    /// # Errors
    ///
    /// Returns an error if live-event state is poisoned.
    pub fn source_node(&self) -> Result<NodeId, NodeError> {
        self.state
            .lock()
            .map(|state| state.source_node)
            .map_err(|_| NodeError::LiveEventHubPoisoned)
    }

    /// Creates a bounded exact-topic subscription.
    ///
    /// Passing no topics subscribes to every event. Delivery begins after this
    /// call; live events have no replay contract.
    ///
    /// # Errors
    ///
    /// Returns an error if live-event state is poisoned.
    pub fn subscribe(
        &self,
        topics: impl IntoIterator<Item = String>,
        capacity: NonZeroUsize,
    ) -> Result<LiveEventSubscription, NodeError> {
        let (sender, live) = flume::bounded(capacity.get());
        self.state
            .lock()
            .map_err(|_| NodeError::LiveEventHubPoisoned)?
            .subscribers
            .push(LiveSubscriber {
                topics: topics.into_iter().collect(),
                sender,
            });
        Ok(LiveEventSubscription { live })
    }

    /// Publishes without waiting for any subscriber.
    ///
    /// # Errors
    ///
    /// Returns an error if live-event state is poisoned or its sequence space
    /// is exhausted.
    pub fn publish(
        &self,
        topic: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<LivePublishReport, NodeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| NodeError::LiveEventHubPoisoned)?;
        let topic = topic.into();
        let sequence = state
            .sequences
            .get(&topic)
            .copied()
            .unwrap_or_default()
            .checked_add(1)
            .ok_or(NodeError::LiveEventSequenceExhausted)?;
        state.sequences.insert(topic.clone(), sequence);
        let event = LiveEvent {
            source_node: state.source_node,
            sequence,
            topic,
            payload,
        };
        let mut delivered = 0usize;
        let mut dropped = 0usize;
        state.subscribers.retain(|subscriber| {
            if !subscriber.topics.is_empty() && !subscriber.topics.contains(&event.topic) {
                return true;
            }
            match subscriber.sender.try_send(event.clone()) {
                Ok(()) => {
                    delivered = delivered.saturating_add(1);
                    true
                }
                Err(flume::TrySendError::Full(_)) => {
                    dropped = dropped.saturating_add(1);
                    true
                }
                Err(flume::TrySendError::Disconnected(_)) => false,
            }
        });
        drop(state);
        Ok(LivePublishReport {
            sequence,
            delivered,
            dropped,
        })
    }
}
