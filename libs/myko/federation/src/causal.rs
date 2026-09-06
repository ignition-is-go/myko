use super::*;

type OriginKey = (u128, u64);
type ReadyKey = (u64, u128, u64);

#[allow(clippy::redundant_pub_crate)]
pub(super) fn scoped_author_parents(
    history: &[EventEnvelope],
    author: NodeId,
    batch: &ChangeBatch,
) -> Vec<EventId> {
    scoped_author_parents_before(history, author, batch, None)
}

fn scoped_author_parents_before(
    history: &[EventEnvelope],
    author: NodeId,
    batch: &ChangeBatch,
    before: Option<LogPosition>,
) -> Vec<EventId> {
    let scopes = write_scopes(batch).collect::<HashSet<_>>();
    let mut latest = HashMap::<&str, EventId>::new();
    for event in history {
        let NodeEvent::CommandCommitted { batch: prior, .. } = &event.event else {
            continue;
        };
        if event.origin.node_id != author
            || before.is_some_and(|before| event.origin.sequence >= before)
            || prior.service_id != batch.service_id
        {
            continue;
        }
        for scope in write_scopes(prior).filter(|scope| scopes.contains(scope)) {
            latest
                .entry(scope)
                .and_modify(|parent| {
                    if event.origin.sequence > parent.sequence {
                        *parent = event.origin;
                    }
                })
                .or_insert(event.origin);
        }
    }
    let mut parents = latest.into_values().collect::<Vec<_>>();
    parents.sort_by_key(|parent| origin_key(*parent));
    parents.dedup();
    parents
}

fn write_scopes(batch: &ChangeBatch) -> impl Iterator<Item = &str> {
    std::iter::once(batch.scope_id.as_str()).chain(
        batch
            .changes
            .iter()
            .filter_map(|mutation| mutation.scope_id.as_deref()),
    )
}

#[derive(Debug, Default)]
#[allow(clippy::redundant_pub_crate)]
pub(super) struct CausalIndex {
    entries: BTreeMap<OriginKey, CausalEntry>,
}

#[derive(Debug, Clone)]
struct CausalEntry {
    origin: EventId,
    position: LogPosition,
    parents: Vec<OriginKey>,
    service_id: Option<ServiceId>,
    write_scopes: std::collections::BTreeSet<String>,
}

#[derive(Debug)]
#[allow(clippy::redundant_pub_crate)]
pub(super) struct CausalAppend(CausalAppendKind);

#[derive(Debug)]
enum CausalAppendKind {
    Duplicate,
    Insert { entry: CausalEntry },
}

impl CausalIndex {
    /// Validates and stages one append without changing the index.
    pub(super) fn prepare(&self, event: &EventEnvelope) -> Result<CausalAppend, NodeError> {
        let key = origin_key(event.origin);
        let parents = unique_parent_keys(event);
        if let Some(existing) = self.entries.get(&key) {
            return if existing.parents == parents {
                Ok(CausalAppend(CausalAppendKind::Duplicate))
            } else {
                Err(NodeError::EventConflict(event.origin))
            };
        }
        let entry = CausalEntry {
            origin: event.origin,
            position: event.position,
            parents,
            service_id: committed_batch(event).map(|batch| batch.service_id.clone()),
            write_scopes: committed_batch(event)
                .map_or_else(std::collections::BTreeSet::new, |batch| {
                    write_scopes(batch).map(str::to_owned).collect()
                }),
        };
        self.reject_combined_cycle(key, &entry)?;
        Ok(CausalAppend(CausalAppendKind::Insert { entry }))
    }

    /// Commits metadata previously returned by [`Self::prepare`].
    pub(super) fn apply(&mut self, append: CausalAppend) {
        let CausalAppend(CausalAppendKind::Insert { entry }) = append else {
            return;
        };
        let key = origin_key(entry.origin);
        self.entries.insert(key, entry);
    }

    pub(super) fn ordered_origins(&self, through: Option<LogPosition>) -> Vec<EventId> {
        self.replay_at(through)
    }

    fn reject_combined_cycle(&self, key: OriginKey, entry: &CausalEntry) -> Result<(), NodeError> {
        let mut parents = self.parent_graph(None);
        parents.insert(key, entry.parents.iter().copied().collect());
        add_author_edges_to_graph(
            &mut parents,
            self.entries.values().chain(std::iter::once(entry)),
        );
        if graph_has_cycle(&parents) {
            return Err(NodeError::CorruptHistory(
                "causal history contains a dependency cycle".to_owned(),
            ));
        }
        Ok(())
    }

    fn parent_graph(
        &self,
        through: Option<LogPosition>,
    ) -> BTreeMap<OriginKey, std::collections::BTreeSet<OriginKey>> {
        let eligible = self
            .entries
            .values()
            .filter(|entry| through.is_none_or(|cut| entry.position <= cut))
            .collect::<Vec<_>>();
        let mut parents = eligible
            .iter()
            .map(|entry| {
                (
                    origin_key(entry.origin),
                    entry.parents.iter().copied().collect(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        add_author_edges_to_graph(&mut parents, eligible.into_iter());
        parents
    }

    fn replay_at(&self, through: Option<LogPosition>) -> Vec<EventId> {
        let parents = self.parent_graph(through);
        let mut children = BTreeMap::<OriginKey, std::collections::BTreeSet<OriginKey>>::new();
        let mut unresolved = parents.clone();
        let mut heights = BTreeMap::<OriginKey, u64>::new();
        for (child, dependencies) in &parents {
            for parent in dependencies {
                children.entry(*parent).or_default().insert(*child);
            }
        }
        let mut ready = std::collections::BTreeSet::<ReadyKey>::new();
        for key in parents.keys().filter(|key| {
            unresolved
                .get(key)
                .is_some_and(std::collections::BTreeSet::is_empty)
        }) {
            heights.insert(*key, 1);
            ready.insert((1, key.0, key.1));
        }
        let mut ordered = Vec::new();
        while let Some(next) = ready.pop_first() {
            let key = (next.1, next.2);
            ordered.push(EventId::new(
                NodeId::from_uuid(Uuid::from_u128(key.0)),
                LogPosition::new(key.1),
            ));
            for child in children.get(&key).into_iter().flatten() {
                let dependencies = unresolved.entry(*child).or_default();
                dependencies.remove(&key);
                if dependencies.is_empty() {
                    let height = parents
                        .get(child)
                        .into_iter()
                        .flatten()
                        .filter_map(|parent| heights.get(parent))
                        .copied()
                        .max()
                        .unwrap_or(0)
                        .saturating_add(1);
                    heights.insert(*child, height);
                    ready.insert((height, child.0, child.1));
                }
            }
        }
        ordered
    }
}

const fn committed_batch(event: &EventEnvelope) -> Option<&ChangeBatch> {
    match &event.event {
        NodeEvent::CommandCommitted { batch, .. } => Some(batch),
        NodeEvent::CommandLifecycle(_) | NodeEvent::FrameworkControl(_) => None,
    }
}

fn add_author_edges_to_graph<'a>(
    parents: &mut BTreeMap<OriginKey, std::collections::BTreeSet<OriginKey>>,
    entries: impl Iterator<Item = &'a CausalEntry>,
) {
    let mut streams = BTreeMap::<(u128, String, String), Vec<&CausalEntry>>::new();
    for entry in entries {
        let Some(service_id) = &entry.service_id else {
            continue;
        };
        for scope in &entry.write_scopes {
            streams
                .entry((
                    entry.origin.node_id.as_uuid().as_u128(),
                    service_id.as_str().to_owned(),
                    scope.clone(),
                ))
                .or_default()
                .push(entry);
        }
    }
    for stream in streams.values_mut() {
        stream.sort_by_key(|entry| entry.origin.sequence);
        for pair in stream.windows(2) {
            if let [parent, child] = pair {
                parents
                    .entry(origin_key(child.origin))
                    .or_default()
                    .insert(origin_key(parent.origin));
            }
        }
    }
}

fn graph_has_cycle(parents: &BTreeMap<OriginKey, std::collections::BTreeSet<OriginKey>>) -> bool {
    let mut colors = BTreeMap::<OriginKey, u8>::new();
    for root in parents.keys().copied() {
        if colors.get(&root) == Some(&2) {
            continue;
        }
        let mut stack = vec![(root, false)];
        while let Some((key, exiting)) = stack.pop() {
            if exiting {
                colors.insert(key, 2);
                continue;
            }
            match colors.get(&key) {
                Some(1) => return true,
                Some(2) => continue,
                Some(_) | None => {}
            }
            if !parents.contains_key(&key) {
                colors.insert(key, 2);
                continue;
            }
            colors.insert(key, 1);
            stack.push((key, true));
            for parent in parents.get(&key).into_iter().flatten().rev() {
                stack.push((*parent, false));
            }
        }
    }
    false
}

/// Returns the causally complete portion of history in deterministic replay order.
///
/// Observer-local positions do not participate in identity or ordering. Events
/// blocked by an absent parent, including their descendants, stay in the input
/// history but are omitted from the returned replay.
///
/// # Errors
/// Rejects conflicting immutable event identities and cyclic dependencies.
pub fn causal_replay(history: &[EventEnvelope]) -> Result<Vec<&EventEnvelope>, NodeError> {
    let mut events = BTreeMap::<OriginKey, &EventEnvelope>::new();
    for envelope in history {
        let key = origin_key(envelope.origin);
        if let Some(existing) = events.get_mut(&key) {
            if existing.origin != envelope.origin
                || existing.recorded_at != envelope.recorded_at
                || existing.event != envelope.event
            {
                return Err(NodeError::EventConflict(envelope.origin));
            }
            if envelope.position < existing.position {
                *existing = envelope;
            }
        } else {
            events.insert(key, envelope);
        }
    }

    let mut index = CausalIndex::default();
    for envelope in events.values() {
        let append = index.prepare(envelope)?;
        index.apply(append);
    }
    index
        .ordered_origins(None)
        .into_iter()
        .map(|origin| {
            events.get(&origin_key(origin)).copied().ok_or_else(|| {
                NodeError::CorruptHistory("causal replay lost an indexed event".to_owned())
            })
        })
        .collect()
}

pub fn causal_parents(envelope: &EventEnvelope) -> Vec<EventId> {
    match &envelope.event {
        NodeEvent::FrameworkControl(control) => control.causal_dependencies(),
        NodeEvent::CommandCommitted { batch, .. } => batch.causal_parents.clone(),
        NodeEvent::CommandLifecycle(command) => match &command.state {
            CommandState::CommittedLocally { position, .. }
            | CommandState::Replicating { position, .. }
            | CommandState::ReplicationDelayed { position, .. }
            | CommandState::Replicated { position, .. }
            | CommandState::Reconciled { position, .. } => vec![*position],
            CommandState::Submitted
            | CommandState::Executing
            | CommandState::Retrying { .. }
            | CommandState::AuthorizationPrepared { .. }
            | CommandState::AuthorizationPending { .. }
            | CommandState::Rejected { .. }
            | CommandState::Cancelled { .. } => Vec::new(),
        },
    }
}

pub fn effective_causal_parents(
    history: &[EventEnvelope],
    envelope: &EventEnvelope,
) -> Vec<EventId> {
    let mut parents = causal_parents(envelope);
    if let NodeEvent::CommandCommitted { batch, .. } = &envelope.event {
        parents.extend(scoped_author_parents_before(
            history,
            envelope.origin.node_id,
            batch,
            Some(envelope.origin.sequence),
        ));
    }
    parents.sort_by_key(|parent| origin_key(*parent));
    parents.dedup();
    parents
}

fn unique_parent_keys(envelope: &EventEnvelope) -> Vec<OriginKey> {
    causal_parents(envelope)
        .into_iter()
        .map(origin_key)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

const fn origin_key(origin: EventId) -> OriginKey {
    (origin.node_id.as_uuid().as_u128(), origin.sequence.get())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn node(value: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(value))
    }

    fn origin(node: u128, sequence: u64) -> EventId {
        EventId::new(self::node(node), LogPosition::new(sequence))
    }

    fn request(id: CommandId) -> CommandRequest {
        let principal = PrincipalId::new("node:test");
        CommandRequest {
            id,
            service_id: ServiceId::new("test"),
            scope_id: ScopeId::new("scope:test"),
            principal_id: principal.clone(),
            authority: AuthorityPresentation::direct_node(principal),
            resource_claims: Vec::new(),
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: "test".to_owned(),
            payload: Vec::new(),
        }
    }

    fn committed(id: EventId, parents: Vec<EventId>) -> EventEnvelope {
        let request = request(CommandId::new());
        let command = CommandSnapshot {
            request: request.clone(),
            state: CommandState::CommittedLocally {
                batch_id: BatchId::new(),
                position: id,
            },
            result: Some(Vec::new()),
            updated_at: id,
        };
        EventEnvelope {
            position: id.sequence,
            origin: id,
            recorded_at: Utc::now(),
            event: NodeEvent::CommandCommitted {
                command,
                batch: ChangeBatch {
                    id: BatchId::new(),
                    command_id: request.id,
                    service_id: request.service_id,
                    scope_id: request.scope_id,
                    causal_parents: parents,
                    changes: Vec::new(),
                },
            },
        }
    }

    fn append(index: &mut CausalIndex, event: &EventEnvelope) {
        let staged = index.prepare(event).unwrap();
        index.apply(staged);
    }

    fn in_scope(mut event: EventEnvelope, scope: &str) -> EventEnvelope {
        if let NodeEvent::CommandCommitted { batch, command } = &mut event.event {
            batch.scope_id = ScopeId::new(scope);
            command.request.scope_id = batch.scope_id.clone();
        }
        event
    }

    #[test]
    fn same_author_sparse_overlapping_writes_follow_origin_order() {
        let prerequisite = committed(origin(2, 1), Vec::new());
        let earlier = committed(origin(1, 10), vec![prerequisite.origin]);
        let later = committed(origin(1, 90), Vec::new());
        let mut index = CausalIndex::default();
        append(&mut index, &later);
        append(&mut index, &earlier);
        append(&mut index, &prerequisite);

        assert_eq!(
            index.ordered_origins(None),
            vec![prerequisite.origin, earlier.origin, later.origin]
        );
    }

    #[test]
    fn unrelated_write_scope_is_not_blocked_by_author_order() {
        let blocked = in_scope(
            committed(origin(1, 10), vec![origin(9, 99)]),
            "scope:blocked",
        );
        let independent = in_scope(committed(origin(1, 20), Vec::new()), "scope:independent");
        let mut index = CausalIndex::default();
        append(&mut index, &blocked);
        append(&mut index, &independent);

        assert_eq!(index.ordered_origins(None), vec![independent.origin]);
    }

    #[test]
    fn late_author_predecessor_does_not_rewrite_an_earlier_cut() {
        let mut later = committed(origin(1, 20), Vec::new());
        later.position = LogPosition::new(1);
        let mut earlier = committed(origin(1, 10), Vec::new());
        earlier.position = LogPosition::new(2);
        let mut index = CausalIndex::default();
        append(&mut index, &later);
        assert_eq!(
            index.ordered_origins(Some(LogPosition::new(1))),
            vec![later.origin]
        );

        append(&mut index, &earlier);
        assert_eq!(
            index.ordered_origins(None),
            vec![earlier.origin, later.origin]
        );
        assert_eq!(
            index.ordered_origins(Some(LogPosition::new(1))),
            vec![later.origin]
        );
    }

    #[test]
    fn inferred_author_edge_and_explicit_dependency_cannot_close_a_cycle() {
        let earlier = committed(origin(1, 10), vec![origin(1, 20)]);
        let later = committed(origin(1, 20), Vec::new());
        let mut index = CausalIndex::default();
        append(&mut index, &earlier);

        assert!(matches!(
            index.prepare(&later),
            Err(NodeError::CorruptHistory(_))
        ));
        assert!(index.ordered_origins(None).is_empty());
    }

    #[test]
    fn scoped_author_parents_use_origin_order_and_ignore_unrelated_writes() {
        let older = committed(origin(1, 10), Vec::new());
        let latest = committed(origin(1, 30), Vec::new());
        let remote = committed(origin(2, 90), Vec::new());
        let mut other_service = committed(origin(1, 40), Vec::new());
        if let NodeEvent::CommandCommitted { batch, command } = &mut other_service.event {
            batch.service_id = ServiceId::new("other-service");
            command.request.service_id = batch.service_id.clone();
        }
        let mut other_scope = committed(origin(1, 50), Vec::new());
        if let NodeEvent::CommandCommitted { batch, command } = &mut other_scope.event {
            batch.scope_id = ScopeId::new("scope:unrelated");
            command.request.scope_id = batch.scope_id.clone();
            command.request.resource_claims.push(ResourceClaim {
                selection: ScopeSelection::Exact(ScopeId::new("scope:test")),
                kind: ResourceClaimKind::Referenced,
                source_node: None,
                service_id: Some(batch.service_id.clone()),
                item_type: None,
                item_id: None,
                required_permissions: vec![FederationPermission::ReadState],
                required_operations: Vec::new(),
                required_capabilities: Vec::new(),
            });
        }
        let target = committed(origin(1, 60), Vec::new());
        let batch = match &target.event {
            NodeEvent::CommandCommitted { batch, .. } => Some(batch),
            NodeEvent::CommandLifecycle(_) | NodeEvent::FrameworkControl(_) => None,
        }
        .unwrap();
        let history = [latest, other_scope, other_service, remote, older];
        assert_eq!(
            scoped_author_parents(&history, node(1), batch),
            vec![origin(1, 30)]
        );
    }

    #[test]
    fn replay_is_causal_deduplicated_and_uses_sparse_origin_ids() {
        let first = committed(origin(2, 7), Vec::new());
        let second = committed(origin(1, 41), vec![first.origin]);
        let independent = committed(origin(1, 3), Vec::new());
        let mut duplicate = first.clone();
        duplicate.position = LogPosition::new(900);
        let events = [first, second, independent];
        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];

        for permutation in permutations {
            let mut index = CausalIndex::default();
            for event_index in permutation {
                append(&mut index, events.get(event_index).unwrap());
            }
            assert_eq!(
                index.ordered_origins(None),
                vec![origin(1, 3), origin(2, 7), origin(1, 41)]
            );

            let history = [
                events.get(permutation[0]).unwrap().clone(),
                duplicate.clone(),
                events.get(permutation[1]).unwrap().clone(),
                events.get(permutation[2]).unwrap().clone(),
            ];
            let replay = causal_replay(&history).unwrap();
            let origins = replay.iter().map(|event| event.origin).collect::<Vec<_>>();
            assert_eq!(origins, vec![origin(1, 3), origin(2, 7), origin(1, 41)]);
        }
    }

    #[test]
    fn missing_parents_quarantine_their_transitive_descendants() {
        let missing = origin(9, 100);
        let blocked = committed(origin(1, 5), vec![missing]);
        let descendant = committed(origin(2, 8), vec![blocked.origin]);
        let complete = committed(origin(3, 13), Vec::new());

        let history = [descendant, complete.clone(), blocked];
        let replay = causal_replay(&history).unwrap();
        assert_eq!(replay, vec![&complete]);
    }

    #[test]
    fn committed_lifecycle_states_wait_for_their_batch_and_its_ancestors() -> Result<(), String> {
        let parent = committed(origin(3, 10), Vec::new());
        let commit = committed(origin(2, 20), vec![parent.origin]);
        let NodeEvent::CommandCommitted { command, batch } = &commit.event else {
            return Err("fixture did not produce a committed batch".to_owned());
        };
        let states = [
            CommandState::CommittedLocally {
                batch_id: batch.id,
                position: commit.origin,
            },
            CommandState::Replicating {
                batch_id: batch.id,
                position: commit.origin,
            },
            CommandState::ReplicationDelayed {
                batch_id: batch.id,
                position: commit.origin,
                reason: "offline".to_owned(),
            },
            CommandState::Replicated {
                batch_id: batch.id,
                position: commit.origin,
                acknowledged_replicas: 1,
                required_replicas: 1,
            },
            CommandState::Reconciled {
                batch_id: batch.id,
                position: commit.origin,
                outcome: Reconciliation::FullyVisible,
            },
        ];
        for state in states {
            let mut snapshot = command.clone();
            snapshot.state = state;
            snapshot.updated_at = origin(1, 30);
            let lifecycle = EventEnvelope {
                position: LogPosition::new(1),
                origin: snapshot.updated_at,
                recorded_at: Utc::now(),
                event: NodeEvent::CommandLifecycle(snapshot),
            };
            let mut index = CausalIndex::default();
            append(&mut index, &lifecycle);
            if !index.ordered_origins(None).is_empty() {
                return Err("lifecycle became ready before its commit".to_owned());
            }
            append(&mut index, &commit);
            if !index.ordered_origins(None).is_empty() {
                return Err("commit and lifecycle became ready before their ancestor".to_owned());
            }
            append(&mut index, &parent);
            if index.ordered_origins(None) != vec![parent.origin, commit.origin, lifecycle.origin] {
                return Err(
                    "late ancestor did not release the committed lifecycle in order".to_owned(),
                );
            }
            if index.ordered_origins(Some(LogPosition::new(19))) != vec![parent.origin] {
                return Err("earlier cut exposed the commit or lifecycle".to_owned());
            }
        }
        Ok(())
    }

    #[test]
    fn cycles_are_rejected_even_when_a_vertex_also_has_a_missing_parent() {
        let left_id = origin(1, 2);
        let right_id = origin(2, 4);
        let left = committed(left_id, vec![right_id, origin(9, 99)]);
        let right = committed(right_id, vec![left_id]);

        let history = [left, right];
        assert!(matches!(
            causal_replay(&history),
            Err(NodeError::CorruptHistory(_))
        ));
    }

    #[test]
    fn conflicting_content_for_one_origin_is_rejected() {
        let event = committed(origin(1, 7), Vec::new());
        let mut conflict = event.clone();
        if let NodeEvent::CommandCommitted { command, .. } = &mut conflict.event {
            command.result = Some(b"different".to_vec());
        }

        let history = [event.clone(), conflict];
        assert_eq!(
            causal_replay(&history),
            Err(NodeError::EventConflict(event.origin))
        );
    }

    #[test]
    fn late_parent_releases_child_at_the_parent_cutoff() {
        let parent_id = origin(1, 2);
        let mut child = committed(origin(2, 9), vec![parent_id]);
        child.position = LogPosition::new(1);
        let mut parent = committed(parent_id, Vec::new());
        parent.position = LogPosition::new(5);
        let mut index = CausalIndex::default();

        append(&mut index, &child);
        assert!(index.ordered_origins(None).is_empty());
        append(&mut index, &parent);

        assert!(index.ordered_origins(Some(LogPosition::new(4))).is_empty());
        assert_eq!(
            index.ordered_origins(Some(LogPosition::new(5))),
            vec![parent_id, child.origin]
        );
    }

    #[test]
    fn unused_staged_append_does_not_change_the_index() {
        let event = committed(origin(1, 20), Vec::new());
        let index = CausalIndex::default();

        let _staged = index.prepare(&event).unwrap();

        assert!(index.ordered_origins(None).is_empty());
    }

    #[test]
    fn filling_a_missing_parent_rejects_a_closing_cycle() {
        let first_id = origin(1, 8);
        let second_id = origin(2, 70);
        let first = committed(first_id, vec![second_id]);
        let second = committed(second_id, vec![first_id]);
        let mut index = CausalIndex::default();
        append(&mut index, &first);

        assert!(matches!(
            index.prepare(&second),
            Err(NodeError::CorruptHistory(_))
        ));
        assert!(index.ordered_origins(None).is_empty());
    }

    #[test]
    fn one_late_root_releases_a_pending_diamond() {
        let root_id = origin(1, 1);
        let branch_id = origin(2, 4);
        let branch = committed(branch_id, vec![root_id]);
        let join = committed(origin(3, 9), vec![root_id, branch_id]);
        let root = committed(root_id, Vec::new());
        let mut index = CausalIndex::default();
        append(&mut index, &branch);
        append(&mut index, &join);

        append(&mut index, &root);

        assert_eq!(
            index.ordered_origins(None),
            vec![root_id, branch_id, join.origin]
        );
    }
}
