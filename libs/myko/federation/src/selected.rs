use std::collections::HashSet;

use thiserror::Error;

use super::{
    AuthorityPresentation, AuthorizationPhase, EventEnvelope, EventId, LogPosition,
    MutationOperation, MykoItem, Node, NodeError, NodeEvent, NodeId, PrincipalId, ScopeId,
    ScopeSelection, ScopeTopology, command_from_event,
};

/// Exact retained history selected at one frozen local recording cut.
///
/// This is local inclusion evidence only. Scope-read authority remains an
/// external precondition; the manifest does not prove replica completeness,
/// currentness, or custody.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedHistoryManifest {
    selection: ScopeSelection,
    through: Option<LogPosition>,
    events: Vec<EventEnvelope>,
}

/// Why a frozen selected history cannot form a closed manifest.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SelectedHistoryManifestError {
    #[error("selected history contains unresolved event {0:?}")]
    PendingHistory(EventId),
    #[error("event {event:?} also affects scope {scope} outside the selection")]
    OutsideSelection { event: EventId, scope: ScopeId },
    #[error(
        "control event {event:?} describes history outside the requested selection: {selection:?}"
    )]
    ControlOutsideSelection {
        event: EventId,
        selection: ScopeSelection,
    },
    #[error("event {event:?} depends on unselected event {dependency:?}")]
    MissingDependency { event: EventId, dependency: EventId },
}

impl SelectedHistoryManifest {
    /// Scope selection used to construct this manifest.
    #[must_use]
    pub const fn selection(&self) -> &ScopeSelection {
        &self.selection
    }

    /// Local recording cut of the node that built this manifest.
    #[must_use]
    pub const fn through(&self) -> Option<LogPosition> {
        self.through
    }

    /// Complete immutable event bodies selected at the frozen cut.
    #[must_use]
    pub fn events(&self) -> &[EventEnvelope] {
        &self.events
    }
}

/// Authorization is current; history is fixed at the caller's consumed cursor.
pub struct SelectedQueryRead<'a> {
    pub authenticated_executor: PrincipalId,
    pub presentation: AuthorityPresentation,
    pub source_node: NodeId,
    pub requested: &'a ScopeSelection,
    pub phase: AuthorizationPhase,
    pub through: Option<LogPosition>,
}

/// Immutable local history at one recording cut, including unresolved history.
///
/// This establishes which locally accepted events can be projected at the cut.
/// It does not establish remote coverage, custody, or authorization to serve data.
pub struct SelectedHistorySnapshot {
    pub(super) through: Option<LogPosition>,
    pub(super) ready: Vec<EventEnvelope>,
    pub(super) topology: ScopeTopology,
    pending: Vec<EventEnvelope>,
}

impl SelectedHistorySnapshot {
    /// Capture the current local cut and read its history once.
    ///
    /// # Errors
    ///
    /// Returns an error if the cut, history, or scope topology cannot be read.
    pub fn current(node: &Node) -> Result<Self, NodeError> {
        Self::at(node, node.local_history_cut()?)
    }

    /// Read dependency-complete history and unresolved events at the same cut.
    ///
    /// # Errors
    ///
    /// Returns an error if history or scope topology cannot be read.
    pub fn at(node: &Node, through: Option<LogPosition>) -> Result<Self, NodeError> {
        let available = node.local_history_cut()?;
        if let Some(requested) = through
            && available.is_none_or(|available| requested > available)
        {
            return Err(NodeError::HistoryCutUnavailable {
                requested,
                available,
            });
        }
        let ready = through
            .map(|cut| node.causal_events_through(cut))
            .transpose()?
            .unwrap_or_default();
        let topology = ScopeTopology::from_events(&ready)?;
        let ready_origins = ready
            .iter()
            .map(|event| event.origin)
            .collect::<HashSet<_>>();
        let pending = node
            .events_after(None)?
            .into_iter()
            .filter(|event| {
                through.is_some_and(|cut| event.position <= cut)
                    && !ready_origins.contains(&event.origin)
            })
            .collect();
        Ok(Self {
            through,
            ready,
            topology,
            pending,
        })
    }

    /// Local recording cut shared by the events and pending-history assessment.
    #[must_use]
    pub const fn through(&self) -> Option<LogPosition> {
        self.through
    }

    /// Dependency-complete events in their causal replay order at this cut.
    #[must_use]
    pub fn ready(&self) -> &[EventEnvelope] {
        &self.ready
    }

    /// Scope topology derived only from the dependency-complete events.
    #[must_use]
    pub const fn topology(&self) -> &ScopeTopology {
        &self.topology
    }

    /// Select all retained history intersecting one scope at this frozen cut.
    ///
    /// Atomic events that also affect an unselected scope and dependencies
    /// outside the selected event set are rejected instead of being omitted.
    /// Unknown or empty scopes yield an empty manifest; that does not prove
    /// that the scope exists or that remote history is complete.
    ///
    /// # Errors
    ///
    /// Returns an error when relevant accepted history is unresolved, an
    /// atomic event crosses the selection boundary, or a selected event's
    /// effective causal dependency is outside the manifest, including an
    /// implicit predecessor in the same scoped author stream.
    pub fn retained_manifest(
        &self,
        selection: &ScopeSelection,
    ) -> Result<SelectedHistoryManifest, SelectedHistoryManifestError> {
        if let Some(event) = self.pending.iter().find(|event| {
            event_intersects_selection(event, selection, &self.topology)
                || matches!(selection, ScopeSelection::Subtree(_))
        }) {
            return Err(SelectedHistoryManifestError::PendingHistory(event.origin));
        }

        let mut events = Vec::new();
        for event in &self.ready {
            if let NodeEvent::FrameworkControl(control) = &event.event {
                if !selections_overlap(selection, &control.selection(), &self.topology) {
                    continue;
                }
                if !selection.covers_in(&control.selection(), &self.topology) {
                    return Err(SelectedHistoryManifestError::ControlOutsideSelection {
                        event: event.origin,
                        selection: control.selection(),
                    });
                }
                events.push(event.clone());
                continue;
            }
            let affected = retained_scope_ids(&event.event);
            let selected = affected
                .iter()
                .filter(|scope| selection.contains_scope(scope, &self.topology))
                .count();
            if selected == 0 {
                continue;
            }
            if let Some(scope) = affected
                .iter()
                .find(|scope| !selection.contains_scope(scope, &self.topology))
            {
                return Err(SelectedHistoryManifestError::OutsideSelection {
                    event: event.origin,
                    scope: scope.clone(),
                });
            }
            events.push(event.clone());
        }

        let selected_origins = events
            .iter()
            .map(|event| event.origin)
            .collect::<HashSet<_>>();
        for event in &events {
            if let Some(parent) = crate::causal::effective_causal_parents(&self.ready, event)
                .iter()
                .find(|parent| !selected_origins.contains(parent))
            {
                return Err(SelectedHistoryManifestError::MissingDependency {
                    event: event.origin,
                    dependency: *parent,
                });
            }
        }
        Ok(SelectedHistoryManifest {
            selection: selection.clone(),
            through: self.through,
            events,
        })
    }

    pub(super) fn observed_source(&self, source: NodeId) -> bool {
        self.ready
            .iter()
            .chain(&self.pending)
            .any(|event| event.origin.node_id == source)
    }

    pub(super) fn has_pending_for<T: MykoItem>(
        &self,
        source: NodeId,
        authorized: &[ScopeSelection],
    ) -> bool {
        self.has_pending_matching::<T>(Some(source), authorized)
    }

    /// Whether unresolved accepted history can affect this local selection.
    ///
    /// All origins participate. An exact scope ignores unrelated pending items;
    /// a subtree conservatively includes pending topology whose parent is unknown.
    #[must_use]
    pub fn has_pending_in<T: MykoItem>(&self, selection: &ScopeSelection) -> bool {
        self.has_pending_matching::<T>(None, std::slice::from_ref(selection))
    }

    fn has_pending_matching<T: MykoItem>(
        &self,
        source: Option<NodeId>,
        selections: &[ScopeSelection],
    ) -> bool {
        self.pending.iter().any(|event| {
            let affects_items = source.is_none_or(|source| event.origin.node_id == source)
                && command_from_event(&event.event)
                    .is_some_and(|command| command.request.service_id == T::SERVICE_ID);
            let affects_topology = matches!(&event.event,
                NodeEvent::CommandCommitted { batch, .. }
                if batch.changes.iter().any(|mutation| mutation.roots_scope));
            (affects_items || affects_topology)
                && selections.iter().any(|selection| {
                    // An absent ancestor edge cannot prove a pending scope is
                    // outside a subtree. Exact scopes do not need that inference.
                    matches!(selection, ScopeSelection::Subtree(_))
                        || event
                            .event
                            .affected_scope_ids()
                            .iter()
                            .any(|scope| selection.contains_scope(scope, &self.topology))
                })
        })
    }
}

fn event_intersects_selection(
    event: &EventEnvelope,
    selection: &ScopeSelection,
    topology: &ScopeTopology,
) -> bool {
    if let NodeEvent::FrameworkControl(control) = &event.event {
        return selections_overlap(selection, &control.selection(), topology);
    }
    retained_scope_ids(&event.event)
        .iter()
        .any(|scope| selection.contains_scope(scope, topology))
}

fn selections_overlap(
    left: &ScopeSelection,
    right: &ScopeSelection,
    topology: &ScopeTopology,
) -> bool {
    left.contains_scope(right.root(), topology) || right.contains_scope(left.root(), topology)
}

fn retained_scope_ids(event: &NodeEvent) -> Vec<ScopeId> {
    let mut scopes = HashSet::from([event.scope_id().clone()]);
    if let NodeEvent::CommandCommitted { batch, .. } = event {
        for mutation in &batch.changes {
            scopes.insert(ScopeId::new(
                mutation
                    .scope_id
                    .as_deref()
                    .unwrap_or(batch.scope_id.as_str()),
            ));
            if mutation.operation == MutationOperation::Set && mutation.roots_scope {
                scopes.insert(ScopeId::for_parts(
                    &mutation.service_id,
                    &mutation.item_type,
                    &mutation.item_id,
                ));
                if let Some(parent) = &mutation.belongs_to {
                    scopes.insert(ScopeId::for_entity(parent));
                }
            }
        }
    }
    scopes.into_iter().collect()
}
