use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use myko_federation::{
    AccessAttempt, AuthorizationBinding, AuthorizationDecision, AuthorizationPhase,
    CertifiedControlChain, CertifiedControlContext, CommandId, ControlAnchor, ControlTransition,
    EventEnvelope, EventId, LogPosition, MykoService as _, Node, NodeEvent, ScopeId, ScopeTopology,
    ServiceId, causal_replay,
    control_quorum::{ControlEpochId, ControlHead, ControlValue, ControllerId},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{
    AuthorityRealmKey, AuthorityService, authority_realm_scope, decision_records::DecisionRecord,
    decision_records::decision_records, evaluator::evaluate_seeded,
};

use super::{AuthorityRotation, CertifiedAuthorityFact, project_facts};

const SELECTION_DOMAIN: &[u8] = b"myko/certified-authority-selection/v1\0";
const DECISION_DOMAIN: &[u8] = b"myko/certified-authority-decision/v1\0";

#[derive(Debug, Clone)]
pub struct AuthorityAnchor {
    realm: AuthorityRealmKey,
    control: ControlAnchor,
}

impl AuthorityAnchor {
    /// Build the static trust anchor used to verify historical authority heads.
    ///
    /// # Errors
    /// Returns an error when the controller set is empty, duplicated, malformed,
    /// or rejected by the control quorum verifier.
    pub fn new(
        realm: AuthorityRealmKey,
        initial_epoch: ControlEpochId,
        genesis: ControlHead,
        controllers: Vec<ControllerId>,
    ) -> Result<Self, String> {
        let control = ControlAnchor::new(
            authority_realm_scope(&realm),
            initial_epoch,
            genesis,
            controllers,
        )?;
        Ok(Self { realm, control })
    }

    #[must_use]
    pub const fn realm_id(&self) -> &AuthorityRealmKey {
        &self.realm
    }

    #[must_use]
    pub const fn genesis(&self) -> ControlHead {
        self.control.genesis()
    }

    fn control_anchor(&self) -> ControlAnchor {
        self.control.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritySelection {
    operation: CommandId,
    realm: AuthorityRealmKey,
    records: Vec<SelectedAuthorityRecord>,
}

impl AuthoritySelection {
    /// Select existing `AuthorityService` command events for one operation.
    ///
    /// # Errors
    /// Returns an error for empty selections, non-authority events, mixed realms,
    /// repeated event identities, conflicting command requests, duplicate
    /// commits, or invalid command/batch bindings.
    pub fn new(operation: CommandId, records: &[EventEnvelope]) -> Result<Self, String> {
        if records.is_empty() {
            return Err("authority selection must include at least one record".to_owned());
        }
        let selected = records
            .iter()
            .map(SelectedAuthorityRecord::from_envelope)
            .collect::<Result<Vec<_>, _>>()?;
        let selected = canonical_records(selected)?;
        let realm = selected
            .first()
            .map(|record| record.realm.clone())
            .ok_or_else(|| "authority selection must include at least one record".to_owned())?;
        if selected.iter().any(|record| record.realm != realm) {
            return Err("authority selection mixes authority realms".to_owned());
        }
        Ok(Self {
            operation,
            realm,
            records: selected,
        })
    }

    #[must_use]
    pub const fn operation(&self) -> CommandId {
        self.operation
    }

    #[must_use]
    pub const fn realm_id(&self) -> &AuthorityRealmKey {
        &self.realm
    }

    /// Encode the selection as the full value signed and chosen by control.
    ///
    /// # Errors
    /// Returns an error when the canonical selection cannot be serialized.
    pub fn control_value(&self) -> Result<ControlValue, String> {
        ControlTransition::retain(self.operation, self.payload_value()?).control_value()
    }

    fn payload_value(&self) -> Result<ControlValue, String> {
        let mut bytes = SELECTION_DOMAIN.to_vec();
        serde_json::to_writer(
            &mut bytes,
            &AuthoritySelectionWire {
                operation: self.operation,
                records: self.records.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
        Ok(ControlValue(bytes))
    }

    pub(super) fn from_payload(value: &ControlValue) -> Result<Self, String> {
        let encoded = value
            .0
            .strip_prefix(SELECTION_DOMAIN)
            .ok_or_else(|| "control value is not a certified authority selection".to_owned())?;
        let wire: AuthoritySelectionWire =
            serde_json::from_slice(encoded).map_err(|error| error.to_string())?;
        if wire.records.is_empty() {
            return Err("authority selection must include at least one record".to_owned());
        }
        let records = canonical_records(wire.records)?;
        let realm = records
            .first()
            .map(|record| record.realm.clone())
            .ok_or_else(|| "authority selection must include at least one record".to_owned())?;
        for record in &records {
            validate_selected_event(record, Some(&realm))?;
        }
        let selection = Self {
            operation: wire.operation,
            realm,
            records,
        };
        if selection.payload_value()? != value.clone() {
            return Err("authority selection is not canonical".to_owned());
        }
        Ok(selection)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityDecisionRoot {
    realm: AuthorityRealmKey,
    request: CommandId,
    phase: AuthorizationPhase,
}

impl AuthorityDecisionRoot {
    /// Stable request identity for one certified decision.
    ///
    /// The root deliberately omits effect bytes, topology, controller sets and
    /// retry predecessor so that a changed body under the same root is a
    /// conflict instead of another spend.
    ///
    /// # Errors
    /// Returns an error when the authority realm is empty.
    pub fn new(
        realm: AuthorityRealmKey,
        request: CommandId,
        phase: AuthorizationPhase,
    ) -> Result<Self, String> {
        if realm.as_str().is_empty() {
            return Err("authority decision realm is empty".to_owned());
        }
        Ok(Self {
            realm,
            request,
            phase,
        })
    }

    #[must_use]
    pub const fn realm_id(&self) -> &AuthorityRealmKey {
        &self.realm
    }

    #[must_use]
    pub const fn request_id(&self) -> CommandId {
        self.request
    }

    #[must_use]
    pub const fn phase(&self) -> AuthorizationPhase {
        self.phase
    }

    fn key(&self) -> DecisionRootKey {
        DecisionRootKey {
            realm: self.realm.as_str().to_owned(),
            request: self.request,
            phase: phase_key(self.phase),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorityDecisionTransition {
    operation: CommandId,
    root: AuthorityDecisionRoot,
    request: AccessAttempt,
    binding: AuthorizationBinding,
    topology: ScopeTopology,
    evaluated_at: DateTime<Utc>,
    decision_id: String,
    seed: [u8; 32],
    decision: AuthorizationDecision,
    records: Vec<DecisionRecord>,
}

impl AuthorityDecisionTransition {
    /// Encode the validated decision as the full value signed and chosen by control.
    ///
    /// # Errors
    /// Returns an error when the canonical decision cannot be serialized.
    pub fn control_value(&self) -> Result<ControlValue, String> {
        ControlTransition::retain(self.operation, self.payload_value()?).control_value()
    }

    #[must_use]
    pub const fn operation(&self) -> CommandId {
        self.operation
    }

    #[must_use]
    pub const fn root(&self) -> &AuthorityDecisionRoot {
        &self.root
    }

    #[must_use]
    pub const fn decision(&self) -> &AuthorizationDecision {
        &self.decision
    }

    #[must_use]
    pub const fn request(&self) -> &AccessAttempt {
        &self.request
    }

    #[must_use]
    pub const fn binding(&self) -> &AuthorizationBinding {
        &self.binding
    }

    #[must_use]
    pub const fn evaluated_at(&self) -> &DateTime<Utc> {
        &self.evaluated_at
    }

    fn records(&self) -> &[DecisionRecord] {
        &self.records
    }

    fn plan(
        operation: CommandId,
        root: AuthorityDecisionRoot,
        request: AccessAttempt,
        evaluated_at: DateTime<Utc>,
        topology: ScopeTopology,
        state: &crate::EvaluationState,
    ) -> Result<Self, String> {
        if root.realm_id().as_str().is_empty() {
            return Err("authority decision realm is empty".to_owned());
        }
        let request = canonical_request(request);
        if request.authorization_phase != root.phase() {
            return Err("authority decision root phase does not match request".to_owned());
        }
        let mut request_for_evaluation = request.clone();
        request_for_evaluation.topology = Some(topology.clone());
        let binding = AuthorizationBinding::from_request(&request_for_evaluation);
        let seed = decision_seed(&root)?;
        let decision_id = decision_id(&root)?;
        let outcome = evaluate_seeded(state, &request_for_evaluation, evaluated_at, seed);
        let records = canonical_decision_records(decision_records(
            root.realm_id(),
            &request_for_evaluation,
            state,
            &outcome,
            &decision_id,
            evaluated_at,
        ));
        Ok(Self {
            operation,
            root,
            request,
            binding,
            topology,
            evaluated_at,
            decision_id,
            seed,
            decision: outcome.decision,
            records,
        })
    }

    fn payload_value(&self) -> Result<ControlValue, String> {
        let mut bytes = DECISION_DOMAIN.to_vec();
        serde_json::to_writer(
            &mut bytes,
            &AuthorityDecisionWire {
                operation: self.operation,
                root: self.root.clone(),
                request: self.request.clone(),
                binding: self.binding.clone(),
                topology: self.topology.clone(),
                evaluated_at: self.evaluated_at,
                decision_id: self.decision_id.clone(),
                seed: self.seed,
                decision: self.decision.clone(),
                records: self.records.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
        Ok(ControlValue(bytes))
    }

    fn from_payload(value: &ControlValue) -> Result<Self, String> {
        let encoded = value
            .0
            .strip_prefix(DECISION_DOMAIN)
            .ok_or_else(|| "control value is not a certified authority decision".to_owned())?;
        let wire: AuthorityDecisionWire =
            serde_json::from_slice(encoded).map_err(|error| error.to_string())?;
        let transition = Self {
            operation: wire.operation,
            root: wire.root,
            request: canonical_request(wire.request),
            binding: wire.binding,
            topology: wire.topology,
            evaluated_at: wire.evaluated_at,
            decision_id: wire.decision_id,
            seed: wire.seed,
            decision: wire.decision,
            records: canonical_decision_records(wire.records),
        };
        transition.validate_static()?;
        if transition.payload_value()? != value.clone() {
            return Err("authority decision is not canonical".to_owned());
        }
        Ok(transition)
    }

    pub(super) fn from_retained_payload(value: &ControlValue) -> Result<Option<Self>, String> {
        if !value.0.starts_with(DECISION_DOMAIN) {
            return Ok(None);
        }
        Self::from_payload(value).map(Some)
    }

    fn validate_static(&self) -> Result<(), String> {
        if self.root.realm_id().as_str().is_empty() {
            return Err("authority decision realm is empty".to_owned());
        }
        if self.request.authorization_phase != self.root.phase() {
            return Err("authority decision root phase does not match request".to_owned());
        }
        let mut request = self.request.clone();
        request.topology = Some(self.topology.clone());
        if AuthorizationBinding::from_request(&request) != self.binding {
            return Err("authority decision binding does not match request".to_owned());
        }
        if decision_seed(&self.root)? != self.seed {
            return Err("authority decision seed does not match root".to_owned());
        }
        if decision_id(&self.root)? != self.decision_id {
            return Err("authority decision id does not match root".to_owned());
        }
        if canonical_decision_records(self.records.clone()) != self.records {
            return Err("authority decision records are not canonical".to_owned());
        }
        Ok(())
    }

    fn validate_against(&self, state: &crate::EvaluationState) -> Result<(), String> {
        let planned = Self::plan(
            self.operation,
            self.root.clone(),
            self.request.clone(),
            self.evaluated_at,
            self.topology.clone(),
            state,
        )?;
        if &planned != self {
            return Err(
                "certified authority decision does not match predecessor evaluation".to_owned(),
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct AuthorityHistory {
    anchor: AuthorityAnchor,
    history: Vec<EventEnvelope>,
    chain: CertifiedControlChain,
}

impl AuthorityHistory {
    /// Reconstruct certified historical authority choices from node history.
    ///
    /// Replay is a tolerant collector. It ignores unrelated, wrong-epoch,
    /// incomplete, or malformed unchosen control evidence. A malformed chosen
    /// head fails when that exact head is requested with [`Self::assess_at`].
    ///
    /// # Errors
    /// Returns an error when the local history itself contains conflicting
    /// immutable event identities or cannot be read.
    pub fn replay(node: &Node, anchor: AuthorityAnchor) -> Result<Self, String> {
        let history = node.events_after(None).map_err(|error| error.to_string())?;
        let chain = CertifiedControlChain::replay(&history, anchor.control_anchor())?;
        Ok(Self {
            anchor,
            history,
            chain,
        })
    }

    #[must_use]
    pub const fn realm_id(&self) -> &AuthorityRealmKey {
        self.anchor.realm_id()
    }

    #[must_use]
    pub(super) fn history(&self) -> &[EventEnvelope] {
        &self.history
    }

    /// Return the certified control context after validating authority history.
    ///
    /// # Errors
    /// Rejects unknown heads, malformed authority transitions, missing selected
    /// bodies, broken causal closure, or invalid typed authority facts.
    pub fn context_at(&self, head: ControlHead) -> Result<CertifiedControlContext, String> {
        let selected = self.selected_at(head)?;
        super::validate_facts(&selected, self.realm_id())?;
        self.chain.context_at(head)
    }

    pub(super) fn validate_transition_at(
        &self,
        head: ControlHead,
        value: &ControlValue,
    ) -> Result<(), String> {
        let candidate = ControlTransition::from_control_value(value)?;
        let mut transitions = self.chain.transitions_to(head)?;
        transitions.push(&candidate);
        let selected = self.selected_from_transitions(transitions)?;
        super::validate_facts(&selected, self.realm_id())
    }

    /// Plan a request-specific certified decision after one historical predecessor.
    ///
    /// This is not live permission by itself. A live caller must still choose
    /// this value through the authority controller quorum and then verify that
    /// the resulting certificate applies to the exact prepared effect it is
    /// about to release.
    ///
    /// # Errors
    /// Rejects invalid predecessor history, malformed roots, and planning
    /// inputs that cannot be canonically encoded.
    ///
    /// `topology` must be locally derived or otherwise trusted by the caller.
    /// Serialized decision payloads bind it for historical replay, but this
    /// method does not authenticate topology received from a client.
    pub fn plan_decision_at(
        &self,
        head: ControlHead,
        operation: CommandId,
        request_id: CommandId,
        request: AccessAttempt,
        evaluated_at: DateTime<Utc>,
        topology: ScopeTopology,
    ) -> Result<AuthorityDecisionTransition, String> {
        let facts = self.selected_at(head)?;
        let state = project_facts(&facts, self.realm_id(), topology.clone())?;
        AuthorityDecisionTransition::plan(
            operation,
            AuthorityDecisionRoot::new(
                self.realm_id().clone(),
                request_id,
                request.authorization_phase,
            )?,
            request,
            evaluated_at,
            topology,
            &state,
        )
    }

    /// Return a previously certified request-specific decision at `head`.
    ///
    /// Exact retries use this to recover the already chosen result instead of
    /// attempting a second spend under the same stable root.
    ///
    /// # Errors
    /// Rejects invalid certified history at the requested head.
    pub fn decision_at(
        &self,
        head: ControlHead,
        root: &AuthorityDecisionRoot,
    ) -> Result<Option<AuthorityDecisionTransition>, String> {
        self.selected_at(head)?;
        for transition in self.chain.transitions_to(head)? {
            let ControlTransition::Retain { payload, .. } = transition else {
                continue;
            };
            if payload.0.starts_with(DECISION_DOMAIN) {
                let decision = AuthorityDecisionTransition::from_payload(payload)?;
                if decision.root() == root {
                    return Ok(Some(decision));
                }
            }
        }
        Ok(None)
    }

    pub(super) fn selected_at(
        &self,
        head: ControlHead,
    ) -> Result<Vec<CertifiedAuthorityFact>, String> {
        let transitions = self.chain.transitions_to(head)?;
        self.selected_from_transitions(transitions)
    }

    fn selected_from_transitions<'a>(
        &self,
        transitions: impl IntoIterator<Item = &'a ControlTransition>,
    ) -> Result<Vec<CertifiedAuthorityFact>, String> {
        let mut replay =
            AuthorityFactReplay::new(retained_by_origin(&self.history)?, self.realm_id());
        for transition in transitions {
            replay.apply(transition)?;
        }
        Ok(replay.output)
    }
}

struct AuthorityFactReplay<'a> {
    retained: BTreeMap<EventKey, &'a EventEnvelope>,
    realm: &'a AuthorityRealmKey,
    operations: BTreeSet<CommandId>,
    selected_events: BTreeSet<EventKey>,
    command_requests: BTreeMap<CommandId, Vec<u8>>,
    committed_commands: BTreeSet<CommandId>,
    decisions: BTreeMap<DecisionRootKey, AuthorityDecisionTransition>,
    decision_records: BTreeSet<(&'static str, String)>,
    selected_history: Vec<EventEnvelope>,
    output: Vec<CertifiedAuthorityFact>,
}

impl<'a> AuthorityFactReplay<'a> {
    const fn new(
        retained: BTreeMap<EventKey, &'a EventEnvelope>,
        realm: &'a AuthorityRealmKey,
    ) -> Self {
        Self {
            retained,
            realm,
            operations: BTreeSet::new(),
            selected_events: BTreeSet::new(),
            command_requests: BTreeMap::new(),
            committed_commands: BTreeSet::new(),
            decisions: BTreeMap::new(),
            decision_records: BTreeSet::new(),
            selected_history: Vec::new(),
            output: Vec::new(),
        }
    }

    fn apply(&mut self, transition: &ControlTransition) -> Result<(), String> {
        if !self.operations.insert(transition.operation()) {
            return Err("authority operation id is reused in certified chain".to_owned());
        }
        let ControlTransition::Retain { operation, payload } = transition else {
            return AuthorityRotation::validate_transition(
                transition.operation(),
                transition.payload(),
                self.realm,
            );
        };
        if payload.0.starts_with(SELECTION_DOMAIN) {
            return self.apply_selection(*operation, payload);
        }
        if payload.0.starts_with(DECISION_DOMAIN) {
            return self.apply_decision(*operation, payload);
        }
        Err("control retain payload is not a certified authority value".to_owned())
    }

    fn apply_selection(
        &mut self,
        operation: CommandId,
        payload: &ControlValue,
    ) -> Result<(), String> {
        let selection = AuthoritySelection::from_payload(payload)?;
        if selection.operation() != operation {
            return Err(
                "authority selection operation does not match control transition".to_owned(),
            );
        }
        if selection.realm_id() != self.realm {
            return Err("authority proposal selects another realm".to_owned());
        }
        let current = self.retained_selection_events(&selection)?;
        self.append_causal_selection(current)
    }

    fn retained_selection_events(
        &mut self,
        selection: &AuthoritySelection,
    ) -> Result<Vec<EventEnvelope>, String> {
        let mut current = Vec::new();
        for record in &selection.records {
            let key = record.key();
            if self.selected_events.contains(&key) {
                return Err("authority event identity is selected more than once".to_owned());
            }
            let retained = self
                .retained
                .get(&key)
                .ok_or_else(|| "selected authority event is not retained".to_owned())?;
            if !record.matches(retained) {
                return Err("retained authority event differs from certified record".to_owned());
            }
            track_command(
                record,
                &mut self.command_requests,
                &mut self.committed_commands,
            )?;
            self.selected_events.insert(key);
            current.push((*retained).clone());
        }
        Ok(current)
    }

    fn append_causal_selection(&mut self, current: Vec<EventEnvelope>) -> Result<(), String> {
        let current_events = current
            .iter()
            .map(|event| EventKey::new(event.origin))
            .collect::<BTreeSet<_>>();
        let mut combined = self.selected_history.clone();
        combined.extend(current);
        let replayed = causal_replay(&combined).map_err(|error| error.to_string())?;
        if replayed.len() != combined.len() {
            return Err("selected authority record has an uncertified causal parent".to_owned());
        }
        for event in replayed
            .into_iter()
            .filter(|event| current_events.contains(&EventKey::new(event.origin)))
            .cloned()
        {
            self.selected_history.push(event.clone());
            self.output.push(CertifiedAuthorityFact::Event(event));
        }
        Ok(())
    }

    fn apply_decision(
        &mut self,
        operation: CommandId,
        payload: &ControlValue,
    ) -> Result<(), String> {
        let decision = AuthorityDecisionTransition::from_payload(payload)?;
        if decision.operation() != operation {
            return Err(
                "authority decision operation does not match control transition".to_owned(),
            );
        }
        if decision.root().realm_id() != self.realm {
            return Err("authority decision names another realm".to_owned());
        }
        self.track_decision_root(&decision)?;
        let state = project_facts(&self.output, self.realm, decision.topology.clone())?;
        decision.validate_against(&state)?;
        for record in decision.records() {
            let identity = (record.item_type(), record.item_id().to_owned());
            if !self.decision_records.insert(identity) {
                return Err("authority decision record identity is reused".to_owned());
            }
            self.output
                .push(CertifiedAuthorityFact::Decision(record.clone()));
        }
        Ok(())
    }

    fn track_decision_root(
        &mut self,
        decision: &AuthorityDecisionTransition,
    ) -> Result<(), String> {
        if let Some(previous) = self
            .decisions
            .insert(decision.root().key(), decision.clone())
        {
            if previous.binding != decision.binding
                || previous.request != decision.request
                || previous.topology != decision.topology
            {
                return Err(
                    "authority decision root was reused with a different binding".to_owned(),
                );
            }
            return Err("authority decision root is selected more than once".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AuthoritySelectionWire {
    operation: CommandId,
    records: Vec<SelectedAuthorityRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AuthorityDecisionWire {
    operation: CommandId,
    root: AuthorityDecisionRoot,
    request: AccessAttempt,
    binding: AuthorizationBinding,
    topology: ScopeTopology,
    evaluated_at: DateTime<Utc>,
    decision_id: String,
    seed: [u8; 32],
    decision: AuthorizationDecision,
    records: Vec<DecisionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DecisionRootKey {
    realm: String,
    request: CommandId,
    phase: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SelectedAuthorityRecord {
    origin: EventId,
    recorded_at: DateTime<Utc>,
    event: NodeEvent,
    realm: AuthorityRealmKey,
}

impl SelectedAuthorityRecord {
    fn from_envelope(envelope: &EventEnvelope) -> Result<Self, String> {
        let realm = validate_envelope(envelope)?;
        Ok(Self {
            origin: envelope.origin,
            recorded_at: envelope.recorded_at,
            event: envelope.event.clone(),
            realm,
        })
    }

    const fn key(&self) -> EventKey {
        EventKey::new(self.origin)
    }

    fn matches(&self, envelope: &EventEnvelope) -> bool {
        self.origin == envelope.origin
            && self.recorded_at == envelope.recorded_at
            && self.event == envelope.event
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EventKey {
    node_id: myko_federation::NodeId,
    sequence: LogPosition,
}

impl EventKey {
    const fn new(origin: EventId) -> Self {
        Self {
            node_id: origin.node_id,
            sequence: origin.sequence,
        }
    }
}

fn retained_by_origin(
    history: &[EventEnvelope],
) -> Result<BTreeMap<EventKey, &EventEnvelope>, String> {
    let mut retained = BTreeMap::new();
    for envelope in history {
        if let Some(previous) = retained.insert(EventKey::new(envelope.origin), envelope)
            && (previous.recorded_at != envelope.recorded_at || previous.event != envelope.event)
        {
            return Err("retained history contains conflicting event identities".to_owned());
        }
    }
    Ok(retained)
}

fn canonical_records(
    mut records: Vec<SelectedAuthorityRecord>,
) -> Result<Vec<SelectedAuthorityRecord>, String> {
    records.sort_unstable_by_key(SelectedAuthorityRecord::key);
    let mut seen_events = BTreeSet::new();
    let mut command_requests = BTreeMap::new();
    let mut committed_commands = BTreeSet::new();
    for record in &records {
        validate_selected_event(record, None)?;
        if !seen_events.insert(record.key()) {
            return Err("authority selection repeats an event identity".to_owned());
        }
        track_command(record, &mut command_requests, &mut committed_commands)?;
    }
    Ok(records)
}

fn track_command(
    record: &SelectedAuthorityRecord,
    command_requests: &mut BTreeMap<CommandId, Vec<u8>>,
    committed_commands: &mut BTreeSet<CommandId>,
) -> Result<(), String> {
    let (command_id, request, committed) = command_request(record)?;
    if let Some(existing) = command_requests.insert(command_id, request.clone())
        && existing != request
    {
        return Err("authority selection contains conflicting command requests".to_owned());
    }
    if committed && !committed_commands.insert(command_id) {
        return Err("authority selection contains duplicate commits for one command".to_owned());
    }
    Ok(())
}

fn command_request(record: &SelectedAuthorityRecord) -> Result<(CommandId, Vec<u8>, bool), String> {
    match &record.event {
        NodeEvent::CommandLifecycle(command) => Ok((
            command.request.id,
            serde_json::to_vec(&command.request).map_err(|error| error.to_string())?,
            false,
        )),
        NodeEvent::CommandCommitted { command, .. } => Ok((
            command.request.id,
            serde_json::to_vec(&command.request).map_err(|error| error.to_string())?,
            true,
        )),
        NodeEvent::FrameworkControl(_) => {
            Err("authority selection can only contain command events".to_owned())
        }
    }
}

fn validate_selected_event(
    record: &SelectedAuthorityRecord,
    expected_realm: Option<&AuthorityRealmKey>,
) -> Result<AuthorityRealmKey, String> {
    let realm = validate_event(&record.event)?;
    if record.realm != realm {
        return Err("authority record realm does not match its event".to_owned());
    }
    if expected_realm.is_some_and(|expected| expected != &realm) {
        return Err("authority selection mixes authority realms".to_owned());
    }
    Ok(realm)
}

fn validate_envelope(envelope: &EventEnvelope) -> Result<AuthorityRealmKey, String> {
    validate_event(&envelope.event)
}

fn validate_event(event: &NodeEvent) -> Result<AuthorityRealmKey, String> {
    let (command, batch) = match event {
        NodeEvent::CommandLifecycle(command) => (command, None),
        NodeEvent::CommandCommitted { command, batch } => (command, Some(batch)),
        NodeEvent::FrameworkControl(_) => {
            return Err("authority selection can only contain command events".to_owned());
        }
    };
    let authority_service = ServiceId::new(AuthorityService::SERVICE_ID);
    if command.request.service_id != authority_service {
        return Err("authority selection contains a non-authority service event".to_owned());
    }
    let realm = realm_from_scope(&command.request.scope_id)?;
    let realm_scope = authority_realm_scope(&realm);
    if command.request.scope_id != realm_scope {
        return Err("authority command scope is not the exact authority realm".to_owned());
    }
    let Some(batch) = batch else {
        return Ok(realm);
    };
    if batch.service_id != authority_service {
        return Err("authority selection contains a non-authority service event".to_owned());
    }
    if batch.command_id != command.request.id {
        return Err("authority command and batch ids do not match".to_owned());
    }
    if batch.scope_id != command.request.scope_id {
        return Err("authority command and batch scopes do not match".to_owned());
    }
    if batch.changes.is_empty() {
        return Err("authority selection contains an empty command batch".to_owned());
    }
    for mutation in &batch.changes {
        if mutation.service_id != AuthorityService::SERVICE_ID.as_str() {
            return Err("authority batch contains a non-authority mutation".to_owned());
        }
        let effective_scope = mutation
            .scope_id
            .as_deref()
            .unwrap_or(batch.scope_id.as_str());
        if effective_scope != realm_scope.as_str() {
            return Err("authority mutation is outside the selected realm".to_owned());
        }
    }
    Ok(realm)
}

fn realm_from_scope(scope: &ScopeId) -> Result<AuthorityRealmKey, String> {
    let empty = AuthorityRealmKey::new("");
    let prefix = authority_realm_scope(&empty);
    let Some(realm) = scope.as_str().strip_prefix(prefix.as_str()) else {
        return Err("scope is not an authority realm scope".to_owned());
    };
    if realm.is_empty() {
        return Err("authority realm id is empty".to_owned());
    }
    Ok(AuthorityRealmKey::new(realm))
}

fn canonical_request(mut request: AccessAttempt) -> AccessAttempt {
    request.topology = None;
    request
}

fn canonical_decision_records(mut records: Vec<DecisionRecord>) -> Vec<DecisionRecord> {
    for record in &mut records {
        if let DecisionRecord::Audit(audit) = record {
            audit.request = canonical_request(audit.request.clone());
        }
    }
    records
}

fn decision_seed(root: &AuthorityDecisionRoot) -> Result<[u8; 32], String> {
    let mut bytes = b"myko/certified-authority-evaluation-seed/v1\0".to_vec();
    serde_json::to_writer(&mut bytes, root).map_err(|error| error.to_string())?;
    Ok(Sha256::digest(bytes).into())
}

fn decision_id(root: &AuthorityDecisionRoot) -> Result<String, String> {
    let mut bytes = b"myko/certified-authority-decision-id/v1\0".to_vec();
    serde_json::to_writer(&mut bytes, root).map_err(|error| error.to_string())?;
    Ok(format!("decision/{:x}", Sha256::digest(bytes)))
}

const fn phase_key(phase: AuthorizationPhase) -> u8 {
    match phase {
        AuthorizationPhase::Admission => 0,
        AuthorizationPhase::Effect => 1,
        AuthorizationPhase::Continuation => 2,
    }
}
