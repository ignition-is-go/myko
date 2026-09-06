use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use myko_federation::{
    CertifiedControlChain, CertifiedControlContext, CommandId, ControlAnchor, ControlTransition,
    EventEnvelope, EventId, LogPosition, MykoService as _, Node, NodeEvent, ScopeId, ServiceId,
    causal_replay,
    control_quorum::{ControlEpochId, ControlHead, ControlValue, ControllerId},
};
use serde::{Deserialize, Serialize};

use crate::{AuthorityRealmKey, AuthorityService, authority_realm_scope};

use super::AuthorityRotation;

const SELECTION_DOMAIN: &[u8] = b"myko/certified-authority-selection/v1\0";

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

    fn from_payload(value: &ControlValue) -> Result<Self, String> {
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

    pub(super) fn selected_at(&self, head: ControlHead) -> Result<Vec<EventEnvelope>, String> {
        let transitions = self.chain.transitions_to(head)?;
        self.selected_from_transitions(transitions)
    }

    fn selected_from_transitions<'a>(
        &self,
        transitions: impl IntoIterator<Item = &'a ControlTransition>,
    ) -> Result<Vec<EventEnvelope>, String> {
        let retained = retained_by_origin(&self.history)?;
        let mut operations = BTreeSet::new();
        let mut selected_events = BTreeSet::new();
        let mut command_requests = BTreeMap::new();
        let mut committed_commands = BTreeSet::new();
        let mut output = Vec::new();

        for transition in transitions {
            if !operations.insert(transition.operation()) {
                return Err("authority operation id is reused in certified chain".to_owned());
            }
            let ControlTransition::Retain { operation, payload } = transition else {
                AuthorityRotation::validate_transition(
                    transition.operation(),
                    transition.payload(),
                    self.realm_id(),
                )?;
                continue;
            };
            let selection = AuthoritySelection::from_payload(payload)?;
            if selection.operation() != *operation {
                return Err(
                    "authority selection operation does not match control transition".to_owned(),
                );
            }
            if selection.realm_id() != self.realm_id() {
                return Err("authority proposal selects another realm".to_owned());
            }
            let mut current = Vec::new();
            for record in &selection.records {
                let key = record.key();
                if selected_events.contains(&key) {
                    return Err("authority event identity is selected more than once".to_owned());
                }
                let retained = retained
                    .get(&key)
                    .ok_or_else(|| "selected authority event is not retained".to_owned())?;
                if !record.matches(retained) {
                    return Err("retained authority event differs from certified record".to_owned());
                }
                track_command(record, &mut command_requests, &mut committed_commands)?;
                selected_events.insert(key);
                current.push((*retained).clone());
            }
            let current_events = current
                .iter()
                .map(|event| EventKey::new(event.origin))
                .collect::<BTreeSet<_>>();
            let mut combined = output.clone();
            combined.extend(current);
            let replayed = causal_replay(&combined).map_err(|error| error.to_string())?;
            if replayed.len() != combined.len() {
                return Err("selected authority record has an uncertified causal parent".to_owned());
            }
            output.extend(
                replayed
                    .into_iter()
                    .filter(|event| current_events.contains(&EventKey::new(event.origin)))
                    .cloned(),
            );
        }

        Ok(output)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AuthoritySelectionWire {
    operation: CommandId,
    records: Vec<SelectedAuthorityRecord>,
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
