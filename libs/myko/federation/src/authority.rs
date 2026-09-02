//! Durable, transport-neutral authority and provenance evaluation.
//!
//! Authentication answers which executor presented a request. Authority keeps
//! that executor distinct from the originating principal and proves every hop
//! between them using immutable, attenuating delegation records.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AccessOperation, AccessPolicy, AccessRequest, FederationPermission, PrincipalId,
    ReplicationSelection, ScopeId, ScopeSelection, ScopeTopology, ServiceId,
};

macro_rules! authority_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn random() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

authority_id!(AuthorityGrantId);
authority_id!(DelegationId);
authority_id!(CapabilityId);
authority_id!(ObligationId);
authority_id!(ChallengeId);
authority_id!(ApprovalId);

/// Stable kind of an authenticated or delegated actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Person,
    Node,
    Agent,
    Command,
    Task,
    Tool,
    Service,
}

/// One actor identity. Its kind is part of every approval and delegation binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Principal {
    pub id: PrincipalId,
    pub kind: PrincipalKind,
}

impl Principal {
    #[must_use]
    pub const fn new(id: PrincipalId, kind: PrincipalKind) -> Self {
        Self { id, kind }
    }

    #[must_use]
    pub fn node(id: PrincipalId) -> Self {
        Self::new(id, PrincipalKind::Node)
    }
}

/// Why a principal delegated authority to another actor.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProvenanceOperation {
    AgentInvocation { agent_id: String },
    CommandInvocation { command_id: String },
    TaskInvocation { task_id: String },
    ToolResourceOperation {
        tool_id: String,
        resource: String,
        operation: String,
    },
    NodeForward { node_id: String },
}

/// One immutable, ledger-backed hop in the original principal's execution chain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProvenanceHop {
    pub delegation_id: DelegationId,
    pub delegator: Principal,
    pub delegate: Principal,
    pub operation: ProvenanceOperation,
}

/// Authority presented with a request without replacing its original principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityPresentation {
    pub principal: Principal,
    pub executor: Principal,
    #[serde(default)]
    pub provenance: Vec<ProvenanceHop>,
    #[serde(default)]
    pub approvals: Vec<ApprovalId>,
}

impl AuthorityPresentation {
    #[must_use]
    pub fn direct(principal: Principal) -> Self {
        Self {
            executor: principal.clone(),
            principal,
            provenance: Vec::new(),
            approvals: Vec::new(),
        }
    }

    #[must_use]
    pub fn direct_node(id: PrincipalId) -> Self {
        Self::direct(Principal::node(id))
    }

    /// Adds a delegation-backed executor without changing the original principal.
    #[must_use]
    pub fn forward(mut self, hop: ProvenanceHop) -> Self {
        self.executor = hop.delegate.clone();
        self.provenance.push(hop);
        self
    }

    #[must_use]
    pub fn with_approval(mut self, approval: ApprovalId) -> Self {
        self.approvals.push(approval);
        self
    }
}

/// Role played by one resource in command authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceClaimKind {
    Primary,
    Referenced,
    Affected,
}

/// One exact or subtree scope claim, optionally narrowed to an item entity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceClaim {
    pub selection: ScopeSelection,
    pub kind: ResourceClaimKind,
    pub service_id: Option<ServiceId>,
    pub item_type: Option<String>,
    pub item_id: Option<String>,
}

impl ResourceClaim {
    #[must_use]
    pub fn scope(scope_id: ScopeId, kind: ResourceClaimKind) -> Self {
        Self {
            selection: ScopeSelection::Exact(scope_id),
            kind,
            service_id: None,
            item_type: None,
            item_id: None,
        }
    }
}

/// Optional service, command, item, lease, and execution-mode attenuation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityConstraints {
    #[serde(default)]
    pub services: Vec<ServiceId>,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub item_types: Vec<String>,
    pub max_lease_seconds: Option<u64>,
    #[serde(default)]
    pub allow_offline: bool,
}

impl AuthorityConstraints {
    fn permits(&self, request: &AccessRequest) -> bool {
        (self.services.is_empty()
            || request
                .service_id
                .as_ref()
                .is_some_and(|service| self.services.contains(service)))
            && (self.commands.is_empty()
                || request
                    .command_type
                    .as_ref()
                    .is_some_and(|command| self.commands.contains(command)))
            && (self.item_types.is_empty()
                || (!request.resource_claims.is_empty()
                    && request.resource_claims.iter().all(|claim| {
                        claim
                            .item_type
                            .as_ref()
                            .is_some_and(|item| self.item_types.contains(item))
                    })))
            && request.lease.as_ref().is_none_or(|lease| {
                (!lease.offline || self.allow_offline)
                    && self
                        .max_lease_seconds
                        .is_none_or(|maximum| lease.duration_seconds <= maximum)
            })
    }

    fn attenuates(&self, parent: &Self) -> bool {
        subset_or_parent_unbounded(&self.services, &parent.services)
            && subset_or_parent_unbounded(&self.commands, &parent.commands)
            && subset_or_parent_unbounded(&self.item_types, &parent.item_types)
            && match (self.max_lease_seconds, parent.max_lease_seconds) {
                (_, None) => true,
                (Some(child), Some(parent)) => child <= parent,
                (None, Some(_)) => false,
            }
            && (!self.allow_offline || parent.allow_offline)
    }
}

fn subset_or_parent_unbounded<T: Eq>(child: &[T], parent: &[T]) -> bool {
    parent.is_empty() || (!child.is_empty() && child.iter().all(|value| parent.contains(value)))
}

/// A registered opaque application permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationCapability {
    pub id: CapabilityId,
    pub description: String,
    #[serde(default)]
    pub constraints: AuthorityConstraints,
}

/// Requested lease for a decision, including explicit offline use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityLeaseRequest {
    pub duration_seconds: u64,
    pub offline: bool,
}

/// Lease actually issued by a permit decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityLease {
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub offline: bool,
}

/// Evaluation phase prevents stream rechecks from consuming one-shot authority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationPhase {
    #[default]
    Admission,
    Effect,
    Continuation,
}

/// One durable scope and capability grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityGrant {
    pub id: AuthorityGrantId,
    pub grantor: Principal,
    pub grantee: Principal,
    pub selection: ScopeSelection,
    #[serde(default)]
    pub permissions: Vec<FederationPermission>,
    #[serde(default)]
    pub operations: Vec<AccessOperation>,
    #[serde(default)]
    pub capabilities: Vec<CapabilityId>,
    #[serde(default)]
    pub constraints: AuthorityConstraints,
    #[serde(default)]
    pub obligations: Vec<ObligationId>,
    pub valid_from: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub max_uses: Option<u64>,
}

impl AuthorityGrant {
    #[must_use]
    pub fn scope(
        grantor: Principal,
        grantee: Principal,
        selection: ScopeSelection,
        permissions: Vec<FederationPermission>,
    ) -> Self {
        Self {
            id: AuthorityGrantId::random(),
            grantor,
            grantee,
            selection,
            permissions,
            operations: Vec::new(),
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            obligations: Vec::new(),
            valid_from: Utc::now(),
            expires_at: None,
            max_uses: None,
        }
    }
}

/// Parent record from which a delegation is attenuated.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum DelegationParent {
    Grant(AuthorityGrantId),
    Delegation(DelegationId),
}

/// Durable delegation between two distinct actors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityDelegation {
    pub id: DelegationId,
    pub parent: DelegationParent,
    pub delegator: Principal,
    pub delegate: Principal,
    /// Ledger-bound reason for this hop; callers cannot rewrite audit provenance.
    pub provenance_operation: ProvenanceOperation,
    #[serde(default)]
    pub selections: Vec<ScopeSelection>,
    #[serde(default)]
    pub operations: Vec<AccessOperation>,
    #[serde(default)]
    pub capabilities: Vec<CapabilityId>,
    #[serde(default)]
    pub constraints: AuthorityConstraints,
    pub expires_at: Option<DateTime<Utc>>,
    pub max_uses: Option<u64>,
}

/// Application-defined approval or external proof required by a matching grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Obligation {
    pub id: ObligationId,
    pub challenge_kind: String,
    pub prompt: String,
    #[serde(default)]
    pub approvers: Vec<Principal>,
    pub approval_lifetime_seconds: u64,
    pub approval_use_count: u64,
}

/// Exact replay-resistant request binding used by challenges and approvals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationBinding {
    pub principal: Principal,
    pub executor: Principal,
    pub provenance: Vec<ProvenanceHop>,
    pub operation: AccessOperation,
    pub service_id: Option<ServiceId>,
    pub command_id: Option<String>,
    pub command_type: Option<String>,
    pub resources: Vec<ResourceClaim>,
    pub capabilities: Vec<CapabilityId>,
    pub arguments_digest: Option<String>,
    pub effect_digest: Option<String>,
}

impl AuthorizationBinding {
    #[must_use]
    pub fn from_request(request: &AccessRequest) -> Self {
        Self {
            principal: request.presentation.principal.clone(),
            executor: request.presentation.executor.clone(),
            provenance: request.presentation.provenance.clone(),
            operation: request.operation,
            service_id: request.service_id.clone(),
            command_id: request.command_id.map(|id| id.to_string()),
            command_type: request.command_type.clone(),
            resources: request.resource_claims.clone(),
            capabilities: request.application_capabilities.clone(),
            arguments_digest: request.arguments_digest.clone(),
            effect_digest: request.effect_digest.clone(),
        }
    }
}

/// Durable pending challenge returned instead of silently denying approvable work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityChallenge {
    pub id: ChallengeId,
    pub obligation_id: ObligationId,
    pub kind: String,
    pub prompt: String,
    pub binding: AuthorizationBinding,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Immutable approval outcome bound to the exact challenged operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub id: ApprovalId,
    pub challenge_id: ChallengeId,
    pub obligation_id: ObligationId,
    pub approver: Principal,
    pub binding: AuthorizationBinding,
    pub approved: bool,
    pub decided_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub max_uses: u64,
}

/// Visibility is explicit so an empty projection never asserts non-existence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceVisibility {
    Present,
    AuthoritativelyAbsent,
    Unbound,
    Unauthorized,
    Undiscoverable,
    Unreachable,
    NotReplicated,
    TopologyIncomplete,
}

/// Machine-readable reason identifying the exact policy record involved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationExplanation {
    pub code: String,
    pub message: String,
    pub grant_id: Option<AuthorityGrantId>,
    pub delegation_id: Option<DelegationId>,
    pub obligation_id: Option<ObligationId>,
    pub constraint: Option<String>,
}

/// Complete explanation and audit payload for one authorization result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationReport {
    pub evaluated_at: DateTime<Utc>,
    pub principal: Principal,
    pub executor: Principal,
    pub operation: AccessOperation,
    pub explanations: Vec<AuthorizationExplanation>,
}

/// Positive decision with an optional bounded online/offline lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermitDecision {
    pub report: AuthorizationReport,
    pub lease: Option<AuthorityLease>,
}

/// Negative decision that does not disclose protected resource existence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenyDecision {
    pub report: AuthorizationReport,
    pub visibility: ResourceVisibility,
}

/// First-class policy result used by sessions, command commits, and applications.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum AuthorizationDecision {
    Permit(PermitDecision),
    Deny(DenyDecision),
    Challenge {
        challenge: AuthorityChallenge,
        report: AuthorizationReport,
    },
}

impl AuthorizationDecision {
    #[must_use]
    pub const fn is_permit(&self) -> bool {
        matches!(self, Self::Permit(_))
    }

    #[must_use]
    pub fn public_message(&self) -> String {
        match self {
            Self::Permit(_) => "authorized".to_owned(),
            Self::Deny(decision) => decision
                .report
                .explanations
                .last()
                .map_or_else(|| "access denied".to_owned(), |value| value.message.clone()),
            Self::Challenge { challenge, .. } => format!(
                "authorization challenge {} ({})",
                challenge.id, challenge.kind
            ),
        }
    }
}

/// One immutable authority-ledger event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthorityRecord {
    CapabilityRegistered(ApplicationCapability),
    GrantIssued(AuthorityGrant),
    GrantRevoked {
        grant_id: AuthorityGrantId,
        revoked_at: DateTime<Utc>,
        reason: String,
    },
    GrantUsed {
        grant_id: AuthorityGrantId,
        used_at: DateTime<Utc>,
    },
    DelegationIssued(AuthorityDelegation),
    DelegationRevoked {
        delegation_id: DelegationId,
        revoked_at: DateTime<Utc>,
        reason: String,
    },
    DelegationUsed {
        delegation_id: DelegationId,
        used_at: DateTime<Utc>,
    },
    ObligationRegistered(Obligation),
    ChallengeIssued(AuthorityChallenge),
    ApprovalRecorded(ApprovalDecision),
    ApprovalUsed {
        approval_id: ApprovalId,
        used_at: DateTime<Utc>,
    },
    DecisionAudited {
        binding: AuthorizationBinding,
        decision: AuthorizationDecision,
    },
}

/// Append-only durability seam for grants, delegation, approval, and audit.
pub trait AuthorityJournal: fmt::Debug + Send + Sync + 'static {
    fn load(&self) -> Result<Vec<AuthorityRecord>, AuthorityError>;
    /// Atomically appends all records or none of them.
    fn append_batch(&self, records: &[AuthorityRecord]) -> Result<(), AuthorityError>;
}

/// Reference journal for tests and embedded nodes.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAuthorityJournal {
    records: Arc<Mutex<Vec<AuthorityRecord>>>,
}

impl InMemoryAuthorityJournal {
    #[must_use]
    pub fn records(&self) -> Vec<AuthorityRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl AuthorityJournal for InMemoryAuthorityJournal {
    fn load(&self) -> Result<Vec<AuthorityRecord>, AuthorityError> {
        Ok(self.records())
    }

    fn append_batch(&self, records: &[AuthorityRecord]) -> Result<(), AuthorityError> {
        self.records
            .lock()
            .map_err(|_| AuthorityError::Poisoned)?
            .extend_from_slice(records);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthorityError {
    #[error("authority state is poisoned")]
    Poisoned,
    #[error("unknown authority record: {0}")]
    UnknownRecord(String),
    #[error("invalid authority attenuation: {0}")]
    InvalidAttenuation(String),
    #[error("authority journal failed: {0}")]
    Journal(String),
}

#[derive(Debug, Default)]
struct AuthorityState {
    capabilities: HashMap<CapabilityId, ApplicationCapability>,
    grants: HashMap<AuthorityGrantId, AuthorityGrant>,
    revoked_grants: HashSet<AuthorityGrantId>,
    grant_uses: HashMap<AuthorityGrantId, u64>,
    delegations: HashMap<DelegationId, AuthorityDelegation>,
    revoked_delegations: HashSet<DelegationId>,
    delegation_uses: HashMap<DelegationId, u64>,
    obligations: HashMap<ObligationId, Obligation>,
    challenges: HashMap<ChallengeId, AuthorityChallenge>,
    approvals: HashMap<ApprovalId, ApprovalDecision>,
    approval_uses: HashMap<ApprovalId, u64>,
    audit: Vec<(AuthorizationBinding, AuthorizationDecision)>,
}

impl AuthorityState {
    fn apply(&mut self, record: &AuthorityRecord) {
        match record {
            AuthorityRecord::CapabilityRegistered(value) => {
                self.capabilities.insert(value.id.clone(), value.clone());
            }
            AuthorityRecord::GrantIssued(value) => {
                self.grants.insert(value.id.clone(), value.clone());
            }
            AuthorityRecord::GrantRevoked { grant_id, .. } => {
                self.revoked_grants.insert(grant_id.clone());
            }
            AuthorityRecord::GrantUsed { grant_id, .. } => {
                *self.grant_uses.entry(grant_id.clone()).or_default() += 1;
            }
            AuthorityRecord::DelegationIssued(value) => {
                self.delegations.insert(value.id.clone(), value.clone());
            }
            AuthorityRecord::DelegationRevoked { delegation_id, .. } => {
                self.revoked_delegations.insert(delegation_id.clone());
            }
            AuthorityRecord::DelegationUsed { delegation_id, .. } => {
                *self
                    .delegation_uses
                    .entry(delegation_id.clone())
                    .or_default() += 1;
            }
            AuthorityRecord::ObligationRegistered(value) => {
                self.obligations.insert(value.id.clone(), value.clone());
            }
            AuthorityRecord::ChallengeIssued(value) => {
                self.challenges.insert(value.id.clone(), value.clone());
            }
            AuthorityRecord::ApprovalRecorded(value) => {
                self.approvals.insert(value.id.clone(), value.clone());
            }
            AuthorityRecord::ApprovalUsed { approval_id, .. } => {
                *self.approval_uses.entry(approval_id.clone()).or_default() += 1;
            }
            AuthorityRecord::DecisionAudited { binding, decision } => {
                self.audit.push((binding.clone(), decision.clone()));
            }
        }
    }
}

/// Journal-backed authoritative evaluator shared by every runtime admission path.
pub struct AuthorityEngine {
    journal: Arc<dyn AuthorityJournal>,
    state: Mutex<AuthorityState>,
    revision: AtomicU64,
}

impl fmt::Debug for AuthorityEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorityEngine")
            .field("revision", &self.revision.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Default for AuthorityEngine {
    fn default() -> Self {
        Self::new(Arc::new(InMemoryAuthorityJournal::default()))
            .expect("an in-memory authority journal loads")
    }
}

impl AuthorityEngine {
    /// Replays every immutable authority record before accepting decisions.
    pub fn new(journal: Arc<dyn AuthorityJournal>) -> Result<Self, AuthorityError> {
        let records = journal.load()?;
        let mut state = AuthorityState::default();
        for record in &records {
            state.apply(record);
        }
        Ok(Self {
            journal,
            state: Mutex::new(state),
            revision: AtomicU64::new(records.len() as u64),
        })
    }

    fn append(&self, record: AuthorityRecord) -> Result<(), AuthorityError> {
        self.append_all(vec![record])
    }

    fn append_all(&self, records: Vec<AuthorityRecord>) -> Result<(), AuthorityError> {
        self.journal.append_batch(&records)?;
        let count = records.len() as u64;
        let mut state = self.state.lock().map_err(|_| AuthorityError::Poisoned)?;
        for record in &records {
            state.apply(record);
        }
        drop(state);
        self.revision.fetch_add(count, Ordering::AcqRel);
        Ok(())
    }

    pub fn register_capability(
        &self,
        capability: ApplicationCapability,
    ) -> Result<(), AuthorityError> {
        self.append(AuthorityRecord::CapabilityRegistered(capability))
    }

    pub fn issue_grant(&self, grant: AuthorityGrant) -> Result<(), AuthorityError> {
        let state = self.state.lock().map_err(|_| AuthorityError::Poisoned)?;
        if grant
            .capabilities
            .iter()
            .any(|id| !state.capabilities.contains_key(id))
        {
            return Err(AuthorityError::InvalidAttenuation(
                "grant references an unregistered application capability".to_owned(),
            ));
        }
        drop(state);
        self.append(AuthorityRecord::GrantIssued(grant))
    }

    pub fn revoke_grant(
        &self,
        grant_id: AuthorityGrantId,
        reason: impl Into<String>,
    ) -> Result<(), AuthorityError> {
        let state = self.state.lock().map_err(|_| AuthorityError::Poisoned)?;
        if !state.grants.contains_key(&grant_id) {
            return Err(AuthorityError::UnknownRecord(grant_id.to_string()));
        }
        drop(state);
        self.append(AuthorityRecord::GrantRevoked {
            grant_id,
            revoked_at: Utc::now(),
            reason: reason.into(),
        })
    }

    pub fn register_obligation(&self, obligation: Obligation) -> Result<(), AuthorityError> {
        self.append(AuthorityRecord::ObligationRegistered(obligation))
    }

    pub fn issue_delegation(
        &self,
        delegation: AuthorityDelegation,
    ) -> Result<(), AuthorityError> {
        self.issue_delegation_with_topology(delegation, None)
    }

    /// Issues a delegation using authoritative topology to prove attenuation.
    pub fn issue_delegation_in(
        &self,
        delegation: AuthorityDelegation,
        topology: &ScopeTopology,
    ) -> Result<(), AuthorityError> {
        self.issue_delegation_with_topology(delegation, Some(topology))
    }

    fn issue_delegation_with_topology(
        &self,
        delegation: AuthorityDelegation,
        topology: Option<&ScopeTopology>,
    ) -> Result<(), AuthorityError> {
        let state = self.state.lock().map_err(|_| AuthorityError::Poisoned)?;
        validate_delegation_attenuation(&state, &delegation, topology)?;
        drop(state);
        self.append(AuthorityRecord::DelegationIssued(delegation))
    }

    pub fn revoke_delegation(
        &self,
        delegation_id: DelegationId,
        reason: impl Into<String>,
    ) -> Result<(), AuthorityError> {
        let state = self.state.lock().map_err(|_| AuthorityError::Poisoned)?;
        if !state.delegations.contains_key(&delegation_id) {
            return Err(AuthorityError::UnknownRecord(delegation_id.to_string()));
        }
        drop(state);
        self.append(AuthorityRecord::DelegationRevoked {
            delegation_id,
            revoked_at: Utc::now(),
            reason: reason.into(),
        })
    }

    pub fn approve(
        &self,
        challenge_id: &ChallengeId,
        approver: Principal,
        approved: bool,
    ) -> Result<ApprovalDecision, AuthorityError> {
        let state = self.state.lock().map_err(|_| AuthorityError::Poisoned)?;
        let challenge = state
            .challenges
            .get(challenge_id)
            .ok_or_else(|| AuthorityError::UnknownRecord(challenge_id.to_string()))?;
        let obligation = state
            .obligations
            .get(&challenge.obligation_id)
            .ok_or_else(|| AuthorityError::UnknownRecord(challenge.obligation_id.to_string()))?;
        if !obligation.approvers.is_empty() && !obligation.approvers.contains(&approver) {
            return Err(AuthorityError::InvalidAttenuation(
                "approver is not permitted by the obligation".to_owned(),
            ));
        }
        let decision = ApprovalDecision {
            id: ApprovalId::random(),
            challenge_id: challenge.id.clone(),
            obligation_id: obligation.id.clone(),
            approver,
            binding: challenge.binding.clone(),
            approved,
            decided_at: Utc::now(),
            expires_at: (Utc::now()
                + ChronoDuration::seconds(obligation.approval_lifetime_seconds as i64))
            .min(challenge.expires_at),
            max_uses: obligation.approval_use_count,
        };
        drop(state);
        self.append(AuthorityRecord::ApprovalRecorded(decision.clone()))?;
        Ok(decision)
    }

    #[must_use]
    pub fn audit(&self) -> Vec<(AuthorizationBinding, AuthorizationDecision)> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .audit
            .clone()
    }

    fn evaluate(&self, request: &AccessRequest) -> AuthorizationDecision {
        let now = Utc::now();
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return deny(request, now, "authority_poisoned", "authority is unavailable"),
        };
        let binding = AuthorizationBinding::from_request(request);

        if request.presentation.executor.id != request.principal_id {
            return deny(
                request,
                now,
                "executor_mismatch",
                "authenticated executor does not match the presented authority chain",
            );
        }
        if let Err(explanation) = validate_provenance(&state, request, now) {
            return deny_with(request, now, explanation);
        }
        if let Some(capability) = request
            .application_capabilities
            .iter()
            .find(|id| !state.capabilities.contains_key(*id))
        {
            return deny(
                request,
                now,
                "unregistered_capability",
                &format!("application capability {capability} is not registered"),
            );
        }
        if request.application_capabilities.iter().any(|id| {
            state
                .capabilities
                .get(id)
                .is_none_or(|capability| !capability.constraints.permits(request))
        }) {
            return deny(
                request,
                now,
                "capability_constraint_denied",
                "application capability constraints do not permit this request",
            );
        }

        let claims = normalized_claims(request);
        if claims.is_empty() {
            return deny(
                request,
                now,
                "unbound_resource",
                "request is not bound to an authorized resource",
            );
        }

        let required_permission = crate::required_permission(request.operation);
        let required_stream = crate::stream_permission(request.operation);
        let mut matched = Vec::<AuthorityGrantId>::new();
        for claim in &claims {
            let grant = state.grants.values().find(|grant| {
                grant.grantee == request.presentation.principal
                    && !state.revoked_grants.contains(&grant.id)
                    && grant.valid_from <= now
                    && grant.expires_at.is_none_or(|expires| expires > now)
                    && grant
                        .max_uses
                        .is_none_or(|max| state.grant_uses.get(&grant.id).copied().unwrap_or(0) < max)
                    && (grant.operations.is_empty() || grant.operations.contains(&request.operation))
                    && required_permission.is_none_or(|value| grant.permissions.contains(&value))
                    && required_stream.is_none_or(|value| grant.permissions.contains(&value))
                    && request
                        .application_capabilities
                        .iter()
                        .all(|id| grant.capabilities.contains(id))
                    && grant.constraints.permits(request)
                    && selection_covers(&grant.selection, &claim.selection, request.topology.as_ref())
            });
            let Some(grant) = grant else {
                return deny(
                    request,
                    now,
                    "uncovered_resource",
                    "no active grant covers every requested and affected scope",
                );
            };
            if !matched.contains(&grant.id) {
                matched.push(grant.id.clone());
            }
        }

        let obligations = matched
            .iter()
            .filter_map(|id| state.grants.get(id))
            .flat_map(|grant| grant.obligations.iter().cloned())
            .collect::<HashSet<_>>();
        for obligation_id in obligations {
            let approved = request.presentation.approvals.iter().find_map(|approval_id| {
                let approval = state.approvals.get(approval_id)?;
                let uses = state.approval_uses.get(approval_id).copied().unwrap_or(0);
                (approval.obligation_id == obligation_id
                    && approval.binding == binding
                    && approval.approved
                    && approval.expires_at > now
                    && uses < approval.max_uses)
                    .then_some(approval_id.clone())
            });
            if let Some(approval_id) = approved {
                if consumes_use(request) {
                    drop(state);
                    if self
                        .append(AuthorityRecord::ApprovalUsed {
                            approval_id,
                            used_at: now,
                        })
                        .is_err()
                    {
                        return deny(request, now, "journal_failed", "approval use was not durable");
                    }
                    state = match self.state.lock() {
                        Ok(state) => state,
                        Err(_) => {
                            return deny(
                                request,
                                now,
                                "authority_poisoned",
                                "authority is unavailable",
                            );
                        }
                    };
                }
                continue;
            }
            let Some(obligation) = state.obligations.get(&obligation_id) else {
                return deny(
                    request,
                    now,
                    "missing_obligation",
                    "grant references an unavailable obligation",
                );
            };
            if let Some(existing) = state.challenges.values().find(|challenge| {
                challenge.obligation_id == obligation_id
                    && challenge.binding == binding
                    && challenge.expires_at > now
            }) {
                return challenge(request, now, existing.clone());
            }
            let value = AuthorityChallenge {
                id: ChallengeId::random(),
                obligation_id: obligation.id.clone(),
                kind: obligation.challenge_kind.clone(),
                prompt: obligation.prompt.clone(),
                binding: binding.clone(),
                issued_at: now,
                expires_at: now
                    + ChronoDuration::seconds(obligation.approval_lifetime_seconds as i64),
            };
            drop(state);
            if self
                .append(AuthorityRecord::ChallengeIssued(value.clone()))
                .is_err()
            {
                return deny(request, now, "journal_failed", "challenge was not durable");
            }
            return challenge(request, now, value);
        }

        if consumes_use(request) {
            let mut usage = matched
                .iter()
                .cloned()
                .map(|grant_id| AuthorityRecord::GrantUsed {
                    grant_id,
                    used_at: now,
                })
                .collect::<Vec<_>>();
            usage.extend(request.presentation.provenance.iter().map(|hop| {
                AuthorityRecord::DelegationUsed {
                    delegation_id: hop.delegation_id.clone(),
                    used_at: now,
                }
            }));
            drop(state);
            if self.append_all(usage).is_err() {
                return deny(request, now, "journal_failed", "authority use was not durable");
            }
        }
        let lease = request.lease.map(|requested| AuthorityLease {
            issued_at: now,
            expires_at: now + ChronoDuration::seconds(requested.duration_seconds as i64),
            offline: requested.offline,
        });
        let explanations = matched
            .into_iter()
            .map(|grant_id| AuthorizationExplanation {
                code: "grant_permit".to_owned(),
                message: format!("authorized by grant {grant_id}"),
                grant_id: Some(grant_id),
                delegation_id: None,
                obligation_id: None,
                constraint: None,
            })
            .collect();
        AuthorizationDecision::Permit(PermitDecision {
            report: report(request, now, explanations),
            lease,
        })
    }

    fn constrain_replication_inner(
        &self,
        request: &AccessRequest,
        selection: &ReplicationSelection,
        topology: &ScopeTopology,
    ) -> Result<ReplicationSelection, AuthorizationDecision> {
        let ReplicationSelection::Scopes(requested) = selection else {
            let decision = self.evaluate(request);
            return if decision.is_permit() {
                Ok(selection.clone())
            } else {
                Err(decision)
            };
        };
        let now = Utc::now();
        let state = self.state.lock().map_err(|_| {
            deny(request, now, "authority_poisoned", "authority is unavailable")
        })?;
        if validate_provenance(&state, request, now).is_err() {
            return Err(self.evaluate(request));
        }
        let grants = state
            .grants
            .values()
            .filter(|grant| {
                grant.grantee == request.presentation.principal
                    && !state.revoked_grants.contains(&grant.id)
                    && grant.valid_from <= now
                    && grant.expires_at.is_none_or(|expires| expires > now)
                    && grant.constraints.permits(request)
                    && grant
                        .permissions
                        .contains(&FederationPermission::ReadHistory)
                    && crate::stream_permission(request.operation)
                        .is_none_or(|permission| grant.permissions.contains(&permission))
            })
            .collect::<Vec<_>>();
        let mut allowed = Vec::new();
        for wanted in requested {
            for grant in &grants {
                if let Some(intersection) =
                    selection_intersection(wanted, &grant.selection, topology)
                    && !allowed.contains(&intersection)
                {
                    allowed.push(intersection);
                }
            }
        }
        if allowed.is_empty() {
            Err(deny(
                request,
                now,
                "empty_replication_intersection",
                "no requested replication scope is authorized",
            ))
        } else {
            Ok(ReplicationSelection::Scopes(allowed))
        }
    }
}

impl AccessPolicy for AuthorityEngine {
    fn authorize(&self, request: &AccessRequest) -> Result<(), String> {
        match self.decide(request) {
            AuthorizationDecision::Permit(_) => Ok(()),
            decision => Err(decision.public_message()),
        }
    }

    fn decide(&self, request: &AccessRequest) -> AuthorizationDecision {
        let binding = AuthorizationBinding::from_request(request);
        let decision = self.evaluate(request);
        if self
            .append(AuthorityRecord::DecisionAudited {
                binding,
                decision: decision.clone(),
            })
            .is_err()
        {
            deny(
                request,
                Utc::now(),
                "audit_not_durable",
                "authorization audit could not be made durable",
            )
        } else {
            decision
        }
    }

    fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    fn constrain_replication(
        &self,
        request: &AccessRequest,
        selection: &ReplicationSelection,
        topology: &ScopeTopology,
    ) -> Result<ReplicationSelection, AuthorizationDecision> {
        self.constrain_replication_inner(request, selection, topology)
    }
}

fn consumes_use(request: &AccessRequest) -> bool {
    request.authorization_phase == AuthorizationPhase::Effect
        || (request.authorization_phase == AuthorizationPhase::Admission
            && request.operation != AccessOperation::SubmitCommand)
}

fn normalized_claims(request: &AccessRequest) -> Vec<ResourceClaim> {
    if !request.resource_claims.is_empty() {
        return request.resource_claims.clone();
    }
    if !request.scope_selections.is_empty() {
        return request
            .scope_selections
            .iter()
            .cloned()
            .map(|selection| ResourceClaim {
                selection,
                kind: ResourceClaimKind::Primary,
                service_id: request.service_id.clone(),
                item_type: None,
                item_id: None,
            })
            .collect();
    }
    request
        .scope_id
        .iter()
        .cloned()
        .map(|scope| ResourceClaim::scope(scope, ResourceClaimKind::Primary))
        .collect()
}

fn validate_provenance(
    state: &AuthorityState,
    request: &AccessRequest,
    now: DateTime<Utc>,
) -> Result<(), AuthorizationExplanation> {
    let presentation = &request.presentation;
    if presentation.provenance.is_empty() {
        if presentation.principal != presentation.executor {
            return Err(explanation(
                "missing_provenance",
                "a different executor requires an immutable delegation chain",
            ));
        }
        return Ok(());
    }
    let mut current = &presentation.principal;
    for hop in &presentation.provenance {
        if &hop.delegator != current {
            return Err(explanation(
                "broken_provenance",
                "delegation chain does not preserve its originating principal",
            ));
        }
        let Some(delegation) = state.delegations.get(&hop.delegation_id) else {
            return Err(explanation(
                "unknown_delegation",
                "provenance references an unknown delegation",
            ));
        };
        if state.revoked_delegations.contains(&delegation.id)
            || delegation.expires_at.is_some_and(|expires| expires <= now)
            || delegation
                .max_uses
                .is_some_and(|maximum| state.delegation_uses.get(&delegation.id).copied().unwrap_or(0) >= maximum)
            || delegation.delegator != hop.delegator
            || delegation.delegate != hop.delegate
            || delegation.provenance_operation != hop.operation
            || (!delegation.operations.is_empty()
                && !delegation.operations.contains(&request.operation))
            || !request
                .application_capabilities
                .iter()
                .all(|id| delegation.capabilities.contains(id))
            || !delegation.constraints.permits(request)
            || normalized_claims(request).iter().any(|claim| {
                !delegation.selections.iter().any(|selection| {
                    selection_covers(selection, &claim.selection, request.topology.as_ref())
                })
            })
        {
            return Err(AuthorizationExplanation {
                code: "delegation_denied".to_owned(),
                message: "delegated executor exceeds or no longer holds its authority".to_owned(),
                grant_id: None,
                delegation_id: Some(delegation.id.clone()),
                obligation_id: None,
                constraint: Some("attenuation".to_owned()),
            });
        }
        current = &hop.delegate;
    }
    if current != &presentation.executor {
        return Err(explanation(
            "executor_not_last",
            "authenticated executor is not the final provenance hop",
        ));
    }
    Ok(())
}

fn validate_delegation_attenuation(
    state: &AuthorityState,
    child: &AuthorityDelegation,
    topology: Option<&ScopeTopology>,
) -> Result<(), AuthorityError> {
    let (owner, selections, operations, capabilities, constraints, expiry, max_uses) =
        match &child.parent {
            DelegationParent::Grant(id) => {
                let parent = state
                    .grants
                    .get(id)
                    .ok_or_else(|| AuthorityError::UnknownRecord(id.to_string()))?;
                (
                    &parent.grantee,
                    vec![parent.selection.clone()],
                    parent.operations.clone(),
                    parent.capabilities.clone(),
                    parent.constraints.clone(),
                    parent.expires_at,
                    parent.max_uses,
                )
            }
            DelegationParent::Delegation(id) => {
                let parent = state
                    .delegations
                    .get(id)
                    .ok_or_else(|| AuthorityError::UnknownRecord(id.to_string()))?;
                (
                    &parent.delegate,
                    parent.selections.clone(),
                    parent.operations.clone(),
                    parent.capabilities.clone(),
                    parent.constraints.clone(),
                    parent.expires_at,
                    parent.max_uses,
                )
            }
        };
    if owner != &child.delegator
        || child
            .selections
            .iter()
            .any(|selection| {
                !selections
                    .iter()
                    .any(|parent| selection_is_subset(selection, parent, topology))
            })
        || (!operations.is_empty()
            && (child.operations.is_empty()
                || child.operations.iter().any(|value| !operations.contains(value))))
        || child
            .capabilities
            .iter()
            .any(|value| !capabilities.contains(value))
        || !child.constraints.attenuates(&constraints)
        || match (child.expires_at, expiry) {
            (_, None) => false,
            (Some(child), Some(parent)) => child > parent,
            (None, Some(_)) => true,
        }
        || match (child.max_uses, max_uses) {
            (_, None) => false,
            (Some(child), Some(parent)) => child > parent,
            (None, Some(_)) => true,
        }
    {
        return Err(AuthorityError::InvalidAttenuation(
            "delegation broadens its parent authority".to_owned(),
        ));
    }
    Ok(())
}

fn selection_is_subset(
    child: &ScopeSelection,
    parent: &ScopeSelection,
    topology: Option<&ScopeTopology>,
) -> bool {
    match (child, parent) {
        (ScopeSelection::Exact(child), ScopeSelection::Exact(parent)) => child == parent,
        (ScopeSelection::Exact(child), ScopeSelection::Subtree(parent)) => {
            child == parent
                || topology.is_some_and(|value| {
                    value.knows(child)
                        && value.knows(parent)
                        && value.is_descendant_of(child, parent)
                })
        }
        (ScopeSelection::Subtree(child), ScopeSelection::Subtree(parent)) => {
            child == parent
                || topology.is_some_and(|value| {
                    value.knows(child)
                        && value.knows(parent)
                        && value.is_descendant_of(child, parent)
                })
        }
        (ScopeSelection::Subtree(_), ScopeSelection::Exact(_)) => false,
    }
}

fn selection_covers(
    grant: &ScopeSelection,
    wanted: &ScopeSelection,
    topology: Option<&ScopeTopology>,
) -> bool {
    match (grant, wanted) {
        (ScopeSelection::Exact(grant), ScopeSelection::Exact(wanted)) => grant == wanted,
        (ScopeSelection::Exact(_), ScopeSelection::Subtree(_)) => false,
        (ScopeSelection::Subtree(grant), ScopeSelection::Exact(wanted)) => {
            grant == wanted
                || topology.is_some_and(|topology| {
                    topology.knows(grant)
                        && topology.knows(wanted)
                        && topology.is_descendant_of(wanted, grant)
                })
        }
        (ScopeSelection::Subtree(grant), ScopeSelection::Subtree(wanted)) => {
            grant == wanted
                || topology.is_some_and(|topology| {
                    topology.knows(grant)
                        && topology.knows(wanted)
                        && topology.is_descendant_of(wanted, grant)
                })
        }
    }
}

fn selection_intersection(
    requested: &ScopeSelection,
    grant: &ScopeSelection,
    topology: &ScopeTopology,
) -> Option<ScopeSelection> {
    if selection_covers(grant, requested, Some(topology)) {
        return Some(requested.clone());
    }
    if selection_covers(requested, grant, Some(topology)) {
        return Some(grant.clone());
    }
    None
}

fn explanation(code: &str, message: &str) -> AuthorizationExplanation {
    AuthorizationExplanation {
        code: code.to_owned(),
        message: message.to_owned(),
        grant_id: None,
        delegation_id: None,
        obligation_id: None,
        constraint: None,
    }
}

fn report(
    request: &AccessRequest,
    now: DateTime<Utc>,
    explanations: Vec<AuthorizationExplanation>,
) -> AuthorizationReport {
    AuthorizationReport {
        evaluated_at: now,
        principal: request.presentation.principal.clone(),
        executor: request.presentation.executor.clone(),
        operation: request.operation,
        explanations,
    }
}

fn deny(
    request: &AccessRequest,
    now: DateTime<Utc>,
    code: &str,
    message: &str,
) -> AuthorizationDecision {
    deny_with(request, now, explanation(code, message))
}

fn deny_with(
    request: &AccessRequest,
    now: DateTime<Utc>,
    explanation: AuthorizationExplanation,
) -> AuthorizationDecision {
    let visibility = if explanation.code == "unbound_resource" {
        ResourceVisibility::Unbound
    } else {
        ResourceVisibility::Unauthorized
    };
    AuthorizationDecision::Deny(DenyDecision {
        report: report(request, now, vec![explanation]),
        visibility,
    })
}

fn challenge(
    request: &AccessRequest,
    now: DateTime<Utc>,
    challenge: AuthorityChallenge,
) -> AuthorizationDecision {
    let obligation_id = challenge.obligation_id.clone();
    AuthorizationDecision::Challenge {
        challenge,
        report: report(
            request,
            now,
            vec![AuthorizationExplanation {
                code: "obligation_unsatisfied".to_owned(),
                message: "authorization requires an approval or external proof".to_owned(),
                grant_id: None,
                delegation_id: None,
                obligation_id: Some(obligation_id),
                constraint: None,
            }],
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccessRequest, ScopeTopology};

    fn principal(value: &str, kind: PrincipalKind) -> Principal {
        Principal::new(PrincipalId::new(value), kind)
    }

    fn request(person: &Principal, scope: &str) -> AccessRequest {
        let presentation = AuthorityPresentation::direct(person.clone());
        AccessRequest::scoped(
            presentation.executor.id.clone(),
            presentation,
            AccessOperation::ReadItems,
            ScopeId::new(scope),
        )
    }

    #[test]
    fn defaults_to_deny_and_requires_every_resource_claim() {
        let engine = AuthorityEngine::default();
        let person = principal("person:one", PrincipalKind::Person);
        let mut access = request(&person, "project:p1");
        assert!(!engine.decide(&access).is_permit());
        engine
            .issue_grant(AuthorityGrant::scope(
                principal("person:owner", PrincipalKind::Person),
                person.clone(),
                ScopeSelection::Exact(ScopeId::new("project:p1")),
                vec![FederationPermission::ReadState],
            ))
            .unwrap();
        access.resource_claims.push(ResourceClaim::scope(
            ScopeId::new("scene:s2"),
            ResourceClaimKind::Referenced,
        ));
        assert!(!engine.decide(&access).is_permit());
    }

    #[test]
    fn delegation_is_attenuating_and_preserves_original_principal() {
        let engine = AuthorityEngine::default();
        let owner = principal("person:owner", PrincipalKind::Person);
        let agent = principal("agent:writer", PrincipalKind::Agent);
        let mut grant = AuthorityGrant::scope(
            owner.clone(),
            owner.clone(),
            ScopeSelection::Exact(ScopeId::new("project:p1")),
            vec![FederationPermission::Write],
        );
        grant.operations = vec![AccessOperation::SubmitCommand];
        engine.issue_grant(grant.clone()).unwrap();
        let delegation = AuthorityDelegation {
            id: DelegationId::random(),
            parent: DelegationParent::Grant(grant.id),
            delegator: owner.clone(),
            delegate: agent.clone(),
            provenance_operation: ProvenanceOperation::AgentInvocation {
                agent_id: "writer".to_owned(),
            },
            selections: vec![ScopeSelection::Exact(ScopeId::new("project:p1"))],
            operations: vec![AccessOperation::SubmitCommand],
            capabilities: Vec::new(),
            constraints: AuthorityConstraints {
                commands: vec!["RenameProject".to_owned()],
                ..AuthorityConstraints::default()
            },
            expires_at: None,
            max_uses: None,
        };
        engine.issue_delegation(delegation.clone()).unwrap();
        let presentation = AuthorityPresentation::direct(owner.clone()).forward(ProvenanceHop {
            delegation_id: delegation.id,
            delegator: owner.clone(),
            delegate: agent.clone(),
            operation: ProvenanceOperation::AgentInvocation {
                agent_id: "writer".to_owned(),
            },
        });
        let mut access = AccessRequest::scoped(
            agent.id.clone(),
            presentation,
            AccessOperation::SubmitCommand,
            ScopeId::new("project:p1"),
        );
        access.command_type = Some("DeleteProject".to_owned());
        assert!(!engine.decide(&access).is_permit());
        access.command_type = Some("RenameProject".to_owned());
        assert!(engine.decide(&access).is_permit());
        assert_eq!(
            AuthorizationBinding::from_request(&access).principal,
            owner
        );
    }

    #[test]
    fn approval_cannot_be_replayed_for_other_arguments() {
        let engine = AuthorityEngine::default();
        let owner = principal("person:owner", PrincipalKind::Person);
        let approver = principal("person:approver", PrincipalKind::Person);
        let obligation = Obligation {
            id: ObligationId::new("review"),
            challenge_kind: "human_review".to_owned(),
            prompt: "Approve this exact effect".to_owned(),
            approvers: vec![approver.clone()],
            approval_lifetime_seconds: 60,
            approval_use_count: 1,
        };
        engine.register_obligation(obligation.clone()).unwrap();
        let mut grant = AuthorityGrant::scope(
            owner.clone(),
            owner.clone(),
            ScopeSelection::Exact(ScopeId::new("project:p1")),
            vec![FederationPermission::Write],
        );
        grant.obligations.push(obligation.id);
        engine.issue_grant(grant).unwrap();
        let mut access = AccessRequest::scoped(
            owner.id.clone(),
            AuthorityPresentation::direct(owner),
            AccessOperation::SubmitCommand,
            ScopeId::new("project:p1"),
        );
        access.arguments_digest = Some("args:a".to_owned());
        let AuthorizationDecision::Challenge { challenge, .. } = engine.decide(&access) else {
            panic!("expected challenge");
        };
        let approval = engine.approve(&challenge.id, approver, true).unwrap();
        access.presentation.approvals.push(approval.id);
        assert!(engine.decide(&access).is_permit());
        access.arguments_digest = Some("args:b".to_owned());
        assert!(matches!(
            engine.decide(&access),
            AuthorizationDecision::Challenge { .. }
        ));
    }

    #[test]
    fn replication_is_intersected_without_partial_atomic_visibility() {
        let engine = AuthorityEngine::default();
        let person = principal("person:reader", PrincipalKind::Person);
        engine
            .issue_grant(AuthorityGrant::scope(
                principal("person:owner", PrincipalKind::Person),
                person.clone(),
                ScopeSelection::Exact(ScopeId::new("scene:s1")),
                vec![FederationPermission::ReadHistory],
            ))
            .unwrap();
        let mut access = request(&person, "scene:s1");
        access.operation = AccessOperation::ReadHistory;
        access.scope_selections = vec![
            ScopeSelection::Exact(ScopeId::new("scene:s1")),
            ScopeSelection::Exact(ScopeId::new("scene:s2")),
        ];
        access.resource_claims.clear();
        let selected = engine
            .constrain_replication(
                &access,
                &ReplicationSelection::Scopes(access.scope_selections.clone()),
                &ScopeTopology::default(),
            )
            .unwrap();
        assert_eq!(
            selected,
            ReplicationSelection::Scopes(vec![ScopeSelection::Exact(ScopeId::new("scene:s1"))])
        );
    }
}
