//! Durable, fail-closed authority for Myko nodes.
//!
//! Every authoritative fact is an ordinary item in [`AuthorityService`]. The
//! evaluator reads only the local node's projection. Decisions with durable
//! effects, including bounded-use consumption, are committed as one
//! service-atomic Myko command before returning.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Duration, Utc};
use hyphae::{Cell, CellImmutable, CellMutable, Mutable as _, SubscriptionGuard};
use myko::{
    AppError, ApplicationHost, CommandContext, CommandError, CommandHandler, MykoApplication,
    myko_view,
    view::{ViewBuildArgs, ViewHandler},
};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy, AccessTarget, ApplicationCapability,
    ApprovalDecision, ApprovalId, AuthorityChallenge, AuthorityConstraints, AuthorityDelegation,
    AuthorityGrant, AuthorityGrantId, AuthorityLease, AuthorityLeaseRequest, AuthorityPresentation,
    AuthorityRealmId as AuthorityRealmKey, AuthorizationBinding, AuthorizationDecision,
    AuthorizationExplanation, AuthorizationReport, CapabilityId, ChallengeId, CommandState,
    DelegationId, DenyDecision, FederationPermission, LeaseId, MykoService as _, Node, Obligation,
    PermitDecision, Principal, PrincipalId, PrincipalKind, ReplicationSelection, ResourceClaim,
    ResourceClaimKind, ResourceVisibility, ScopeId, ScopeSelection, ScopeTopology,
};
use myko_items::{myko_command, myko_item, myko_service};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[allow(clippy::redundant_pub_crate)]
mod domain;
pub use domain::*;
use domain::{administration_claim, authority_presentation, realm_item_id, record_id};

#[allow(clippy::redundant_pub_crate)]
mod commands;
use commands::{
    BootstrapRealm, DecideChallenge, EvaluateAuthority, PutCapability, PutDelegation, PutObligation,
};
pub use commands::{IssueAuthorityGrant, RevocationKind, RevokeAuthorityFact};

#[allow(clippy::redundant_pub_crate)]
mod facts;
use facts::{
    AuthorityFactSources, EvaluationOutcome, EvaluationState, is_stream, load_state,
    permission_for, requires_durable_evaluation,
};

#[allow(clippy::redundant_pub_crate)]
mod evaluator;
use evaluator::{deny, evaluate};

mod policy;
pub use policy::*;

#[cfg(test)]
#[allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::redundant_clone,
    clippy::too_many_lines,
    clippy::unwrap_used
)]
mod tests;
