//! Native authority administration backed by Myko's durable authority service.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use hyphae::MapDiff;
use myko_authority::{AuthorityGrantsView, GrantRecord, RevocationKind};
use myko_federation::{
    AccessOperation, AuthorityConstraints, AuthorityGrant, AuthorityGrantId, CapabilityId,
    FederationPermission, LiveCollectionRevision, LogPosition, ObligationId, Principal,
    PrincipalId, PrincipalKind, ScopeId, ScopeSelection,
};

use crate::BlockingCollectionSubscription;

use super::federation::parse_node_id;
use super::{
    MykoFederationError, NativeAuthorityAccess, NativeAuthorityContext, authority_error,
    transport_error,
};

/// Principal kinds accepted by Myko's generated native authority API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum MykoPrincipalKind {
    Person,
    Node,
    Agent,
    Command,
    Task,
    Tool,
    Service,
}

/// One transport-neutral authority principal.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MykoPrincipal {
    pub id: String,
    pub kind: MykoPrincipalKind,
}

/// One exact scope or complete nested scope subtree.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MykoScopeSelection {
    pub scope_id: String,
    pub include_descendants: bool,
}

/// Framework data permissions which can be carried by a native grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum MykoFederationPermission {
    ReadState,
    ReadHistory,
    Subscribe,
    Write,
    Reshare,
    Admin,
}

/// Transport-neutral operations which can be carried by a native grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum MykoAccessOperation {
    ReadHistory,
    ReadItems,
    FollowItems,
    FollowHandler,
    FollowHistory,
    SubscribeLive,
    SubmitCommand,
    ReadCommand,
    ReadCommands,
    WatchCommand,
    WatchCommands,
    CancelCommand,
    ApproveAuthority,
    AdministerAuthority,
    DelegateAuthority,
}

/// Authority fact kinds accepted by the generic revocation command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum MykoRevocationKind {
    Grant,
    Delegation,
    Obligation,
    Capability,
}

/// Optional attenuation applied while evaluating one authority grant.
#[derive(Debug, Clone, Default, PartialEq, Eq, uniffi::Record)]
pub struct MykoAuthorityConstraints {
    pub service_ids: Vec<String>,
    pub commands: Vec<String>,
    pub item_types: Vec<String>,
    pub max_lease_seconds: Option<u64>,
    pub allow_offline: bool,
}

/// Complete immutable grant returned by a live native authority view.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MykoAuthorityGrant {
    pub id: String,
    pub realm_id: String,
    pub grantor: MykoPrincipal,
    pub grantee: MykoPrincipal,
    pub selection: MykoScopeSelection,
    pub permissions: Vec<MykoFederationPermission>,
    pub operations: Vec<MykoAccessOperation>,
    pub capability_ids: Vec<String>,
    pub constraints: MykoAuthorityConstraints,
    pub obligation_ids: Vec<String>,
    pub valid_from_unix_millis: i64,
    pub expires_at_unix_millis: Option<i64>,
    pub max_uses: Option<u64>,
}

/// Durable grant record, including revocation rather than hiding it as removal.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MykoAuthorityGrantRecord {
    pub grant: MykoAuthorityGrant,
    pub revoked_at_unix_millis: Option<i64>,
}

/// Authenticated input for one immutable authority grant.
///
/// Myko assigns its identity, realm, grantor, and validity start. Applications
/// supply only the grantee and the typed authority being granted.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MykoAuthorityGrantInput {
    pub grantee: MykoPrincipal,
    pub selection: MykoScopeSelection,
    pub permissions: Vec<MykoFederationPermission>,
    pub operations: Vec<MykoAccessOperation>,
    pub capability_ids: Vec<String>,
    pub constraints: MykoAuthorityConstraints,
    pub obligation_ids: Vec<String>,
    pub expires_at_unix_millis: Option<i64>,
    pub max_uses: Option<u64>,
}

/// One lossless authority-grant collection revision.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoAuthorityGrantsUpdate {
    pub lifecycle: String,
    pub reason: Option<String>,
    pub reset: bool,
    pub upserts: Vec<MykoAuthorityGrantRecord>,
    pub removed_grant_ids: Vec<String>,
}

/// Long-lived grant-state subscription for one authority realm.
#[derive(uniffi::Object)]
pub struct MykoAuthorityGrantsSubscription {
    subscription: BlockingCollectionSubscription<GrantRecord, LogPosition>,
}

/// Reusable generated authority surface for a composed native Myko node.
#[derive(uniffi::Object)]
pub struct MykoAuthority {
    application: Arc<dyn NativeAuthorityAccess>,
}

impl MykoAuthority {
    /// Binds native authority administration to an application's active node.
    #[must_use]
    pub fn new(application: Arc<dyn NativeAuthorityAccess>) -> Arc<Self> {
        Arc::new(Self { application })
    }

    fn context(&self) -> Result<NativeAuthorityContext, MykoFederationError> {
        self.application.authority_context()
    }

    fn grant_subscription(
        &self,
        source_node: myko_federation::NodeId,
        realm_id: myko_federation::AuthorityRealmId,
    ) -> Result<Arc<MykoAuthorityGrantsSubscription>, MykoFederationError> {
        let subscription = self
            .application
            .application()?
            .watch_view_live(&AuthorityGrantsView {
                source_node,
                realm_id,
            })
            .map_err(|error| authority_error(&error))?;
        Ok(Arc::new(MykoAuthorityGrantsSubscription {
            subscription: BlockingCollectionSubscription::new(subscription.clone(), &subscription),
        }))
    }
}

#[uniffi::export]
impl MykoAuthority {
    /// Watches the active node's own configured authority realm.
    ///
    /// Native application code does not need to duplicate the node identity or
    /// application-selected realm at the foreign-language boundary.
    ///
    /// # Errors
    ///
    /// Returns an error while the node or its authority context is unavailable.
    pub fn subscribe_local_grants(
        &self,
    ) -> Result<Arc<MykoAuthorityGrantsSubscription>, MykoFederationError> {
        let application = self.application.application()?;
        let context = self.context()?;
        self.grant_subscription(application.node_id(), context.realm_id)
    }

    /// Watches every durable grant record for one source node and realm.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid node ID, inactive node, unavailable
    /// source, or authority view which cannot open.
    #[allow(clippy::needless_pass_by_value)] // UniFFI exports owned Swift strings.
    pub fn subscribe_grants(
        &self,
        source_node_id: String,
        realm_id: String,
    ) -> Result<Arc<MykoAuthorityGrantsSubscription>, MykoFederationError> {
        let source_node = parse_node_id(&source_node_id)?;
        self.grant_subscription(
            source_node,
            myko_federation::AuthorityRealmId::new(realm_id),
        )
    }

    /// Issues one immutable grant as the application-authenticated authority.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed input, an inactive node, or a rejected
    /// durable authority command.
    pub fn issue_grant(
        &self,
        input: MykoAuthorityGrantInput,
    ) -> Result<String, MykoFederationError> {
        let context = self.context()?;
        let grant_id = AuthorityGrantId::random();
        let expires_at = input
            .expires_at_unix_millis
            .map(parse_timestamp)
            .transpose()?;
        let grant = AuthorityGrant {
            id: grant_id.clone(),
            realm_id: context.realm_id.clone(),
            grantor: context.presentation.principal.clone(),
            grantee: input.grantee.into(),
            selection: input.selection.into(),
            permissions: input.permissions.into_iter().map(Into::into).collect(),
            operations: input.operations.into_iter().map(Into::into).collect(),
            capabilities: input
                .capability_ids
                .into_iter()
                .map(CapabilityId::new)
                .collect(),
            constraints: input.constraints.into(),
            obligations: input
                .obligation_ids
                .into_iter()
                .map(ObligationId::new)
                .collect(),
            valid_from: Utc::now(),
            expires_at,
            max_uses: input.max_uses,
        };
        context
            .policy
            .issue_grant(context.authenticated, context.presentation, grant)
            .map_err(|error| authority_error(&error))?;
        Ok(grant_id.to_string())
    }

    /// Revokes one durable authority fact as the authenticated administrator.
    ///
    /// # Errors
    ///
    /// Returns an error while the node is inactive or the durable revocation
    /// command is rejected.
    pub fn revoke(&self, kind: MykoRevocationKind, id: String) -> Result<(), MykoFederationError> {
        let context = self.context()?;
        context
            .policy
            .revoke(context.authenticated, context.presentation, kind.into(), id)
            .map_err(|error| authority_error(&error))
    }
}

crate::export_blocking_collection_subscription! {
    MykoAuthorityGrantsSubscription => MykoAuthorityGrantsUpdate,
    field = subscription,
    error = MykoFederationError,
    transport_error = transport_error,
    map = |revision, _owner| Ok(authority_grants_update(revision)),
}

fn authority_grants_update(
    revision: LiveCollectionRevision<GrantRecord, LogPosition>,
) -> MykoAuthorityGrantsUpdate {
    let (lifecycle, reason) = crate::project_subscription_liveness(&revision.state.liveness);
    let mut reset = false;
    let mut changes = BTreeMap::new();
    if let Some(diff) = revision.diff {
        project_grant_diff(diff, &mut reset, &mut changes);
    }
    let mut upserts = Vec::new();
    let mut removed_grant_ids = Vec::new();
    for (grant_id, change) in changes {
        match change {
            Some(grant) => upserts.push(grant_record(grant)),
            None => removed_grant_ids.push(grant_id),
        }
    }
    upserts.sort_by(|left, right| left.grant.id.cmp(&right.grant.id));
    MykoAuthorityGrantsUpdate {
        lifecycle,
        reason,
        reset,
        upserts,
        removed_grant_ids,
    }
}

fn project_grant_diff(
    diff: MapDiff<Arc<str>, Arc<GrantRecord>>,
    reset: &mut bool,
    changes: &mut BTreeMap<String, Option<GrantRecord>>,
) {
    match diff {
        MapDiff::Initial { entries } => {
            *reset = true;
            changes.clear();
            for (_, record) in entries {
                changes.insert(record.grant.id.to_string(), Some(record.as_ref().clone()));
            }
        }
        MapDiff::Insert { value, .. } => {
            changes.insert(value.grant.id.to_string(), Some(value.as_ref().clone()));
        }
        MapDiff::Remove { old_value, .. } => {
            changes.insert(old_value.grant.id.to_string(), None);
        }
        MapDiff::Update {
            old_value,
            new_value,
            ..
        } => {
            changes.insert(old_value.grant.id.to_string(), None);
            changes.insert(
                new_value.grant.id.to_string(),
                Some(new_value.as_ref().clone()),
            );
        }
        MapDiff::Batch { changes: batch } => {
            for diff in batch {
                project_grant_diff(diff, reset, changes);
            }
        }
    }
}

fn grant_record(record: GrantRecord) -> MykoAuthorityGrantRecord {
    MykoAuthorityGrantRecord {
        grant: record.grant.into(),
        revoked_at_unix_millis: record.revoked_at.map(|at| at.timestamp_millis()),
    }
}

fn parse_timestamp(value: i64) -> Result<DateTime<Utc>, MykoFederationError> {
    DateTime::from_timestamp_millis(value)
        .ok_or_else(|| authority_error(&format!("invalid Unix timestamp in milliseconds: {value}")))
}

impl From<MykoPrincipal> for Principal {
    fn from(value: MykoPrincipal) -> Self {
        Self::new(PrincipalId::new(value.id), value.kind.into())
    }
}

impl From<Principal> for MykoPrincipal {
    fn from(value: Principal) -> Self {
        Self {
            id: value.id.to_string(),
            kind: value.kind.into(),
        }
    }
}

impl From<MykoPrincipalKind> for PrincipalKind {
    fn from(value: MykoPrincipalKind) -> Self {
        match value {
            MykoPrincipalKind::Person => Self::Person,
            MykoPrincipalKind::Node => Self::Node,
            MykoPrincipalKind::Agent => Self::Agent,
            MykoPrincipalKind::Command => Self::Command,
            MykoPrincipalKind::Task => Self::Task,
            MykoPrincipalKind::Tool => Self::Tool,
            MykoPrincipalKind::Service => Self::Service,
        }
    }
}

impl From<PrincipalKind> for MykoPrincipalKind {
    fn from(value: PrincipalKind) -> Self {
        match value {
            PrincipalKind::Person => Self::Person,
            PrincipalKind::Node => Self::Node,
            PrincipalKind::Agent => Self::Agent,
            PrincipalKind::Command => Self::Command,
            PrincipalKind::Task => Self::Task,
            PrincipalKind::Tool => Self::Tool,
            PrincipalKind::Service => Self::Service,
        }
    }
}

impl From<MykoScopeSelection> for ScopeSelection {
    fn from(value: MykoScopeSelection) -> Self {
        let scope = ScopeId::new(value.scope_id);
        if value.include_descendants {
            Self::Subtree(scope)
        } else {
            Self::Exact(scope)
        }
    }
}

impl From<ScopeSelection> for MykoScopeSelection {
    fn from(value: ScopeSelection) -> Self {
        match value {
            ScopeSelection::Exact(scope_id) => Self {
                scope_id: scope_id.to_string(),
                include_descendants: false,
            },
            ScopeSelection::Subtree(scope_id) => Self {
                scope_id: scope_id.to_string(),
                include_descendants: true,
            },
        }
    }
}

impl From<MykoFederationPermission> for FederationPermission {
    fn from(value: MykoFederationPermission) -> Self {
        match value {
            MykoFederationPermission::ReadState => Self::ReadState,
            MykoFederationPermission::ReadHistory => Self::ReadHistory,
            MykoFederationPermission::Subscribe => Self::Subscribe,
            MykoFederationPermission::Write => Self::Write,
            MykoFederationPermission::Reshare => Self::Reshare,
            MykoFederationPermission::Admin => Self::Admin,
        }
    }
}

impl From<FederationPermission> for MykoFederationPermission {
    fn from(value: FederationPermission) -> Self {
        match value {
            FederationPermission::ReadState => Self::ReadState,
            FederationPermission::ReadHistory => Self::ReadHistory,
            FederationPermission::Subscribe => Self::Subscribe,
            FederationPermission::Write => Self::Write,
            FederationPermission::Reshare => Self::Reshare,
            FederationPermission::Admin => Self::Admin,
        }
    }
}

impl From<MykoAccessOperation> for AccessOperation {
    fn from(value: MykoAccessOperation) -> Self {
        match value {
            MykoAccessOperation::ReadHistory => Self::ReadHistory,
            MykoAccessOperation::ReadItems => Self::ReadItems,
            MykoAccessOperation::FollowItems => Self::FollowItems,
            MykoAccessOperation::FollowHandler => Self::FollowHandler,
            MykoAccessOperation::FollowHistory => Self::FollowHistory,
            MykoAccessOperation::SubscribeLive => Self::SubscribeLive,
            MykoAccessOperation::SubmitCommand => Self::SubmitCommand,
            MykoAccessOperation::ReadCommand => Self::ReadCommand,
            MykoAccessOperation::ReadCommands => Self::ReadCommands,
            MykoAccessOperation::WatchCommand => Self::WatchCommand,
            MykoAccessOperation::WatchCommands => Self::WatchCommands,
            MykoAccessOperation::CancelCommand => Self::CancelCommand,
            MykoAccessOperation::ApproveAuthority => Self::ApproveAuthority,
            MykoAccessOperation::AdministerAuthority => Self::AdministerAuthority,
            MykoAccessOperation::DelegateAuthority => Self::DelegateAuthority,
        }
    }
}

impl From<AccessOperation> for MykoAccessOperation {
    fn from(value: AccessOperation) -> Self {
        match value {
            AccessOperation::ReadHistory => Self::ReadHistory,
            AccessOperation::ReadItems => Self::ReadItems,
            AccessOperation::FollowItems => Self::FollowItems,
            AccessOperation::FollowHandler => Self::FollowHandler,
            AccessOperation::FollowHistory => Self::FollowHistory,
            AccessOperation::SubscribeLive => Self::SubscribeLive,
            AccessOperation::SubmitCommand => Self::SubmitCommand,
            AccessOperation::ReadCommand => Self::ReadCommand,
            AccessOperation::ReadCommands => Self::ReadCommands,
            AccessOperation::WatchCommand => Self::WatchCommand,
            AccessOperation::WatchCommands => Self::WatchCommands,
            AccessOperation::CancelCommand => Self::CancelCommand,
            AccessOperation::ApproveAuthority => Self::ApproveAuthority,
            AccessOperation::AdministerAuthority => Self::AdministerAuthority,
            AccessOperation::DelegateAuthority => Self::DelegateAuthority,
        }
    }
}

impl From<MykoRevocationKind> for RevocationKind {
    fn from(value: MykoRevocationKind) -> Self {
        match value {
            MykoRevocationKind::Grant => Self::Grant,
            MykoRevocationKind::Delegation => Self::Delegation,
            MykoRevocationKind::Obligation => Self::Obligation,
            MykoRevocationKind::Capability => Self::Capability,
        }
    }
}

impl From<MykoAuthorityConstraints> for AuthorityConstraints {
    fn from(value: MykoAuthorityConstraints) -> Self {
        Self {
            services: value
                .service_ids
                .into_iter()
                .map(myko_federation::ServiceId::new)
                .collect(),
            commands: value.commands,
            item_types: value.item_types,
            max_lease_seconds: value.max_lease_seconds,
            allow_offline: value.allow_offline,
        }
    }
}

impl From<AuthorityConstraints> for MykoAuthorityConstraints {
    fn from(value: AuthorityConstraints) -> Self {
        Self {
            service_ids: value
                .services
                .into_iter()
                .map(|service| service.to_string())
                .collect(),
            commands: value.commands,
            item_types: value.item_types,
            max_lease_seconds: value.max_lease_seconds,
            allow_offline: value.allow_offline,
        }
    }
}

impl From<AuthorityGrant> for MykoAuthorityGrant {
    fn from(value: AuthorityGrant) -> Self {
        Self {
            id: value.id.to_string(),
            realm_id: value.realm_id.to_string(),
            grantor: value.grantor.into(),
            grantee: value.grantee.into(),
            selection: value.selection.into(),
            permissions: value.permissions.into_iter().map(Into::into).collect(),
            operations: value.operations.into_iter().map(Into::into).collect(),
            capability_ids: value
                .capabilities
                .into_iter()
                .map(|capability| capability.to_string())
                .collect(),
            constraints: value.constraints.into(),
            obligation_ids: value
                .obligations
                .into_iter()
                .map(|obligation| obligation.to_string())
                .collect(),
            valid_from_unix_millis: value.valid_from.timestamp_millis(),
            expires_at_unix_millis: value.expires_at.map(|at| at.timestamp_millis()),
            max_uses: value.max_uses,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_grant_projection_preserves_typed_contract() {
        let valid_from = Utc::now();
        let expires_at = valid_from + chrono::Duration::seconds(1);
        let valid_from_unix_millis = valid_from.timestamp_millis();
        let expires_at_unix_millis = expires_at.timestamp_millis();
        let grant = AuthorityGrant {
            id: AuthorityGrantId::new("grant:a"),
            realm_id: myko_federation::AuthorityRealmId::new("main"),
            grantor: Principal::node(PrincipalId::new("node:owner")),
            grantee: Principal::new(PrincipalId::new("agent:fern"), PrincipalKind::Agent),
            selection: ScopeSelection::Subtree(ScopeId::new("scope:agents")),
            permissions: vec![FederationPermission::ReadState],
            operations: vec![AccessOperation::FollowHandler],
            capabilities: vec![CapabilityId::new("forrest.agent.observe")],
            constraints: AuthorityConstraints {
                services: vec![myko_federation::ServiceId::new("forrest.agents")],
                commands: vec!["forrest.agent.send-message".to_owned()],
                item_types: vec!["forrest.agent-message".to_owned()],
                max_lease_seconds: Some(30),
                allow_offline: true,
            },
            obligations: vec![ObligationId::new("approval:a")],
            valid_from,
            expires_at: Some(expires_at),
            max_uses: Some(3),
        };

        let projected = MykoAuthorityGrant::from(grant);
        assert_eq!(projected.id, "grant:a");
        assert_eq!(projected.realm_id, "main");
        assert_eq!(projected.grantee.kind, MykoPrincipalKind::Agent);
        assert!(projected.selection.include_descendants);
        assert_eq!(
            projected.permissions,
            vec![MykoFederationPermission::ReadState]
        );
        assert_eq!(
            projected.operations,
            vec![MykoAccessOperation::FollowHandler]
        );
        assert_eq!(projected.valid_from_unix_millis, valid_from_unix_millis);
        assert_eq!(
            projected.expires_at_unix_millis,
            Some(expires_at_unix_millis)
        );
        assert_eq!(projected.max_uses, Some(3));
        assert_eq!(
            projected.constraints,
            MykoAuthorityConstraints {
                service_ids: vec!["forrest.agents".to_owned()],
                commands: vec!["forrest.agent.send-message".to_owned()],
                item_types: vec!["forrest.agent-message".to_owned()],
                max_lease_seconds: Some(30),
                allow_offline: true,
            }
        );
    }
}
