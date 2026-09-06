//! Native authority administration backed by Myko's durable authority service.

use std::collections::BTreeMap;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

use chrono::{DateTime, Utc};
use hyphae::{Signal, SubscriptionGuard, Watchable as _};
use myko_authority::{AuthorityGrantsView, GrantRecord, RevocationKind};
use myko_federation::{
    AccessOperation, AuthorityConstraints, AuthorityGrant, AuthorityGrantId, CapabilityId,
    FederationPermission, LiveSubscription, LiveSubscriptionState, LogPosition, ObligationId,
    Principal, PrincipalId, PrincipalKind, ScopeId, ScopeSelection,
};

use super::federation::parse_node_id;
use super::{
    MykoFederationError, NativeAuthorityAccess, NativeAuthorityContext, authority_error,
    transport_error,
};

type RetainedGrantRows = BTreeMap<Arc<str>, Arc<dyn myko::item::AnyItem>>;
type RetainedGrantState = LiveSubscriptionState<RetainedGrantRows, LogPosition>;

struct RetainedGrantPublicationQueue {
    state: Arc<RetainedGrantQueueState>,
    changes: Mutex<Option<SubscriptionGuard>>,
    cancelled: AtomicBool,
}

struct RetainedGrantQueueState {
    pending: Mutex<RetainedGrantPending>,
    changed: Condvar,
}

#[derive(Default)]
struct RetainedGrantPending {
    next: Option<RetainedGrantState>,
    cancelled: bool,
}

impl RetainedGrantPublicationQueue {
    fn new(subscription: &LiveSubscription<RetainedGrantRows, LogPosition>) -> Self {
        let state = Arc::new(RetainedGrantQueueState {
            pending: Mutex::new(RetainedGrantPending::default()),
            changed: Condvar::new(),
        });
        let callback_state = Arc::clone(&state);
        let initial = AtomicBool::new(true);
        let changes = subscription.publication().subscribe(move |signal| {
            if let Signal::Value(publication) = signal
                && !initial.swap(false, Ordering::AcqRel)
            {
                let should_notify = {
                    let mut pending = callback_state
                        .pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if pending.cancelled {
                        false
                    } else {
                        pending.next = Some(publication.state.clone());
                        true
                    }
                };
                if should_notify {
                    callback_state.changed.notify_all();
                }
            }
        });
        Self {
            state,
            changes: Mutex::new(Some(changes)),
            cancelled: AtomicBool::new(false),
        }
    }

    fn next(&self) -> Result<RetainedGrantState, crate::SubscriptionCancelled> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(crate::SubscriptionCancelled);
        }
        let mut pending = self
            .state
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if pending.cancelled {
                return Err(crate::SubscriptionCancelled);
            }
            if let Some(state) = pending.next.take() {
                return Ok(state);
            }
            pending = self
                .state
                .changed
                .wait(pending)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn discard_pending(&self) {
        if let Ok(mut pending) = self.state.pending.lock() {
            pending.next = None;
        }
    }

    fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Ok(mut changes) = self.changes.lock() {
            drop(changes.take());
        }
        {
            let mut pending = self
                .state
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.cancelled = true;
            pending.next = None;
        }
        self.state.changed.notify_all();
    }
}

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
    subscription: LiveSubscription<RetainedGrantRows, LogPosition>,
    publications: RetainedGrantPublicationQueue,
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
        realm_id: &myko_federation::AuthorityRealmId,
    ) -> Result<Arc<MykoAuthorityGrantsSubscription>, MykoFederationError> {
        let application = self.application.application()?;
        let request = Arc::new(myko::core::request::RequestContext::internal(
            Arc::from("native-authority-grants"),
            application.server().host_id,
            "native-authority",
        ));
        let output = application
            .server()
            .handler_registry
            .open_federated_view(
                <AuthorityGrantsView as myko::view::ViewIdStatic>::view_id_static().as_ref(),
                serde_json::to_value(AuthorityGrantsView {
                    source_node,
                    realm_id: realm_id.clone(),
                })
                .map_err(|error| authority_error(&error))?,
                request,
                Arc::clone(application.server()),
                myko::server::federated_source::FederatedRequest {
                    source_node: Some(source_node),
                    scope_id: Some(myko_authority::authority_realm_scope(realm_id)),
                },
            )
            .map_err(|error| authority_error(&error))?;
        let myko::view::RegisteredViewOutput::RetainedPublication(subscription) = output else {
            return Err(authority_error(
                "authority grants view did not return a retained publication",
            ));
        };
        let publications = RetainedGrantPublicationQueue::new(&subscription);
        Ok(Arc::new(MykoAuthorityGrantsSubscription {
            subscription,
            publications,
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
        self.grant_subscription(application.node_id(), &context.realm_id)
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
        let realm_id = myko_federation::AuthorityRealmId::new(realm_id);
        self.grant_subscription(source_node, &realm_id)
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

#[uniffi::export]
impl MykoAuthorityGrantsSubscription {
    /// Returns a complete retained grant snapshot plus the latest lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an authority bridge error if the retained publication contains a
    /// row with the wrong registered item type.
    pub fn current(&self) -> Result<MykoAuthorityGrantsUpdate, MykoFederationError> {
        self.publications.discard_pending();
        authority_grants_snapshot_update(self.subscription.current())
    }

    /// Waits for the next retained grant snapshot.
    ///
    /// # Errors
    ///
    /// Returns an authority bridge error when the stream is cancelled or if the
    /// retained publication contains a row with the wrong registered item type.
    pub fn next(&self) -> Result<MykoAuthorityGrantsUpdate, MykoFederationError> {
        self.publications
            .next()
            .map_err(|error| transport_error(&error))
            .and_then(authority_grants_snapshot_update)
    }

    /// Cancels the subscription and wakes a blocked [`Self::next`] call.
    pub fn cancel(&self) {
        self.publications.cancel();
    }
}

fn authority_grants_snapshot_update(
    state: RetainedGrantState,
) -> Result<MykoAuthorityGrantsUpdate, MykoFederationError> {
    let (lifecycle, reason) = crate::project_subscription_liveness(&state.liveness);
    let mut upserts = Vec::new();
    if let Some(rows) = state.value {
        for value in rows.into_values() {
            let record = value
                .as_any()
                .downcast_ref::<GrantRecord>()
                .ok_or_else(|| {
                    authority_error("authority grants publication contained a non-grant row")
                })?;
            upserts.push(grant_record(record.clone()));
        }
    }
    upserts.sort_by(|left, right| left.grant.id.cmp(&right.grant.id));
    Ok(MykoAuthorityGrantsUpdate {
        lifecycle,
        reason,
        reset: true,
        upserts,
        removed_grant_ids: Vec::new(),
    })
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

    #[test]
    fn authority_grant_subscription_cancel_wakes_blocked_next() {
        let (_writer, live) = myko_federation::live_subscription(LiveSubscriptionState {
            value: Some(RetainedGrantRows::new()),
            through: None,
            liveness: myko_federation::SubscriptionLiveness::Current,
        });
        let subscription = Arc::new(MykoAuthorityGrantsSubscription {
            subscription: live.clone(),
            publications: RetainedGrantPublicationQueue::new(&live),
        });
        let blocked = Arc::clone(&subscription);
        let (started_tx, started_rx) = flume::bounded(1);
        let (done_tx, done_rx) = flume::bounded(1);
        let worker = std::thread::spawn(move || {
            let _started = started_tx.send(());
            let result = blocked.next();
            let _done = done_tx.send(matches!(
                result,
                Err(MykoFederationError::Unavailable { .. })
            ));
        });

        assert_eq!(
            started_rx.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(())
        );
        subscription.cancel();
        assert_eq!(
            done_rx.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(true)
        );
        assert!(worker.join().is_ok());
    }

    #[test]
    fn authority_grant_subscription_cancel_does_not_block_with_pending_update() {
        let (writer, live) = myko_federation::live_subscription(LiveSubscriptionState {
            value: Some(RetainedGrantRows::new()),
            through: None,
            liveness: myko_federation::SubscriptionLiveness::Current,
        });
        let subscription = Arc::new(MykoAuthorityGrantsSubscription {
            subscription: live.clone(),
            publications: RetainedGrantPublicationQueue::new(&live),
        });
        writer.replace(LiveSubscriptionState {
            value: Some(RetainedGrantRows::new()),
            through: Some(LogPosition::new(1)),
            liveness: myko_federation::SubscriptionLiveness::Current,
        });
        let cancel_target = Arc::clone(&subscription);
        let (done_tx, done_rx) = flume::bounded(1);
        let worker = std::thread::spawn(move || {
            cancel_target.cancel();
            let _done = done_tx.send(());
        });

        assert_eq!(
            done_rx.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(())
        );
        assert!(worker.join().is_ok());
    }
}
