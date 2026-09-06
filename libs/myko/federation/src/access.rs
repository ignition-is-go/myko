use super::*;

/// Exponential reconnect timing shared by long-lived transport adapters.
///
/// A policy is deliberately small and copyable so applications can tune
/// interactive, mobile, and background-node behavior without replacing a
/// transport implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    initial_delay: Duration,
    maximum_delay: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            maximum_delay: Duration::from_secs(5),
        }
    }
}

impl ReconnectPolicy {
    /// Creates a bounded exponential reconnect policy.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero initial delay or a maximum below the
    /// initial delay.
    pub fn new(initial_delay: Duration, maximum_delay: Duration) -> Result<Self, &'static str> {
        if initial_delay.is_zero() {
            return Err("reconnect initial delay must be non-zero");
        }
        if maximum_delay < initial_delay {
            return Err("reconnect maximum delay must not be below the initial delay");
        }
        Ok(Self {
            initial_delay,
            maximum_delay,
        })
    }

    /// Returns the first retry delay.
    #[must_use]
    pub const fn initial_delay(self) -> Duration {
        self.initial_delay
    }

    /// Returns the exponential successor, capped by this policy's maximum.
    #[must_use]
    pub fn next_delay(self, current: Duration) -> Duration {
        current.saturating_mul(2).min(self.maximum_delay)
    }
}

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a new random identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Wraps an existing UUID.
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Returns the underlying UUID.
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates an identifier from an application-defined value.
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the identifier as text.
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

uuid_id!(BatchId);
uuid_id!(CommandId);
uuid_id!(NodeId);
uuid_id!(StorageIncarnationId);
string_id!(PrincipalId);
string_id!(ScopeId);
string_id!(ServiceId);

impl PrincipalId {
    /// Returns the canonical principal identity for a Myko node.
    #[must_use]
    pub fn for_node(node_id: NodeId) -> Self {
        Self::new(format!("node:{node_id}"))
    }
}

impl PartialEq<ServiceTypeId> for ServiceId {
    fn eq(&self, other: &ServiceTypeId) -> bool {
        self.0 == other.as_str()
    }
}

impl PartialEq<ServiceId> for ServiceTypeId {
    fn eq(&self, other: &ServiceId) -> bool {
        other == self
    }
}

impl ScopeId {
    /// Creates the canonical federation scope rooted at one typed item.
    ///
    /// Applications should not duplicate textual scope conventions. The item
    /// owning service, item type, and generated typed ID together form the
    /// stable wire identity. Including the root service prevents identically
    /// named item types from colliding across application services.
    #[must_use]
    pub fn for_item<T: MykoItem>(id: &T::Id) -> Self {
        Self::for_parts(T::SERVICE_ID.as_str(), T::ITEM_TYPE, id.as_ref())
    }

    /// Creates a canonical scope identity from an erased root entity.
    #[must_use]
    pub fn for_entity(entity: &EntityRef) -> Self {
        Self::for_parts(&entity.service_id, &entity.item_type, &entity.id)
    }

    /// Creates a canonical scope identity from stable schema components.
    #[must_use]
    pub fn for_parts(service_id: &str, item_type: &str, item_id: &str) -> Self {
        let item_name = snake_case_type_name(item_type);
        Self::new(format!("{service_id}/{item_name}:{item_id}"))
    }

    /// Compares two canonical scope identities.
    #[doc(hidden)]
    #[must_use]
    pub fn equivalent_to(&self, other: &Self) -> bool {
        self == other
    }
}

fn snake_case_type_name(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();
    let mut previous = None;
    while let Some(character) = characters.next() {
        let previous_is_lower_or_digit = previous.is_some_and(|previous: char| {
            previous.is_ascii_lowercase() || previous.is_ascii_digit()
        });
        let next_is_lower = characters.peek().is_some_and(char::is_ascii_lowercase);
        let starts_word = previous.is_some_and(|_| {
            character.is_ascii_uppercase() && (previous_is_lower_or_digit || next_is_lower)
        });
        if starts_word {
            output.push('_');
        }
        output.extend(character.to_lowercase());
        previous = Some(character);
    }
    output
}

/// A node-local, monotonically increasing history position.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct LogPosition(u64);

impl LogPosition {
    /// The first valid position in a node log.
    pub const FIRST: Self = Self(1);

    /// Creates a position from its wire representation.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire representation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(super) fn next(self) -> Result<Self, NodeError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(NodeError::PositionExhausted)
    }
}

/// Globally unique origin of an immutable event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId {
    pub node_id: NodeId,
    pub sequence: LogPosition,
}

impl EventId {
    /// Creates an origin identifier from a node and its local sequence.
    #[must_use]
    pub const fn new(node_id: NodeId, sequence: LogPosition) -> Self {
        Self { node_id, sequence }
    }
}

/// Durable command admission metadata owned by Myko.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandRequest {
    pub id: CommandId,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub principal_id: PrincipalId,
    /// Original principal and store-verifiable executor/provenance chain.
    #[serde(default = "default_authority_presentation")]
    pub authority: AuthorityPresentation,
    /// Claims declared before handler execution. Actual reads/mutations must
    /// remain within this set and are verified again before commit.
    #[serde(default)]
    pub resource_claims: Vec<ResourceClaim>,
    #[serde(default)]
    pub application_capabilities: Vec<CapabilityId>,
    pub arguments_digest: Option<String>,
    pub command_type: String,
    pub payload: Vec<u8>,
}

/// Authenticated-transport submission before Myko binds its principal.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSubmission {
    pub id: CommandId,
    pub service_id: ServiceId,
    pub command_type: String,
    pub payload: Vec<u8>,
}

impl CommandSubmission {
    #[doc(hidden)]
    pub fn for_command<C: MykoCommand>(command: &C) -> Result<Self, NodeError> {
        Ok(Self {
            id: CommandId::new(),
            service_id: ServiceId::new(C::SERVICE_ID),
            command_type: C::COMMAND_TYPE.to_owned(),
            payload: serde_json::to_vec(command)
                .map_err(|error| NodeError::CommandEncoding(error.to_string()))?,
        })
    }

    #[doc(hidden)]
    #[must_use]
    pub fn authenticate(self, scope_id: ScopeId, principal_id: PrincipalId) -> CommandRequest {
        let authority = AuthorityPresentation::direct_node(principal_id.clone());
        let arguments_digest = Some(digest_bytes(&self.payload));
        let primary_claim = ResourceClaim::scope(scope_id.clone(), ResourceClaimKind::Primary);
        CommandRequest {
            id: self.id,
            service_id: self.service_id,
            scope_id,
            principal_id,
            authority,
            resource_claims: vec![primary_claim],
            application_capabilities: Vec::new(),
            arguments_digest,
            command_type: self.command_type,
            payload: self.payload,
        }
    }
}

impl CommandRequest {
    /// Binds an untrusted wire presentation after the session has authenticated
    /// the final executor; the policy still validates every stored hop.
    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.principal_id = authority.principal.id.clone();
        self.authority = authority;
        self
    }

    pub(super) fn for_command<C: MykoCommand>(
        scope_id: ScopeId,
        principal_id: PrincipalId,
        command: &C,
    ) -> Result<Self, NodeError> {
        CommandSubmission::for_command(command)
            .map(|submission| submission.authenticate(scope_id, principal_id))
    }

    /// Decodes this request through one generated application-command contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the service/type identity or payload is invalid.
    #[doc(hidden)]
    pub fn command<C: MykoCommand>(&self) -> Result<C, NodeError> {
        decode_declared_body(self)
    }
}

pub(super) fn digest_bytes(value: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(value))
}

/// Transport-neutral operation presented to a node access policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessOperation {
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

/// One transport-neutral authority over a single federated scope.
///
/// These are deliberately narrower than an application's own permissions.
/// They describe what a remote Myko principal may do with framework data; an
/// application may layer its domain authorization on top of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FederationPermission {
    /// Read the current materialized state in the scope.
    ReadState,
    /// Read immutable history in the scope.
    ReadHistory,
    /// Keep a state or history stream open after its initial snapshot.
    Subscribe,
    /// Submit or cancel commands in the scope.
    Write,
    /// Permit the grantee to make the scope available to another principal.
    ///
    /// Transport adapters do not infer this from a connection; applications
    /// must explicitly use it when they implement delegation.
    Reshare,
    /// Administer the scope's federation grants.
    Admin,
}

/// Kind of registered application handler selected by a live subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandlerKind {
    Command,
    Query,
    Report,
    View,
}

impl HandlerKind {
    /// Returns the stable lowercase wire representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Query => "query",
            Self::Report => "report",
            Self::View => "view",
        }
    }
}

/// Structured identity of a registered handler subscription.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HandlerAccess {
    pub kind: HandlerKind,
    pub handler_id: String,
}

/// Exact domain target authorized for one prepared operation.
///
/// Target identity is represented by the variant that owns it, so downstream
/// policy code never reconciles competing optional service, scope, command,
/// handler, and live-topic fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessTarget {
    NodeIdentity,
    ScopeCatalog,
    History(ReplicationSelection),
    LiveTopics(Vec<String>),
    Scope(ScopeId),
    ScopeSet(Vec<ScopeSelection>),
    ServiceScope {
        service_id: ServiceId,
        scope_id: ScopeId,
    },
    Items {
        source_node: Option<NodeId>,
        service_id: ServiceId,
        scope_id: ScopeId,
        item_type: String,
    },
    Handler {
        access: HandlerAccess,
        source_node: Option<NodeId>,
        scope_id: Option<ScopeId>,
    },
    Command(CommandId),
    KnownCommand {
        command_id: CommandId,
        service_id: ServiceId,
        scope_id: ScopeId,
        command_type: String,
        principal_id: PrincipalId,
    },
    CommandCatalog {
        source_node: Option<NodeId>,
        service_id: ServiceId,
        scope_id: ScopeId,
        command_type: String,
    },
    AuthorityApproval(ChallengeId),
}

/// The portion of the nested scope tree covered by one grant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeGrantCoverage {
    /// Authorizes only the named scope.
    #[default]
    Exact,
    /// Authorizes the named scope and allows requesting its complete subtree.
    Subtree,
}

/// A directional, non-transitive grant from one node to one principal.
///
/// Pairing authenticates a peer but never creates this record. A grant only
/// applies only to its `grantee` and selected scope coverage; it cannot
/// authorize another node through the grantee unless the issuing application
/// explicitly models a reshare operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeGrant {
    pub scope_id: ScopeId,
    #[serde(default)]
    pub coverage: ScopeGrantCoverage,
    pub grantee: PrincipalId,
    pub permissions: Vec<FederationPermission>,
}

impl ScopeGrant {
    /// Returns whether this direct grant permits an authenticated request.
    #[must_use]
    pub fn authorizes(&self, request: &AccessAttempt) -> bool {
        let selections = request.scope_selections();
        self.authorizes_request(request)
            && !selections.is_empty()
            && selections.iter().all(|selection| self.covers(selection))
    }

    fn authorizes_request(&self, request: &AccessAttempt) -> bool {
        self.grantee == request.principal_id
            && required_permission(request.operation)
                .is_some_and(|permission| self.permissions.contains(&permission))
            && stream_permission(request.operation)
                .is_none_or(|permission| self.permissions.contains(&permission))
    }

    fn covers(&self, selection: &ScopeSelection) -> bool {
        match selection {
            ScopeSelection::Exact(scope_id) => scope_id.equivalent_to(&self.scope_id),
            ScopeSelection::Subtree(scope_id) => {
                scope_id.equivalent_to(&self.scope_id)
                    && self.coverage == ScopeGrantCoverage::Subtree
            }
        }
    }
}

const fn required_permission(operation: AccessOperation) -> Option<FederationPermission> {
    match operation {
        AccessOperation::ReadHistory | AccessOperation::FollowHistory => {
            Some(FederationPermission::ReadHistory)
        }
        AccessOperation::ReadItems
        | AccessOperation::FollowItems
        | AccessOperation::FollowHandler
        | AccessOperation::ReadCommand
        | AccessOperation::ReadCommands
        | AccessOperation::WatchCommand
        | AccessOperation::WatchCommands => Some(FederationPermission::ReadState),
        AccessOperation::SubmitCommand | AccessOperation::CancelCommand => {
            Some(FederationPermission::Write)
        }
        AccessOperation::AdministerAuthority => Some(FederationPermission::Admin),
        AccessOperation::DelegateAuthority => Some(FederationPermission::Reshare),
        // Live topics do not carry a scope identifier. A scope grant must not
        // accidentally disclose them; an application can authorize exact
        // topics in its own policy once it has mapped them to a scope.
        AccessOperation::SubscribeLive | AccessOperation::ApproveAuthority => None,
    }
}

const fn stream_permission(operation: AccessOperation) -> Option<FederationPermission> {
    match operation {
        AccessOperation::FollowItems
        | AccessOperation::FollowHandler
        | AccessOperation::FollowHistory
        | AccessOperation::WatchCommand
        | AccessOperation::WatchCommands => Some(FederationPermission::Subscribe),
        AccessOperation::ReadHistory
        | AccessOperation::ReadItems
        | AccessOperation::SubscribeLive
        | AccessOperation::SubmitCommand
        | AccessOperation::ReadCommand
        | AccessOperation::ReadCommands
        | AccessOperation::CancelCommand
        | AccessOperation::ApproveAuthority
        | AccessOperation::AdministerAuthority
        | AccessOperation::DelegateAuthority => None,
    }
}

/// Immutable transport policy backed by direct [`ScopeGrant`] records.
///
/// This is a reusable baseline for applications whose federation authority is
/// entirely scope-based. Richer applications can compose the same grant check
/// with domain-specific authorization in their own [`AccessPolicy`].
#[derive(Debug, Clone, Default)]
pub struct ScopeGrantPolicy {
    grants: Vec<ScopeGrant>,
}

impl ScopeGrantPolicy {
    /// Creates a policy from the current authoritative direct grants.
    #[must_use]
    pub const fn new(grants: Vec<ScopeGrant>) -> Self {
        Self { grants }
    }

    /// Returns the direct grants used by this policy.
    #[must_use]
    pub fn grants(&self) -> &[ScopeGrant] {
        &self.grants
    }
}

/// Authenticated operation used for authorization before node access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessAttempt {
    /// Cryptographically authenticated transport identity. This is never
    /// replaced by a principal asserted inside the wire presentation.
    pub principal_id: PrincipalId,
    /// Original principal, final executor, immutable provenance, and approvals.
    #[serde(default = "default_authority_presentation")]
    pub presentation: AuthorityPresentation,
    pub operation: AccessOperation,
    pub target: AccessTarget,
    /// Complete preflight or actual command resource set.
    #[serde(default)]
    pub resource_claims: Vec<ResourceClaim>,
    /// Registered opaque application capabilities required by this operation.
    #[serde(default)]
    pub application_capabilities: Vec<CapabilityId>,
    /// Stable digests bind approvals without exposing arguments or effects.
    pub arguments_digest: Option<String>,
    pub effect_digest: Option<String>,
    pub lease: Option<AuthorityLeaseRequest>,
    #[serde(default)]
    pub authorization_phase: AuthorizationPhase,
    /// Authoritative topology is supplied locally to the evaluator and never
    /// accepted from an untrusted transport.
    #[serde(skip)]
    pub topology: Option<ScopeTopology>,
}

fn default_authority_presentation() -> AuthorityPresentation {
    AuthorityPresentation::direct_node(PrincipalId::new("unauthenticated"))
}

impl AccessAttempt {
    /// Creates a request bound to one exact scope and authenticated executor.
    #[must_use]
    pub fn scoped(
        principal_id: PrincipalId,
        presentation: AuthorityPresentation,
        operation: AccessOperation,
        scope_id: ScopeId,
    ) -> Self {
        Self {
            principal_id,
            presentation,
            operation,
            target: AccessTarget::Scope(scope_id.clone()),
            resource_claims: vec![ResourceClaim::scope(scope_id, ResourceClaimKind::Primary)],
            application_capabilities: Vec::new(),
            arguments_digest: None,
            effect_digest: None,
            lease: None,
            authorization_phase: AuthorizationPhase::Admission,
            topology: None,
        }
    }

    #[must_use]
    pub const fn service_id(&self) -> Option<&ServiceId> {
        match &self.target {
            AccessTarget::ServiceScope { service_id, .. }
            | AccessTarget::Items { service_id, .. }
            | AccessTarget::KnownCommand { service_id, .. }
            | AccessTarget::CommandCatalog { service_id, .. } => Some(service_id),
            _ => None,
        }
    }

    #[must_use]
    pub fn scope_id(&self) -> Option<&ScopeId> {
        match &self.target {
            AccessTarget::Scope(scope_id)
            | AccessTarget::ServiceScope { scope_id, .. }
            | AccessTarget::Items { scope_id, .. }
            | AccessTarget::KnownCommand { scope_id, .. }
            | AccessTarget::CommandCatalog { scope_id, .. } => Some(scope_id),
            AccessTarget::History(ReplicationSelection::ServiceScope { scope_id, .. }) => {
                Some(scope_id)
            }
            AccessTarget::ScopeSet(selections)
            | AccessTarget::History(
                ReplicationSelection::Scopes(selections)
                | ReplicationSelection::Intersection {
                    scopes: selections, ..
                },
            ) => match selections.as_slice() {
                [ScopeSelection::Exact(scope_id)] => Some(scope_id),
                _ => None,
            },
            AccessTarget::Handler { scope_id, .. } => scope_id.as_ref(),
            _ => None,
        }
    }

    #[must_use]
    pub const fn command_id(&self) -> Option<CommandId> {
        match self.target {
            AccessTarget::Command(command_id) | AccessTarget::KnownCommand { command_id, .. } => {
                Some(command_id)
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn command_type(&self) -> Option<&str> {
        match &self.target {
            AccessTarget::KnownCommand { command_type, .. }
            | AccessTarget::CommandCatalog { command_type, .. } => Some(command_type),
            _ => None,
        }
    }

    #[must_use]
    pub const fn command_principal_id(&self) -> Option<&PrincipalId> {
        match &self.target {
            AccessTarget::KnownCommand { principal_id, .. } => Some(principal_id),
            _ => None,
        }
    }

    #[must_use]
    pub fn scope_selections(&self) -> Vec<ScopeSelection> {
        if !self.resource_claims.is_empty() {
            return self
                .resource_claims
                .iter()
                .map(|claim| claim.selection.clone())
                .collect();
        }
        match &self.target {
            AccessTarget::ScopeSet(selections)
            | AccessTarget::History(
                ReplicationSelection::Scopes(selections)
                | ReplicationSelection::Intersection {
                    scopes: selections, ..
                },
            ) => selections.clone(),
            AccessTarget::History(ReplicationSelection::ServiceScope { scope_id, .. })
            | AccessTarget::Scope(scope_id)
            | AccessTarget::ServiceScope { scope_id, .. }
            | AccessTarget::Items { scope_id, .. }
            | AccessTarget::KnownCommand { scope_id, .. }
            | AccessTarget::CommandCatalog { scope_id, .. } => {
                vec![ScopeSelection::Exact(scope_id.clone())]
            }
            AccessTarget::Handler {
                scope_id: Some(scope_id),
                ..
            } => vec![ScopeSelection::Exact(scope_id.clone())],
            _ => Vec::new(),
        }
    }

    #[must_use]
    pub const fn handler(&self) -> Option<&HandlerAccess> {
        match &self.target {
            AccessTarget::Handler { access, .. } => Some(access),
            _ => None,
        }
    }

    #[must_use]
    pub fn live_topics(&self) -> &[String] {
        match &self.target {
            AccessTarget::LiveTopics(topics) => topics,
            _ => &[],
        }
    }

    /// Returns whether this request targets one typed item service.
    #[must_use]
    pub fn service_is<T: MykoItem>(&self) -> bool {
        self.service_id()
            .is_some_and(|service_id| *service_id == T::SERVICE_ID)
    }

    /// Returns whether this request targets one typed application service.
    #[must_use]
    pub fn service_is_service<S: MykoService>(&self) -> bool {
        self.service_id()
            .is_some_and(|service_id| *service_id == S::SERVICE_ID)
    }

    /// Returns whether this request targets one typed command contract.
    #[must_use]
    pub fn command_is<C: MykoCommand>(&self) -> bool {
        self.service_id()
            .is_some_and(|service_id| *service_id == C::SERVICE_ID)
            && self.command_type() == Some(C::COMMAND_TYPE)
    }
}

/// An approval decision that may require remote authority coordination.
pub type AuthorityApprovalFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<ApprovalDecision, AuthorizationFailure>>
            + Send
            + 'a,
    >,
>;

/// Pluggable authorization decision shared by transport adapters.
pub trait AccessPolicy: fmt::Debug + Send + Sync + 'static {
    /// Returns a first-class permit, deny, or durable challenge decision.
    ///
    /// # Errors
    /// Returns unavailable authority separately from any policy decision.
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> PolicyDecision<'a>;

    /// Returns the shared reactive revision for facts that may alter existing
    /// decisions. Myko subscribes directly to this Hyphae cell; policies do
    /// not maintain transport-specific subscriber registries.
    fn revision_cell(&self) -> Option<Cell<u64, CellImmutable>> {
        None
    }

    /// Intersects selective replication with current grants. Policies without
    /// a richer model must authorize the complete request or deny it.
    ///
    /// # Errors
    ///
    /// Returns the structured authorization decision when the selection is not
    /// permitted.
    fn constrain_replication(
        &self,
        request: &AccessAttempt,
        selection: &ReplicationSelection,
        _topology: &ScopeTopology,
    ) -> Result<ReplicationSelection, AuthorizationFailure> {
        self.decide(request).into_immediate()?.into_permit()?;
        Ok(selection.clone())
    }

    /// Records an approval only when the transport executor and authority
    /// presentation are validated by a durable policy implementation.
    ///
    /// # Errors
    ///
    /// Returns a structured denial when this policy cannot validate or record
    /// the approval.
    fn approve<'a>(
        &'a self,
        _authenticated_executor: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        _challenge_id: &'a ChallengeId,
        _approved: bool,
    ) -> AuthorityApprovalFuture<'a> {
        Box::pin(async move {
            Err(AuthorizationFailure::Deny(Box::new(DenyDecision {
                report: AuthorizationReport {
                    evaluated_at: Utc::now(),
                    principal: presentation.principal.clone(),
                    executor: presentation.executor.clone(),
                    operation: AccessOperation::ApproveAuthority,
                    explanations: vec![AuthorizationExplanation {
                        code: "approval_unsupported".to_owned(),
                        message: "this access policy does not accept approvals".to_owned(),
                        grant_id: None,
                        delegation_id: None,
                        obligation_id: None,
                        constraint: None,
                    }],
                },
                visibility: ResourceVisibility::Unbound,
            })))
        })
    }

    /// Registers one opaque application capability before grants may cite it.
    ///
    /// # Errors
    ///
    /// Returns an error when the policy cannot durably register the capability.
    fn register_application_capability(
        &self,
        _authenticated_executor: &PrincipalId,
        _presentation: &AuthorityPresentation,
        _capability: ApplicationCapability,
    ) -> Result<(), String> {
        Err("this access policy does not accept application capabilities".to_owned())
    }
}

impl AccessPolicy for ScopeGrantPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> crate::PolicyDecision<'a> {
        let selections = request.scope_selections();
        let rule = if !selections.is_empty()
            && selections.iter().all(|selection| {
                self.grants
                    .iter()
                    .any(|grant| grant.authorizes_request(request) && grant.covers(selection))
            }) {
            Ok(())
        } else {
            Err("scope grant does not permit this operation".to_owned())
        };
        Ok(AuthorizationDecision::from_rule(request, rule)).into()
    }
}

/// Explicit development policy that grants every authenticated request.
///
/// Production nodes replace this with a grant-backed policy; keeping the
/// permissive policy explicit avoids baking transport identity into graph
/// semantics while bootstrapping local development.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAllAccessPolicy;

impl AccessPolicy for AllowAllAccessPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> crate::PolicyDecision<'a> {
        Ok(AuthorizationDecision::from_rule(request, Ok(()))).into()
    }
}

/// Denies every application and federation operation.
///
/// Edge nodes use this policy while retaining a normal authenticated transport
/// identity. Pairing and descriptor verification remain available through
/// their dedicated Iroh protocol, but an edge node cannot accidentally expose
/// its local journal or application handlers to a peer.
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyAllAccessPolicy;

impl AccessPolicy for DenyAllAccessPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> crate::PolicyDecision<'a> {
        Ok(AuthorizationDecision::from_rule(
            request,
            Err("this Myko node does not serve application or federation data".to_owned()),
        ))
        .into()
    }
}
