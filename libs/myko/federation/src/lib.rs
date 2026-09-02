//! Transport-neutral federation primitives for Myko 7.
//!
//! This crate deliberately has no socket, HTTP, Iroh, Tokio, or storage-engine
//! dependency. A node is an authenticated command/history endpoint. It also
//! provides a bounded [`LiveEventHub`] for coalescible, non-authoritative state.
//! Network protocols and durable storage implement adapters around these
//! transport-neutral contracts.

#![forbid(unsafe_code)]

mod reactive;

pub use reactive::{
    LiveCollection, LiveCollectionError, LiveCollectionRevision, LiveCollectionState,
    LiveCollectionWriter, LiveSubscription, LiveSubscriptionState, LiveSubscriptionWriter,
    SubscriptionLiveness, live_collection, live_subscription,
};

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque, hash_map::Entry},
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub use myko_items::{
    ConcreteEndpoint, Directed, EdgeEnds, EndpointSpec, EntityRef, GraphEdge, ItemMutation,
    ItemProjection, ItemQuery, MutationOperation, MykoCommand, MykoCommandContract, MykoItem,
    MykoOperation, MykoService, ServiceTypeId, TypedEdgeEnds, Undirected,
};

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
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
string_id!(PrincipalId);
string_id!(ScopeId);
string_id!(ServiceId);

impl ScopeId {
    /// Creates the canonical federation scope rooted at one typed item.
    ///
    /// Applications should not duplicate textual scope conventions. The item
    /// type and its generated typed ID are sufficient to derive the stable
    /// wire identity.
    #[must_use]
    pub fn for_item<T: MykoItem>(id: &T::Id) -> Self {
        let mut item_type = String::with_capacity(T::ITEM_TYPE.len());
        for (index, character) in T::ITEM_TYPE.chars().enumerate() {
            if character.is_uppercase() && index > 0 {
                item_type.push('_');
            }
            item_type.extend(character.to_lowercase());
        }
        Self::new(format!("{item_type}:{}", id.as_ref()))
    }
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

    fn next(self) -> Result<Self, NodeError> {
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
        CommandRequest {
            id: self.id,
            service_id: self.service_id,
            scope_id,
            principal_id,
            command_type: self.command_type,
            payload: self.payload,
        }
    }
}

impl CommandRequest {
    fn for_command<C: MykoCommand>(
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

/// Transport-neutral operation presented to a node access policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
}

/// One transport-neutral authority over a single federated scope.
///
/// These are deliberately narrower than an application's own permissions.
/// They describe what a remote Myko principal may do with framework data; an
/// application may layer its domain authorization on top of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

/// A directional, non-transitive grant from one node to one principal.
///
/// Pairing authenticates a peer but never creates this record. A grant only
/// applies to its exact `grantee` and `scope_id`; it cannot authorize another
/// node through the grantee unless the issuing application explicitly models a
/// reshare operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeGrant {
    pub scope_id: ScopeId,
    pub grantee: PrincipalId,
    pub permissions: Vec<FederationPermission>,
}

impl ScopeGrant {
    /// Returns whether this direct grant permits an authenticated request.
    #[must_use]
    pub fn authorizes(&self, request: &AccessRequest) -> bool {
        self.grantee == request.principal_id
            && request.scope_id.as_ref() == Some(&self.scope_id)
            && required_permission(request.operation)
                .is_some_and(|permission| self.permissions.contains(&permission))
            && stream_permission(request.operation)
                .is_none_or(|permission| self.permissions.contains(&permission))
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
        // Live topics do not carry a scope identifier. A scope grant must not
        // accidentally disclose them; an application can authorize exact
        // topics in its own policy once it has mapped them to a scope.
        AccessOperation::SubscribeLive => None,
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
        | AccessOperation::CancelCommand => None,
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

/// Authenticated request metadata used for authorization before node access.
///
/// A transport maps its cryptographic peer identity to `principal_id`. Scope,
/// service, and command metadata are populated when the operation has them;
/// live subscriptions instead carry their exact topics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessRequest {
    pub principal_id: PrincipalId,
    pub operation: AccessOperation,
    pub service_id: Option<ServiceId>,
    pub scope_id: Option<ScopeId>,
    pub command_id: Option<CommandId>,
    pub command_type: Option<String>,
    pub command_principal_id: Option<PrincipalId>,
    pub live_topics: Vec<String>,
}

impl AccessRequest {
    /// Returns whether this request targets one typed item service.
    #[must_use]
    pub fn service_is<T: MykoItem>(&self) -> bool {
        self.service_id
            .as_ref()
            .is_some_and(|service_id| service_id.as_str() == T::SERVICE_ID.as_str())
    }

    /// Returns whether this request targets one typed command contract.
    #[must_use]
    pub fn command_is<C: MykoCommand>(&self) -> bool {
        self.service_id
            .as_ref()
            .is_some_and(|service_id| service_id.as_str() == C::SERVICE_ID.as_str())
            && self.command_type.as_deref() == Some(C::COMMAND_TYPE)
    }
}

/// Pluggable authorization decision shared by transport adapters.
pub trait AccessPolicy: fmt::Debug + Send + Sync + 'static {
    /// Authorizes one authenticated request before history or state is exposed.
    ///
    /// # Errors
    ///
    /// Returns a public-safe denial reason when access is not granted.
    fn authorize(&self, request: &AccessRequest) -> Result<(), String>;
}

impl AccessPolicy for ScopeGrantPolicy {
    fn authorize(&self, request: &AccessRequest) -> Result<(), String> {
        if self.grants.iter().any(|grant| grant.authorizes(request)) {
            Ok(())
        } else {
            Err("scope grant does not permit this operation".to_owned())
        }
    }
}

/// Compatibility policy that grants every authenticated request.
///
/// Production nodes replace this with a grant-backed policy; keeping the
/// permissive policy explicit avoids baking transport identity into graph
/// semantics while bootstrapping local development.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAllAccessPolicy;

impl AccessPolicy for AllowAllAccessPolicy {
    fn authorize(&self, _request: &AccessRequest) -> Result<(), String> {
        Ok(())
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
    fn authorize(&self, _request: &AccessRequest) -> Result<(), String> {
        Err("this Myko node does not serve application or federation data".to_owned())
    }
}

/// All authoritative graph changes accepted from one command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeBatch {
    pub id: BatchId,
    pub command_id: CommandId,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub causal_parents: Vec<EventId>,
    pub changes: Vec<ItemMutation>,
}

/// Reconciliation outcome after concurrent changes have been considered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reconciliation {
    FullyVisible,
    PartiallySuperseded,
    FullySuperseded,
}

/// Durable lifecycle of an idempotent command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CommandState {
    Submitted,
    /// A claimed handler encountered a transient failure and released the
    /// command for another ordered attempt.
    Retrying {
        reason: String,
    },
    Executing,
    CommittedLocally {
        batch_id: BatchId,
        position: EventId,
    },
    Replicating {
        batch_id: BatchId,
        position: EventId,
    },
    ReplicationDelayed {
        batch_id: BatchId,
        position: EventId,
        reason: String,
    },
    Replicated {
        batch_id: BatchId,
        position: EventId,
        acknowledged_replicas: u32,
        required_replicas: u32,
    },
    Reconciled {
        batch_id: BatchId,
        position: EventId,
        outcome: Reconciliation,
    },
    Rejected {
        reason: String,
    },
    Cancelled {
        reason: String,
    },
}

impl CommandState {
    /// Returns whether command execution has ended locally.
    #[must_use]
    pub const fn is_terminal_locally(&self) -> bool {
        matches!(
            self,
            Self::CommittedLocally { .. }
                | Self::Replicating { .. }
                | Self::ReplicationDelayed { .. }
                | Self::Replicated { .. }
                | Self::Reconciled { .. }
                | Self::Rejected { .. }
                | Self::Cancelled { .. }
        )
    }

    /// Returns whether the command committed an authoritative change batch.
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(
            self,
            Self::CommittedLocally { .. }
                | Self::Replicating { .. }
                | Self::ReplicationDelayed { .. }
                | Self::Replicated { .. }
                | Self::Reconciled { .. }
        )
    }
}

/// Latest durable view of a command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSnapshot {
    pub request: CommandRequest,
    pub state: CommandState,
    pub result: Option<Vec<u8>>,
    pub updated_at: EventId,
}

impl CommandSnapshot {
    /// Decodes this command's result using its generated typed contract.
    ///
    /// `None` means the command has not produced a result. The command's
    /// durable lifecycle remains available through [`Self::state`].
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot belongs to another contract or its
    /// result bytes do not match `C::Output`.
    pub fn typed_result<C: MykoCommand>(&self) -> Result<Option<C::Output>, NodeError> {
        if self.request.service_id.as_str() != C::SERVICE_ID.as_str()
            || self.request.command_type != C::COMMAND_TYPE
        {
            return Err(NodeError::CommandSchemaMismatch {
                expected_service: C::SERVICE_ID.as_str(),
                expected_command: C::COMMAND_TYPE,
                actual_service: self.request.service_id.as_str().to_owned(),
                actual_command: self.request.command_type.clone(),
            });
        }
        self.result
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(|error| NodeError::ResultDecoding(error.to_string()))
    }

    /// Decodes a completed typed result or reports a terminal command failure.
    ///
    /// `None` means the command is still progressing. A locally terminal
    /// successful command without an encoded result is rejected as corrupt
    /// lifecycle state rather than leaving a caller waiting forever.
    ///
    /// # Errors
    ///
    /// Returns an error for a schema mismatch, invalid result, rejection,
    /// cancellation, or missing terminal result.
    pub fn typed_completion<C: MykoCommand>(&self) -> Result<Option<C::Output>, NodeError> {
        if let Some(result) = self.typed_result::<C>()? {
            return Ok(Some(result));
        }
        match &self.state {
            CommandState::Rejected { reason } => Err(NodeError::CommandRejected {
                command_id: self.request.id,
                reason: reason.clone(),
            }),
            CommandState::Cancelled { reason } => Err(NodeError::CommandCancelled {
                command_id: self.request.id,
                reason: reason.clone(),
            }),
            state if state.is_terminal_locally() => Err(NodeError::ResultDecoding(format!(
                "command {} reached {state:?} without a typed result",
                self.request.id
            ))),
            _ => Ok(None),
        }
    }
}

/// Transport-neutral response from a command endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResponse {
    /// Stable Myko identity of the node serving this response.
    pub source_node: NodeId,
    /// Current durable command state, or `None` for an unknown ID.
    pub command: Option<CommandSnapshot>,
}

/// Default number of current command states returned by one transport page.
pub const DEFAULT_COMMAND_STATE_PAGE_SIZE: u32 = 256;

/// Hard framework limit for one current-command transport page.
pub const MAX_COMMAND_STATE_PAGE_SIZE: u32 = 4_096;

/// Transport-neutral request for one page of current command state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStateRequest {
    /// Authoritative command source, or the serving node when omitted.
    pub source_node: Option<NodeId>,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub command_type: String,
    /// Immutable serving-log ceiling selected by the first page.
    pub snapshot_through: Option<LogPosition>,
    /// Exclusive lexical command-ID cursor within that snapshot.
    pub after_command_id: Option<String>,
    pub page_size: u32,
}

impl CommandStateRequest {
    /// Creates a request for one declared command contract at an explicit source.
    #[must_use]
    pub fn for_declared<C: MykoCommand>(source_node: NodeId, scope_id: ScopeId) -> Self {
        Self {
            source_node: Some(source_node),
            service_id: ServiceId::new(C::SERVICE_ID),
            scope_id,
            command_type: C::COMMAND_TYPE.to_owned(),
            snapshot_through: None,
            after_command_id: None,
            page_size: DEFAULT_COMMAND_STATE_PAGE_SIZE,
        }
    }

    /// Creates a request for the serving node's declared commands.
    #[must_use]
    pub fn for_serving_declared<C: MykoCommand>(scope_id: ScopeId) -> Self {
        Self {
            source_node: None,
            service_id: ServiceId::new(C::SERVICE_ID),
            scope_id,
            command_type: C::COMMAND_TYPE.to_owned(),
            snapshot_through: None,
            after_command_id: None,
            page_size: DEFAULT_COMMAND_STATE_PAGE_SIZE,
        }
    }

    /// Selects the requested transport page size.
    #[must_use]
    pub const fn with_page_size(mut self, page_size: u32) -> Self {
        self.page_size = page_size;
        self
    }
}

/// One current command plus durable ordering metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStateEntry {
    pub admitted_at: LogPosition,
    pub last_changed_at: LogPosition,
    pub command: CommandSnapshot,
}

/// One bounded cursor-stable page of current command states.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStatePage {
    pub serving_node: NodeId,
    pub through: Option<LogPosition>,
    pub request: CommandStateRequest,
    pub commands: Vec<CommandStateEntry>,
    pub next_after_command_id: Option<String>,
}

/// Complete current command state collected from one or more bounded pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStateSnapshot {
    pub serving_node: NodeId,
    pub through: Option<LogPosition>,
    pub request: CommandStateRequest,
    pub commands: Vec<CommandStateEntry>,
}

/// Lossless cursor request for one source/service/scope command catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandWatchRequest {
    pub serving_node: NodeId,
    pub source_node: NodeId,
    pub service_id: ServiceId,
    pub scope_id: ScopeId,
    pub command_type: String,
    pub after: Option<LogPosition>,
}

/// One matching durable command transition on a catalog stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStateUpdate {
    pub through: LogPosition,
    pub command: CommandSnapshot,
}

/// Client-side materializer for a snapshot-then-live command catalog.
pub struct CommandStateStream {
    request: CommandWatchRequest,
    through: Option<LogPosition>,
    commands: BTreeMap<String, CommandStateEntry>,
}

/// One decoded application command lifecycle from a typed catalog.
pub struct TypedCommandState<C: MykoCommand> {
    pub admitted_at: LogPosition,
    pub last_changed_at: LogPosition,
    pub id: CommandId,
    pub scope_id: ScopeId,
    pub principal_id: PrincipalId,
    pub command: C,
    pub state: CommandState,
    pub result: Option<C::Output>,
    pub updated_at: EventId,
}

impl CommandStateSnapshot {
    /// Decodes all current commands using one typed application contract.
    ///
    /// Results retain admission order rather than lexical command-ID order.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested contract or any body/result is invalid.
    pub fn typed<C: MykoCommand>(&self) -> Result<Vec<TypedCommandState<C>>, NodeError> {
        if self.request.service_id.as_str() != C::SERVICE_ID.as_str()
            || self.request.command_type != C::COMMAND_TYPE
        {
            return Err(NodeError::CommandSchemaMismatch {
                expected_service: C::SERVICE_ID.as_str(),
                expected_command: C::COMMAND_TYPE,
                actual_service: self.request.service_id.as_str().to_owned(),
                actual_command: self.request.command_type.clone(),
            });
        }
        let mut commands = self.commands.iter().collect::<Vec<_>>();
        commands.sort_unstable_by(|left, right| {
            left.admitted_at.cmp(&right.admitted_at).then_with(|| {
                left.command
                    .request
                    .id
                    .to_string()
                    .cmp(&right.command.request.id.to_string())
            })
        });
        commands
            .into_iter()
            .map(decode_typed_command_state::<C>)
            .collect()
    }

    /// Creates the lossless follow cursor for this completed snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot has no resolved source or its request
    /// is not bound to the collected serving-log ceiling.
    pub fn watch_request(&self) -> Result<CommandWatchRequest, NodeError> {
        let source_node = self.request.source_node.ok_or_else(|| {
            NodeError::InvalidCommandState(
                "command snapshot did not resolve its authoritative source".to_owned(),
            )
        })?;
        if self.request.snapshot_through != self.through || self.request.after_command_id.is_some()
        {
            return Err(NodeError::InvalidCommandState(
                "command snapshot is not bound to one complete cursor".to_owned(),
            ));
        }
        Ok(CommandWatchRequest {
            serving_node: self.serving_node,
            source_node,
            service_id: self.request.service_id.clone(),
            scope_id: self.request.scope_id.clone(),
            command_type: self.request.command_type.clone(),
            after: self.through,
        })
    }

    fn from_first_page(
        page: CommandStatePage,
    ) -> Result<(Self, Option<CommandStateRequest>), NodeError> {
        if page.request.after_command_id.is_some() {
            return Err(NodeError::InvalidCommandState(
                "a complete command snapshot must begin without a command cursor".to_owned(),
            ));
        }
        let next = next_command_state_request(&page)?;
        Ok((
            Self {
                serving_node: page.serving_node,
                through: page.through,
                request: page.request,
                commands: page.commands,
            },
            next,
        ))
    }

    fn append_page(
        &mut self,
        expected_request: &CommandStateRequest,
        page: CommandStatePage,
    ) -> Result<Option<CommandStateRequest>, NodeError> {
        if &page.request != expected_request
            || page.serving_node != self.serving_node
            || page.through != self.through
        {
            return Err(NodeError::InvalidCommandState(
                "command-state pagination changed request, server, or snapshot cursor".to_owned(),
            ));
        }
        let next = next_command_state_request(&page)?;
        self.commands.extend(page.commands);
        Ok(next)
    }
}

impl CommandWatchRequest {
    /// Filters one durable envelope into this exact command contract.
    #[must_use]
    pub fn update_from_envelope(&self, envelope: &EventEnvelope) -> Option<CommandStateUpdate> {
        if envelope.origin.node_id != self.source_node {
            return None;
        }
        let command = command_from_event(&envelope.event);
        (command.request.service_id == self.service_id
            && command.request.scope_id == self.scope_id
            && command.request.command_type == self.command_type)
            .then(|| CommandStateUpdate {
                through: envelope.position,
                command: command.clone(),
            })
    }
}

impl CommandStateStream {
    /// Starts a live materializer from a completed command snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot identity, cursor, or entries are
    /// malformed.
    pub fn from_snapshot(snapshot: &CommandStateSnapshot) -> Result<Self, NodeError> {
        let request = snapshot.watch_request()?;
        let mut commands = BTreeMap::new();
        for entry in &snapshot.commands {
            validate_command_state_entry(&request, snapshot.through, entry)?;
            let key = entry.command.request.id.to_string();
            if commands.insert(key, entry.clone()).is_some() {
                return Err(NodeError::InvalidCommandState(
                    "command snapshot contains a duplicate command ID".to_owned(),
                ));
            }
        }
        Ok(Self {
            request,
            through: snapshot.through,
            commands,
        })
    }

    /// Returns the exact remote follow request represented by this stream.
    #[must_use]
    pub const fn request(&self) -> &CommandWatchRequest {
        &self.request
    }

    /// Returns the currently materialized command catalog.
    #[must_use]
    pub fn current(&self) -> CommandStateSnapshot {
        CommandStateSnapshot {
            serving_node: self.request.serving_node,
            through: self.through,
            request: CommandStateRequest {
                source_node: Some(self.request.source_node),
                service_id: self.request.service_id.clone(),
                scope_id: self.request.scope_id.clone(),
                command_type: self.request.command_type.clone(),
                snapshot_through: self.through,
                after_command_id: None,
                page_size: DEFAULT_COMMAND_STATE_PAGE_SIZE,
            },
            commands: self.commands.values().cloned().collect(),
        }
    }

    /// Applies one matching durable transition atomically.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale stream cursor or mismatched command.
    pub fn apply(
        &mut self,
        update: &CommandStateUpdate,
    ) -> Result<CommandStateSnapshot, NodeError> {
        if self
            .through
            .is_some_and(|through| update.through <= through)
        {
            return Err(NodeError::InvalidCommandState(
                "command stream did not advance its serving cursor".to_owned(),
            ));
        }
        validate_command_update(&self.request, update)?;
        let key = update.command.request.id.to_string();
        if let Some(entry) = self.commands.get_mut(&key) {
            entry.admitted_at = entry.admitted_at.min(update.command.updated_at.sequence);
            if command_transition_is_newer(&entry.command, &update.command) {
                entry.last_changed_at = update.through;
                entry.command = update.command.clone();
            }
        } else {
            self.commands.insert(
                key,
                CommandStateEntry {
                    admitted_at: update.command.updated_at.sequence,
                    last_changed_at: update.through,
                    command: update.command.clone(),
                },
            );
        }
        self.through = Some(update.through);
        Ok(self.current())
    }
}

/// Gap-free current-then-live watch for one durable command lifecycle.
pub struct CommandWatch {
    command_id: CommandId,
    current: CommandSnapshot,
    events: EventSubscription,
}

impl CommandWatch {
    /// Returns the latest lifecycle state materialized by this watch.
    #[must_use]
    pub const fn current(&self) -> &CommandSnapshot {
        &self.current
    }

    /// Waits for the command's next durable lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns an error if the event subscription closes.
    pub fn recv(&mut self) -> Result<CommandSnapshot, NodeError> {
        loop {
            let envelope = self.events.recv()?;
            let command = command_from_event(&envelope.event);
            if command.request.id == self.command_id
                && command_transition_is_newer(&self.current, command)
            {
                self.current = command.clone();
                return Ok(self.current.clone());
            }
        }
    }

    /// Asynchronously waits for the command's next durable lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns an error if the event subscription closes.
    pub async fn recv_async(&mut self) -> Result<CommandSnapshot, NodeError> {
        loop {
            let envelope = self.events.recv_async().await?;
            let command = command_from_event(&envelope.event);
            if command.request.id == self.command_id
                && command_transition_is_newer(&self.current, command)
            {
                self.current = command.clone();
                return Ok(self.current.clone());
            }
        }
    }
}

/// Result of admitting a stable command ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "admission", content = "command", rename_all = "snake_case")]
pub enum CommandAdmission {
    /// This node atomically won admission and may execute the command.
    Execute(CommandSnapshot),
    /// The command already exists; observe or resume it without re-execution.
    Resume(CommandSnapshot),
}

impl CommandAdmission {
    /// Returns the current command snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &CommandSnapshot {
        match self {
            Self::Execute(snapshot) | Self::Resume(snapshot) => snapshot,
        }
    }

    /// Returns whether the caller owns this execution attempt.
    #[must_use]
    pub const fn should_execute(&self) -> bool {
        matches!(self, Self::Execute(_))
    }
}

/// One immutable entry in node history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Cursor in the observing node's replay log.
    pub position: LogPosition,
    /// Stable identity assigned by the node that originally accepted the event.
    pub origin: EventId,
    pub recorded_at: DateTime<Utc>,
    pub event: NodeEvent,
}

/// Outcome of ingesting a replicated immutable event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IngestStatus {
    Applied { position: LogPosition },
    Duplicate,
}

/// Immutable events exported from one peer's local replay cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationBatch {
    pub source_node: NodeId,
    pub after: Option<LogPosition>,
    pub through: Option<LogPosition>,
    pub events: Vec<EventEnvelope>,
}

/// Immutable events for one exact scope plus a source-log cursor watermark.
///
/// Unlike a full [`ReplicationBatch`], event positions may contain gaps because
/// entries belonging to other scopes are omitted. `through` still advances
/// over those entries, allowing a short-lived client to resume without pulling
/// the complete node history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedReplicationBatch {
    pub source_node: NodeId,
    pub scope_id: ScopeId,
    pub after: Option<LogPosition>,
    pub through: Option<LogPosition>,
    pub events: Vec<EventEnvelope>,
}

/// Resume position bound to one source history and one exact scope.
///
/// A scoped checkpoint must never be reused for another scope. If the serving
/// transport identity begins advertising a different source node, consumers
/// discard the position and replay the requested scope from its beginning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedReplicationCheckpoint {
    pub source_node: NodeId,
    pub scope_id: ScopeId,
    pub position: Option<LogPosition>,
}

impl ScopedReplicationCheckpoint {
    /// Creates a source- and scope-bound resume checkpoint.
    #[must_use]
    pub const fn new(
        source_node: NodeId,
        scope_id: ScopeId,
        position: Option<LogPosition>,
    ) -> Self {
        Self {
            source_node,
            scope_id,
            position,
        }
    }
}

/// Result of idempotently applying a replication batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationReport {
    pub source_node: NodeId,
    pub through: Option<LogPosition>,
    pub applied: usize,
    pub duplicates: usize,
}

/// Result of applying one scope-filtered replication batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedReplicationReport {
    pub source_node: NodeId,
    pub scope_id: ScopeId,
    pub through: Option<LogPosition>,
    pub applied: usize,
    pub duplicates: usize,
}

/// One bounded, lexically ordered page of application scope identifiers.
///
/// Transport adapters filter scopes through their access policy before
/// constructing a page. The cursor is the last returned scope when more
/// authorized scopes remain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeCatalogPage {
    pub source_node: NodeId,
    pub scopes: Vec<ScopeId>,
    pub next_after: Option<ScopeId>,
}

impl ScopedReplicationReport {
    /// Returns the checked cursor for the next pull or follow of this scope.
    #[must_use]
    pub fn checkpoint(&self) -> ScopedReplicationCheckpoint {
        ScopedReplicationCheckpoint::new(self.source_node, self.scope_id.clone(), self.through)
    }
}

/// Opaque node-local identity for one transport peer's replay progress.
///
/// Cursor keys are deliberately not part of replicated graph state. A
/// transport chooses a stable namespace and peer identity, while storage
/// adapters persist the resulting local checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplicationCursorKey {
    transport: String,
    peer: String,
}

impl ReplicationCursorKey {
    /// Creates a transport-scoped peer cursor key.
    #[must_use]
    pub fn new(transport: impl Into<String>, peer: impl Into<String>) -> Self {
        Self {
            transport: transport.into(),
            peer: peer.into(),
        }
    }

    /// Returns the transport namespace.
    #[must_use]
    pub fn transport(&self) -> &str {
        &self.transport
    }

    /// Returns the transport-defined stable peer identity.
    #[must_use]
    pub fn peer(&self) -> &str {
        &self.peer
    }
}

/// Durable progress for one transport peer and one source-node history.
///
/// The source identity is part of the checkpoint because a transport peer can
/// be reconfigured with a fresh Myko journal. In that case its positions start
/// over and a follower must not apply the old journal's cursor to the new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationCheckpoint {
    pub source_node: NodeId,
    pub position: Option<LogPosition>,
}

impl ReplicationCheckpoint {
    /// Creates a checkpoint for a source node and its last ingested position.
    #[must_use]
    pub const fn new(source_node: NodeId, position: Option<LogPosition>) -> Self {
        Self {
            source_node,
            position,
        }
    }
}

/// Transport-neutral events observed by replicas, services, and clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeEvent {
    CommandLifecycle(CommandSnapshot),
    CommandCommitted {
        command: CommandSnapshot,
        batch: ChangeBatch,
    },
}

impl NodeEvent {
    /// Returns the application-owned scope affected by this event.
    #[must_use]
    pub const fn scope_id(&self) -> &ScopeId {
        match self {
            Self::CommandLifecycle(command) | Self::CommandCommitted { command, .. } => {
                &command.request.scope_id
            }
        }
    }
}

const fn command_from_event(event: &NodeEvent) -> &CommandSnapshot {
    match event {
        NodeEvent::CommandLifecycle(command) | NodeEvent::CommandCommitted { command, .. } => {
            command
        }
    }
}

const fn command_transition_is_newer(
    current: &CommandSnapshot,
    candidate: &CommandSnapshot,
) -> bool {
    candidate.updated_at.node_id.as_uuid().as_u128()
        == current.updated_at.node_id.as_uuid().as_u128()
        && candidate.updated_at.sequence.get() > current.updated_at.sequence.get()
}

/// Errors raised by the command/history substrate.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NodeError {
    #[error("command ID {0} was reused with different content")]
    CommandConflict(CommandId),
    #[error("unknown command ID {0}")]
    UnknownCommand(CommandId),
    #[error("command ID {command_id} was rejected: {reason}")]
    CommandRejected {
        command_id: CommandId,
        reason: String,
    },
    #[error("command ID {command_id} was cancelled: {reason}")]
    CommandCancelled {
        command_id: CommandId,
        reason: String,
    },
    #[error("command ID {0} is not executing")]
    CommandNotExecuting(CommandId),
    #[error("command ID {command_id} originated on foreign node {origin}")]
    ForeignCommand {
        command_id: CommandId,
        origin: NodeId,
    },
    #[error("change batch does not match admitted command {0}")]
    BatchMismatch(CommandId),
    #[error("invalid item mutation: {0}")]
    InvalidItemMutation(String),
    #[error(
        "item service mismatch: command belongs to {command_service}, item belongs to {item_service}"
    )]
    ItemServiceMismatch {
        command_service: String,
        item_service: &'static str,
    },
    #[error("invalid item-state page: {0}")]
    InvalidItemState(String),
    #[error("invalid command-state page: {0}")]
    InvalidCommandState(String),
    #[error("command payload encoding failed: {0}")]
    CommandEncoding(String),
    #[error("command payload decoding failed: {0}")]
    CommandDecoding(String),
    #[error(
        "command schema mismatch: expected {expected_service}/{expected_command}, got {actual_service}/{actual_command}"
    )]
    CommandSchemaMismatch {
        expected_service: &'static str,
        expected_command: &'static str,
        actual_service: String,
        actual_command: String,
    },
    #[error("command result encoding failed: {0}")]
    ResultEncoding(String),
    #[error("command result decoding failed: {0}")]
    ResultDecoding(String),
    #[error("node state lock is poisoned")]
    Poisoned,
    #[error("node log position space is exhausted")]
    PositionExhausted,
    #[error("event subscription is disconnected")]
    SubscriptionDisconnected,
    #[error("live-event hub state is poisoned")]
    LiveEventHubPoisoned,
    #[error("live-event sequence space is exhausted")]
    LiveEventSequenceExhausted,
    #[error("backend error: {0}")]
    Backend(String),
    #[error("corrupt event history: {0}")]
    CorruptHistory(String),
    #[error("invalid replication batch: {0}")]
    InvalidReplicationBatch(String),
}

/// Durable append-only storage used by the reference event-sourced backend.
///
/// Implementations must make a successful [`Self::append`] durable before
/// returning. Events are supplied in strictly increasing node-local position
/// order and must be replayed in that same order after restart.
pub trait EventJournal: Send + Sync + 'static {
    /// Returns the stable identity stored with this journal.
    ///
    /// # Errors
    ///
    /// Returns an error if journal metadata cannot be read.
    fn node_id(&self) -> Result<NodeId, NodeError>;

    /// Replays every locally observed event in node-local position order.
    ///
    /// # Errors
    ///
    /// Returns an error if durable history cannot be read or decoded.
    fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError>;

    /// Atomically and durably appends one immutable event.
    ///
    /// # Errors
    ///
    /// Returns an error unless the event has been durably committed.
    fn append(&self, event: &EventEnvelope) -> Result<(), NodeError>;
}

/// Node-local durable checkpoints for transport replication followers.
///
/// A follower may save an identity-only checkpoint after authenticating a
/// source. It must save a positioned checkpoint only after the corresponding
/// batch was ingested successfully. A crash before that save may replay
/// duplicates, which the event substrate handles idempotently; saving a
/// position before ingest could lose history and is therefore forbidden.
pub trait ReplicationCursorStore: Send + Sync + 'static {
    /// Loads the source identity and last successfully ingested position.
    ///
    /// # Errors
    ///
    /// Returns an error when local checkpoint storage cannot be read.
    fn load_checkpoint(
        &self,
        key: &ReplicationCursorKey,
    ) -> Result<Option<ReplicationCheckpoint>, NodeError>;

    /// Durably stores source identity and replication progress for a peer.
    /// Implementations must reject attempts to move backwards within the same
    /// source history, but must allow a different source identity to replace a
    /// prior checkpoint and begin again without a position.
    ///
    /// # Errors
    ///
    /// Returns an error unless the checkpoint is durable before returning.
    fn save_checkpoint(
        &self,
        key: &ReplicationCursorKey,
        checkpoint: ReplicationCheckpoint,
    ) -> Result<(), NodeError>;
}

/// Replay followed by lossless live delivery from the same logical cursor.
pub struct EventSubscription {
    backlog: VecDeque<EventEnvelope>,
    live: flume::Receiver<EventEnvelope>,
}

impl EventSubscription {
    /// Receives the next replayed or live event.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub fn recv(&mut self) -> Result<EventEnvelope, NodeError> {
        if let Some(event) = self.backlog.pop_front() {
            return Ok(event);
        }
        self.live
            .recv()
            .map_err(|_| NodeError::SubscriptionDisconnected)
    }

    /// Attempts to receive without blocking.
    #[must_use]
    pub fn try_recv(&mut self) -> Option<EventEnvelope> {
        self.backlog
            .pop_front()
            .or_else(|| self.live.try_recv().ok())
    }

    /// Receives the next event until `timeout` elapses.
    ///
    /// A timeout is reported as `Ok(None)`; a closed backend remains an error.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<EventEnvelope>, NodeError> {
        if let Some(event) = self.backlog.pop_front() {
            return Ok(Some(event));
        }
        match self.live.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
            Err(flume::RecvTimeoutError::Timeout) => Ok(None),
            Err(flume::RecvTimeoutError::Disconnected) => Err(NodeError::SubscriptionDisconnected),
        }
    }

    /// Asynchronously receives the next replayed or live event.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub async fn recv_async(&mut self) -> Result<EventEnvelope, NodeError> {
        if let Some(event) = self.backlog.pop_front() {
            return Ok(event);
        }
        self.live
            .recv_async()
            .await
            .map_err(|_| NodeError::SubscriptionDisconnected)
    }
}

/// Gap-free notification stream for committed application item changes.
///
/// Command admission, execution, retry, and cancellation transitions are
/// intentionally hidden. Application supervisors can use this as an opaque
/// dependency wakeup without inspecting event envelopes or feeding a
/// command's own retry lifecycle back into its dispatch loop.
pub struct ItemChangeSubscription {
    events: EventSubscription,
}

impl ItemChangeSubscription {
    /// Receives the position of the next committed item change.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub fn recv(&mut self) -> Result<LogPosition, NodeError> {
        loop {
            let envelope = self.events.recv()?;
            if event_changes_items(&envelope.event) {
                return Ok(envelope.position);
            }
        }
    }

    /// Receives the next committed item change until `timeout` elapses.
    ///
    /// Unrelated command lifecycle events do not restart the timeout.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<LogPosition>, NodeError> {
        let deadline = Instant::now().checked_add(timeout);
        loop {
            let remaining = deadline.map_or(timeout, |deadline| {
                deadline.saturating_duration_since(Instant::now())
            });
            let Some(envelope) = self.events.recv_timeout(remaining)? else {
                return Ok(None);
            };
            if event_changes_items(&envelope.event) {
                return Ok(Some(envelope.position));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(None);
            }
        }
    }

    /// Asynchronously receives the position of the next committed item change.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::SubscriptionDisconnected`] if the backend closes.
    pub async fn recv_async(&mut self) -> Result<LogPosition, NodeError> {
        loop {
            let envelope = self.events.recv_async().await?;
            if event_changes_items(&envelope.event) {
                return Ok(envelope.position);
            }
        }
    }

    /// Attempts to receive a committed item change without blocking.
    #[must_use]
    pub fn try_recv(&mut self) -> Option<LogPosition> {
        while let Some(envelope) = self.events.try_recv() {
            if event_changes_items(&envelope.event) {
                return Some(envelope.position);
            }
        }
        None
    }
}

const fn event_changes_items(event: &NodeEvent) -> bool {
    matches!(
        event,
        NodeEvent::CommandCommitted { batch, .. } if !batch.changes.is_empty()
    )
}

/// Gap-free local work feed for one application service or command contract.
///
/// The initial queue is materialized from a bounded history prefix. New local
/// submissions and durable retries then arrive through the node's lossless
/// event subscription. Replicated commands are projections and never enter
/// this executable feed.
pub struct PendingCommandSubscription {
    local_node: NodeId,
    service_id: Option<ServiceId>,
    command_type: Option<String>,
    pending: VecDeque<CommandSnapshot>,
    events: EventSubscription,
}

impl PendingCommandSubscription {
    /// Returns the exact service filter, or `None` when every local
    /// application command is observed.
    #[must_use]
    pub const fn service_id(&self) -> Option<&ServiceId> {
        self.service_id.as_ref()
    }

    /// Returns the exact command filter, or `None` for all service commands.
    #[must_use]
    pub fn command_type(&self) -> Option<&str> {
        self.command_type.as_deref()
    }

    /// Receives the next currently executable local command.
    ///
    /// A competing handler may advance the lifecycle before the caller claims
    /// it. Myko's admission API resolves that race idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying event subscription closes.
    pub fn recv(&mut self) -> Result<CommandSnapshot, NodeError> {
        loop {
            if let Some(command) = self.pending.pop_front() {
                return Ok(command);
            }
            let envelope = self.events.recv()?;
            if let Some(command) = self.match_pending(&envelope) {
                return Ok(command);
            }
        }
    }

    /// Asynchronously receives the next currently executable local command.
    ///
    /// This is the cancellation-friendly service-loop boundary for async node
    /// compositions. It preserves the same replay-first and local-origin
    /// guarantees as [`Self::recv`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying event subscription closes.
    pub async fn recv_async(&mut self) -> Result<CommandSnapshot, NodeError> {
        loop {
            if let Some(command) = self.pending.pop_front() {
                return Ok(command);
            }
            let envelope = self.events.recv_async().await?;
            if let Some(command) = self.match_pending(&envelope) {
                return Ok(command);
            }
        }
    }

    /// Receives local work until the total timeout elapses.
    ///
    /// Unrelated federation events do not restart the timeout. A timeout is
    /// reported as `Ok(None)` so a supervisor can check its shutdown signal.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying event subscription closes.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<CommandSnapshot>, NodeError> {
        if let Some(command) = self.pending.pop_front() {
            return Ok(Some(command));
        }
        let deadline = Instant::now().checked_add(timeout);
        loop {
            let remaining = deadline.map_or(timeout, |deadline| {
                deadline.saturating_duration_since(Instant::now())
            });
            let Some(envelope) = self.events.recv_timeout(remaining)? else {
                return Ok(None);
            };
            if let Some(command) = self.match_pending(&envelope) {
                return Ok(Some(command));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(None);
            }
        }
    }

    /// Attempts to receive local work without blocking.
    #[must_use]
    pub fn try_recv(&mut self) -> Option<CommandSnapshot> {
        if let Some(command) = self.pending.pop_front() {
            return Some(command);
        }
        while let Some(envelope) = self.events.try_recv() {
            if let Some(command) = self.match_pending(&envelope) {
                return Some(command);
            }
        }
        None
    }

    fn match_pending(&self, envelope: &EventEnvelope) -> Option<CommandSnapshot> {
        if envelope.origin.node_id != self.local_node {
            return None;
        }
        let command = command_from_event(&envelope.event);
        if self
            .service_id
            .as_ref()
            .is_some_and(|expected| command.request.service_id != *expected)
            || self
                .command_type
                .as_deref()
                .is_some_and(|expected| command.request.command_type != expected)
            || !matches!(
                command.state,
                CommandState::Submitted | CommandState::Retrying { .. }
            )
        {
            return None;
        }
        Some(command.clone())
    }
}

fn materialize_pending_local_commands(
    history: &[EventEnvelope],
    local_node: NodeId,
    service_id: Option<&ServiceId>,
    command_type: Option<&str>,
) -> VecDeque<CommandSnapshot> {
    let mut current = HashMap::<CommandId, (LogPosition, CommandSnapshot)>::new();
    for envelope in history {
        if envelope.origin.node_id != local_node {
            continue;
        }
        let command = command_from_event(&envelope.event);
        if service_id.is_some_and(|expected| command.request.service_id != *expected)
            || command_type.is_some_and(|expected| command.request.command_type != expected)
        {
            continue;
        }
        match current.entry(command.request.id) {
            Entry::Vacant(entry) => {
                entry.insert((envelope.position, command.clone()));
            }
            Entry::Occupied(mut entry) => {
                if command_transition_is_newer(&entry.get().1, command) {
                    entry.get_mut().1 = command.clone();
                }
            }
        }
    }
    let mut pending = current
        .into_values()
        .filter(|(_, command)| {
            matches!(
                command.state,
                CommandState::Submitted | CommandState::Retrying { .. }
            )
        })
        .collect::<Vec<_>>();
    pending.sort_unstable_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            left.1
                .request
                .id
                .to_string()
                .cmp(&right.1.request.id.to_string())
        })
    });
    pending.into_iter().map(|(_, command)| command).collect()
}

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
    pub fn query<Q>(&self, query: Q) -> Result<ItemQuerySnapshot<Q::Output>, NodeError>
    where
        Q: ItemQuery,
    {
        if self.request.service_id.as_str() != Q::Item::SERVICE_ID.as_str()
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
                .apply_at_order(
                    &item.mutation,
                    item.last_changed_at.get(),
                    item.change_index,
                )
                .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?;
        }
        Ok(ItemQuerySnapshot {
            through: self.through,
            value: query.execute(&projection),
        })
    }

    fn from_first_page(page: ItemStatePage) -> Result<(Self, Option<ItemStateRequest>), NodeError> {
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

    fn append_page(
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
        if command.request.service_id != self.service_id
            || command.request.scope_id != self.scope_id
        {
            return Ok(None);
        }
        let mut changes = Vec::new();
        for mutation in &batch.changes {
            if mutation.item_type != self.item_type {
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
    ) -> Result<(ItemQuerySnapshot<Q::Output>, Self), NodeError> {
        if snapshot.request.service_id.as_str() != Q::Item::SERVICE_ID.as_str()
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
                .apply_at_order(
                    &item.mutation,
                    item.last_changed_at.get(),
                    item.change_index,
                )
                .map_err(|error| NodeError::InvalidItemState(error.to_string()))?;
        }
        let value = query.clone().execute(&projection);
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
    pub fn current(&self) -> Q::Output {
        self.query.clone().execute(&self.projection)
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
    ) -> Result<ItemQueryUpdate<Q::Output>, NodeError> {
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
                .apply_at_order(mutation, update.through.get(), change_index)
                .map_err(|error| NodeError::InvalidItemState(error.to_string()))?
            {
                return Err(NodeError::InvalidItemState(
                    "item update contained another item type".to_owned(),
                ));
            }
        }
        let value = self.query.clone().execute(&projection);
        self.projection = projection;
        self.through = Some(update.through);
        Ok(ItemQueryUpdate {
            position: update.through,
            value,
        })
    }
}

fn validate_command_state_entry(
    request: &CommandWatchRequest,
    through: Option<LogPosition>,
    entry: &CommandStateEntry,
) -> Result<(), NodeError> {
    if entry.command.updated_at.node_id != request.source_node
        || entry.command.request.service_id != request.service_id
        || entry.command.request.scope_id != request.scope_id
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

fn validate_command_update(
    request: &CommandWatchRequest,
    update: &CommandStateUpdate,
) -> Result<(), NodeError> {
    if update.command.updated_at.node_id != request.source_node
        || update.command.request.service_id != request.service_id
        || update.command.request.scope_id != request.scope_id
        || update.command.request.command_type != request.command_type
    {
        return Err(NodeError::InvalidCommandState(
            "command stream update does not match its source or contract".to_owned(),
        ));
    }
    Ok(())
}

fn validate_command_state_request(request: &CommandStateRequest) -> Result<(), NodeError> {
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

fn materialize_command_state_entries(
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
            || command.request.scope_id != request.scope_id
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

fn next_command_state_request(
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
            || entry.command.request.scope_id != page.request.scope_id
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

fn materialize_item_state_entries(
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
        if command.request.service_id != request.service_id
            || command.request.scope_id != request.scope_id
        {
            continue;
        }
        for (index, mutation) in batch.changes.into_iter().enumerate() {
            if mutation.item_type != request.item_type {
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

/// Replay-then-live typed query materialization over a typed projection.
///
/// The application sees generated query results rather than federation
/// envelopes. Each update is emitted only after its complete atomic batch has
/// been applied to the typed projection.
pub struct ItemQueryWatch<Q: ItemQuery> {
    query: Q,
    projection: ItemProjection<Q::Item>,
    source_node: Option<NodeId>,
    service_id: ServiceId,
    scope_id: Option<ScopeId>,
    events: EventSubscription,
}

impl<Q: ItemQuery> ItemQueryWatch<Q> {
    /// Computes the query's current value without advancing the subscription.
    #[must_use]
    pub fn current(&self) -> Q::Output {
        self.query.clone().execute(&self.projection)
    }

    /// Waits for the next atomic batch that changes this item projection.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription closes or matching item history is
    /// malformed.
    pub fn recv(&mut self) -> Result<ItemQueryUpdate<Q::Output>, NodeError> {
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
    pub async fn recv_async(&mut self) -> Result<ItemQueryUpdate<Q::Output>, NodeError> {
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
    ) -> Result<Option<ItemQueryUpdate<Q::Output>>, NodeError> {
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
    pub fn try_recv(&mut self) -> Result<Option<ItemQueryUpdate<Q::Output>>, NodeError> {
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
    ) -> Result<Option<ItemQueryUpdate<Q::Output>>, NodeError> {
        let advances_cursor = match &envelope.event {
            NodeEvent::CommandCommitted { command, .. } => {
                self.source_node
                    .is_none_or(|source_node| envelope.origin.node_id == source_node)
                    && command.request.service_id == self.service_id
                    && self
                        .scope_id
                        .as_ref()
                        .is_none_or(|scope_id| command.request.scope_id == *scope_id)
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
            value: self.query.clone().execute(&self.projection),
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

/// Pluggable atomic command log and subscription backend.
pub trait NodeBackend: Send + Sync + 'static {
    /// Returns this node's stable identity.
    fn node_id(&self) -> NodeId;

    /// Durably submits a command without granting execution to the caller.
    ///
    /// # Errors
    ///
    /// Returns an error on storage failure or conflicting reuse of a command ID.
    fn submit(&self, request: CommandRequest) -> Result<CommandSnapshot, NodeError>;

    /// Atomically claims a submitted command for local execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown or backend state cannot be updated.
    fn claim(&self, command_id: CommandId) -> Result<CommandAdmission, NodeError>;

    /// Atomically admits a stable command or returns its existing lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an error on storage failure or conflicting reuse of a command ID.
    fn admit(&self, request: CommandRequest) -> Result<CommandAdmission, NodeError>;

    /// Atomically commits the command result and its complete change batch.
    ///
    /// # Errors
    ///
    /// Returns an error when the command cannot be committed atomically.
    fn commit(
        &self,
        command_id: CommandId,
        batch: ChangeBatch,
        result: Vec<u8>,
    ) -> Result<CommandSnapshot, NodeError>;

    /// Rejects an executing command before any authoritative change is committed.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is absent or no longer executing.
    fn reject(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError>;

    /// Releases an executing command for a later attempt after a transient
    /// handler failure.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is absent, no longer executing, or
    /// the retry lifecycle cannot be durably appended.
    fn retry(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError>;

    /// Cancels submitted or executing work without committing graph changes.
    ///
    /// A terminal command is returned unchanged, making repeated cancellation
    /// idempotent and allowing callers to detect a commit that won the race.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is absent or storage cannot append the
    /// terminal lifecycle event.
    fn cancel(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError>;

    /// Reads the current lifecycle of a command.
    ///
    /// # Errors
    ///
    /// Returns an error when backend state cannot be read.
    fn command(&self, command_id: CommandId) -> Result<Option<CommandSnapshot>, NodeError>;

    /// Returns the node that originated a command's first immutable event.
    ///
    /// Backends may override this history-derived default with an index.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    fn command_origin(&self, command_id: CommandId) -> Result<Option<NodeId>, NodeError> {
        Ok(self.events_after(None)?.into_iter().find_map(|envelope| {
            let command = match envelope.event {
                NodeEvent::CommandLifecycle(command)
                | NodeEvent::CommandCommitted { command, .. } => command,
            };
            (command.request.id == command_id).then_some(envelope.origin.node_id)
        }))
    }

    /// Reads immutable events after an exclusive cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    fn events_after(&self, after: Option<LogPosition>) -> Result<Vec<EventEnvelope>, NodeError>;

    /// Subscribes without a replay/live race. The cursor is exclusive.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot establish the subscription.
    fn subscribe(&self, after: Option<LogPosition>) -> Result<EventSubscription, NodeError>;

    /// Idempotently ingests an immutable event received from another node.
    ///
    /// # Errors
    ///
    /// Returns an error when replicated history conflicts with an existing
    /// stable command ID or contains an invalid change batch.
    fn ingest(&self, event: EventEnvelope) -> Result<IngestStatus, NodeError>;
}

/// Cloneable application handle to a transport- and storage-neutral node.
#[derive(Clone)]
pub struct Node {
    backend: Arc<dyn NodeBackend>,
    readiness: Arc<NodeReadiness>,
    command_dispatch: Arc<Mutex<()>>,
}

#[derive(Debug, Default)]
struct NodeReadiness {
    startup_gates: AtomicUsize,
    waiters: Mutex<Vec<Waker>>,
}

/// RAII ownership of one unfinished node-startup phase.
///
/// Every transport waits until all startup gates have been released before it
/// serves application or federation requests. Dropping the guard releases its
/// phase, including during error unwinding.
#[derive(Debug)]
pub struct NodeStartupGuard {
    readiness: Arc<NodeReadiness>,
    released: bool,
}

impl NodeStartupGuard {
    /// Completes this startup phase and wakes transports when it was the last.
    pub fn ready(mut self) {
        self.release();
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        if self.readiness.startup_gates.fetch_sub(1, Ordering::AcqRel) == 1 {
            let mut registered = self
                .readiness
                .waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let waiters = std::mem::take(&mut *registered);
            drop(registered);
            for waiter in waiters {
                waiter.wake();
            }
        }
    }
}

impl Drop for NodeStartupGuard {
    fn drop(&mut self) {
        self.release();
    }
}

struct NodeReadyFuture {
    readiness: Arc<NodeReadiness>,
}

impl Future for NodeReadyFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.readiness.startup_gates.load(Ordering::Acquire) == 0 {
            return Poll::Ready(());
        }
        let mut waiters = self
            .readiness
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.readiness.startup_gates.load(Ordering::Acquire) == 0 {
            return Poll::Ready(());
        }
        if !waiters
            .iter()
            .any(|waiter| waiter.will_wake(context.waker()))
        {
            waiters.push(context.waker().clone());
        }
        Poll::Pending
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Node")
            .field("node_id", &self.node_id())
            .finish_non_exhaustive()
    }
}

/// A typed application command plus Myko-owned admission metadata.
#[cfg(test)]
#[derive(Debug, Clone)]
struct DeclaredCommand<C: MykoCommand> {
    id: CommandId,
    scope_id: ScopeId,
    principal_id: PrincipalId,
    body: C,
}

#[cfg(test)]
impl<C: MykoCommand> DeclaredCommand<C> {
    /// Creates a typed command ready for submission through any transport.
    #[must_use]
    pub const fn new(id: CommandId, scope_id: ScopeId, principal_id: PrincipalId, body: C) -> Self {
        Self {
            id,
            scope_id,
            principal_id,
            body,
        }
    }

    /// Encodes the declared command into the transport-neutral request shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the typed body cannot be serialized.
    pub fn request(&self) -> Result<CommandRequest, NodeError> {
        Ok(CommandRequest {
            id: self.id,
            service_id: ServiceId::new(C::SERVICE_ID),
            scope_id: self.scope_id.clone(),
            principal_id: self.principal_id.clone(),
            command_type: C::COMMAND_TYPE.to_owned(),
            payload: serde_json::to_vec(&self.body)
                .map_err(|error| NodeError::CommandEncoding(error.to_string()))?,
        })
    }
}

/// Boxed transport-neutral command operation.
pub type CommandClientFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CommandResponse, E>> + Send + 'a>>;

/// Boxed update from a transport-neutral command lifecycle subscription.
pub type CommandSubscriptionFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CommandSnapshot, E>> + Send + 'a>>;

/// Boxed setup of one transport-neutral command lifecycle subscription.
pub type CommandWatchFuture<'a, S, E> = Pin<Box<dyn Future<Output = Result<S, E>> + Send + 'a>>;

/// Boxed typed completion of one submitted application command.
pub type TypedCommandClientFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Common command surface implemented by embedded and remote node clients.
///
/// Applications submit and observe the same stable command contract whether
/// the endpoint is in-process, native Iroh, or an optional short-lived edge
/// adapter. Claiming and execution are intentionally absent from this client
/// interface.
pub trait CommandClient: Send + Sync {
    type Error: From<NodeError> + Send + 'static;

    /// Durably submits one transport-authenticated wire value without claiming execution.
    #[doc(hidden)]
    fn submit_submission(&self, command: CommandSubmission)
    -> CommandClientFuture<'_, Self::Error>;

    /// Reads the current durable lifecycle for a stable command ID.
    fn command_state(&self, command_id: CommandId) -> CommandClientFuture<'_, Self::Error>;

    /// Durably cancels submitted or executing work.
    fn cancel_command(
        &self,
        command_id: CommandId,
        reason: String,
    ) -> CommandClientFuture<'_, Self::Error>;

    /// Submits a typed application command without exposing its wire envelope.
    #[doc(hidden)]
    fn submit_typed_command<C>(&self, command: C) -> CommandClientFuture<'_, Self::Error>
    where
        Self: Sized,
        C: MykoCommand,
    {
        let submission = CommandSubmission::for_command(&command).map_err(Self::Error::from);
        Box::pin(async move { self.submit_submission(submission?).await })
    }
}

/// Current-then-live command lifecycle independent of its transport.
pub trait CommandSubscription: Send {
    type Error: From<NodeError> + Send + 'static;

    /// Returns the latest coherently observed durable state.
    fn current(&self) -> &CommandSnapshot;

    /// Waits for the next durable lifecycle transition.
    fn recv(&mut self) -> CommandSubscriptionFuture<'_, Self::Error>;
}

/// Command client that can watch one command through its typed result.
///
/// The default execution helper owns admission/watch races and typed result
/// decoding so application clients never inspect command IDs, wire results, or
/// lifecycle variants.
pub trait CommandWatchingClient: CommandClient {
    type Subscription: CommandSubscription<Error = Self::Error>;

    /// Opens a gap-free current-then-live lifecycle subscription.
    fn watch_command(
        &self,
        command_id: CommandId,
    ) -> CommandWatchFuture<'_, Self::Subscription, Self::Error>;

    /// Submits a command and watches it until its typed result is durable.
    #[doc(hidden)]
    fn exec_typed_command<C>(
        &self,
        command: C,
    ) -> TypedCommandClientFuture<'_, C::Output, Self::Error>
    where
        Self: Sized,
        C: MykoCommand,
    {
        let submission = CommandSubmission::for_command(&command).map_err(Self::Error::from);
        Box::pin(async move {
            let submission = submission?;
            let command_id = submission.id;
            let response = self.submit_submission(submission).await?;
            let current = response
                .command
                .ok_or_else(|| Self::Error::from(NodeError::UnknownCommand(command_id)))?;
            if let Some(result) = current.typed_completion::<C>().map_err(Self::Error::from)? {
                return Ok(result);
            }
            let mut subscription = self.watch_command(command_id).await?;
            loop {
                if let Some(result) = subscription
                    .current()
                    .typed_completion::<C>()
                    .map_err(Self::Error::from)?
                {
                    return Ok(result);
                }
                let _updated = subscription.recv().await?;
            }
        })
    }
}

impl CommandSubscription for CommandWatch {
    type Error = NodeError;

    fn current(&self) -> &CommandSnapshot {
        &self.current
    }

    fn recv(&mut self) -> CommandSubscriptionFuture<'_, Self::Error> {
        Box::pin(self.recv_async())
    }
}

/// Boxed transport-neutral command-catalog page operation.
pub type CommandStatePageFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CommandStatePage, E>> + Send + 'a>>;

/// Boxed transport-neutral complete command-catalog operation.
pub type CommandStateFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CommandStateSnapshot, E>> + Send + 'a>>;

/// Common bounded command-catalog surface for embedded and remote clients.
pub trait CommandStateClient: Send + Sync {
    type Error: From<NodeError> + Send + 'static;

    /// Reads one bounded page of current command lifecycle state.
    fn command_state_page(
        &self,
        request: CommandStateRequest,
    ) -> CommandStatePageFuture<'_, Self::Error>;

    /// Reads every page of one cursor-stable current command catalog.
    fn command_states(&self, request: CommandStateRequest) -> CommandStateFuture<'_, Self::Error>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let first = self.command_state_page(request).await?;
            let (mut snapshot, mut next) =
                CommandStateSnapshot::from_first_page(first).map_err(Self::Error::from)?;
            while let Some(request) = next {
                let page = self.command_state_page(request.clone()).await?;
                next = snapshot
                    .append_page(&request, page)
                    .map_err(Self::Error::from)?;
            }
            Ok(snapshot)
        })
    }
}

impl CommandStateClient for Node {
    type Error = NodeError;

    fn command_state_page(
        &self,
        request: CommandStateRequest,
    ) -> CommandStatePageFuture<'_, Self::Error> {
        Box::pin(std::future::ready(Self::command_state_page(self, request)))
    }
}

/// Boxed transport-neutral current-state page operation.
pub type ItemStatePageFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<ItemStatePage, E>> + Send + 'a>>;

/// Boxed transport-neutral complete current-state operation.
pub type ItemStateFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<ItemStateSnapshot, E>> + Send + 'a>>;

/// Boxed transport-neutral typed current-state operation.
pub type TypedItemClientFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<ItemQuerySnapshot<T>, E>> + Send + 'a>>;

/// Common one-shot typed state surface for embedded and remote node clients.
///
/// Lossless watches still use replication/follow streams. This facade gives
/// short-lived clients a bounded current projection without importing or
/// decoding application history.
pub trait ItemClient: Send + Sync {
    type Error: From<NodeError> + Send + 'static;

    /// Reads one bounded schema-specific current-state page.
    fn item_state_page(&self, request: ItemStateRequest) -> ItemStatePageFuture<'_, Self::Error>;

    /// Reads every page of one cursor-stable current-state snapshot.
    fn item_state(&self, request: ItemStateRequest) -> ItemStateFuture<'_, Self::Error>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let first = self.item_state_page(request).await?;
            let (mut snapshot, mut next) =
                ItemStateSnapshot::from_first_page(first).map_err(Self::Error::from)?;
            while let Some(request) = next {
                let page = self.item_state_page(request.clone()).await?;
                next = snapshot
                    .append_page(&request, page)
                    .map_err(Self::Error::from)?;
            }
            Ok(snapshot)
        })
    }

    /// Reads and executes a generated typed query through the common client.
    fn query_items<'a, Q>(
        &'a self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> TypedItemClientFuture<'a, Q::Output, Self::Error>
    where
        Self: Sized,
        Q: ItemQuery + Send + 'a,
        Q::Item: Send,
        Q::Output: Send + 'a,
    {
        let request = ItemStateRequest::for_item::<Q::Item>(source_node, scope_id);
        Box::pin(async move {
            self.item_state(request)
                .await?
                .query(query)
                .map_err(Self::Error::from)
        })
    }

    /// Reads the serving node's authoritative items and executes a typed query.
    fn query_serving_items<'a, Q>(
        &'a self,
        scope_id: ScopeId,
        query: Q,
    ) -> TypedItemClientFuture<'a, Q::Output, Self::Error>
    where
        Self: Sized,
        Q: ItemQuery + Send + 'a,
        Q::Item: Send,
        Q::Output: Send + 'a,
    {
        let request = ItemStateRequest::for_serving_item::<Q::Item>(scope_id);
        Box::pin(async move {
            self.item_state(request)
                .await?
                .query(query)
                .map_err(Self::Error::from)
        })
    }
}

impl ItemClient for Node {
    type Error = NodeError;

    fn item_state_page(&self, request: ItemStateRequest) -> ItemStatePageFuture<'_, Self::Error> {
        Box::pin(std::future::ready(Self::item_state_page(self, request)))
    }
}

/// Result of claiming a locally originated declared command.
#[derive(Debug)]
pub enum DeclaredCommandAdmission<C: MykoCommand> {
    /// Execute the decoded application command exactly once.
    Execute(DeclaredCommandContext<C>),
    /// The command already has a durable lifecycle; do not execute it again.
    Resume(CommandSnapshot),
}

/// How one pending declared command was resolved by framework dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandDispatchDisposition {
    /// The application handler emitted and committed its typed result.
    Committed,
    /// The framework durably rejected a malformed body or handler failure.
    Rejected,
    /// The handler durably released a transient failure for another attempt.
    Retrying,
    /// Another claimant already advanced the command lifecycle.
    Resumed,
}

/// Application-selected lifecycle for a declared handler failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandHandlerError {
    /// The command is invalid for the domain and must become terminal.
    Reject(String),
    /// The command remains valid but a transient dependency is unavailable.
    Retry(String),
}

impl CommandHandlerError {
    #[must_use]
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject(reason.into())
    }

    #[must_use]
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry(reason.into())
    }
}

impl From<String> for CommandHandlerError {
    fn from(reason: String) -> Self {
        Self::Reject(reason)
    }
}

/// Durable outcome of dispatching one pending declared command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandDispatchResult {
    pub command: CommandSnapshot,
    pub disposition: CommandDispatchDisposition,
}

impl CommandDispatchResult {
    /// Returns the framework-owned stable identity without exposing its wire request.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command.request.id
    }

    /// Returns the current durable lifecycle without exposing its wire request.
    #[must_use]
    pub const fn state(&self) -> &CommandState {
        &self.command.state
    }
}

/// Myko-owned execution context paired with a decoded command body.
#[derive(Debug)]
pub struct DeclaredCommandContext<C: MykoCommand> {
    inner: CommandContext,
    body: C,
}

impl<C: MykoCommand> DeclaredCommandContext<C> {
    /// Returns the decoded application command body.
    #[must_use]
    pub const fn body(&self) -> &C {
        &self.body
    }

    /// Returns a cloneable atomic command capability substrate.
    #[doc(hidden)]
    #[must_use]
    pub const fn command_context(&self) -> &CommandContext {
        &self.inner
    }

    /// Returns the immutable Myko request metadata.
    #[must_use]
    pub const fn request(&self) -> &CommandRequest {
        self.inner.request()
    }

    /// Adds a typed item replacement to this command's atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item cannot be encoded.
    pub fn emit_set<T: MykoItem>(&mut self, item: &T) -> Result<(), NodeError> {
        self.inner.emit_set(item)
    }

    /// Adds a typed item deletion to this command's atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item belongs to another service.
    pub fn emit_delete<T: MykoItem>(&mut self, id: &T::Id) -> Result<(), NodeError> {
        self.inner.emit_delete::<T>(id)
    }

    /// Queries typed current state in this command's service and scope.
    ///
    /// # Errors
    ///
    /// Returns an error if the typed projection cannot be materialized.
    pub fn query<Q: ItemQuery>(&self, query: Q) -> Result<Q::Output, NodeError> {
        self.inner.query(query)
    }

    /// Atomically commits emitted items and this command's declared result.
    ///
    /// # Errors
    ///
    /// Returns an error if result encoding or durable commit fails.
    pub fn commit(self, result: &C::Output) -> Result<CommandSnapshot, NodeError> {
        self.inner.commit(result)
    }

    /// Rejects the command without committing emitted items.
    ///
    /// # Errors
    ///
    /// Returns an error if durable rejection fails.
    pub fn reject(self, reason: impl Into<String>) -> Result<CommandSnapshot, NodeError> {
        self.inner.reject(reason)
    }

    /// Releases a transient failure for another ordered dispatch attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the retry state cannot be durably appended.
    pub fn retry(self, reason: impl Into<String>) -> Result<CommandSnapshot, NodeError> {
        self.inner.retry(reason)
    }
}

fn decode_declared_body<C: MykoCommand>(request: &CommandRequest) -> Result<C, NodeError> {
    if request.service_id.as_str() != C::SERVICE_ID.as_str()
        || request.command_type != C::COMMAND_TYPE
    {
        return Err(NodeError::CommandSchemaMismatch {
            expected_service: C::SERVICE_ID.as_str(),
            expected_command: C::COMMAND_TYPE,
            actual_service: request.service_id.as_str().to_owned(),
            actual_command: request.command_type.clone(),
        });
    }
    serde_json::from_slice(&request.payload)
        .map_err(|error| NodeError::CommandDecoding(error.to_string()))
}

fn decode_typed_command_state<C: MykoCommand>(
    entry: &CommandStateEntry,
) -> Result<TypedCommandState<C>, NodeError> {
    let command = entry.command.request.command::<C>()?;
    let result = entry
        .command
        .result
        .as_deref()
        .map(serde_json::from_slice)
        .transpose()
        .map_err(|error| NodeError::ResultDecoding(error.to_string()))?;
    Ok(TypedCommandState {
        admitted_at: entry.admitted_at,
        last_changed_at: entry.last_changed_at,
        id: entry.command.request.id,
        scope_id: entry.command.request.scope_id.clone(),
        principal_id: entry.command.request.principal_id.clone(),
        command,
        state: entry.command.state.clone(),
        result,
        updated_at: entry.command.updated_at,
    })
}

/// Result of claiming a command through Myko's typed application boundary.
#[derive(Debug)]
pub enum TypedCommandAdmission {
    /// This node owns execution and has created an atomic item context.
    Execute(CommandContext),
    /// The command already has a durable lifecycle; do not execute it again.
    Resume(CommandSnapshot),
}

/// Atomic application command context owned by Myko.
///
/// Handlers emit typed item sets/deletes and a typed result. Myko supplies the
/// batch identity, service/scope identity, causal parent, serialization,
/// validation, and durable commit.
#[derive(Debug, Clone)]
pub struct CommandContext {
    node: Node,
    command: CommandSnapshot,
    changes: Arc<Mutex<Vec<ItemMutation>>>,
}

impl CommandContext {
    /// Returns the immutable request being executed.
    #[must_use]
    pub const fn request(&self) -> &CommandRequest {
        &self.command.request
    }

    /// Returns the node executing this command.
    #[doc(hidden)]
    #[must_use]
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// Returns how many typed mutations this command has emitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the shared atomic mutation batch is unavailable.
    pub fn change_count(&self) -> Result<usize, NodeError> {
        self.changes
            .lock()
            .map(|changes| changes.len())
            .map_err(|_| NodeError::Poisoned)
    }

    /// Adds a typed item replacement to this command's atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item cannot be encoded.
    pub fn emit_set<T: MykoItem>(&self, item: &T) -> Result<(), NodeError> {
        self.require_item_service::<T>()?;
        self.changes.lock().map_err(|_| NodeError::Poisoned)?.push(
            ItemMutation::set(item)
                .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?,
        );
        Ok(())
    }

    /// Adds a typed item deletion to this command's atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the item belongs to another service.
    pub fn emit_delete<T: MykoItem>(&self, id: &T::Id) -> Result<(), NodeError> {
        self.require_item_service::<T>()?;
        self.changes
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .push(ItemMutation::delete::<T>(id));
        Ok(())
    }

    /// Executes a generated query against this service/scope's current local
    /// authoritative item state.
    ///
    /// # Errors
    ///
    /// Returns an error if current typed state cannot be materialized.
    pub fn query<Q>(&self, query: Q) -> Result<Q::Output, NodeError>
    where
        Q: ItemQuery,
    {
        self.require_item_service::<Q::Item>()?;
        self.node
            .query_items_in(self.node.node_id(), &self.command.request.scope_id, query)
    }

    fn require_item_service<T: MykoItem>(&self) -> Result<(), NodeError> {
        if self.command.request.service_id.as_str() != T::SERVICE_ID.as_str() {
            return Err(NodeError::ItemServiceMismatch {
                command_service: self.command.request.service_id.as_str().to_owned(),
                item_service: T::SERVICE_ID.as_str(),
            });
        }
        Ok(())
    }

    /// Atomically commits emitted items and a JSON-encoded typed result.
    ///
    /// # Errors
    ///
    /// Returns an error if the result cannot be encoded or the durable commit
    /// fails.
    pub fn commit<R: Serialize>(self, result: &R) -> Result<CommandSnapshot, NodeError> {
        let encoded = serde_json::to_vec(result)
            .map_err(|error| NodeError::ResultEncoding(error.to_string()))?;
        self.commit_bytes(encoded)
    }

    /// Atomically commits emitted items and an application-owned result body.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable commit fails.
    pub fn commit_bytes(self, result: Vec<u8>) -> Result<CommandSnapshot, NodeError> {
        let changes = self
            .changes
            .lock()
            .map_err(|_| NodeError::Poisoned)?
            .clone();
        self.node.commit(
            self.command.request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: self.command.request.id,
                service_id: self.command.request.service_id.clone(),
                scope_id: self.command.request.scope_id.clone(),
                causal_parents: vec![self.command.updated_at],
                changes,
            },
            result,
        )
    }

    /// Rejects this executing command without committing emitted items.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable rejection fails.
    pub fn reject(self, reason: impl Into<String>) -> Result<CommandSnapshot, NodeError> {
        self.node.reject(self.command.request.id, reason)
    }

    /// Releases this execution attempt for a later handler retry.
    ///
    /// Emitted items are discarded; only the retry lifecycle and reason are
    /// durably recorded.
    ///
    /// # Errors
    ///
    /// Returns an error if the retry state cannot be durably appended.
    pub fn retry(self, reason: impl Into<String>) -> Result<CommandSnapshot, NodeError> {
        self.node.retry(self.command.request.id, reason)
    }
}

impl Node {
    /// Creates a node backed by an in-memory immutable log.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::with_backend(Arc::new(InMemoryBackend::new(NodeId::new())))
    }

    /// Opens an event-sourced node over a durable journal.
    ///
    /// The complete command projection and replay log are reconstructed from
    /// immutable history before the node is returned.
    ///
    /// # Errors
    ///
    /// Returns an error when journal metadata or history cannot be recovered.
    pub fn from_journal(journal: Arc<dyn EventJournal>) -> Result<Self, NodeError> {
        Ok(Self::with_backend(Arc::new(InMemoryBackend::from_journal(
            journal,
        )?)))
    }

    /// Creates a node around a storage plugin.
    #[must_use]
    pub fn with_backend(backend: Arc<dyn NodeBackend>) -> Self {
        Self {
            backend,
            readiness: Arc::new(NodeReadiness::default()),
            command_dispatch: Arc::new(Mutex::new(())),
        }
    }

    /// Holds the node below its startup-ready barrier until the guard is
    /// completed or dropped.
    #[must_use]
    pub fn hold_startup(&self) -> NodeStartupGuard {
        self.readiness.startup_gates.fetch_add(1, Ordering::AcqRel);
        NodeStartupGuard {
            readiness: Arc::clone(&self.readiness),
            released: false,
        }
    }

    /// Returns whether every declared startup phase has completed.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.readiness.startup_gates.load(Ordering::Acquire) == 0
    }

    /// Waits without polling until every declared startup phase has completed.
    pub async fn wait_until_ready(&self) {
        NodeReadyFuture {
            readiness: Arc::clone(&self.readiness),
        }
        .await;
    }

    /// Returns the stable node identity.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.backend.node_id()
    }

    /// Durably submits a command without making the client its executor.
    ///
    /// # Errors
    ///
    /// Returns an error on backend failure or conflicting command reuse.
    pub fn submit(&self, request: CommandRequest) -> Result<CommandSnapshot, NodeError> {
        self.backend.submit(request)
    }

    /// Durably submits a typed application command without executing it.
    ///
    /// # Errors
    ///
    /// Returns an error if its body cannot be encoded, storage fails, or its
    /// stable identity conflicts with a different request.
    pub fn submit_command<C: MykoCommand>(
        &self,
        scope_id: ScopeId,
        command: &C,
    ) -> Result<CommandSnapshot, NodeError> {
        self.submit_authenticated_command(
            scope_id,
            PrincipalId::new(format!("node:{}", self.node_id())),
            command,
        )
    }

    /// Submits through a principal already authenticated by a Myko session.
    #[doc(hidden)]
    pub fn submit_authenticated_command<C: MykoCommand>(
        &self,
        scope_id: ScopeId,
        principal_id: PrincipalId,
        command: &C,
    ) -> Result<CommandSnapshot, NodeError> {
        self.submit(CommandRequest::for_command(
            scope_id,
            principal_id,
            command,
        )?)
    }

    /// Atomically claims a submitted command for a local handler.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown or cannot be updated.
    pub fn claim(&self, command_id: CommandId) -> Result<CommandAdmission, NodeError> {
        self.backend.claim(command_id)
    }

    /// Claims a locally originated command and creates a typed atomic item
    /// context for its handler.
    ///
    /// Replicated command events are projections, not executable work on the
    /// observing node. This method enforces that invariant before claiming.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown, originated on another node,
    /// or cannot be claimed durably.
    pub fn begin_command(&self, command_id: CommandId) -> Result<TypedCommandAdmission, NodeError> {
        let origin = self
            .command_origin(command_id)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if origin != self.node_id() {
            return Err(NodeError::ForeignCommand { command_id, origin });
        }
        Ok(match self.claim(command_id)? {
            CommandAdmission::Execute(command) => TypedCommandAdmission::Execute(CommandContext {
                node: self.clone(),
                command,
                changes: Arc::new(Mutex::new(Vec::new())),
            }),
            CommandAdmission::Resume(command) => TypedCommandAdmission::Resume(command),
        })
    }

    /// Claims and decodes a locally originated declared command.
    ///
    /// The service and command wire identities must exactly match `C` before
    /// application code receives the payload.
    ///
    /// # Errors
    ///
    /// Returns an error if admission fails, the command originated elsewhere,
    /// its declared schema does not match, or its payload is malformed.
    pub fn begin_declared_command<C: MykoCommand>(
        &self,
        command_id: CommandId,
    ) -> Result<DeclaredCommandAdmission<C>, NodeError> {
        let snapshot = self
            .command(command_id)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        let body = decode_declared_body::<C>(&snapshot.request)?;
        match self.begin_command(command_id)? {
            TypedCommandAdmission::Execute(context) => {
                Ok(DeclaredCommandAdmission::Execute(DeclaredCommandContext {
                    inner: context,
                    body,
                }))
            }
            TypedCommandAdmission::Resume(snapshot) => {
                Ok(DeclaredCommandAdmission::Resume(snapshot))
            }
        }
    }

    /// Atomically admits an idempotent command.
    ///
    /// # Errors
    ///
    /// Returns an error on backend failure or conflicting command reuse.
    pub fn admit(&self, request: CommandRequest) -> Result<CommandAdmission, NodeError> {
        self.backend.admit(request)
    }

    /// Atomically appends the command's complete authoritative change batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is absent, no longer executing, or the
    /// batch does not match its service and scope.
    pub fn commit(
        &self,
        command_id: CommandId,
        batch: ChangeBatch,
        result: Vec<u8>,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.commit(command_id, batch, result)
    }

    /// Rejects an executing command without committing graph changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is absent or no longer executing.
    pub fn reject(
        &self,
        command_id: CommandId,
        reason: impl Into<String>,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.reject(command_id, reason.into())
    }

    /// Releases an executing command for another ordered handler attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is absent, no longer executing, or the
    /// retry state cannot be durably appended.
    pub fn retry(
        &self,
        command_id: CommandId,
        reason: impl Into<String>,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.retry(command_id, reason.into())
    }

    /// Cancels submitted or executing work without committing graph changes.
    ///
    /// Terminal commands are returned unchanged, so callers can distinguish a
    /// successful cancellation from a commit or rejection that won the race.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is absent or cancellation cannot be
    /// durably recorded.
    pub fn cancel(
        &self,
        command_id: CommandId,
        reason: impl Into<String>,
    ) -> Result<CommandSnapshot, NodeError> {
        self.backend.cancel(command_id, reason.into())
    }

    /// Reads the current lifecycle for a command.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot be read.
    pub fn command(&self, command_id: CommandId) -> Result<Option<CommandSnapshot>, NodeError> {
        self.backend.command(command_id)
    }

    /// Reads one current command state and starts a gap-free lifecycle watch.
    ///
    /// The snapshot is reconstructed from the same bounded history prefix used
    /// to establish the subscription, so a concurrent transition is delivered
    /// after it rather than falling into a query-to-subscribe race.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown or history/subscription
    /// access fails.
    pub fn watch_command(
        &self,
        command_id: CommandId,
    ) -> Result<(CommandResponse, CommandWatch), NodeError> {
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let events = self.subscribe(through)?;
        let current = self
            .command(command_id)?
            .ok_or(NodeError::UnknownCommand(command_id))?;
        Ok((
            CommandResponse {
                source_node: self.node_id(),
                command: Some(current.clone()),
            },
            CommandWatch {
                command_id,
                current,
                events,
            },
        ))
    }

    /// Waits for a command to become visible, then watches its lifecycle
    /// without a visibility-to-subscribe race.
    ///
    /// This is the local-node path for a command submitted through another
    /// mesh peer: the remote response may arrive before replication makes the
    /// command visible in this node's projection. The subscription is opened
    /// before checking current state, so the first replicated lifecycle cannot
    /// be missed.
    ///
    /// # Errors
    ///
    /// Returns an error if history access or the live subscription fails.
    pub async fn watch_command_eventually(
        &self,
        command_id: CommandId,
    ) -> Result<(CommandResponse, CommandWatch), NodeError> {
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let mut events = self.subscribe(through)?;
        let current = match self.command(command_id)? {
            Some(current) => current,
            None => loop {
                let envelope = events.recv_async().await?;
                let command = command_from_event(&envelope.event);
                if command.request.id == command_id {
                    break command.clone();
                }
            },
        };
        Ok((
            CommandResponse {
                source_node: self.node_id(),
                command: Some(current.clone()),
            },
            CommandWatch {
                command_id,
                current,
                events,
            },
        ))
    }

    /// Returns the stable node identity that first originated a command.
    ///
    /// This lets an application distinguish locally admitted work from a
    /// replicated projection before performing a node-local effect.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn command_origin(&self, command_id: CommandId) -> Result<Option<NodeId>, NodeError> {
        self.backend.command_origin(command_id)
    }

    /// Returns locally originated submitted commands for one stable wire
    /// contract in their original admission order.
    ///
    /// Current lifecycle state comes from the backend projection rather than
    /// whichever raw lifecycle event happened to appear last. Replicated
    /// submissions are never returned as locally executable work.
    ///
    /// # Errors
    ///
    /// Returns an error when history or current command state cannot be read,
    /// or when history references a command missing from the projection.
    pub fn pending_local_commands(
        &self,
        service_id: &str,
        command_type: &str,
    ) -> Result<Vec<CommandSnapshot>, NodeError> {
        Ok(self
            .pending_local_service_commands(service_id)?
            .into_iter()
            .filter(|command| command.request.command_type == command_type)
            .collect())
    }

    /// Returns every locally originated submitted command for one service in
    /// original admission order, preserving order across command types.
    ///
    /// # Errors
    ///
    /// Returns an error when history or current command state cannot be read,
    /// or when history references a command missing from the projection.
    pub fn pending_local_service_commands(
        &self,
        service_id: &str,
    ) -> Result<Vec<CommandSnapshot>, NodeError> {
        let history = self.events_after(None)?;
        Ok(materialize_pending_local_commands(
            &history,
            self.node_id(),
            Some(&ServiceId::new(service_id)),
            None,
        )
        .into())
    }

    /// Returns every locally originated submitted application command in its
    /// original admission order, preserving order across services and command
    /// types.
    ///
    /// # Errors
    ///
    /// Returns an error when history or current command state cannot be read,
    /// or when history references a command missing from the projection.
    pub fn pending_local_application_commands(&self) -> Result<Vec<CommandSnapshot>, NodeError> {
        let history = self.events_after(None)?;
        Ok(materialize_pending_local_commands(&history, self.node_id(), None, None).into())
    }

    /// Starts a gap-free work feed for every locally originated command in one
    /// application service.
    ///
    /// The returned feed first yields commands that were still submitted or
    /// retrying at the captured history boundary, then follows new admissions
    /// and retries without polling. Replicated command lifecycles are omitted.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or a lossless event
    /// subscription cannot be established.
    pub fn watch_pending_local_service_commands(
        &self,
        service_id: impl Into<String>,
    ) -> Result<PendingCommandSubscription, NodeError> {
        self.watch_pending_local_commands(Some(ServiceId::new(service_id)), None)
    }

    /// Starts a gap-free work feed for every locally originated application
    /// command, regardless of service or concrete operation.
    ///
    /// This is the framework-facing feed used by a composed application
    /// runtime. Applications should consume their generated handler registry
    /// rather than splitting this feed back into manually named services.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or a lossless event
    /// subscription cannot be established.
    pub fn watch_pending_local_application_commands(
        &self,
    ) -> Result<PendingCommandSubscription, NodeError> {
        self.watch_pending_local_commands(None, None)
    }

    /// Starts a gap-free work feed for one declared command contract.
    ///
    /// Myko owns restart catch-up, local-origin filtering, and the transition
    /// from replay to live delivery. The consuming service only claims and
    /// handles the yielded stable command IDs.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or a lossless event
    /// subscription cannot be established.
    pub fn watch_pending_typed<C: MykoCommand>(
        &self,
    ) -> Result<PendingCommandSubscription, NodeError> {
        self.watch_pending_local_commands(
            Some(ServiceId::new(C::SERVICE_ID)),
            Some(C::COMMAND_TYPE.to_owned()),
        )
    }

    /// Returns every locally executable command body of one typed contract.
    ///
    /// Myko owns service/type filtering and typed request decoding; application
    /// code receives typed values instead of rebuilding wire checks.
    ///
    /// # Errors
    ///
    /// Returns an error when pending history cannot be read or a matching
    /// command does not satisfy its declared wire contract.
    pub fn pending_typed<C: MykoCommand>(&self) -> Result<Vec<C>, NodeError> {
        self.pending_local_commands(C::SERVICE_ID.as_str(), C::COMMAND_TYPE)?
            .iter()
            .map(|command| command.request.command::<C>())
            .collect()
    }

    fn watch_pending_local_commands(
        &self,
        service_id: Option<ServiceId>,
        command_type: Option<String>,
    ) -> Result<PendingCommandSubscription, NodeError> {
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let events = self.subscribe(through)?;
        let local_node = self.node_id();
        let pending = materialize_pending_local_commands(
            &history,
            local_node,
            service_id.as_ref(),
            command_type.as_deref(),
        );
        Ok(PendingCommandSubscription {
            local_node,
            service_id,
            command_type,
            pending,
            events,
        })
    }

    /// Dispatches one command through its declared payload/result contract.
    ///
    /// # Errors
    ///
    /// Returns an error when admission, commit, or durable rejection fails.
    pub fn dispatch_declared_command<C, F>(
        &self,
        command_id: CommandId,
        handle: F,
    ) -> Result<CommandDispatchResult, NodeError>
    where
        C: MykoCommand,
        F: FnOnce(&mut DeclaredCommandContext<C>) -> Result<C::Output, CommandHandlerError>,
    {
        // Claim, execute, and commit are one process-local ownership interval.
        // A competing synchronous caller must observe the terminal result, not
        // the transient `Executing` snapshot produced by the retained driver.
        let _dispatch = self
            .command_dispatch
            .lock()
            .map_err(|_| NodeError::Poisoned)?;
        match self.begin_declared_command::<C>(command_id) {
            Ok(DeclaredCommandAdmission::Execute(mut context)) => {
                let handled = handle(&mut context);
                let (command, disposition) = match handled {
                    Ok(output) => (
                        context.commit(&output)?,
                        CommandDispatchDisposition::Committed,
                    ),
                    Err(CommandHandlerError::Reject(reason)) => (
                        context.reject(format!("declared command handler failed: {reason}"))?,
                        CommandDispatchDisposition::Rejected,
                    ),
                    Err(CommandHandlerError::Retry(reason)) => {
                        (context.retry(reason)?, CommandDispatchDisposition::Retrying)
                    }
                };
                Ok(CommandDispatchResult {
                    command,
                    disposition,
                })
            }
            Ok(DeclaredCommandAdmission::Resume(command)) => Ok(CommandDispatchResult {
                command,
                disposition: CommandDispatchDisposition::Resumed,
            }),
            Err(
                error @ (NodeError::CommandDecoding(_) | NodeError::CommandSchemaMismatch { .. }),
            ) => {
                let reason = format!("invalid declared command: {error}");
                let (command, disposition) = match self.begin_command(command_id)? {
                    TypedCommandAdmission::Execute(context) => (
                        context.reject(reason)?,
                        CommandDispatchDisposition::Rejected,
                    ),
                    TypedCommandAdmission::Resume(command) => {
                        (command, CommandDispatchDisposition::Resumed)
                    }
                };
                Ok(CommandDispatchResult {
                    command,
                    disposition,
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Dispatches every currently pending local command declared as `C`.
    ///
    /// Myko owns ordered discovery, local-origin admission, payload decoding,
    /// atomic commit, and durable rejection. The application closure owns only
    /// domain validation, typed item emission, and the declared result.
    ///
    /// A malformed matching payload is rejected without preventing later
    /// commands from running. Handlers explicitly classify domain rejection
    /// versus a transient retry.
    ///
    /// # Errors
    ///
    /// Returns an error when history, admission, commit, or rejection fails.
    pub fn dispatch_declared<C, F>(
        &self,
        mut handle: F,
    ) -> Result<Vec<CommandDispatchResult>, NodeError>
    where
        C: MykoCommand,
        F: FnMut(&mut DeclaredCommandContext<C>) -> Result<C::Output, CommandHandlerError>,
    {
        let mut results = Vec::new();
        for pending in self.pending_local_commands(C::SERVICE_ID.as_str(), C::COMMAND_TYPE)? {
            results.push(
                self.dispatch_declared_command::<C, _>(pending.request.id, |context| {
                    handle(context)
                })?,
            );
        }
        Ok(results)
    }

    /// Reads immutable events after an exclusive cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot be read.
    pub fn events_after(
        &self,
        after: Option<LogPosition>,
    ) -> Result<Vec<EventEnvelope>, NodeError> {
        self.backend.events_after(after)
    }

    /// Materializes one bounded page of current command states.
    ///
    /// The first page fixes a serving-log ceiling retained by every
    /// continuation, so concurrent lifecycle transitions cannot create gaps or
    /// duplicates in the collected catalog.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid page request or malformed history.
    pub fn command_state_page(
        &self,
        mut request: CommandStateRequest,
    ) -> Result<CommandStatePage, NodeError> {
        validate_command_state_request(&request)?;
        let source_node = request.source_node.unwrap_or_else(|| self.node_id());
        request.source_node = Some(source_node);
        let history = self.events_after(None)?;
        let latest = history.last().map(|envelope| envelope.position);
        let through = match request.snapshot_through {
            Some(requested) if latest.is_none_or(|latest| requested > latest) => {
                return Err(NodeError::InvalidCommandState(format!(
                    "command-state snapshot cursor {} is newer than serving history",
                    requested.get()
                )));
            }
            Some(requested) => Some(requested),
            None => latest,
        };
        request.snapshot_through = through;
        let current = materialize_command_state_entries(history, source_node, &request, through);
        let page_size = usize::try_from(request.page_size).map_err(|error| {
            NodeError::InvalidCommandState(format!(
                "command-state page size is not addressable: {error}"
            ))
        })?;
        let mut commands = current
            .into_iter()
            .filter(|(command_id, _entry)| {
                request
                    .after_command_id
                    .as_deref()
                    .is_none_or(|cursor| command_id.as_str() > cursor)
            })
            .take(page_size.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = commands.len() > page_size;
        if has_more {
            let _overflow = commands.pop();
        }
        let next_after_command_id = has_more
            .then(|| {
                commands
                    .last()
                    .map(|(command_id, _entry)| command_id.clone())
            })
            .flatten();
        let commands = commands
            .into_iter()
            .map(|(_command_id, entry)| entry)
            .collect();
        Ok(CommandStatePage {
            serving_node: self.node_id(),
            through,
            request,
            commands,
            next_after_command_id,
        })
    }

    /// Collects every page of one cursor-stable current command catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if any page changes identity, ordering, or cursor.
    pub fn command_states(
        &self,
        request: CommandStateRequest,
    ) -> Result<CommandStateSnapshot, NodeError> {
        let first = self.command_state_page(request)?;
        let (mut snapshot, mut next) = CommandStateSnapshot::from_first_page(first)?;
        while let Some(request) = next {
            let page = self.command_state_page(request.clone())?;
            next = snapshot.append_page(&request, page)?;
        }
        Ok(snapshot)
    }

    /// Materializes one bounded page of schema-specific current state.
    ///
    /// The first page fixes a node-log ceiling. Continuation requests retain
    /// that ceiling, so commits arriving during pagination cannot create gaps
    /// or duplicates in the collected snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid page request or malformed matching
    /// history.
    pub fn item_state_page(
        &self,
        mut request: ItemStateRequest,
    ) -> Result<ItemStatePage, NodeError> {
        if request.item_type.is_empty()
            || request.schema_version == 0
            || request.page_size == 0
            || request.page_size > MAX_ITEM_STATE_PAGE_SIZE
            || request.after_item_id.as_ref().is_some_and(String::is_empty)
        {
            return Err(NodeError::InvalidItemState(format!(
                "item-state request requires a schema, a non-empty cursor, and a page size between 1 and {MAX_ITEM_STATE_PAGE_SIZE}"
            )));
        }
        let source_node = request.source_node.unwrap_or_else(|| self.node_id());
        request.source_node = Some(source_node);
        let history = self.events_after(None)?;
        let latest = history.last().map(|envelope| envelope.position);
        let through = match request.snapshot_through {
            Some(requested) if latest.is_none_or(|latest| requested > latest) => {
                return Err(NodeError::InvalidItemState(format!(
                    "item-state snapshot cursor {} is newer than serving history",
                    requested.get()
                )));
            }
            Some(requested) => Some(requested),
            None => latest,
        };
        request.snapshot_through = through;
        let current = materialize_item_state_entries(history, source_node, &request, through)?;
        let page_size = usize::try_from(request.page_size).map_err(|error| {
            NodeError::InvalidItemState(format!("item-state page size is not addressable: {error}"))
        })?;
        let mut items = current
            .into_values()
            .filter(|item| {
                request
                    .after_item_id
                    .as_deref()
                    .is_none_or(|cursor| item.mutation.item_id.as_str() > cursor)
            })
            .take(page_size.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = items.len() > page_size;
        if has_more {
            let _overflow = items.pop();
        }
        let next_after_item_id = has_more
            .then(|| items.last().map(|item| item.mutation.item_id.clone()))
            .flatten();
        Ok(ItemStatePage {
            serving_node: self.node_id(),
            through,
            request,
            items,
            next_after_item_id,
        })
    }

    /// Materializes every bounded page of one current-state snapshot locally.
    ///
    /// Transport clients use the equivalent framework-owned asynchronous
    /// collector on [`ItemClient::item_state`].
    ///
    /// # Errors
    ///
    /// Returns an error if any page is invalid or matching history is
    /// malformed.
    pub fn item_state_snapshot(
        &self,
        request: ItemStateRequest,
    ) -> Result<ItemStateSnapshot, NodeError> {
        let first = self.item_state_page(request)?;
        let (mut snapshot, mut next) = ItemStateSnapshot::from_first_page(first)?;
        while let Some(request) = next {
            let page = self.item_state_page(request.clone())?;
            next = snapshot.append_page(&request, page)?;
        }
        Ok(snapshot)
    }

    /// Materializes current state for one typed item schema from all known
    /// local and replicated command batches.
    ///
    /// Applications normally use [`Self::query_items`] rather than handling
    /// federation envelopes or serialized item mutations themselves.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or contains a malformed
    /// mutation for `T`.
    pub fn project_items<T: MykoItem>(&self) -> Result<ItemProjection<T>, NodeError> {
        self.project_items_from::<T>(None)
    }

    /// Materializes one typed schema, optionally restricted to its immutable
    /// source-node identity.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read or contains a malformed
    /// mutation for `T`.
    pub fn project_items_from<T: MykoItem>(
        &self,
        source_node: Option<NodeId>,
    ) -> Result<ItemProjection<T>, NodeError> {
        self.project_items_matching::<T>(source_node, None)
    }

    fn project_items_matching<T: MykoItem>(
        &self,
        source_node: Option<NodeId>,
        service_scope: Option<(&ServiceId, &ScopeId)>,
    ) -> Result<ItemProjection<T>, NodeError> {
        let mut projection = ItemProjection::default();
        for envelope in self.events_after(None)? {
            let _changed =
                apply_item_envelope(&mut projection, &envelope, source_node, service_scope)?;
        }
        Ok(projection)
    }

    /// Executes a generated typed query against current local and replicated
    /// item state.
    ///
    /// # Errors
    ///
    /// Returns an error when item state cannot be materialized from history.
    pub fn query_items<Q>(&self, query: Q) -> Result<Q::Output, NodeError>
    where
        Q: ItemQuery,
    {
        Ok(self.project_items::<Q::Item>()?.query(query))
    }

    /// Executes a generated typed query against one authoritative source's
    /// current item state.
    ///
    /// # Errors
    ///
    /// Returns an error when source state cannot be materialized from history.
    pub fn query_items_from<Q>(&self, source_node: NodeId, query: Q) -> Result<Q::Output, NodeError>
    where
        Q: ItemQuery,
    {
        Ok(self
            .project_items_from::<Q::Item>(Some(source_node))?
            .query(query))
    }

    /// Executes a typed query within one authoritative source, application
    /// service, and federation scope.
    ///
    /// This is the normal application-facing projection boundary: storage and
    /// replicated history stay behind the node while the caller works only
    /// with generated item/query types.
    ///
    /// # Errors
    ///
    /// Returns an error when scoped item state cannot be materialized.
    pub fn query_items_in<Q>(
        &self,
        source_node: NodeId,
        scope_id: &ScopeId,
        query: Q,
    ) -> Result<Q::Output, NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        Ok(self
            .project_items_matching::<Q::Item>(Some(source_node), Some((&service_id, scope_id)))?
            .query(query))
    }

    /// Executes a typed query within one application service and federation
    /// scope across every authoritative source represented in this node.
    ///
    /// This preserves source provenance during ingestion while allowing
    /// naturally federated application state, such as an agent mailbox, to be
    /// consumed without decoding raw history.
    ///
    /// # Errors
    ///
    /// Returns an error when scoped item state cannot be materialized.
    pub fn query_items_across_sources_in<Q>(
        &self,
        scope_id: &ScopeId,
        query: Q,
    ) -> Result<Q::Output, NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        Ok(self
            .project_items_matching::<Q::Item>(None, Some((&service_id, scope_id)))?
            .query(query))
    }

    /// Returns authoritative sources that have changed one typed item schema
    /// in the requested service scope.
    ///
    /// Source discovery is derived inside the framework so applications never
    /// inspect command envelopes or serialized mutation payloads.
    ///
    /// # Errors
    ///
    /// Returns an error when durable history cannot be read.
    pub fn item_sources_in<T: MykoItem>(
        &self,
        scope_id: &ScopeId,
    ) -> Result<Vec<NodeId>, NodeError> {
        let mut sources = BTreeMap::new();
        for envelope in self.events_after(None)? {
            let NodeEvent::CommandCommitted { command, batch } = envelope.event else {
                continue;
            };
            if command.request.service_id.as_str() == T::SERVICE_ID.as_str()
                && command.request.scope_id == *scope_id
                && batch.changes.iter().any(ItemMutation::is::<T>)
            {
                sources
                    .entry(envelope.origin.node_id.to_string())
                    .or_insert(envelope.origin.node_id);
            }
        }
        Ok(sources.into_values().collect())
    }

    /// Starts a gap-free replay-then-live typed query watch within one source,
    /// application service, and federation scope.
    ///
    /// The returned snapshot covers every event through its cursor. The watch
    /// begins strictly after that cursor, including events committed while the
    /// initial projection was being built.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read, a matching item mutation
    /// is malformed, or a gap-free subscription cannot be established.
    pub fn watch_items_in<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<(ItemQuerySnapshot<Q::Output>, ItemQueryWatch<Q>), NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let mut projection = ItemProjection::default();
        for envelope in &history {
            let _changed = apply_item_envelope(
                &mut projection,
                envelope,
                Some(source_node),
                Some((&service_id, &scope_id)),
            )?;
        }
        let snapshot = ItemQuerySnapshot {
            through,
            value: query.clone().execute(&projection),
        };
        let events = self.subscribe(through)?;
        Ok((
            snapshot,
            ItemQueryWatch {
                query,
                projection,
                source_node: Some(source_node),
                service_id,
                scope_id: Some(scope_id),
                events,
            },
        ))
    }

    /// Opens a gap-free typed query across every scope owned by one source.
    ///
    /// The returned snapshot and watch share one event-log boundary. Retaining
    /// the watch therefore observes every later matching commit without
    /// polling or a snapshot/subscription race.
    ///
    /// # Errors
    ///
    /// Returns an error if history cannot be projected or its live
    /// continuation cannot be opened.
    pub fn watch_items_from<Q>(
        &self,
        source_node: NodeId,
        query: Q,
    ) -> Result<(ItemQuerySnapshot<Q::Output>, ItemQueryWatch<Q>), NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let mut projection = ItemProjection::default();
        for envelope in &history {
            let _changed = apply_item_envelope(&mut projection, envelope, Some(source_node), None)?;
        }
        let snapshot = ItemQuerySnapshot {
            through,
            value: query.clone().execute(&projection),
        };
        let events = self.subscribe(through)?;
        Ok((
            snapshot,
            ItemQueryWatch {
                query,
                projection,
                source_node: Some(source_node),
                service_id,
                scope_id: None,
                events,
            },
        ))
    }

    /// Starts a gap-free typed query watch within one application service and
    /// federation scope across every authoritative source represented here.
    ///
    /// Newly ingested events from any source enter the same typed projection;
    /// callers never need to inspect federation envelopes or poll for sources.
    ///
    /// # Errors
    ///
    /// Returns an error when history cannot be read, a matching item mutation
    /// is malformed, or the subscription cannot be established.
    pub fn watch_items_across_sources_in<Q>(
        &self,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<(ItemQuerySnapshot<Q::Output>, ItemQueryWatch<Q>), NodeError>
    where
        Q: ItemQuery,
    {
        let service_id = ServiceId::new(Q::Item::SERVICE_ID);
        let history = self.events_after(None)?;
        let through = history.last().map(|envelope| envelope.position);
        let mut projection = ItemProjection::default();
        for envelope in &history {
            let _changed = apply_item_envelope(
                &mut projection,
                envelope,
                None,
                Some((&service_id, &scope_id)),
            )?;
        }
        let snapshot = ItemQuerySnapshot {
            through,
            value: query.clone().execute(&projection),
        };
        let events = self.subscribe(through)?;
        Ok((
            snapshot,
            ItemQueryWatch {
                query,
                projection,
                source_node: None,
                service_id,
                scope_id: Some(scope_id),
                events,
            },
        ))
    }

    /// Returns every scope observed in immutable history in stable order.
    ///
    /// This is a local projection primitive. A transport must authorize each
    /// scope before disclosing its identifier to a remote principal.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn scope_ids(&self) -> Result<Vec<ScopeId>, NodeError> {
        let mut scopes: Vec<_> = self
            .events_after(None)?
            .into_iter()
            .map(|envelope| envelope.event.scope_id().clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        scopes.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(scopes)
    }

    /// Creates a replay-then-live subscription without a cursor gap.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot establish the subscription.
    pub fn subscribe(&self, after: Option<LogPosition>) -> Result<EventSubscription, NodeError> {
        self.backend.subscribe(after)
    }

    /// Starts a gap-free subscription after the node's current durable boundary.
    ///
    /// Existing history is used only to capture the boundary and is not
    /// replayed to the caller. An event committed concurrently with this call
    /// is still delivered by the backend subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when history or the backend subscription cannot be
    /// read.
    pub fn subscribe_from_now(&self) -> Result<EventSubscription, NodeError> {
        let through = self
            .events_after(None)?
            .last()
            .map(|envelope| envelope.position);
        self.subscribe(through)
    }

    /// Starts a gap-free opaque notification stream for application item changes.
    ///
    /// Existing history establishes the durable boundary but is not replayed.
    /// Command-only lifecycle transitions are filtered inside Myko, so callers
    /// never need to inspect wire envelopes or command identities.
    ///
    /// # Errors
    ///
    /// Returns an error when history or the backend subscription cannot be read.
    pub fn subscribe_item_changes_from_now(&self) -> Result<ItemChangeSubscription, NodeError> {
        self.subscribe_from_now()
            .map(|events| ItemChangeSubscription { events })
    }

    /// Idempotently ingests an immutable event received from another node.
    ///
    /// # Errors
    ///
    /// Returns an error when replicated history conflicts with local command
    /// identity or contains an invalid atomic batch.
    pub fn ingest(&self, event: EventEnvelope) -> Result<IngestStatus, NodeError> {
        self.backend.ingest(event)
    }

    /// Exports immutable events after an exclusive local replay cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn export(&self, after: Option<LogPosition>) -> Result<ReplicationBatch, NodeError> {
        let events = self.events_after(after)?;
        let through = events.last().map(|event| event.position).or(after);
        Ok(ReplicationBatch {
            source_node: self.node_id(),
            after,
            through,
            events,
        })
    }

    /// Exports one exact application scope while retaining the source cursor.
    ///
    /// The cursor advances across unrelated events without disclosing them.
    /// Consumers must keep separate cursors for separate source/scope pairs.
    ///
    /// # Errors
    ///
    /// Returns an error when backend history cannot be read.
    pub fn export_scope(
        &self,
        scope_id: ScopeId,
        after: Option<LogPosition>,
    ) -> Result<ScopedReplicationBatch, NodeError> {
        let suffix = self.events_after(after)?;
        let through = suffix.last().map(|event| event.position).or(after);
        let events = suffix
            .into_iter()
            .filter(|event| event.event.scope_id() == &scope_id)
            .collect();
        Ok(ScopedReplicationBatch {
            source_node: self.node_id(),
            scope_id,
            after,
            through,
            events,
        })
    }

    /// Applies a transport-delivered replication batch idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error when any event conflicts with stable command identity
    /// or contains an invalid atomic change batch.
    pub fn ingest_batch(&self, batch: ReplicationBatch) -> Result<ReplicationReport, NodeError> {
        validate_replication_batch(&batch)?;
        let mut applied = 0usize;
        let mut duplicates = 0usize;
        for event in batch.events {
            match self.ingest(event)? {
                IngestStatus::Applied { .. } => applied = applied.saturating_add(1),
                IngestStatus::Duplicate => duplicates = duplicates.saturating_add(1),
            }
        }
        Ok(ReplicationReport {
            source_node: batch.source_node,
            through: batch.through,
            applied,
            duplicates,
        })
    }

    /// Applies a scope-filtered transport batch idempotently.
    ///
    /// Source positions may contain gaps, but every included event must belong
    /// to the declared scope and lie strictly inside the cursor interval.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch cursor is invalid, an event belongs to a
    /// different scope, or replicated command identity conflicts locally.
    pub fn ingest_scoped_batch(
        &self,
        batch: ScopedReplicationBatch,
    ) -> Result<ScopedReplicationReport, NodeError> {
        validate_scoped_replication_batch(&batch)?;
        let mut applied = 0usize;
        let mut duplicates = 0usize;
        for event in batch.events {
            match self.ingest(event)? {
                IngestStatus::Applied { .. } => applied = applied.saturating_add(1),
                IngestStatus::Duplicate => duplicates = duplicates.saturating_add(1),
            }
        }
        Ok(ScopedReplicationReport {
            source_node: batch.source_node,
            scope_id: batch.scope_id,
            through: batch.through,
            applied,
            duplicates,
        })
    }
}

fn validate_replication_batch(batch: &ReplicationBatch) -> Result<(), NodeError> {
    let mut expected = batch
        .after
        .map_or(Ok(LogPosition::FIRST), LogPosition::next)?;
    for event in &batch.events {
        if event.position != expected {
            return Err(NodeError::InvalidReplicationBatch(format!(
                "expected source position {}, received {}",
                expected.get(),
                event.position.get()
            )));
        }
        expected = expected.next()?;
    }
    let observed_through = batch
        .events
        .last()
        .map(|event| event.position)
        .or(batch.after);
    if batch.through != observed_through {
        return Err(NodeError::InvalidReplicationBatch(format!(
            "declared through {:?} does not match observed {:?}",
            batch.through, observed_through
        )));
    }
    Ok(())
}

fn validate_scoped_replication_batch(batch: &ScopedReplicationBatch) -> Result<(), NodeError> {
    if matches!((batch.after, batch.through), (Some(_), None))
        || matches!((batch.after, batch.through), (Some(after), Some(through)) if through < after)
    {
        return Err(NodeError::InvalidReplicationBatch(
            "scoped replication cursor moved backwards".to_owned(),
        ));
    }
    let mut previous = batch.after;
    for event in &batch.events {
        if previous.is_some_and(|position| event.position <= position)
            || batch.through.is_none_or(|through| event.position > through)
        {
            return Err(NodeError::InvalidReplicationBatch(format!(
                "scoped event position {} is outside its cursor interval",
                event.position.get()
            )));
        }
        if event.event.scope_id() != &batch.scope_id {
            return Err(NodeError::InvalidReplicationBatch(format!(
                "event at position {} does not belong to scope {}",
                event.position.get(),
                batch.scope_id
            )));
        }
        previous = Some(event.position);
    }
    Ok(())
}

fn validate_change_batch(batch: &ChangeBatch) -> Result<(), NodeError> {
    for mutation in &batch.changes {
        mutation
            .validate_envelope()
            .map_err(|error| NodeError::InvalidItemMutation(error.to_string()))?;
        if mutation.service_id != batch.service_id.as_str() {
            return Err(NodeError::InvalidItemMutation(format!(
                "item mutation belongs to service {}, batch belongs to {}",
                mutation.service_id, batch.service_id
            )));
        }
    }
    Ok(())
}

fn apply_item_envelope<T: MykoItem>(
    projection: &mut ItemProjection<T>,
    envelope: &EventEnvelope,
    source_node: Option<NodeId>,
    service_scope: Option<(&ServiceId, &ScopeId)>,
) -> Result<bool, NodeError> {
    if source_node.is_some_and(|source| source != envelope.origin.node_id) {
        return Ok(false);
    }
    let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
        return Ok(false);
    };
    if service_scope.is_some_and(|(service, scope)| {
        &command.request.service_id != service || &command.request.scope_id != scope
    }) {
        return Ok(false);
    }
    let mut changed = false;
    for (index, mutation) in batch.changes.iter().enumerate() {
        let change_index = u32::try_from(index).map_err(|error| {
            NodeError::CorruptHistory(format!(
                "item batch contains too many ordered changes: {error}"
            ))
        })?;
        changed |= projection
            .apply_at_order(mutation, envelope.position.get(), change_index)
            .map_err(|error| NodeError::CorruptHistory(error.to_string()))?;
    }
    Ok(changed)
}

/// Reference backend used by embedded nodes and tests.
pub struct InMemoryBackend {
    node_id: NodeId,
    state: Mutex<MemoryState>,
    journal: Option<Arc<dyn EventJournal>>,
}

#[derive(Default)]
struct MemoryState {
    next_position: LogPosition,
    commands: HashMap<CommandId, CommandSnapshot>,
    events: Vec<EventEnvelope>,
    seen_origins: HashSet<EventId>,
    subscribers: Vec<flume::Sender<EventEnvelope>>,
}

impl InMemoryBackend {
    /// Creates an empty backend with a stable node identity.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            state: Mutex::new(MemoryState {
                next_position: LogPosition::FIRST,
                ..MemoryState::default()
            }),
            journal: None,
        }
    }

    /// Reconstructs the reference backend from a durable immutable journal.
    ///
    /// # Errors
    ///
    /// Returns an error if history is corrupt, out of order, or cannot be read.
    pub fn from_journal(journal: Arc<dyn EventJournal>) -> Result<Self, NodeError> {
        let node_id = journal.node_id()?;
        let mut state = MemoryState {
            next_position: LogPosition::FIRST,
            ..MemoryState::default()
        };
        for envelope in journal.replay()? {
            if envelope.position != state.next_position {
                return Err(NodeError::CorruptHistory(format!(
                    "expected position {}, found {}",
                    state.next_position.get(),
                    envelope.position.get()
                )));
            }
            if !state.seen_origins.insert(envelope.origin) {
                return Err(NodeError::CorruptHistory(format!(
                    "duplicate event origin {:?}",
                    envelope.origin
                )));
            }
            Self::validate_event(&state, &envelope.event)?;
            Self::apply_event(&mut state, &envelope.event);
            state.next_position = state.next_position.next()?;
            state.events.push(envelope);
        }
        let backend = Self {
            node_id,
            state: Mutex::new(state),
            journal: Some(journal),
        };
        backend.requeue_abandoned_local_claims()?;
        Ok(backend)
    }

    fn requeue_abandoned_local_claims(&self) -> Result<(), NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let mut abandoned = state
            .commands
            .values()
            .filter(|snapshot| {
                matches!(snapshot.state, CommandState::Executing)
                    && snapshot.updated_at.node_id == self.node_id
            })
            .map(|snapshot| (snapshot.updated_at.sequence, snapshot.request.clone()))
            .collect::<Vec<_>>();
        abandoned.sort_by_key(|(position, _)| *position);
        for (_, request) in abandoned {
            let position = state.next_position;
            let snapshot = CommandSnapshot {
                request,
                state: CommandState::Submitted,
                result: None,
                updated_at: EventId::new(self.node_id, position),
            };
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot))?;
        }
        drop(state);
        Ok(())
    }

    fn append_locked(
        &self,
        state: &mut MemoryState,
        event: NodeEvent,
    ) -> Result<EventEnvelope, NodeError> {
        let position = state.next_position;
        let next_position = position.next()?;
        Self::validate_event(state, &event)?;
        let envelope = EventEnvelope {
            position,
            origin: EventId::new(self.node_id, position),
            recorded_at: Utc::now(),
            event,
        };
        if let Some(journal) = &self.journal {
            journal.append(&envelope)?;
        }
        Self::apply_event(state, &envelope.event);
        state.next_position = next_position;
        state.seen_origins.insert(envelope.origin);
        state.events.push(envelope.clone());
        Ok(envelope)
    }

    fn validate_event(state: &MemoryState, event: &NodeEvent) -> Result<(), NodeError> {
        match event {
            NodeEvent::CommandLifecycle(snapshot) => {
                if let Some(existing) = state.commands.get(&snapshot.request.id)
                    && existing.request != snapshot.request
                {
                    return Err(NodeError::CommandConflict(snapshot.request.id));
                }
            }
            NodeEvent::CommandCommitted { command, batch } => {
                if batch.command_id != command.request.id
                    || batch.service_id != command.request.service_id
                    || batch.scope_id != command.request.scope_id
                {
                    return Err(NodeError::BatchMismatch(command.request.id));
                }
                validate_change_batch(batch)?;
                if let Some(existing) = state.commands.get(&command.request.id)
                    && existing.request != command.request
                {
                    return Err(NodeError::CommandConflict(command.request.id));
                }
            }
        }
        Ok(())
    }

    fn apply_event(state: &mut MemoryState, event: &NodeEvent) {
        match event {
            NodeEvent::CommandLifecycle(snapshot) => {
                let should_apply = state
                    .commands
                    .get(&snapshot.request.id)
                    .is_none_or(|existing| Self::lifecycle_supersedes(existing, snapshot));
                if should_apply {
                    state.commands.insert(snapshot.request.id, snapshot.clone());
                }
            }
            NodeEvent::CommandCommitted { command, .. } => {
                state.commands.insert(command.request.id, command.clone());
            }
        }
    }

    fn lifecycle_supersedes(existing: &CommandSnapshot, incoming: &CommandSnapshot) -> bool {
        if existing.state.is_committed() {
            return false;
        }
        let existing_terminal = matches!(
            existing.state,
            CommandState::Rejected { .. } | CommandState::Cancelled { .. }
        );
        if !existing_terminal {
            return true;
        }
        let incoming_rank = match incoming.state {
            CommandState::Cancelled { .. } => 2,
            CommandState::Rejected { .. } => 1,
            _ => 0,
        };
        let existing_rank = match existing.state {
            CommandState::Cancelled { .. } => 2,
            CommandState::Rejected { .. } => 1,
            _ => 0,
        };
        let incoming_order = (
            incoming.updated_at.node_id.as_uuid(),
            incoming.updated_at.sequence,
        );
        let existing_order = (
            existing.updated_at.node_id.as_uuid(),
            existing.updated_at.sequence,
        );
        incoming_rank > existing_rank
            || (incoming_rank == existing_rank && incoming_order > existing_order)
    }

    fn broadcast_locked(state: &mut MemoryState, envelope: &EventEnvelope) {
        state
            .subscribers
            .retain(|subscriber| subscriber.send(envelope.clone()).is_ok());
    }

    fn matches_cursor(event: &EventEnvelope, after: Option<LogPosition>) -> bool {
        after.is_none_or(|cursor| event.position > cursor)
    }
}

impl NodeBackend for InMemoryBackend {
    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn submit(&self, request: CommandRequest) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        if let Some(existing) = state.commands.get(&request.id) {
            if existing.request != request {
                return Err(NodeError::CommandConflict(request.id));
            }
            return Ok(existing.clone());
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request,
            state: CommandState::Submitted,
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn claim(&self, command_id: CommandId) -> Result<CommandAdmission, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if !matches!(
            existing.state,
            CommandState::Submitted | CommandState::Retrying { .. }
        ) {
            return Ok(CommandAdmission::Resume(existing));
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::Executing,
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(CommandAdmission::Execute(snapshot))
    }

    fn admit(&self, request: CommandRequest) -> Result<CommandAdmission, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        if let Some(existing) = state.commands.get(&request.id) {
            if existing.request != request {
                return Err(NodeError::CommandConflict(request.id));
            }
            return Ok(CommandAdmission::Resume(existing.clone()));
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request,
            state: CommandState::Executing,
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        debug_assert_eq!(envelope.position, position);
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(CommandAdmission::Execute(snapshot))
    }

    fn commit(
        &self,
        command_id: CommandId,
        batch: ChangeBatch,
        result: Vec<u8>,
    ) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;

        if existing.state.is_committed() {
            return Ok(existing);
        }
        if !matches!(existing.state, CommandState::Executing) {
            return Err(NodeError::CommandNotExecuting(command_id));
        }
        if batch.command_id != command_id
            || batch.service_id != existing.request.service_id
            || batch.scope_id != existing.request.scope_id
        {
            return Err(NodeError::BatchMismatch(command_id));
        }
        validate_change_batch(&batch)?;

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::CommittedLocally {
                batch_id: batch.id,
                position: origin,
            },
            result: Some(result),
            updated_at: origin,
        };
        let envelope = self.append_locked(
            &mut state,
            NodeEvent::CommandCommitted {
                command: snapshot.clone(),
                batch,
            },
        )?;
        debug_assert_eq!(envelope.position, position);
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn reject(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if !matches!(existing.state, CommandState::Executing) {
            return Err(NodeError::CommandNotExecuting(command_id));
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::Rejected { reason },
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn retry(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if !matches!(existing.state, CommandState::Executing) {
            return Err(NodeError::CommandNotExecuting(command_id));
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::Retrying { reason },
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn cancel(&self, command_id: CommandId, reason: String) -> Result<CommandSnapshot, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let existing = state
            .commands
            .get(&command_id)
            .cloned()
            .ok_or(NodeError::UnknownCommand(command_id))?;
        if existing.state.is_terminal_locally() {
            return Ok(existing);
        }

        let position = state.next_position;
        let origin = EventId::new(self.node_id, position);
        let snapshot = CommandSnapshot {
            request: existing.request,
            state: CommandState::Cancelled { reason },
            result: None,
            updated_at: origin,
        };
        let envelope =
            self.append_locked(&mut state, NodeEvent::CommandLifecycle(snapshot.clone()))?;
        Self::broadcast_locked(&mut state, &envelope);
        drop(state);
        Ok(snapshot)
    }

    fn command(&self, command_id: CommandId) -> Result<Option<CommandSnapshot>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state.commands.get(&command_id).cloned())
    }

    fn events_after(&self, after: Option<LogPosition>) -> Result<Vec<EventEnvelope>, NodeError> {
        let state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        Ok(state
            .events
            .iter()
            .filter(|event| Self::matches_cursor(event, after))
            .cloned()
            .collect())
    }

    fn subscribe(&self, after: Option<LogPosition>) -> Result<EventSubscription, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        let backlog = state
            .events
            .iter()
            .filter(|event| Self::matches_cursor(event, after))
            .cloned()
            .collect();
        let (sender, live) = flume::unbounded();
        state.subscribers.push(sender);
        drop(state);
        Ok(EventSubscription { backlog, live })
    }

    fn ingest(&self, event: EventEnvelope) -> Result<IngestStatus, NodeError> {
        let mut state = self.state.lock().map_err(|_| NodeError::Poisoned)?;
        if state.seen_origins.contains(&event.origin) {
            return Ok(IngestStatus::Duplicate);
        }

        let local_position = state.next_position;
        let next_position = local_position.next()?;
        let imported = EventEnvelope {
            position: local_position,
            origin: event.origin,
            recorded_at: event.recorded_at,
            event: event.event,
        };
        Self::validate_event(&state, &imported.event)?;
        if let Some(journal) = &self.journal {
            journal.append(&imported)?;
        }
        Self::apply_event(&mut state, &imported.event);
        state.next_position = next_position;
        state.seen_origins.insert(imported.origin);
        state.events.push(imported.clone());
        Self::broadcast_locked(&mut state, &imported);
        drop(state);
        Ok(IngestStatus::Applied {
            position: local_position,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use myko_items::{myko_item, myko_service};

    #[test]
    fn startup_gates_are_shared_by_every_node_clone() {
        let node = Node::in_memory();
        let peer_handle = node.clone();
        assert!(node.is_ready());

        let startup = node.hold_startup();
        assert!(!node.is_ready());
        assert!(!peer_handle.is_ready());

        startup.ready();
        assert!(node.is_ready());
        assert!(peer_handle.is_ready());
    }

    #[test]
    fn scope_grants_are_directional_and_require_subscription_explicitly() {
        let scope_id = ScopeId::new("project:pine");
        let principal = PrincipalId::new("iroh:node-b");
        let policy = ScopeGrantPolicy::new(vec![ScopeGrant {
            scope_id: scope_id.clone(),
            grantee: principal.clone(),
            permissions: vec![FederationPermission::ReadState],
        }]);
        let read = AccessRequest {
            principal_id: principal,
            operation: AccessOperation::ReadItems,
            service_id: None,
            scope_id: Some(scope_id),
            command_id: None,
            command_type: None,
            command_principal_id: None,
            live_topics: Vec::new(),
        };
        assert!(policy.authorize(&read).is_ok());

        let mut follow = read.clone();
        follow.operation = AccessOperation::FollowItems;
        assert!(policy.authorize(&follow).is_err());
        let mut wrong_principal = read;
        wrong_principal.principal_id = PrincipalId::new("iroh:node-c");
        assert!(policy.authorize(&wrong_principal).is_err());
    }

    #[myko_service(TestRecord, TestMarker)]
    pub struct TestService;

    #[myko_item(service = TestService, scope_root)]
    pub struct TestRecord {
        pub value: String,
    }

    #[myko_item(service = TestService, scope_root)]
    pub struct TestMarker {
        pub value: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct PutRecord {
        pub id: String,
        pub value: String,
    }

    impl MykoOperation for PutRecord {
        const OPERATION_ID: &'static str = stringify!(PutRecord);
    }

    impl MykoCommandContract for PutRecord {
        type Output = bool;
        type Service = TestService;
        type Scope = TestRecord;
    }

    impl MykoCommand for PutRecord {}

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct OtherCommand {
        pub value: String,
    }

    #[myko_service(OtherRecord)]
    pub struct OtherService;

    #[myko_item(service = OtherService)]
    pub struct OtherRecord {
        pub value: String,
    }

    impl MykoOperation for OtherCommand {
        const OPERATION_ID: &'static str = stringify!(OtherCommand);
    }

    impl MykoCommandContract for OtherCommand {
        type Output = ();
        type Service = OtherService;
        type Scope = OtherRecord;
    }

    impl MykoCommand for OtherCommand {}

    struct FailingJournal {
        node_id: NodeId,
    }

    impl EventJournal for FailingJournal {
        fn node_id(&self) -> Result<NodeId, NodeError> {
            Ok(self.node_id)
        }

        fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError> {
            Ok(Vec::new())
        }

        fn append(&self, _event: &EventEnvelope) -> Result<(), NodeError> {
            Err(NodeError::Backend("injected append failure".to_owned()))
        }
    }

    fn request(id: CommandId) -> CommandRequest {
        CommandRequest {
            id,
            service_id: ServiceId::new(TestService::SERVICE_ID),
            scope_id: ScopeId::new("session:test"),
            principal_id: PrincipalId::new("human:test"),
            command_type: "prompt".to_owned(),
            payload: b"hello".to_vec(),
        }
    }

    fn batch(command: &CommandRequest) -> ChangeBatch {
        ChangeBatch {
            id: BatchId::new(),
            command_id: command.id,
            service_id: command.service_id.clone(),
            scope_id: command.scope_id.clone(),
            causal_parents: Vec::new(),
            changes: vec![ItemMutation {
                service_id: command.service_id.as_str().to_owned(),
                item_type: "message".to_owned(),
                item_id: "message:1".to_owned(),
                schema_version: 1,
                operation: MutationOperation::Set,
                payload: Some(b"hello".to_vec()),
            }],
        }
    }

    fn commit_test_record(node: &Node, id: &str, value: &str) -> TestRecord {
        commit_test_record_in(node, ScopeId::new("session:test"), id, value)
    }

    fn commit_test_record_in(node: &Node, scope_id: ScopeId, id: &str, value: &str) -> TestRecord {
        let mut request = request(CommandId::new());
        request.scope_id = scope_id;
        let executing = node.admit(request.clone()).unwrap().snapshot().clone();
        let record = TestRecord {
            id: TestRecordId::from(id),
            value: value.to_owned(),
        };
        node.commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: request.scope_id,
                causal_parents: vec![executing.updated_at],
                changes: vec![ItemMutation::set(&record).unwrap()],
            },
            Vec::new(),
        )
        .unwrap();
        record
    }

    fn commit_test_marker(node: &Node, id: &str, value: &str) {
        let request = request(CommandId::new());
        let executing = node.admit(request.clone()).unwrap().snapshot().clone();
        let marker = TestMarker {
            id: TestMarkerId::from(id),
            value: value.to_owned(),
        };
        node.commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: request.scope_id,
                causal_parents: vec![executing.updated_at],
                changes: vec![ItemMutation::set(&marker).unwrap()],
            },
            Vec::new(),
        )
        .unwrap();
    }

    #[test]
    fn stable_command_id_executes_once_and_resumes_after_commit() {
        let node = Node::in_memory();
        let request = request(CommandId::new());

        assert!(node.admit(request.clone()).unwrap().should_execute());
        assert!(!node.admit(request.clone()).unwrap().should_execute());
        node.commit(request.id, batch(&request), b"done".to_vec())
            .unwrap();

        let resumed = node.admit(request).unwrap().snapshot().clone();
        assert!(resumed.state.is_committed());
        assert_eq!(resumed.result.as_deref(), Some(b"done".as_slice()));
    }

    #[test]
    fn command_watch_starts_current_then_updates_without_a_gap() {
        let node = Node::in_memory();
        let request = request(CommandId::new());
        node.submit(request.clone()).unwrap();
        let (initial, mut watch) = node.watch_command(request.id).unwrap();
        assert!(initial.command.is_some_and(|command| {
            command.request == request && command.state == CommandState::Submitted
        }));

        node.claim(request.id).unwrap();
        assert_eq!(watch.recv().unwrap().state, CommandState::Executing);
        node.cancel(request.id, "stopped").unwrap();
        assert_eq!(
            watch.recv().unwrap().state,
            CommandState::Cancelled {
                reason: "stopped".to_owned()
            }
        );
        assert!(matches!(
            node.watch_command(CommandId::new()),
            Err(NodeError::UnknownCommand(_))
        ));
    }

    #[test]
    fn command_catalog_pages_hold_the_first_log_ceiling() {
        let node = Node::in_memory();
        let scope_id = ScopeId::new("session:test");
        let principal_id = PrincipalId::new("human:test");
        let first = DeclaredCommand::new(
            CommandId::from_uuid(Uuid::from_u128(1)),
            scope_id.clone(),
            principal_id.clone(),
            PutRecord {
                id: "record-1".to_owned(),
                value: "first".to_owned(),
            },
        );
        let third = DeclaredCommand::new(
            CommandId::from_uuid(Uuid::from_u128(3)),
            scope_id.clone(),
            principal_id.clone(),
            PutRecord {
                id: "record-3".to_owned(),
                value: "third".to_owned(),
            },
        );
        node.submit(first.request().unwrap()).unwrap();
        node.submit(third.request().unwrap()).unwrap();

        let first_page = node
            .command_state_page(
                CommandStateRequest::for_serving_declared::<PutRecord>(scope_id.clone())
                    .with_page_size(1),
            )
            .unwrap();
        assert_eq!(first_page.commands.len(), 1);
        assert_eq!(
            first_page
                .commands
                .first()
                .map(|entry| entry.command.request.id),
            Some(first.id)
        );
        let through = first_page.through;
        let (mut snapshot, next) = CommandStateSnapshot::from_first_page(first_page).unwrap();

        let concurrent = DeclaredCommand::new(
            CommandId::from_uuid(Uuid::from_u128(2)),
            scope_id.clone(),
            principal_id,
            PutRecord {
                id: "record-2".to_owned(),
                value: "too late for this snapshot".to_owned(),
            },
        );
        node.submit(concurrent.request().unwrap()).unwrap();
        let next = next.unwrap();
        assert_eq!(next.snapshot_through, through);
        let second_page = node.command_state_page(next.clone()).unwrap();
        assert_eq!(second_page.through, through);
        assert!(snapshot.append_page(&next, second_page).unwrap().is_none());
        assert_eq!(
            snapshot
                .typed::<PutRecord>()
                .unwrap()
                .into_iter()
                .map(|entry| entry.id)
                .collect::<Vec<_>>(),
            vec![first.id, third.id]
        );
        assert_eq!(
            node.command_states(CommandStateRequest::for_serving_declared::<PutRecord>(
                scope_id
            ))
            .unwrap()
            .commands
            .len(),
            3
        );
    }

    #[test]
    fn typed_command_catalog_decodes_body_result_and_admission_order() {
        let node = Node::in_memory();
        let scope_id = ScopeId::new("session:test");
        let principal_id = PrincipalId::new("human:test");
        let first = DeclaredCommand::new(
            CommandId::new(),
            scope_id.clone(),
            principal_id.clone(),
            PutRecord {
                id: "record-1".to_owned(),
                value: "first".to_owned(),
            },
        );
        node.submit(first.request().unwrap()).unwrap();
        let DeclaredCommandAdmission::Execute(context) =
            node.begin_declared_command::<PutRecord>(first.id).unwrap()
        else {
            return;
        };
        context.commit(&true).unwrap();
        let second = DeclaredCommand::new(
            CommandId::new(),
            scope_id.clone(),
            principal_id,
            PutRecord {
                id: "record-2".to_owned(),
                value: "second".to_owned(),
            },
        );
        node.submit(second.request().unwrap()).unwrap();

        let catalog = node
            .command_states(CommandStateRequest::for_serving_declared::<PutRecord>(
                scope_id,
            ))
            .unwrap()
            .typed::<PutRecord>()
            .unwrap();
        assert_eq!(
            catalog.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![first.id, second.id]
        );
        if let [first_state, second_state] = catalog.as_slice() {
            assert_eq!(first_state.command.id, first.body.id);
            assert_eq!(first_state.command.value, first.body.value);
            assert_eq!(first_state.result, Some(true));
            assert!(first_state.state.is_committed());
            assert_eq!(second_state.command.id, second.body.id);
            assert_eq!(second_state.command.value, second.body.value);
            assert_eq!(second_state.result, None);
            assert_eq!(second_state.state, CommandState::Submitted);
        } else {
            assert_eq!(catalog.len(), 2);
        }
    }

    #[test]
    fn command_catalog_ignores_stale_lifecycle_events() {
        let source = Node::in_memory();
        let command = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-1".to_owned(),
                value: "cancel me".to_owned(),
            },
        );
        source.submit(command.request().unwrap()).unwrap();
        source.claim(command.id).unwrap();
        source.cancel(command.id, "stopped").unwrap();

        let replica = Node::in_memory();
        for event in source.events_after(None).unwrap().into_iter().rev() {
            replica.ingest(event).unwrap();
        }
        let catalog = replica
            .command_states(CommandStateRequest::for_declared::<PutRecord>(
                source.node_id(),
                command.scope_id,
            ))
            .unwrap();
        if let [entry] = catalog.commands.as_slice() {
            assert_eq!(entry.admitted_at, LogPosition::FIRST);
            assert_eq!(entry.last_changed_at, LogPosition::FIRST);
            assert!(matches!(
                entry.command.state,
                CommandState::Cancelled { ref reason } if reason == "stopped"
            ));
        } else {
            assert_eq!(catalog.commands.len(), 1);
        }
    }

    #[test]
    fn command_catalog_stream_adds_and_advances_matching_commands() {
        let node = Node::in_memory();
        let scope_id = ScopeId::new("session:test");
        let first = DeclaredCommand::new(
            CommandId::new(),
            scope_id.clone(),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-1".to_owned(),
                value: "first".to_owned(),
            },
        );
        node.submit(first.request().unwrap()).unwrap();
        let snapshot = node
            .command_states(CommandStateRequest::for_serving_declared::<PutRecord>(
                scope_id.clone(),
            ))
            .unwrap();
        let mut stream = CommandStateStream::from_snapshot(&snapshot).unwrap();
        let second = DeclaredCommand::new(
            CommandId::new(),
            scope_id,
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-2".to_owned(),
                value: "second".to_owned(),
            },
        );
        node.submit(second.request().unwrap()).unwrap();
        node.claim(second.id).unwrap();

        let events = node.events_after(snapshot.through).unwrap();
        for event in &events {
            let Some(update) = stream.request().update_from_envelope(event) else {
                assert!(stream.request().update_from_envelope(event).is_some());
                return;
            };
            let _current = stream.apply(&update).unwrap();
        }
        let current = stream.current().typed::<PutRecord>().unwrap();
        assert_eq!(
            current.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![first.id, second.id]
        );
        if let [first_state, second_state] = current.as_slice() {
            assert_eq!(first_state.state, CommandState::Submitted);
            assert_eq!(second_state.state, CommandState::Executing);
        } else {
            assert_eq!(current.len(), 2);
        }
        let Some(first_event) = events.first() else {
            assert!(!events.is_empty());
            return;
        };
        let mut foreign = first_event.clone();
        foreign.origin.node_id = NodeId::new();
        assert!(stream.request().update_from_envelope(&foreign).is_none());
    }

    #[test]
    fn typed_command_context_owns_atomic_item_batch_and_result_encoding() {
        let node = Node::in_memory();
        let request = request(CommandId::new());
        node.submit(request.clone()).unwrap();
        let admission = node.begin_command(request.id).unwrap();
        assert!(matches!(&admission, TypedCommandAdmission::Execute(_)));
        let TypedCommandAdmission::Execute(context) = admission else {
            return;
        };
        assert!(context.query(GetAllTestRecords).unwrap().is_empty());
        let record = TestRecord {
            id: TestRecordId::from("record-1"),
            value: "owned by Myko".to_owned(),
        };
        context.emit_set(&record).unwrap();
        assert_eq!(context.change_count(), Ok(1));
        let committed = context.commit(&true).unwrap();
        assert_eq!(committed.result.as_deref(), Some(b"true".as_slice()));
        assert_eq!(
            node.query_items_in(node.node_id(), &request.scope_id, GetAllTestRecords,)
                .unwrap(),
            vec![record]
        );
        assert!(matches!(
            node.begin_command(request.id).unwrap(),
            TypedCommandAdmission::Resume(_)
        ));
    }

    #[test]
    fn command_context_rejects_items_owned_by_another_service() {
        let node = Node::in_memory();
        let request = request(CommandId::new());
        let _submitted = node.submit(request.clone()).unwrap();
        let context = match node.begin_command(request.id).unwrap() {
            TypedCommandAdmission::Execute(context) => context,
            TypedCommandAdmission::Resume(_) => return,
        };
        let record = OtherRecord {
            id: OtherRecordId::from("other-1"),
            value: "must not cross services".to_owned(),
        };
        assert!(matches!(
            context.emit_set(&record),
            Err(NodeError::ItemServiceMismatch {
                item_service,
                ..
            }) if item_service == OtherService::SERVICE_ID.as_str()
        ));
        assert!(matches!(
            context.emit_delete::<OtherRecord>(&record.id),
            Err(NodeError::ItemServiceMismatch {
                item_service,
                ..
            }) if item_service == OtherService::SERVICE_ID.as_str()
        ));
        assert!(matches!(
            context.query(GetAllOtherRecords),
            Err(NodeError::ItemServiceMismatch {
                item_service,
                ..
            }) if item_service == OtherService::SERVICE_ID.as_str()
        ));
        assert_eq!(context.change_count(), Ok(0));
        let _rejected = context.reject("test complete").unwrap();
    }

    #[test]
    fn raw_batch_rejects_a_forged_item_service() {
        let node = Node::in_memory();
        let request = request(CommandId::new());
        let executing = node.admit(request.clone()).unwrap().snapshot().clone();
        let record = TestRecord {
            id: TestRecordId::from("record-1"),
            value: "forged ownership".to_owned(),
        };
        let mut mutation = ItemMutation::set(&record).unwrap();
        mutation.service_id = "other".to_owned();
        let result = node.commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: request.scope_id,
                causal_parents: vec![executing.updated_at],
                changes: vec![mutation],
            },
            Vec::new(),
        );
        assert!(matches!(result, Err(NodeError::InvalidItemMutation(_))));
    }

    #[test]
    fn typed_command_context_refuses_replicated_execution() {
        let source = Node::in_memory();
        let request = request(CommandId::new());
        source.submit(request.clone()).unwrap();
        let replica = Node::in_memory();
        for event in source.events_after(None).unwrap() {
            let _status = replica.ingest(event).unwrap();
        }
        assert!(matches!(
            replica.begin_command(request.id),
            Err(NodeError::ForeignCommand { .. })
        ));
    }

    #[test]
    fn declared_command_owns_submission_decoding_items_and_typed_result() {
        let node = Node::in_memory();
        let command = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-1".to_owned(),
                value: "declared command".to_owned(),
            },
        );
        node.submit(command.request().unwrap()).unwrap();
        let admission = node
            .begin_declared_command::<PutRecord>(command.id)
            .unwrap();
        assert!(matches!(&admission, DeclaredCommandAdmission::Execute(_)));
        let DeclaredCommandAdmission::Execute(mut context) = admission else {
            return;
        };
        let record = TestRecord {
            id: TestRecordId::from(context.body().id.clone()),
            value: context.body().value.clone(),
        };
        context.emit_set(&record).unwrap();
        let committed = context.commit(&true).unwrap();
        assert_eq!(committed.result.as_deref(), Some(b"true".as_slice()));
        assert_eq!(
            node.query_items_in(node.node_id(), &command.scope_id, GetAllTestRecords,)
                .unwrap(),
            vec![record]
        );
        assert!(matches!(
            node.begin_declared_command::<PutRecord>(command.id)
                .unwrap(),
            DeclaredCommandAdmission::Resume(_)
        ));
    }

    #[test]
    fn concurrent_declared_dispatch_resumes_only_after_the_owner_commits() {
        let node = Node::in_memory();
        let command = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-1".to_owned(),
                value: "owned execution".to_owned(),
            },
        );
        node.submit(command.request().unwrap()).unwrap();

        let (owner_started_tx, owner_started_rx) = std::sync::mpsc::channel();
        let (release_owner_tx, release_owner_rx) = std::sync::mpsc::channel();
        let owner_node = node.clone();
        let command_id = command.id;
        let owner_thread = std::thread::spawn(move || {
            owner_node.dispatch_declared_command::<PutRecord, _>(command_id, |_| {
                owner_started_tx.send(()).unwrap();
                release_owner_rx.recv().unwrap();
                Ok(true)
            })
        });
        owner_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let (contender_started_tx, contender_started_rx) = std::sync::mpsc::channel();
        let (contender_result_tx, contender_result_rx) = std::sync::mpsc::channel();
        let contender_node = node;
        let contender = std::thread::spawn(move || {
            contender_started_tx.send(()).unwrap();
            let result =
                contender_node.dispatch_declared_command::<PutRecord, _>(command_id, |_| Ok(false));
            contender_result_tx.send(result).unwrap();
        });
        contender_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(
            contender_result_rx
                .recv_timeout(Duration::from_millis(25))
                .is_err()
        );

        release_owner_tx.send(()).unwrap();
        let owner_result = owner_thread.join().unwrap().unwrap();
        let resumed = contender_result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        contender.join().unwrap();

        assert_eq!(
            owner_result.disposition,
            CommandDispatchDisposition::Committed
        );
        assert_eq!(resumed.disposition, CommandDispatchDisposition::Resumed);
        assert_eq!(
            resumed.command.typed_completion::<PutRecord>().unwrap(),
            Some(true)
        );
    }

    #[test]
    fn declared_command_schema_mismatch_does_not_claim_execution() {
        let node = Node::in_memory();
        let command = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-1".to_owned(),
                value: "not other".to_owned(),
            },
        );
        node.submit(command.request().unwrap()).unwrap();
        assert!(matches!(
            node.begin_declared_command::<OtherCommand>(command.id),
            Err(NodeError::CommandSchemaMismatch { .. })
        ));
        let snapshot = node.command(command.id).unwrap().unwrap();
        assert!(matches!(snapshot.state, CommandState::Submitted));
    }

    #[test]
    fn declared_dispatch_rejects_malformed_work_and_continues_in_order() {
        let node = Node::in_memory();
        let first = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-1".to_owned(),
                value: "first".to_owned(),
            },
        );
        node.submit(first.request().unwrap()).unwrap();
        let malformed_id = CommandId::new();
        node.submit(CommandRequest {
            id: malformed_id,
            service_id: ServiceId::new(PutRecord::SERVICE_ID),
            scope_id: ScopeId::new("session:test"),
            principal_id: PrincipalId::new("human:test"),
            command_type: PutRecord::COMMAND_TYPE.to_owned(),
            payload: b"not json".to_vec(),
        })
        .unwrap();
        let second = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-2".to_owned(),
                value: "second".to_owned(),
            },
        );
        node.submit(second.request().unwrap()).unwrap();

        let dispatched = node
            .dispatch_declared::<PutRecord, _>(|context| {
                let record = TestRecord {
                    id: TestRecordId::from(context.body().id.clone()),
                    value: context.body().value.clone(),
                };
                context
                    .emit_set(&record)
                    .map_err(|error| error.to_string())?;
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            dispatched
                .iter()
                .map(|result| result.disposition)
                .collect::<Vec<_>>(),
            vec![
                CommandDispatchDisposition::Committed,
                CommandDispatchDisposition::Rejected,
                CommandDispatchDisposition::Committed,
            ]
        );
        assert!(matches!(
            node.command(malformed_id).unwrap().unwrap().state,
            CommandState::Rejected { .. }
        ));
        assert_eq!(
            node.query_items_in(
                node.node_id(),
                &ScopeId::new("session:test"),
                GetAllTestRecords,
            )
            .unwrap()
            .len(),
            2
        );
    }

    #[test]
    fn local_pending_discovery_never_executes_a_replicated_submission() {
        let source = Node::in_memory();
        let command = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-1".to_owned(),
                value: "foreign".to_owned(),
            },
        );
        source.submit(command.request().unwrap()).unwrap();
        let replica = Node::in_memory();
        for event in source.events_after(None).unwrap() {
            let _status = replica.ingest(event).unwrap();
        }
        assert!(
            replica
                .pending_local_commands(PutRecord::SERVICE_ID.as_str(), PutRecord::COMMAND_TYPE)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn declared_pending_watch_replays_current_then_follows_without_polling() {
        let node = Node::in_memory();
        let completed = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "completed".to_owned(),
                value: "old".to_owned(),
            },
        );
        node.submit(completed.request().unwrap()).unwrap();
        node.dispatch_declared_command::<PutRecord, _>(completed.id, |_| Ok(true))
            .unwrap();
        let queued = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "queued".to_owned(),
                value: "restart catch-up".to_owned(),
            },
        );
        node.submit(queued.request().unwrap()).unwrap();

        let mut pending = node.watch_pending_typed::<PutRecord>().unwrap();
        assert_eq!(
            pending.service_id().map(ServiceId::as_str),
            Some(PutRecord::SERVICE_ID.as_str())
        );
        assert_eq!(pending.command_type(), Some(PutRecord::COMMAND_TYPE));
        assert_eq!(pending.recv().unwrap().request.id, queued.id);

        let unrelated = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            OtherCommand {
                value: "ignore me".to_owned(),
            },
        );
        node.submit(unrelated.request().unwrap()).unwrap();
        assert!(
            pending
                .recv_timeout(Duration::from_millis(5))
                .unwrap()
                .is_none()
        );

        let live = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "live".to_owned(),
                value: "event driven".to_owned(),
            },
        );
        node.submit(live.request().unwrap()).unwrap();
        assert_eq!(
            pending
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap()
                .request
                .id,
            live.id
        );
    }

    #[test]
    fn service_pending_watch_preserves_admission_order_and_omits_foreign_work() {
        let node = Node::in_memory();
        let first = request(CommandId::new());
        node.submit(first.clone()).unwrap();
        let second = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "second".to_owned(),
                value: "same service".to_owned(),
            },
        );
        node.submit(second.request().unwrap()).unwrap();

        let source = Node::in_memory();
        let foreign = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:remote"),
            PutRecord {
                id: "foreign".to_owned(),
                value: "projection only".to_owned(),
            },
        );
        source.submit(foreign.request().unwrap()).unwrap();
        for event in source.events_after(None).unwrap() {
            let _status = node.ingest(event).unwrap();
        }

        let mut pending = node
            .watch_pending_local_service_commands(TestService::SERVICE_ID.as_str())
            .unwrap();
        assert_eq!(pending.command_type(), None);
        assert_eq!(pending.recv().unwrap().request.id, first.id);
        assert_eq!(pending.recv().unwrap().request.id, second.id);
        assert!(
            pending
                .recv_timeout(Duration::from_millis(5))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn application_pending_watch_preserves_order_across_services() {
        let node = Node::in_memory();
        let first = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            OtherCommand {
                value: "first service".to_owned(),
            },
        );
        node.submit(first.request().unwrap()).unwrap();
        let second = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "second".to_owned(),
                value: "second service".to_owned(),
            },
        );
        node.submit(second.request().unwrap()).unwrap();

        assert_eq!(
            node.pending_local_application_commands()
                .unwrap()
                .into_iter()
                .map(|command| command.request.id)
                .collect::<Vec<_>>(),
            vec![first.id, second.id]
        );
        let mut pending = node.watch_pending_local_application_commands().unwrap();
        assert_eq!(pending.service_id(), None);
        assert_eq!(pending.command_type(), None);
        assert_eq!(pending.recv().unwrap().request.id, first.id);
        assert_eq!(pending.recv().unwrap().request.id, second.id);
    }

    #[test]
    fn declared_dispatch_durably_retries_transient_handler_failures() {
        let node = Node::in_memory();
        let command = DeclaredCommand::new(
            CommandId::new(),
            ScopeId::new("session:test"),
            PrincipalId::new("human:test"),
            PutRecord {
                id: "record-1".to_owned(),
                value: "retry me".to_owned(),
            },
        );
        node.submit(command.request().unwrap()).unwrap();
        let mut item_changes = node.subscribe_item_changes_from_now().unwrap();
        let retrying = node
            .dispatch_declared_command::<PutRecord, _>(command.id, |_| {
                Err(CommandHandlerError::retry("workspace registry unavailable"))
            })
            .unwrap();
        assert_eq!(retrying.disposition, CommandDispatchDisposition::Retrying);
        assert!(matches!(
            retrying.command.state,
            CommandState::Retrying { .. }
        ));
        assert_eq!(
            node.pending_local_commands(PutRecord::SERVICE_ID.as_str(), PutRecord::COMMAND_TYPE)
                .unwrap()
                .len(),
            1
        );
        assert!(item_changes.try_recv().is_none());

        let committed = node
            .dispatch_declared_command::<PutRecord, _>(command.id, |context| {
                let record = TestRecord {
                    id: TestRecordId::from(context.body().id.clone()),
                    value: context.body().value.clone(),
                };
                context
                    .emit_set(&record)
                    .map_err(|error| CommandHandlerError::retry(error.to_string()))?;
                Ok(true)
            })
            .unwrap();
        assert_eq!(committed.disposition, CommandDispatchDisposition::Committed);
        assert!(committed.command.state.is_committed());
        assert!(item_changes.try_recv().is_some());
    }

    #[test]
    fn typed_query_materializes_replicated_service_scope_state() {
        let source = Node::in_memory();
        let request = request(CommandId::new());
        let executing = source.admit(request.clone()).unwrap().snapshot().clone();
        let record = TestRecord {
            id: TestRecordId::from("record-1"),
            value: "federated".to_owned(),
        };
        source
            .commit(
                request.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: request.id,
                    service_id: request.service_id.clone(),
                    scope_id: request.scope_id.clone(),
                    causal_parents: vec![executing.updated_at],
                    changes: vec![ItemMutation::set(&record).unwrap()],
                },
                Vec::new(),
            )
            .unwrap();

        let replica = Node::in_memory();
        for event in source.events_after(None).unwrap() {
            let _status = replica.ingest(event).unwrap();
        }
        let projected = replica
            .query_items_in(source.node_id(), &request.scope_id, GetAllTestRecords)
            .unwrap();
        assert_eq!(projected, vec![record]);
    }

    #[test]
    fn current_item_state_is_bounded_and_rehydrates_a_typed_query() {
        let node = Node::in_memory();
        let first = commit_test_record(&node, "record-1", "first");
        let second = commit_test_record(&node, "record-2", "second");
        let request =
            ItemStateRequest::for_item::<TestRecord>(node.node_id(), ScopeId::new("session:test"));
        let snapshot = node.item_state_snapshot(request).unwrap();
        assert_eq!(snapshot.serving_node, node.node_id());
        assert!(snapshot.through.is_some());
        assert_eq!(snapshot.items.len(), 2);
        assert_eq!(
            snapshot.query(GetAllTestRecords).unwrap().value,
            vec![first, second]
        );
    }

    #[test]
    fn item_state_pages_hold_the_first_log_ceiling_during_concurrent_commits() {
        let node = Node::in_memory();
        let first = commit_test_record(&node, "record-1", "first");
        let third = commit_test_record(&node, "record-3", "third");
        let request =
            ItemStateRequest::for_serving_item::<TestRecord>(ScopeId::new("session:test"))
                .with_page_size(1);
        let first_page = node.item_state_page(request).unwrap();
        assert_eq!(first_page.items.len(), 1);
        assert!(first_page.next_after_item_id.is_some());
        let through = first_page.through;
        let (mut snapshot, next) = ItemStateSnapshot::from_first_page(first_page).unwrap();

        let concurrent = commit_test_record(&node, "record-2", "too late for this snapshot");
        let next = next.unwrap();
        assert_eq!(next.snapshot_through, through);
        let second_page = node.item_state_page(next.clone()).unwrap();
        assert_eq!(second_page.through, through);
        assert!(snapshot.append_page(&next, second_page).unwrap().is_none());
        assert_eq!(
            snapshot.query(GetAllTestRecords).unwrap().value,
            vec![first, third]
        );
        assert_eq!(
            node.query_items(GetAllTestRecords).unwrap(),
            vec![
                TestRecord {
                    id: TestRecordId::from("record-1"),
                    value: "first".to_owned(),
                },
                concurrent,
                TestRecord {
                    id: TestRecordId::from("record-3"),
                    value: "third".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn typed_item_stream_applies_each_atomic_update_or_none_of_it() {
        let node = Node::in_memory();
        let first = commit_test_record(&node, "record-1", "initial");
        let snapshot = node
            .item_state_snapshot(ItemStateRequest::for_serving_item::<TestRecord>(
                ScopeId::new("session:test"),
            ))
            .unwrap();
        let (initial, mut stream) =
            ItemQueryStream::from_snapshot(&snapshot, GetAllTestRecords).unwrap();
        assert_eq!(initial.value, vec![first.clone()]);

        let second = commit_test_record(&node, "record-2", "live");
        let follow = snapshot.follow_request().unwrap();
        let update = node
            .events_after(snapshot.through)
            .unwrap()
            .iter()
            .find_map(|envelope| follow.update_from_envelope(envelope).transpose())
            .transpose()
            .unwrap()
            .unwrap();
        assert_eq!(stream.apply(&update).unwrap().value, vec![first, second]);

        let before_invalid = stream.current();
        let mut invalid = update;
        invalid.changes.push(ItemMutation {
            service_id: TestRecord::SERVICE_ID.as_str().to_owned(),
            item_type: TestRecord::ITEM_TYPE.to_owned(),
            item_id: "broken".to_owned(),
            schema_version: TestRecord::SCHEMA_VERSION,
            operation: MutationOperation::Set,
            payload: Some(b"not-json".to_vec()),
        });
        invalid.through = LogPosition::new(invalid.through.get().saturating_add(1));
        assert!(stream.apply(&invalid).is_err());
        assert_eq!(stream.current(), before_invalid);
    }

    #[test]
    fn typed_query_watch_replays_then_tracks_replicated_batches_without_a_gap() {
        let source = Node::in_memory();
        let first = commit_test_record(&source, "record-1", "initial");
        let replica = Node::in_memory();
        for event in source.events_after(None).unwrap() {
            let _status = replica.ingest(event).unwrap();
        }
        let (snapshot, mut watch) = replica
            .watch_items_in(
                source.node_id(),
                ScopeId::new("session:test"),
                GetAllTestRecords,
            )
            .unwrap();
        assert_eq!(snapshot.value, vec![first.clone()]);
        assert!(
            watch
                .recv_timeout(Duration::from_millis(5))
                .unwrap()
                .is_none()
        );

        let second = commit_test_record(&source, "record-2", "live");
        for event in source.events_after(None).unwrap() {
            let _status = replica.ingest(event).unwrap();
        }
        let update = watch.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(update.value, vec![first, second]);
        assert!(watch.try_recv().unwrap().is_none());
    }

    #[test]
    fn typed_query_watch_from_tracks_every_scope_owned_by_one_source() {
        let node = Node::in_memory();
        let first =
            commit_test_record_in(&node, ScopeId::new("session:first"), "record-1", "first");
        let (snapshot, mut watch) = node
            .watch_items_from(node.node_id(), GetAllTestRecords)
            .unwrap();
        assert_eq!(snapshot.value, vec![first.clone()]);

        let second =
            commit_test_record_in(&node, ScopeId::new("session:second"), "record-2", "second");
        let update = watch.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(update.value, vec![first, second]);
        assert!(watch.try_recv().unwrap().is_none());
    }

    #[test]
    fn typed_query_watch_advances_across_other_item_types_in_the_same_scope() {
        let node = Node::in_memory();
        let record = commit_test_record(&node, "record-1", "stable");
        let (snapshot, mut watch) = node
            .watch_items_in(
                node.node_id(),
                ScopeId::new("session:test"),
                GetAllTestRecords,
            )
            .unwrap();

        commit_test_marker(&node, "marker-1", "same atomic cursor stream");
        let update = watch.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();

        assert_eq!(update.value, vec![record]);
        assert!(
            snapshot
                .through
                .is_none_or(|through| update.position > through)
        );
    }

    #[test]
    fn malformed_item_mutation_is_rejected_before_commit() {
        let node = Node::in_memory();
        let request = request(CommandId::new());
        let executing = node.admit(request.clone()).unwrap().snapshot().clone();
        let invalid = ItemMutation {
            service_id: TestRecord::SERVICE_ID.as_str().to_owned(),
            item_type: TestRecord::ITEM_TYPE.to_owned(),
            item_id: "record-1".to_owned(),
            schema_version: TestRecord::SCHEMA_VERSION,
            operation: MutationOperation::Delete,
            payload: Some(Vec::new()),
        };
        let result = node.commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: request.scope_id,
                causal_parents: vec![executing.updated_at],
                changes: vec![invalid],
            },
            Vec::new(),
        );
        assert!(matches!(result, Err(NodeError::InvalidItemMutation(_))));
        assert!(
            !node
                .command(request.id)
                .unwrap()
                .unwrap()
                .state
                .is_committed()
        );
    }

    #[test]
    fn scoped_replication_omits_other_scopes_and_advances_its_watermark() {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let wanted = request(CommandId::new());
        let mut hidden = request(CommandId::new());
        hidden.scope_id = ScopeId::new("session:hidden");

        source.submit(wanted.clone()).unwrap();
        source.submit(hidden.clone()).unwrap();
        source.claim(wanted.id).unwrap();
        source
            .commit(wanted.id, batch(&wanted), b"done".to_vec())
            .unwrap();

        let scoped = source.export_scope(wanted.scope_id.clone(), None).unwrap();
        assert_eq!(scoped.events.len(), 3);
        assert_eq!(
            scoped
                .events
                .iter()
                .map(|event| event.position)
                .collect::<Vec<_>>(),
            vec![
                LogPosition::new(1),
                LogPosition::new(3),
                LogPosition::new(4)
            ]
        );
        assert_eq!(scoped.through, Some(LogPosition::new(4)));
        let report = target.ingest_scoped_batch(scoped).unwrap();
        assert_eq!(report.applied, 3);
        assert!(
            target
                .command(wanted.id)
                .unwrap()
                .is_some_and(|command| command.state.is_committed())
        );
        assert!(target.command(hidden.id).unwrap().is_none());

        source.cancel(hidden.id, "hidden cancellation").unwrap();
        let advanced = source
            .export_scope(wanted.scope_id, report.through)
            .unwrap();
        assert!(advanced.events.is_empty());
        assert_eq!(advanced.after, Some(LogPosition::new(4)));
        assert_eq!(advanced.through, Some(LogPosition::new(5)));
        let advanced_report = target.ingest_scoped_batch(advanced).unwrap();
        assert_eq!(advanced_report.applied, 0);
        assert_eq!(advanced_report.through, Some(LogPosition::new(5)));
    }

    #[test]
    fn short_lived_client_submits_and_only_a_handler_claims_execution() {
        let node = Node::in_memory();
        let request = request(CommandId::new());

        let submitted = node.submit(request.clone()).unwrap();
        assert!(matches!(submitted.state, CommandState::Submitted));
        assert!(matches!(
            node.claim(request.id).unwrap(),
            CommandAdmission::Execute(_)
        ));
        assert!(matches!(
            node.claim(request.id).unwrap(),
            CommandAdmission::Resume(CommandSnapshot {
                state: CommandState::Executing,
                ..
            })
        ));
    }

    #[test]
    fn cancellation_is_terminal_idempotent_and_blocks_execution() {
        let node = Node::in_memory();
        let queued = request(CommandId::new());
        node.submit(queued.clone()).unwrap();

        let cancelled = node.cancel(queued.id, "operator stopped it").unwrap();
        assert!(matches!(
            cancelled.state,
            CommandState::Cancelled { ref reason } if reason == "operator stopped it"
        ));
        assert!(cancelled.state.is_terminal_locally());
        assert!(!cancelled.state.is_committed());
        assert!(!node.claim(queued.id).unwrap().should_execute());
        assert_eq!(
            node.cancel(queued.id, "different retry reason").unwrap(),
            cancelled
        );

        let running = request(CommandId::new());
        node.admit(running.clone()).unwrap();
        node.cancel(running.id, "cancel running work").unwrap();
        assert_eq!(
            node.commit(running.id, batch(&running), Vec::new()),
            Err(NodeError::CommandNotExecuting(running.id))
        );
    }

    #[test]
    fn stale_lifecycle_events_cannot_resurrect_a_cancelled_command() {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let command = request(CommandId::new());
        source.submit(command.clone()).unwrap();
        source.claim(command.id).unwrap();
        source.cancel(command.id, "stop").unwrap();

        for event in source.events_after(None).unwrap().into_iter().rev() {
            target.ingest(event).unwrap();
        }
        assert!(matches!(
            target.command(command.id).unwrap().map(|value| value.state),
            Some(CommandState::Cancelled { reason }) if reason == "stop"
        ));
    }

    #[test]
    fn failed_durable_append_never_changes_visible_state() {
        let node = Node::from_journal(Arc::new(FailingJournal {
            node_id: NodeId::new(),
        }))
        .unwrap();
        let command = request(CommandId::new());
        let mut events = node.subscribe(None).unwrap();

        assert!(matches!(
            node.submit(command.clone()),
            Err(NodeError::Backend(_))
        ));
        assert!(node.command(command.id).unwrap().is_none());
        assert!(node.events_after(None).unwrap().is_empty());
        assert!(events.try_recv().is_none());
    }

    #[test]
    fn subscription_replays_then_continues_without_a_cursor_gap() {
        let node = Node::in_memory();
        let first = request(CommandId::new());
        node.admit(first).unwrap();

        let mut events = node.subscribe(None).unwrap();
        assert_eq!(events.recv().unwrap().position, LogPosition::new(1));

        let second = request(CommandId::new());
        let second_id = second.id;
        node.admit(second).unwrap();
        assert_eq!(events.recv().unwrap().position, LogPosition::new(2));
        assert!(node.command(second_id).unwrap().is_some());
    }

    #[test]
    fn subscription_from_now_omits_existing_history_and_follows_new_events() {
        let node = Node::in_memory();
        node.admit(request(CommandId::new())).unwrap();

        let mut events = node.subscribe_from_now().unwrap();
        assert!(events.try_recv().is_none());

        node.admit(request(CommandId::new())).unwrap();
        assert_eq!(events.recv().unwrap().position, LogPosition::new(2));
    }

    #[test]
    fn scope_catalog_is_sorted_and_deduplicated() {
        let node = Node::in_memory();
        let mut second = request(CommandId::new());
        second.scope_id = ScopeId::new("session:zulu");
        node.admit(second).unwrap();
        let mut first = request(CommandId::new());
        first.scope_id = ScopeId::new("session:alpha");
        node.admit(first).unwrap();
        let mut duplicate = request(CommandId::new());
        duplicate.scope_id = ScopeId::new("session:zulu");
        node.admit(duplicate).unwrap();

        assert_eq!(
            node.scope_ids().unwrap(),
            vec![ScopeId::new("session:alpha"), ScopeId::new("session:zulu")]
        );
    }

    #[test]
    fn commit_rejects_a_batch_from_another_scope() {
        let node = Node::in_memory();
        let request = request(CommandId::new());
        node.admit(request.clone()).unwrap();
        let mut wrong = batch(&request);
        wrong.scope_id = ScopeId::new("session:other");

        assert_eq!(
            node.commit(request.id, wrong, Vec::new()),
            Err(NodeError::BatchMismatch(request.id))
        );
        assert!(matches!(
            node.command(request.id)
                .unwrap()
                .map(|snapshot| snapshot.state),
            Some(CommandState::Executing)
        ));
    }

    #[test]
    fn another_node_ingests_origin_events_exactly_once() {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let request = request(CommandId::new());
        source.admit(request.clone()).unwrap();
        source
            .commit(request.id, batch(&request), b"done".to_vec())
            .unwrap();
        let source_events = source.events_after(None).unwrap();

        for event in &source_events {
            assert!(matches!(
                target.ingest(event.clone()).unwrap(),
                IngestStatus::Applied { .. }
            ));
        }
        let first_source_event = source_events.first().cloned().unwrap();
        assert_eq!(
            target.ingest(first_source_event.clone()).unwrap(),
            IngestStatus::Duplicate
        );
        assert!(
            target
                .command(request.id)
                .unwrap()
                .is_some_and(|command| command.state.is_committed())
        );

        let target_events = target.events_after(None).unwrap();
        assert_eq!(target_events.len(), source_events.len());
        let first_target_event = target_events.first().unwrap();
        assert_eq!(first_target_event.position, LogPosition::new(1));
        assert_eq!(first_target_event.origin, first_source_event.origin);
    }

    #[test]
    fn command_origin_survives_replication_without_becoming_the_replica() {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let source_command = request(CommandId::new());
        source.submit(source_command.clone()).unwrap();

        assert_eq!(
            source.command_origin(source_command.id).unwrap(),
            Some(source.node_id())
        );
        assert_eq!(target.command_origin(source_command.id).unwrap(), None);
        target.ingest_batch(source.export(None).unwrap()).unwrap();
        assert_eq!(
            target.command_origin(source_command.id).unwrap(),
            Some(source.node_id())
        );
        assert_ne!(source.node_id(), target.node_id());

        let target_command = request(CommandId::new());
        target.submit(target_command.clone()).unwrap();
        assert_eq!(
            target.command_origin(target_command.id).unwrap(),
            Some(target.node_id())
        );
    }

    #[test]
    fn invalid_batch_cursor_is_rejected_before_any_event_is_ingested() {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let command = request(CommandId::new());
        source.admit(command).unwrap();
        let mut batch = source.export(None).unwrap();
        batch.through = Some(LogPosition::new(99));

        assert!(matches!(
            target.ingest_batch(batch),
            Err(NodeError::InvalidReplicationBatch(_))
        ));
        assert!(target.events_after(None).unwrap().is_empty());
    }

    #[test]
    fn live_events_are_filtered_and_drop_only_for_a_slow_subscriber() {
        let node_id = NodeId::new();
        let hub = LiveEventHub::new(node_id);
        let capacity = NonZeroUsize::new(1).unwrap();
        let mut all = hub.subscribe(Vec::new(), capacity).unwrap();
        let mut selected = hub
            .subscribe(vec!["session:a".to_owned()], capacity)
            .unwrap();

        let first = hub.publish("session:a", b"one".to_vec()).unwrap();
        assert_eq!(first.delivered, 2);
        assert_eq!(first.dropped, 0);
        let second = hub.publish("session:a", b"two".to_vec()).unwrap();
        assert_eq!(second.delivered, 0);
        assert_eq!(second.dropped, 2);

        let all_first = all.recv().unwrap();
        let selected_first = selected.recv().unwrap();
        assert_eq!(all_first, selected_first);
        assert_eq!(all_first.source_node, node_id);
        assert_eq!(all_first.sequence, 1);

        let unrelated = hub.publish("session:b", b"other".to_vec()).unwrap();
        assert_eq!(unrelated.delivered, 1);
        assert_eq!(unrelated.dropped, 0);
        assert_eq!(all.recv().unwrap().topic, "session:b");
        assert!(selected.try_recv().is_none());

        drop(all);
        let resumed = hub.publish("session:a", b"three".to_vec()).unwrap();
        assert_eq!(resumed.sequence, 3);
        assert_eq!(resumed.delivered, 1);
        assert_eq!(resumed.dropped, 0);
        assert_eq!(selected.recv().unwrap().payload, b"three");
    }
}
